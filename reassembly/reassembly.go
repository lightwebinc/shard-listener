// Package reassembly implements BRC-130 fragment reassembly for the listener.
//
// # Overview
//
// The proxy splits large BSV transactions into K BRC-130 fragment datagrams,
// each carrying a slice of the original payload together with TxID, FragIndex,
// FragTotal, and OrigPayloadLen. The listener receives fragments in any order
// and reassembles them into the original payload keyed by TxID.
//
// # Reassembly buffer
//
// A [Buffer] holds at most MaxSlots reassembly slots. Each slot is keyed by a
// [32]byte TxID and tracks the K expected fragments. When all K fragments
// arrive the payload is verified (SHA256d(payload) == TxID, optional) and
// delivered via the callback.
//
// Slots that never complete are evicted after TTL. The oldest slot is evicted
// when the slot limit is reached.
//
// # Thread safety
//
// [Buffer] is safe for concurrent use from multiple goroutines (one per
// SO_REUSEPORT worker).
package reassembly

import (
	"crypto/sha256"
	"fmt"
	"sync"
	"time"

	"github.com/lightwebinc/shard-common/frame"
)

const (
	// DefaultMaxSlots is the default maximum number of concurrent reassembly
	// slots. Each slot holds up to OrigPayloadLen bytes; MaxSlots bounds the
	// peak heap pressure from in-flight large transactions.
	DefaultMaxSlots = 4096

	// DefaultTTL is the default time before an incomplete slot is abandoned.
	DefaultTTL = 10 * time.Second

	// DefaultMaxObjectBytes bounds one declared original payload (OrigPayloadLen).
	// The field is ATTACKER-DECLARED: without a bound, one fragment claiming
	// OrigPayloadLen≈4 GiB with FragTotal=65535 opens a slot whose frags array
	// alone costs ~1.5 MiB — 4096 such datagrams command gigabytes of heap.
	// 64 MiB matches the object-stream ceiling (objfmt.DefaultMaxObject); the
	// BEEF plane (OrigFrameVer 0x09) is separately clamped to the operator's
	// -beef-max-object-bytes via SetMaxObjectBytesV9.
	DefaultMaxObjectBytes = 64 << 20
)

// Callback is invoked when BRC-124/BRC-128 (OrigFrameVer V2) reassembly
// completes successfully. payload is the reassembled bytes — a newly allocated
// slice owned by the caller. f is the synthetic [frame.Frame] built from the
// fragment metadata (TxID, SubtreeID, HashKey, SeqNum). The callback is called
// with the Buffer's lock held; it must not call back into the Buffer.
type Callback func(payload []byte, f *frame.Frame)

// BlockCallback is invoked when BRC-131 (OrigFrameVer V4) reassembly
// completes. payload is the reassembled BRC-131 payload; bf carries the
// block-frame metadata reconstructed from fragment header byte 7 (MsgType)
// and the TxID slot (ContentID).
type BlockCallback func(payload []byte, bf *frame.BlockFrame)

// SubtreeDataCallback is invoked when BRC-132 (OrigFrameVer V5) reassembly
// completes. payload is the reassembled subtree data payload; sf carries the
// frame metadata (MsgType, SubtreeID, HashKey, SeqNum). SHA256d verification
// is never applied to V5 slots because SubtreeID is a Merkle root, not a
// double-hash of the payload.
type SubtreeDataCallback func(payload []byte, sf *frame.SubtreeDataFrame)

// BEEFCallback is invoked when BRC-148 (OrigFrameVer V9) reassembly
// completes. payload is the reassembled BEEF object; bf carries the frame
// metadata — ContentID from the fragment TxID slot (which is SHA-256d of
// the object by construction, so the verifyHash check applies exactly) and
// TopicID from the fragment SubtreeID slot.
type BEEFCallback func(payload []byte, bf *frame.BEEFFrame)

// Buffer holds in-progress BRC-130 reassembly slots.
type Buffer struct {
	mu                sync.Mutex
	slots             map[[32]byte]*slot
	insertOrder       [][32]byte // eviction order (FIFO)
	maxSlots          int
	ttl               time.Duration
	verifyHash        bool                // SHA256d check for V2 slots
	verifyMerkle      bool                // optional Merkle root check for V5 slots
	onComplete        Callback            // V2 (FrameVerV2) completion
	onCompleteBlock   BlockCallback       // V4 (FrameVerV4) completion
	onCompleteSubtree SubtreeDataCallback // V5 (FrameVerV5) completion
	onCompleteBEEF    BEEFCallback        // V9 (FrameVerV9) completion
	onAbandoned       func()              // metrics hook: one call per evicted slot
	// onIncomplete is called for a slot evicted with SOME fragments present,
	// carrying the flow key and the SeqNums that never arrived. It is what turns
	// an abandoned object into a recovery request: losing one trailing fragment
	// loses the WHOLE object, and no successor frame reveals it, so without this
	// the listener silently discards objects it could have asked for.
	onIncomplete func(hashKey uint64, groupIdx uint32, subtreeID [32]byte, missing []uint64)
	// groupIdxFn resolves a fragment's shard group index. Injected so this package
	// stays free of shard semantics.
	groupIdxFn       func(*frame.FragFrame) uint32
	onStarted        func() // metrics hook
	onHashMismatch   func() // metrics hook (SHA256d mismatch, V2)
	onMerkleMismatch func() // metrics hook (Merkle root mismatch, V5)
	maxObject        int    // general declared-length cap (DefaultMaxObjectBytes)
	maxObjectV9      int    // BRC-148 plane cap (-beef-max-object-bytes); 0 = use general
}

// slot holds the fragments received so far for one TxID.
type slot struct {
	key            [32]byte // map key: txID, or SHA-256(ContentID ∥ TopicID) for V9
	txID           [32]byte
	subtreeID      [32]byte
	hashKey        uint64 // from the first fragment received
	seqNum         uint64 // from the first fragment received
	origPayloadLen uint32
	fragTotal      uint16
	received       uint16   // count of distinct fragments received
	frags          [][]byte // indexed by FragIndex; nil = not yet received
	// fragSeq holds each received fragment's own SeqNum, indexed by FragIndex
	// (0 = not received). Without it an incomplete slot knows WHICH FragIndex is
	// missing but has no way back to the SeqNum a NACK must name — the slot's
	// scalar seqNum belongs to whichever fragment happened to arrive first.
	// Interpolating between known neighbours (below) needs only per-object SeqNum
	// monotonicity; deriving from a single base index would additionally assume
	// strictly consecutive per-object emission, which the fragmenter does not
	// guarantee.
	fragSeq []uint64
	// groupIdx is the flow's shard group, supplied by the caller (this package
	// deliberately knows nothing about shard group semantics). Needed so a recovery
	// request for missing fragments names the correct flow.
	groupIdx     uint32
	deadline     time.Time
	origFrameVer byte // from the first fragment received (0/2=V2, 4=V4, 5=V5)
	msgType      byte // frame-type-specific message type preserved from byte 7
}

// New creates a Buffer.
//
//   - maxSlots: maximum concurrent reassembly slots (0 → DefaultMaxSlots).
//   - ttl: slot TTL before abandonment (0 → DefaultTTL).
//   - verifyHash: if true, SHA256d(payload) is verified against TxID for V2
//     slots; mismatches are dropped. Always false for V5 subtree data slots.
//   - cb: called on successful V2 (FrameVerV2) completion.
func New(maxSlots int, ttl time.Duration, verifyHash bool, cb Callback) *Buffer {
	if maxSlots <= 0 {
		maxSlots = DefaultMaxSlots
	}
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Buffer{
		slots:      make(map[[32]byte]*slot, maxSlots),
		maxSlots:   maxSlots,
		ttl:        ttl,
		verifyHash: verifyHash,
		onComplete: cb,
		maxObject:  DefaultMaxObjectBytes,
	}
}

// SetMaxObjectBytes overrides the general declared-length bound (0 keeps the
// default; this is a hard cap on OrigPayloadLen, never an allocation hint).
func (b *Buffer) SetMaxObjectBytes(n int) {
	if n > 0 {
		b.maxObject = n
	}
}

// SetMaxObjectBytesV9 sets the BRC-148 object-plane bound: fragments with
// OrigFrameVer 0x09 are clamped to the operator's -beef-max-object-bytes
// (matching the ingress bound, BRC-149's MUST), independent of the general cap.
func (b *Buffer) SetMaxObjectBytesV9(n int) { b.maxObjectV9 = n }

// SetAbandonedHook sets a metrics hook called once per abandoned slot.
func (b *Buffer) SetAbandonedHook(fn func()) { b.onAbandoned = fn }

// SetIncompleteHook installs the callback invoked when a partially-filled slot is
// evicted. Fragments are cached and NACK-recoverable individually (each carries
// its own HashKey/SeqNum), so the missing ones can be requested exactly like any
// other gap.
func (b *Buffer) SetIncompleteHook(fn func(hashKey uint64, groupIdx uint32, subtreeID [32]byte, missing []uint64)) {
	b.onIncomplete = fn
}

// SetGroupIdxFunc supplies the shard group resolver used to label a slot's flow.
// Without it slots carry group 0 and a recovery request cannot name the flow.
func (b *Buffer) SetGroupIdxFunc(fn func(*frame.FragFrame) uint32) { b.groupIdxFn = fn }

// notifyIncomplete reports the SeqNums an evicted slot never received. Must be
// called with b.mu held, BEFORE the slot is removed.
func (b *Buffer) notifyIncomplete(s *slot) {
	if b.onIncomplete == nil {
		return
	}
	if missing := s.missingSeqNums(); len(missing) > 0 {
		b.onIncomplete(s.hashKey, s.groupIdx, s.subtreeID, missing)
	}
}

// SetStartedHook sets a metrics hook called when a new slot is opened.
func (b *Buffer) SetStartedHook(fn func()) { b.onStarted = fn }

// SetHashMismatchHook sets a metrics hook called when SHA256d verification fails (V2).
func (b *Buffer) SetHashMismatchHook(fn func()) { b.onHashMismatch = fn }

// SetMerkleMismatchHook sets a metrics hook called when optional Merkle root
// verification fails for a V5 subtree data slot.
func (b *Buffer) SetMerkleMismatchHook(fn func()) { b.onMerkleMismatch = fn }

// SetBlockCallback registers the callback invoked on successful V4 (BRC-131)
// reassembly. If nil, completed V4 slots are silently discarded.
func (b *Buffer) SetBlockCallback(cb BlockCallback) { b.onCompleteBlock = cb }

// SetBEEFCallback registers the callback invoked on successful V9 (BRC-148)
// completion. Without it, reassembled BEEF objects are dropped.
func (b *Buffer) SetBEEFCallback(cb BEEFCallback) { b.onCompleteBEEF = cb }

// SetSubtreeDataCallback registers the callback invoked on successful V5
// (BRC-132) reassembly. SHA256d verification is never applied for V5 slots.
// If nil, completed V5 slots are silently discarded.
func (b *Buffer) SetSubtreeDataCallback(cb SubtreeDataCallback) { b.onCompleteSubtree = cb }

// SetVerifyMerkle enables optional post-reassembly Merkle root verification
// for V5 subtree data slots. This is expensive at large node counts and is
// disabled by default.
func (b *Buffer) SetVerifyMerkle(v bool) { b.verifyMerkle = v }

// Observe processes one BRC-130 fragment. It opens a new slot on the first
// fragment for a TxID, stores subsequent fragments, and calls the completion
// callback when all fragments have arrived.
//
// Expired slots are lazily evicted when Observe is called.
func (b *Buffer) Observe(ff *frame.FragFrame) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	b.evictExpired(now)

	key := slotKey(ff)
	s, exists := b.slots[key]

	if !exists {
		// Reject pathological fragment metadata before opening a slot.
		if ff.FragTotal == 0 || ff.FragIndex >= ff.FragTotal {
			return
		}
		if ff.OrigPayloadLen == 0 || uint32(ff.FragTotal) > ff.OrigPayloadLen+1 {
			// FragTotal > payload bytes is impossible under any valid MTU.
			return
		}
		// Declared-length bound BEFORE the slot exists: OrigPayloadLen sizes
		// both the frags array (via FragTotal) and the final assembly copy.
		maxObj := b.maxObject
		if ff.OrigFrameVer == frame.FrameVerV9 && b.maxObjectV9 > 0 {
			maxObj = b.maxObjectV9
		}
		if maxObj > 0 && int(ff.OrigPayloadLen) > maxObj {
			return
		}

		// Evict the oldest slot if at capacity.
		if len(b.slots) >= b.maxSlots {
			b.evictOldest()
		}

		s = &slot{
			key:            key,
			txID:           ff.TxID,
			subtreeID:      ff.SubtreeID,
			hashKey:        ff.HashKey,
			seqNum:         ff.SeqNum,
			origPayloadLen: ff.OrigPayloadLen,
			fragTotal:      ff.FragTotal,
			frags:          make([][]byte, ff.FragTotal),
			fragSeq:        make([]uint64, ff.FragTotal),
			groupIdx:       groupIdxOf(b.groupIdxFn, ff),
			deadline:       now.Add(b.ttl),
			origFrameVer:   ff.OrigFrameVer,
			msgType:        ff.MsgType,
		}
		b.slots[key] = s
		b.insertOrder = append(b.insertOrder, key)
		if b.onStarted != nil {
			b.onStarted()
		}
	}

	// Ignore duplicates and mismatched metadata.
	if ff.FragTotal != s.fragTotal || ff.FragIndex >= s.fragTotal {
		return
	}
	if s.frags[ff.FragIndex] != nil {
		return // duplicate
	}

	// Store a copy of the fragment data (the source buffer is reused by the
	// receive loop between calls).
	cp := make([]byte, len(ff.FragData))
	copy(cp, ff.FragData)
	s.frags[ff.FragIndex] = cp
	s.fragSeq[ff.FragIndex] = ff.SeqNum
	s.received++

	if s.received < s.fragTotal {
		return // not complete yet
	}

	// All fragments arrived — reassemble.
	b.complete(s)
}

// complete assembles the payload, dispatches the appropriate callback based on
// OrigFrameVer, and removes the slot. Must be called with b.mu held.
//
// OrigFrameVer dispatch:
//   - 0x00 / 0x02 → SHA256d verification (if verifyHash); deliver via onComplete (V2).
//   - 0x04        → deliver via onCompleteBlock (V4 BRC-131); no SHA256d.
//   - 0x05        → deliver via onCompleteSubtree (V5 BRC-132); no SHA256d;
//     optional Merkle root verification via verifyMerkle.
func (b *Buffer) complete(s *slot) {
	payload := make([]byte, 0, s.origPayloadLen)
	for _, frag := range s.frags {
		payload = append(payload, frag...)
	}

	switch s.origFrameVer {
	case frame.FrameVerV4:
		// BRC-131 block control: deliver without SHA256d verification.
		bf := &frame.BlockFrame{
			MsgType: s.msgType,
			HashKey: s.hashKey,
			SeqNum:  s.seqNum,
			Payload: payload,
		}
		copy(bf.ContentID[:], s.txID[:])
		b.removeSlot(s.key)
		if b.onCompleteBlock != nil {
			b.onCompleteBlock(payload, bf)
		}

	case frame.FrameVerV5:
		// BRC-132 subtree data: SHA256d verification never applies.
		// Merkle root recomputation (verifyMerkle) is deferred to the callback
		// layer because it requires payload decoding + tree recomputation.
		sf := &frame.SubtreeDataFrame{
			MsgType: s.msgType,
			HashKey: s.hashKey,
			SeqNum:  s.seqNum,
			Payload: payload,
		}
		copy(sf.SubtreeID[:], s.txID[:])
		b.removeSlot(s.key)
		if b.onCompleteSubtree != nil {
			b.onCompleteSubtree(payload, sf)
		}

	case frame.FrameVerV9:
		// BRC-148 BEEF object: the TxID slot is the ContentID —
		// SHA-256d(object) by definition — so the verifyHash check is the
		// spec's reassembly verification exactly. Deliver with FrameVer and
		// TopicID preserved so the object routes down the BEEF path, not
		// the tx path.
		if b.verifyHash {
			first := sha256.Sum256(payload)
			second := sha256.Sum256(first[:])
			if second != s.txID {
				if b.onHashMismatch != nil {
					b.onHashMismatch()
				}
				b.removeSlot(s.key)
				return
			}
		}
		bf := &frame.BEEFFrame{
			HashKey: s.hashKey,
			SeqNum:  s.seqNum,
			Payload: payload,
		}
		copy(bf.ContentID[:], s.txID[:])
		copy(bf.TopicID[:], s.subtreeID[:])
		b.removeSlot(s.key)
		if b.onCompleteBEEF != nil {
			b.onCompleteBEEF(payload, bf)
		}

	default:
		// OrigFrameVer 0x00 / 0x02 (or any unrecognised value): treat as V2.
		// SHA256d verification applied when verifyHash is set.
		if b.verifyHash {
			first := sha256.Sum256(payload)
			second := sha256.Sum256(first[:])
			if second != s.txID {
				if b.onHashMismatch != nil {
					b.onHashMismatch()
				}
				b.removeSlot(s.key)
				return
			}
		}

		// Build a synthetic Frame so the caller can route it through the
		// existing egress and gap-tracking paths unchanged.
		f := &frame.Frame{
			Version:   frame.FrameVerV2,
			TxID:      s.txID,
			HashKey:   s.hashKey,
			SeqNum:    s.seqNum,
			SubtreeID: s.subtreeID,
			Payload:   payload,
		}
		b.removeSlot(s.key)
		if b.onComplete != nil {
			b.onComplete(payload, f)
		}
	}
}

// slotKey returns the reassembly map key for a fragment. BRC-124/131/132
// fragments key on the offset-8 field alone. BRC-148 BEEF fragments MUST key
// on the (ContentID, TopicID) PAIR: sibling emissions of one object to
// different topics share a ContentID by construction, so a bare-ContentID key
// collapses them into one slot and silently delivers only the first topic.
// Mirrors the pair key the proxy claims at ingress.
func slotKey(ff *frame.FragFrame) [32]byte {
	if ff.OrigFrameVer != frame.FrameVerV9 {
		return ff.TxID
	}
	var buf [64]byte
	copy(buf[:32], ff.TxID[:])      // ContentID
	copy(buf[32:], ff.SubtreeID[:]) // TopicID
	return sha256.Sum256(buf[:])
}

// evictExpired removes all slots whose deadline has passed.
// Must be called with b.mu held.
// missingSeqNums returns the SeqNums of fragments this slot never received.
//
// A fragment's SeqNum is recovered by interpolating from the nearest RECEIVED
// fragment: SeqNums within one object are monotonic in FragIndex, so for a known
// fragment at index j with SeqNum S, the fragment at index i is S+(i-j). Scanning
// from the nearest neighbour keeps that extrapolation as short as possible, which
// matters if the origin ever interleaves other frames into the same flow — a long
// extrapolation would drift, a short one stays exact for the common contiguous case.
//
// Returns nil if no fragment was ever received (nothing to anchor on).
func (s *slot) missingSeqNums() []uint64 {
	if s.received == 0 {
		return nil
	}
	out := make([]uint64, 0, int(s.fragTotal)-int(s.received))
	for i := 0; i < int(s.fragTotal); i++ {
		if s.frags[i] != nil {
			continue
		}
		// Nearest received neighbour, preferring the closest on either side.
		best := -1
		for d := 1; d < int(s.fragTotal); d++ {
			if i-d >= 0 && s.frags[i-d] != nil {
				best = i - d
				break
			}
			if i+d < int(s.fragTotal) && s.frags[i+d] != nil {
				best = i + d
				break
			}
		}
		if best < 0 {
			continue
		}
		base := s.fragSeq[best]
		delta := int64(i - best)
		if int64(base)+delta < 0 {
			continue
		}
		out = append(out, uint64(int64(base)+delta))
	}
	return out
}

func (b *Buffer) evictExpired(now time.Time) {
	for _, txID := range b.insertOrder {
		s, ok := b.slots[txID]
		if !ok {
			continue
		}
		if now.After(s.deadline) {
			b.notifyIncomplete(s)
			if b.onAbandoned != nil {
				b.onAbandoned()
			}
			b.removeSlot(txID)
		}
	}
	// Compact insertOrder.
	live := b.insertOrder[:0]
	for _, txID := range b.insertOrder {
		if _, ok := b.slots[txID]; ok {
			live = append(live, txID)
		}
	}
	b.insertOrder = live
}

// evictOldest removes the oldest slot (FIFO order). Must be called with b.mu held.
func (b *Buffer) evictOldest() {
	for i, txID := range b.insertOrder {
		if _, ok := b.slots[txID]; ok {
			// No incomplete-notify here: this is capacity pressure, and asking for
			// missing fragments while already at the slot cap adds load exactly
			// when there is none to spare. TTL expiry is the tail-loss path.
			if b.onAbandoned != nil {
				b.onAbandoned()
			}
			b.removeSlot(txID)
			b.insertOrder = append(b.insertOrder[:i], b.insertOrder[i+1:]...)
			return
		}
	}
}

// removeSlot deletes a slot from the map (not from insertOrder — compact separately).
func (b *Buffer) removeSlot(txID [32]byte) {
	delete(b.slots, txID)
}

// Stats returns current reassembly buffer statistics.
func (b *Buffer) Stats() (activeSlots int) {
	b.mu.Lock()
	activeSlots = len(b.slots)
	b.mu.Unlock()
	return
}

// Tick evicts all slots whose TTL has expired. It is safe to call from a
// background goroutine; it acquires the buffer's mutex and returns quickly.
// Calling Tick periodically (e.g., every second) prevents lazy-eviction lag
// from contaminating metric windows across successive test runs.
func (b *Buffer) Tick() {
	b.mu.Lock()
	b.evictExpired(time.Now())
	b.mu.Unlock()
}

// Len returns the number of active (incomplete) reassembly slots.
func (b *Buffer) Len() int {
	b.mu.Lock()
	n := len(b.slots)
	b.mu.Unlock()
	return n
}

// Purge evicts all slots, calling the abandoned hook for each.
func (b *Buffer) Purge() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for txID := range b.slots {
		// No incomplete-notify: a purge is a deliberate reset, not observed loss.
		if b.onAbandoned != nil {
			b.onAbandoned()
		}
		delete(b.slots, txID)
	}
	b.insertOrder = b.insertOrder[:0]
}

// validateFragment is a guard used by tests to verify fragment metadata.
func validateFragment(ff *frame.FragFrame) error {
	if ff.FragTotal == 0 {
		return fmt.Errorf("FragTotal=0")
	}
	if ff.FragIndex >= ff.FragTotal {
		return fmt.Errorf("FragIndex=%d >= FragTotal=%d", ff.FragIndex, ff.FragTotal)
	}
	return nil
}

// groupIdxOf resolves a fragment's group index via the injected resolver, or 0
// when none is installed (recovery of that slot's tail is then skipped rather
// than mislabelled).
func groupIdxOf(fn func(*frame.FragFrame) uint32, ff *frame.FragFrame) uint32 {
	if fn == nil {
		return 0
	}
	return fn(ff)
}
