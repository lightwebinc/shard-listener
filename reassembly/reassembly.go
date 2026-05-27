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
	onAbandoned       func()              // metrics hook: one call per evicted slot
	onStarted         func()              // metrics hook
	onHashMismatch    func()              // metrics hook (SHA256d mismatch, V2)
	onMerkleMismatch  func()              // metrics hook (Merkle root mismatch, V5)
}

// slot holds the fragments received so far for one TxID.
type slot struct {
	txID           [32]byte
	subtreeID      [32]byte
	hashKey        uint64 // from the first fragment received
	seqNum         uint64 // from the first fragment received
	origPayloadLen uint32
	fragTotal      uint16
	received       uint16   // count of distinct fragments received
	frags          [][]byte // indexed by FragIndex; nil = not yet received
	deadline       time.Time
	origFrameVer   byte // from the first fragment received (0/2=V2, 4=V4, 5=V5)
	msgType        byte // frame-type-specific message type preserved from byte 7
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
	}
}

// SetAbandonedHook sets a metrics hook called once per abandoned slot.
func (b *Buffer) SetAbandonedHook(fn func()) { b.onAbandoned = fn }

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

	txID := ff.TxID
	s, exists := b.slots[txID]

	if !exists {
		// Reject pathological fragment metadata before opening a slot.
		if ff.FragTotal == 0 || ff.FragIndex >= ff.FragTotal {
			return
		}
		if ff.OrigPayloadLen == 0 || uint32(ff.FragTotal) > ff.OrigPayloadLen+1 {
			// FragTotal > payload bytes is impossible under any valid MTU.
			return
		}

		// Evict the oldest slot if at capacity.
		if len(b.slots) >= b.maxSlots {
			b.evictOldest()
		}

		s = &slot{
			txID:           txID,
			subtreeID:      ff.SubtreeID,
			hashKey:        ff.HashKey,
			seqNum:         ff.SeqNum,
			origPayloadLen: ff.OrigPayloadLen,
			fragTotal:      ff.FragTotal,
			frags:          make([][]byte, ff.FragTotal),
			deadline:       now.Add(b.ttl),
			origFrameVer:   ff.OrigFrameVer,
			msgType:        ff.MsgType,
		}
		b.slots[txID] = s
		b.insertOrder = append(b.insertOrder, txID)
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
		b.removeSlot(s.txID)
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
		b.removeSlot(s.txID)
		if b.onCompleteSubtree != nil {
			b.onCompleteSubtree(payload, sf)
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
				b.removeSlot(s.txID)
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
		b.removeSlot(s.txID)
		if b.onComplete != nil {
			b.onComplete(payload, f)
		}
	}
}

// evictExpired removes all slots whose deadline has passed.
// Must be called with b.mu held.
func (b *Buffer) evictExpired(now time.Time) {
	for _, txID := range b.insertOrder {
		s, ok := b.slots[txID]
		if !ok {
			continue
		}
		if now.After(s.deadline) {
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
