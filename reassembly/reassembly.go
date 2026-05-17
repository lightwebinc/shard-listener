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

	"github.com/lightwebinc/bitcoin-shard-common/frame"
)

const (
	// DefaultMaxSlots is the default maximum number of concurrent reassembly
	// slots. Each slot holds up to OrigPayloadLen bytes; MaxSlots bounds the
	// peak heap pressure from in-flight large transactions.
	DefaultMaxSlots = 4096

	// DefaultTTL is the default time before an incomplete slot is abandoned.
	DefaultTTL = 10 * time.Second
)

// Callback is invoked when reassembly completes successfully.
// payload is the reassembled bytes — a newly allocated slice owned by the
// caller. f is the synthetic [frame.Frame] built from the fragment metadata
// (TxID, SubtreeID, HashKey, SeqNum) so the caller can route it via the
// existing egress and gap-tracking paths. The callback is called with the
// Buffer's lock held; it must not call back into the Buffer.
type Callback func(payload []byte, f *frame.Frame)

// Buffer holds in-progress BRC-130 reassembly slots.
type Buffer struct {
	mu           sync.Mutex
	slots        map[[32]byte]*slot
	insertOrder  [][32]byte // eviction order (FIFO)
	maxSlots     int
	ttl          time.Duration
	verifyHash   bool
	onComplete   Callback
	onAbandoned  func()       // metrics hook: one call per evicted slot
	onStarted    func()       // metrics hook
	onHashMismatch func()     // metrics hook
}

// slot holds the fragments received so far for one TxID.
type slot struct {
	txID           [32]byte
	subtreeID      [32]byte
	hashKey        uint64   // from the first fragment received
	seqNum         uint64   // from the first fragment received
	origPayloadLen uint32
	fragTotal      uint16
	received       uint16         // count of distinct fragments received
	frags          [][]byte       // indexed by FragIndex; nil = not yet received
	deadline       time.Time
}

// New creates a Buffer.
//
//   - maxSlots: maximum concurrent reassembly slots (0 → DefaultMaxSlots).
//   - ttl: slot TTL before abandonment (0 → DefaultTTL).
//   - verifyHash: if true, SHA256d(payload) is verified against TxID; mismatches are dropped.
//   - cb: called on successful completion.
func New(maxSlots int, ttl time.Duration, verifyHash bool, cb Callback) *Buffer {
	if maxSlots <= 0 {
		maxSlots = DefaultMaxSlots
	}
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Buffer{
		slots:    make(map[[32]byte]*slot, maxSlots),
		maxSlots: maxSlots,
		ttl:      ttl,
		verifyHash: verifyHash,
		onComplete:  cb,
	}
}

// SetAbandonedHook sets a metrics hook called once per abandoned slot.
func (b *Buffer) SetAbandonedHook(fn func()) { b.onAbandoned = fn }

// SetStartedHook sets a metrics hook called when a new slot is opened.
func (b *Buffer) SetStartedHook(fn func()) { b.onStarted = fn }

// SetHashMismatchHook sets a metrics hook called when SHA256d verification fails.
func (b *Buffer) SetHashMismatchHook(fn func()) { b.onHashMismatch = fn }

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
	b.complete(s, now)
}

// complete assembles the payload, optionally verifies its hash, then invokes
// the callback and removes the slot.  Must be called with b.mu held.
func (b *Buffer) complete(s *slot, now time.Time) {
	payload := make([]byte, 0, s.origPayloadLen)
	for _, frag := range s.frags {
		payload = append(payload, frag...)
	}

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

	// Build a synthetic Frame so the caller can route it through the existing
	// egress and gap-tracking paths unchanged.
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

// frameError implements error for format messages without fmt dependency at runtime.
type frameError string

func (e frameError) Error() string { return string(e) }

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
