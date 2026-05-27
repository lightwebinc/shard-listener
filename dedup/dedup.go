// Package dedup implements a fixed-capacity sliding-window deduplicator
// for downstream egress in shard-listener.
//
// Frames are keyed by (groupIdx, subtreeID, seqNum). When a multicast
// retransmit arrives after the inline frame has already been forwarded
// downstream, the dedup set causes the listener to suppress the duplicate
// before egress while still letting the gap tracker observe it.
//
// Implementation is a fixed-size map plus a ring buffer of inserted keys.
// On insert past capacity, the oldest key is evicted. Each entry carries a
// monotonic expiry timestamp; expired hits are treated as misses and replaced.
// The set is goroutine-safe via a single mutex.
package dedup

import (
	"sync"
	"time"
)

// Key identifies a unique frame for deduplication purposes.
type Key struct {
	GroupIdx  uint32
	SubtreeID [32]byte
	SeqNum    uint64
}

// Set is a fixed-capacity, TTL-bounded duplicate detector.
type Set struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration

	entries map[Key]int64 // key → expiry unix-nanos
	ring    []Key
	head    int // next insert position
	count   int // populated slots
}

// New constructs a Set with the given capacity (number of distinct keys it
// can remember) and TTL (max age of a remembered key). capacity <= 0 disables
// the set (every SeenAndAdd returns false). ttl <= 0 disables expiry.
func New(capacity int, ttl time.Duration) *Set {
	if capacity <= 0 {
		return &Set{}
	}
	return &Set{
		capacity: capacity,
		ttl:      ttl,
		entries:  make(map[Key]int64, capacity),
		ring:     make([]Key, capacity),
	}
}

// SeenAndAdd reports whether key was already remembered (not expired) and
// inserts it (refreshing the expiry) when not. A disabled Set always returns
// false and does not record anything.
func (s *Set) SeenAndAdd(key Key) bool {
	if s.capacity == 0 {
		return false
	}
	now := time.Now().UnixNano()
	expiry := now
	if s.ttl > 0 {
		expiry = now + s.ttl.Nanoseconds()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if exp, ok := s.entries[key]; ok {
		if s.ttl <= 0 || exp >= now {
			// Refresh expiry but report as duplicate.
			s.entries[key] = expiry
			return true
		}
		// Stale: fall through and replace the entry.
	}

	// Evict the oldest key if the ring is full.
	if s.count == s.capacity {
		old := s.ring[s.head]
		delete(s.entries, old)
	} else {
		s.count++
	}
	s.ring[s.head] = key
	s.head = (s.head + 1) % s.capacity
	s.entries[key] = expiry
	return false
}

// Len returns the number of currently remembered keys (including expired
// entries that have not yet been overwritten). For tests/diagnostics.
func (s *Set) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}
