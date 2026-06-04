// Package subtreegroup provides a thread-safe, time-bounded registry that
// maps 128-bit group IDs to sets of 32-byte subtree IDs. Listeners use it to
// implement dynamic subtree-group-based frame filtering (BRC-127).
//
// Entries are added by [SubtreeGroupAnnounceListener] as announcements arrive and
// expire automatically when not refreshed before their TTL elapses. [Evict]
// must be called periodically (e.g. on a 1-second tick) to purge stale entries.
//
// Only groups whose IDs appear in the subscribedGroups constructor argument are
// tracked; announcements for other group IDs are silently ignored.
package subtreegroup

import (
	"sync"
	"time"
)

// Registry stores the currently-live subtree-to-group memberships for a set
// of subscribed group IDs. Safe for concurrent use.
type Registry struct {
	mu               sync.RWMutex
	subscribedGroups map[[16]byte]struct{}               // set for O(1) membership test
	entries          map[[16]byte]map[[32]byte]time.Time // groupID → subtreeID → expiry
	defaultTTL       time.Duration
}

// New returns a Registry that tracks subtree IDs only for the specified
// group IDs. defaultTTL is used when an announcement carries TTL = 0.
func New(subscribedGroups [][16]byte, defaultTTL time.Duration) *Registry {
	r := &Registry{
		subscribedGroups: make(map[[16]byte]struct{}, len(subscribedGroups)),
		entries:          make(map[[16]byte]map[[32]byte]time.Time, len(subscribedGroups)),
		defaultTTL:       defaultTTL,
	}
	for _, g := range subscribedGroups {
		r.subscribedGroups[g] = struct{}{}
		r.entries[g] = make(map[[32]byte]time.Time)
	}
	return r
}

// Add records that subtreeID is a member of groupID with the given TTL.
// If ttl is zero, the registry's defaultTTL is used.
// The call is a no-op if groupID is not in the subscribed set.
func (r *Registry) Add(groupID [16]byte, subtreeID [32]byte, ttl time.Duration) {
	if _, ok := r.subscribedGroups[groupID]; !ok {
		return
	}
	if ttl <= 0 {
		ttl = r.defaultTTL
	}
	expiry := time.Now().Add(ttl)

	r.mu.Lock()
	r.entries[groupID][subtreeID] = expiry
	r.mu.Unlock()
}

// Remove deletes a specific (groupID, subtreeID) entry. No-op if absent.
func (r *Registry) Remove(groupID [16]byte, subtreeID [32]byte) {
	if _, ok := r.subscribedGroups[groupID]; !ok {
		return
	}
	r.mu.Lock()
	delete(r.entries[groupID], subtreeID)
	r.mu.Unlock()
}

// Contains reports whether subtreeID is a live member of any subscribed group.
// This is the hot-path call from filter.Allow(); it holds only a read lock.
func (r *Registry) Contains(subtreeID [32]byte) bool {
	r.mu.RLock()
	for _, subs := range r.entries {
		if expiry, ok := subs[subtreeID]; ok {
			if time.Now().Before(expiry) {
				r.mu.RUnlock()
				return true
			}
		}
	}
	r.mu.RUnlock()
	return false
}

// Evict removes all entries whose TTL has elapsed. Call this on a periodic
// tick (e.g. every second) to bound memory use. Returns the number of entries
// removed.
func (r *Registry) Evict() int {
	now := time.Now()
	var n int
	r.mu.Lock()
	for gid, subs := range r.entries {
		for sid, expiry := range subs {
			if now.After(expiry) {
				delete(r.entries[gid], sid)
				n++
			}
		}
	}
	r.mu.Unlock()
	return n
}

// Len returns the total number of (groupID, subtreeID) pairs currently stored,
// including entries that have expired but not yet been evicted.
func (r *Registry) Len() int {
	r.mu.RLock()
	n := 0
	for _, subs := range r.entries {
		n += len(subs)
	}
	r.mu.RUnlock()
	return n
}
