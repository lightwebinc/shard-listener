package discovery

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// EndpointEntry represents a discovered or seeded retry endpoint.
type EndpointEntry struct {
	Addr            string // host:port for NACK dispatch
	Tier            uint8  // 0 = closest to source (max 255)
	Preference      uint8  // weighting within a tier; higher = more preferred
	Flags           uint16
	Expires         time.Time // lastSeen + 3 × BeaconInterval
	InstanceID      uint32
	SupportsUnicast bool // derived from Flags & FlagUnicastRetransmit
}

// Registry maintains the live endpoint set from beacons plus the static seed list.
// It is safe for concurrent use.
type Registry struct {
	mu      sync.Mutex
	entries map[uint32]*EndpointEntry // keyed by InstanceID
	seeds   []*EndpointEntry          // static seeds (Tier=0xFF, Preference=0)

	// snapshot is the copy-on-write sorted slice; rebuilt on mutation.
	snapshot []*EndpointEntry
	dirty    bool
}

// NewRegistry creates an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		entries: make(map[uint32]*EndpointEntry),
	}
}

// Upsert inserts or refreshes an endpoint entry from a decoded ADVERT.
// Called by the beacon listener goroutine.
func (r *Registry) Upsert(a *ADVERT) {
	r.mu.Lock()
	defer r.mu.Unlock()

	addr := fmt.Sprintf("[%s]:%d", a.NACKAddr, a.NACKPort)
	ttl := time.Duration(a.BeaconInterval) * 3 * time.Second

	entry, ok := r.entries[a.InstanceID]
	if ok {
		entry.Addr = addr
		entry.Tier = a.Tier
		entry.Preference = a.Preference
		entry.Flags = a.Flags
		entry.Expires = time.Now().Add(ttl)
		entry.SupportsUnicast = a.Flags&FlagUnicastRetransmit != 0
	} else {
		r.entries[a.InstanceID] = &EndpointEntry{
			Addr:            addr,
			Tier:            a.Tier,
			Preference:      a.Preference,
			Flags:           a.Flags,
			Expires:         time.Now().Add(ttl),
			InstanceID:      a.InstanceID,
			SupportsUnicast: a.Flags&FlagUnicastRetransmit != 0,
		}
	}
	r.dirty = true
}

// Evict removes expired entries. Called periodically (1 s tick).
func (r *Registry) Evict() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	for id, e := range r.entries {
		if now.After(e.Expires) {
			delete(r.entries, id)
			r.dirty = true
		}
	}
}

// Seed loads static endpoint addresses at Tier=0xFF, Preference=0 (lowest priority).
// These are only used when no beacon-discovered endpoints exist at lower tiers.
func (r *Registry) Seed(addrs []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.seeds = make([]*EndpointEntry, 0, len(addrs))
	for i, addr := range addrs {
		r.seeds = append(r.seeds, &EndpointEntry{
			Addr:       addr,
			Tier:       0xFF,
			Preference: 0,
			InstanceID: uint32(0xFFFFF000 + i),                      // synthetic IDs
			Expires:    time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), // never expire
		})
	}
	r.dirty = true
}

// Snapshot returns a copy-on-write sorted slice of all active endpoints.
// Sorted by (Tier ASC, Preference DESC). The returned slice is safe to read
// without holding any lock. Callers must not modify the returned slice.
func (r *Registry) Snapshot() []*EndpointEntry {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.dirty || r.snapshot == nil {
		// Rebuild: merge beacon entries + seeds.
		//
		// Seeds sit at Tier 0xFF, so they always sort BEHIND every beacon entry
		// and are only ever reached after the discovered caches have missed.
		// They are therefore kept even when beacons exist, deduped by address so
		// a cache that both beacons and is seeded appears once at its real tier.
		//
		// Dropping them whenever any beacon existed silently truncated recovery
		// to beacon scope: with a site-scoped beacon a listener discovers only
		// its own site's caches, and if the loss is UPSTREAM of that site every
		// discovered cache is missing exactly the frames being asked for. The
		// NACK then MISSes its way through the local tier and the gap is evicted
		// while a remote cache holding the frame sits in the seed list, unasked.
		// Tier escalation is free (see gapEntry), so carrying the remote seeds
		// costs nothing until the local tier is exhausted.
		merged := make([]*EndpointEntry, 0, len(r.entries)+len(r.seeds))
		seen := make(map[string]struct{}, len(r.entries))
		for _, e := range r.entries {
			merged = append(merged, e)
			seen[e.Addr] = struct{}{}
		}
		for _, s := range r.seeds {
			if _, dup := seen[s.Addr]; !dup {
				merged = append(merged, s)
			}
		}

		sort.Slice(merged, func(i, j int) bool {
			if merged[i].Tier != merged[j].Tier {
				return merged[i].Tier < merged[j].Tier
			}
			return merged[i].Preference > merged[j].Preference // higher preference first
		})

		// Store a deep copy as the internal cache
		snap := make([]*EndpointEntry, len(merged))
		for i, e := range merged {
			cp := *e
			snap[i] = &cp
		}
		r.snapshot = snap
		r.dirty = false
	}

	// Always return a fresh deep copy so callers cannot mutate the cache.
	cp := make([]*EndpointEntry, len(r.snapshot))
	for i, e := range r.snapshot {
		dup := *e
		cp[i] = &dup
	}
	return cp
}

// Len returns the total number of entries (beacon + seed).
func (r *Registry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries) + len(r.seeds)
}
