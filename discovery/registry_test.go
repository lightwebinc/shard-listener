package discovery

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func makeAdvert(tier, pref uint8, instanceID uint32) *ADVERT {
	return &ADVERT{
		Scope:          0x05,
		NACKAddr:       net.ParseIP("fd20::41"),
		NACKPort:       9300,
		Tier:           tier,
		Preference:     pref,
		BeaconInterval: 60,
		Flags:          FlagMulticastRetransmit,
		InstanceID:     instanceID,
	}
}

func TestRegistry_UpsertAndSnapshot(t *testing.T) {
	r := NewRegistry()
	r.Upsert(makeAdvert(0, 128, 1))
	r.Upsert(makeAdvert(1, 200, 2))

	snap := r.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("Snapshot len = %d, want 2", len(snap))
	}
	if snap[0].Tier != 0 {
		t.Errorf("snap[0].Tier = %d, want 0", snap[0].Tier)
	}
	if snap[1].Tier != 1 {
		t.Errorf("snap[1].Tier = %d, want 1", snap[1].Tier)
	}
}

func TestRegistry_Evict(t *testing.T) {
	r := NewRegistry()

	// Create an advert with very short TTL (BeaconInterval=1 → TTL=3s)
	a := makeAdvert(0, 128, 1)
	a.BeaconInterval = 1
	r.Upsert(a)

	// Manually expire the entry
	r.mu.Lock()
	for _, e := range r.entries {
		e.Expires = time.Now().Add(-1 * time.Second)
	}
	r.mu.Unlock()

	r.Evict()

	snap := r.Snapshot()
	if len(snap) != 0 {
		t.Errorf("expected 0 entries after eviction, got %d", len(snap))
	}
}

func TestRegistry_Evict_retainsLive(t *testing.T) {
	r := NewRegistry()
	r.Upsert(makeAdvert(0, 128, 1)) // TTL=180s, should survive

	// Add one that's expired
	a := makeAdvert(0, 128, 2)
	a.BeaconInterval = 1
	r.Upsert(a)
	r.mu.Lock()
	r.entries[2].Expires = time.Now().Add(-1 * time.Second)
	r.mu.Unlock()

	r.Evict()

	snap := r.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(snap))
	}
	if snap[0].InstanceID != 1 {
		t.Errorf("wrong entry survived: InstanceID=%d", snap[0].InstanceID)
	}
}

func TestRegistry_Seed(t *testing.T) {
	r := NewRegistry()
	r.Seed([]string{"host1:9300", "host2:9300"})

	snap := r.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("Snapshot len = %d, want 2", len(snap))
	}
	for _, e := range snap {
		if e.Tier != 0xFF {
			t.Errorf("seeded Tier = %d, want 0xFF", e.Tier)
		}
		if e.Preference != 0 {
			t.Errorf("seeded Preference = %d, want 0", e.Preference)
		}
	}
}

func TestRegistry_UpsertRefreshesExpiry(t *testing.T) {
	r := NewRegistry()
	a := makeAdvert(0, 128, 42)
	a.BeaconInterval = 10 // TTL=30s

	r.Upsert(a)
	r.mu.Lock()
	firstExpires := r.entries[42].Expires
	r.mu.Unlock()

	time.Sleep(10 * time.Millisecond)
	r.Upsert(a)

	r.mu.Lock()
	secondExpires := r.entries[42].Expires
	r.mu.Unlock()

	if !secondExpires.After(firstExpires) {
		t.Error("Upsert did not refresh expiry")
	}
}

func TestRegistry_Snapshot_copyOnWrite(t *testing.T) {
	r := NewRegistry()
	r.Upsert(makeAdvert(0, 128, 1))

	snap1 := r.Snapshot()

	// Mutate the returned entry
	snap1[0].Tier = 99

	snap2 := r.Snapshot()
	if snap2[0].Tier == 99 {
		t.Error("Snapshot is not a deep copy; mutation leaked")
	}
}

func TestRegistry_SnapshotSorting(t *testing.T) {
	r := NewRegistry()
	// Insert in reverse order
	r.Upsert(makeAdvert(2, 50, 1))
	r.Upsert(makeAdvert(0, 100, 2))
	r.Upsert(makeAdvert(0, 200, 3))
	r.Upsert(makeAdvert(1, 150, 4))
	r.Seed([]string{"seed:9300"})

	// Beacon entries sort ahead of the seed, which trails at Tier 0xFF.
	snap := r.Snapshot()
	if len(snap) != 5 {
		t.Fatalf("Snapshot len = %d, want 5 (4 beacons + trailing seed)", len(snap))
	}

	// Expected order: Tier 0 Pref 200, Tier 0 Pref 100, Tier 1 Pref 150, Tier 2 Pref 50, seed 0xFF
	expected := []struct {
		tier uint8
		pref uint8
	}{
		{0, 200},
		{0, 100},
		{1, 150},
		{2, 50},
		{0xFF, 0},
	}
	for i, want := range expected {
		if snap[i].Tier != want.tier || snap[i].Preference != want.pref {
			t.Errorf("snap[%d]: tier=%d pref=%d, want tier=%d pref=%d",
				i, snap[i].Tier, snap[i].Preference, want.tier, want.pref)
		}
	}
}

func TestRegistry_SeedRetainedBehindBeacons(t *testing.T) {
	// A beacon does NOT suppress the seeds: it only outranks them. Seeds are the
	// deeper fallback that recovers loss upstream of the beacon's scope, where
	// every locally-discovered cache is missing the same frames.
	r := NewRegistry()
	r.Seed([]string{"seed:9300"})
	r.Upsert(makeAdvert(0, 128, 1))

	snap := r.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("Snapshot len = %d, want 2 (beacon + seed fallback)", len(snap))
	}
	if snap[0].Tier != 0 {
		t.Errorf("beacon endpoint must sort first, got tier=%d", snap[0].Tier)
	}
	if snap[1].Tier != 0xFF || snap[1].Addr != "seed:9300" {
		t.Errorf("seed must sort last, got tier=%d addr=%q", snap[1].Tier, snap[1].Addr)
	}
}

func TestRegistry_SeedDedupedAgainstBeacon(t *testing.T) {
	// A cache that both beacons and appears in the static list must appear once,
	// at its real (beacon) tier — not twice.
	r := NewRegistry()
	adv := makeAdvert(0, 128, 1)
	advAddr := fmt.Sprintf("[%s]:%d", adv.NACKAddr, adv.NACKPort)
	r.Seed([]string{advAddr, "remote:9300"})
	r.Upsert(adv)

	snap := r.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("Snapshot len = %d, want 2 (deduped beacon + distinct seed)", len(snap))
	}
	if snap[0].Addr != advAddr || snap[0].Tier != 0 {
		t.Errorf("beacon entry wrong: addr=%q tier=%d", snap[0].Addr, snap[0].Tier)
	}
	if snap[1].Addr != "remote:9300" {
		t.Errorf("distinct seed missing, got %q", snap[1].Addr)
	}
}

func TestRegistry_SeedFallbackOnly(t *testing.T) {
	// Seeds ARE returned when no beacon entries exist.
	r := NewRegistry()
	r.Seed([]string{"seed1:9300", "seed2:9300"})

	snap := r.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("Snapshot len = %d, want 2 (seed fallback active)", len(snap))
	}
	for _, e := range snap {
		if e.Tier != 0xFF {
			t.Errorf("seed Tier = %d, want 0xFF", e.Tier)
		}
	}
}
