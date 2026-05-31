package manifest

import (
	"context"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	commanifest "github.com/lightwebinc/shard-common/manifest"
)

func mustAddr(s string) netip.Addr {
	a, err := netip.ParseAddr(s)
	if err != nil {
		panic(err)
	}
	return a
}

func authoritativePilotManifest(instanceID uint32, shardBits uint8, groups []uint16) *frame.ShardManifest {
	m := &frame.ShardManifest{
		Flags:            frame.ShardManifestFlagAuthoritative | frame.ShardManifestFlagGroupsValid | frame.ShardManifestFlagPilotOnly,
		InstanceID:       instanceID,
		Epoch:            1746800000,
		AnnounceInterval: 300,
		ShardBits:        shardBits,
		RoleHint:         frame.RoleHintManifestOnly,
		Groups:           append([]uint16(nil), groups...),
	}
	return m
}

func TestApplier_FiresShardBitsChangeHook(t *testing.T) {
	reg := commanifest.NewRegistry(60 * time.Second)
	ev := commanifest.NewEvaluator(commanifest.EvaluatorConfig{
		Quorum:     2,
		Hysteresis: 1 * time.Nanosecond,
	})
	// Two authoritative announcers agreeing on SB=8.
	reg.Upsert(mustAddr("fd20::1"), authoritativePilotManifest(1, 8, []uint16{0, 1}))
	reg.Upsert(mustAddr("fd20::2"), authoritativePilotManifest(2, 8, []uint16{0, 1}))

	var mu sync.Mutex
	fired := []struct{ prev, next uint8 }{}

	a := &Applier{
		Registry:  reg,
		Evaluator: ev,
		Interval:  10 * time.Millisecond,
		Hooks: Hooks{
			OnShardBitsChange: func(prev, next uint8) {
				mu.Lock()
				defer mu.Unlock()
				fired = append(fired, struct{ prev, next uint8 }{prev, next})
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	a.Run(ctx)

	mu.Lock()
	defer mu.Unlock()
	if len(fired) == 0 {
		t.Fatalf("OnShardBitsChange never fired")
	}
	if fired[0].prev != 0 || fired[0].next != 8 {
		t.Errorf("first change = %+v, want {prev:0 next:8}", fired[0])
	}
}

func TestApplier_PilotGroupsDiff(t *testing.T) {
	reg := commanifest.NewRegistry(60 * time.Second)
	ev := commanifest.NewEvaluator(commanifest.EvaluatorConfig{
		Quorum:     2,
		Hysteresis: 1 * time.Nanosecond,
	})

	var mu sync.Mutex
	addedSeen := []uint16{}
	removedSeen := []uint16{}

	a := &Applier{
		Registry:  reg,
		Evaluator: ev,
		Interval:  10 * time.Millisecond,
		Hooks: Hooks{
			OnPilotGroupsChange: func(added, removed []uint16) {
				mu.Lock()
				defer mu.Unlock()
				addedSeen = append(addedSeen, added...)
				removedSeen = append(removedSeen, removed...)
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go a.Run(ctx)

	// Tick 1: two pilots agree on groups {0, 1}.
	reg.Upsert(mustAddr("fd20::1"), authoritativePilotManifest(1, 8, []uint16{0, 1}))
	reg.Upsert(mustAddr("fd20::2"), authoritativePilotManifest(2, 8, []uint16{0, 1}))
	time.Sleep(50 * time.Millisecond)

	// Tick 2: both pilots add group 2.
	reg.Upsert(mustAddr("fd20::1"), authoritativePilotManifest(1, 8, []uint16{0, 1, 2}))
	reg.Upsert(mustAddr("fd20::2"), authoritativePilotManifest(2, 8, []uint16{0, 1, 2}))
	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	// We should have seen the bootstrap of {0, 1} as additions, then {2}.
	found2 := false
	for _, v := range addedSeen {
		if v == 2 {
			found2 = true
			break
		}
	}
	if !found2 {
		t.Errorf("OnPilotGroupsChange did not report addition of 2; saw added=%v", addedSeen)
	}
}

func TestDiffUint16(t *testing.T) {
	cases := []struct {
		name          string
		before, after []uint16
		wantAdded     []uint16
		wantRemoved   []uint16
	}{
		{
			name:        "all added",
			before:      nil,
			after:       []uint16{1, 2, 3},
			wantAdded:   []uint16{1, 2, 3},
			wantRemoved: nil,
		},
		{
			name:        "all removed",
			before:      []uint16{1, 2, 3},
			after:       nil,
			wantAdded:   nil,
			wantRemoved: []uint16{1, 2, 3},
		},
		{
			name:        "partial overlap",
			before:      []uint16{1, 2, 3},
			after:       []uint16{2, 3, 4},
			wantAdded:   []uint16{4},
			wantRemoved: []uint16{1},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, r := diffUint16(tc.before, tc.after)
			if !equalUint16(a, tc.wantAdded) {
				t.Errorf("added = %v, want %v", a, tc.wantAdded)
			}
			if !equalUint16(r, tc.wantRemoved) {
				t.Errorf("removed = %v, want %v", r, tc.wantRemoved)
			}
		})
	}
}

func equalUint16(a, b []uint16) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
