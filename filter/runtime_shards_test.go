package filter_test

import (
	"sync"
	"testing"

	"github.com/lightwebinc/shard-listener/filter"
)

// Runtime shards (BRC-139 pilot groups) widen a -shard-include filter and
// narrow back when they are withdrawn; the -shard-include shards and the
// subtree rules hold throughout.
func TestSetRuntimeShards(t *testing.T) {
	excluded := subtreeID(0xEE)
	f := filter.New([]uint32{0}, nil, [][32]byte{excluded}, nil)
	allow := func(group uint32, subtree [32]byte) string {
		ok, reason := f.Allow(group, makeFrame(subtree))
		if ok {
			return "allow"
		}
		return reason
	}

	if got := allow(1, subtreeID(1)); got != "shard_filter" {
		t.Fatalf("shard 1 before runtime shards: %s, want shard_filter", got)
	}
	f.SetRuntimeShards([]uint32{1})
	if got := allow(1, subtreeID(1)); got != "allow" {
		t.Errorf("runtime shard 1: %s, want allow", got)
	}
	if got := allow(1, excluded); got != "subtree_exclude" {
		t.Errorf("excluded subtree on runtime shard 1: %s, want subtree_exclude", got)
	}
	if got := allow(0, subtreeID(1)); got != "allow" {
		t.Errorf("-shard-include shard 0: %s, want allow", got)
	}
	f.SetRuntimeShards(nil)
	if got := allow(1, subtreeID(1)); got != "shard_filter" {
		t.Errorf("shard 1 after its runtime entry was withdrawn: %s, want shard_filter", got)
	}
	if got := allow(0, subtreeID(1)); got != "allow" {
		t.Errorf("-shard-include shard 0 after withdrawal: %s, want allow", got)
	}
}

// A filter without -shard-include admits every shard; runtime shards must not
// turn it into an allowlist.
func TestSetRuntimeShards_OpenFilterStaysOpen(t *testing.T) {
	f := filter.New(nil, nil, nil, nil)
	f.SetRuntimeShards([]uint32{1})
	if ok, reason := f.Allow(2, makeFrame(subtreeID(1))); !ok {
		t.Errorf("shard 2 on an open filter: %s, want allow", reason)
	}
}

// Workers call Allow while the manifest applier replaces the runtime shards;
// run with -race.
func TestSetRuntimeShards_ConcurrentWithAllow(t *testing.T) {
	f := filter.New([]uint32{0}, nil, nil, nil)
	fr := makeFrame(subtreeID(1))
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 2000 {
			f.SetRuntimeShards([]uint32{uint32(i % 4)})
		}
	}()
	go func() {
		defer wg.Done()
		for range 2000 {
			if ok, _ := f.Allow(0, fr); !ok {
				t.Error("-shard-include shard 0 rejected during a runtime update")
				return
			}
		}
	}()
	wg.Wait()
}
