package filter_test

import (
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-listener/filter"
	"github.com/lightwebinc/shard-listener/subtreegroup"
)

func makeFrame(subtree [32]byte) *frame.Frame {
	return &frame.Frame{
		Version:   frame.FrameVerV2,
		SubtreeID: subtree,
	}
}

func subtreeID(b byte) [32]byte {
	var id [32]byte
	id[0] = b
	return id
}

func TestAllowAll(t *testing.T) {
	f := filter.New(nil, nil, nil, nil)
	if ok, _ := f.Allow(0, makeFrame([32]byte{})); !ok {
		t.Error("empty filter should allow everything")
	}
	if ok, _ := f.Allow(1000, makeFrame(subtreeID(0xAB))); !ok {
		t.Error("empty filter should allow any shard/subtree")
	}
}

func TestShardInclude(t *testing.T) {
	f := filter.New([]uint32{5, 7}, nil, nil, nil)
	if ok, _ := f.Allow(5, makeFrame([32]byte{})); !ok {
		t.Error("shard 5 should be allowed")
	}
	if ok, _ := f.Allow(7, makeFrame([32]byte{})); !ok {
		t.Error("shard 7 should be allowed")
	}
	if ok, reason := f.Allow(3, makeFrame([32]byte{})); ok {
		t.Error("shard 3 should be denied")
	} else if reason != "shard_filter" {
		t.Errorf("expected reason shard_filter, got %q", reason)
	}
	if ok, reason := f.Allow(0, makeFrame([32]byte{})); ok {
		t.Error("shard 0 should be denied")
	} else if reason != "shard_filter" {
		t.Errorf("expected reason shard_filter, got %q", reason)
	}
}

func TestSubtreeInclude(t *testing.T) {
	allowed := subtreeID(0x01)
	f := filter.New(nil, [][32]byte{allowed}, nil, nil)
	if ok, _ := f.Allow(0, makeFrame(allowed)); !ok {
		t.Error("included subtree should be allowed")
	}
	if ok, reason := f.Allow(0, makeFrame(subtreeID(0x02))); ok {
		t.Error("non-included subtree should be denied")
	} else if reason != "subtree_include_miss" {
		t.Errorf("expected reason subtree_include_miss, got %q", reason)
	}
}

func TestSubtreeExclude(t *testing.T) {
	excluded := subtreeID(0xFF)
	f := filter.New(nil, nil, [][32]byte{excluded}, nil)
	if ok, reason := f.Allow(0, makeFrame(excluded)); ok {
		t.Error("excluded subtree should be denied")
	} else if reason != "subtree_exclude" {
		t.Errorf("expected reason subtree_exclude, got %q", reason)
	}
	if ok, _ := f.Allow(0, makeFrame(subtreeID(0x01))); !ok {
		t.Error("non-excluded subtree should be allowed")
	}
}

func TestExcludeOverridesInclude(t *testing.T) {
	id := subtreeID(0xAA)
	f := filter.New(nil, [][32]byte{id}, [][32]byte{id}, nil)
	if ok, _ := f.Allow(0, makeFrame(id)); ok {
		t.Error("exclude should win over include")
	}
}

func TestV1ZeroSubtreeNotInInclude(t *testing.T) {
	allowed := subtreeID(0x01)
	f := filter.New(nil, [][32]byte{allowed}, nil, nil)
	v1 := &frame.Frame{Version: frame.FrameVerV1, SubtreeID: [32]byte{}}
	if ok, _ := f.Allow(0, v1); ok {
		t.Error("BRC-12 frame with zero SubtreeID should be denied when subtree-include is non-empty")
	}
}

func TestV1ZeroSubtreeInInclude(t *testing.T) {
	f := filter.New(nil, [][32]byte{{}}, nil, nil)
	v1 := &frame.Frame{Version: frame.FrameVerV1, SubtreeID: [32]byte{}}
	if ok, _ := f.Allow(0, v1); !ok {
		t.Error("BRC-12 frame allowed when zero SubtreeID is explicitly included")
	}
}

func makeGroupID(b byte) [16]byte {
	var g [16]byte
	g[0] = b
	return g
}

func TestGroupRegistryAllows(t *testing.T) {
	reg := subtreegroup.New([][16]byte{makeGroupID(1)}, 900*time.Second)
	reg.Add(makeGroupID(1), subtreeID(0x55), 10*time.Second)
	f := filter.New(nil, nil, nil, reg)
	if ok, _ := f.Allow(0, makeFrame(subtreeID(0x55))); !ok {
		t.Error("subtree in subscribed group should be allowed")
	}
	if ok, reason := f.Allow(0, makeFrame(subtreeID(0x56))); ok {
		t.Error("subtree not in any group should be denied")
	} else if reason != "subtree_include_miss" {
		t.Errorf("expected subtree_include_miss, got %q", reason)
	}
}

func TestGroupRegistryAndStaticIncludeUnion(t *testing.T) {
	reg := subtreegroup.New([][16]byte{makeGroupID(1)}, 900*time.Second)
	reg.Add(makeGroupID(1), subtreeID(0x10), 10*time.Second)
	// Static include has 0x20, group has 0x10 — both should pass
	f := filter.New(nil, [][32]byte{subtreeID(0x20)}, nil, reg)
	if ok, _ := f.Allow(0, makeFrame(subtreeID(0x10))); !ok {
		t.Error("subtree in group should pass even when not in static include")
	}
	if ok, _ := f.Allow(0, makeFrame(subtreeID(0x20))); !ok {
		t.Error("subtree in static include should pass even when not in group")
	}
	if ok, _ := f.Allow(0, makeFrame(subtreeID(0x30))); ok {
		t.Error("subtree in neither static nor group should be denied")
	}
}

func TestGroupExcludeOverridesGroup(t *testing.T) {
	reg := subtreegroup.New([][16]byte{makeGroupID(1)}, 900*time.Second)
	id := subtreeID(0xCC)
	reg.Add(makeGroupID(1), id, 10*time.Second)
	// Same id is in the group but also in exclude — exclude wins
	f := filter.New(nil, nil, [][32]byte{id}, reg)
	if ok, reason := f.Allow(0, makeFrame(id)); ok {
		t.Error("exclude should win over group membership")
	} else if reason != "subtree_exclude" {
		t.Errorf("expected subtree_exclude, got %q", reason)
	}
}
