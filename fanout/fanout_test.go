package fanout_test

import (
	"testing"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/seqhash"
	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/shard-listener/fanout"
	"github.com/lightwebinc/shard-listener/filter"
)

// recSink records the IDs/counts of frames delivered to one consumer.
type recSink struct {
	tx     int
	blocks int
	raw    int
	closed bool
}

func (r *recSink) Send(raw []byte, f *frame.Frame) error                        { r.tx++; return nil }
func (r *recSink) SendBlock(raw []byte, bf *frame.BlockFrame) error             { r.blocks++; return nil }
func (r *recSink) SendSubtreeData(raw []byte, sf *frame.SubtreeDataFrame) error { return nil }
func (r *recSink) SendRaw(buf []byte) error                                     { r.raw++; return nil }
func (r *recSink) Proto() string                                                { return "rec" }
func (r *recSink) Close() error                                                 { r.closed = true; return nil }

// txInShard returns a frame whose GroupIndex (top shardBits bits of TxID[0])
// equals the given shard, for shardBits=2.
func txInShard(shardIdx byte, sub [32]byte) *frame.Frame {
	var txid [32]byte
	txid[0] = shardIdx << 6 // top 2 bits = shardIdx
	return &frame.Frame{Version: frame.FrameVerV2, TxID: txid, SubtreeID: sub}
}

func subtreeID(b byte) [32]byte {
	var id [32]byte
	id[0] = b
	return id
}

func TestShardRouting(t *testing.T) {
	eng := shard.New(0xFF05, shard.DefaultGroupID, 2)
	s := fanout.New(eng)

	a := &recSink{}
	b := &recSink{}
	both := &recSink{}
	all := &recSink{}

	s.Apply([]*fanout.Consumer{
		{ID: "a", Shards: []uint32{0}, Sink: a},
		{ID: "b", Shards: []uint32{1}, Sink: b},
		{ID: "both", Shards: []uint32{0, 1}, Sink: both},
		{ID: "all", Shards: nil, Sink: all}, // no restriction
	})

	if err := s.Send(nil, txInShard(0, [32]byte{})); err != nil {
		t.Fatalf("send: %v", err)
	}
	// shard 0 → a, both, all (not b)
	if a.tx != 1 || both.tx != 1 || all.tx != 1 || b.tx != 0 {
		t.Fatalf("shard0 routing wrong: a=%d b=%d both=%d all=%d", a.tx, b.tx, both.tx, all.tx)
	}

	if err := s.Send(nil, txInShard(1, [32]byte{})); err != nil {
		t.Fatalf("send: %v", err)
	}
	// shard 1 → b, both, all (a unchanged)
	if a.tx != 1 || b.tx != 1 || both.tx != 2 || all.tx != 2 {
		t.Fatalf("shard1 routing wrong: a=%d b=%d both=%d all=%d", a.tx, b.tx, both.tx, all.tx)
	}

	// shard 2 → only all (no consumer subscribed)
	if err := s.Send(nil, txInShard(2, [32]byte{})); err != nil {
		t.Fatalf("send: %v", err)
	}
	if all.tx != 3 || a.tx != 1 || b.tx != 1 || both.tx != 2 {
		t.Fatalf("shard2 routing wrong: a=%d b=%d both=%d all=%d", a.tx, b.tx, both.tx, all.tx)
	}
}

func TestSubtreeResidualFilter(t *testing.T) {
	eng := shard.New(0xFF05, shard.DefaultGroupID, 2)
	s := fanout.New(eng)

	want := subtreeID(0x01)
	// Consumer subscribed to shard 0 but only subtree 0x01 (subtree-only filter,
	// empty shardInclude so the index handles shard selection).
	c := &recSink{}
	s.Apply([]*fanout.Consumer{
		{ID: "c", Shards: []uint32{0}, Filt: filter.New(nil, [][32]byte{want}, nil, nil), Sink: c},
	})

	// Right shard, wrong subtree → dropped by residual filter.
	if err := s.Send(nil, txInShard(0, subtreeID(0x99))); err != nil {
		t.Fatal(err)
	}
	if c.tx != 0 {
		t.Fatalf("wrong-subtree frame should be filtered, got tx=%d", c.tx)
	}

	// Right shard, right subtree → delivered.
	if err := s.Send(nil, txInShard(0, want)); err != nil {
		t.Fatal(err)
	}
	if c.tx != 1 {
		t.Fatalf("matching-subtree frame should be delivered, got tx=%d", c.tx)
	}
}

func TestOwnTrafficExclusion(t *testing.T) {
	eng := shard.New(0xFF05, shard.DefaultGroupID, 2)
	s := fanout.New(eng)

	var ownA, ownB [16]byte
	ownA[15] = 0xAA
	ownB[15] = 0xBB

	a := &recSink{} // opted in, own-ingress ownA
	b := &recSink{} // opted in, own-ingress ownB
	c := &recSink{} // opted out (zero own-ingress) — control: always receives
	s.Apply([]*fanout.Consumer{
		{ID: "a", Shards: []uint32{0}, Sink: a, OwnIngressIP: ownA},
		{ID: "b", Shards: []uint32{0}, Sink: b, OwnIngressIP: ownB},
		{ID: "c", Shards: []uint32{0}, Sink: c},
	})

	// Frame originated by A: stamp the HashKey the proxy would derive for ownA.
	sub := subtreeID(0x07)
	f := txInShard(0, sub)
	groupIdx := eng.GroupIndex(&f.TxID)
	f.HashKey = seqhash.Hash(ownA, groupIdx, sub)

	if err := s.Send(nil, f); err != nil {
		t.Fatal(err)
	}
	// A's own frame is suppressed for A, but cross-delivered to B and the opted-out C.
	if a.tx != 0 {
		t.Fatalf("own frame should be excluded for originator, got a.tx=%d", a.tx)
	}
	if b.tx != 1 || c.tx != 1 {
		t.Fatalf("own frame must still cross-deliver: b.tx=%d c.tx=%d", b.tx, c.tx)
	}

	// A frame with an unrelated (non-proxy) HashKey matches nobody's identity →
	// exclusion no-ops, everyone receives it.
	f2 := txInShard(0, sub)
	f2.HashKey = 0xDEADBEEF
	if err := s.Send(nil, f2); err != nil {
		t.Fatal(err)
	}
	if a.tx != 1 || b.tx != 2 || c.tx != 2 {
		t.Fatalf("non-matching HashKey must deliver to all: a=%d b=%d c=%d", a.tx, b.tx, c.tx)
	}
}

func TestControlFramesBroadcast(t *testing.T) {
	eng := shard.New(0xFF05, shard.DefaultGroupID, 2)
	s := fanout.New(eng)

	a := &recSink{}
	b := &recSink{}
	s.Apply([]*fanout.Consumer{
		{ID: "a", Shards: []uint32{0}, Sink: a},
		{ID: "b", Shards: []uint32{3}, Sink: b},
	})

	if err := s.SendBlock(nil, &frame.BlockFrame{}); err != nil {
		t.Fatal(err)
	}
	if err := s.SendRaw(nil); err != nil {
		t.Fatal(err)
	}
	// Both consumers receive control frames regardless of shard subscription.
	if a.blocks != 1 || b.blocks != 1 || a.raw != 1 || b.raw != 1 {
		t.Fatalf("control frames should broadcast: a{blocks=%d raw=%d} b{blocks=%d raw=%d}", a.blocks, a.raw, b.blocks, b.raw)
	}
}

func TestApplyReplacesAndClose(t *testing.T) {
	eng := shard.New(0xFF05, shard.DefaultGroupID, 2)
	s := fanout.New(eng)

	old := &recSink{}
	s.Apply([]*fanout.Consumer{{ID: "old", Shards: []uint32{0}, Sink: old}})

	// Replace the table; the old consumer is no longer in it.
	cur := &recSink{}
	s.Apply([]*fanout.Consumer{{ID: "cur", Shards: []uint32{0}, Sink: cur}})

	if err := s.Send(nil, txInShard(0, [32]byte{})); err != nil {
		t.Fatal(err)
	}
	if old.tx != 0 || cur.tx != 1 {
		t.Fatalf("apply should replace table: old=%d cur=%d", old.tx, cur.tx)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !cur.closed {
		t.Fatal("Close should close current consumers")
	}
}
