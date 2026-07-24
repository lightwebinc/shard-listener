package fanout_test

import (
	"testing"

	"github.com/lightwebinc/shard-common/bundle"
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
	beef   int
	closed bool
}

func (r *recSink) SendBeef(_ []byte, _ *frame.BEEFFrame) error { r.beef++; return nil }

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

// bundleRecSink is a bundle-capable sink: it records whole bundles handed to it
// (and the member frames it would get if decoalesced, via the embedded recSink).
type bundleRecSink struct {
	recSink
	bundles int
	members int
}

func (b *bundleRecSink) SendBundle(raw []byte, bun *bundle.Bundle) error {
	b.bundles++
	b.members += len(bun.Members)
	return nil
}

// seqRecSink captures the per-member SeqNums delivered (non-capable path).
type seqRecSink struct {
	recSink
	seqs []uint64
}

func (s *seqRecSink) Send(raw []byte, f *frame.Frame) error {
	s.tx++
	s.seqs = append(s.seqs, f.SeqNum)
	return nil
}

func makeBundle(t *testing.T, group uint16, sub [32]byte, hashKey uint64, n int) ([]byte, *bundle.Bundle) {
	t.Helper()
	b := &bundle.Bundle{GroupIdx: group, ShardBits: 2, SubtreeID: sub, HashKey: hashKey, SeqNum: 7}
	for i := 0; i < n; i++ {
		b.Members = append(b.Members, bundle.Member{Tx: []byte{byte('a' + i)}})
	}
	raw, err := b.Encode()
	if err != nil {
		t.Fatalf("encode bundle: %v", err)
	}
	parsed, err := bundle.Decode(raw)
	if err != nil {
		t.Fatalf("decode bundle: %v", err)
	}
	return raw, parsed
}

// A bundle-capable consumer receives the whole bundle intact; a non-capable
// consumer on the same shard receives the decoalesced members, re-stamped with
// a fresh monotonic per-flow egress SeqNum.
func TestSendBundle_CapableWholeVsDecoalesce(t *testing.T) {
	eng := shard.New(0xFF05, shard.DefaultGroupID, 2)
	s := fanout.New(eng)

	capable := &bundleRecSink{}
	plain := &seqRecSink{}
	s.Apply([]*fanout.Consumer{
		{ID: "capable", Shards: []uint32{0}, Sink: capable, BundleCapable: true},
		{ID: "plain", Shards: []uint32{0}, Sink: plain},
	})

	raw, b := makeBundle(t, 0, subtreeID(0), 0xABCD, 3)
	if err := s.SendBundle(raw, b); err != nil {
		t.Fatalf("SendBundle: %v", err)
	}

	if capable.bundles != 1 || capable.members != 3 || capable.tx != 0 {
		t.Errorf("capable consumer: bundles=%d members=%d tx=%d, want 1/3/0 (whole bundle, no decoalesce)", capable.bundles, capable.members, capable.tx)
	}
	if plain.tx != 3 || len(plain.seqs) != 3 {
		t.Fatalf("plain consumer: tx=%d seqs=%v, want 3 decoalesced members", plain.tx, plain.seqs)
	}
	for i, sq := range plain.seqs {
		if sq != uint64(i+1) {
			t.Errorf("plain member %d SeqNum=%d, want %d (monotonic re-stamp)", i, sq, i+1)
		}
	}

	// A second bundle of the same flow continues the egress sequence (4,5,6).
	raw2, b2 := makeBundle(t, 0, subtreeID(0), 0xABCD, 3)
	if err := s.SendBundle(raw2, b2); err != nil {
		t.Fatalf("SendBundle 2: %v", err)
	}
	if got := plain.seqs[3:]; len(got) != 3 || got[0] != 4 || got[2] != 6 {
		t.Errorf("second bundle seqs=%v, want contiguous 4,5,6", got)
	}
}

// A bundle whose HashKey matches a consumer's own ingress identity is excluded
// wholesale from that consumer (bundle-granularity own-traffic exclusion).
func TestSendBundle_OwnTrafficExcluded(t *testing.T) {
	eng := shard.New(0xFF05, shard.DefaultGroupID, 2)
	s := fanout.New(eng)

	var ownIP [16]byte
	ownIP[15] = 9
	sub := subtreeID(0)
	ownHash := seqhash.Hash(ownIP, 0, sub)

	c := &seqRecSink{}
	s.Apply([]*fanout.Consumer{
		{ID: "c", Shards: []uint32{0}, Sink: c, OwnIngressIP: ownIP},
	})

	raw, b := makeBundle(t, 0, sub, ownHash, 4)
	if err := s.SendBundle(raw, b); err != nil {
		t.Fatalf("SendBundle: %v", err)
	}
	if c.tx != 0 {
		t.Errorf("own-traffic bundle delivered (tx=%d), want 0 (excluded)", c.tx)
	}

	// A bundle with a different HashKey is delivered normally.
	raw2, b2 := makeBundle(t, 0, sub, ownHash+1, 4)
	if err := s.SendBundle(raw2, b2); err != nil {
		t.Fatalf("SendBundle 2: %v", err)
	}
	if c.tx != 4 {
		t.Errorf("non-own bundle: tx=%d, want 4", c.tx)
	}
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

// countObs records the tunnel-bound ingress the fan-out taps on the own-traffic
// drop path.
type countObs struct {
	calls   int
	wire    int
	members int
}

func (o *countObs) ObserveIngress(wire, members int) {
	o.calls++
	o.wire += wire
	o.members += members
}

func TestIngressObserverTapsOwnTraffic(t *testing.T) {
	eng := shard.New(0xFF05, shard.DefaultGroupID, 2)
	s := fanout.New(eng)

	var ownA [16]byte
	ownA[15] = 0xAA
	obs := &countObs{}
	a := &recSink{}
	s.Apply([]*fanout.Consumer{
		{ID: "a", Shards: []uint32{0}, Sink: a, OwnIngressIP: ownA, IngressObs: obs},
	})

	// A frame A sent up its own tunnel: excluded from egress, observed as ingress.
	sub := subtreeID(0x07)
	f := txInShard(0, sub)
	groupIdx := eng.GroupIndex(&f.TxID)
	f.HashKey = seqhash.Hash(ownA, groupIdx, sub)
	raw := make([]byte, 200)
	if err := s.Send(raw, f); err != nil {
		t.Fatal(err)
	}
	if a.tx != 0 {
		t.Fatalf("own frame must stay excluded from egress, got a.tx=%d", a.tx)
	}
	if obs.calls != 1 || obs.wire != 200 || obs.members != 1 {
		t.Fatalf("tx ingress: calls=%d wire=%d members=%d, want 1/200/1", obs.calls, obs.wire, obs.members)
	}

	// A bundle A sent up its tunnel: excluded, observed with the member count.
	braw, b := makeBundle(t, 0, sub, seqhash.Hash(ownA, 0, sub), 5)
	if err := s.SendBundle(braw, b); err != nil {
		t.Fatal(err)
	}
	if a.tx != 0 {
		t.Fatalf("own bundle must stay excluded, got a.tx=%d", a.tx)
	}
	if obs.calls != 2 || obs.members != 6 { // 1 tx + 5 bundle members
		t.Fatalf("bundle ingress: calls=%d members=%d, want 2/6", obs.calls, obs.members)
	}

	// A frame that is not A's own traffic is delivered and NOT observed.
	f2 := txInShard(0, sub)
	f2.HashKey = 0xDEADBEEF
	if err := s.Send(make([]byte, 50), f2); err != nil {
		t.Fatal(err)
	}
	if a.tx != 1 {
		t.Fatalf("non-own frame must deliver, got a.tx=%d", a.tx)
	}
	if obs.calls != 2 {
		t.Fatalf("non-own frame must not be observed as ingress, calls=%d", obs.calls)
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
