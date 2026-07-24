package fanout_test

import (
	"testing"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-common/seqhash"
	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/shard-listener/fanout"
)

func beefSinkFixture(t *testing.T) (*fanout.Sink, *shard.PlaneEngine) {
	t.Helper()
	s := fanout.New(shard.New(0xFF05, shard.DefaultGroupID, 8))
	pe, err := shard.NewPlane(0xFF05, shard.DefaultGroupID, 4, shard.DomainBEEF)
	if err != nil {
		t.Fatalf("NewPlane: %v", err)
	}
	s.SetBEEFEngine(pe)
	return s, pe
}

func beefFrameFor(t *testing.T, topic string, obj []byte) ([]byte, *frame.BEEFFrame) {
	t.Helper()
	raw, err := objfmt.BEEFMulticastBytes(objfmt.TopicID(topic), obj)
	if err != nil {
		t.Fatalf("BEEFMulticastBytes: %v", err)
	}
	bf, err := frame.DecodeBEEF(raw)
	if err != nil {
		t.Fatalf("DecodeBEEF: %v", err)
	}
	return raw, bf
}

var beefV1Obj = []byte{0x01, 0x00, 0xBE, 0xEF, 0x42}

// TestSendBeef_TopicAndVersionElection covers the spec's filter composition:
// group membership → topic filter → version filter → delivery, plus
// absent-filter admit-all (aggregator).
func TestSendBeef_TopicAndVersionElection(t *testing.T) {
	s, _ := beefSinkFixture(t)
	topic := "tm_files"
	tid := objfmt.TopicID(topic)

	elects := &recSink{}
	other := &recSink{}
	agg := &recSink{}
	wrongVer := &recSink{}

	s.Apply([]*fanout.Consumer{
		{ID: "elects", Sink: elects, TopicSet: map[[32]byte]struct{}{tid: {}}},
		{ID: "other", Sink: other, TopicSet: map[[32]byte]struct{}{objfmt.TopicID("tm_other"): {}}},
		{ID: "agg", Sink: agg}, // no topic filter, no version filter
		{ID: "wrongver", Sink: wrongVer, BEEFVersions: map[uint32]struct{}{objfmt.BEEFMarkerV2: {}}},
	})

	raw, bf := beefFrameFor(t, topic, beefV1Obj)
	if err := s.SendBeef(raw, bf); err != nil {
		t.Fatalf("SendBeef: %v", err)
	}

	if elects.beef != 1 {
		t.Errorf("electing consumer got %d, want 1", elects.beef)
	}
	if other.beef != 0 {
		t.Errorf("non-electing consumer got %d, want 0 (topic filter)", other.beef)
	}
	if agg.beef != 1 {
		t.Errorf("aggregator got %d, want 1 (absent filters admit all)", agg.beef)
	}
	if wrongVer.beef != 0 {
		t.Errorf("v2-only consumer got %d, want 0 (version filter)", wrongVer.beef)
	}
}

// TestSendBeef_ShardRouting proves a consumer with a shard restriction only
// receives BEEF frames on its elected band groups.
func TestSendBeef_ShardRouting(t *testing.T) {
	s, pe := beefSinkFixture(t)
	topic := "tm_routed"
	tid := objfmt.TopicID(topic)
	group := pe.GroupIndex(&tid)

	onGroup := &recSink{}
	offGroup := &recSink{}
	s.Apply([]*fanout.Consumer{
		{ID: "on", Sink: onGroup, Shards: []uint32{group}},
		{ID: "off", Sink: offGroup, Shards: []uint32{group ^ 0x1}}, // sibling band group
	})

	raw, bf := beefFrameFor(t, topic, beefV1Obj)
	if err := s.SendBeef(raw, bf); err != nil {
		t.Fatalf("SendBeef: %v", err)
	}
	if onGroup.beef != 1 || offGroup.beef != 0 {
		t.Fatalf("shard routing: on=%d off=%d, want 1/0", onGroup.beef, offGroup.beef)
	}
}

// TestSendBeef_OwnTrafficExclusion is the BEEF own-traffic regression: the
// expected HashKey uses the banded group index and a ZERO 32-byte ingredient
// (TopicID excluded), so a consumer's own submission is excluded and counted
// via its ingress observer.
func TestSendBeef_OwnTrafficExclusion(t *testing.T) {
	s, pe := beefSinkFixture(t)
	topic := "tm_own"
	tid := objfmt.TopicID(topic)
	group := pe.GroupIndex(&tid)

	var ownIP [16]byte
	ownIP[0], ownIP[15] = 0xfd, 0x77

	raw, bf := beefFrameFor(t, topic, beefV1Obj)
	var zero [32]byte
	bf.HashKey = seqhash.Hash(ownIP, group, zero) // as the proxy stamps it

	own := &recSink{}
	obs := &countObs{}
	otherC := &recSink{}
	s.Apply([]*fanout.Consumer{
		{ID: "own", Sink: own, OwnIngressIP: ownIP, IngressObs: obs},
		{ID: "other", Sink: otherC},
	})

	if err := s.SendBeef(raw, bf); err != nil {
		t.Fatalf("SendBeef: %v", err)
	}
	if own.beef != 0 {
		t.Errorf("own consumer received its own submission back (%d)", own.beef)
	}
	if obs.calls != 1 {
		t.Errorf("ingress observer saw %d, want 1", obs.calls)
	}
	if otherC.beef != 1 {
		t.Errorf("other consumer got %d, want 1", otherC.beef)
	}
}
