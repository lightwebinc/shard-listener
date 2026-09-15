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
	otherObs, wrongObs, electsObs := &filterRec{}, &filterRec{}, &filterRec{}

	s.Apply([]*fanout.Consumer{
		{ID: "elects", Sink: elects, TopicSet: map[[32]byte]struct{}{tid: {}}, BEEFObs: electsObs},
		{ID: "other", Sink: other, TopicSet: map[[32]byte]struct{}{objfmt.TopicID("tm_other"): {}}, BEEFObs: otherObs},
		{ID: "agg", Sink: agg, AllTopics: true}, // explicit aggregator; no version filter
		{ID: "wrongver", Sink: wrongVer, AllTopics: true, BEEFVersions: map[uint32]struct{}{objfmt.BEEFMarkerV2: {}}, BEEFObs: wrongObs},
	})

	raw, bf := beefFrameFor(t, topic, beefV1Obj)
	if err := s.SendBeef(raw, bf); err != nil {
		t.Fatalf("SendBeef: %v", err)
	}

	// The observer sees exactly what each consumer's OWN election excluded,
	// with the reason and the wire size — and nothing for a delivered frame.
	if otherObs.n != 1 || otherObs.reason != fanout.FilterTopic || otherObs.wire != len(raw) {
		t.Errorf("topic-filtered observer: %+v, want 1×%s of %d bytes", *otherObs, fanout.FilterTopic, len(raw))
	}
	if wrongObs.n != 1 || wrongObs.reason != fanout.FilterVersion {
		t.Errorf("version-filtered observer: %+v, want 1×%s", *wrongObs, fanout.FilterVersion)
	}
	if electsObs.n != 0 {
		t.Errorf("electing consumer's observer fired %d times for a delivered frame", electsObs.n)
	}

	if elects.beef != 1 {
		t.Errorf("electing consumer got %d, want 1", elects.beef)
	}
	if other.beef != 0 {
		t.Errorf("non-electing consumer got %d, want 0 (topic filter)", other.beef)
	}
	if agg.beef != 1 {
		t.Errorf("explicit aggregator got %d, want 1 (AllTopics admits every topic)", agg.beef)
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
		{ID: "on", Sink: onGroup, Shards: []uint32{group}, AllTopics: true},
		{ID: "off", Sink: offGroup, Shards: []uint32{group ^ 0x1}, AllTopics: true}, // sibling band group
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
		{ID: "own", Sink: own, OwnIngressIP: ownIP, IngressObs: obs, AllTopics: true},
		{ID: "other", Sink: otherC, AllTopics: true},
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

// filterRec records the last BEEF filter observation for a consumer.
type filterRec struct {
	n      int
	reason string
	wire   int
}

func (f *filterRec) ObserveBEEFFiltered(reason string, wire int) {
	f.n++
	f.reason, f.wire = reason, wire
}

// TestSendBeef_EmptyElectionDeliversNothing pins the 2026-09-15 ruling. An
// empty topic election used to mean "every topic on the plane", which made a
// billable trap out of the provisioning flow: a tunnel request elects LANES
// and topics are joined afterwards, so between the two every consumer was an
// all-plane aggregator and was billed for the whole plane without having asked
// for anything. "No subscription yet" and "send me everything" must not be the
// same wire state, and the safe reading of silence is to send nothing.
func TestSendBeef_EmptyElectionDeliversNothing(t *testing.T) {
	s, _ := beefSinkFixture(t)
	unsubscribed, unsubObs := &recSink{}, &filterRec{}
	aggregator := &recSink{}

	s.Apply([]*fanout.Consumer{
		// Provisioned with a BEEF lane, no topics joined yet: the exact shape
		// the provisioning flow produces before the first subscription.
		{ID: "unsubscribed", Sink: unsubscribed, BEEFObs: unsubObs},
		// The same shape, but having explicitly asked for the whole plane.
		{ID: "aggregator", Sink: aggregator, AllTopics: true},
	})

	raw, bf := beefFrameFor(t, "tm_anything", beefV1Obj)
	if err := s.SendBeef(raw, bf); err != nil {
		t.Fatalf("SendBeef: %v", err)
	}

	if unsubscribed.beef != 0 {
		t.Errorf("a consumer that elected NO topic received %d objects: it would be billed for a plane it never subscribed to", unsubscribed.beef)
	}
	if unsubObs.n != 1 || unsubObs.reason != fanout.FilterTopic {
		t.Errorf("unsubscribed consumer's observer = %+v, want 1×%s so the non-delivery reads as FILTERED, not as loss", *unsubObs, fanout.FilterTopic)
	}
	if aggregator.beef != 1 {
		t.Errorf("explicit aggregator got %d, want 1: asking for the whole plane must still work", aggregator.beef)
	}
}
