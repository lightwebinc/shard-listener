package fanout_test

import (
	"errors"
	"testing"

	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/shard-listener/fanout"
)

// TestSendBeef_NoEngineIsAnError is the B3 regression: without a plane engine
// the group index (and therefore the own-traffic HashKey) cannot be computed,
// so SendBeef must FAIL rather than fall back to the full consumer table —
// a fallback bypasses group election and silently disables own-traffic
// exclusion and ingress metering.
func TestSendBeef_NoEngineIsAnError(t *testing.T) {
	s := fanout.New(shard.New(0xFF05, shard.DefaultGroupID, 8)) // no SetBEEFEngine
	sink := &recSink{}
	s.Apply([]*fanout.Consumer{{ID: "c", Sink: sink}})

	raw, bf := beefFrameFor(t, "tm_no_engine", beefV1Obj)
	err := s.SendBeef(raw, bf)
	if !errors.Is(err, fanout.ErrBEEFEngineUnset) {
		t.Fatalf("err = %v, want ErrBEEFEngineUnset", err)
	}
	if sink.beef != 0 {
		t.Fatalf("delivered %d frames without an engine, want 0", sink.beef)
	}
	_ = objfmt.BEEFMarkerV1
}
