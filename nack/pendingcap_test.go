package nack_test

import (
	"testing"
	"time"

	"github.com/lightwebinc/shard-listener/nack"
)

// TestPendingCapBooksTheRest: registering gaps is bounded per flow, so a
// sender that jumps again and again cannot grow the table without limit.
// SeqNums past the bound are booked detected and unrecovered (reason
// "capacity"), so the books still show the loss.
func TestPendingCapBooksTheRest(t *testing.T) {
	rec := newRecorder(t)
	tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second, MaxForwardJump: 100, MaxPendingPerFlow: 250}, nil, nil, rec, nil)
	var seq uint64 = 1
	observeRun(tr, emitterA, seq, seq, 0)
	for range 4 { // four jumps of 100 missing: 400 > 250
		seq += 101
		observeRun(tr, emitterA, seq, seq, 0)
	}
	if g := tr.PendingGaps(); g != 250 {
		t.Fatalf("PendingGaps = %d, want the cap 250", g)
	}
	out := promText(t, rec)
	if v := counter(out, "bsl_gaps_detected_total"); v != 400 {
		t.Errorf("gaps detected = %v, want 400 (registered and refused)", v)
	}
	if v := counter(out, "bsl_gaps_abandoned_total", `reason="capacity"`); v != 150 {
		t.Errorf("gaps abandoned for capacity = %v, want 150", v)
	}
	if v := counter(out, "bsl_gaps_unrecovered_total"); v != 150 {
		t.Errorf("gaps unrecovered = %v, want 150", v)
	}
}
