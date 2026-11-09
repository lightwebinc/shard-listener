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

// TestPendingTotalCapAcrossFlows: the pending table is bounded across all
// flows too, so many flows losing at once cannot exhaust memory. The count
// follows fills and a proxy restart, and freed room is used again.
func TestPendingTotalCapAcrossFlows(t *testing.T) {
	rec := newRecorder(t)
	tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second, MaxForwardJump: 100, MaxPendingTotal: 250}, nil, nil, rec, nil)
	jump := func(hk, to uint64) {
		tr.Observe(0, [32]byte{}, hk, 1, [32]byte{}, emitterA)
		tr.Observe(0, [32]byte{}, hk, to, [32]byte{}, emitterA)
	}
	for hk := uint64(1); hk <= 3; hk++ { // three flows, 100 missing each: 300 > 250
		jump(hk, 102)
	}
	if g := tr.PendingGaps(); g != 250 {
		t.Fatalf("PendingGaps = %d, want the cap 250", g)
	}
	out := promText(t, rec)
	if v := counter(out, "bsl_gaps_detected_total"); v != 300 {
		t.Errorf("gaps detected = %v, want 300", v)
	}
	if v := counter(out, "bsl_gaps_abandoned_total", `reason="capacity"`); v != 50 {
		t.Errorf("gaps abandoned for capacity = %v, want 50", v)
	}

	for s := uint64(2); s < 12; s++ { // ten repairs on flow 1
		tr.Fill(1, s)
	}
	if g := tr.PendingGaps(); g != 240 {
		t.Fatalf("PendingGaps after 10 fills = %d, want 240", g)
	}
	tr.Observe(0, [32]byte{}, 2, 1, [32]byte{}, emitterA) // flow 2's proxy restarted
	if g := tr.PendingGaps(); g != 140 {
		t.Fatalf("PendingGaps after a restart = %d, want 140", g)
	}
	tr.Observe(0, [32]byte{}, 3, 153, [32]byte{}, emitterA) // flow 3 loses 50 more
	if g := tr.PendingGaps(); g != 190 {
		t.Fatalf("PendingGaps after freed room is used = %d, want 190", g)
	}
}
