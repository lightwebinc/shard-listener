package nack_test

import (
	"net"
	"testing"
	"time"

	"github.com/lightwebinc/shard-listener/nack"
)

var (
	flowEmitter = net.ParseIP("2001:db8::10")
	retryNode   = net.ParseIP("2001:db8::20")
)

// emitTo drives flowA from 1 to last off the wire from src, skipping gaps.
func emitTo(tr *nack.Tracker, src net.IP, last uint64, gaps ...uint64) {
	skip := map[uint64]bool{}
	for _, g := range gaps {
		skip[g] = true
	}
	for s := uint64(1); s <= last; s++ {
		if !skip[s] {
			tr.Observe(0, [32]byte{}, flowA, s, [32]byte{}, src)
		}
	}
}

// A multicast retransmit is a live frame (it has a datagram source) with a
// SeqNum from the flow's past, sent by a retry endpoint, often for another
// listener's gap. One below the restart threshold must not read as a proxy
// restart: the flow keeps its pending gaps and its baseline, and its source
// stays the emitter, so a later genuine restart is still attributed to it.
func TestRetransmitLowSeqDoesNotResetFlow(t *testing.T) {
	rec := newRecorder(t)
	tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second}, nil, nil, rec, nil)
	emitTo(tr, flowEmitter, 150, 130)
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("setup: PendingGaps = %d, want 1 (130)", g)
	}

	tr.Observe(0, [32]byte{}, flowA, 5, [32]byte{}, retryNode) // a retransmit of 5

	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("after a retransmit of 5: PendingGaps = %d, want 1 (130 must survive)", g)
	}
	if v := counter(promText(t, rec), "bsl_gaps_abandoned_total", `reason="restart"`); v != 0 {
		t.Fatalf("a retransmit was booked as a proxy restart (%v gaps abandoned)", v)
	}
	// The baseline is untouched: the emitter's next frame is contiguous.
	tr.Observe(0, [32]byte{}, flowA, 151, [32]byte{}, flowEmitter)
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("after the next wire frame: PendingGaps = %d, want 1 (no phantom gaps)", g)
	}

	// The emitter's genuine restart still resets the flow, and its gaps are
	// attributed to the emitter, not to the retry endpoint.
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, flowEmitter)
	if g := tr.PendingGaps(); g != 0 {
		t.Fatalf("after the emitter's restart: PendingGaps = %d, want 0 (flushed)", g)
	}
	out := promText(t, rec)
	if v := counter(out, "bsl_gaps_abandoned_total", `reason="restart"`, `source="2001:db8::10"`); v != 1 {
		t.Errorf("restart abandons attributed to the emitter = %v, want 1", v)
	}
	if v := counter(out, "bsl_gaps_abandoned_total", `source="2001:db8::20"`); v != 0 {
		t.Errorf("gaps attributed to the retry endpoint = %v, want 0", v)
	}
}

// A retransmit that fills a pending gap below the threshold fills only that
// gap and leaves the flow's source alone.
func TestRetransmitFillKeepsEmitter(t *testing.T) {
	rec := newRecorder(t)
	tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second}, nil, nil, rec, nil)
	emitTo(tr, flowEmitter, 150, 60, 130)

	tr.Observe(0, [32]byte{}, flowA, 60, [32]byte{}, retryNode) // repairs 60

	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("PendingGaps = %d, want 1 (130 must survive the repair of 60)", g)
	}
	if v := counter(promText(t, rec), "bsl_gaps_suppressed_total", `source="2001:db8::10"`); v != 1 {
		t.Errorf("repair of 60 attributed to the emitter = %v, want 1", v)
	}
}

// A genuine emitter change, a new source that keeps advancing the flow (relay
// or proxy failover; 9 frames here, past the 8 in a row that confirm it),
// makes the new source the one whose restart counts; the old emitter's late
// low frame is then history, not a restart. Frames without identities: the
// source rule decides.
func TestRestartFollowsCurrentEmitter(t *testing.T) {
	tr := newTestTracker()
	failover := net.ParseIP("2001:db8::11")
	emitTo(tr, flowEmitter, 140)
	for s := uint64(141); s <= 150; s++ {
		if s != 145 {
			tr.Observe(0, [32]byte{}, flowA, s, [32]byte{}, failover)
		}
	}
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("setup: PendingGaps = %d, want 1 (145)", g)
	}

	tr.Observe(0, [32]byte{}, flowA, 4, [32]byte{}, flowEmitter) // the old emitter, late
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("after the old emitter's late frame: PendingGaps = %d, want 1", g)
	}
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{}, failover) // the current emitter restarts
	if g := tr.PendingGaps(); g != 0 {
		t.Fatalf("after the current emitter's restart: PendingGaps = %d, want 0 (flushed)", g)
	}
}

// frameID is a distinct, non-zero frame identity (a TxID) per SeqNum and
// numbering epoch.
func frameID(epoch byte, seq uint64) [32]byte {
	var id [32]byte
	id[0], id[1], id[2], id[31] = epoch, byte(seq), byte(seq>>8), 0x5A
	return id
}

// emitIDs is emitTo with each frame's identity, epoch 1.
func emitIDs(tr *nack.Tracker, src net.IP, last uint64, gaps ...uint64) {
	skip := map[uint64]bool{}
	for _, g := range gaps {
		skip[g] = true
	}
	for s := uint64(1); s <= last; s++ {
		if !skip[s] {
			tr.Observe(0, [32]byte{}, flowA, s, frameID(1, s), src)
		}
	}
}

// A repair can advance a flow: a frame this listener lost at the tail,
// multicast for another listener's probe or NACK before this listener knew
// it was missing (no pending entry, filled == false). A short run of them must
// not take the emitter over, or the emitter's later restart is read as a
// retransmit and the flow is never reset. Frames without identities exercise
// the source rule, the only evidence for such frames.
func TestRepairAboveBaselineKeepsEmitter(t *testing.T) {
	rec := newRecorder(t)
	tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second}, nil, nil, rec, nil)
	emitTo(tr, flowEmitter, 150, 130)
	for s := uint64(151); s <= 153; s++ {
		tr.Observe(0, [32]byte{}, flowA, s, [32]byte{}, retryNode) // repairs above the baseline
	}
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("setup: PendingGaps = %d, want 1 (130)", g)
	}

	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, flowEmitter) // the emitter restarts

	if g := tr.PendingGaps(); g != 0 {
		t.Fatalf("after the emitter's restart: PendingGaps = %d, want 0 (the repairs took the emitter over)", g)
	}
	if v := counter(promText(t, rec), "bsl_gaps_abandoned_total", `reason="restart"`, `source="2001:db8::10"`); v != 1 {
		t.Errorf("restart abandons attributed to the emitter = %v, want 1", v)
	}
}

// A retransmit of an early frame can come from the emitter's own address: a
// retry endpoint on the emitter's node, or one behind a relay that re-emits
// every frame from its own address. It is the same frame again, so it is not
// a restart, whatever its source: the flow keeps its gaps and its baseline.
func TestRetransmitFromEmitterAddressKeepsFlow(t *testing.T) {
	rec := newRecorder(t)
	tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second}, nil, nil, rec, nil)
	emitIDs(tr, flowEmitter, 150, 130)

	tr.Observe(0, [32]byte{}, flowA, 5, frameID(1, 5), flowEmitter) // a retransmit of 5

	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("after a retransmit of 5 from the emitter's address: PendingGaps = %d, want 1 (130 must survive)", g)
	}
	if v := counter(promText(t, rec), "bsl_gaps_abandoned_total", `reason="restart"`); v != 0 {
		t.Fatalf("a retransmit was booked as a proxy restart (%v gaps abandoned)", v)
	}
	tr.Observe(0, [32]byte{}, flowA, 151, frameID(1, 151), flowEmitter)
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("after the next wire frame: PendingGaps = %d, want 1 (no phantom gaps)", g)
	}
}

// A restart is a new frame at an old number, whatever its source: here a
// submitter failed over to a proxy whose counter for the flow is fresh, so
// the new numbering arrives from a new address.
func TestNewFrameAtOldNumberRestartsFromAnySource(t *testing.T) {
	rec := newRecorder(t)
	tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second}, nil, nil, rec, nil)
	failover := net.ParseIP("2001:db8::11")
	emitIDs(tr, flowEmitter, 150, 130)

	tr.Observe(0, [32]byte{}, flowA, 1, frameID(2, 1), failover) // a new frame numbered 1

	if g := tr.PendingGaps(); g != 0 {
		t.Fatalf("after a new frame at SeqNum 1: PendingGaps = %d, want 0 (flushed)", g)
	}
	if v := counter(promText(t, rec), "bsl_gaps_abandoned_total", `reason="restart"`, `source="2001:db8::10"`); v != 1 {
		t.Errorf("restart abandons attributed to the old emitter = %v, want 1", v)
	}
	tr.Observe(0, [32]byte{}, flowA, 2, frameID(2, 2), failover)
	if g := tr.PendingGaps(); g != 0 {
		t.Fatalf("after the new numbering's next frame: PendingGaps = %d, want 0 (contiguous)", g)
	}
}

// After a restart, frames of the numbering it retired can still be
// retransmitted, for any listener's gap from before the restart. Such a frame
// differs from the new numbering's frame at its SeqNum, but it is the retired
// numbering's frame, so it is a retransmit, not a second restart. A frame new
// to both numberings still restarts the flow.
func TestRetiredNumberingRetransmitIsNotRestart(t *testing.T) {
	rec := newRecorder(t)
	tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second}, nil, nil, rec, nil)
	failover := net.ParseIP("2001:db8::11")
	emitIDs(tr, flowEmitter, 150)
	tr.Observe(0, [32]byte{}, flowA, 1, frameID(2, 1), failover) // a fresh counter: restart
	for s := uint64(2); s <= 150; s++ {
		if s != 140 {
			tr.Observe(0, [32]byte{}, flowA, s, frameID(2, s), failover)
		}
	}
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("setup: PendingGaps = %d, want 1 (140)", g)
	}

	tr.Observe(0, [32]byte{}, flowA, 1, frameID(1, 1), retryNode) // a delayed retransmit of the old 1

	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("after a retransmit of the retired numbering's 1: PendingGaps = %d, want 1 (140 must survive)", g)
	}
	tr.Observe(0, [32]byte{}, flowA, 151, frameID(2, 151), failover)
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("after the next wire frame: PendingGaps = %d, want 1 (no phantom gaps)", g)
	}
	if v := counter(promText(t, rec), "bsl_gaps_abandoned_total", `reason="restart"`); v != 0 {
		t.Errorf("restart abandons = %v, want 0 (the first restart had no gaps to abandon)", v)
	}

	tr.Observe(0, [32]byte{}, flowA, 1, frameID(3, 1), failover) // a frame new to both: restart
	if g := tr.PendingGaps(); g != 0 {
		t.Fatalf("after a new frame at 1: PendingGaps = %d, want 0 (flushed)", g)
	}
}

// A late fill is a replay, not an advance: one that lands beyond the baseline
// (here a gap the restart abandoned; a confirmed tail probe that was given up
// on is another) must not count toward a candidate emitter, or it can promote
// a source the emitter's next restart is then measured against.
func TestLateFillDoesNotConfirmEmitter(t *testing.T) {
	tr := newTestTracker()
	relay := net.ParseIP("2001:db8::30")
	emitTo(tr, flowEmitter, 150, 140)                            // 140 pending
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, flowEmitter) // restart: 140 abandoned, baseline 3
	for s := uint64(4); s <= 10; s++ {
		tr.Observe(0, [32]byte{}, flowA, s, [32]byte{}, relay) // 7 advances from another source
	}
	tr.Observe(0, [32]byte{}, flowA, 140, [32]byte{}, relay) // a late fill beyond the baseline
	if g := tr.PendingGaps(); g == 0 {
		t.Fatal("setup: the jump to 140 registered no gaps")
	}

	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{}, flowEmitter) // the emitter restarts

	if g := tr.PendingGaps(); g != 0 {
		t.Fatalf("after the emitter's restart: PendingGaps = %d, want 0 (the late fill promoted the other source)", g)
	}
}

// A late fill answers a gap this flow registered and gave up on: it is a
// retransmit, so it never restarts the flow, even from the emitter's address.
func TestLateFillIsNeverRestart(t *testing.T) {
	rec := newRecorder(t)
	tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second, MaxPendingPerFlow: 1}, nil, nil, rec, nil)
	emitTo(tr, flowEmitter, 150, 4, 5) // 4 pending, 5 abandoned for capacity
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("setup: PendingGaps = %d, want 1 (4)", g)
	}

	tr.Observe(0, [32]byte{}, flowA, 5, [32]byte{}, flowEmitter) // 5 turns up after all

	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("after the late fill of 5: PendingGaps = %d, want 1 (4 must survive)", g)
	}
	out := promText(t, rec)
	if v := counter(out, "bsl_gaps_late_filled_total"); v != 1 {
		t.Errorf("late fills = %v, want 1", v)
	}
	if v := counter(out, "bsl_gaps_abandoned_total", `reason="restart"`); v != 0 {
		t.Errorf("a late fill was booked as a proxy restart (%v gaps abandoned)", v)
	}
}
