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

// A genuine emitter change, a new source continuing the sequence (relay or
// proxy failover), makes the new source the one whose restart counts; the old
// emitter's late low frame is then history, not a restart.
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
