package nack_test

import (
	"context"
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/shard-listener/metrics"
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

// A gap's metrics carry the source it was detected under. After an emitter
// change (a failover source that keeps advancing the flow, past the 8 frames
// in a row that confirm it), a delayed fill of a gap the old emitter's stream
// left is booked under the old emitter, as its detection was, not under the
// flow's new one.
func TestFillAfterEmitterChangeKeepsGapSource(t *testing.T) {
	rec := newRecorder(t)
	tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second}, nil, nil, rec, nil)
	failover := net.ParseIP("2001:db8::11")
	emitTo(tr, flowEmitter, 140, 130) // 130 lost under the emitter
	for s := uint64(141); s <= 150; s++ {
		tr.Observe(0, [32]byte{}, flowA, s, [32]byte{}, failover) // the failover takes over
	}
	tr.Observe(0, [32]byte{}, flowA, 130, [32]byte{}, retryNode) // the delayed fill of 130

	out := promText(t, rec)
	if v := counter(out, "bsl_gaps_detected_total", `source="2001:db8::10"`); v != 1 {
		t.Fatalf("setup: gaps detected under the emitter = %v, want 1", v)
	}
	if v := counter(out, "bsl_gaps_suppressed_total", `source="2001:db8::10"`); v != 1 {
		t.Errorf("fill of 130 under the emitter = %v, want 1 (booked where it was detected)", v)
	}
	if v := counter(out, "bsl_gaps_suppressed_total", `source="2001:db8::11"`); v != 0 {
		t.Errorf("fill of 130 under the failover = %v, want 0", v)
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

// A retired numbering's frame can arrive before the new numbering reaches its
// SeqNum. It is a retransmit there too: it must not advance the new numbering
// or register the frames still to come as gaps. Otherwise the new numbering's
// own frame at that SeqNum arrives as a duplicate and goes unrecorded, and a
// later retransmit of it, from the emitter's address, finds nothing to compare
// with and restarts the flow, abandoning a real gap.
func TestRetiredFrameAboveBaselineDoesNotAdvance(t *testing.T) {
	rec := newRecorder(t)
	tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second}, nil, nil, rec, nil)
	failover := net.ParseIP("2001:db8::11")
	emitIDs(tr, flowEmitter, 150)
	tr.Observe(0, [32]byte{}, flowA, 1, frameID(2, 1), failover) // a fresh counter: restart

	tr.Observe(0, [32]byte{}, flowA, 50, frameID(1, 50), retryNode) // the old 50, before the new 2
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("after the retired numbering's 50: PendingGaps = %d, want 0 (it advanced the new numbering)", g)
	}

	for s := uint64(2); s <= 150; s++ {
		if s != 140 {
			tr.Observe(0, [32]byte{}, flowA, s, frameID(2, s), failover)
		}
	}
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("setup: PendingGaps = %d, want 1 (140)", g)
	}

	tr.Observe(0, [32]byte{}, flowA, 50, frameID(2, 50), failover) // a retransmit of the new 50

	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("after a retransmit of the new 50: PendingGaps = %d, want 1 (140 must survive)", g)
	}
	tr.Observe(0, [32]byte{}, flowA, 151, frameID(2, 151), failover)
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("after the next wire frame: PendingGaps = %d, want 1 (no phantom gaps)", g)
	}
	if v := counter(promText(t, rec), "bsl_gaps_abandoned_total", `reason="restart"`); v != 0 {
		t.Errorf("restart abandons = %v, want 0", v)
	}
}

// After a restart at B/1, the new numbering's 2 is missing when the retired
// numbering's 2 arrives. The retired frame must not fill that gap: filling it
// would leave the new 2 to arrive as a duplicate, unrecorded, and a later
// retransmit of the new 2 from the emitter's address, past the threshold,
// would then find nothing to compare with and restart the flow, abandoning a
// real gap. The new 2 fills its own gap and is recorded.
func TestRetiredFrameDoesNotFillCurrentGap(t *testing.T) {
	rec := newRecorder(t)
	tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second}, nil, nil, rec, nil)
	failover := net.ParseIP("2001:db8::11")
	emitIDs(tr, flowEmitter, 150)
	tr.Observe(0, [32]byte{}, flowA, 1, frameID(2, 1), failover) // a fresh counter: restart
	tr.Observe(0, [32]byte{}, flowA, 3, frameID(2, 3), failover) // the new 2 is missing
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("setup: PendingGaps = %d, want 1 (2)", g)
	}

	tr.Observe(0, [32]byte{}, flowA, 2, frameID(1, 2), retryNode) // the retired numbering's 2
	if g := tr.PendingGaps(); g != 1 {
		t.Errorf("after the retired numbering's 2: PendingGaps = %d, want 1 (it filled the new numbering's gap)", g)
	}
	tr.Observe(0, [32]byte{}, flowA, 2, frameID(2, 2), retryNode) // the new 2, repaired
	for s := uint64(4); s <= 150; s++ {
		if s != 140 {
			tr.Observe(0, [32]byte{}, flowA, s, frameID(2, s), failover)
		}
	}
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("setup: PendingGaps = %d, want 1 (140)", g)
	}

	tr.Observe(0, [32]byte{}, flowA, 2, frameID(2, 2), failover) // a retransmit of the new 2

	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("after a retransmit of the new 2: PendingGaps = %d, want 1 (140 must survive)", g)
	}
	tr.Observe(0, [32]byte{}, flowA, 151, frameID(2, 151), failover)
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("after the next wire frame: PendingGaps = %d, want 1 (no phantom gaps)", g)
	}
	if v := counter(promText(t, rec), "bsl_gaps_abandoned_total", `reason="restart"`); v != 0 {
		t.Errorf("restart abandons = %v, want 0", v)
	}
}

// The same refusal on the unicast return channel: a cache that still holds
// the retired numbering's frame at the gap's SeqNum returns it. The worker
// re-injects (delivers) it, but it is not the gap's repair, so the NACK
// exchange must not close the gap; it moves on as on a MISS.
func TestRetiredUnicastReturnDoesNotCloseGap(t *testing.T) {
	mockConn, err := net.ListenPacket("udp", "[::1]:0")
	if err != nil {
		t.Skipf("UDP loopback unavailable: %v", err)
	}
	defer func() { _ = mockConn.Close() }()
	go func() {
		buf := make([]byte, 256)
		for {
			_, src, err := mockConn.ReadFrom(buf)
			if err != nil {
				return
			}
			_, _ = mockConn.WriteTo(dataFrame(), src)
			var resp [nack.ResponseSize]byte
			nack.EncodeResponse(&nack.Response{MsgType: nack.MsgTypeACK, Flags: 0x02, SeqNum: 2}, resp[:])
			_, _ = mockConn.WriteTo(resp[:], src)
		}
	}()

	cfg := nack.TrackerConfig{JitterMax: 0, BackoffBase: time.Second, BackoffMax: 2 * time.Second, MaxRetries: 50, GapTTL: 10 * time.Second}
	tr := nack.New(cfg, []string{mockConn.LocalAddr().String()}, nil, nil, nil)
	failover := net.ParseIP("2001:db8::11")
	emitIDs(tr, flowEmitter, 150)
	tr.Observe(0, [32]byte{}, flowA, 1, frameID(2, 1), failover) // a fresh counter: restart
	tr.Observe(0, [32]byte{}, flowA, 3, frameID(2, 3), failover) // the new 2 is missing

	returned := make(chan struct{}, 64)
	tr.SetRecoverFunc(func([]byte) bool {
		// What the worker does with the return: re-inject it, here the
		// retired numbering's 2, through the pipeline (no datagram source).
		tr.Observe(0, [32]byte{}, flowA, 2, frameID(1, 2), nil)
		returned <- struct{}{}
		return true
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	select {
	case <-returned:
	case <-time.After(3 * time.Second):
		t.Fatal("the NACK for 2 was never answered")
	}
	time.Sleep(200 * time.Millisecond) // the exchange settles after the re-injection
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("after the retired numbering's 2 came back unicast: PendingGaps = %d, want 1 (the wrong frame closed the gap)", g)
	}
}

// After a restart, this numbering's 3 is given up on (here for capacity) while
// the flow stays active. The retired numbering's 3 arriving then is not this
// numbering's late fill: it must leave the abandonment record alone, and book
// nothing. This numbering's own 3, arriving late, still books the late fill.
func TestRetiredFrameIsNotCurrentLateFill(t *testing.T) {
	rec := newRecorder(t)
	tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second, MaxPendingPerFlow: 1}, nil, nil, rec, nil)
	failover := net.ParseIP("2001:db8::11")
	emitIDs(tr, flowEmitter, 150)
	tr.Observe(0, [32]byte{}, flowA, 1, frameID(2, 1), failover) // a fresh counter: restart
	tr.Observe(0, [32]byte{}, flowA, 4, frameID(2, 4), failover) // 2 pending, 3 abandoned for capacity
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("setup: PendingGaps = %d, want 1 (2)", g)
	}

	tr.Observe(0, [32]byte{}, flowA, 3, frameID(1, 3), retryNode) // the retired numbering's 3
	if v := counter(promText(t, rec), "bsl_gaps_late_filled_total"); v != 0 {
		t.Errorf("after the retired numbering's 3: late fills = %v, want 0 (this numbering's 3 is still missing)", v)
	}

	tr.Observe(0, [32]byte{}, flowA, 3, frameID(2, 3), retryNode) // this numbering's 3, late
	if v := counter(promText(t, rec), "bsl_gaps_late_filled_total"); v != 1 {
		t.Errorf("after this numbering's 3: late fills = %v, want 1", v)
	}
	if g := tr.PendingGaps(); g != 1 {
		t.Errorf("PendingGaps = %d, want 1 (2)", g)
	}
}

// A tail probe after a restart can be answered with the retired numbering's
// frame: the cache still holds it at that SeqNum. Here the cache ACKs first,
// then returns the frame. The ACK says nothing about this numbering, so the
// probe must not be confirmed, and the retired frame answers it MISS: nothing
// is booked detected or unrecovered for a SeqNum this numbering never sent.
func TestRetiredReturnAnswersProbeMiss(t *testing.T) {
	mockConn, err := net.ListenPacket("udp", "[::1]:0")
	if err != nil {
		t.Skipf("UDP loopback unavailable: %v", err)
	}
	defer func() { _ = mockConn.Close() }()
	asked := make(chan struct{}, 64)
	go func() {
		buf := make([]byte, 256)
		for {
			_, src, err := mockConn.ReadFrom(buf)
			if err != nil {
				return
			}
			var resp [nack.ResponseSize]byte // the ACK first, then the frame
			nack.EncodeResponse(&nack.Response{MsgType: nack.MsgTypeACK, Flags: 0x02, SeqNum: 2}, resp[:])
			_, _ = mockConn.WriteTo(resp[:], src)
			_, _ = mockConn.WriteTo(dataFrame(), src)
			select {
			case asked <- struct{}{}:
			default:
			}
		}
	}()

	rec := newRecorder(t)
	cfg := nack.TrackerConfig{
		JitterMax: time.Millisecond, BackoffBase: 20 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
		MaxRetries: 2, GapTTL: 2 * time.Second,
		TailProbe: true, TailProbeMinIdle: 50 * time.Millisecond, TailProbeMaxMisses: 1,
	}
	tr := nack.New(cfg, []string{mockConn.LocalAddr().String()}, nil, rec, nil)
	tr.SetRecoverFunc(func([]byte) bool {
		// What the worker does with the return: re-inject it, here the
		// retired numbering's 2, through the pipeline (no datagram source).
		tr.Observe(0, [32]byte{}, flowA, 2, frameID(1, 2), nil)
		return true
	})
	failover := net.ParseIP("2001:db8::11")
	for s := uint64(1); s <= 150; s++ { // a settled rate, for the probe
		tr.Observe(0, [32]byte{}, flowA, s, frameID(1, s), flowEmitter)
		time.Sleep(100 * time.Microsecond)
	}
	tr.Observe(0, [32]byte{}, flowA, 1, frameID(2, 1), failover) // restart, then quiet
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	select {
	case <-asked:
	case <-time.After(3 * time.Second):
		t.Fatal("no tail probe for 2 was sent")
	}
	time.Sleep(time.Second) // the probe's retries and its eviction, if it was confirmed

	out := promText(t, rec)
	if v := counter(out, "bsl_gaps_detected_total"); v != 0 {
		t.Errorf("gaps detected = %v, want 0 (the retired numbering's ACK confirmed the probe)", v)
	}
	if v := counter(out, "bsl_gaps_unrecovered_total"); v != 0 {
		t.Errorf("gaps unrecovered = %v, want 0 (a SeqNum this numbering never sent)", v)
	}
	if v := counter(out, "bsl_tail_probes_miss_total"); v < 1 {
		t.Errorf("tail probe misses = %v, want at least 1 (the retired frame answers MISS)", v)
	}
}

// probeCache is a retry endpoint for tail-probe tests. It records the SeqNum
// of every NACK and, once released, replies with resp; withData sends a data
// frame after it, as a unicast return. With only set, it answers NACKs for
// that SeqNum alone.
type probeCache struct {
	conn    net.PacketConn
	asked   chan uint64
	answer  chan struct{}
	release func()
	last    atomicU64
	only    uint64
}

// atomicU64 is the last NACKed SeqNum, read by the recover callback.
type atomicU64 struct {
	mu sync.Mutex
	v  uint64
}

func (a *atomicU64) set(v uint64) { a.mu.Lock(); a.v = v; a.mu.Unlock() }
func (a *atomicU64) get() uint64  { a.mu.Lock(); defer a.mu.Unlock(); return a.v }

func newProbeCache(t *testing.T, resp byte, withData bool) *probeCache {
	t.Helper()
	conn, err := net.ListenPacket("udp", "[::1]:0")
	if err != nil {
		t.Skipf("UDP loopback unavailable: %v", err)
	}
	c := &probeCache{conn: conn, asked: make(chan uint64, 256), answer: make(chan struct{})}
	var once sync.Once
	c.release = func() { once.Do(func() { close(c.answer) }) }
	t.Cleanup(func() {
		c.release()
		_ = conn.Close()
	})
	go func() {
		buf := make([]byte, 256)
		for {
			n, src, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			if n < 24 {
				continue
			}
			seq := binary.BigEndian.Uint64(buf[16:24])
			c.last.set(seq)
			select {
			case c.asked <- seq:
			default:
			}
			<-c.answer
			if c.only != 0 && seq != c.only {
				continue
			}
			var r [nack.ResponseSize]byte
			nack.EncodeResponse(&nack.Response{MsgType: resp, Flags: 0x02, SeqNum: seq}, r[:])
			_, _ = conn.WriteTo(r[:], src)
			if withData {
				_, _ = conn.WriteTo(dataFrame(), src)
			}
		}
	}()
	return c
}

func (c *probeCache) addr() string { return c.conn.LocalAddr().String() }

// waitAsked waits for a NACK for seq.
func (c *probeCache) waitAsked(t *testing.T, seq uint64) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case s := <-c.asked:
			if s == seq {
				return
			}
		case <-deadline:
			t.Fatalf("no NACK for %d", seq)
		}
	}
}

// probeTracker is a tracker that probes quiet tails, for a settled flow.
func probeTracker(t *testing.T, cache *probeCache, rec *metrics.Recorder, tailTTL time.Duration) *nack.Tracker {
	t.Helper()
	cfg := nack.TrackerConfig{
		JitterMax: time.Millisecond, BackoffBase: 20 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
		MaxRetries: 50, GapTTL: 10 * time.Second, TailTTL: tailTTL,
		TailProbe: true, TailProbeMinIdle: 50 * time.Millisecond,
	}
	return nack.New(cfg, []string{cache.addr()}, nil, rec, nil)
}

// settleIDs drives flowA from 1 to 150 from the emitter, with a settled rate.
func settleIDs(tr *nack.Tracker) {
	for s := uint64(1); s <= 150; s++ {
		tr.Observe(0, [32]byte{}, flowA, s, frameID(1, s), flowEmitter)
		time.Sleep(100 * time.Microsecond)
	}
}

// A later frame proves a tail probe's loss. From then on it is an ordinary
// gap: booked detected once, and a frame of the retired numbering at that
// SeqNum leaves it pending, as it does any gap of this numbering. (A probe of
// a flow that stays quiet still reads a retired frame as a MISS.)
func TestProvedProbeKeepsGapAgainstRetiredFrame(t *testing.T) {
	rec := newRecorder(t)
	cache := newProbeCache(t, nack.MsgTypeACK, false) // never answers
	tr := probeTracker(t, cache, rec, 0)
	settleIDs(tr)
	failover := net.ParseIP("2001:db8::11")
	tr.Observe(0, [32]byte{}, flowA, 1, frameID(2, 1), failover) // restart, then quiet
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)
	cache.waitAsked(t, 2) // the tail probe for 2

	tr.Observe(0, [32]byte{}, flowA, 3, frameID(2, 3), failover)  // 3 proves 2 lost
	tr.Observe(0, [32]byte{}, flowA, 2, frameID(1, 2), retryNode) // the retired numbering's 2

	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("after the retired numbering's 2: PendingGaps = %d, want 1 (the proved gap was dropped as a MISS)", g)
	}
	if v := counter(promText(t, rec), "bsl_gaps_detected_total"); v != 1 {
		t.Errorf("gaps detected = %v, want 1 (booked once, when 3 proved it)", v)
	}
}

// The same with a cache that answers MISS, after a later frame (or an
// incomplete object) proved the probe's loss while the exchange ran: the MISS
// is an ordinary one, so the gap escalates and stays pending instead of being
// retired as an idle flow's.
func TestProvedProbeMissEscalates(t *testing.T) {
	for _, tc := range []struct {
		name  string
		prove func(tr *nack.Tracker)
	}{
		{"later frame", func(tr *nack.Tracker) { tr.Observe(0, [32]byte{}, flowA, 152, frameID(1, 152), flowEmitter) }},
		{"incomplete object", func(tr *nack.Tracker) { tr.RequestGaps(flowA, 0, [32]byte{}, []uint64{151}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := newRecorder(t)
			cache := newProbeCache(t, nack.MsgTypeMISS, false)
			tr := probeTracker(t, cache, rec, 0)
			settleIDs(tr)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tr.Start(ctx)
			cache.waitAsked(t, 151) // the tail probe for 151

			tc.prove(tr)
			cache.release() // MISS, now that 151 is proved lost
			time.Sleep(200 * time.Millisecond)

			if g := tr.PendingGaps(); g != 1 {
				t.Fatalf("PendingGaps = %d, want 1 (the MISS retired a proved gap)", g)
			}
			out := promText(t, rec)
			if v := counter(out, "bsl_gaps_detected_total"); v != 1 {
				t.Errorf("gaps detected = %v, want 1", v)
			}
			if v := counter(out, "bsl_tail_probes_miss_total"); v != 0 {
				t.Errorf("tail probe misses = %v, want 0", v)
			}
		})
	}
}

// A probe proved lost by another source's frame is booked detected under the
// source it was opened for, as its fill is, not under the frame that proved
// it: the per-source detected and suppressed counters stay in step.
func TestProvedProbeDetectedUnderItsSource(t *testing.T) {
	rec := newRecorder(t)
	cache := newProbeCache(t, nack.MsgTypeACK, false) // holds the probe unanswered
	tr := probeTracker(t, cache, rec, 0)
	settleIDs(tr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)
	cache.waitAsked(t, 151) // the tail probe for 151, opened for the emitter

	other := net.ParseIP("2001:db8::11")
	tr.Observe(0, [32]byte{}, flowA, 152, frameID(1, 152), other)     // 152 proves 151 lost
	tr.Observe(0, [32]byte{}, flowA, 151, frameID(1, 151), retryNode) // and its repair fills it

	out := promText(t, rec)
	if v := counter(out, "bsl_gaps_detected_total", `source="2001:db8::10"`); v != 1 {
		t.Errorf("151 detected under the emitter = %v, want 1", v)
	}
	if v := counter(out, "bsl_gaps_detected_total", `source="2001:db8::11"`); v != 0 {
		t.Errorf("151 detected under the source of 152 = %v, want 0", v)
	}
	if v := counter(out, "bsl_gaps_suppressed_total", `source="2001:db8::10"`); v != 1 {
		t.Errorf("151 filled under the emitter = %v, want 1", v)
	}
}

// A proved probe that a cache answers with a unicast-only ACK, on a listener
// that cannot re-inject, is a real loss with no way back: it is booked
// unrecovered, not retired silently as an unanswered probe would be.
func TestProvedProbeNoRecoverIsBookedUnrecovered(t *testing.T) {
	rec := newRecorder(t)
	cache := newProbeCache(t, nack.MsgTypeACK, false) // ACK flags: unicast only
	cache.only = 151                                  // the next probe goes unanswered
	tr := probeTracker(t, cache, rec, 0)
	settleIDs(tr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)
	cache.waitAsked(t, 151)

	tr.Observe(0, [32]byte{}, flowA, 152, frameID(1, 152), flowEmitter) // 152 proves 151 lost
	cache.release()
	time.Sleep(200 * time.Millisecond)

	out := promText(t, rec)
	if v := counter(out, "bsl_gaps_detected_total"); v != 1 {
		t.Errorf("gaps detected = %v, want 1", v)
	}
	if v := counter(out, "bsl_gaps_abandoned_total", `reason="no_recover"`); v != 1 {
		t.Errorf("gaps abandoned no_recover = %v, want 1 (a proved loss with no re-injection path)", v)
	}
}

// A retired frame observed by another worker (a cache's address hashes to a
// different receive socket) must not move the flow to that worker. Here the
// flow then goes quiet: its tail probe is the next gap, and the probe's
// unicast return is re-injected into the worker that holds the flow.
func TestRetiredFrameKeepsFlowWorker(t *testing.T) {
	cache := newProbeCache(t, nack.MsgTypeACK, true)
	cache.release()
	tr := probeTracker(t, cache, nil, 0)
	got := make(chan int, 8)
	for id := range 2 {
		tr.RegisterRecover(id, func([]byte) bool { got <- id; return true })
	}
	failover := net.ParseIP("2001:db8::11")
	for s := uint64(1); s <= 150; s++ {
		tr.ObserveFrom(0, 0, [32]byte{}, flowA, s, frameID(1, s), flowEmitter)
		time.Sleep(100 * time.Microsecond)
	}
	tr.ObserveFrom(0, 0, [32]byte{}, flowA, 1, frameID(2, 1), failover)  // restart
	tr.ObserveFrom(1, 0, [32]byte{}, flowA, 5, frameID(1, 5), retryNode) // retired 5, at worker 1
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	select {
	case id := <-got:
		if id != 0 {
			t.Fatalf("the probe's return was re-injected into worker %d, want 0 (the retired frame moved the flow)", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no probe return was re-injected")
	}
}

// retiredRecover re-injects, as the worker would, the retired numbering's
// frame at the SeqNum the cache was last asked for.
func retiredRecover(tr *nack.Tracker, cache *probeCache) func([]byte) bool {
	return func([]byte) bool {
		s := cache.last.get()
		tr.Observe(0, [32]byte{}, flowA, s, frameID(1, s), nil)
		return true
	}
}

// A cache that keeps answering a tail probe with the retired numbering's
// frame must not keep the probe going: the frame is no sign of life, so the
// misses add up and probing stops at the limit (3). This numbering's own
// traffic still resets the count.
func TestRetiredResponsesStopProbing(t *testing.T) {
	cache := newProbeCache(t, nack.MsgTypeACK, true)
	cache.release()
	tr := probeTracker(t, cache, nil, 0)
	tr.SetRecoverFunc(retiredRecover(tr, cache))
	settleIDs(tr)
	failover := net.ParseIP("2001:db8::11")
	tr.Observe(0, [32]byte{}, flowA, 1, frameID(2, 1), failover) // restart, then quiet
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	time.Sleep(1200 * time.Millisecond)
	probes := 0
	for len(cache.asked) > 0 {
		if <-cache.asked == 2 {
			probes++
		}
	}
	if probes != 3 {
		t.Fatalf("tail probes for 2 = %d, want 3 (then stop)", probes)
	}

	tr.Observe(0, [32]byte{}, flowA, 2, frameID(2, 2), failover) // this numbering speaks again
	cache.waitAsked(t, 3)
}

// The same budget holds on a listener that cannot re-inject. A cache's
// unicast-only ACK for a tail probe at a SeqNum the retired numbering held is
// refused as confirmation (it may be that numbering's frame), so the probe
// ends as an unconfirmed one: retired without booking a loss, but counted
// against the miss budget, so probing stops at the limit (3) instead of
// repeating until the flow expires.
func TestRefusedProbeACKCountsAgainstMissBudget(t *testing.T) {
	rec := newRecorder(t)
	cache := newProbeCache(t, nack.MsgTypeACK, false) // ACK flags: unicast only, no frame
	cache.release()
	tr := probeTracker(t, cache, rec, 0) // no recover callback
	settleIDs(tr)
	failover := net.ParseIP("2001:db8::11")
	tr.Observe(0, [32]byte{}, flowA, 1, frameID(2, 1), failover) // restart, then quiet
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	time.Sleep(1200 * time.Millisecond)
	probes := 0
	for len(cache.asked) > 0 {
		if <-cache.asked == 2 {
			probes++
		}
	}
	if probes != 3 {
		t.Fatalf("tail probes for 2 = %d, want 3 (then stop)", probes)
	}
	out := promText(t, rec)
	if v := counter(out, "bsl_gaps_unrecovered_total"); v != 0 {
		t.Errorf("gaps unrecovered = %v, want 0 (a probe nothing confirmed is no loss)", v)
	}
	if v := counter(out, "bsl_gaps_abandoned_total", `reason="no_recover"`); v != 0 {
		t.Errorf("gaps abandoned no_recover = %v, want 0", v)
	}
}

// Nor may it keep an idle flow allocated: once probing stops, the flow ages
// out on its own idle clock.
func TestRetiredResponsesDoNotKeepFlowAlive(t *testing.T) {
	cache := newProbeCache(t, nack.MsgTypeACK, true)
	cache.release()
	tr := probeTracker(t, cache, nil, 300*time.Millisecond)
	tr.SetRecoverFunc(retiredRecover(tr, cache))
	settleIDs(tr)
	tr.Observe(0, [32]byte{}, flowA, 1, frameID(2, 1), net.ParseIP("2001:db8::11")) // restart, then quiet
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for tr.ActiveFlows() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the idle flow is still allocated after 3 s (ActiveFlows = %d)", tr.ActiveFlows())
		}
		time.Sleep(20 * time.Millisecond)
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
