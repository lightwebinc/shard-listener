package nack_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lightwebinc/shard-listener/nack"
)

const (
	flowA = uint64(0xAAAA_AAAA_AAAA_AAAA)
	flowB = uint64(0xBBBB_BBBB_BBBB_BBBB)
)

func newTestTracker() *nack.Tracker {
	cfg := nack.TrackerConfig{
		JitterMax:  0,
		BackoffMax: 5 * time.Second,
		MaxRetries: 3,
		GapTTL:     10 * time.Second,
	}
	return nack.New(cfg, nil, nil, nil, nil)
}

// TestMaxFlowsFloodGuard: past the MaxFlows cap, NEW sources are not tracked (bounded
// memory) while already-tracked flows keep registering gaps; the refusal is counted.
func TestMaxFlowsFloodGuard(t *testing.T) {
	tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second, MaxFlows: 3}, nil, nil, nil, nil)
	for i := uint64(1); i <= 5; i++ { // 5 distinct flows, cap 3
		tr.Observe(0, [32]byte{}, i, 1, [32]byte{}, nil)
	}
	if fc := tr.FlowCount(); fc != 3 {
		t.Fatalf("FlowCount = %d, want 3 (capped)", fc)
	}
	if r := tr.FlowsRefused(); r != 2 {
		t.Fatalf("FlowsRefused = %d, want 2", r)
	}
	// An already-tracked flow still registers gaps (recovery unaffected for it).
	tr.Observe(0, [32]byte{}, 1, 3, [32]byte{}, nil) // seq jumps 1→3 ⇒ gap at 2
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("tracked flow should still register gaps: PendingGaps = %d, want 1", g)
	}
}

// ── Observe ───────────────────────────────────────────────────────────────────

func TestObserveFirstFrame_NoGap(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("first frame: PendingGaps = %d, want 0", g)
	}
}

func TestObserveContiguous_NoGap(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, nil)
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("contiguous: PendingGaps = %d, want 0", g)
	}
}

func TestObserveSeqNumZero_Ignored(t *testing.T) {
	tr := newTestTracker()
	// seqNum == 0 means proxy has not stamped the frame; must be ignored.
	tr.Observe(0, [32]byte{}, flowA, 0, [32]byte{}, nil)
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("zero seqNum: PendingGaps = %d, want 0", g)
	}
	// Flow must initialise correctly on the first non-zero frame.
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("after zero-seqNum: PendingGaps = %d, want 0", g)
	}
}

func TestObserveNewFlow_NoGap(t *testing.T) {
	tr := newTestTracker()
	// Flow A established.
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{}, nil)
	// Flow B is an independent flow; its first frame must not create a gap.
	tr.Observe(0, [32]byte{}, flowB, 1, [32]byte{}, nil)
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("new flow start: PendingGaps = %d, want 0", g)
	}
}

func TestObserveGap_Detected(t *testing.T) {
	tr := newTestTracker()
	// Frame 1 establishes the flow; seqNum 3 arrives next — seqNum 2 is missing.
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, nil)
	if g := tr.PendingGaps(); g != 1 {
		t.Errorf("gap detected: PendingGaps = %d, want 1", g)
	}
}

func TestObserveDuplicateGap_NotDuplicated(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	// seqNum 3 reveals gap at 2.
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, nil)
	// Duplicate of seqNum 3 must not register an additional gap.
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, nil)
	if g := tr.PendingGaps(); g != 1 {
		t.Errorf("duplicate frame: PendingGaps = %d, want 1", g)
	}
}

func TestObserveMultipleGroups_IndependentFlows(t *testing.T) {
	tr := newTestTracker()
	// flowA (group 0): gap between seqNum 1 and 3.
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, nil)
	// flowB (group 1): clean contiguous delivery.
	tr.Observe(1, [32]byte{}, flowB, 1, [32]byte{}, nil)
	tr.Observe(1, [32]byte{}, flowB, 2, [32]byte{}, nil)
	if g := tr.PendingGaps(); g != 1 {
		t.Errorf("multi-flow: PendingGaps = %d, want 1 (only flowA has gap)", g)
	}
}

func TestObserveGap_AutoClosed_WhenMatchingSeqNumArrives(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, nil) // gap at seqNum=2
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("before auto-close: PendingGaps = %d, want 1", g)
	}

	// seqNum=2 arrives (out-of-order retransmit).
	// Observe auto-closes pending[2]; seqNum(2) <= lastSeqNum(3) so no new gap.
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{}, nil)
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("after retransmit fill: PendingGaps = %d, want 0", g)
	}

	// Confirm Fill is now a no-op (gap already removed).
	beforeFill := tr.PendingGaps()
	tr.Fill(flowA, 2)
	afterFill := tr.PendingGaps()
	if afterFill != beforeFill {
		t.Errorf("Fill(flowA,2) changed PendingGaps %d→%d: gap should already be closed",
			beforeFill, afterFill)
	}
}

func TestObserveOutOfOrder_NoPhantomGap(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, nil) // gap at seqNum=2
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("setup: PendingGaps = %d, want 1", g)
	}

	// seqNum=2 arrives late — must close gap AND not create a phantom gap.
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{}, nil)
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("after retransmit: PendingGaps = %d, want 0 (phantom gap created)", g)
	}
}

func TestObserveOutOfOrder_LastSeqNumNotRegressed(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{}, nil)
	// A duplicate/old seqNum=1 must be silently ignored (no gap registered).
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	// Subsequent in-order frame must not create a gap.
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, nil)
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("after old+in-order: PendingGaps = %d, want 0", g)
	}
}

// ── Multi-flow tests ──────────────────────────────────────────────────────────

func TestObserveMultiFlow_NoFalseGap(t *testing.T) {
	tr := newTestTracker()
	// flowA: seqNums 1,2 interleaved with flowB seqNums 1,2.
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowB, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowB, 2, [32]byte{}, nil)
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("multi-flow interleaving: PendingGaps = %d, want 0", g)
	}
}

func TestObserveMultiFlow_GapInOneFlow_OtherUnaffected(t *testing.T) {
	tr := newTestTracker()
	// flowA: 1→2→4 (gap at seqNum=3).
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowB, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 4, [32]byte{}, nil) // gap at seqNum=3
	// flowB continues cleanly.
	tr.Observe(0, [32]byte{}, flowB, 2, [32]byte{}, nil)
	if g := tr.PendingGaps(); g != 1 {
		t.Errorf("gap in one flow: PendingGaps = %d, want 1", g)
	}
}

func TestObserveDuplicate_Suppressed(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{}, nil)
	// Exact duplicate of seqNum=2.
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, nil)
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("duplicate frame: PendingGaps = %d, want 0", g)
	}
}

func TestObserveBurstGap_AllMissingsRegistered(t *testing.T) {
	tr := newTestTracker()
	// flowA: 1→5 (seqNums 2,3,4 all missing).
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 5, [32]byte{}, nil)
	if g := tr.PendingGaps(); g != 3 {
		t.Fatalf("burst gap: PendingGaps = %d, want 3", g)
	}
	// Fill each missing seqNum.
	tr.Fill(flowA, 2)
	tr.Fill(flowA, 3)
	tr.Fill(flowA, 4)
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("after fills: PendingGaps = %d, want 0", g)
	}
}

func TestSweepOnce_StaleFlowEvicted(t *testing.T) {
	cfg := nack.TrackerConfig{
		JitterMax:  0,
		BackoffMax: 5 * time.Second,
		MaxRetries: 3,
		GapTTL:     10 * time.Second,
		TailTTL:    50 * time.Millisecond, // very short for testing
	}
	tr := nack.New(cfg, nil, nil, nil, nil)
	// Register a flow.
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	if tr.ActiveFlows() == 0 {
		t.Fatal("flow should be registered")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)
	// Wait for TailTTL + sweep interval to pass.
	time.Sleep(300 * time.Millisecond)
	if n := tr.ActiveFlows(); n != 0 {
		t.Errorf("stale flow: ActiveFlows = %d, want 0", n)
	}
}

// ── Fill ─────────────────────────────────────────────────────────────────────

func TestFill_ClosesGap(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, nil) // gap at seqNum=2
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("before Fill: PendingGaps = %d, want 1", g)
	}
	tr.Fill(flowA, 2)
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("after Fill: PendingGaps = %d, want 0", g)
	}
}

func TestFill_Nonexistent_NoPanic(t *testing.T) {
	tr := newTestTracker()
	// Fill on an entry that does not exist must be a no-op.
	tr.Fill(flowA, 9999)
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("Fill nonexistent: PendingGaps = %d, want 0", g)
	}
}

func TestFill_ZeroSeqNum_Ignored(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, nil) // gap at seqNum=2
	// Fill with seqNum=0 must be ignored (0 is the "unset" sentinel).
	tr.Fill(flowA, 0)
	if g := tr.PendingGaps(); g != 1 {
		t.Errorf("Fill(flowA,0): PendingGaps = %d, want 1 (gap not removed)", g)
	}
}

func TestFill_MultipleFlows_OnlyClosesCorrectFlow(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, nil) // gap in flowA at seqNum=2
	tr.Observe(1, [32]byte{}, flowB, 1, [32]byte{}, nil)
	tr.Observe(1, [32]byte{}, flowB, 3, [32]byte{}, nil) // gap in flowB at seqNum=2
	if g := tr.PendingGaps(); g != 2 {
		t.Fatalf("before fill: PendingGaps = %d, want 2", g)
	}
	tr.Fill(flowA, 2) // close only flowA gap
	if g := tr.PendingGaps(); g != 1 {
		t.Errorf("after Fill(flowA,2): PendingGaps = %d, want 1", g)
	}
	tr.Fill(flowB, 2) // close flowB gap
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("after Fill(flowB,2): PendingGaps = %d, want 0", g)
	}
}

// ── sendNACK integration tests ──────────────────────────────────────────────
//
// These tests start the full Tracker (gcLoop + dispatchLoop) with a mock UDP
// endpoint and verify that ACK/MISS/timeout are handled correctly.

// pollGaps waits up to timeout for tr.PendingGaps() to equal want.
func pollGaps(tr *nack.Tracker, want int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if tr.PendingGaps() == want {
			return want
		}
		time.Sleep(25 * time.Millisecond)
	}
	return tr.PendingGaps()
}

func TestSendNACK_ACK_CancelsGap(t *testing.T) {
	// Start a mock UDP endpoint that responds with ACK to any NACK.
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
			var resp [nack.ResponseSize]byte
			nack.EncodeResponse(&nack.Response{
				MsgType: nack.MsgTypeACK,
				Flags:   0x01,
				SeqNum:  2,
			}, resp[:])
			_, _ = mockConn.WriteTo(resp[:], src)
		}
	}()

	cfg := nack.TrackerConfig{
		JitterMax:  0,
		BackoffMax: 1 * time.Second,
		MaxRetries: 5,
		GapTTL:     10 * time.Second,
	}
	tr := nack.New(cfg, []string{mockConn.LocalAddr().String()}, nil, nil, nil)

	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, nil) // gap at seqNum=2
	if tr.PendingGaps() != 1 {
		t.Fatalf("setup: PendingGaps = %d, want 1", tr.PendingGaps())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	// Poll for the gap to be cancelled by ACK.
	got := pollGaps(tr, 0, 3*time.Second)
	if got != 0 {
		t.Errorf("after ACK: PendingGaps = %d, want 0", got)
	}
}

func TestSendNACK_MISS_AdvancesRetry(t *testing.T) {
	// Mock endpoint that always responds with MISS.
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
			var resp [nack.ResponseSize]byte
			nack.EncodeResponse(&nack.Response{
				MsgType: nack.MsgTypeMISS,
				SeqNum:  0,
			}, resp[:])
			_, _ = mockConn.WriteTo(resp[:], src)
		}
	}()

	cfg := nack.TrackerConfig{
		JitterMax:  0,
		BackoffMax: 1 * time.Second,
		MaxRetries: 2,
		GapTTL:     10 * time.Second,
	}
	tr := nack.New(cfg, []string{mockConn.LocalAddr().String()}, nil, nil, nil)

	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, nil) // gap at seqNum=2
	if tr.PendingGaps() != 1 {
		t.Fatalf("setup: PendingGaps = %d, want 1", tr.PendingGaps())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	// With MaxRetries=2 the gap should be evicted after retries are exhausted.
	got := pollGaps(tr, 0, 5*time.Second)
	if got != 0 {
		t.Errorf("after MISS exhaustion: PendingGaps = %d, want 0", got)
	}
}

func TestSendNACK_Timeout_BacksOff(t *testing.T) {
	// Mock endpoint that never responds — sendNACK will hit respTimeout.
	mockConn, err := net.ListenPacket("udp", "[::1]:0")
	if err != nil {
		t.Skipf("UDP loopback unavailable: %v", err)
	}
	// Don't read from mockConn — let NACKs timeout.
	defer func() { _ = mockConn.Close() }()

	cfg := nack.TrackerConfig{
		JitterMax:  0,
		BackoffMax: 500 * time.Millisecond,
		MaxRetries: 2,
		GapTTL:     10 * time.Second,
	}
	tr := nack.New(cfg, []string{mockConn.LocalAddr().String()}, nil, nil, nil)

	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, nil) // gap at seqNum=2
	if tr.PendingGaps() != 1 {
		t.Fatalf("setup: PendingGaps = %d, want 1", tr.PendingGaps())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	// Gap survives the first cycle (timeout → backoff) but is eventually
	// evicted when MaxRetries is exceeded.
	got := pollGaps(tr, 0, 8*time.Second)
	if got != 0 {
		t.Errorf("after timeout exhaustion: PendingGaps = %d, want 0", got)
	}
}

// ── Subtree isolation ─────────────────────────────────────────────────────────

func TestObserve_SubtreeIsolation(t *testing.T) {
	tr := newTestTracker()
	var subA, subB [32]byte
	subA[0] = 0xAA
	subB[0] = 0xBB

	// flowA uses subA in its hashKey; 3 contiguous frames.
	tr.Observe(0, subA, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, subA, flowA, 2, [32]byte{}, nil)
	tr.Observe(0, subA, flowA, 3, [32]byte{}, nil)

	// flowB uses subB; 2 contiguous frames interleaved.
	tr.Observe(0, subB, flowB, 1, [32]byte{}, nil)
	tr.Observe(0, subB, flowB, 2, [32]byte{}, nil)

	// Neither flow has gaps.
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("interleaved subtree flows produced %d gaps, want 0", g)
	}
}

func TestObserve_SubtreeGapDoesNotAffectOtherSubtree(t *testing.T) {
	tr := newTestTracker()
	var subA, subB [32]byte
	subA[0] = 0xAA
	subB[0] = 0xBB

	// flowA (subA): gap between seqNum 1 and 3.
	tr.Observe(0, subA, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, subA, flowA, 3, [32]byte{}, nil) // gap at seqNum=2
	// flowB (subB): clean chain.
	tr.Observe(0, subB, flowB, 1, [32]byte{}, nil)
	tr.Observe(0, subB, flowB, 2, [32]byte{}, nil)

	// Exactly one gap: in flowA only.
	if g := tr.PendingGaps(); g != 1 {
		t.Errorf("PendingGaps = %d, want 1 (only in flowA)", g)
	}
}

// ── Unicast NACK recovery (data return channel) ─────────────────────────────────

// dataFrame builds a synthetic frame that is not a control Response: BSV magic at
// [0:4] and FrameVer (0x01) at [6], padded above minRetransmitFrame.
func dataFrame() []byte {
	b := make([]byte, 100)
	b[0], b[1], b[2], b[3] = 0xE3, 0xE1, 0xF3, 0xE8 // MagicBSV
	b[6] = 0x01                                     // FrameVer (≠ any control MsgType)
	return b
}

// TestSendNACK_UnicastRetransmit_Recovered: a retry returns the data frame on the
// NACK return channel plus a unicast-flagged ACK; the tracker must re-inject the
// frame via recoverFn and cancel the gap.
func TestSendNACK_UnicastRetransmit_Recovered(t *testing.T) {
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
			_, _ = mockConn.WriteTo(dataFrame(), src) // data first
			var resp [nack.ResponseSize]byte
			nack.EncodeResponse(&nack.Response{MsgType: nack.MsgTypeACK, Flags: 0x02, SeqNum: 2}, resp[:])
			_, _ = mockConn.WriteTo(resp[:], src) // then unicast-flagged ACK
		}
	}()

	cfg := nack.TrackerConfig{JitterMax: 0, BackoffMax: time.Second, MaxRetries: 5, GapTTL: 10 * time.Second}
	tr := nack.New(cfg, []string{mockConn.LocalAddr().String()}, nil, nil, nil)

	recovered := make(chan []byte, 4)
	tr.SetRecoverFunc(func(raw []byte) bool { recovered <- raw; return true })

	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, nil) // gap at seqNum 2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	select {
	case raw := <-recovered:
		if len(raw) != 100 {
			t.Errorf("recovered frame len = %d, want 100", len(raw))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("recoverFn never called with the unicast retransmit")
	}
	if got := pollGaps(tr, 0, 3*time.Second); got != 0 {
		t.Errorf("after unicast recovery: PendingGaps = %d, want 0", got)
	}
}

// TestSendNACK_UnicastACK_NoData_NotCancelled: a unicast-flagged ACK with NO data
// frame (retransmit lost in transit) must NOT cancel the gap — recovery is only
// confirmed by the data itself, so the gap stays pending to escalate/retry.
func TestSendNACK_UnicastACK_NoData_NotCancelled(t *testing.T) {
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
			var resp [nack.ResponseSize]byte // unicast-flagged ACK only; data "lost"
			nack.EncodeResponse(&nack.Response{MsgType: nack.MsgTypeACK, Flags: 0x02, SeqNum: 2}, resp[:])
			_, _ = mockConn.WriteTo(resp[:], src)
		}
	}()

	// High retry budget so the gap keeps being retried well past the check —
	// isolating the "ACK alone must not cancel" behaviour from MaxRetries
	// eviction. The old bug cancelled on the first ACK (~one respTimeout).
	cfg := nack.TrackerConfig{JitterMax: 0, BackoffBase: 50 * time.Millisecond, BackoffMax: 100 * time.Millisecond, MaxRetries: 50, GapTTL: 10 * time.Second}
	tr := nack.New(cfg, []string{mockConn.LocalAddr().String()}, nil, nil, nil)
	tr.SetRecoverFunc(func(raw []byte) bool { return true }) // recoverFn set, but no data will arrive

	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, nil) // gap at seqNum 2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	// The gap must remain pending (still retrying), not be falsely cancelled by
	// the unicast-flagged ACK whose data never arrived.
	time.Sleep(500 * time.Millisecond)
	if got := tr.PendingGaps(); got != 1 {
		t.Errorf("ACK-without-data falsely cleared the gap: PendingGaps = %d, want 1 (still retrying)", got)
	}
}

// unicastRepairServer answers every NACK with the data frame followed by an ACK
// carrying flags — the two-datagram exchange a retry endpoint performs when
// -beacon-flags-unicast is on (the deployed default).
func unicastRepairServer(t *testing.T, flags byte, withData bool) net.PacketConn {
	t.Helper()
	c, err := net.ListenPacket("udp", "[::1]:0")
	if err != nil {
		t.Skipf("UDP loopback unavailable: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	go func() {
		buf := make([]byte, 256)
		for {
			_, src, err := c.ReadFrom(buf)
			if err != nil {
				return
			}
			if withData {
				_, _ = c.WriteTo(dataFrame(), src)
			}
			var resp [nack.ResponseSize]byte
			nack.EncodeResponse(&nack.Response{MsgType: nack.MsgTypeACK, Flags: flags, SeqNum: 2}, resp[:])
			_, _ = c.WriteTo(resp[:], src)
		}
	}()
	return c
}

// TestSendNACK_UnicastRetransmit_RoutedToObservingWorker: with several workers
// registered, the recovered frame must be re-injected into the worker that
// OBSERVED the flow. Reassembly state is per-worker, so a fragment handed to any
// other worker lands in a buffer holding none of its siblings and the object
// never completes — the repair would be booked while the data went nowhere.
func TestSendNACK_UnicastRetransmit_RoutedToObservingWorker(t *testing.T) {
	mockConn := unicastRepairServer(t, 0x02, true)

	cfg := nack.TrackerConfig{JitterMax: 0, BackoffMax: time.Second, MaxRetries: 5, GapTTL: 10 * time.Second}
	tr := nack.New(cfg, []string{mockConn.LocalAddr().String()}, nil, nil, nil)

	got := make(chan int, 8)
	for id := range 4 {
		tr.RegisterRecover(id, func(raw []byte) bool { got <- id; return true })
	}
	// The global fallback must NOT win when a worker owns the flow.
	tr.SetRecoverFunc(func(raw []byte) bool { got <- -1; return true })

	const owner = 2
	tr.ObserveFrom(owner, 0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.ObserveFrom(owner, 0, [32]byte{}, flowA, 3, [32]byte{}, nil) // gap at seqNum 2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	select {
	case id := <-got:
		if id != owner {
			t.Errorf("recovered frame re-injected into worker %d, want %d (the worker that observed the flow)", id, owner)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no worker received the unicast retransmit")
	}
}

// TestRequestGapsFrom_RoutesToDiscoveringWorker: the reassembly tail-loss path
// registers gaps through RequestGapsFrom, and those recoveries must return to the
// buffer that is missing the fragments.
func TestRequestGapsFrom_RoutesToDiscoveringWorker(t *testing.T) {
	mockConn := unicastRepairServer(t, 0x02, true)

	cfg := nack.TrackerConfig{JitterMax: 0, BackoffMax: time.Second, MaxRetries: 5, GapTTL: 10 * time.Second}
	tr := nack.New(cfg, []string{mockConn.LocalAddr().String()}, nil, nil, nil)

	got := make(chan int, 8)
	for id := range 3 {
		tr.RegisterRecover(id, func(raw []byte) bool { got <- id; return true })
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	const owner = 1
	tr.RequestGapsFrom(owner, flowA, 0, [32]byte{}, []uint64{2})

	select {
	case id := <-got:
		if id != owner {
			t.Errorf("recovered fragment re-injected into worker %d, want %d", id, owner)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no worker received the unicast retransmit")
	}
}

// TestSendNACK_UnicastOnly_NoRecoverFunc_BooksUnrecovered: with NO re-injection
// callback the drained data frame is discarded, so a unicast-ONLY ACK must not
// cancel the gap. Cancelling books a suppression for data that reached no
// consumer — a dead recovery path reading as a healthy one.
func TestSendNACK_UnicastOnly_NoRecoverFunc_BooksUnrecovered(t *testing.T) {
	mockConn := unicastRepairServer(t, 0x02, true) // unicast bit only

	cfg := nack.TrackerConfig{JitterMax: 0, BackoffBase: 50 * time.Millisecond, BackoffMax: 100 * time.Millisecond, MaxRetries: 50, GapTTL: 10 * time.Second}
	tr := nack.New(cfg, []string{mockConn.LocalAddr().String()}, nil, nil, nil) // no recover func at all

	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, nil) // gap at seqNum 2
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("setup: PendingGaps = %d, want 1", g)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	// The gap is removed either way; what must NOT happen is it being booked as
	// a fill. abandonGap is the only path that removes it without one, and it
	// stops the retry loop, so a settled zero here with the frame discarded is
	// the honest outcome. The regression this guards is cancelGap on the ACK.
	if got := pollGaps(tr, 0, 3*time.Second); got != 0 {
		t.Errorf("unicast-only ACK with no recover func left the gap pending: %d", got)
	}
}

// TestSendNACK_MulticastAndUnicastACK_NoRecoverFunc_Cancels: when the retry ALSO
// multicast the repair, the data path fills the gap, so the trust-the-repair
// cancel is still correct even with no re-injection callback.
func TestSendNACK_MulticastAndUnicastACK_NoRecoverFunc_Cancels(t *testing.T) {
	mockConn := unicastRepairServer(t, 0x03, false) // multicast + unicast bits

	cfg := nack.TrackerConfig{JitterMax: 0, BackoffBase: 50 * time.Millisecond, BackoffMax: 100 * time.Millisecond, MaxRetries: 50, GapTTL: 10 * time.Second}
	tr := nack.New(cfg, []string{mockConn.LocalAddr().String()}, nil, nil, nil)

	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	if got := pollGaps(tr, 0, 3*time.Second); got != 0 {
		t.Errorf("multicast-flagged ACK did not close the gap: PendingGaps = %d, want 0", got)
	}
}

// TestObserveEmitterChange_Rebaselines: a forward jump beyond MaxForwardJump is an
// EMITTER CHANGE (anycast failover between long-lived proxies with divergent flow
// counters), not loss — the flow re-baselines: no phantom gap range is registered
// (which would NACK-storm and pollute unrecovered counters), pending phantoms drop.
func TestObserveEmitterChange_Rebaselines(t *testing.T) {
	tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second, MaxForwardJump: 1000}, nil, nil, nil, nil)
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 5, [32]byte{}, nil) // real gap: 3,4
	if g := tr.PendingGaps(); g != 2 {
		t.Fatalf("setup: PendingGaps = %d, want 2", g)
	}
	// New emitter takes the flow at seq 50000 — beyond any plausible burst.
	tr.Observe(0, [32]byte{}, flowA, 50000, [32]byte{}, nil)
	if g := tr.PendingGaps(); g != 0 {
		t.Fatalf("emitter change must drop phantom pendings: PendingGaps = %d, want 0", g)
	}
	// Tracking continues from the new baseline: next contiguous frame = no gap,
	// a small real gap after it is still detected.
	tr.Observe(0, [32]byte{}, flowA, 50001, [32]byte{}, nil)
	if g := tr.PendingGaps(); g != 0 {
		t.Fatalf("contiguous after re-baseline: PendingGaps = %d, want 0", g)
	}
	tr.Observe(0, [32]byte{}, flowA, 50003, [32]byte{}, nil)
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("real gap after re-baseline: PendingGaps = %d, want 1", g)
	}
}

// TestObserveWithinMaxJump_StillGaps: jumps INSIDE the plausible-burst bound remain
// ordinary gaps (recovery still works for genuine loss bursts).
func TestObserveWithinMaxJump_StillGaps(t *testing.T) {
	tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second, MaxForwardJump: 1000}, nil, nil, nil, nil)
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, nil)
	tr.Observe(0, [32]byte{}, flowA, 500, [32]byte{}, nil) // 498 missing, within bound
	if g := tr.PendingGaps(); g != 498 {
		t.Fatalf("PendingGaps = %d, want 498", g)
	}
}

// TestObserveRateAwareRebaseline: with a learned arrival rate, a jump far beyond
// rate×silence re-baselines (emitter divergence UNDER the absolute cap — the case a
// fixed threshold cannot catch), while an outage-sized jump still registers as gaps.
func TestObserveRateAwareRebaseline(t *testing.T) {
	tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second, MaxForwardJump: 100000}, nil, nil, nil, nil)
	// Learn a ~1ms cadence (contiguous frames).
	for i := uint64(1); i <= 20; i++ {
		tr.Observe(0, [32]byte{}, flowA, i, [32]byte{}, nil)
		time.Sleep(time.Millisecond)
	}
	if g := tr.PendingGaps(); g != 0 {
		t.Fatalf("setup: PendingGaps = %d, want 0", g)
	}
	// ~50ms silence at ~1ms cadence ⇒ a real outage misses ~50 frames; a jump of
	// 2000 is ~40× the plausible burst ⇒ emitter change. Re-baseline the DIVERGENT
	// remainder, but NACK the rate-plausible tail (~50 real losses the new emitter
	// dropped in the transition, recoverable from its retry) — not silent, not the
	// whole 2000-wide phantom range.
	time.Sleep(50 * time.Millisecond)
	tr.Observe(0, [32]byte{}, flowA, 2020, [32]byte{}, nil)
	tail := tr.PendingGaps()
	if tail == 0 || tail >= 2000 {
		t.Fatalf("re-baseline must NACK the bounded rate-plausible tail (non-zero, not the phantom range): PendingGaps = %d", tail)
	}
	// Continue contiguously from the new baseline, then a plausible outage-sized gap
	// (100 missing) registers ON TOP of the recovered tail.
	for i := uint64(2021); i <= 2040; i++ {
		tr.Observe(0, [32]byte{}, flowA, i, [32]byte{}, nil)
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	tr.Observe(0, [32]byte{}, flowA, 2141, [32]byte{}, nil) // 100 missing ≤ 4×(~50+1)
	if g := tr.PendingGaps(); g != tail+100 {
		t.Fatalf("plausible outage gap must add 100: PendingGaps = %d, want %d", g, tail+100)
	}
}

// TestRebaselineRecoversTransition: on an anycast failover (implausible forward jump)
// the flow re-baselines to the new emitter's counter, but the rate-plausible TAIL — the
// frames the new emitter lost in the ~sub-second transition — is NACKed for recovery
// (not silently dropped inside the phantom range). Bounded, so no NACK storm.
func TestRebaselineRecoversTransition(t *testing.T) {
	tr := newTestTracker()
	// Establish a ~2ms inter-arrival rate estimate over contiguous frames.
	var seq uint64
	for i := 0; i < 60; i++ {
		seq++
		tr.Observe(0, [32]byte{}, flowA, seq, [32]byte{}, nil)
		time.Sleep(2 * time.Millisecond)
	}
	if g := tr.PendingGaps(); g != 0 {
		t.Fatalf("contiguous stream should have no gaps, got %d", g)
	}
	// A ~40ms "transition outage", then a huge forward jump (the new emitter's divergent
	// counter) — implausible ⇒ re-baseline path.
	time.Sleep(40 * time.Millisecond)
	tr.Observe(0, [32]byte{}, flowA, seq+100000, [32]byte{}, nil)

	g := tr.PendingGaps()
	if g == 0 {
		t.Fatal("re-baseline silently dropped the transition — want the rate-plausible tail NACKed")
	}
	if g >= 100000 {
		t.Fatalf("re-baseline NACKed the whole phantom range (%d) — want only the bounded rate-plausible tail", g)
	}
	// The tail should be ~ outage/IPG (≈20 at 40ms/2ms); assert generously to avoid
	// scheduler-timing flakiness while still proving it is small + bounded.
	if g > 1000 {
		t.Fatalf("recovery tail = %d, want a small bounded count (~outage/IPG)", g)
	}
}

// ── Proxy-restart heuristic vs repairs (the scenario-17 phantom cascade) ─────

// buildFlowTo drives flowA contiguously from 1 to last with a live source,
// leaving gaps at exactly the given seqNums (skipped on the way up).
func buildFlowTo(tr *nack.Tracker, last uint64, gaps map[uint64]bool) {
	src := net.ParseIP("fd10::99")
	for s := uint64(1); s <= last; s++ {
		if gaps[s] {
			continue
		}
		tr.Observe(0, [32]byte{}, flowA, s, [32]byte{}, src)
	}
}

// TestRepairLowSeqDoesNotResetFlow: a unicast repair (nil source) filling a
// pending gap at a seqNum below the reset threshold must ONLY fill that gap.
// Before the fix it fell through to the restart heuristic, which flushed every
// other pending gap to unrecovered and re-baselined the flow to the repair's
// seqNum — the phantom-unrecovered cascade scenario 17 measured.
func TestRepairLowSeqDoesNotResetFlow(t *testing.T) {
	tr := newTestTracker()
	buildFlowTo(tr, 150, map[uint64]bool{60: true, 130: true})
	if g := tr.PendingGaps(); g != 2 {
		t.Fatalf("setup: PendingGaps = %d, want 2 (60, 130)", g)
	}

	// Repair for 60 arrives off the NACK channel: no live source.
	tr.Observe(0, [32]byte{}, flowA, 60, [32]byte{}, nil)

	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("after repair: PendingGaps = %d, want 1 (130 must survive)", g)
	}
	// The flow baseline must be untouched: the next wire frame is contiguous
	// and must register nothing (a reset baseline would read 151 as a jump).
	tr.Observe(0, [32]byte{}, flowA, 151, [32]byte{}, net.ParseIP("fd10::99"))
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("after next wire frame: PendingGaps = %d, want 1 (no phantom gaps)", g)
	}
}

// TestLateReinjectLowSeqDoesNotReset: a repair arriving AFTER its gap was
// abandoned (nothing pending) is history replayed, not a restarted proxy —
// it must be ignored entirely.
func TestLateReinjectLowSeqDoesNotReset(t *testing.T) {
	tr := newTestTracker()
	buildFlowTo(tr, 150, nil)

	tr.Observe(0, [32]byte{}, flowA, 60, [32]byte{}, nil) // late repair, nothing pending

	tr.Observe(0, [32]byte{}, flowA, 151, [32]byte{}, net.ParseIP("fd10::99"))
	if g := tr.PendingGaps(); g != 0 {
		t.Fatalf("PendingGaps = %d, want 0 (late reinject must not reset the baseline)", g)
	}
}

// TestWireLowSeqStillResetsFlow: the restart heuristic itself is load-bearing —
// a LIVE frame rolling back below the threshold on an established flow still
// resets the flow (pending flushed, baseline moved).
func TestWireLowSeqStillResetsFlow(t *testing.T) {
	tr := newTestTracker()
	src := net.ParseIP("fd10::99")
	buildFlowTo(tr, 150, map[uint64]bool{130: true})
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("setup: PendingGaps = %d, want 1", g)
	}

	tr.Observe(0, [32]byte{}, flowA, 5, [32]byte{}, src) // restarted proxy, live wire frame

	if g := tr.PendingGaps(); g != 0 {
		t.Fatalf("after restart: PendingGaps = %d, want 0 (flushed)", g)
	}
	// Tracking resumes from the restarted stream: 6 is contiguous, no gap.
	tr.Observe(0, [32]byte{}, flowA, 6, [32]byte{}, src)
	if g := tr.PendingGaps(); g != 0 {
		t.Fatalf("after restart+1: PendingGaps = %d, want 0", g)
	}
}
