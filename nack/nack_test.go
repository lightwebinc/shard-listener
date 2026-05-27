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

// ── Observe ───────────────────────────────────────────────────────────────────

func TestObserveFirstFrame_NoGap(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("first frame: PendingGaps = %d, want 0", g)
	}
}

func TestObserveContiguous_NoGap(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{})
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{})
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("contiguous: PendingGaps = %d, want 0", g)
	}
}

func TestObserveSeqNumZero_Ignored(t *testing.T) {
	tr := newTestTracker()
	// seqNum == 0 means proxy has not stamped the frame; must be ignored.
	tr.Observe(0, [32]byte{}, flowA, 0, [32]byte{})
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("zero seqNum: PendingGaps = %d, want 0", g)
	}
	// Flow must initialise correctly on the first non-zero frame.
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("after zero-seqNum: PendingGaps = %d, want 0", g)
	}
}

func TestObserveNewFlow_NoGap(t *testing.T) {
	tr := newTestTracker()
	// Flow A established.
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{})
	// Flow B is an independent flow; its first frame must not create a gap.
	tr.Observe(0, [32]byte{}, flowB, 1, [32]byte{})
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("new flow start: PendingGaps = %d, want 0", g)
	}
}

func TestObserveGap_Detected(t *testing.T) {
	tr := newTestTracker()
	// Frame 1 establishes the flow; seqNum 3 arrives next — seqNum 2 is missing.
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{})
	if g := tr.PendingGaps(); g != 1 {
		t.Errorf("gap detected: PendingGaps = %d, want 1", g)
	}
}

func TestObserveDuplicateGap_NotDuplicated(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	// seqNum 3 reveals gap at 2.
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{})
	// Duplicate of seqNum 3 must not register an additional gap.
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{})
	if g := tr.PendingGaps(); g != 1 {
		t.Errorf("duplicate frame: PendingGaps = %d, want 1", g)
	}
}

func TestObserveMultipleGroups_IndependentFlows(t *testing.T) {
	tr := newTestTracker()
	// flowA (group 0): gap between seqNum 1 and 3.
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{})
	// flowB (group 1): clean contiguous delivery.
	tr.Observe(1, [32]byte{}, flowB, 1, [32]byte{})
	tr.Observe(1, [32]byte{}, flowB, 2, [32]byte{})
	if g := tr.PendingGaps(); g != 1 {
		t.Errorf("multi-flow: PendingGaps = %d, want 1 (only flowA has gap)", g)
	}
}

func TestObserveGap_AutoClosed_WhenMatchingSeqNumArrives(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}) // gap at seqNum=2
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("before auto-close: PendingGaps = %d, want 1", g)
	}

	// seqNum=2 arrives (out-of-order retransmit).
	// Observe auto-closes pending[2]; seqNum(2) <= lastSeqNum(3) so no new gap.
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{})
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
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}) // gap at seqNum=2
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("setup: PendingGaps = %d, want 1", g)
	}

	// seqNum=2 arrives late — must close gap AND not create a phantom gap.
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{})
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("after retransmit: PendingGaps = %d, want 0 (phantom gap created)", g)
	}
}

func TestObserveOutOfOrder_LastSeqNumNotRegressed(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{})
	// A duplicate/old seqNum=1 must be silently ignored (no gap registered).
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	// Subsequent in-order frame must not create a gap.
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{})
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("after old+in-order: PendingGaps = %d, want 0", g)
	}
}

// ── Multi-flow tests ──────────────────────────────────────────────────────────

func TestObserveMultiFlow_NoFalseGap(t *testing.T) {
	tr := newTestTracker()
	// flowA: seqNums 1,2 interleaved with flowB seqNums 1,2.
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flowB, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{})
	tr.Observe(0, [32]byte{}, flowB, 2, [32]byte{})
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("multi-flow interleaving: PendingGaps = %d, want 0", g)
	}
}

func TestObserveMultiFlow_GapInOneFlow_OtherUnaffected(t *testing.T) {
	tr := newTestTracker()
	// flowA: 1→2→4 (gap at seqNum=3).
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{})
	tr.Observe(0, [32]byte{}, flowB, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flowA, 4, [32]byte{}) // gap at seqNum=3
	// flowB continues cleanly.
	tr.Observe(0, [32]byte{}, flowB, 2, [32]byte{})
	if g := tr.PendingGaps(); g != 1 {
		t.Errorf("gap in one flow: PendingGaps = %d, want 1", g)
	}
}

func TestObserveDuplicate_Suppressed(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{})
	// Exact duplicate of seqNum=2.
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{})
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{})
	if g := tr.PendingGaps(); g != 0 {
		t.Errorf("duplicate frame: PendingGaps = %d, want 0", g)
	}
}

func TestObserveBurstGap_AllMissingsRegistered(t *testing.T) {
	tr := newTestTracker()
	// flowA: 1→5 (seqNums 2,3,4 all missing).
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flowA, 5, [32]byte{})
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
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
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
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}) // gap at seqNum=2
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
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}) // gap at seqNum=2
	// Fill with seqNum=0 must be ignored (0 is the "unset" sentinel).
	tr.Fill(flowA, 0)
	if g := tr.PendingGaps(); g != 1 {
		t.Errorf("Fill(flowA,0): PendingGaps = %d, want 1 (gap not removed)", g)
	}
}

func TestFill_MultipleFlows_OnlyClosesCorrectFlow(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}) // gap in flowA at seqNum=2
	tr.Observe(1, [32]byte{}, flowB, 1, [32]byte{})
	tr.Observe(1, [32]byte{}, flowB, 3, [32]byte{}) // gap in flowB at seqNum=2
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

	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}) // gap at seqNum=2
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

	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}) // gap at seqNum=2
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

	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}) // gap at seqNum=2
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
	tr.Observe(0, subA, flowA, 1, [32]byte{})
	tr.Observe(0, subA, flowA, 2, [32]byte{})
	tr.Observe(0, subA, flowA, 3, [32]byte{})

	// flowB uses subB; 2 contiguous frames interleaved.
	tr.Observe(0, subB, flowB, 1, [32]byte{})
	tr.Observe(0, subB, flowB, 2, [32]byte{})

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
	tr.Observe(0, subA, flowA, 1, [32]byte{})
	tr.Observe(0, subA, flowA, 3, [32]byte{}) // gap at seqNum=2
	// flowB (subB): clean chain.
	tr.Observe(0, subB, flowB, 1, [32]byte{})
	tr.Observe(0, subB, flowB, 2, [32]byte{})

	// Exactly one gap: in flowA only.
	if g := tr.PendingGaps(); g != 1 {
		t.Errorf("PendingGaps = %d, want 1 (only in flowA)", g)
	}
}
