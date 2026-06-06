package nack_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lightwebinc/shard-listener/discovery"
	"github.com/lightwebinc/shard-listener/nack"
)

// newSilentEndpoint registers an endpoint that receives NACKs but never
// responds, forcing the listener to hit respTimeout (a failed recovery round).
func newSilentEndpoint(t *testing.T) *mockEndpoint {
	t.Helper()
	c, err := net.ListenPacket("udp", "[::1]:0")
	if err != nil {
		t.Skipf("udp loopback unavailable: %v", err)
	}
	m := &mockEndpoint{conn: c, addr: c.LocalAddr().(*net.UDPAddr), silent: true}
	go m.run()
	t.Cleanup(func() { _ = c.Close() })
	return m
}

// TestBackoff_EscalationDoesNotConsumeRetryBudget is the regression test for the
// retry-budget fix. Two cold lower tiers (always MISS) plus a deepest tier that
// MISSes twice before warming up. MaxRetries=3 counts *failed recovery rounds*,
// not total attempts. The two free escalation hops (retry1, retry2) must not
// consume the budget, so the gap survives both deepest-tier MISSes and recovers
// on the third attempt. Under the old total-attempts cap the gap would have been
// evicted at retry3's first MISS (attempt #3) and counted unrecovered.
func TestBackoff_EscalationDoesNotConsumeRetryBudget(t *testing.T) {
	r1 := newMockEndpoint(t, 1<<30) // always MISS
	r2 := newMockEndpoint(t, 1<<30) // always MISS
	r3 := newMockEndpoint(t, 2)     // MISS twice, then ACK

	reg := discovery.NewRegistry()
	upsertEndpoint(t, reg, r1, 0, 128, 1)
	upsertEndpoint(t, reg, r2, 0, 64, 2)
	upsertEndpoint(t, reg, r3, 1, 128, 3)

	cfg := nack.TrackerConfig{
		JitterMax:   0,
		BackoffBase: 50 * time.Millisecond,
		BackoffMax:  150 * time.Millisecond,
		MaxRetries:  3, // 3 failed rounds; would evict at attempt #3 under old semantics
		GapTTL:      10 * time.Second,
	}
	tr := nack.New(cfg, nil, nil, nil, reg)

	const flow = uint64(0xC0FFEE)
	tr.Observe(0, [32]byte{}, flow, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flow, 3, [32]byte{}) // gap at seqNum=2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	if got := pollGaps(tr, 0, 5*time.Second); got != 0 {
		t.Fatalf("gap not recovered: PendingGaps = %d, want 0 (escalation must not consume retry budget)", got)
	}
}

// TestBackoff_TimeoutFailsOverToDeepestTier verifies that timeouts (no response)
// at lower tiers still advance toward the deepest tier rather than stalling, so
// a gap recovers when only the deepest endpoint is healthy.
func TestBackoff_TimeoutFailsOverToDeepestTier(t *testing.T) {
	r1 := newSilentEndpoint(t)  // never responds
	r2 := newSilentEndpoint(t)  // never responds
	r3 := newMockEndpoint(t, 0) // ACK immediately

	reg := discovery.NewRegistry()
	upsertEndpoint(t, reg, r1, 0, 128, 1)
	upsertEndpoint(t, reg, r2, 0, 64, 2)
	upsertEndpoint(t, reg, r3, 1, 128, 3)

	cfg := nack.TrackerConfig{
		JitterMax:   0,
		BackoffBase: 50 * time.Millisecond,
		BackoffMax:  150 * time.Millisecond,
		MaxRetries:  6,
		GapTTL:      10 * time.Second,
	}
	tr := nack.New(cfg, nil, nil, nil, reg)

	const flow = uint64(0xBEEF)
	tr.Observe(0, [32]byte{}, flow, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flow, 3, [32]byte{}) // gap at seqNum=2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	if got := pollGaps(tr, 0, 6*time.Second); got != 0 {
		t.Fatalf("gap not recovered via failover: PendingGaps = %d, want 0", got)
	}
	if c := r3.count.Load(); c == 0 {
		t.Errorf("deepest tier never reached after lower-tier timeouts")
	}
}

// TestBackoff_DeepTierFirstRetryIsPrompt verifies the backoff magnitude is
// seeded by failed rounds, not total attempts. With three tiers and a large
// BackoffMax, the deepest tier MISSes once then ACKs. The old code seeded the
// exponent from the total attempt count (≈3 hops), so the first retry3 retry
// waited 1<<3 * 500ms ≈ 4s. The fix seeds from failRounds (1), so the retry
// fires at ~BackoffBase. Recovery well under 1.5s discriminates the two.
func TestBackoff_DeepTierFirstRetryIsPrompt(t *testing.T) {
	r1 := newMockEndpoint(t, 1<<30) // always MISS
	r2 := newMockEndpoint(t, 1<<30) // always MISS
	r3 := newMockEndpoint(t, 1)     // MISS once, then ACK

	reg := discovery.NewRegistry()
	upsertEndpoint(t, reg, r1, 0, 128, 1)
	upsertEndpoint(t, reg, r2, 0, 64, 2)
	upsertEndpoint(t, reg, r3, 1, 128, 3)

	cfg := nack.TrackerConfig{
		JitterMax:   0,
		BackoffBase: 100 * time.Millisecond,
		BackoffMax:  5 * time.Second, // large: old 1<<N seeding would wait ~4s here
		MaxRetries:  5,
		GapTTL:      10 * time.Second,
	}
	tr := nack.New(cfg, nil, nil, nil, reg)

	const flow = uint64(0xABCD)
	tr.Observe(0, [32]byte{}, flow, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flow, 3, [32]byte{}) // gap at seqNum=2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	tr.Start(ctx)

	if got := pollGaps(tr, 0, 1500*time.Millisecond); got != 0 {
		t.Fatalf("gap not recovered promptly: PendingGaps = %d after %v (old backoff would need ~4s)", got, time.Since(start))
	}
}
