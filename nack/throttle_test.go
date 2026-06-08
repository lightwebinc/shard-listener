package nack_test

import (
	"context"
	"testing"
	"time"

	"github.com/lightwebinc/shard-listener/discovery"
	"github.com/lightwebinc/shard-listener/nack"
)

// TestThrottle_DoesNotConsumeRetryBudgetOrEscalate verifies the listener's
// handling of a THROTTLED congestion signal. A single endpoint THROTTLEs the
// first several attempts, then ACKs. With MaxRetries=2 (failed-round cap) the
// gap must still recover: THROTTLED is flow control, not a failed round, so it
// neither evicts the gap nor advances past the (only, healthy) endpoint. If
// THROTTLED were treated as a failure the gap would be evicted unrecovered
// before the ACK.
func TestThrottle_DoesNotConsumeRetryBudgetOrEscalate(t *testing.T) {
	ep := newMockEndpoint(t, 0) // ACK once past the throttle phase
	ep.throttleThen = 4         // THROTTLE the first 4 attempts

	reg := discovery.NewRegistry()
	upsertEndpoint(t, reg, ep, 0, 128, 1)

	cfg := nack.TrackerConfig{
		JitterMax:   0,
		BackoffBase: 50 * time.Millisecond,
		BackoffMax:  80 * time.Millisecond, // caps the THROTTLED hold so the test runs fast
		MaxRetries:  2,
		GapTTL:      10 * time.Second,
	}
	tr := nack.New(cfg, nil, nil, nil, reg)

	const flow = uint64(0xD00D)
	tr.Observe(0, [32]byte{}, flow, 1, [32]byte{})
	tr.Observe(0, [32]byte{}, flow, 3, [32]byte{}) // gap at seqNum=2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	if got := pollGaps(tr, 0, 5*time.Second); got != 0 {
		t.Fatalf("gap not recovered: PendingGaps = %d, want 0 (THROTTLED must not consume the retry budget)", got)
	}
	// The endpoint must have been hit more than MaxRetries times, proving the
	// throttles did not count toward eviction.
	if c := ep.count.Load(); c <= int64(cfg.MaxRetries) {
		t.Errorf("endpoint hit %d times, want > MaxRetries=%d (throttles should not count)", c, cfg.MaxRetries)
	}
}
