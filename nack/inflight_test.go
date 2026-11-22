package nack

import (
	"testing"
	"time"
)

// One NACK exchange per gap. A dispatch can wait for a sendNACK slot past
// the nextAttempt the sweep stamped on it; the sweep must not dispatch the
// same gap again while that exchange is queued or running, or two exchanges
// re-inject and book one gap at once.
func TestSweepDispatchesOneExchangePerGap(t *testing.T) {
	tr := New(TrackerConfig{JitterMax: 0, GapTTL: 10 * time.Second, MaxRetries: 50}, nil, nil, nil, nil)
	var zero [32]byte
	tr.Observe(1, zero, 42, 1, zero, nil)
	tr.Observe(1, zero, 42, 3, zero, nil) // 2 pending
	now := time.Now()

	tr.sweepOnce(now.Add(time.Millisecond))
	if n := len(tr.nackQueue); n != 1 {
		t.Fatalf("first sweep queued %d exchanges, want 1", n)
	}
	// No dispatcher runs here, so the exchange is still queued, long past the
	// nextAttempt the sweep stamped.
	tr.sweepOnce(now.Add(time.Second))
	if n := len(tr.nackQueue); n != 1 {
		t.Fatalf("a second exchange for 2 was queued while the first was in flight: %d queued, want 1", n)
	}
}
