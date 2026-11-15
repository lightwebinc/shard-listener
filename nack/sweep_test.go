package nack

import (
	"testing"
	"time"
)

// TestNextSweep: a cheap sweep keeps the base interval, a costly one waits
// ten times its own duration, and the wait is capped.
func TestNextSweep(t *testing.T) {
	for _, c := range []struct{ took, want time.Duration }{
		{0, sweepInterval},
		{2 * time.Millisecond, sweepInterval},
		{10 * time.Millisecond, sweepInterval},
		{30 * time.Millisecond, 300 * time.Millisecond},
		{80 * time.Millisecond, 800 * time.Millisecond},
		{500 * time.Millisecond, maxSweepInterval},
	} {
		if got := nextSweep(c.took); got != c.want {
			t.Errorf("nextSweep(%v) = %v, want %v", c.took, got, c.want)
		}
	}
}

// TestSweepStopsOfferingToAFullQueue: with more due gaps than the NACK queue
// holds, one sweep fills the queue and leaves the rest due, and the next
// sweep offers them once senders have made room.
func TestSweepStopsOfferingToAFullQueue(t *testing.T) {
	tr := New(TrackerConfig{GapTTL: time.Minute, MaxRetries: 3}, nil, nil, nil, nil)
	const bursts = 3 // each a forward jump of a whole MaxForwardJump
	seq := uint64(1)
	tr.Observe(1, [32]byte{}, 0x55, seq, [32]byte{}, nil)
	for i := 0; i < bursts; i++ {
		seq += defaultMaxForwardJump + 1
		tr.Observe(1, [32]byte{}, 0x55, seq, [32]byte{}, nil)
	}
	const gaps = bursts * defaultMaxForwardJump
	if got := tr.PendingGaps(); got != gaps {
		t.Fatalf("pending gaps = %d, want %d", got, gaps)
	}
	if gaps <= cap(tr.nackQueue) {
		t.Fatalf("the test needs more gaps (%d) than the queue holds (%d)", gaps, cap(tr.nackQueue))
	}

	now := time.Now().Add(time.Second) // every gap is due, none expired
	tr.sweepOnce(now)
	if got := len(tr.nackQueue); got != cap(tr.nackQueue) {
		t.Fatalf("queue holds %d after one sweep, want it full (%d)", got, cap(tr.nackQueue))
	}
	// Make room as senders would, then sweep again: gaps the first sweep left
	// due are offered now (the ones it queued are not due again yet).
	for len(tr.nackQueue) > 0 {
		<-tr.nackQueue
	}
	tr.sweepOnce(now.Add(time.Millisecond))
	if got := len(tr.nackQueue); got != cap(tr.nackQueue) {
		t.Fatalf("queue holds %d after the second sweep, want it full again (%d)", got, cap(tr.nackQueue))
	}
	if got := tr.PendingGaps(); got != gaps {
		t.Fatalf("pending gaps = %d after two sweeps, want all %d still pending", got, gaps)
	}
}
