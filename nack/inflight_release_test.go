package nack

import (
	"testing"
	"time"
)

// The end of an exchange releases its gap for the next dispatch, and only
// the entry it was copied from: an entry that replaced it at the same SeqNum
// keeps its own exchange in flight.
func TestExchangeEndReleasesItsGap(t *testing.T) {
	tr := New(TrackerConfig{JitterMax: 0, GapTTL: 10 * time.Second, MaxRetries: 50}, nil, nil, nil, nil)
	var zero [32]byte
	tr.Observe(1, zero, 42, 1, zero, nil)
	tr.Observe(1, zero, 42, 3, zero, nil) // 2 pending
	now := time.Now()

	tr.sweepOnce(now.Add(time.Millisecond))
	first := <-tr.nackQueue
	tr.endExchange(first)
	tr.sweepOnce(now.Add(time.Second))
	if n := len(tr.nackQueue); n != 1 {
		t.Fatalf("after the exchange ended: %d queued, want 1 (the gap is due again)", n)
	}
	old := <-tr.nackQueue

	// The gap closes while that exchange runs, and a new one opens at the same
	// SeqNum with an exchange of its own.
	tr.Fill(42, 2)
	tr.RequestGaps(42, 1, zero, []uint64{2})
	fresh := <-tr.nackQueue

	inFlight := func() bool {
		tr.mu.Lock()
		defer tr.mu.Unlock()
		return tr.flows[42].pending[2].inFlight
	}
	tr.endExchange(old)
	if !inFlight() {
		t.Fatal("the old exchange's end released the new gap's exchange")
	}
	tr.endExchange(fresh)
	if inFlight() {
		t.Fatal("the new exchange's end did not release its gap")
	}
}
