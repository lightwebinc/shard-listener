package nack_test

import (
	"context"
	"testing"
	"time"

	"github.com/lightwebinc/shard-listener/nack"
)

// ackCfg keeps the retry ladder short so an unconfirmed repair is asked for
// again and given up on within the test's time.
func ackCfg() nack.TrackerConfig {
	return nack.TrackerConfig{
		JitterMax:   0,
		BackoffBase: 20 * time.Millisecond,
		BackoffMax:  40 * time.Millisecond,
		MaxRetries:  3,
		GapTTL:      10 * time.Second,
	}
}

// openGap registers one gap at SeqNum 2 on flowA.
func openGap(tr *nack.Tracker) {
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, emitterA)
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, emitterA)
}

// TestACKWithoutConfirmationChannel_LostCopyIsAskedAgainThenUnrecovered: an
// ACK without the unicast flag (multicast-only, bare, or a unicast copy this
// listener cannot take) says a copy was dispatched on the data plane, not that
// it arrived (BRC-126 §ACK Response, "Repair confirmation"). When that copy is
// lost on the same impaired link, the gap must stay open, be asked for again,
// and after MaxRetries be booked unrecovered. Closing it on the ACK books a
// repair for a frame no consumer received.
func TestACKWithoutConfirmationChannel_LostCopyIsAskedAgainThenUnrecovered(t *testing.T) {
	cases := []struct {
		name    string
		flags   byte
		recover bool
	}{
		{"multicast-flagged", 0x01, true},
		{"bare", 0x00, true},
		{"multicast-and-unicast-without-reinject", 0x03, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			retry := newFrameRetry(t, c.flags, false) // the copy never arrives
			rec := newRecorder(t)
			tr := nack.New(ackCfg(), []string{retry.addr()}, nil, rec, nil)
			if c.recover {
				tr.SetRecoverFunc(func([]byte) bool { return true })
			}
			openGap(tr)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tr.Start(ctx)

			if got := pollGaps(tr, 0, 5*time.Second); got != 0 {
				t.Fatalf("PendingGaps = %d, want 0 once the retry budget is spent", got)
			}
			if n := retry.times(2); n != ackCfg().MaxRetries {
				t.Errorf("SeqNum 2 asked %d times, want %d: an unconfirmed ACK is a round, not a repair", n, ackCfg().MaxRetries)
			}
			out := promText(t, rec)
			if v := counter(out, "bsl_gaps_suppressed_total"); v != 0 {
				t.Errorf("gaps suppressed = %v, want 0: nothing arrived", v)
			}
			if v := counter(out, "bsl_gaps_unrecovered_total"); v != 1 {
				t.Errorf("gaps unrecovered = %v, want 1", v)
			}
		})
	}
}

// TestACKWithoutConfirmationChannel_DataPlaneCopyCloses: the same ACK, and the
// multicast copy does arrive on the data path. The gap closes as a fill, on
// the frame, with one NACK.
func TestACKWithoutConfirmationChannel_DataPlaneCopyCloses(t *testing.T) {
	retry := newFrameRetry(t, 0x01, false)
	rec := newRecorder(t)
	cfg := ackCfg()
	cfg.BackoffBase, cfg.BackoffMax = 500*time.Millisecond, time.Second
	tr := nack.New(cfg, []string{retry.addr()}, nil, rec, nil)
	tr.SetRecoverFunc(func([]byte) bool { return true })
	openGap(tr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	select {
	case seq := <-retry.acked:
		// The retry's multicast copy reaches the worker: Observe auto-fills.
		tr.Observe(0, [32]byte{}, flowA, seq, [32]byte{}, emitterA)
	case <-time.After(3 * time.Second):
		t.Fatal("the gap was never NACKed")
	}
	if got := pollGaps(tr, 0, 2*time.Second); got != 0 {
		t.Fatalf("PendingGaps = %d, want 0 after the data-plane copy", got)
	}
	if n := retry.times(2); n != 1 {
		t.Errorf("SeqNum 2 asked %d times, want 1", n)
	}
	out := promText(t, rec)
	if v := counter(out, "bsl_gaps_suppressed_total"); v != 1 {
		t.Errorf("gaps suppressed = %v, want 1", v)
	}
	if v := counter(out, "bsl_gaps_unrecovered_total"); v != 0 {
		t.Errorf("gaps unrecovered = %v, want 0", v)
	}
}
