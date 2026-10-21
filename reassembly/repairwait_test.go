package reassembly

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
)

// repairWaitFrag builds fragment i of a 4-fragment, 40-byte object on flow 0xCAFE
// with SeqNum 10+i.
func repairWaitFrag(i int) *frame.FragFrame {
	return &frame.FragFrame{
		TxID:    [32]byte{0x77},
		HashKey: 0xCAFE, SeqNum: uint64(10 + i),
		FragIndex: uint16(i), FragTotal: 4,
		OrigPayloadLen: 40, FragData: make([]byte, 10),
	}
}

type repairWaitCounters struct {
	completed, abandoned, extended, lateDone, requested atomic.Int32
	reason                                              atomic.Value
}

func newRepairWaitBuffer(ttl time.Duration, c *repairWaitCounters) *Buffer {
	b := New(16, ttl, false, func([]byte, *frame.Frame) { c.completed.Add(1) })
	b.SetAbandonedHook(func() { c.abandoned.Add(1) })
	b.SetAbandonedReasonHook(func(r string) { c.reason.Store(r) })
	b.SetExtendedHook(func() { c.extended.Add(1) })
	b.SetLateCompleteHook(func() { c.lateDone.Add(1) })
	b.SetIncompleteHook(func(uint64, uint32, [32]byte, []uint64) { c.requested.Add(1) })
	return b
}

// A slot whose missing fragment is still in repair at its TTL must be held, and
// must complete when the repaired fragment lands after the TTL. This is the
// item-77 case: every gap repaired, the object abandoned anyway.
func TestRepairWaitHoldsSlotWhileRepairPending(t *testing.T) {
	var c repairWaitCounters
	b := newRepairWaitBuffer(40*time.Millisecond, &c)
	var pending atomic.Bool
	pending.Store(true)
	b.SetRepairWait(func(hk uint64, missing []uint64) bool {
		if hk != 0xCAFE || len(missing) != 1 || missing[0] != 12 {
			t.Errorf("pending asked about hk=%x missing=%v, want cafe [12]", hk, missing)
		}
		return pending.Load()
	}, 5*time.Second)

	for _, i := range []int{0, 1, 3} { // fragment 2 (SeqNum 12) lost
		b.Observe(repairWaitFrag(i))
	}
	for range 6 { // well past three TTLs
		time.Sleep(30 * time.Millisecond)
		b.Tick()
	}
	if b.Len() != 1 {
		t.Fatalf("slot evicted at TTL while repair was pending (abandoned=%d)", c.abandoned.Load())
	}
	b.Observe(repairWaitFrag(2)) // the repair lands
	pending.Store(false)

	if c.completed.Load() != 1 || c.abandoned.Load() != 0 {
		t.Fatalf("completed=%d abandoned=%d, want 1/0", c.completed.Load(), c.abandoned.Load())
	}
	if c.extended.Load() != 1 || c.lateDone.Load() != 1 {
		t.Errorf("extended=%d lateDone=%d, want 1/1 (counted once per slot)", c.extended.Load(), c.lateDone.Load())
	}
	if c.requested.Load() != 1 {
		t.Errorf("incomplete hook called %d times, want once (first expiry)", c.requested.Load())
	}
}

// When the repair path gives up (nothing pending after the slot asked), the slot
// is abandoned as real loss, not held to its maximum age.
func TestRepairWaitAbandonsWhenRepairGivesUp(t *testing.T) {
	var c repairWaitCounters
	b := newRepairWaitBuffer(40*time.Millisecond, &c)
	b.SetRepairWait(func(uint64, []uint64) bool { return false }, 5*time.Second)

	for _, i := range []int{0, 1, 2} { // tail lost
		b.Observe(repairWaitFrag(i))
	}
	time.Sleep(60 * time.Millisecond)
	b.Tick()

	if c.requested.Load() != 1 {
		t.Fatalf("tail never requested: incomplete hook calls=%d", c.requested.Load())
	}
	if c.abandoned.Load() != 1 || b.Len() != 0 {
		t.Fatalf("abandoned=%d len=%d, want 1/0", c.abandoned.Load(), b.Len())
	}
	if r, _ := c.reason.Load().(string); r != AbandonExpired {
		t.Errorf("reason=%q want %q", r, AbandonExpired)
	}
}

// A tail the tracker did not know about is requested at the first expiry, and
// the slot waits for that repair rather than being dropped in the same call.
func TestRepairWaitRequestsTailThenWaits(t *testing.T) {
	var c repairWaitCounters
	b := newRepairWaitBuffer(40*time.Millisecond, &c)
	var asked atomic.Bool
	b.SetIncompleteHook(func(uint64, uint32, [32]byte, []uint64) { asked.Store(true) })
	// The tracker holds the gap only once the slot has asked for it.
	b.SetRepairWait(func(uint64, []uint64) bool { return asked.Load() }, 5*time.Second)

	for _, i := range []int{0, 1, 2} {
		b.Observe(repairWaitFrag(i))
	}
	time.Sleep(60 * time.Millisecond)
	b.Tick()
	if b.Len() != 1 {
		t.Fatal("slot dropped in the same expiry that requested its tail")
	}
	b.Observe(repairWaitFrag(3))
	if c.completed.Load() != 1 || c.lateDone.Load() != 1 {
		t.Fatalf("completed=%d lateDone=%d, want 1/1", c.completed.Load(), c.lateDone.Load())
	}
}

// The repair wait is bounded: at the maximum age a slot still in repair is
// abandoned and reported as repair timing, not loss.
func TestRepairWaitBoundedByMaxAge(t *testing.T) {
	var c repairWaitCounters
	b := newRepairWaitBuffer(30*time.Millisecond, &c)
	b.SetRepairWait(func(uint64, []uint64) bool { return true }, 100*time.Millisecond)

	for _, i := range []int{0, 1, 3} {
		b.Observe(repairWaitFrag(i))
	}
	deadline := time.Now().Add(time.Second)
	for b.Len() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		b.Tick()
	}
	if b.Len() != 0 {
		t.Fatal("slot held past its maximum age")
	}
	if r, _ := c.reason.Load().(string); r != AbandonRepairTimeout {
		t.Errorf("reason=%q want %q", r, AbandonRepairTimeout)
	}
}

// Capacity eviction reports its own reason.
func TestAbandonReasonCapacity(t *testing.T) {
	var c repairWaitCounters
	b := New(1, time.Minute, false, func([]byte, *frame.Frame) {})
	b.SetAbandonedReasonHook(func(r string) { c.reason.Store(r) })
	b.Observe(repairWaitFrag(0))
	other := repairWaitFrag(0)
	other.TxID = [32]byte{0x78}
	b.Observe(other)
	if r, _ := c.reason.Load().(string); r != AbandonCapacity {
		t.Errorf("reason=%q want %q", r, AbandonCapacity)
	}
}
