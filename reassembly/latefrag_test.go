package reassembly

import (
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
)

// TestReassembly_LateCopyDoesNotReopenCompletedSlot is the multicast-repair
// regression. NACK repair is answered to the whole band, so every member that
// did NOT lose a fragment receives a copy of one it already consumed. Before
// the completion memory that copy opened a fresh slot, which (a) re-delivered
// the object once the remaining copies followed and (b) expired into an
// onIncomplete NACK for fragments the listener already had — repair feeding
// repair.
func TestReassembly_LateCopyDoesNotReopenCompletedSlot(t *testing.T) {
	payload := buildEFPayloadR(t, 120)
	txID := canonicalID(t, payload)
	half := len(payload) / 2
	frags := [][]byte{payload[:half], payload[half:]}

	var delivered int
	var started, late, incomplete int
	b := New(16, time.Second, true, func([]byte, *frame.Frame) { delivered++ })
	b.SetStartedHook(func() { started++ })
	b.SetLateFragmentHook(func() { late++ })
	b.SetIncompleteHook(func(uint64, uint32, [32]byte, []uint64) { incomplete++ })

	observe := func(i int) {
		b.Observe(buildFragFrame(txID, uint32(len(payload)), uint16(i), 2, frags[i]))
	}

	observe(0)
	observe(1)
	if delivered != 1 || started != 1 {
		t.Fatalf("first pass: delivered=%d started=%d, want 1/1", delivered, started)
	}

	// The whole object arrives a second time, as multicast repair would deliver
	// it to a member that lost nothing.
	observe(0)
	observe(1)

	if delivered != 1 {
		t.Errorf("object delivered %d times, want 1 (late repair copies re-reassembled it)", delivered)
	}
	if started != 1 {
		t.Errorf("reassembly slots opened = %d, want 1 (a late copy reopened the slot)", started)
	}
	if late != 2 {
		t.Errorf("late fragments suppressed = %d, want 2", late)
	}

	// A slot that was never opened cannot expire into a recovery request for
	// fragments the listener already has.
	b.Tick()
	time.Sleep(1100 * time.Millisecond)
	b.Tick()
	if incomplete != 0 {
		t.Errorf("onIncomplete fired %d times for an object that completed", incomplete)
	}
}

// TestReassembly_CompletionMemoryExpires confirms the suppression is a window,
// not a permanent block: past the completion TTL the same key reassembles again,
// which is what lets a genuinely re-sent object through.
func TestReassembly_CompletionMemoryExpires(t *testing.T) {
	payload := buildEFPayloadR(t, 100)
	txID := canonicalID(t, payload)
	half := len(payload) / 2
	frags := [][]byte{payload[:half], payload[half:]}

	var delivered int
	b := New(16, time.Second, true, func([]byte, *frame.Frame) { delivered++ })
	b.SetCompletionTTL(50 * time.Millisecond)

	send := func() {
		for i := range frags {
			b.Observe(buildFragFrame(txID, uint32(len(payload)), uint16(i), 2, frags[i]))
		}
	}
	send()
	time.Sleep(80 * time.Millisecond)
	send()

	if delivered != 2 {
		t.Errorf("delivered %d, want 2 (completion memory must expire)", delivered)
	}
}

// TestReassembly_CompletionMemoryBounded holds the memory to the slot cap so a
// sustained fragment stream cannot grow it without limit.
func TestReassembly_CompletionMemoryBounded(t *testing.T) {
	const cap = 8
	b := New(cap, time.Second, false, func([]byte, *frame.Frame) {})

	for n := 0; n < cap*4; n++ {
		var txID [32]byte
		txID[0], txID[1] = byte(n), byte(n>>8)
		data := []byte{byte(n), 0x02, 0x03, 0x04}
		b.Observe(buildFragFrame(txID, 8, 0, 2, data))
		b.Observe(buildFragFrame(txID, 8, 1, 2, data))
	}

	b.mu.Lock()
	gotDone, gotOrder := len(b.done), len(b.doneOrder)
	b.mu.Unlock()
	if gotDone > cap || gotOrder > cap {
		t.Errorf("completion memory = %d keys / %d order entries, want <= %d", gotDone, gotOrder, cap)
	}
}
