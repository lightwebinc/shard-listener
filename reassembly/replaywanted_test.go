package reassembly

import (
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
)

// A repair coordinator that NACKed a completed object's fragments needs the
// object rebuilt inside the completion window; without the hook the
// retransmitted copies are suppressed as late and the repair never lands.
func TestReassembly_WantedReplayRebuildsCompletedObject(t *testing.T) {
	payload := buildEFPayloadR(t, 120)
	txID := canonicalID(t, payload)
	half := len(payload) / 2
	frags := [][]byte{payload[:half], payload[half:]}

	var delivered, late int
	wanted := false
	b := New(16, time.Second, true, func([]byte, *frame.Frame) { delivered++ })
	b.SetLateFragmentHook(func() { late++ })
	b.SetReplayWanted(func(key [32]byte) bool {
		if key != txID {
			t.Errorf("asked about key %x, want the object's slot key", key[:4])
		}
		return wanted
	})
	observe := func(i int) {
		b.Observe(buildFragFrame(txID, uint32(len(payload)), uint16(i), 2, frags[i]))
	}

	observe(0)
	observe(1)
	observe(0)
	observe(1)
	if delivered != 1 || late != 2 {
		t.Fatalf("unwanted copies: delivered=%d late=%d, want 1/2", delivered, late)
	}

	wanted = true
	observe(0)
	observe(1)
	if delivered != 2 {
		t.Fatalf("wanted replay delivered %d times, want 2", delivered)
	}

	// Rebuilt once, the object is remembered as done again.
	wanted = false
	observe(0)
	if delivered != 2 || late != 3 {
		t.Fatalf("after the replay: delivered=%d late=%d, want 2/3", delivered, late)
	}
}
