package reassembly

import (
	"bytes"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
)

// A short interior fragment breaks the BRC-130 offset grid: concatenating in
// index order would place every later byte at the wrong offset and yield a
// payload of the wrong length. With -verify-payload-hash off (the default)
// nothing downstream catches it, so reassembly must.
func TestReassembly_ShortInteriorFragmentDropsObject(t *testing.T) {
	payload := bytes.Repeat([]byte{0xA5}, 90) // 3 × 30
	txID := txIDOf(payload)

	var delivered [][]byte
	var bad int
	b := New(16, time.Second, false, func(p []byte, _ *frame.Frame) {
		delivered = append(delivered, append([]byte(nil), p...))
	})
	b.SetBadFragmentHook(func() { bad++ })

	b.Observe(buildFragFrame(txID, 90, 0, 3, payload[0:30]))
	b.Observe(buildFragFrame(txID, 90, 1, 3, payload[30:50])) // short: 20, want 30
	b.Observe(buildFragFrame(txID, 90, 2, 3, payload[60:90]))

	if len(delivered) != 0 {
		t.Errorf("delivered %d payloads (len %d); a mis-sized object must not reach the callback",
			len(delivered), len(delivered[0]))
	}
	if bad != 1 {
		t.Errorf("bad-fragment hook fired %d times, want 1", bad)
	}
}

// The same object, correctly fragmented, still completes — the check must not
// cost a legitimate reassembly.
func TestReassembly_UniformFragmentsStillComplete(t *testing.T) {
	payload := bytes.Repeat([]byte{0xA5}, 90)
	txID := txIDOf(payload)

	var got []byte
	var bad int
	b := New(16, time.Second, false, func(p []byte, _ *frame.Frame) { got = p })
	b.SetBadFragmentHook(func() { bad++ })

	b.Observe(buildFragFrame(txID, 90, 1, 3, payload[30:60]))
	b.Observe(buildFragFrame(txID, 90, 2, 3, payload[60:90]))
	b.Observe(buildFragFrame(txID, 90, 0, 3, payload[0:30]))

	if !bytes.Equal(got, payload) {
		t.Errorf("payload = %d bytes, want %d", len(got), len(payload))
	}
	if bad != 0 {
		t.Errorf("bad-fragment hook fired %d times on a valid object", bad)
	}
}

// Validation is order-independent: the final fragment implies the same
// fragSize as the interior ones, so a short interior is caught even when the
// tail arrives first.
func TestReassembly_FragLenCheckIsOrderIndependent(t *testing.T) {
	payload := bytes.Repeat([]byte{0x5A}, 100) // 40 + 40 + 20
	txID := txIDOf(payload)

	var calls, bad int
	b := New(16, time.Second, false, func(_ []byte, _ *frame.Frame) { calls++ })
	b.SetBadFragmentHook(func() { bad++ })

	b.Observe(buildFragFrame(txID, 100, 2, 3, payload[80:100])) // final, implies 40
	b.Observe(buildFragFrame(txID, 100, 0, 3, payload[0:35]))   // short interior
	if bad != 1 || calls != 0 {
		t.Fatalf("bad=%d calls=%d, want bad=1 calls=0", bad, calls)
	}
}

// A final fragment that does not complete the declared length is equally
// unreassemblable (its remainder must divide across the interior fragments).
func TestReassembly_ShortFinalFragmentDropsObject(t *testing.T) {
	payload := bytes.Repeat([]byte{0x11}, 100)
	txID := txIDOf(payload)

	var calls, bad int
	b := New(16, time.Second, false, func(_ []byte, _ *frame.Frame) { calls++ })
	b.SetBadFragmentHook(func() { bad++ })

	b.Observe(buildFragFrame(txID, 100, 0, 3, payload[0:40]))
	b.Observe(buildFragFrame(txID, 100, 1, 3, payload[40:80]))
	b.Observe(buildFragFrame(txID, 100, 2, 3, payload[80:95])) // 15, want 20
	if bad != 1 || calls != 0 {
		t.Fatalf("bad=%d calls=%d, want bad=1 calls=0", bad, calls)
	}
}

// A single-fragment object must carry the whole declared payload.
func TestReassembly_SingleFragmentMustMatchDeclaredLength(t *testing.T) {
	payload := bytes.Repeat([]byte{0x22}, 50)
	txID := txIDOf(payload)

	var calls, bad int
	b := New(16, time.Second, false, func(_ []byte, _ *frame.Frame) { calls++ })
	b.SetBadFragmentHook(func() { bad++ })

	b.Observe(buildFragFrame(txID, 50, 0, 1, payload[:40]))
	if bad != 1 || calls != 0 {
		t.Fatalf("bad=%d calls=%d, want bad=1 calls=0", bad, calls)
	}

	b.Observe(buildFragFrame(txID, 50, 0, 1, payload))
	if calls != 1 {
		t.Fatalf("calls=%d, want 1 after a correctly sized single fragment", calls)
	}
}
