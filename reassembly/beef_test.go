package reassembly

import (
	"bytes"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
)

// TestReassembly_V9BeefCallback proves OrigFrameVer 0x09 fragments complete
// through the BEEF callback with FrameVer/TopicID preserved — not re-encoded
// as V2 down the tx path — and that the verifyHash check is exactly the
// spec's ContentID verification (SHA-256d of the object).
func TestReassembly_V9BeefCallback(t *testing.T) {
	payload := []byte{0x01, 0x00, 0xBE, 0xEF, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE}
	first := sha256.Sum256(payload)
	contentID := sha256.Sum256(first[:]) // SHA-256d — the BRC-148 ContentID

	var topicID [32]byte
	for i := range topicID {
		topicID[i] = byte(0x80 + i)
	}

	var gotBF *frame.BEEFFrame
	b := New(16, time.Second, true, nil) // verifyHash on: ContentID check applies
	b.SetBEEFCallback(func(_ []byte, bf *frame.BEEFFrame) { gotBF = bf })

	half := len(payload) / 2
	for i, data := range [][]byte{payload[:half], payload[half:]} {
		ff := buildFragFrameWithVer(contentID, uint32(len(payload)), uint16(i), 2, data, frame.FrameVerV9, 0)
		ff.SubtreeID = topicID // TopicID rides the SubtreeID slot
		b.Observe(ff)
	}

	if gotBF == nil {
		t.Fatal("BEEF callback not invoked")
	}
	if gotBF.ContentID != contentID || gotBF.TopicID != topicID {
		t.Error("ContentID/TopicID not preserved through reassembly")
	}
	if !bytes.Equal(gotBF.Payload, payload) {
		t.Error("payload not reassembled verbatim")
	}
	if gotBF.HashKey == 0 || gotBF.SeqNum == 0 {
		t.Error("flow metadata lost")
	}
}

// TestReassembly_V9HashMismatchDropped proves a corrupted object (ContentID
// no longer SHA-256d of the bytes) is dropped, not delivered.
func TestReassembly_V9HashMismatchDropped(t *testing.T) {
	payload := []byte{0x01, 0x00, 0xBE, 0xEF, 0x01, 0x02}
	var wrongID [32]byte // not the SHA-256d of payload
	wrongID[0] = 0x99

	delivered := false
	mismatches := 0
	b := New(16, time.Second, true, nil)
	b.SetBEEFCallback(func(_ []byte, _ *frame.BEEFFrame) { delivered = true })
	b.SetHashMismatchHook(func() { mismatches++ })

	ff := buildFragFrameWithVer(wrongID, uint32(len(payload)), 0, 1, payload, frame.FrameVerV9, 0)
	b.Observe(ff)

	if delivered {
		t.Fatal("corrupted BEEF object delivered")
	}
	if mismatches != 1 {
		t.Fatalf("hash mismatch hook fired %d times, want 1", mismatches)
	}
}
