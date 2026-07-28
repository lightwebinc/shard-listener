package reassembly

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
)

// TestReassembly_V9SiblingTopicsDoNotCollide is the B2 regression: two BEEF
// objects with the SAME ContentID but different TopicIDs (sibling emissions of
// one object, or two single-topic submissions of it) must reassemble into
// SEPARATE slots and deliver BOTH. A bare-ContentID slot key collapses them
// and silently drops one.
func TestReassembly_V9SiblingTopicsDoNotCollide(t *testing.T) {
	payload := []byte{0x01, 0x00, 0xBE, 0xEF, 0xAA, 0xBB, 0xCC, 0xDD}
	first := sha256.Sum256(payload)
	contentID := sha256.Sum256(first[:])

	var topicA, topicB [32]byte
	for i := range topicA {
		topicA[i] = byte(0x10 + i)
		topicB[i] = byte(0x90 + i)
	}

	var got []frame.BEEFFrame
	b := New(16, time.Second, true, nil)
	b.SetBEEFCallback(func(_ []byte, bf *frame.BEEFFrame) { got = append(got, *bf) })

	half := len(payload) / 2
	for _, topic := range [][32]byte{topicA, topicB} {
		for i, data := range [][]byte{payload[:half], payload[half:]} {
			ff := buildFragFrameWithVer(contentID, uint32(len(payload)), uint16(i), 2, data, frame.FrameVerV9, 0)
			ff.SubtreeID = topic
			b.Observe(ff)
		}
	}

	if len(got) != 2 {
		t.Fatalf("delivered %d objects, want 2 (sibling topics collapsed into one slot)", len(got))
	}
	if got[0].TopicID == got[1].TopicID {
		t.Fatal("both deliveries carry the same TopicID")
	}
	for _, bf := range got {
		if bf.ContentID != contentID {
			t.Error("ContentID not preserved")
		}
	}
}

// TestReassembly_NonBEEFKeyUnchanged guards that the pair key applies only to
// V9 — other classes still key on the offset-8 field alone.
func TestReassembly_NonBEEFKeyUnchanged(t *testing.T) {
	var id, subA, subB [32]byte
	id[0], subA[0], subB[0] = 0x11, 0x22, 0x33
	a := buildFragFrameWithVer(id, 4, 0, 1, []byte{1, 2, 3, 4}, frame.FrameVerV5, 0)
	a.SubtreeID = subA
	if slotKey(a) != id {
		t.Fatal("V5 must key on the offset-8 field alone")
	}
	b := buildFragFrameWithVer(id, 4, 0, 1, []byte{1, 2, 3, 4}, frame.FrameVerV9, 0)
	b.SubtreeID = subB
	if slotKey(b) == id {
		t.Fatal("V9 must key on the (ContentID, TopicID) pair")
	}
}
