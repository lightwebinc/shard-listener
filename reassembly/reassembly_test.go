package reassembly

import (
	"bytes"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
)

// buildFragFrame constructs a *frame.FragFrame from raw components.
func buildFragFrame(txID [32]byte, origLen uint32, idx, total uint16, data []byte) *frame.FragFrame {
	cp := make([]byte, len(data))
	copy(cp, data)
	return &frame.FragFrame{
		TxID:           txID,
		HashKey:        0xAABBCCDDEEFF0011,
		SeqNum:         uint64(idx) + 1,
		OrigPayloadLen: origLen,
		FragIndex:      idx,
		FragTotal:      total,
		FragData:       cp,
	}
}

// txIDOf returns the SHA256d (TxID) of payload, as used for hash verification.
func txIDOf(payload []byte) [32]byte {
	first := sha256.Sum256(payload)
	return sha256.Sum256(first[:])
}

// TestReassembly_SingleFragment verifies a single-fragment frame completes
// immediately and delivers the exact payload bytes.
func TestReassembly_SingleFragment(t *testing.T) {
	payload := []byte("single-fragment-payload")
	txID := txIDOf(payload)

	var got []byte
	var gotFrame *frame.Frame
	b := New(16, time.Second, true, func(p []byte, f *frame.Frame) {
		got = p
		gotFrame = f
	})

	ff := buildFragFrame(txID, uint32(len(payload)), 0, 1, payload)
	b.Observe(ff)

	if !bytes.Equal(got, payload) {
		t.Errorf("payload mismatch: got %q, want %q", got, payload)
	}
	if gotFrame == nil {
		t.Fatal("callback not called")
	}
	if gotFrame.TxID != txID {
		t.Errorf("Frame.TxID mismatch")
	}
	if gotFrame.Version != frame.FrameVerV2 {
		t.Errorf("Frame.Version = 0x%02X, want FrameVerV2", gotFrame.Version)
	}
}

// TestReassembly_MultiFragment verifies in-order delivery of K fragments.
func TestReassembly_MultiFragment(t *testing.T) {
	payload := bytes.Repeat([]byte("abcdefgh"), 50) // 400 bytes
	txID := txIDOf(payload)
	const K = 4
	fragSize := len(payload) / K

	var got []byte
	b := New(16, time.Second, true, func(p []byte, _ *frame.Frame) {
		got = p
	})

	for i := 0; i < K; i++ {
		start := i * fragSize
		end := start + fragSize
		if i == K-1 {
			end = len(payload)
		}
		ff := buildFragFrame(txID, uint32(len(payload)), uint16(i), K, payload[start:end])
		b.Observe(ff)
	}

	if !bytes.Equal(got, payload) {
		t.Errorf("reassembled payload mismatch (len got=%d, want=%d)", len(got), len(payload))
	}
}

// TestReassembly_OutOfOrder verifies correct reassembly when fragments arrive
// in reverse order.
func TestReassembly_OutOfOrder(t *testing.T) {
	payload := bytes.Repeat([]byte("XYZW"), 100) // 400 bytes
	txID := txIDOf(payload)
	const K = 4
	fragSize := len(payload) / K

	var got []byte
	b := New(16, time.Second, true, func(p []byte, _ *frame.Frame) {
		got = p
	})

	// Deliver in reverse order.
	for i := K - 1; i >= 0; i-- {
		start := i * fragSize
		end := start + fragSize
		ff := buildFragFrame(txID, uint32(len(payload)), uint16(i), K, payload[start:end])
		b.Observe(ff)
	}

	if !bytes.Equal(got, payload) {
		t.Errorf("out-of-order reassembly failed (len=%d)", len(got))
	}
}

// TestReassembly_Duplicate verifies that duplicate fragments are ignored.
func TestReassembly_Duplicate(t *testing.T) {
	payload := []byte("two-fragment-payload-here!!")
	txID := txIDOf(payload)
	half := len(payload) / 2

	calls := 0
	b := New(16, time.Second, true, func(_ []byte, _ *frame.Frame) {
		calls++
	})

	ff0 := buildFragFrame(txID, uint32(len(payload)), 0, 2, payload[:half])
	ff1 := buildFragFrame(txID, uint32(len(payload)), 1, 2, payload[half:])

	b.Observe(ff0)
	b.Observe(ff0) // duplicate — must be ignored
	b.Observe(ff1)

	if calls != 1 {
		t.Errorf("callback called %d times, want 1", calls)
	}
}

// TestReassembly_HashMismatch verifies that a completed reassembly with a bad
// TxID is dropped and the hash-mismatch hook is called.
func TestReassembly_HashMismatch(t *testing.T) {
	payload := []byte("correct-payload")
	var badTxID [32]byte // all zeros — will not match SHA256d(payload)

	calls := 0
	mismatches := 0
	b := New(16, time.Second, true, func(_ []byte, _ *frame.Frame) {
		calls++
	})
	b.SetHashMismatchHook(func() { mismatches++ })

	ff := buildFragFrame(badTxID, uint32(len(payload)), 0, 1, payload)
	b.Observe(ff)

	if calls != 0 {
		t.Errorf("callback should not be called on hash mismatch")
	}
	if mismatches != 1 {
		t.Errorf("hash mismatch hook called %d times, want 1", mismatches)
	}
}

// TestReassembly_HashMismatch_Disabled verifies that when verifyHash=false,
// a mismatched TxID is still delivered.
func TestReassembly_HashMismatch_Disabled(t *testing.T) {
	payload := []byte("any-payload")
	var badTxID [32]byte

	calls := 0
	b := New(16, time.Second, false, func(_ []byte, _ *frame.Frame) {
		calls++
	})

	ff := buildFragFrame(badTxID, uint32(len(payload)), 0, 1, payload)
	b.Observe(ff)

	if calls != 1 {
		t.Errorf("callback called %d times, want 1 when hash verify disabled", calls)
	}
}

// TestReassembly_TTLEviction verifies that expired slots are abandoned.
func TestReassembly_TTLEviction(t *testing.T) {
	payload := []byte("ttl-test-payload!!")
	txID := txIDOf(payload)
	half := len(payload) / 2

	completed := 0
	abandoned := 0
	b := New(16, 10*time.Millisecond, true, func(_ []byte, _ *frame.Frame) {
		completed++
	})
	b.SetAbandonedHook(func() { abandoned++ })

	// Send only first fragment.
	ff0 := buildFragFrame(txID, uint32(len(payload)), 0, 2, payload[:half])
	b.Observe(ff0)
	if b.Len() != 1 {
		t.Fatalf("want 1 active slot, got %d", b.Len())
	}

	// Wait for TTL.
	time.Sleep(20 * time.Millisecond)

	// Trigger eviction via a new Observe call (lazy GC).
	var other [32]byte
	other[0] = 0xFF
	ffNew := buildFragFrame(other, 1, 0, 1, []byte("x"))
	b.Observe(ffNew) // this triggers evictExpired()

	if abandoned != 1 {
		t.Errorf("abandoned hook called %d times, want 1", abandoned)
	}
	if completed != 0 {
		t.Errorf("completed should be 0 after TTL eviction")
	}
}

// TestReassembly_MaxSlotsEviction verifies that the oldest slot is evicted
// when the buffer is full.
func TestReassembly_MaxSlotsEviction(t *testing.T) {
	abandoned := 0
	b := New(2, time.Minute, false, nil) // maxSlots=2
	b.SetAbandonedHook(func() { abandoned++ })

	var tx1, tx2, tx3 [32]byte
	tx1[0] = 0x01
	tx2[0] = 0x02
	tx3[0] = 0x03

	payload := []byte("payload-xx")
	ff1 := buildFragFrame(tx1, uint32(len(payload)), 0, 2, payload[:5])
	ff2 := buildFragFrame(tx2, uint32(len(payload)), 0, 2, payload[:5])
	ff3 := buildFragFrame(tx3, uint32(len(payload)), 0, 2, payload[:5])

	b.Observe(ff1) // slots: [tx1]
	b.Observe(ff2) // slots: [tx1, tx2]
	b.Observe(ff3) // full; tx1 evicted (FIFO); slots: [tx2, tx3]

	if abandoned != 1 {
		t.Errorf("abandoned called %d times, want 1 on overflow eviction", abandoned)
	}
	if b.Len() != 2 {
		t.Errorf("Len() = %d, want 2 after eviction", b.Len())
	}
}

// TestReassembly_StartedHook verifies the started hook fires once per new slot.
func TestReassembly_StartedHook(t *testing.T) {
	started := 0
	b := New(16, time.Second, false, nil)
	b.SetStartedHook(func() { started++ })

	var tx1, tx2 [32]byte
	tx1[0] = 0x01
	tx2[0] = 0x02

	payload := []byte("x")
	b.Observe(buildFragFrame(tx1, 1, 0, 1, payload))
	b.Observe(buildFragFrame(tx2, 1, 0, 1, payload))

	if started != 2 {
		t.Errorf("started hook called %d times, want 2", started)
	}
}

// TestReassembly_Purge verifies Purge removes all slots and calls the
// abandoned hook for each.
func TestReassembly_Purge(t *testing.T) {
	abandoned := 0
	b := New(16, time.Minute, false, nil)
	b.SetAbandonedHook(func() { abandoned++ })

	for i := 0; i < 3; i++ {
		var txID [32]byte
		txID[0] = byte(i + 1)
		ff := buildFragFrame(txID, 2, 0, 2, []byte("x")) // never complete
		b.Observe(ff)
	}

	if b.Len() != 3 {
		t.Fatalf("want 3 slots before purge, got %d", b.Len())
	}
	b.Purge()
	if b.Len() != 0 {
		t.Errorf("Len() = %d after Purge, want 0", b.Len())
	}
	if abandoned != 3 {
		t.Errorf("abandoned hook called %d times after Purge, want 3", abandoned)
	}
}

// TestReassembly_BadFragTotal_Ignored verifies that a fragment with
// FragTotal=0 is silently dropped.
func TestReassembly_BadFragTotal_Ignored(t *testing.T) {
	b := New(16, time.Second, false, nil)
	ff := &frame.FragFrame{FragTotal: 0, FragIndex: 0, OrigPayloadLen: 10, FragData: []byte("x")}
	b.Observe(ff) // must not panic
	if b.Len() != 0 {
		t.Errorf("Len() = %d, want 0 for bad FragTotal=0", b.Len())
	}
}

// TestReassembly_MetadataMismatch_Ignored verifies that a fragment with
// different FragTotal for the same TxID is ignored.
func TestReassembly_MetadataMismatch_Ignored(t *testing.T) {
	var txID [32]byte
	txID[0] = 0x01
	payload := []byte("mismatch-test!!")

	calls := 0
	b := New(16, time.Second, false, func(_ []byte, _ *frame.Frame) {
		calls++
	})

	// First fragment: FragTotal=2.
	ff0 := buildFragFrame(txID, uint32(len(payload)), 0, 2, payload[:7])
	b.Observe(ff0)

	// Second fragment: FragTotal=3 (different — should be ignored).
	ffBad := &frame.FragFrame{
		TxID:           txID,
		OrigPayloadLen: uint32(len(payload)),
		FragTotal:      3, // mismatch
		FragIndex:      1,
		FragData:       payload[7:],
	}
	b.Observe(ffBad)

	// Correct second fragment.
	ff1 := buildFragFrame(txID, uint32(len(payload)), 1, 2, payload[7:])
	b.Observe(ff1)

	if calls != 1 {
		t.Errorf("callback called %d times, want 1", calls)
	}
}

// buildFragFrameWithVer constructs a *frame.FragFrame with origFrameVer and msgType set.
func buildFragFrameWithVer(txID [32]byte, origLen uint32, idx, total uint16, data []byte, origFrameVer, msgType byte) *frame.FragFrame {
	cp := make([]byte, len(data))
	copy(cp, data)
	return &frame.FragFrame{
		TxID:           txID,
		HashKey:        0xDEADBEEFCAFEBABE,
		SeqNum:         uint64(idx) + 1,
		OrigPayloadLen: origLen,
		FragIndex:      idx,
		FragTotal:      total,
		FragData:       cp,
		OrigFrameVer:   origFrameVer,
		MsgType:        msgType,
	}
}

func TestReassembly_V4BlockCallback(t *testing.T) {
	payload := []byte("block-announce-payload-data")
	var txID [32]byte
	for i := range txID {
		txID[i] = byte(i + 1) // ContentID (BlockHash)
	}

	var gotPayload []byte
	var gotBF *frame.BlockFrame
	b := New(16, time.Second, false, nil)
	b.SetBlockCallback(func(p []byte, bf *frame.BlockFrame) {
		gotPayload = p
		gotBF = bf
	})

	ff := buildFragFrameWithVer(txID, uint32(len(payload)), 0, 1, payload, frame.FrameVerV4, frame.BlockMsgAnnounce)
	b.Observe(ff)

	if gotBF == nil {
		t.Fatal("BlockCallback not called")
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Errorf("payload mismatch: got %q, want %q", gotPayload, payload)
	}
	if gotBF.MsgType != frame.BlockMsgAnnounce {
		t.Errorf("MsgType: got 0x%02X, want 0x%02X", gotBF.MsgType, frame.BlockMsgAnnounce)
	}
	if gotBF.ContentID != txID {
		t.Errorf("ContentID mismatch")
	}
	if gotBF.HashKey != 0xDEADBEEFCAFEBABE {
		t.Errorf("HashKey mismatch: got %d", gotBF.HashKey)
	}
}

func TestReassembly_V5SubtreeDataCallback(t *testing.T) {
	payload := make([]byte, 128)
	for i := range payload {
		payload[i] = byte(i & 0xFF)
	}
	var subtreeID [32]byte
	for i := range subtreeID {
		subtreeID[i] = byte(i + 10) // SubtreeID (Merkle root)
	}

	var gotPayload []byte
	var gotSF *frame.SubtreeDataFrame
	b := New(16, time.Second, false, nil)
	b.SetSubtreeDataCallback(func(p []byte, sf *frame.SubtreeDataFrame) {
		gotPayload = p
		gotSF = sf
	})

	ff := buildFragFrameWithVer(subtreeID, uint32(len(payload)), 0, 1, payload, frame.FrameVerV5, frame.SubtreeMsgHashesOnly)
	b.Observe(ff)

	if gotSF == nil {
		t.Fatal("SubtreeDataCallback not called")
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Errorf("payload mismatch")
	}
	if gotSF.MsgType != frame.SubtreeMsgHashesOnly {
		t.Errorf("MsgType: got 0x%02X, want 0x%02X", gotSF.MsgType, frame.SubtreeMsgHashesOnly)
	}
	if gotSF.SubtreeID != subtreeID {
		t.Errorf("SubtreeID mismatch")
	}
	if gotSF.HashKey != 0xDEADBEEFCAFEBABE {
		t.Errorf("HashKey mismatch: got %d", gotSF.HashKey)
	}
}

func TestReassembly_V5NoSHA256dVerification(t *testing.T) {
	// For V5 frames, SHA256d verification must NOT be applied even when
	// verifyHash=true, because SubtreeID is a Merkle root, not a payload hash.
	payload := []byte("subtree-data-payload")
	var subtreeID [32]byte
	subtreeID[0] = 0xAB // deliberately not SHA256d(payload)

	called := false
	b := New(16, time.Second, true /* verifyHash=true */, nil)
	b.SetSubtreeDataCallback(func(p []byte, sf *frame.SubtreeDataFrame) {
		called = true
	})
	hashMismatch := false
	b.SetHashMismatchHook(func() { hashMismatch = true })

	ff := buildFragFrameWithVer(subtreeID, uint32(len(payload)), 0, 1, payload, frame.FrameVerV5, frame.SubtreeMsgHashesOnly)
	b.Observe(ff)

	if !called {
		t.Error("SubtreeDataCallback not called: SHA256d was incorrectly applied to V5 slot")
	}
	if hashMismatch {
		t.Error("hash_mismatch hook fired for V5 slot: SHA256d must not be applied")
	}
}

func TestReassembly_V5MultiFragment(t *testing.T) {
	chunk1 := bytes.Repeat([]byte{0xAA}, 500)
	chunk2 := bytes.Repeat([]byte{0xBB}, 300)
	var subtreeID [32]byte
	subtreeID[7] = 0xFF

	var gotPayload []byte
	b := New(16, time.Second, false, nil)
	b.SetSubtreeDataCallback(func(p []byte, sf *frame.SubtreeDataFrame) {
		gotPayload = p
	})

	origLen := uint32(len(chunk1) + len(chunk2))
	ff0 := buildFragFrameWithVer(subtreeID, origLen, 0, 2, chunk1, frame.FrameVerV5, frame.SubtreeMsgFullNodes)
	ff1 := buildFragFrameWithVer(subtreeID, origLen, 1, 2, chunk2, frame.FrameVerV5, frame.SubtreeMsgFullNodes)
	b.Observe(ff0)
	if gotPayload != nil {
		t.Error("callback should not fire after fragment 0 of 2")
	}
	b.Observe(ff1)
	if gotPayload == nil {
		t.Fatal("SubtreeDataCallback not called after all fragments")
	}
	want := append(chunk1, chunk2...)
	if !bytes.Equal(gotPayload, want) {
		t.Errorf("payload mismatch: len got=%d want=%d", len(gotPayload), len(want))
	}
}

// TestValidateFragment is a sanity check on the validation guard.
func TestValidateFragment(t *testing.T) {
	ff := &frame.FragFrame{FragTotal: 3, FragIndex: 1}
	if err := validateFragment(ff); err != nil {
		t.Errorf("valid fragment: %v", err)
	}
	ff.FragTotal = 0
	if err := validateFragment(ff); err == nil {
		t.Error("want error for FragTotal=0")
	}
	ff.FragTotal = 2
	ff.FragIndex = 2
	if err := validateFragment(ff); err == nil {
		t.Error("want error for FragIndex >= FragTotal")
	}
}
