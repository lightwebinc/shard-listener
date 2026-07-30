package reassembly

import (
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
)

// A slot that expires holding some fragments must report the SeqNums it never
// received. Losing a trailing fragment loses the WHOLE object and no successor
// frame reveals it, so silently dropping the slot (the previous behaviour)
// discards objects the listener could have asked for.
func TestIncompleteSlotReportsMissingSeqNums(t *testing.T) {
	b := New(16, 50*time.Millisecond, false, func([]byte, *frame.Frame) {})
	b.SetGroupIdxFunc(func(*frame.FragFrame) uint32 { return 0x1234 })

	var gotHash uint64
	var gotGroup uint32
	var gotMissing []uint64
	b.SetIncompleteHook(func(hashKey uint64, groupIdx uint32, _ [32]byte, missing []uint64) {
		gotHash, gotGroup, gotMissing = hashKey, groupIdx, missing
	})

	// A 5-fragment object; deliver 0,1,2 and lose the 3,4 TAIL.
	const hk = uint64(0xAABB)
	for i := 0; i < 3; i++ {
		b.Observe(&frame.FragFrame{
			HashKey: hk, SeqNum: uint64(100 + i),
			FragIndex: uint16(i), FragTotal: 5,
			OrigPayloadLen: 50, FragData: []byte{byte(i)},
		})
	}

	time.Sleep(80 * time.Millisecond)
	b.Tick() // drives evictExpired

	if gotMissing == nil {
		t.Fatal("expired incomplete slot reported nothing — tail loss stays invisible")
	}
	if gotHash != hk {
		t.Errorf("hashKey=%x want %x", gotHash, hk)
	}
	if gotGroup != 0x1234 {
		t.Errorf("groupIdx=%x want 0x1234 — recovery would name the wrong flow", gotGroup)
	}
	// Fragment i carried SeqNum 100+i, so the missing tail is 103,104.
	want := []uint64{103, 104}
	if len(gotMissing) != len(want) {
		t.Fatalf("missing=%v want %v", gotMissing, want)
	}
	for i := range want {
		if gotMissing[i] != want[i] {
			t.Fatalf("missing=%v want %v (SeqNum interpolation is wrong; a NACK would "+
				"request a frame that was never sent)", gotMissing, want)
		}
	}
}

// A HEAD loss must interpolate backwards from a later fragment, since the slot's
// scalar seqNum belongs to whichever fragment arrived first.
func TestIncompleteSlotInterpolatesBackwards(t *testing.T) {
	b := New(16, 50*time.Millisecond, false, func([]byte, *frame.Frame) {})
	b.SetGroupIdxFunc(func(*frame.FragFrame) uint32 { return 1 })
	var gotMissing []uint64
	b.SetIncompleteHook(func(_ uint64, _ uint32, _ [32]byte, missing []uint64) {
		gotMissing = missing
	})

	// Deliver only fragment 2 (SeqNum 202) of 4; 0,1,3 are missing.
	b.Observe(&frame.FragFrame{
		HashKey: 7, SeqNum: 202, FragIndex: 2, FragTotal: 4,
		OrigPayloadLen: 40, FragData: []byte{9},
	})
	time.Sleep(80 * time.Millisecond)
	b.Tick()

	want := map[uint64]bool{200: true, 201: true, 203: true}
	if len(gotMissing) != 3 {
		t.Fatalf("missing=%v want 3 entries {200,201,203}", gotMissing)
	}
	for _, m := range gotMissing {
		if !want[m] {
			t.Errorf("missing contains %d, which was never a SeqNum of this object", m)
		}
	}
}

// A slot that received NOTHING has no anchor to interpolate from and must stay
// silent rather than invent SeqNums.
func TestEmptySlotReportsNothing(t *testing.T) {
	b := New(16, 50*time.Millisecond, false, func([]byte, *frame.Frame) {})
	called := false
	b.SetIncompleteHook(func(uint64, uint32, [32]byte, []uint64) { called = true })
	time.Sleep(80 * time.Millisecond)
	b.Tick()
	if called {
		t.Error("incomplete hook fired with no slots — would NACK invented SeqNums")
	}
}
