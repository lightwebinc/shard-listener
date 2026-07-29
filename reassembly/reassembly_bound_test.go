package reassembly

import (
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
)

func boundFrag(ver byte, origLen uint32, total uint16, seed byte) *frame.FragFrame {
	return &frame.FragFrame{
		TxID: [32]byte{seed}, HashKey: uint64(seed), SeqNum: 1,
		FragIndex: 0, FragTotal: total, OrigPayloadLen: origLen,
		OrigFrameVer: ver, FragData: []byte{0x01},
	}
}

// OrigPayloadLen is attacker-declared: a fragment claiming a ~4 GiB original
// with FragTotal 65535 must NOT open a slot (its frags array alone costs
// ~1.5 MiB; thousands of such datagrams command gigabytes). The general bound
// (64 MiB) gates V2; the V9 bound is the operator's -beef-max-object-bytes.
func TestDeclaredLengthBounds(t *testing.T) {
	opened := 0
	b := New(8, time.Second, false, func([]byte, *frame.Frame) {})
	b.SetStartedHook(func() { opened++ })
	b.SetMaxObjectBytesV9(1 << 20)

	b.Observe(boundFrag(frame.FrameVerV2, 0xFFFF0000, 65535, 1)) // ~4 GiB declared tx
	if opened != 0 {
		t.Fatalf("4 GiB declared V2 opened a slot")
	}
	b.Observe(boundFrag(frame.FrameVerV9, 2<<20, 1800, 2)) // 2 MiB > 1 MiB beef bound
	if opened != 0 {
		t.Fatalf("over-bound V9 opened a slot")
	}
	b.Observe(boundFrag(frame.FrameVerV9, 512<<10, 450, 3)) // 512 KiB — within bound
	if opened != 1 {
		t.Fatalf("in-bound V9 rejected (opened=%d)", opened)
	}
	b.Observe(boundFrag(frame.FrameVerV2, 32<<20, 30000, 4)) // 32 MiB tx — within general
	if opened != 2 {
		t.Fatalf("in-bound V2 rejected (opened=%d)", opened)
	}
}
