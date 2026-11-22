package listener

import (
	"bytes"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/shard"
	"golang.org/x/sys/unix"

	"github.com/lightwebinc/shard-listener/egress"
	"github.com/lightwebinc/shard-listener/filter"
	"github.com/lightwebinc/shard-listener/nack"
	"github.com/lightwebinc/shard-listener/reassembly"
)

// A fragmented object is observed again when it completes, at the SeqNum of
// the fragment that opened its slot, after its later fragments have moved the
// flow on. When that SeqNum is at or below the restart threshold and the flow
// is past it, the observation is the same object, not a restarted proxy, even
// though it carries the emitter's address. Read as a restart, it reset the
// flow to that SeqNum, and the completing fragment then read as a jump that
// registered every fragment in between as lost.
func TestReassembledEarlyObjectDoesNotRestartFlow(t *testing.T) {
	addr, _, cleanup := newSink(t)
	defer cleanup()
	iface := loopbackIface(t)
	egr, err := egress.New(addr, "udp", false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = egr.Close() })
	tr := nack.New(nack.TrackerConfig{GapTTL: time.Minute}, nil, iface, nil, nil)
	eng := shard.New(0xFF05, shard.DefaultGroupID, 2)
	w := New(0, iface, 9999, nil, eng, filter.New(nil, nil, nil, nil), egr, nil, tr, nil, false)
	w.SetReassemblyBuffer(reassembly.New(reassembly.DefaultMaxSlots, time.Minute, false, w.DeliverReassembled))

	emitter := &unix.SockaddrInet6{Addr: [16]byte{0x20, 0x01, 0x0d, 0xb8, 15: 0x10}}
	const hashKey = uint64(0x1234_5678_9ABC_DEF0)
	whole := func(seq uint64) {
		f := &frame.Frame{Version: frame.FrameVerV2, TxID: [32]byte{1, byte(seq)}, HashKey: hashKey, SeqNum: seq, Payload: []byte("tx")}
		buf := make([]byte, frame.HeaderSize+len(f.Payload))
		n, err := frame.Encode(f, buf)
		if err != nil {
			t.Fatal(err)
		}
		w.handleDatagram(buf[:n], emitter, false, nil)
	}

	for s := uint64(1); s <= 49; s++ {
		whole(s)
	}
	// One transaction in 101 fragments, SeqNums 50..150: it completes on the
	// last one, after the flow passed the threshold (100).
	const fragSize, total = 64, 101
	payload := bytes.Repeat([]byte{0xAB}, fragSize*total)
	txid := [32]byte{2, 0xFF}
	for i := range total {
		buf := make([]byte, frame.HeaderSizeV3+fragSize)
		n, err := frame.EncodeFragment(buf, txid, [32]byte{}, hashKey, uint64(50+i), uint32(len(payload)),
			uint16(i), total, frame.FrameVerV2, payload[i*fragSize:(i+1)*fragSize])
		if err != nil {
			t.Fatal(err)
		}
		w.handleDatagram(buf[:n], emitter, false, nil)
	}
	if g := tr.PendingGaps(); g != 0 {
		t.Fatalf("after the object completed: PendingGaps = %d, want 0 (the object read as a restart)", g)
	}
	whole(151)
	if g := tr.PendingGaps(); g != 0 {
		t.Fatalf("after the next frame: PendingGaps = %d, want 0", g)
	}
}
