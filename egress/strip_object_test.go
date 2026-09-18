package egress

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
)

// subtreeObject builds a real BRC-143 subtree object: merkle root, node count,
// then the ordered 32-byte node hashes. The first node is the coinbase
// placeholder, which BRC-143 defines as 32 bytes of 0xFF.
func subtreeObject(t *testing.T, nodes int) []byte {
	t.Helper()
	obj := make([]byte, objfmt.SubtreeHeaderSize+nodes*32)
	for i := 0; i < 32; i++ {
		obj[i] = byte(0xA0 + i) // a recognisable, non-zero root
	}
	binary.BigEndian.PutUint64(obj[32:40], uint64(nodes))
	for n := 0; n < nodes; n++ {
		off := objfmt.SubtreeHeaderSize + n*32
		for i := 0; i < 32; i++ {
			if n == 0 {
				obj[off+i] = 0xFF
			} else {
				obj[off+i] = byte(n*7 + i)
			}
		}
	}
	return obj
}

// TestSendSubtreeData_StripDeliversBRC143Object pins the stripped subtree
// egress to a byte-for-byte BRC-143 round trip. A subtree enters the fabric as
// a BRC-143 object and is re-encoded as a BRC-132 frame, whose payload omits
// the merkle root (BRC-132 keeps it in the frame header) and adds aggregate
// fields and a conflict tail. Stripping must rebuild the object, not forward
// that payload.
func TestSendSubtreeData_StripDeliversBRC143Object(t *testing.T) {
	addr, pc, cleanup := newUDPSink(t)
	defer cleanup()
	s, err := New(addr, "udp", true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	want := subtreeObject(t, 4)
	raw, err := objfmt.MulticastBytes(objfmt.ClassSubtree, want)
	if err != nil {
		t.Fatalf("frame the subtree the way the proxy does: %v", err)
	}
	sf, err := frame.DecodeSubtreeData(raw)
	if err != nil {
		t.Fatalf("decode the frame the way the listener does: %v", err)
	}

	// The defect this test exists for: the frame payload is not the object.
	if bytes.Equal(sf.Payload, want) {
		t.Fatal("precondition: BRC-132 payload should differ from the BRC-143 object")
	}
	if bytes.Contains(sf.Payload, want[:32]) {
		t.Fatal("precondition: BRC-132 payload should not carry the merkle root")
	}

	if err := s.SendSubtreeData(raw, sf); err != nil {
		t.Fatalf("SendSubtreeData: %v", err)
	}
	buf := make([]byte, 4096)
	_ = pc.SetReadDeadline(time.Now().Add(time.Second))
	n, _, err := pc.ReadFrom(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	got := buf[:n]
	if !bytes.Equal(got, want) {
		t.Fatalf("stripped subtree is not the BRC-143 object\n got  %x\n want %x", got, want)
	}
	if size, err := objfmt.SubtreeSize(got); err != nil || size != len(got) {
		t.Fatalf("delivered bytes must parse as one whole BRC-143 object: size=%d err=%v", size, err)
	}
}

// TestSendBlock_StripRefusesFrameWithoutWholeBlock pins the block side to the
// same helper: a block-control frame whose payload is not a whole BRC-144
// object cannot be stripped into one, so it is refused rather than forwarded
// as bytes a BRC-144 reader would mis-parse.
func TestSendBlock_StripRefusesFrameWithoutWholeBlock(t *testing.T) {
	addr, _, cleanup := newUDPSink(t)
	defer cleanup()
	s, err := New(addr, "udp", true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	// A block-control frame carrying only an 80-byte header: no counts, no
	// roots, no coinbase, so not a BRC-144 object.
	f := &frame.BlockFrame{MsgType: frame.BlockMsgAnnounce, Payload: make([]byte, 80)}
	raw := make([]byte, frame.HeaderSize+len(f.Payload))
	if _, err := frame.EncodeBlock(f, raw); err != nil {
		t.Skipf("cannot build a block frame with this codec version: %v", err)
	}
	bf, err := frame.DecodeBlock(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := s.SendBlock(raw, bf); err == nil {
		t.Fatal("a frame without a whole block must be refused, not forwarded")
	}
}
