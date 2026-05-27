package nack_test

import (
	"testing"

	"github.com/lightwebinc/shard-listener/nack"
)

func TestNACKSize(t *testing.T) {
	if nack.NACKSize != 64 {
		t.Errorf("NACKSize = %d, want 64", nack.NACKSize)
	}
}

func TestEncodeDecodeNACK_SubtreeID(t *testing.T) {
	var sub [32]byte
	for i := range sub {
		sub[i] = byte(i + 1)
	}
	n := &nack.NACK{
		MsgType:   nack.MsgTypeNACK,
		HashKey:   0xAABBCCDDEEFF0011,
		StartSeq:  0x1122334455667788,
		EndSeq:    0x1122334455667788,
		SubtreeID: sub,
	}
	var buf [nack.NACKSize]byte
	nack.Encode(n, buf[:])

	got, err := nack.Decode(buf[:])
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.SubtreeID != sub {
		t.Errorf("SubtreeID = %x, want %x", got.SubtreeID, sub)
	}
	if got.HashKey != n.HashKey {
		t.Errorf("HashKey = 0x%016X, want 0x%016X", got.HashKey, n.HashKey)
	}
}

func TestResponseSize(t *testing.T) {
	if nack.ResponseSize != 16 {
		t.Errorf("ResponseSize = %d, want 16", nack.ResponseSize)
	}
}

func TestEncodeDecodeNACK_RoundTrip(t *testing.T) {
	n := &nack.NACK{
		MsgType:  nack.MsgTypeNACK,
		HashKey:  0xDEADBEEFCAFEBABE,
		StartSeq: 0x0102030405060708,
		EndSeq:   0x0102030405060708,
	}
	var buf [nack.NACKSize]byte
	nack.Encode(n, buf[:])

	got, err := nack.Decode(buf[:])
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.MsgType != nack.MsgTypeNACK {
		t.Errorf("MsgType = 0x%02X, want 0x%02X", got.MsgType, nack.MsgTypeNACK)
	}
	if got.HashKey != n.HashKey {
		t.Errorf("HashKey = 0x%016X, want 0x%016X", got.HashKey, n.HashKey)
	}
	if got.StartSeq != n.StartSeq {
		t.Errorf("StartSeq = 0x%016X, want 0x%016X", got.StartSeq, n.StartSeq)
	}
	if got.EndSeq != n.EndSeq {
		t.Errorf("EndSeq = 0x%016X, want 0x%016X", got.EndSeq, n.EndSeq)
	}
}

func TestEncodeDecodeNACK_StartSeqEndSeq(t *testing.T) {
	n := &nack.NACK{
		MsgType:  nack.MsgTypeNACK,
		HashKey:  0xAAAAAAAAAAAAAAAA,
		StartSeq: 100,
		EndSeq:   200,
	}
	var buf [nack.NACKSize]byte
	nack.Encode(n, buf[:])

	got, err := nack.Decode(buf[:])
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.StartSeq != n.StartSeq {
		t.Errorf("StartSeq = %d, want %d", got.StartSeq, n.StartSeq)
	}
	if got.EndSeq != n.EndSeq {
		t.Errorf("EndSeq = %d, want %d", got.EndSeq, n.EndSeq)
	}
}

func TestDecodeNACK_ErrShort(t *testing.T) {
	_, err := nack.Decode(make([]byte, nack.NACKSize-1))
	if err != nack.ErrBadNACK {
		t.Errorf("want ErrBadNACK for short buf, got %v", err)
	}
}

func TestDecodeNACK_ErrBadMagic(t *testing.T) {
	buf := make([]byte, nack.NACKSize)
	buf[0] = 0xFF
	_, err := nack.Decode(buf)
	if err != nack.ErrBadNACK {
		t.Errorf("want ErrBadNACK for bad magic, got %v", err)
	}
}

func TestDecodeNACK_ErrBadMsgType(t *testing.T) {
	var buf [nack.NACKSize]byte
	nack.Encode(&nack.NACK{MsgType: nack.MsgTypeNACK}, buf[:])
	buf[6] = 0x99
	_, err := nack.Decode(buf[:])
	if err != nack.ErrBadNACK {
		t.Errorf("want ErrBadNACK for unknown MsgType, got %v", err)
	}
}

func TestEncodeDecodeACK(t *testing.T) {
	r := &nack.Response{
		MsgType: nack.MsgTypeACK,
		Flags:   0x03, // multicast_sent | unicast_sent
		SeqNum:  0xAABBCCDDEEFF0011,
	}
	var buf [nack.ResponseSize]byte
	nack.EncodeResponse(r, buf[:])

	got, err := nack.DecodeResponse(buf[:])
	if err != nil {
		t.Fatalf("DecodeResponse: %v", err)
	}
	if got.MsgType != nack.MsgTypeACK {
		t.Errorf("MsgType = 0x%02X, want ACK", got.MsgType)
	}
	if got.Flags != 0x03 {
		t.Errorf("Flags = 0x%02X, want 0x03", got.Flags)
	}
	if got.SeqNum != r.SeqNum {
		t.Errorf("SeqNum = 0x%016X, want 0x%016X", got.SeqNum, r.SeqNum)
	}
}

func TestEncodeDecodeMISS(t *testing.T) {
	r := &nack.Response{
		MsgType: nack.MsgTypeMISS,
		Flags:   0x00,
		SeqNum:  0, // zero for MISS
	}
	var buf [nack.ResponseSize]byte
	nack.EncodeResponse(r, buf[:])

	got, err := nack.DecodeResponse(buf[:])
	if err != nil {
		t.Fatalf("DecodeResponse: %v", err)
	}
	if got.MsgType != nack.MsgTypeMISS {
		t.Errorf("MsgType = 0x%02X, want MISS", got.MsgType)
	}
	if got.SeqNum != 0 {
		t.Errorf("SeqNum = %d, want 0 for MISS", got.SeqNum)
	}
}

func TestDecodeResponse_ErrShort(t *testing.T) {
	_, err := nack.DecodeResponse(make([]byte, nack.ResponseSize-1))
	if err != nack.ErrBadResponse {
		t.Errorf("want ErrBadResponse for short buf, got %v", err)
	}
}

func TestDecodeResponse_ErrBadMagic(t *testing.T) {
	var buf [nack.ResponseSize]byte
	nack.EncodeResponse(&nack.Response{MsgType: nack.MsgTypeACK}, buf[:])
	buf[0] = 0xFF
	_, err := nack.DecodeResponse(buf[:])
	if err != nack.ErrBadResponse {
		t.Errorf("want ErrBadResponse for bad magic, got %v", err)
	}
}

func TestACKFlags_multicast_unicast(t *testing.T) {
	for _, tc := range []struct {
		flags byte
		mc    bool
		uc    bool
	}{
		{0x00, false, false},
		{0x01, true, false},
		{0x02, false, true},
		{0x03, true, true},
	} {
		r := &nack.Response{MsgType: nack.MsgTypeACK, Flags: tc.flags}
		var buf [nack.ResponseSize]byte
		nack.EncodeResponse(r, buf[:])
		got, err := nack.DecodeResponse(buf[:])
		if err != nil {
			t.Fatalf("flags=0x%02X: %v", tc.flags, err)
		}
		mc := got.Flags&0x01 != 0
		uc := got.Flags&0x02 != 0
		if mc != tc.mc || uc != tc.uc {
			t.Errorf("flags=0x%02X: mc=%v uc=%v, want mc=%v uc=%v",
				tc.flags, mc, uc, tc.mc, tc.uc)
		}
	}
}
