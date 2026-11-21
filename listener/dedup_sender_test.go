package listener

import (
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/bundle"
	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/shard-listener/dedup"
	"github.com/lightwebinc/shard-listener/filter"
)

// Two senders' flows share (group, subtree) and, because SeqNum counts per
// flow, routinely share a SeqNum too. The egress dedup key must carry the
// flow's HashKey: without it the second sender's frame is suppressed as a
// duplicate of the first's (and still observed by the gap tracker, so the
// loss is never even NACKed). Each test sends sender A, sender B at the same
// SeqNum, then sender A's retransmit, and expects exactly A and B downstream.

const (
	senderAHashKey = 0xA1A1A1A1A1A1A1A1
	senderBHashKey = 0xB2B2B2B2B2B2B2B2
)

// collect drains ch until it has been quiet for the settle window.
func collect(ch <-chan []byte, settle time.Duration) [][]byte {
	var out [][]byte
	for {
		select {
		case d := <-ch:
			out = append(out, d)
		case <-time.After(settle):
			return out
		}
	}
}

func buildFlowFrame(t *testing.T, txid [32]byte, payload []byte, hashKey, seq uint64) []byte {
	t.Helper()
	f := &frame.Frame{
		Version: frame.FrameVerV2,
		TxID:    txid,
		HashKey: hashKey,
		SeqNum:  seq,
		Payload: payload,
	}
	buf := make([]byte, frame.HeaderSize+len(payload))
	n, err := frame.Encode(f, buf)
	if err != nil {
		t.Fatal(err)
	}
	return buf[:n]
}

func TestProcessFrame_EgressDedup_DistinctSendersBothForwarded(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	w := newWorker(t, addr, filter.New(nil, nil, nil, nil)) // ShardBits 2
	w.SetEgressDedup(dedup.New(64, time.Second))

	// Different TxIDs in the same group (top two bits 00 → group 0), zero
	// subtree, both at SeqNum 7.
	a := buildFlowFrame(t, [32]byte{0x01}, []byte("sender-a"), senderAHashKey, 7)
	b := buildFlowFrame(t, [32]byte{0x02}, []byte("sender-b"), senderBHashKey, 7)
	w.processFrame(a)
	w.processFrame(b)
	w.processFrame(a) // sender A's retransmit

	got := map[string]int{}
	for _, d := range collect(ch, 300*time.Millisecond) {
		f, err := frame.Decode(d)
		if err != nil {
			t.Fatalf("decode forwarded frame: %v", err)
		}
		got[string(f.Payload)]++
	}
	if got["sender-a"] != 1 || got["sender-b"] != 1 || len(got) != 2 {
		t.Fatalf("forwarded %v, want sender-a once and sender-b once (B must not be taken for A's duplicate, A's retransmit must be)", got)
	}
}

func TestDeliverBundle_EgressDedup_DistinctSendersBothDelivered(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	w := newWorker(t, addr, filter.New(nil, nil, nil, nil)) // ShardBits 2
	w.SetEgressDedup(dedup.New(64, time.Second))

	mk := func(hashKey uint64, tx string) []byte {
		b := &bundle.Bundle{
			Flags:     bundle.FlagTxIDsPresent,
			HashKey:   hashKey,
			SeqNum:    3,
			GroupIdx:  0,
			ShardBits: 2, // the listener's generation: no re-bucketing
			Members:   []bundle.Member{{TxID: [32]byte{0x01}, Tx: []byte(tx)}},
		}
		raw, err := b.Encode()
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	a := mk(senderAHashKey, "bundle-a")
	b := mk(senderBHashKey, "bundle-b")
	w.processFrame(a)
	w.processFrame(b)
	w.processFrame(a) // sender A's retransmit

	got := map[string]int{}
	for _, d := range collect(ch, 300*time.Millisecond) {
		f, err := frame.Decode(d)
		if err != nil {
			t.Fatalf("decode decoalesced member: %v", err)
		}
		got[string(f.Payload)]++
	}
	if got["bundle-a"] != 1 || got["bundle-b"] != 1 || len(got) != 2 {
		t.Fatalf("delivered %v, want bundle-a once and bundle-b once", got)
	}
}

func TestDeliverBeef_EgressDedup_DistinctSendersBothForwarded(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	w := newWorker(t, addr, filter.New(nil, nil, nil, nil))
	pe, err := shard.NewPlane(0xFF05, shard.DefaultGroupID, 0, shard.DomainBEEF)
	if err != nil {
		t.Fatal(err)
	}
	w.SetBEEF(pe, nil, nil, false) // admit every topic and version
	w.SetEgressDedup(dedup.New(64, time.Second))

	topic := objfmt.TopicID("tm_dedup")
	mk := func(hashKey uint64, obj []byte) []byte {
		bf := &frame.BEEFFrame{
			ContentID: objfmt.ContentID(obj),
			HashKey:   hashKey,
			SeqNum:    5,
			TopicID:   topic,
			Payload:   obj,
		}
		buf := make([]byte, frame.HeaderSize+len(obj))
		n, err := frame.EncodeBEEF(bf, buf)
		if err != nil {
			t.Fatal(err)
		}
		return buf[:n]
	}
	objA := []byte{0x01, 0x00, 0xBE, 0xEF, 0xA1}
	objB := []byte{0x01, 0x00, 0xBE, 0xEF, 0xB2}
	a := mk(senderAHashKey, objA)
	b := mk(senderBHashKey, objB)
	w.processFrame(a)
	w.processFrame(b)
	w.processFrame(a) // sender A's retransmit

	got := map[[32]byte]int{}
	for _, d := range collect(ch, 300*time.Millisecond) {
		bf, err := frame.DecodeBEEF(d)
		if err != nil {
			t.Fatalf("decode forwarded BEEF frame: %v", err)
		}
		got[bf.ContentID]++
	}
	if got[objfmt.ContentID(objA)] != 1 || got[objfmt.ContentID(objB)] != 1 || len(got) != 2 {
		t.Fatalf("forwarded %d distinct objects (%v), want object A once and object B once", len(got), got)
	}
}
