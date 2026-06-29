package listener

import (
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/bundle"
	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-listener/filter"
)

// A BRC-142 bundle is edge-decoalesced into individual BRC-124 frames, each
// forwarded downstream with a re-stamped monotonic egress SeqNum.
func TestProcessBundle_DecoalescesMembers(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	w := newWorker(t, addr, filter.New(nil, nil, nil, nil))

	b := &bundle.Bundle{
		Flags:     bundle.FlagTxIDsPresent,
		HashKey:   0x1122334455667788,
		SeqNum:    1,
		GroupIdx:  0,
		ShardBits: 2,
		Members: []bundle.Member{
			{TxID: [32]byte{0xAA}, Tx: []byte("tx-one")},
			{TxID: [32]byte{0xBB}, Tx: []byte("tx-two")},
		},
	}
	raw, err := b.Encode()
	if err != nil {
		t.Fatal(err)
	}
	w.processFrame(raw)

	// Two decoalesced members, matched by payload (UDP order not guaranteed).
	seqByPayload := map[string]uint64{}
	for i := 0; i < 2; i++ {
		select {
		case d := <-ch:
			f, derr := frame.Decode(d)
			if derr != nil {
				t.Fatalf("decode forwarded member: %v", derr)
			}
			if f.Version != frame.FrameVerV2 {
				t.Errorf("member FrameVer = 0x%02X, want 0x02", f.Version)
			}
			if f.HashKey != b.HashKey {
				t.Errorf("member HashKey = 0x%016X, want bundle flow 0x%016X", f.HashKey, b.HashKey)
			}
			seqByPayload[string(f.Payload)] = f.SeqNum
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for member %d", i)
		}
	}

	if seqByPayload["tx-one"] != 1 {
		t.Errorf("tx-one egress SeqNum = %d, want 1", seqByPayload["tx-one"])
	}
	if seqByPayload["tx-two"] != 2 {
		t.Errorf("tx-two egress SeqNum = %d, want 2", seqByPayload["tx-two"])
	}
}

// A bundle built at a coarser ShardBits than the listener's generation is
// re-bucketed to the local ShardBits before delivery (BRC-142 §Re-bucketing):
// every member is delivered (none dropped) and re-stamped onto a fresh local
// child flow (its HashKey is no longer the parent bundle's), rather than being
// delivered at its original generation.
func TestProcessBundle_RebucketsCoarserGeneration(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	w := newWorker(t, addr, filter.New(nil, nil, nil, nil)) // engine ShardBits=2

	b := &bundle.Bundle{
		HashKey:   0xABCDEF0123456789,
		SeqNum:    1,
		GroupIdx:  0,
		ShardBits: 1, // coarser than the listener's 2 → must re-bucket
		Members: []bundle.Member{
			{Tx: []byte("rb-tx-one")},
			{Tx: []byte("rb-tx-two")},
			{Tx: []byte("rb-tx-three")},
		},
	}
	raw, err := b.Encode()
	if err != nil {
		t.Fatal(err)
	}
	w.processFrame(raw)

	got := map[string]uint64{} // payload -> delivered HashKey
	for i := 0; i < 3; i++ {
		select {
		case d := <-ch:
			f, derr := frame.Decode(d)
			if derr != nil {
				t.Fatalf("decode re-bucketed member: %v", derr)
			}
			got[string(f.Payload)] = f.HashKey
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for re-bucketed member %d (got %d so far)", i, len(got))
		}
	}
	if len(got) != 3 {
		t.Fatalf("delivered %d distinct members, want 3 (re-bucket must not drop members)", len(got))
	}
	for p, hk := range got {
		if hk == b.HashKey {
			t.Errorf("member %q kept the parent HashKey 0x%016X — was not re-bucketed onto a local flow", p, b.HashKey)
		}
	}
}
