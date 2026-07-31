package listener

import (
	"errors"
	"testing"

	"github.com/lightwebinc/shard-common/frame"

	"github.com/lightwebinc/shard-listener/filter"
)

// captureHeaderSink records every BRC-135 frame offered to the per-consumer
// header seam. It implements only egress.HeaderSink — a sink that wants headers
// need not be a full EgressSink.
type captureHeaderSink struct {
	raws     [][]byte
	payloads [][]byte
	txids    [][32]byte
	seqNums  []uint64
	err      error
}

func (c *captureHeaderSink) SendHeader(raw []byte, hf *frame.Frame) error {
	// Copy: the seam explicitly does not permit retaining either buffer, so a
	// test that aliased them would pass while a real sink corrupted data.
	c.raws = append(c.raws, append([]byte(nil), raw...))
	c.payloads = append(c.payloads, append([]byte(nil), hf.Payload...))
	c.txids = append(c.txids, hf.TxID)
	c.seqNums = append(c.seqNums, hf.SeqNum)
	return c.err
}

// TestHeaderFanout_ReceivesDecodedHeader proves the per-consumer seam is fed
// from a block announce with the decoded view a class router needs: the block
// hash in TxID and the 80-byte block header as Payload.
func TestHeaderFanout_ReceivesDecodedHeader(t *testing.T) {
	mainAddr, _, mainCleanup := newSink(t)
	defer mainCleanup()

	w := newWorker(t, mainAddr, filter.New(nil, nil, nil, nil))
	const emitterHashKey uint64 = 0x0102030405060708
	w.SetHeaderEmitterIdentity(emitterHashKey)

	cap := &captureHeaderSink{}
	w.SetHeaderFanout(cap)

	w.processBlockFrame(buildBlockAnnounceFrame(t, 0xAA, 0xDEADBEEF, 42))

	if len(cap.raws) != 1 {
		t.Fatalf("header fanout got %d frames, want 1", len(cap.raws))
	}
	if len(cap.raws[0]) != frame.BlockHeaderFrameSize {
		t.Errorf("raw len = %d, want %d", len(cap.raws[0]), frame.BlockHeaderFrameSize)
	}
	if !frame.IsBlockHeaderFrame(cap.raws[0]) {
		t.Errorf("raw is not a BRC-135 frame (FrameVer=0x%02X)", cap.raws[0][6])
	}
	if len(cap.payloads[0]) != frame.BlockHeaderSize {
		t.Fatalf("payload len = %d, want %d", len(cap.payloads[0]), frame.BlockHeaderSize)
	}
	if cap.payloads[0][0] != 0x01 {
		t.Errorf("block header version byte = 0x%02X, want 0x01", cap.payloads[0][0])
	}
	if cap.txids[0][0] != 0xAA {
		t.Errorf("TxID[0] (block hash) = 0x%02X, want 0xAA", cap.txids[0][0])
	}
	if cap.seqNums[0] != 1 {
		t.Errorf("SeqNum = %d, want 1 (first emission)", cap.seqNums[0])
	}
}

// TestHeaderFanout_WithoutNodeGlobalEgress is the case that motivates D8: a
// commercial edge configures NO -header-egress-addr and NO multicast header
// egress, yet consumers must still be able to elect the lane. Before the seam
// existed the emit site was gated on those two senders, so this produced
// nothing at all.
func TestHeaderFanout_WithoutNodeGlobalEgress(t *testing.T) {
	mainAddr, _, mainCleanup := newSink(t)
	defer mainCleanup()

	w := newWorker(t, mainAddr, filter.New(nil, nil, nil, nil))
	if w.headerEgr != nil || w.headerMCastEgr != nil {
		t.Fatal("test precondition: node-global header egress must be unset")
	}

	cap := &captureHeaderSink{}
	w.SetHeaderFanout(cap)
	w.processBlockFrame(buildBlockAnnounceFrame(t, 0xC1, 1, 1))

	if len(cap.raws) != 1 {
		t.Fatalf("header fanout got %d frames, want 1 with no node-global egress configured", len(cap.raws))
	}
}

// TestHeaderFanout_SkipsCoinbase pins that only BlockAnnounce yields a header;
// a BRC-133 coinbase frame carries no block header to extract.
func TestHeaderFanout_SkipsCoinbase(t *testing.T) {
	mainAddr, _, mainCleanup := newSink(t)
	defer mainCleanup()

	w := newWorker(t, mainAddr, filter.New(nil, nil, nil, nil))
	cap := &captureHeaderSink{}
	w.SetHeaderFanout(cap)

	w.processBlockFrame(buildBlockCoinbaseFrame(t, 0xBB))

	if len(cap.raws) != 0 {
		t.Fatalf("coinbase produced %d header emissions, want 0", len(cap.raws))
	}
}

// TestHeaderFanout_SeqNumMonotonic guards the per-emitter counter: BRC-135
// consumers track gaps on (HashKey, SeqNum), so a repeated or reset SeqNum
// would read downstream as loss.
func TestHeaderFanout_SeqNumMonotonic(t *testing.T) {
	mainAddr, _, mainCleanup := newSink(t)
	defer mainCleanup()

	w := newWorker(t, mainAddr, filter.New(nil, nil, nil, nil))
	w.SetHeaderEmitterIdentity(0x99)
	cap := &captureHeaderSink{}
	w.SetHeaderFanout(cap)

	for i := 0; i < 3; i++ {
		w.processBlockFrame(buildBlockAnnounceFrame(t, byte(0xA0+i), 7, uint64(i+1)))
	}
	want := []uint64{1, 2, 3}
	if len(cap.seqNums) != len(want) {
		t.Fatalf("got %d emissions, want %d", len(cap.seqNums), len(want))
	}
	for i, s := range cap.seqNums {
		if s != want[i] {
			t.Errorf("emission %d: SeqNum = %d, want %d", i, s, want[i])
		}
	}
}

// TestHeaderFanout_BlockedByPoWGate is the security property the lane depends
// on: headers are derived downstream of blockGateAllows, so a consumer is never
// delivered a header from an announce the edge rejected.
func TestHeaderFanout_BlockedByPoWGate(t *testing.T) {
	mainAddr, _, mainCleanup := newSink(t)
	defer mainCleanup()

	w := newWorker(t, mainAddr, filter.New(nil, nil, nil, nil))
	// The announce helper builds an all-but-empty 80-byte header, whose nBits
	// is zero and therefore expands to no valid target — it cannot pass.
	w.SetBlockPoW(true, 0, nil)

	cap := &captureHeaderSink{}
	w.SetHeaderFanout(cap)
	w.processBlockFrame(buildBlockAnnounceFrame(t, 0xAA, 1, 1))

	if len(cap.raws) != 0 {
		t.Fatalf("a PoW-rejected announce produced %d header emissions, want 0", len(cap.raws))
	}
}

// TestHeaderFanout_SendErrorDoesNotPanic keeps a failing consumer lane from
// taking down the block path.
func TestHeaderFanout_SendErrorDoesNotPanic(t *testing.T) {
	mainAddr, _, mainCleanup := newSink(t)
	defer mainCleanup()

	w := newWorker(t, mainAddr, filter.New(nil, nil, nil, nil))
	cap := &captureHeaderSink{err: errors.New("lane down")}
	w.SetHeaderFanout(cap)

	w.processBlockFrame(buildBlockAnnounceFrame(t, 0xAA, 1, 1))

	if len(cap.raws) != 1 {
		t.Fatalf("got %d emissions, want 1 (error path still delivers the attempt)", len(cap.raws))
	}
}
