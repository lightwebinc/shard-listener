package fanout_test

import (
	"errors"
	"testing"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/shard-listener/fanout"
)

// headerRecSink is a header-capable consumer sink: a full EgressSink that also
// implements egress.HeaderSink.
type headerRecSink struct {
	recSink
	headers int
	err     error
}

func (h *headerRecSink) SendHeader(_ []byte, _ *frame.Frame) error {
	h.headers++
	return h.err
}

func brc135Frame(t *testing.T, blockHashByte byte, seqNum uint64) ([]byte, *frame.Frame) {
	t.Helper()
	var blockHash [32]byte
	blockHash[0] = blockHashByte
	hdr80 := make([]byte, frame.BlockHeaderSize)
	hdr80[0] = 0x01
	buf := make([]byte, frame.BlockHeaderFrameSize)
	if _, err := frame.EncodeBlockHeader(blockHash, 0xABCD, seqNum, hdr80, buf); err != nil {
		t.Fatal(err)
	}
	hf, err := frame.DecodeBlockHeader(buf)
	if err != nil {
		t.Fatal(err)
	}
	return buf, hf
}

// TestSendHeader_BroadcastsToHeaderCapableOnly is the core election property:
// headers reach every consumer whose sink can take them, and a consumer whose
// sink does not implement the optional seam is skipped WITHOUT an error — not
// implementing it means "no header lane", which is the ordinary case.
func TestSendHeader_BroadcastsToHeaderCapableOnly(t *testing.T) {
	capable1 := &headerRecSink{}
	capable2 := &headerRecSink{}
	plain := &recSink{}

	s := fanout.New(shard.New(0xFF05, shard.DefaultGroupID, 2))
	s.Apply([]*fanout.Consumer{
		{ID: "c1", Sink: capable1},
		{ID: "plain", Sink: plain},
		{ID: "c2", Sink: capable2},
	})

	raw, hf := brc135Frame(t, 0xAA, 1)
	if err := s.SendHeader(raw, hf); err != nil {
		t.Fatalf("SendHeader: %v", err)
	}

	if capable1.headers != 1 || capable2.headers != 1 {
		t.Errorf("header-capable consumers got %d/%d, want 1/1", capable1.headers, capable2.headers)
	}
	// The plain sink must not have been fed through any other method as a
	// substitute — notably not SendRaw, which would land on its tx lane.
	if plain.raw != 0 || plain.tx != 0 || plain.blocks != 0 {
		t.Errorf("non-header consumer received traffic: raw=%d tx=%d blocks=%d", plain.raw, plain.tx, plain.blocks)
	}
}

// TestSendHeader_ShardElectionIgnored pins that headers are fleet-global: a
// consumer restricted to one shard still receives them, matching SendBlock.
func TestSendHeader_ShardElectionIgnored(t *testing.T) {
	sharded := &headerRecSink{}
	s := fanout.New(shard.New(0xFF05, shard.DefaultGroupID, 2))
	s.Apply([]*fanout.Consumer{{ID: "sharded", Shards: []uint32{3}, Sink: sharded}})

	raw, hf := brc135Frame(t, 0xBB, 1)
	if err := s.SendHeader(raw, hf); err != nil {
		t.Fatalf("SendHeader: %v", err)
	}
	if sharded.headers != 1 {
		t.Errorf("shard-restricted consumer got %d headers, want 1", sharded.headers)
	}
}

// TestSendHeader_ErrorDoesNotStopDelivery mirrors the other broadcast paths:
// one failing consumer must not deny the rest, and the first error is still
// surfaced so the worker records an egress error.
func TestSendHeader_ErrorDoesNotStopDelivery(t *testing.T) {
	bad := &headerRecSink{err: errors.New("lane down")}
	good := &headerRecSink{}

	s := fanout.New(shard.New(0xFF05, shard.DefaultGroupID, 2))
	s.Apply([]*fanout.Consumer{{ID: "bad", Sink: bad}, {ID: "good", Sink: good}})

	raw, hf := brc135Frame(t, 0xCC, 1)
	if err := s.SendHeader(raw, hf); err == nil {
		t.Error("SendHeader returned nil, want the first consumer's error")
	}
	if good.headers != 1 {
		t.Errorf("healthy consumer got %d headers, want 1", good.headers)
	}
}

// TestSendHeader_NoConsumers is the quiet case: an edge with nobody electing
// headers must not error, or every block announce would tick an egress error.
func TestSendHeader_NoConsumers(t *testing.T) {
	s := fanout.New(shard.New(0xFF05, shard.DefaultGroupID, 2))
	raw, hf := brc135Frame(t, 0xDD, 1)
	if err := s.SendHeader(raw, hf); err != nil {
		t.Fatalf("SendHeader with no consumers: %v", err)
	}
}
