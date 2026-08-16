package fanout_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/shard-listener/egress"
	"github.com/lightwebinc/shard-listener/fanout"
)

// notElectedSink models a class router: it refuses every class with an error
// wrapping egress.ErrNotElected, exactly as a per-class router sink does for
// a class the consumer did not elect.
type notElectedSink struct{ recSink }

func (n *notElectedSink) wrap() error {
	return fmt.Errorf("classrouter: class not elected by consumer: %w", egress.ErrNotElected)
}
func (n *notElectedSink) Send(raw []byte, f *frame.Frame) error          { return n.wrap() }
func (n *notElectedSink) SendBeef(raw []byte, bf *frame.BEEFFrame) error { return n.wrap() }
func (n *notElectedSink) SendHeader(_ []byte, _ *frame.Frame) error      { return n.wrap() }

// A consumer that did not elect the class must NOT turn the batch into an
// error. Before this, the worker booked an egress FAILURE and skipped its
// forwarded counter on every ordinary frame, because a per-class fan-out
// always has consumers that did not elect a given class. Symptom:
// bsl_header_forwarded_total stalls while headers are still delivered, and
// bsl_header_egress_errors_total counts nearly every frame.
func TestNotElectedIsNotABatchError(t *testing.T) {
	if !egress.IsNotElected(fmt.Errorf("wrapped: %w", egress.ErrNotElected)) {
		t.Fatal("IsNotElected must see through wrapping")
	}

	unelected := &notElectedSink{}
	delivered := &headerRecSink{}

	s := fanout.New(shard.New(0xFF05, shard.DefaultGroupID, 2))
	s.Apply([]*fanout.Consumer{
		{ID: "unelected", Sink: unelected},
		{ID: "elected", Sink: delivered},
	})

	raw, hf := brc135Frame(t, 0xAA, 1)
	if err := s.SendHeader(raw, hf); err != nil {
		t.Fatalf("SendHeader returned %v; a not-elected consumer must not fail the batch", err)
	}
	if delivered.headers != 1 {
		t.Fatalf("electing consumer got %d headers, want 1", delivered.headers)
	}
}

// A REAL failure must still surface, or genuine breakage goes unreported.
func TestRealErrorStillSurfaces(t *testing.T) {
	bad := &headerRecSink{err: errors.New("connection reset")}
	unelected := &notElectedSink{}

	s := fanout.New(shard.New(0xFF05, shard.DefaultGroupID, 2))
	s.Apply([]*fanout.Consumer{{ID: "unelected", Sink: unelected}, {ID: "bad", Sink: bad}})

	raw, hf := brc135Frame(t, 0xBB, 1)
	err := s.SendHeader(raw, hf)
	if err == nil {
		t.Fatal("a real delivery failure must still be returned")
	}
	if egress.IsNotElected(err) {
		t.Fatalf("real error was misclassified as not-elected: %v", err)
	}
}

// The same rule applies to the tx path, where the false-error volume is worst
// (bsl_egress_errors_total ~= every frame received).
func TestNotElectedIsNotABatchErrorOnTx(t *testing.T) {
	unelected := &notElectedSink{}
	good := &recSink{}

	s := fanout.New(shard.New(0xFF05, shard.DefaultGroupID, 2))
	s.Apply([]*fanout.Consumer{{ID: "unelected", Sink: unelected}, {ID: "good", Sink: good}})

	if err := s.Send(nil, txInShard(0, subtreeID(1))); err != nil {
		t.Fatalf("Send returned %v; a not-elected consumer must not fail the batch", err)
	}
	if good.tx != 1 {
		t.Fatalf("electing consumer got %d tx frames, want 1", good.tx)
	}
}
