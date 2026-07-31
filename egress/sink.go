package egress

import (
	"errors"

	"github.com/lightwebinc/shard-common/frame"
)

// ErrNotElected reports that a consumer did not elect the class being
// delivered, so the frame was deliberately not sent to it. It is a routing
// OUTCOME, not a failure: in a per-class fan-out most consumers do not elect
// most classes, so this is the common case on every frame.
//
// A class-routing sink must return an error (rather than nil) for an unelected
// class, or a metering wrapper keyed on "err == nil" would bill a delivery that
// never happened. But an aggregating fan-out must NOT surface it as the batch's
// error, or the worker books an egress failure — and skips its "forwarded"
// counter — on ordinary traffic. Wrapping this sentinel is what lets both hold
// at once: the biller still sees a non-nil error, while the fan-out recognises
// it with [errors.Is] and keeps aggregating.
//
// Class routers should wrap it (`fmt.Errorf("...: %w", egress.ErrNotElected)`)
// so the specific message survives for logs.
var ErrNotElected = errors.New("egress: class not elected by consumer")

// IsNotElected reports whether err is (or wraps) [ErrNotElected] — i.e. the
// frame was withheld by election rather than lost.
func IsNotElected(err error) bool { return errors.Is(err, ErrNotElected) }

// EgressSink is the forwarding contract the listener worker delivers decoded
// frames through. The default single-destination [Sender] satisfies it; an
// alternative sink — for example a multi-consumer fan-out in a downstream
// build — implements the same methods to receive every frame the worker would
// otherwise unicast to one downstream.
//
// The interface mirrors *Sender's surface exactly, so the default forwarding
// path is a drop-in and the public seam stays addressing- and policy-agnostic.
// A fan-out sink that needs the shard index recomputes it from f.TxID with its
// own shard.Engine rather than the seam leaking routing detail. Nothing here
// reveals how many consumers exist or how they are selected.
type EgressSink interface {
	// Send forwards a BRC-12 / BRC-124 / BRC-128 (or BRC-134 anchor) frame.
	Send(raw []byte, f *frame.Frame) error
	// SendBlock forwards a BRC-131 block control frame.
	SendBlock(raw []byte, bf *frame.BlockFrame) error
	// SendSubtreeData forwards a BRC-132 subtree data frame.
	SendSubtreeData(raw []byte, sf *frame.SubtreeDataFrame) error
	// SendBeef forwards a BRC-148 BEEF object frame (FrameVer 0x09).
	SendBeef(raw []byte, bf *frame.BEEFFrame) error
	// SendRaw forwards an arbitrary buffer verbatim (BRC-135 header egress).
	SendRaw(buf []byte) error
	// Proto returns the egress protocol label used in metrics ("udp"/"tcp").
	Proto() string
	// Close releases the sink's resources.
	Close() error
}

// compile-time assertion that the built-in Sender satisfies the seam.
var _ EgressSink = (*Sender)(nil)

// HeaderSink is an OPTIONAL seam an [EgressSink] may also implement to receive
// BRC-135 block headers as a routable class rather than as the worker's
// node-global header egress. A downstream fan-out build implements it to offer
// headers as a per-consumer elected lane; a sink that does not implement it is
// simply never offered headers, so this is additive to [EgressSink] and every
// existing implementation keeps compiling.
//
// It is a separate interface rather than an EgressSink method for the same
// reason [BundleSink] is: only a fan-out sink can act on it, and widening the
// base seam would force a no-op method onto every implementation of a contract
// that is deliberately small.
//
// SendRaw is NOT this seam. It is class-agnostic — a fan-out sink cannot tell a
// header from any other buffer, so it can neither route on election nor meter
// under the right class, and a class router forwarding it would have to guess a
// lane. Headers therefore get their own typed method, exactly as blocks,
// subtree data, and BEEF objects do.
type HeaderSink interface {
	// SendHeader forwards one BRC-135 block header frame. raw is the whole
	// 172-byte frame (92-byte header + the 80-byte block header); hf is the
	// decoded view (Version FrameVerV7, TxID = the block hash) whose Payload
	// aliases raw's 80-byte block header — the form a subscriber that wants
	// merkle roots actually consumes. Neither buffer may be retained after the
	// call returns.
	SendHeader(raw []byte, hf *frame.Frame) error
}
