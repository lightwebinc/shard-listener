package egress

import "github.com/lightwebinc/shard-common/frame"

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
	// SendRaw forwards an arbitrary buffer verbatim (BRC-135 header egress).
	SendRaw(buf []byte) error
	// Proto returns the egress protocol label used in metrics ("udp"/"tcp").
	Proto() string
	// Close releases the sink's resources.
	Close() error
}

// compile-time assertion that the built-in Sender satisfies the seam.
var _ EgressSink = (*Sender)(nil)
