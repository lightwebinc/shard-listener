package egress

import (
	"github.com/lightwebinc/shard-common/bundle"
	"github.com/lightwebinc/shard-common/frame"
)

// BundleSink is an OPTIONAL capability a sink may implement on top of
// [EgressSink] to receive BRC-142 bundle (coalescing) frames INTACT rather than
// as decoalesced individual frames.
//
// A bundle-aware sink — a fan-out that delivers whole bundles end-to-end to
// bundle-capable consumers and decoalesces for the rest — implements SendBundle
// so the listener worker can hand it the parsed bundle without splitting it
// first. A sink that does NOT implement BundleSink is handed the bundle's
// members as individual frames via [EgressSink.Send], exactly as before, so the
// default forwarding path is unchanged.
//
// raw is the verbatim on-wire bundle datagram (valid until SendBundle returns);
// b is its parse (Member.Tx slices alias raw). A sink that forwards raw onward
// must copy it if it outlives the call.
type BundleSink interface {
	SendBundle(raw []byte, b *bundle.Bundle) error
}

// DeliverBundle hands bundle b to sink: if sink is a [BundleSink] it receives
// the whole bundle intact; otherwise b is decoalesced into individual BRC-124
// frames delivered via sink.Send. It lets a pass-through wrapper (a dedup or
// flush sink that does not itself decode bundles) forward a bundle toward a
// bundle-aware inner sink, while still delivering correctly if the inner chain
// turns out not to be bundle-aware. raw is the verbatim bundle datagram.
//
// The decoalesced fallback leaves each member's SeqNum at 0 (the bundle SeqNum
// is frame-bound and does not survive a split); a sink that re-stamps per-flow
// egress sequences — the fan-out — implements BundleSink and so takes the intact
// path, never this fallback.
func DeliverBundle(sink EgressSink, raw []byte, b *bundle.Bundle) error {
	if bs, ok := sink.(BundleSink); ok {
		return bs.SendBundle(raw, b)
	}
	var firstErr error
	for _, mf := range bundle.Decoalesce(b) {
		buf := make([]byte, frame.HeaderSize+len(mf.Payload))
		n, err := frame.Encode(mf, buf)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := sink.Send(buf[:n], mf); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
