// Package fanout implements decode-once, subscription-indexed egress fan-out
// for shard-listener. One listener worker decodes each multicast frame exactly
// once (the expensive path) and delivers it to the subset of a consumer table
// whose subscription admits it — rather than running one listener process per
// consumer, which would pay full ingress + decode N times (Linux delivers each
// multicast datagram to every SO_REUSEPORT socket).
//
// The subscription is inverted into a shard reverse-index so per-frame cost is
// O(consumers-on-that-shard), not O(all-consumers). The residual subtree
// predicate reuses filter.Filter.Allow.
//
// This package is the generic, addressing-agnostic capability: it carries no
// customer identity, pricing, or placement logic. A downstream build supplies
// the consumer table (typically pushed from an external control plane) and the
// per-consumer egress sinks; the differentiated subscription model lives there.
package fanout

import (
	"errors"
	"sync"

	"github.com/lightwebinc/shard-common/bundle"
	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-common/seqhash"
	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/shard-listener/egress"
	"github.com/lightwebinc/shard-listener/filter"
)

// Consumer is one downstream subscriber in the fan-out table.
//
// ID is an opaque label for metrics/logging and carries no customer identity.
// Shards is the explicit shard-index subscription (nil/empty = all shards) and
// drives the reverse index. Filt enforces the residual subtree predicate and
// should carry only subtree include/exclude (shard selection is handled by the
// index); a Filt that also restricts shards still works, only redundantly.
// Sink is this consumer's egress (a unicast *egress.Sender, a multicast
// re-emit sink, or any EgressSink).
//
// OwnIngressIP is the consumer's own ingress identity (the tunnel-inner IPv6
// the proxy observes on its UP path). When non-zero, a transaction frame whose
// proxy-stamped HashKey matches seqhash.Hash(OwnIngressIP, groupIdx, SubtreeID)
// is the consumer's own traffic returning down its egress link and is skipped —
// removing own-traffic-back on metered unicast tunnels. Zero (the default) keeps
// the original behaviour: the consumer receives its own traffic. Control frames
// are never excluded. This is addressing-agnostic: it keys on the originating
// consumer's ingress identity, not on which edge or spine forwarded the frame.
// BundleCapable marks a consumer that can decode BRC-142 bundle frames itself,
// so [Sink.SendBundle] delivers the whole bundle to it intact (one datagram for
// many transactions) instead of decoalescing into individual frames. The
// default (false) preserves the edge-decoalesce contract: the consumer receives
// individual BRC-124 frames. A capable consumer's Sink must implement
// [egress.BundleSink]; if it does not, the bundle is decoalesced as a fallback.
//
// IngressObs, when non-nil, taps the frames this consumer sent up its own tunnel
// — the own-traffic the fan-out identifies (by the OwnIngressIP HashKey match)
// and drops from egress. It is a pure measurement seam: it never changes which
// frames are delivered. A downstream build supplies it to meter that
// (non-billable) tunnel-bound ingress volume. It fires only when OwnIngressIP is
// set (own-traffic exclusion is what surfaces the frames to observe).
type Consumer struct {
	ID            string
	Shards        []uint32
	Filt          *filter.Filter
	Sink          egress.EgressSink
	OwnIngressIP  [16]byte
	BundleCapable bool
	IngressObs    IngressObserver

	// TopicSet is the consumer's elected BRC-148 overlay topics (TopicID =
	// SHA-256 of the topic name). Empty/nil admits every topic on the
	// consumer's elected groups (aggregator mode, per the spec).
	TopicSet map[[32]byte]struct{}

	// BEEFVersions is the consumer's accepted BEEF encoding capability set
	// (payload version words, uint32 LE). Empty/nil admits all encodings.
	BEEFVersions map[uint32]struct{}

	// BEEFObs, when non-nil, is told about every BEEF frame this consumer's own
	// elections EXCLUDED at fan-out — a topic it did not elect, or an encoding
	// it does not accept. A pure measurement seam: it never changes what is
	// delivered. Without it a mis-specified profile (a topic name that matches
	// nothing, a version word the publisher never emits) is indistinguishable
	// from loss — the consumer simply receives nothing, and every counter
	// upstream of the election says the frames arrived.
	BEEFObs BEEFObserver
}

// BEEFObserver measures BEEF frames a consumer's elections filtered out.
// ObserveBEEFFiltered is called on the single-threaded worker hot path once per
// excluded frame, so implementations must be cheap and non-blocking. reason is
// one of [FilterTopic] or [FilterVersion]; wire is the datagram byte length.
type BEEFObserver interface {
	ObserveBEEFFiltered(reason string, wire int)
}

// Filter reasons reported through [BEEFObserver].
const (
	FilterTopic   = "topic"   // TopicID not in the consumer's elected set
	FilterVersion = "version" // BEEF version word not in the consumer's accepted set
)

// IngressObserver measures a consumer's own tunnel-bound ingress — the frames
// the fan-out matches as the consumer's own traffic (returning on the fabric)
// and excludes from egress. A downstream build implements it to feed that
// upstream volume into its own metering. ObserveIngress is called on the
// single-threaded worker hot path, once per own-traffic datagram, so
// implementations must be cheap and non-blocking.
type IngressObserver interface {
	// ObserveIngress records one own-traffic datagram the consumer sent up its
	// tunnel: wire is the datagram byte length, members is the transaction count
	// (1 for a BRC-124/128 frame, len(Members) for a BRC-142 bundle).
	ObserveIngress(wire, members int)
}

// Sink is an egress.EgressSink that fans each frame out to the matching subset
// of a consumer table. Shard-routed transaction frames (Send) use the reverse
// index; fleet-global control frames (SendBlock, SendSubtreeData, SendRaw) are
// broadcast to every consumer, matching the worker's bypass-filter semantics
// for those frame classes.
//
// A worker runs single-threaded on the multicast hot path, so Send is not
// called concurrently with itself; the RWMutex guards only table swaps via
// Apply against the reading hot path.
type Sink struct {
	engine *shard.Engine

	// beefEngine derives BRC-148 domain-tagged group indices from TopicIDs
	// for SendBeef's reverse-index routing AND for the own-traffic HashKey
	// comparison. REQUIRED for BEEF delivery: SendBeef returns
	// ErrBEEFEngineUnset when it is nil rather than mis-routing.
	beefEngine *shard.PlaneEngine

	mu        sync.RWMutex
	consumers []*Consumer            // full table — broadcast set for control frames
	byShard   map[uint32][]*Consumer // shard idx -> consumers subscribed to that shard
	allShards []*Consumer            // consumers with no shard restriction

	// egressSeq re-stamps a fresh monotonic per-bundle-flow SeqNum (keyed by the
	// bundle HashKey) onto each member when decoalescing for a non-capable
	// consumer — the bundle SeqNum is frame-bound and does not survive the split.
	// Touched only on the single-threaded worker hot path (SendBundle), so it
	// needs no lock beyond the worker's own serialisation.
	egressSeq map[uint64]uint64
}

// compile-time assertions that Sink satisfies the listener's egress seams.
var (
	_ egress.EgressSink       = (*Sink)(nil)
	_ egress.BundleSink       = (*Sink)(nil)
	_ egress.HeaderSink       = (*Sink)(nil)
	_ egress.HeaderFanoutSink = (*Sink)(nil)
)

// New constructs an empty fan-out sink. engine derives a frame's shard index
// from its TxID (the same derivation the worker uses) so Send can route without
// the seam leaking routing detail. Populate the table with Apply.
func New(engine *shard.Engine) *Sink {
	return &Sink{engine: engine, byShard: make(map[uint32][]*Consumer)}
}

// Apply atomically replaces the consumer table and rebuilds the reverse index.
// It is the open end of the external control-plane push contract: a downstream build calls it
// whenever consumers arrive or leave. Safe to call concurrently with Send.
func (s *Sink) Apply(consumers []*Consumer) {
	byShard := make(map[uint32][]*Consumer)
	var allShards []*Consumer
	for _, c := range consumers {
		if len(c.Shards) == 0 {
			allShards = append(allShards, c)
			continue
		}
		for _, sh := range c.Shards {
			byShard[sh] = append(byShard[sh], c)
		}
	}
	s.mu.Lock()
	s.consumers = consumers
	s.byShard = byShard
	s.allShards = allShards
	s.mu.Unlock()
}

// Send shard-routes a BRC-124/BRC-128 transaction frame to every consumer
// subscribed to the frame's shard whose subtree filter admits it. Delivery
// continues to all matching consumers even if one errors; the first error is
// returned so the worker still records an egress error.
// ErrBEEFEngineUnset is returned by SendBeef when no BRC-148 plane engine has
// been wired. It is a configuration error, never a per-frame condition: the
// engine is required to derive the domain-tagged group index that both
// reverse-index routing and own-traffic exclusion depend on.
var ErrBEEFEngineUnset = errors.New("fanout: BEEF plane engine not set")

// SetBEEFEngine wires the BRC-148 plane engine used to route SendBeef via
// the shard reverse index. Call before the worker starts.
func (s *Sink) SetBEEFEngine(pe *shard.PlaneEngine) { s.beefEngine = pe }

// SendBeef fans a BRC-148 BEEF object frame to the consumers whose group
// election covers its domain-tagged group, then applies each consumer's
// topic filter and BEEF-version (encoding capability) filter — group
// membership → topic filter → version filter → delivery, per the spec.
// Own-traffic exclusion uses the BEEF flow identity: XXH64(consumer ingress
// IP ∥ banded groupIdx ∥ zeros) — the 32-byte ingredient is ZERO (TopicID is
// excluded from BEEF flow keys).
func (s *Sink) SendBeef(raw []byte, bf *frame.BEEFFrame) error {
	// Without a plane engine there is no group index, so neither the
	// reverse-index route nor the own-traffic HashKey comparison
	// (XXH64(ip ∥ bandedGroupIdx ∥ zero32)) can be computed. Falling back to
	// the full consumer table would bypass group election AND silently
	// disable own-traffic exclusion + ingress metering — a wrong delivery
	// that looks like success. Drop instead; SetBEEFEngine is required
	// wiring for any build that elects BEEF.
	if s.beefEngine == nil {
		return ErrBEEFEngineUnset
	}
	s.mu.RLock()
	groupIdx := s.beefEngine.GroupIndex(&bf.TopicID)
	bucket := s.byShard[groupIdx]
	allShards := s.allShards
	s.mu.RUnlock()

	var firstErr error
	deliver := func(c *Consumer) {
		if len(c.TopicSet) > 0 {
			if _, ok := c.TopicSet[bf.TopicID]; !ok {
				if c.BEEFObs != nil {
					c.BEEFObs.ObserveBEEFFiltered(FilterTopic, len(raw))
				}
				return
			}
		}
		if len(c.BEEFVersions) > 0 {
			w, ok := objfmt.BEEFVersionWord(bf.Payload)
			if _, accepted := c.BEEFVersions[w]; !ok || !accepted {
				if c.BEEFObs != nil {
					c.BEEFObs.ObserveBEEFFiltered(FilterVersion, len(raw))
				}
				return
			}
		}
		if isOwnBeef(c, groupIdx, bf) {
			if c.IngressObs != nil {
				c.IngressObs.ObserveIngress(len(raw), 1)
			}
			return
		}
		if err := c.Sink.SendBeef(raw, bf); err != nil && firstErr == nil && !egress.IsNotElected(err) {
			firstErr = err
		}
	}
	for _, c := range bucket {
		deliver(c)
	}
	for _, c := range allShards {
		deliver(c)
	}
	return firstErr
}

// isOwnBeef reports whether bf is the consumer's own submission returning on
// the fabric: the frame HashKey equals the BEEF flow identity of the
// consumer's ingress IP on this group (zero 32-byte ingredient per BRC-148).
func isOwnBeef(c *Consumer, groupIdx uint32, bf *frame.BEEFFrame) bool {
	if c.OwnIngressIP == ([16]byte{}) || bf.HashKey == 0 {
		return false
	}
	var zero [32]byte
	return bf.HashKey == seqhash.Hash(c.OwnIngressIP, groupIdx, zero)
}

func (s *Sink) Send(raw []byte, f *frame.Frame) error {
	groupIdx := s.engine.GroupIndex(&f.TxID)

	s.mu.RLock()
	bucket := s.byShard[groupIdx]
	allShards := s.allShards
	s.mu.RUnlock()

	var firstErr error
	deliver := func(c *Consumer) {
		if c.Filt != nil {
			if ok, _ := c.Filt.Allow(groupIdx, f); !ok {
				return
			}
		}
		if isOwnTraffic(c, groupIdx, f) {
			if c.IngressObs != nil {
				c.IngressObs.ObserveIngress(len(raw), 1)
			}
			return
		}
		if err := c.Sink.Send(raw, f); err != nil && firstErr == nil && !egress.IsNotElected(err) {
			firstErr = err
		}
	}
	for _, c := range bucket {
		deliver(c)
	}
	for _, c := range allShards {
		deliver(c)
	}
	return firstErr
}

// SendBundle delivers a BRC-142 bundle to every consumer subscribed to the
// bundle's shard whose subtree filter admits it. Per consumer:
//   - bundle-capable (and its Sink is an [egress.BundleSink]) → the whole bundle
//     intact (one datagram, many transactions), so the coalescing saving reaches
//     that consumer's last hop;
//   - otherwise → the bundle decoalesced into individual BRC-124 frames, each
//     re-stamped with a fresh monotonic per-flow egress SeqNum, preserving the
//     edge-decoalesce contract.
//
// The group is the bundle's own GroupIdx (a bundle carries many TxIDs and is
// bound to one group); the subtree filter is applied at bundle granularity (a
// bundle is one (group, subtree) flow). Decoalescing happens at most once per
// bundle and is shared across all non-capable consumers. raw is the verbatim
// bundle datagram; b is its parse (members alias raw).
func (s *Sink) SendBundle(raw []byte, b *bundle.Bundle) error {
	groupIdx := uint32(b.GroupIdx)

	s.mu.RLock()
	bucket := s.byShard[groupIdx]
	allShards := s.allShards
	s.mu.RUnlock()

	// Bundle-granularity subtree predicate: a bundle shares one SubtreeID.
	probe := &frame.Frame{Version: frame.FrameVerV2, SubtreeID: b.SubtreeID, HashKey: b.HashKey}

	var members []encodedMember // lazily decoalesced+re-stamped, shared by non-capable consumers
	var firstErr error
	deliver := func(c *Consumer) {
		if c.Filt != nil {
			if ok, _ := c.Filt.Allow(groupIdx, probe); !ok {
				return
			}
		}
		if isOwnBundle(c, groupIdx, b) {
			if c.IngressObs != nil {
				c.IngressObs.ObserveIngress(len(raw), len(b.Members))
			}
			return
		}
		if c.BundleCapable {
			if bs, ok := c.Sink.(egress.BundleSink); ok {
				if err := bs.SendBundle(raw, b); err != nil && firstErr == nil && !egress.IsNotElected(err) {
					firstErr = err
				}
				return
			}
			// Capable flag but the sink can't take bundles: fall back to members.
		}
		if members == nil {
			members = s.decoalesce(b)
		}
		for i := range members {
			if err := c.Sink.Send(members[i].raw, members[i].f); err != nil && firstErr == nil && !egress.IsNotElected(err) {
				firstErr = err
			}
		}
	}
	for _, c := range bucket {
		deliver(c)
	}
	for _, c := range allShards {
		deliver(c)
	}
	return firstErr
}

// encodedMember is one decoalesced bundle member: its re-encoded BRC-124 wire
// bytes and the parsed frame (re-stamped SeqNum), reused across consumers.
type encodedMember struct {
	raw []byte
	f   *frame.Frame
}

// decoalesce splits b into individual BRC-124 frames once, re-stamping each with
// a fresh monotonic per-flow egress SeqNum (keyed by the bundle HashKey) so a
// non-capable consumer retains gap detection. Members that fail to re-encode are
// skipped (the egress sequence still advances so a single bad member does not
// desync the rest).
func (s *Sink) decoalesce(b *bundle.Bundle) []encodedMember {
	if s.egressSeq == nil {
		s.egressSeq = make(map[uint64]uint64)
	}
	mfs := bundle.Decoalesce(b)
	out := make([]encodedMember, 0, len(mfs))
	for _, mf := range mfs {
		s.egressSeq[b.HashKey]++
		mf.SeqNum = s.egressSeq[b.HashKey]
		buf := make([]byte, frame.HeaderSize+len(mf.Payload))
		n, err := frame.Encode(mf, buf)
		if err != nil {
			continue
		}
		out = append(out, encodedMember{raw: buf[:n], f: mf})
	}
	return out
}

// isOwnBundle reports whether bundle b is consumer c's own traffic returning
// down its egress link — the bundle-granularity analogue of [isOwnTraffic]. A
// bundle is one source flow (one HashKey), so the whole bundle is skipped when
// its HashKey matches the one the proxy derives from c's ingress identity.
func isOwnBundle(c *Consumer, groupIdx uint32, b *bundle.Bundle) bool {
	if c.OwnIngressIP == ([16]byte{}) {
		return false
	}
	return b.HashKey == seqhash.Hash(c.OwnIngressIP, groupIdx, b.SubtreeID)
}

// isOwnTraffic reports whether frame f is consumer c's own transaction returning
// down its egress link, so the fan-out can skip it. It fires only when c opted in
// (non-zero OwnIngressIP) and the frame's proxy-stamped HashKey matches the one
// the proxy would derive from c's ingress identity. A false positive requires a
// 64-bit XXH64 collision (negligible); a frame whose HashKey was not derived from
// c's ingress IP simply does not match and is delivered, exactly as before.
func isOwnTraffic(c *Consumer, groupIdx uint32, f *frame.Frame) bool {
	if c.OwnIngressIP == ([16]byte{}) || f.Version != frame.FrameVerV2 {
		return false
	}
	return f.HashKey == seqhash.Hash(c.OwnIngressIP, groupIdx, f.SubtreeID)
}

// SendBlock broadcasts a BRC-131 block control frame to every consumer (block
// announcements are fleet-global, not shard-scoped).
func (s *Sink) SendBlock(raw []byte, bf *frame.BlockFrame) error {
	return s.broadcast(func(c *Consumer) error { return c.Sink.SendBlock(raw, bf) })
}

// SendSubtreeData broadcasts a BRC-132 subtree data frame to every consumer.
func (s *Sink) SendSubtreeData(raw []byte, sf *frame.SubtreeDataFrame) error {
	return s.broadcast(func(c *Consumer) error { return c.Sink.SendSubtreeData(raw, sf) })
}

// SendHeader broadcasts a BRC-135 block header frame to every consumer, exactly
// as blocks and subtree data are broadcast: headers are fleet-global, not
// shard-scoped, so there is no index to route on. Which consumers actually
// receive one is decided by their own sink — a class router delivers only to a
// consumer that elected a header lane and reports the rest as not-elected.
//
// A consumer whose sink does not implement [egress.HeaderSink] is skipped
// rather than failed: the seam is optional, so not implementing it means "this
// consumer has no header lane", which is the common case and not an error.
func (s *Sink) SendHeader(raw []byte, hf *frame.Frame) error {
	_, err := s.SendHeaderN(raw, hf)
	return err
}

// SendHeaderN is [SendHeader] plus the number of consumers the header actually
// reached, so the caller can tell "delivered to nobody" (every consumer skipped
// or unelected) from "delivered". Satisfies [egress.HeaderFanoutSink].
func (s *Sink) SendHeaderN(raw []byte, hf *frame.Frame) (int, error) {
	s.mu.RLock()
	cs := s.consumers
	s.mu.RUnlock()

	delivered := 0
	var firstErr error
	for _, c := range cs {
		hs, ok := c.Sink.(egress.HeaderSink)
		if !ok {
			continue // no header lane on this consumer
		}
		err := hs.SendHeader(raw, hf)
		switch {
		case err == nil:
			delivered++
		case egress.IsNotElected(err):
			// withheld by election, not a failure
		case firstErr == nil:
			firstErr = err
		}
	}
	return delivered, firstErr
}

// SendRaw broadcasts an arbitrary buffer (BRC-135 header egress) to every
// consumer.
func (s *Sink) SendRaw(buf []byte) error {
	return s.broadcast(func(c *Consumer) error { return c.Sink.SendRaw(buf) })
}

// broadcast delivers to every consumer and returns the first REAL error. An
// [egress.ErrNotElected] return is skipped: in a per-class fan-out most
// consumers have not elected most classes, so treating election as a failure
// would make the worker's egress-error counter tick on ordinary traffic and
// suppress its forwarded counter entirely.
func (s *Sink) broadcast(send func(*Consumer) error) error {
	s.mu.RLock()
	cs := s.consumers
	s.mu.RUnlock()
	var firstErr error
	for _, c := range cs {
		if err := send(c); err != nil && firstErr == nil && !egress.IsNotElected(err) {
			firstErr = err
		}
	}
	return firstErr
}

// Proto returns the egress protocol label used in the worker's metrics.
func (s *Sink) Proto() string { return "fanout" }

// Close closes every consumer's sink, returning the first error.
func (s *Sink) Close() error {
	s.mu.RLock()
	cs := s.consumers
	s.mu.RUnlock()
	var firstErr error
	for _, c := range cs {
		if err := c.Sink.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
