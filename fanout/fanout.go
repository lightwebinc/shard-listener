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
// the consumer table (typically from a broker push) and the per-consumer
// egress sinks; the differentiated subscription model lives there.
package fanout

import (
	"sync"

	"github.com/lightwebinc/shard-common/frame"
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
type Consumer struct {
	ID     string
	Shards []uint32
	Filt   *filter.Filter
	Sink   egress.EgressSink
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

	mu        sync.RWMutex
	consumers []*Consumer            // full table — broadcast set for control frames
	byShard   map[uint32][]*Consumer // shard idx -> consumers subscribed to that shard
	allShards []*Consumer            // consumers with no shard restriction
}

// compile-time assertion that Sink satisfies the listener's egress seam.
var _ egress.EgressSink = (*Sink)(nil)

// New constructs an empty fan-out sink. engine derives a frame's shard index
// from its TxID (the same derivation the worker uses) so Send can route without
// the seam leaking routing detail. Populate the table with Apply.
func New(engine *shard.Engine) *Sink {
	return &Sink{engine: engine, byShard: make(map[uint32][]*Consumer)}
}

// Apply atomically replaces the consumer table and rebuilds the reverse index.
// It is the open end of the broker push-contract: a downstream build calls it
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
		if err := c.Sink.Send(raw, f); err != nil && firstErr == nil {
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

// SendBlock broadcasts a BRC-131 block control frame to every consumer (block
// announcements are fleet-global, not shard-scoped).
func (s *Sink) SendBlock(raw []byte, bf *frame.BlockFrame) error {
	return s.broadcast(func(c *Consumer) error { return c.Sink.SendBlock(raw, bf) })
}

// SendSubtreeData broadcasts a BRC-132 subtree data frame to every consumer.
func (s *Sink) SendSubtreeData(raw []byte, sf *frame.SubtreeDataFrame) error {
	return s.broadcast(func(c *Consumer) error { return c.Sink.SendSubtreeData(raw, sf) })
}

// SendRaw broadcasts an arbitrary buffer (BRC-135 header egress) to every
// consumer.
func (s *Sink) SendRaw(buf []byte) error {
	return s.broadcast(func(c *Consumer) error { return c.Sink.SendRaw(buf) })
}

func (s *Sink) broadcast(send func(*Consumer) error) error {
	s.mu.RLock()
	cs := s.consumers
	s.mu.RUnlock()
	var firstErr error
	for _, c := range cs {
		if err := send(c); err != nil && firstErr == nil {
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
