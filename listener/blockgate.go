package listener

import (
	"sync"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/pow"
)

// CoinbaseCorrelator is a shared, TTL-bounded set of coinbase TxIDs taken from
// proof-of-work-valid block announcements. A BRC-133 coinbase frame is
// forwarded only if its TxID appears here — i.e. some validated block
// referenced it as its coinbase. It is shared across all workers because a
// block announce and its coinbase can land on different worker sockets under
// SO_REUSEPORT, so it must be concurrency-safe.
//
// This is a domain-local anti-spam correlation, not consensus: an uncorrelated
// coinbase (one arriving before its block, or with no matching block) is
// dropped at fan-out; it is re-evaluated if the block announce arrives and the
// coinbase is re-sent. Block announcements are low-rate, so the set stays tiny.
type CoinbaseCorrelator struct {
	mu  sync.Mutex
	ttl time.Duration
	cap int
	m   map[[32]byte]int64 // coinbase txid -> unix-nano insert time
}

// NewCoinbaseCorrelator builds a correlator. capacity bounds the entry count
// (≤0 = unbounded); ttl bounds entry age (≤0 = no expiry).
func NewCoinbaseCorrelator(capacity int, ttl time.Duration) *CoinbaseCorrelator {
	return &CoinbaseCorrelator{ttl: ttl, cap: capacity, m: make(map[[32]byte]int64)}
}

// Add records a validated block's coinbase TxID.
func (c *CoinbaseCorrelator) Add(txid [32]byte) {
	if c == nil {
		return
	}
	now := time.Now().UnixNano()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cap > 0 && len(c.m) >= c.cap {
		c.sweep(now)
	}
	c.m[txid] = now
}

// Has reports whether txid was recorded by a validated block within the TTL,
// expiring it lazily if stale.
func (c *CoinbaseCorrelator) Has(txid [32]byte) bool {
	if c == nil {
		return false
	}
	now := time.Now().UnixNano()
	c.mu.Lock()
	defer c.mu.Unlock()
	ins, ok := c.m[txid]
	if !ok {
		return false
	}
	if c.ttl > 0 && now-ins > c.ttl.Nanoseconds() {
		delete(c.m, txid)
		return false
	}
	return true
}

// sweep drops expired entries; caller holds the lock. If nothing is expired and
// the set is still at capacity, the oldest entry is evicted to bound memory.
func (c *CoinbaseCorrelator) sweep(now int64) {
	if c.ttl > 0 {
		for k, ins := range c.m {
			if now-ins > c.ttl.Nanoseconds() {
				delete(c.m, k)
			}
		}
	}
	if c.cap > 0 && len(c.m) >= c.cap {
		var oldestK [32]byte
		var oldest int64
		first := true
		for k, ins := range c.m {
			if first || ins < oldest {
				oldestK, oldest, first = k, ins, false
			}
		}
		if !first {
			delete(c.m, oldestK)
		}
	}
}

// blockGateAllows reports whether a BRC-131 block control frame may be
// forwarded under the optional block-control gate. When the gate is off it
// always allows. When on: a block frame must carry valid proof of work (its
// in-frame 80-byte header hashes under the claimed target, which must meet the
// floor). payload is the fabric block payload (bf.Payload on the direct path,
// the reassembled payload on the fragmentation path); its first 80 bytes are
// the block header. Drops are counted via the metrics reason label.
//
// Coinbase correlation is retired: since the push model carries the coinbase
// INLINE inside the block body (BRC-144), there is no separate BRC-133 coinbase
// frame to validate against a block-recorded coinbase TxID. A stray
// BlockMsgCoinbase frame (legacy) is dropped as uncorrelated when the
// correlator is configured.
func (w *Worker) blockGateAllows(bf *frame.BlockFrame, payload []byte) bool {
	if !w.requireBlockPoW {
		return true
	}
	switch bf.MsgType {
	case frame.BlockMsgAnnounce:
		if len(payload) < pow.HeaderSize || !pow.CheckHeader(payload[:pow.HeaderSize], w.powFloor) {
			if w.rec != nil {
				w.rec.FrameDropped(w.id, "block_pow")
			}
			return false
		}
	case frame.BlockMsgCoinbase:
		if w.coinbaseCorr != nil && !w.coinbaseCorr.Has(bf.ContentID) {
			if w.rec != nil {
				w.rec.FrameDropped(w.id, "coinbase_uncorrelated")
			}
			return false
		}
	}
	return true
}
