package listener

import (
	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/pow"
)

// blockGateAllows reports whether a BRC-131 block control frame may be
// forwarded under the block-control gate. When the gate is off it always
// allows. When on: a block frame must carry valid proof of work (its in-frame
// 80-byte header hashes under the claimed target, which must meet the floor).
// payload is the fabric block payload (bf.Payload on the direct path, the
// reassembled payload on the fragmentation path); its first 80 bytes are the
// block header. Drops are counted via the metrics reason label.
//
// A standalone BRC-133 coinbase frame is LEGACY and is dropped while the gate
// is on. It carries no proof of work of its own, so nothing about it can be
// validated in isolation; the push model supersedes it by carrying the
// coinbase INLINE in the block body (BRC-144), where it inherits the
// announce's PoW.
//
// This previously ran through a CoinbaseCorrelator that was meant to admit a
// coinbase whose TxID a PoW-valid announce had recorded. That correlator was
// never populated — its Add method had no caller — so it could only ever say
// "no", and the -coinbase-corr-cap flag's effect was the inverse of its
// documentation: a non-zero cap dropped EVERY coinbase frame and zero
// forwarded every one, unvalidated. Both flags and the type are gone; the drop
// is now explicit and honest.
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
		if w.rec != nil {
			w.rec.FrameDropped(w.id, "coinbase_legacy")
		}
		return false
	}
	return true
}
