// BRC-148 BEEF object plane delivery: FrameVer 0x09 handling through the
// listener's standard pipeline — election filters, cross-listener dedup,
// inline-vs-retransmit suppression, egress, and NACK gap tracking.

package listener

import (
	"crypto/sha256"
	"fmt"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"

	"github.com/lightwebinc/shard-listener/dedup"
)

// beefPairKey derives the cross-listener egress-dedup claim key for a BEEF
// frame: SHA-256(ContentID ∥ TopicID). Keying the pair — never the bare
// ContentID — keeps sibling emissions of a multi-topic submission (which
// share a ContentID across groups) independently deliverable.
func beefPairKey(contentID, topicID [32]byte) [32]byte {
	h := sha256.New()
	h.Write(contentID[:])
	h.Write(topicID[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// processBeefFrame handles a raw BRC-148 BEEF object datagram.
func (w *Worker) processBeefFrame(raw []byte) {
	bf, err := frame.DecodeBEEF(raw)
	if err != nil {
		if w.rec != nil {
			w.rec.FrameDropped(w.id, "decode_error")
		}
		if w.debug {
			w.log.Debug("beef decode error", "err", err, "len", len(raw))
		}
		return
	}
	w.deliverBeef(raw, bf)
}

// DeliverReassembledBeef re-encodes a BRC-130-reassembled BEEF object as a
// whole FrameVer 0x09 frame and routes it through the standard BEEF delivery
// path — filters evaluate on whole objects, per the spec. Wire it to
// [reassembly.Buffer.SetBEEFCallback]. The payload argument is the callback
// contract's reassembled bytes; it aliases bf.Payload.
func (w *Worker) DeliverReassembledBeef(_ []byte, bf *frame.BEEFFrame) {
	buf := make([]byte, frame.HeaderSize+len(bf.Payload))
	n, err := frame.EncodeBEEF(bf, buf)
	if err != nil {
		if w.rec != nil {
			w.rec.FrameDropped(w.id, "beef_reencode_error")
		}
		return
	}
	w.deliverBeef(buf[:n], bf)
}

// deliverBeef is the shared BEEF delivery core: group membership (implicit —
// the frame arrived on a joined group) → topic filter → version filter →
// dedup gates → egress → gap tracking. NACK observation uses a ZERO
// SubtreeID (a BEEF flow interleaves topics, so a gap's TopicID is
// unknowable) and the ContentID as the frame identity.
func (w *Worker) deliverBeef(raw []byte, bf *frame.BEEFFrame) {
	if w.beefEngine == nil {
		if w.rec != nil {
			w.rec.FrameDropped(w.id, "beef_disabled")
		}
		return
	}
	if w.rec != nil {
		w.rec.FrameReceived(w.id, w.iface.Name, "brc148")
	}

	// Courtesy ingress-set mark (pair key — matches the proxy's claim key).
	if w.txDedup != nil && w.txDedup.HasIngressMark() {
		w.txDedup.Mark(beefPairKey(bf.ContentID, bf.TopicID))
	}

	// Optional ContentID verification (debug/test support; the BEEF
	// analogue of -verify-payload-hash).
	if w.beefVerifyContent {
		first := sha256.Sum256(bf.Payload)
		second := sha256.Sum256(first[:])
		if second != bf.ContentID {
			if w.rec != nil {
				w.rec.FrameInvalidPayload(w.id)
			}
			return
		}
	}

	groupIdx := w.beefEngine.GroupIndex(&bf.TopicID)

	// Worker-level election: topic filter then version (encoding
	// capability) filter; absent filters admit everything (aggregator).
	if len(w.beefTopics) > 0 {
		if _, ok := w.beefTopics[bf.TopicID]; !ok {
			if w.rec != nil {
				w.rec.FrameDropped(w.id, "topic_filter")
			}
			return
		}
	}
	if len(w.beefVersions) > 0 {
		word, ok := objfmt.BEEFVersionWord(bf.Payload)
		if !ok {
			if w.rec != nil {
				w.rec.FrameDropped(w.id, "beef_version_filter")
			}
			return
		}
		if _, accepted := w.beefVersions[word]; !accepted {
			if w.rec != nil {
				w.rec.FrameDropped(w.id, "beef_version_filter")
			}
			return
		}
	}

	var zeroSub [32]byte

	// Cross-listener egress dedup: claim the (ContentID, TopicID) pair.
	if w.txDedup != nil {
		claimed, claimErr := w.txDedup.Claim(beefPairKey(bf.ContentID, bf.TopicID))
		if claimErr != nil {
			if w.rec != nil {
				w.rec.TxDedupError()
			}
		} else if !claimed {
			if w.rec != nil {
				w.rec.FrameTxDeduped(w.id)
			}
			if w.tracker != nil && bf.SeqNum != 0 {
				w.tracker.Observe(groupIdx, zeroSub, bf.HashKey, bf.SeqNum, bf.ContentID, w.curSource)
			}
			return
		}
	}

	// Inline-vs-retransmit suppression (same frame arriving twice).
	if w.dedupSet != nil && bf.SeqNum != 0 {
		if w.dedupSet.SeenAndAdd(dedup.Key{GroupIdx: groupIdx, SubtreeID: bf.TopicID, SeqNum: bf.SeqNum}) {
			if w.rec != nil {
				w.rec.FrameDeduped(w.id)
			}
			if w.tracker != nil {
				w.tracker.Observe(groupIdx, zeroSub, bf.HashKey, bf.SeqNum, bf.ContentID, w.curSource)
			}
			return
		}
	}

	if err := w.egr.SendBeef(raw, bf); err != nil {
		if w.rec != nil {
			w.rec.EgressError(w.id)
		}
		w.log.Debug("beef egress send error", "err", err)
	} else if w.rec != nil {
		w.rec.FrameForwarded(w.id, w.egr.Proto())
	}

	// Gap tracking: BEEF flows are per (sender, group); the NACK's SubtreeID
	// field is zero per BRC-148 (responders ignore it).
	if w.tracker != nil && bf.SeqNum != 0 {
		w.tracker.Observe(groupIdx, zeroSub, bf.HashKey, bf.SeqNum, bf.ContentID, w.curSource)
	}

	if w.debug {
		w.log.Debug("beef frame forwarded",
			"topic_prefix", fmt.Sprintf("%x", bf.TopicID[:4]),
			"group", groupIdx,
			"seq_num", bf.SeqNum,
		)
	}
}
