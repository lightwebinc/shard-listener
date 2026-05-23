// Package listener implements the multicast receive workers for
// bitcoin-shard-listener.
//
// # Worker model
//
// Each Worker binds one UDP socket with SO_REUSEPORT on the configured port
// and joins all configured multicast groups on the configured interface. The
// kernel distributes incoming datagrams across all SO_REUSEPORT workers; the
// same source will consistently land on the same worker, giving CPU-local
// per-sender gap tracking with no lock contention between workers.
//
// # Hot path per frame
//
//  1. ReadFrom (per-worker receive buffer)
//  2. frame.Decode — extract TxID, Version, HashKey, SeqNum
//  3. shard.Engine.GroupIndex — derive groupIdx from TxID
//  4. filter.Filter.Allow — shard/subtree gating
//  5. egress.Sender.Send — unicast forward to downstream
//  6. nack.Tracker.Observe — gap detection (BRC-124/BRC-128 only, non-zero SeqNum)
package listener

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"net"
	"time"

	"golang.org/x/sys/unix"

	"github.com/lightwebinc/bitcoin-shard-common/frame"
	"github.com/lightwebinc/bitcoin-shard-common/shard"

	"github.com/lightwebinc/bitcoin-shard-listener/dedup"
	"github.com/lightwebinc/bitcoin-shard-listener/egress"
	"github.com/lightwebinc/bitcoin-shard-listener/filter"
	"github.com/lightwebinc/bitcoin-shard-listener/metrics"
	"github.com/lightwebinc/bitcoin-shard-listener/nack"
	"github.com/lightwebinc/bitcoin-shard-listener/reassembly"
)

const (
	recvBufSize = 4 * 1024 * 1024 // per-worker UDP receive buffer

	// socketRecvBuf is the UDP receive buffer requested on each worker socket.
	socketRecvBuf = 64 * 1024 * 1024 // 64 MiB
)

// Worker is a single multicast receive goroutine.
type Worker struct {
	id                int
	iface             *net.Interface
	port              int
	groups            []*net.UDPAddr // multicast groups to join
	engine            *shard.Engine
	filt              *filter.Filter
	egr               *egress.Sender
	mcastEgr          *egress.MCastSender // nil when multicast egress is disabled
	headerEgr         *egress.Sender      // nil when unicast header egress is disabled
	headerMCastEgr    *egress.MCastSender // nil when multicast header egress is disabled
	tracker           *nack.Tracker
	rec               *metrics.Recorder
	debug             bool
	verifyPayloadHash bool
	senderACL         *filter.SenderACL  // nil = accept every source
	dedupSet          *dedup.Set         // nil = dedup disabled
	reassemBuf        *reassembly.Buffer // nil = BRC-130 disabled
	log               *slog.Logger
}

// SetEgressDedup attaches a duplicate-suppression set keyed on
// (groupIdx, subtreeID, SeqNum). When set, retransmits whose key was already
// forwarded recently are dropped before egress. nil disables dedup. Defaults
// to disabled.
func (w *Worker) SetEgressDedup(s *dedup.Set) {
	w.dedupSet = s
}

// SetReassemblyBuffer attaches a BRC-130 fragment reassembly buffer to the
// worker. When set, BRC-130 fragment datagrams are routed to the buffer
// instead of being forwarded directly. Completed reassemblies are delivered
// through the buffer's callback. nil disables BRC-130 handling (fragments
// are dropped as unknown-version frames).
func (w *Worker) SetReassemblyBuffer(b *reassembly.Buffer) {
	w.reassemBuf = b
}

// New constructs a Worker. mcastEgr may be nil to disable multicast egress.
func New(
	id int,
	iface *net.Interface,
	port int,
	groups []*net.UDPAddr,
	engine *shard.Engine,
	filt *filter.Filter,
	egr *egress.Sender,
	mcastEgr *egress.MCastSender,
	tracker *nack.Tracker,
	rec *metrics.Recorder,
	debug bool,
) *Worker {
	return &Worker{
		id:       id,
		iface:    iface,
		port:     port,
		groups:   groups,
		engine:   engine,
		filt:     filt,
		egr:      egr,
		mcastEgr: mcastEgr,
		tracker:  tracker,
		rec:      rec,
		debug:    debug,
		log:      slog.Default().With("component", "listener", "worker", id),
	}
}

// SetHeaderEgress attaches a unicast sender for stripped block header
// retransmission. When set, BlockAnnounce frames trigger extraction of
// the 80-byte block header and re-encoding as a 172-byte stripped
// BRC-131 frame sent to the configured downstream. nil disables.
func (w *Worker) SetHeaderEgress(s *egress.Sender) { w.headerEgr = s }

// SetHeaderMCastEgress attaches a multicast sender for stripped block
// header retransmission to the CtrlGroupBlockHeader (0xFFFA) multicast
// group. nil disables.
func (w *Worker) SetHeaderMCastEgress(s *egress.MCastSender) { w.headerMCastEgr = s }

// SetSenderACL attaches a CIDR-based sender filter. When set, datagrams whose
// IPv6 source address is rejected by the ACL are dropped before decode and
// counted under bsl_frames_dropped_total{reason="sender_filter"}. The same
// ACL is shared with the BRC-127 announcement listener so trust boundaries
// are configured once. nil (default) accepts every source.
func (w *Worker) SetSenderACL(a *filter.SenderACL) {
	w.senderACL = a
}

// SetVerifyPayloadHash toggles SHA256d(payload)==TxID verification on
// BRC-124/BRC-128 frames. When true, frames whose payload hash does not match
// their TxID are dropped before egress and gap tracking, and
// bsl_frames_invalid_payload_total is incremented. Defaults to false.
func (w *Worker) SetVerifyPayloadHash(v bool) {
	w.verifyPayloadHash = v
}

// Run opens a SO_REUSEPORT socket, joins all multicast groups, and processes
// frames until ctx is cancelled.
//
// The socket is created via raw syscalls so it is never registered with Go's
// internal edge-triggered epoll. Blocking Recvfrom is used so the OS thread
// parks in the kernel and wakes the moment a datagram arrives, with zero
// scheduler overhead between the wakeup and the read.
func (w *Worker) Run(ctx context.Context) error {
	fd, err := openRawSocket(w.port)
	if err != nil {
		return fmt.Errorf("worker %d: open socket: %w", w.id, err)
	}

	for _, grp := range w.groups {
		mreq := &unix.IPv6Mreq{Interface: uint32(w.iface.Index)}
		copy(mreq.Multiaddr[:], grp.IP.To16())
		if err := unix.SetsockoptIPv6Mreq(fd, unix.IPPROTO_IPV6, unix.IPV6_JOIN_GROUP, mreq); err != nil {
			_ = unix.Close(fd)
			return fmt.Errorf("worker %d: join group %s: %w", w.id, grp.IP, err)
		}
	}

	if w.rec != nil {
		w.rec.WorkerReady()
		defer w.rec.WorkerDone()
	}

	w.log.Info("listener worker ready", "iface", w.iface.Name, "port", w.port, "groups", len(w.groups))

	// SO_RCVTIMEO makes Recvfrom wake up periodically so we can check ctx.
	// This is the reliable shutdown mechanism: closing the fd from another
	// goroutine is POSIX-undefined and does not always unblock recvfrom on
	// all Linux kernel versions. Keep the fd-close goroutine as a fast path
	// for kernels that do support it.
	tv := unix.NsecToTimeval((200 * time.Millisecond).Nanoseconds())
	_ = unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv)

	go func() {
		<-ctx.Done()
		_ = unix.Close(fd)
	}()

	buf := make([]byte, recvBufSize)
	for {
		n, from, err := unix.Recvfrom(fd, buf, 0)
		if err != nil {
			if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
				if ctx.Err() != nil {
					return nil
				}
				continue
			}
			if err == unix.EBADF || err == unix.EINVAL {
				return nil
			}
			if err == unix.EINTR {
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			w.log.Error("recvfrom error", "err", err)
			continue
		}
		if n > 0 {
			if w.senderACL != nil {
				ip := sockaddrIP(from)
				if !w.senderACL.Allow(ip) {
					if w.rec != nil {
						w.rec.FrameDropped(w.id, "sender_filter")
					}
					if w.debug {
						w.log.Debug("sender filter rejected", "src", ip)
					}
					continue
				}
			}
			w.processFrame(buf[:n])
		}
	}
}

func (w *Worker) processFrame(raw []byte) {
	// BRC-131 block control frame (FrameVer 0x04): route to block handler.
	if frame.IsBlockFrame(raw) {
		w.processBlockFrame(raw)
		return
	}

	// BRC-132 subtree data frame (FrameVer 0x05): route to subtree data handler.
	if frame.IsSubtreeDataFrame(raw) {
		w.processSubtreeDataFrame(raw)
		return
	}

	// BRC-134 anchor transaction frame (FrameVer 0x06): route to anchor handler.
	if frame.IsAnchorFrame(raw) {
		w.processAnchorFrame(raw)
		return
	}

	// BRC-130 fragment: route to reassembly buffer and return.
	if frame.IsFragment(raw) {
		if w.reassemBuf == nil {
			if w.rec != nil {
				w.rec.FrameDropped(w.id, "no_reassembly_buffer")
			}
			return
		}
		ff, err := frame.DecodeFragment(raw)
		if err != nil {
			if w.rec != nil {
				w.rec.FrameDropped(w.id, "frag_decode_error")
			}
			if w.debug {
				w.log.Debug("BRC-130 decode error", "err", err)
			}
			return
		}
		w.reassemBuf.Observe(ff)
		return
	}

	f, err := frame.Decode(raw)
	if err != nil {
		if w.rec != nil {
			w.rec.FrameDropped(w.id, "decode_error")
		}
		if w.debug {
			w.log.Debug("decode error", "err", err, "len", len(raw))
		}
		return
	}

	if w.rec != nil {
		ver := "brc12"
		if f.Version == frame.FrameVerV2 {
			ver = "brc124"
		}
		w.rec.FrameReceived(w.id, w.iface.Name, ver)
	}

	// Optional payload-hash verification (GAP-2). Only meaningful for V2
	// frames (BRC-12 has no chain semantics; the TxID is still the BSV
	// double-SHA256 of the payload but legacy frames are forwarded verbatim
	// regardless). When disabled, this branch is skipped entirely.
	if w.verifyPayloadHash && f.Version == frame.FrameVerV2 {
		first := sha256.Sum256(f.Payload)
		second := sha256.Sum256(first[:])
		if second != f.TxID {
			if w.rec != nil {
				w.rec.FrameInvalidPayload(w.id)
			}
			if w.debug {
				w.log.Debug("payload hash mismatch",
					"txid_prefix", fmt.Sprintf("%x", f.TxID[:8]),
					"computed_prefix", fmt.Sprintf("%x", second[:8]),
					"payload_len", len(f.Payload),
				)
			}
			return
		}
	}

	groupIdx := w.engine.GroupIndex(&f.TxID)

	if allow, reason := w.filt.Allow(groupIdx, f); !allow {
		if w.rec != nil {
			w.rec.FrameDropped(w.id, reason)
		}
		return
	}

	// Egress duplicate suppression (GAP-3): when an inline frame and its
	// retransmit both reach the listener (common at 1+% loss with a warm
	// retry endpoint), forward only the first. Gap-state suppression in
	// nack.Tracker.Observe is independent and still runs below.
	if w.dedupSet != nil && f.Version == frame.FrameVerV2 && f.SeqNum != 0 {
		if w.dedupSet.SeenAndAdd(dedup.Key{GroupIdx: groupIdx, SubtreeID: f.SubtreeID, SeqNum: f.SeqNum}) {
			if w.rec != nil {
				w.rec.FrameDeduped(w.id)
			}
			// Skip egress, but still let the tracker observe the frame so
			// gap-fill bookkeeping stays accurate.
			if w.tracker != nil {
				w.tracker.Observe(groupIdx, f.SubtreeID, f.HashKey, f.SeqNum, f.TxID)
			}
			return
		}
	}

	if err := w.egr.Send(raw, f); err != nil {
		if w.rec != nil {
			w.rec.EgressError(w.id)
		}
		w.log.Debug("egress send error", "err", err)
	} else {
		if w.rec != nil {
			w.rec.FrameForwarded(w.id, w.egr.Proto())
		}
	}

	// Multicast egress fan-out: fires independently of unicast outcome.
	if w.mcastEgr != nil {
		if err := w.mcastEgr.Send(raw, f, groupIdx); err != nil {
			if w.rec != nil {
				w.rec.MCEgressError(w.id)
			}
			w.log.Debug("mc egress send error", "err", err)
		} else {
			if w.rec != nil {
				w.rec.FrameForwarded(w.id, w.mcastEgr.Proto())
			}
		}
	}

	// Gap tracking: BRC-124/BRC-128 only, SeqNum must be non-zero (proxy-stamped).
	if w.tracker != nil && f.Version == frame.FrameVerV2 && f.SeqNum != 0 {
		w.tracker.Observe(groupIdx, f.SubtreeID, f.HashKey, f.SeqNum, f.TxID)
	}

	if w.debug {
		w.log.Debug("frame forwarded",
			"version", f.Version,
			"group", groupIdx,
			"seq_num", f.SeqNum,
		)
	}
}

// processBlockFrame handles BRC-131 block control frames (FrameVer 0x04).
// Block frames bypass shard/subtree filtering (they carry block metadata, not
// transactions) and are forwarded directly to egress. Gap tracking is performed
// on the block control flow so NACK-based retransmission can recover lost
// block announcements.
func (w *Worker) processBlockFrame(raw []byte) {
	bf, err := frame.DecodeBlock(raw)
	if err != nil {
		if w.rec != nil {
			w.rec.FrameDropped(w.id, "decode_error")
		}
		if w.debug {
			w.log.Debug("block frame decode error", "err", err, "len", len(raw))
		}
		return
	}

	if w.rec != nil {
		w.rec.FrameReceived(w.id, w.iface.Name, "brc131")
	}

	if err := w.egr.SendBlock(raw, bf); err != nil {
		if w.rec != nil {
			w.rec.EgressError(w.id)
		}
		w.log.Debug("block egress send error", "err", err)
	} else {
		if w.rec != nil {
			w.rec.FrameForwarded(w.id, w.egr.Proto())
		}
	}

	// Block header egress: extract and retransmit the 80-byte header.
	if w.headerEgr != nil || w.headerMCastEgr != nil {
		w.emitBlockHeader(bf)
	}

	// Gap tracking on the control flow uses a zero SubtreeID.
	if w.tracker != nil && bf.SeqNum != 0 {
		var zeroSub [32]byte
		w.tracker.Observe(uint32(shard.CtrlGroupControl), zeroSub, bf.HashKey, bf.SeqNum, bf.ContentID)
	}

	if w.debug {
		w.log.Debug("block frame forwarded",
			"msg_type", bf.MsgType,
			"content_id", fmt.Sprintf("%x", bf.ContentID[:8]),
			"seq_num", bf.SeqNum,
		)
	}
}

// emitBlockHeader extracts the 80-byte block header from a BlockAnnounce
// payload, re-encodes it as a stripped BRC-131 frame (92B header + 80B
// payload = 172B), and sends it to configured header egress endpoints.
// The BRC-131 header preserves ContentID (block hash), HashKey (sender
// attribution), and SeqNum (per-sender ordering) so downstream SPV
// consumers can track multiple chain tips from competing miners.
func (w *Worker) emitBlockHeader(bf *frame.BlockFrame) {
	if bf.MsgType != frame.BlockMsgAnnounce {
		return
	}
	if len(bf.Payload) < frame.BlockHeaderSize {
		return
	}

	// Build a stripped BlockFrame: payload = just the 80-byte header.
	stripped := &frame.BlockFrame{
		MsgType:   bf.MsgType,
		ContentID: bf.ContentID,
		HashKey:   bf.HashKey,
		SeqNum:    bf.SeqNum,
		Payload:   bf.Payload[:frame.BlockHeaderSize],
	}
	buf := make([]byte, frame.HeaderSize+frame.BlockHeaderSize)
	if _, err := frame.EncodeBlock(stripped, buf); err != nil {
		w.log.Debug("header egress encode error", "err", err)
		return
	}

	// Unicast header egress.
	if w.headerEgr != nil {
		if err := w.headerEgr.SendRaw(buf); err != nil {
			if w.rec != nil {
				w.rec.HeaderEgressError(w.id)
			}
			w.log.Debug("header egress send error", "err", err)
		} else if w.rec != nil {
			w.rec.HeaderForwarded(w.id)
		}
	}

	// Multicast header egress.
	if w.headerMCastEgr != nil {
		if err := w.headerMCastEgr.SendToGroup(buf, shard.CtrlGroupBlockHeader); err != nil {
			if w.rec != nil {
				w.rec.HeaderEgressError(w.id)
			}
			w.log.Debug("header mc egress send error", "err", err)
		} else if w.rec != nil {
			w.rec.HeaderForwarded(w.id)
		}
	}
}

// processSubtreeDataFrame handles BRC-132 subtree data frames (FrameVer 0x05).
// Subtree data frames bypass shard/subtree filtering and are forwarded directly
// to egress. Gap tracking is performed on the (HashKey, 0xFFFB, subtreeID) flow.
func (w *Worker) processSubtreeDataFrame(raw []byte) {
	sf, err := frame.DecodeSubtreeData(raw)
	if err != nil {
		if w.rec != nil {
			w.rec.FrameDropped(w.id, "decode_error")
		}
		if w.debug {
			w.log.Debug("subtree data frame decode error", "err", err, "len", len(raw))
		}
		return
	}

	if w.rec != nil {
		w.rec.FrameReceived(w.id, w.iface.Name, "brc132")
	}

	if err := w.egr.SendSubtreeData(raw, sf); err != nil {
		if w.rec != nil {
			w.rec.EgressError(w.id)
		}
		w.log.Debug("subtree data egress send error", "err", err)
	} else {
		if w.rec != nil {
			w.rec.FrameForwarded(w.id, w.egr.Proto())
		}
	}

	// Gap tracking on the subtree data flow uses SubtreeID as the flow scope.
	if w.tracker != nil && sf.SeqNum != 0 {
		w.tracker.Observe(uint32(shard.CtrlGroupSubtreeAnnounce), sf.SubtreeID, sf.HashKey, sf.SeqNum, sf.SubtreeID)
	}

	if w.debug {
		w.log.Debug("subtree data frame forwarded",
			"msg_type", sf.MsgType,
			"subtree_id", fmt.Sprintf("%x", sf.SubtreeID[:8]),
			"seq_num", sf.SeqNum,
		)
	}
}

// processAnchorFrame handles BRC-134 chained anchor transaction frames
// (FrameVer 0x06). Anchor frames bypass shard/subtree filtering and are
// forwarded directly to egress. Gap tracking is performed on the control flow
// so NACK-based retransmission can recover lost anchor frames.
func (w *Worker) processAnchorFrame(raw []byte) {
	f, err := frame.DecodeAnchor(raw)
	if err != nil {
		if w.rec != nil {
			w.rec.FrameDropped(w.id, "decode_error")
		}
		if w.debug {
			w.log.Debug("anchor frame decode error", "err", err, "len", len(raw))
		}
		return
	}

	if w.rec != nil {
		w.rec.FrameReceived(w.id, w.iface.Name, "brc134")
	}

	if err := w.egr.Send(raw, f); err != nil {
		if w.rec != nil {
			w.rec.EgressError(w.id)
		}
		w.log.Debug("anchor egress send error", "err", err)
	} else {
		if w.rec != nil {
			w.rec.FrameForwarded(w.id, w.egr.Proto())
		}
	}

	// Gap tracking uses a virtual anchor groupIdx (0xFFF9) that matches the
	// proxy's HashKey derivation for BRC-134 frames. This gives anchors their
	// own independent flow label ("brc134") in the gap tracker.
	if w.tracker != nil && f.SeqNum != 0 {
		const anchorGroupIdx = uint32(0xFFF9)
		var zeroSub [32]byte
		w.tracker.Observe(anchorGroupIdx, zeroSub, f.HashKey, f.SeqNum, f.TxID)
	}

	if w.debug {
		w.log.Debug("anchor frame forwarded",
			"txid", fmt.Sprintf("%x", f.TxID[:8]),
			"seq_num", f.SeqNum,
		)
	}
}

// DeliverReassembledBlock is the reassembly.BlockCallback invoked when a V4
// (BRC-131) fragment set completes reassembly. bf carries the reconstructed
// block frame metadata; the payload is the full reassembled BRC-131 payload.
// This method is called with the Buffer's lock held.
func (w *Worker) DeliverReassembledBlock(payload []byte, bf *frame.BlockFrame) {
	if w.rec != nil {
		w.rec.ReassemblyCompleted()
	}

	// Re-encode as BRC-131 so the Sender has a valid wire buffer.
	raw := make([]byte, frame.HeaderSize+len(payload))
	if _, err := frame.EncodeBlock(bf, raw); err != nil {
		if w.debug {
			w.log.Debug("reassembled block frame encode error", "err", err)
		}
		return
	}

	if w.rec != nil {
		w.rec.FrameReceived(w.id, w.iface.Name, "brc131_reassembled")
	}

	if err := w.egr.SendBlock(raw, bf); err != nil {
		if w.rec != nil {
			w.rec.EgressError(w.id)
		}
		w.log.Debug("reassembled block egress send error", "err", err)
	} else {
		if w.rec != nil {
			w.rec.FrameForwarded(w.id, w.egr.Proto())
		}
	}

	// Block header egress: extract and retransmit the 80-byte header.
	if w.headerEgr != nil || w.headerMCastEgr != nil {
		w.emitBlockHeader(bf)
	}

	if w.tracker != nil && bf.SeqNum != 0 {
		var zeroSub [32]byte
		w.tracker.Observe(uint32(shard.CtrlGroupControl), zeroSub, bf.HashKey, bf.SeqNum, bf.ContentID)
	}

	if w.debug {
		w.log.Debug("reassembled block frame forwarded",
			"msg_type", bf.MsgType,
			"content_id", fmt.Sprintf("%x", bf.ContentID[:8]),
		)
	}
}

// DeliverReassembledSubtreeData is the reassembly.SubtreeDataCallback invoked
// when a V5 (BRC-132) fragment set completes reassembly. sf carries the
// reconstructed subtree data frame metadata. SHA256d verification is never
// applied for V5 slots. This method is called with the Buffer's lock held.
func (w *Worker) DeliverReassembledSubtreeData(payload []byte, sf *frame.SubtreeDataFrame) {
	if w.rec != nil {
		w.rec.ReassemblyCompleted()
	}

	// Re-encode as BRC-132 so the Sender has a valid wire buffer.
	raw := make([]byte, frame.HeaderSize+len(payload))
	if _, err := frame.EncodeSubtreeData(sf, raw); err != nil {
		if w.debug {
			w.log.Debug("reassembled subtree data frame encode error", "err", err)
		}
		return
	}

	if w.rec != nil {
		w.rec.FrameReceived(w.id, w.iface.Name, "brc132_reassembled")
	}

	if err := w.egr.SendSubtreeData(raw, sf); err != nil {
		if w.rec != nil {
			w.rec.EgressError(w.id)
		}
		w.log.Debug("reassembled subtree data egress send error", "err", err)
	} else {
		if w.rec != nil {
			w.rec.FrameForwarded(w.id, w.egr.Proto())
		}
	}

	if w.tracker != nil && sf.SeqNum != 0 {
		w.tracker.Observe(uint32(shard.CtrlGroupSubtreeAnnounce), sf.SubtreeID, sf.HashKey, sf.SeqNum, sf.SubtreeID)
	}

	if w.debug {
		w.log.Debug("reassembled subtree data frame forwarded",
			"msg_type", sf.MsgType,
			"subtree_id", fmt.Sprintf("%x", sf.SubtreeID[:8]),
		)
	}
}

// DeliverReassembled is the reassembly.Callback: it receives a completed,
// payload-verified Frame (synthetic BRC-124) and routes it through filter,
// egress, and gap tracking exactly as a normal inline frame would be.
// raw is re-encoded here so downstream egress code receives a valid wire buffer.
//
// This method is called from within the reassembly.Buffer's mutex; it must not
// call back into the buffer.
func (w *Worker) DeliverReassembled(payload []byte, f *frame.Frame) {
	groupIdx := w.engine.GroupIndex(&f.TxID)

	if w.rec != nil {
		w.rec.ReassemblyCompleted()
	}

	if allow, reason := w.filt.Allow(groupIdx, f); !allow {
		if w.rec != nil {
			w.rec.FrameDropped(w.id, reason)
		}
		return
	}

	if w.rec != nil {
		w.rec.FrameReceived(w.id, w.iface.Name, "brc130")
	}

	// Re-encode as BRC-124 so the Sender has a valid wire buffer.
	raw := make([]byte, frame.HeaderSize+len(payload))
	if _, err := frame.Encode(f, raw); err != nil {
		if w.debug {
			w.log.Debug("reassembled frame encode error", "err", err)
		}
		return
	}

	if err := w.egr.Send(raw, f); err != nil {
		if w.rec != nil {
			w.rec.EgressError(w.id)
		}
	} else {
		if w.rec != nil {
			w.rec.FrameForwarded(w.id, w.egr.Proto())
		}
	}

	if w.mcastEgr != nil {
		if err := w.mcastEgr.Send(raw, f, groupIdx); err != nil {
			if w.rec != nil {
				w.rec.MCEgressError(w.id)
			}
		} else {
			if w.rec != nil {
				w.rec.FrameForwarded(w.id, w.mcastEgr.Proto())
			}
		}
	}

	if w.tracker != nil && f.SeqNum != 0 {
		w.tracker.Observe(groupIdx, f.SubtreeID, f.HashKey, f.SeqNum, f.TxID)
	}
}

// sockaddrIP extracts the source IP from a unix.Sockaddr returned by
// Recvfrom on an AF_INET6 socket. Dual-stack sockets surface IPv4 sources
// as IPv4-mapped IPv6 addresses; returning the raw 16-byte form lets
// net.IPNet.Contains match either an IPv6 or IPv4 CIDR via Go's normal
// IPv4-in-IPv6 handling.
func sockaddrIP(sa unix.Sockaddr) net.IP {
	if sa6, ok := sa.(*unix.SockaddrInet6); ok {
		ip := make(net.IP, 16)
		copy(ip, sa6.Addr[:])
		return ip
	}
	return nil
}

// openRawSocket creates a UDP6 socket with SO_REUSEPORT bound to [::]:port
// using raw syscalls, bypassing Go's net package so the fd is never registered
// with Go's internal edge-triggered epoll.
func openRawSocket(port int) (int, error) {
	fd, err := unix.Socket(unix.AF_INET6, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("socket: %w", err)
	}
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
		_ = unix.Close(fd)
		return -1, fmt.Errorf("SO_REUSEPORT: %w", err)
	}
	// Receive buffer: ignore error — kernel silently caps at rmem_max.
	_ = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF, socketRecvBuf)
	sa := &unix.SockaddrInet6{Port: port}
	if err := unix.Bind(fd, sa); err != nil {
		_ = unix.Close(fd)
		return -1, fmt.Errorf("bind [::]::%d: %w", port, err)
	}
	return fd, nil
}
