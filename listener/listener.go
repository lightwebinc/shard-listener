// Package listener implements the multicast receive workers for
// shard-listener.
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
	"encoding/binary"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"github.com/lightwebinc/shard-common/bundle"
	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/netjoin"
	"github.com/lightwebinc/shard-common/pow"
	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/shard-listener/dedup"
	"github.com/lightwebinc/shard-listener/egress"
	"github.com/lightwebinc/shard-listener/filter"
	"github.com/lightwebinc/shard-listener/metrics"
	"github.com/lightwebinc/shard-listener/nack"
	"github.com/lightwebinc/shard-listener/reassembly"
	"github.com/lightwebinc/shard-listener/txdedup"
)

const (
	recvBufSize = 4 * 1024 * 1024 // per-worker UDP receive buffer

	// socketRecvBuf is the UDP receive buffer requested on each worker socket.
	socketRecvBuf = 64 * 1024 * 1024 // 64 MiB

	// rebucketMaxBytes caps a re-bucketed bundle datagram (BRC-142 §Re-bucketing).
	// Re-bucketed children are delivered whole only to bundle-capable consumers, so
	// the public-internet MTU baseline (1500) is the safe size; edge-decoalesced
	// delivery splits them regardless of this cap.
	rebucketMaxBytes = 1500
)

// Worker is a single multicast receive goroutine.
// GroupSources maps a multicast group (16-byte IPv6 in netip.Addr form)
// to its SSM source list. Groups absent from the map, or with empty
// source lists, are joined ASM-style via IPV6_JOIN_GROUP. Each control
// group's source list is the matching sources.bootstrap.<group> bucket;
// the data-plane source list is the manifest-derived publisher union
// (or sources.static for lab/CI).
type GroupSources map[netip.Addr][]netip.Addr

type Worker struct {
	id                int
	iface             *net.Interface
	port              int
	groups            []*net.UDPAddr // multicast groups to join at startup
	groupSources      GroupSources   // optional SSM per-group source map
	engine            *shard.Engine
	filt              *filter.Filter
	egr               egress.EgressSink   // default *egress.Sender; swappable fan-out sink
	mcastEgr          *egress.MCastSender // nil when multicast egress is disabled
	headerEgr         *egress.Sender      // nil when unicast header egress is disabled
	headerMCastEgr    *egress.MCastSender // nil when multicast header egress is disabled
	headerFanout      egress.HeaderSink   // nil when no per-consumer header lane exists (OSS default)
	headerHashKey     uint64              // BRC-135 emitter HashKey (XXH64(emitterIPv6 ∥ 0xFFFA ∥ zeros))
	headerSeqNum      atomic.Uint64       // BRC-135 monotonic per-emitter counter (starts at 1)
	tracker           *nack.Tracker
	rec               *metrics.Recorder
	debug             bool
	verifyPayloadHash bool
	senderACL         *filter.SenderACL     // nil = accept every source
	dedupSet          *dedup.Set            // nil = dedup disabled
	txDedup           *txdedup.Store        // nil = cross-listener TxID dedup disabled
	reassemBuf        *reassembly.Buffer    // nil = BRC-130 disabled
	beefEngine        *shard.PlaneEngine    // nil = BRC-148 BEEF plane disabled (V9 frames dropped)
	beefTopics        map[[32]byte]struct{} // worker-level topic election; empty = admit all topics
	beefVersions      map[uint32]struct{}   // accepted BEEF version words; empty = admit all
	beefVerifyContent bool                  // debug: verify ContentID == SHA-256d(payload)
	requireBlockPoW   bool                  // gate BRC-131 announces on header PoW before fan-out
	powFloor          *big.Int              // PoW difficulty floor; nil = self-consistency only
	coinbaseCorr      *CoinbaseCorrelator   // shared; nil = no coinbase correlation
	log               *slog.Logger

	// curSource is the source address of the frame currently being processed,
	// set under procMu before processFrame so the gap tracker can attribute
	// per-source loss. nil for tracker-reinjected (recovered) frames. Kept as
	// net.IP (not a string) so the hot RX path does no per-frame stringification
	// — the address is stringified only at the rare gap/NACK metric-emit sites.
	curSource net.IP

	// Runtime join management for BRC-139 auto-join and live-resharding
	// bridging mode. joinFd is the worker's IPv6 multicast socket; it is
	// set in Run after openRawSocket succeeds, and -1 before Run and
	// after Run returns. joinMu guards joinedGroups and the underlying
	// setsockopt calls so concurrent AddGroup/RemoveGroup calls from
	// auto-config / applier goroutines do not race.
	joinMu       sync.Mutex
	joinFd       int                     // -1 when worker is not running
	joinedGroups map[netip.Addr]struct{} // currently-joined data-plane groups

	// procMu serialises processFrame so a recovered frame re-injected by the
	// NACK tracker (via Reinject, on the tracker goroutine) is mutually
	// exclusive with the Run receive loop. The lock is uncontended on the hot
	// path (single-worker receive) and only meets the rare unicast-recovery
	// re-injection.
	procMu sync.Mutex

	// egressSeq assigns a monotonic per-bundle-flow (keyed by the bundle
	// HashKey) egress SeqNum to decoalesced BRC-142 members: the per-tx SeqNum
	// does not survive coalescing, so the edge re-stamps a fresh sequence so
	// downstream consumers retain gap detection. Accessed only from
	// processBundle, which runs under procMu, so it needs no further locking.
	egressSeq map[uint64]uint64

	// rebucketer re-coalesces a bundle built at a different ShardBits generation
	// into this listener's generation (BRC-142 §Re-bucketing) before delivery, so
	// cross-generation / re-shard bundles route and filter at the local groups
	// rather than over-delivering to finer subscribers. Lazily built on the first
	// mismatched bundle; its per-flow SeqNum map persists across bundles so the
	// re-bucketed child streams stay monotonic. Used only from processBundle
	// (under procMu). nil until first needed (the same-generation common case
	// never touches it).
	rebucketer *bundle.Rebucketer

	// relay marks this listener an intentional re-bucket relay (-rebucket-relay).
	// When false (a bare edge listener), re-bucketing raises the
	// bsl_rebucket_unguarded alarm; warnedRebucket gates a one-shot WARN so the
	// generation-mismatch misconfig is logged loudly once, not per bundle.
	relay          bool
	warnedRebucket bool
}

// SetGroupSources configures per-group SSM source lists for the data-plane
// join. Must be called before [Worker.Run]. Groups absent from src (or
// with empty source lists) are joined ASM-style.
func (w *Worker) SetGroupSources(src GroupSources) {
	w.groupSources = src
}

// SetEgressDedup attaches a duplicate-suppression set keyed on
// (groupIdx, subtreeID, SeqNum). When set, retransmits whose key was already
// forwarded recently are dropped before egress. nil disables dedup. Defaults
// to disabled.
func (w *Worker) SetEgressDedup(s *dedup.Set) {
	w.dedupSet = s
}

// SetTxDedup attaches a Redis-backed cross-listener TxID dedup store. When
// set, each BRC-124/BRC-128 or BRC-134 frame races to claim its TxID in
// Redis; only the first listener to claim it forwards egress. nil disables
// cross-listener dedup. Defaults to disabled.
func (w *Worker) SetTxDedup(s *txdedup.Store) {
	w.txDedup = s
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
	egr egress.EgressSink,
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
		joinFd:   -1,
		log:      slog.Default().With("component", "listener", "worker", id),
	}
}

// SetHeaderEgress attaches a unicast sender for BRC-135 block header
// retransmission. When set, BlockAnnounce frames trigger extraction of
// the 80-byte block header and re-encoding as a 172-byte BRC-135 frame
// (FrameVer 0x07) sent to the configured downstream. nil disables.
func (w *Worker) SetHeaderEgress(s *egress.Sender) { w.headerEgr = s }

// SetHeaderMCastEgress attaches a multicast sender for BRC-135 block
// header retransmission to the GroupBlockHeader (0xFFFA) multicast
// group. nil disables.
func (w *Worker) SetHeaderMCastEgress(s *egress.MCastSender) { w.headerMCastEgr = s }

// SetHeaderFanout attaches the per-consumer BRC-135 header seam: a routing sink
// that offers headers as an ELECTED class, the way blocks, subtree data, and
// BEEF objects are already offered. nil (the OSS default) disables it, leaving
// only the node-global unicast/multicast header egress above.
//
// Unlike those two, this seam is set unconditionally by a fan-out build rather
// than by an operator flag: whether a header crosses any consumer wire is the
// consumer's election, decided downstream in the class router, not the node's
// configuration. The cost of feeding it when nobody has elected is one 172-byte
// encode per block announce — roughly one per ten minutes — which is why it
// needs no enabling flag of its own.
//
// It is attached separately from the main egress sink because a sink may
// implement [egress.HeaderSink] and still not want headers (a receiver-tier
// MultiSink already forwards the whole BRC-131 announce, from which the
// delivery tier derives its own headers; feeding both would double-emit).
func (w *Worker) SetHeaderFanout(s egress.HeaderSink) { w.headerFanout = s }

// SetRebucketRelay marks this listener an intentional BRC-142 re-bucket relay.
// A relay re-buckets without raising the unguarded-rebucket alarm; a bare
// listener (default) raises it, since a generation mismatch is a misconfig.
func (w *Worker) SetRebucketRelay(relay bool) { w.relay = relay }

// SetHeaderEmitterIdentity sets the BRC-135 emitter HashKey for block
// header egress. HashKey is the stable per-emitter flow identifier
// computed once as XXH64(emitterIPv6 ∥ 0xFFFA ∥ zeros[32]) — the
// GroupBlockHeader index matches the actual BRC-135 egress group.
// It is stamped into every BRC-135 frame emitted by this worker. The
// companion SeqNum counter is reset to start at 1 on the next emission.
func (w *Worker) SetHeaderEmitterIdentity(hashKey uint64) {
	w.headerHashKey = hashKey
	w.headerSeqNum.Store(0)
}

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

// SetBEEF wires the BRC-148 BEEF object plane: the plane-aware derivation
// engine, the worker-level topic election (TopicIDs; empty = admit all —
// aggregator mode), the accepted encoding version words (empty = admit all),
// and the optional ContentID verification (debug/test support, the BEEF
// analogue of SetVerifyPayloadHash). Call before Run.
func (w *Worker) SetBEEF(pe *shard.PlaneEngine, topics map[[32]byte]struct{}, versions map[uint32]struct{}, verifyContent bool) {
	w.beefEngine = pe
	w.beefTopics = topics
	w.beefVersions = versions
	w.beefVerifyContent = verifyContent
}

// SetBlockPoW enables the block-control gate before fan-out. Inter-domain block
// announcements reach the listener by multicast without passing our proxy, so
// the listener must independently validate them. When require is true, a
// BRC-131 BlockAnnounce is forwarded only if its in-frame 80-byte header
// satisfies proof of work at a target no easier than floorBits (Bitcoin
// compact nBits; 0 = self-consistency only), and a BRC-133 coinbase is
// forwarded only if corr holds its TxID (recorded from a validated block).
// corr is shared across workers; pass nil to skip coinbase correlation.
// Validates the artifact, not the emitter — permissionless. Must be called
// before Run.
func (w *Worker) SetBlockPoW(require bool, floorBits uint32, corr *CoinbaseCorrelator) {
	w.requireBlockPoW = require
	w.coinbaseCorr = corr
	if require && floorBits != 0 {
		w.powFloor = pow.CompactToTarget(floorBits)
	} else {
		w.powFloor = nil
	}
}

// AddGroup joins the worker's socket to the given multicast group at
// runtime (BRC-139 auto-join and live-resharding bridging-mode primitive).
// sources is the SSM source filter; pass nil/empty for an ASM join.
//
// Safe to call concurrently from goroutines other than the receive loop.
// Returns an error if the worker has not started yet or has already
// stopped.
func (w *Worker) AddGroup(group netip.Addr, sources []netip.Addr) error {
	w.joinMu.Lock()
	defer w.joinMu.Unlock()
	if w.joinFd < 0 {
		return fmt.Errorf("worker %d: AddGroup before Run", w.id)
	}
	if _, ok := w.joinedGroups[group]; ok {
		return nil // idempotent
	}
	if err := netjoin.Join(w.joinFd, w.iface.Index, group, sources); err != nil {
		return fmt.Errorf("worker %d: AddGroup %s: %w", w.id, group, err)
	}
	w.joinedGroups[group] = struct{}{}
	return nil
}

// RemoveGroup leaves the given multicast group at runtime. Safe to call
// concurrently. Idempotent (no-op if the group is not currently joined).
// Returns an error if the worker has not started yet or has already
// stopped.
func (w *Worker) RemoveGroup(group netip.Addr, sources []netip.Addr) error {
	w.joinMu.Lock()
	defer w.joinMu.Unlock()
	if w.joinFd < 0 {
		return fmt.Errorf("worker %d: RemoveGroup before Run", w.id)
	}
	if _, ok := w.joinedGroups[group]; !ok {
		return nil
	}
	if err := netjoin.Leave(w.joinFd, w.iface.Index, group, sources); err != nil {
		return fmt.Errorf("worker %d: RemoveGroup %s: %w", w.id, group, err)
	}
	delete(w.joinedGroups, group)
	return nil
}

// JoinedGroups returns a snapshot of currently-joined multicast groups.
// Returned slice is owned by the caller. Returns nil when the worker is
// not running.
func (w *Worker) JoinedGroups() []netip.Addr {
	w.joinMu.Lock()
	defer w.joinMu.Unlock()
	if w.joinedGroups == nil {
		return nil
	}
	out := make([]netip.Addr, 0, len(w.joinedGroups))
	for g := range w.joinedGroups {
		out = append(out, g)
	}
	return out
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

	w.joinMu.Lock()
	w.joinFd = fd
	w.joinedGroups = make(map[netip.Addr]struct{})
	w.joinMu.Unlock()
	defer func() {
		w.joinMu.Lock()
		w.joinFd = -1
		w.joinedGroups = nil
		w.joinMu.Unlock()
	}()

	for _, grp := range w.groups {
		ga, ok := netip.AddrFromSlice(grp.IP.To16())
		if !ok {
			_ = unix.Close(fd)
			return fmt.Errorf("worker %d: bad group address %s", w.id, grp.IP)
		}
		var srcs []netip.Addr
		if w.groupSources != nil {
			srcs = w.groupSources[ga]
		}
		if err := netjoin.Join(fd, w.iface.Index, ga, srcs); err != nil {
			_ = unix.Close(fd)
			return fmt.Errorf("worker %d: join group %s (%d sources): %w", w.id, grp.IP, len(srcs), err)
		}
		w.joinMu.Lock()
		w.joinedGroups[ga] = struct{}{}
		w.joinMu.Unlock()
	}

	return w.serve(ctx, fd, "listener worker ready")
}

// RunUnicastIngest is the delivery-mode receive loop (P3b): it binds the same
// SO_REUSEPORT UDP socket but does NOT join any multicast group — a delivery
// worker receives raw frames UNICAST from a receiver that already joined fabric
// (S,G) and ran gap/NACK/dedup. Each datagram is fed through the normal frame
// dispatch (processFrame) so the egress fan-out sink, per-consumer filtering,
// own-traffic exclusion and metering all run unchanged; the receiver-side stages
// (gap tracker, cross-listener dedup, reassembly) are simply absent (nil) on a
// delivery worker, so processFrame skips them.
func (w *Worker) RunUnicastIngest(ctx context.Context) error {
	fd, err := openRawSocket(w.port)
	if err != nil {
		return fmt.Errorf("delivery worker %d: open socket: %w", w.id, err)
	}
	return w.serve(ctx, fd, "delivery unicast ingest ready")
}

// serve runs the receive loop on an already-bound socket (Run also pre-joins the
// multicast groups first): mark the worker ready, arm SO_RCVTIMEO + the ctx-close
// fast path, then read each datagram into processFrame. Shared by Run (multicast)
// and RunUnicastIngest (delivery unicast ingest).
func (w *Worker) serve(ctx context.Context, fd int, readyMsg string) error {
	if w.rec != nil {
		w.rec.WorkerReady()
		defer w.rec.WorkerDone()
	}

	w.log.Info(readyMsg, "iface", w.iface.Name, "port", w.port, "groups", len(w.groups))

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
			// Extract the source only when something consumes it (sender ACL or the
			// gap tracker), so a worker with neither pays no per-frame cost.
			var src net.IP
			if w.senderACL != nil || w.tracker != nil {
				src = sockaddrIP(from)
			}
			if w.senderACL != nil {
				if !w.senderACL.Allow(src) {
					if w.rec != nil {
						w.rec.FrameDropped(w.id, "sender_filter")
					}
					if w.debug {
						w.log.Debug("sender filter rejected", "src", src)
					}
					continue
				}
			}
			w.procMu.Lock()
			w.curSource = src
			w.processFrame(buf[:n])
			w.procMu.Unlock()
		}
	}
}

// Reinject feeds a recovered raw frame through the normal receive pipeline
// (shard filter, own-traffic exclusion, gap tracking, and fan-out egress) as
// if it had arrived on the wire. It is called by the NACK tracker when a retry
// endpoint returns a frame over the unicast NACK return channel, so a
// gap-repaired frame reaches downstream consumers without any client logic.
// Serialised against the Run loop via procMu.
func (w *Worker) Reinject(raw []byte) {
	w.procMu.Lock()
	w.curSource = nil // recovered frame: no live source — don't clobber per-flow source attribution
	w.processFrame(raw)
	w.procMu.Unlock()
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
		// Fragment-level gap tracking: feed each fragment's (HashKey,
		// SeqNum) to the NACK tracker so interior fragment loss within an
		// object's flow is detected and recovered — the retry endpoint
		// caches fragments under the same (HashKey, SeqNum) key. This is
		// essential for pushed objects (subtree/block/BEEF), where each
		// object is its own single-object flow with no successor whole
		// frame to reveal a gap between reassembled objects. Tail-only loss
		// (the highest fragment index, with no later fragment to expose the
		// gap) still relies on reassembly timeout.
		if w.tracker != nil && ff.SeqNum != 0 {
			// BRC-148 §Frame carriage: object-plane NACKs carry a ZERO
			// SubtreeID. A BEEF fragment's SubtreeID slot holds the TopicID,
			// so it must be zeroed here — fragments and whole frames share a
			// HashKey (one flow), and emitting two different SubtreeID values
			// on one flow's NACKs is a wire inconsistency.
			sub := ff.SubtreeID
			if ff.OrigFrameVer == frame.FrameVerV9 {
				sub = [32]byte{}
			}
			w.tracker.Observe(w.fragGroupIdx(ff), sub, ff.HashKey, ff.SeqNum, ff.TxID, w.curSource)
		}
		return
	}

	// BRC-142 bundle frame (FrameVer 0x08): edge-decoalesce and forward members.
	if frame.IsBundle(raw) {
		w.processBundle(raw)
		return
	}

	// BRC-148 BEEF object frame (FrameVer 0x09): route to the BEEF handler.
	if frame.IsBEEFFrame(raw) {
		w.processBeefFrame(raw)
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

	// Courtesy ingress-set mark: best-effort SETNX into the local proxy's
	// namespace so sibling proxies treat this TxID as "already on the network"
	// — useful when a frame arrived via a cross-site bridge or a path the
	// local proxy itself did not observe. Async; never affects forward path.
	if w.txDedup != nil && w.txDedup.HasIngressMark() && f.Version == frame.FrameVerV2 {
		w.txDedup.Mark(f.TxID)
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

	// Cross-listener TxID dedup: when multiple listeners receive the same
	// multicast frame, only the first to claim the TxID in Redis forwards it.
	// Fail-open: on Redis error the frame is forwarded and the error is counted.
	if w.txDedup != nil && f.Version == frame.FrameVerV2 {
		claimed, claimErr := w.txDedup.Claim(f.TxID)
		if claimErr != nil {
			if w.rec != nil {
				w.rec.TxDedupError()
			}
			if w.debug {
				w.log.Debug("txid dedup redis error (fail-open)", "err", claimErr)
			}
		} else if !claimed {
			if w.rec != nil {
				w.rec.FrameTxDeduped(w.id)
			}
			// Skip egress, but still observe so gap-fill bookkeeping stays accurate.
			if w.tracker != nil && f.SeqNum != 0 {
				w.tracker.Observe(groupIdx, f.SubtreeID, f.HashKey, f.SeqNum, f.TxID, w.curSource)
			}
			return
		}
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
				w.tracker.Observe(groupIdx, f.SubtreeID, f.HashKey, f.SeqNum, f.TxID, w.curSource)
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
		w.tracker.Observe(groupIdx, f.SubtreeID, f.HashKey, f.SeqNum, f.TxID, w.curSource)
	}

	if w.debug {
		w.log.Debug("frame forwarded",
			"version", f.Version,
			"group", groupIdx,
			"seq_num", f.SeqNum,
		)
	}
}

// fragGroupIdx returns the flow's group index for a fragment, used only for
// the NACK tracker's metric label (the flow identity is the fragment
// HashKey). It mirrors the per-OrigFrameVer routing the retry endpoint
// applies to the same cached fragment.
// FragGroupIdx exposes the fragment group resolver to the reassembly buffer so an
// incomplete slot can name its flow when requesting the fragments it never got.
func (w *Worker) FragGroupIdx(ff *frame.FragFrame) uint32 { return w.fragGroupIdx(ff) }

func (w *Worker) fragGroupIdx(ff *frame.FragFrame) uint32 {
	switch ff.OrigFrameVer {
	case frame.FrameVerV4, frame.FrameVerV6:
		return uint32(shard.GroupBlockBroadcast)
	case frame.FrameVerV5:
		return uint32(shard.GroupSubtreeDataAnnounce)
	case frame.FrameVerV9:
		if w.beefEngine != nil {
			return w.beefEngine.GroupIndex(&ff.SubtreeID) // SubtreeID slot carries the TopicID
		}
		return 0x1000
	default:
		return w.engine.GroupIndex(&ff.TxID)
	}
}

// processBundle handles BRC-142 bundle frames (FrameVer 0x08) via
// edge-decoalesce: it filters at bundle granularity, gap-tracks the bundle
// stream (so a missing bundle is NACK'd and recovered whole), then splits the
// bundle into individual BRC-124 frames and forwards each downstream.
//
// Dedup is applied to the whole bundle (not per member): the bundle is the
// retransmission unit, and bundle-level dedup keeps the re-stamped egress
// sequence consistent if several HA listeners receive the same bundle (only the
// winner forwards it). Each emitted member is re-stamped with a fresh monotonic
// egress SeqNum keyed by the bundle flow HashKey, because the per-tx SeqNum did
// not survive coalescing. Runs under procMu (egressSeq needs no further lock).
func (w *Worker) processBundle(raw []byte) {
	b, err := bundle.Decode(raw)
	if err != nil {
		if w.rec != nil {
			w.rec.FrameDropped(w.id, "bundle_decode_error")
		}
		if w.debug {
			w.log.Debug("bundle decode error", "err", err, "len", len(raw))
		}
		return
	}
	if w.rec != nil {
		w.rec.FrameReceived(w.id, w.iface.Name, "brc142")
	}

	// Generation alignment (BRC-142 §Re-bucketing). A bundle's GroupIdx names a
	// group at the bundle's own ShardBits; if that differs from this listener's
	// generation, delivering the bundle as-is would route/filter against the
	// wrong (cross-generation) groups and over-deliver a coarser parent bundle to
	// a finer subscriber. Re-bucket to the local ShardBits first — split the
	// bundle and re-coalesce each member into its correct local-generation group
	// — then deliver each re-bucketed bundle through the normal path. ShardBits 0
	// (unset) and the same-generation common case skip this entirely.
	//
	// The re-bucketed children are re-stamped on a fresh flow keyed by the parent
	// HashKey, so their SeqNum streams are LOCALLY monotonic — which is NOT usable
	// for upstream-loss gap detection: the child counter advances only for parents
	// that arrive, so a dropped parent bundle leaves no hole in any child stream.
	// Upstream loss is detected on the PARENT stream instead (the identity the
	// origin's retry cached), by Observe-ing the parent (HashKey, SeqNum) below,
	// survivorship-gated. Caveat: own-traffic exclusion keys on
	// HashKey = hash(originalSenderIP, group, subtree), which cannot be recomputed
	// at the local groups from the opaque parent HashKey, so own-traffic exclusion
	// does not apply to re-bucketed (cross-generation) flows — a documented
	// re-shard-boundary limitation, not a per-tx regression in the common path.
	if b.ShardBits != 0 && uint(b.ShardBits) != w.engine.ShardBits() {
		if w.rebucketer == nil {
			// carryTxID=true: re-coalescing recomputes a missing TxID as SHA256d,
			// which is wrong for EF members, so preserve TxIDs across the re-bucket.
			w.rebucketer = bundle.NewRebucketer(w.engine, rebucketMaxBytes, 0, true)
		}
		var sender [16]byte
		binary.BigEndian.PutUint64(sender[0:8], b.HashKey)
		children := w.rebucketer.Rebucket(sender, b)
		if w.rec != nil {
			w.rec.BundleRebucketed(w.id)
		}
		// Guard: re-bucketing is a relay-only operation. On a bare listener a
		// ShardBits mismatch is almost always a misconfiguration, so raise an alarm
		// metric on every re-bucket and WARN once. Delivery still proceeds (the
		// parent-stream tracking below makes ingress loss recoverable); refusing
		// would only discard deliverable members.
		if !w.relay {
			if w.rec != nil {
				w.rec.RebucketUnguarded(w.id)
			}
			if !w.warnedRebucket {
				w.warnedRebucket = true
				w.log.Warn("re-bucketing on a non-relay listener (generation mismatch): match the proxy ShardBits, or set -rebucket-relay with a co-located child-generation retry — re-multicast children are otherwise unrecoverable",
					"bundle_shardbits", b.ShardBits, "listener_shardbits", w.engine.ShardBits())
			}
		}
		if w.debug {
			w.log.Debug("bundle re-bucketed", "from_shardbits", b.ShardBits,
				"to_shardbits", w.engine.ShardBits(), "members", len(b.Members), "children", len(children))
		}
		// Deliver children WITHOUT gap-tracking their local (phantom) streams
		// (track=false): a re-bucketed child's SeqNum comes from a local counter,
		// not a recoverable identity.
		for _, child := range children {
			w.deliverBundle(child, nil, false) // nil raw: encoded on demand if delivered whole
		}
		// Gap-track the PARENT bundle stream on the identity the origin's retry
		// cached (parent HashKey, SeqNum) so upstream loss is detected and
		// NACK-recovered; the recovered parent's re-entry auto-fills the gap. This
		// MUST witness EVERY received parent, not a filter-surviving subset: nack
		// detects gaps by sequential arithmetic, so observing the stream sparsely
		// would misread filtered-out parents as losses and NACK-storm them. A
		// listener that joins a coarse group receives every parent in it (it filters
		// MEMBERS, not parents), so dense observation is correct; recovering a whole
		// coarse parent to extract a wanted subset is the inherent, bounded byte cost
		// of re-bucketing (one NACK per lost parent, proportional to loss), not a
		// per-parent-avoidable one.
		if w.tracker != nil && b.SeqNum != 0 {
			var zero [32]byte // a bundle has no single TxID
			w.tracker.Observe(uint32(b.GroupIdx), b.SubtreeID, b.HashKey, b.SeqNum, zero, w.curSource)
		}
		return
	}

	w.deliverBundle(b, raw, true)
}

// deliverBundle filters, optionally gap-tracks, dedups, and delivers one
// generation-aligned bundle (the local-ShardBits common case, or a child
// produced by re-bucketing). raw is the verbatim datagram for the whole-bundle
// (consumer-decoalesce) path; it is nil for a re-bucketed child, encoded on
// demand only if a bundle-capable consumer takes it whole. track gap-tracks this
// bundle's identity (true on the common path; false for a re-bucketed child,
// whose parent the caller tracks separately). Runs under procMu.
func (w *Worker) deliverBundle(b *bundle.Bundle, raw []byte, track bool) {
	groupIdx := uint32(b.GroupIdx)

	// Filter at bundle granularity — a bundle is one (group, subtree).
	probe := &frame.Frame{Version: frame.FrameVerV2, SubtreeID: b.SubtreeID}
	if allow, reason := w.filt.Allow(groupIdx, probe); !allow {
		if w.rec != nil {
			w.rec.FrameDropped(w.id, reason)
		}
		return
	}

	// track gates gap-tracking of THIS bundle's identity: true for a
	// generation-aligned bundle (the common path), false for a re-bucketed child
	// whose local SeqNum is a phantom (the caller tracks the parent instead).
	observe := func() {
		if track && w.tracker != nil && b.SeqNum != 0 {
			var zero [32]byte // a bundle has no single TxID
			w.tracker.Observe(groupIdx, b.SubtreeID, b.HashKey, b.SeqNum, zero, w.curSource)
		}
	}

	// Cross-listener egress dedup, at the BUNDLE level: only the first listener
	// in a deployment forwards a given bundle. Keyed by a synthetic bundle id
	// (HashKey ∥ SeqNum) since a bundle has no TxID. Still observe for gap
	// bookkeeping.
	if w.txDedup != nil && b.SeqNum != 0 {
		var bid [32]byte
		binary.BigEndian.PutUint64(bid[0:8], b.HashKey)
		binary.BigEndian.PutUint64(bid[8:16], b.SeqNum)
		claimed, claimErr := w.txDedup.Claim(bid)
		if claimErr != nil {
			if w.rec != nil {
				w.rec.TxDedupError()
			}
		} else if !claimed {
			if w.rec != nil {
				w.rec.FrameTxDeduped(w.id)
			}
			observe()
			return
		}
	}

	// Local egress duplicate suppression (inline frame + its retransmit), at the
	// bundle level. Still observe so gap-fill bookkeeping stays accurate.
	if w.dedupSet != nil && b.SeqNum != 0 {
		if w.dedupSet.SeenAndAdd(dedup.Key{GroupIdx: groupIdx, SubtreeID: b.SubtreeID, SeqNum: b.SeqNum}) {
			if w.rec != nil {
				w.rec.FrameDeduped(w.id)
			}
			observe()
			return
		}
	}

	observe()

	if w.egressSeq == nil {
		w.egressSeq = make(map[uint64]uint64)
	}

	// A bundle-aware fan-out sink takes the whole bundle and decides delivery per
	// consumer (intact to bundle-capable consumers, decoalesced for the rest);
	// the per-flow egress re-stamp then lives in that sink. A plain sink is handed
	// decoalesced individual BRC-124 frames, re-stamped here (the edge-decoalesce
	// default). The multicast re-emit path (mcastEgr), when present, always
	// receives decoalesced members on its own re-stamp stream.
	bs, bundleAware := w.egr.(egress.BundleSink)
	if bundleAware {
		// A re-bucketed child has no source datagram; encode it so the bundle-aware
		// sink can deliver it whole to a capable consumer.
		if raw == nil {
			encoded, encErr := b.Encode()
			if encErr != nil {
				if w.rec != nil {
					w.rec.EgressError(w.id)
				}
				w.log.Debug("re-bucketed bundle encode error", "err", encErr)
			} else {
				raw = encoded
			}
		}
		if raw != nil {
			if err := bs.SendBundle(raw, b); err != nil {
				if w.rec != nil {
					w.rec.EgressError(w.id)
				}
				w.log.Debug("bundle egress send error", "err", err)
			} else if w.rec != nil {
				w.rec.FrameForwarded(w.id, w.egr.Proto())
			}
		}
	}

	if !bundleAware || w.mcastEgr != nil {
		for _, mf := range bundle.Decoalesce(b) {
			// Re-stamp a fresh monotonic per-flow egress SeqNum (keyed by the bundle
			// flow HashKey) so downstream consumers retain gap detection.
			w.egressSeq[b.HashKey]++
			mf.SeqNum = w.egressSeq[b.HashKey]

			buf := make([]byte, frame.HeaderSize+len(mf.Payload))
			n, encErr := frame.Encode(mf, buf)
			if encErr != nil {
				if w.rec != nil {
					w.rec.EgressError(w.id)
				}
				w.log.Debug("bundle member encode error", "err", encErr)
				continue
			}
			out := buf[:n]

			if !bundleAware {
				if err := w.egr.Send(out, mf); err != nil {
					if w.rec != nil {
						w.rec.EgressError(w.id)
					}
					w.log.Debug("egress send error", "err", err)
				} else if w.rec != nil {
					w.rec.FrameForwarded(w.id, w.egr.Proto())
				}
			}

			if w.mcastEgr != nil {
				if err := w.mcastEgr.Send(out, mf, groupIdx); err != nil {
					if w.rec != nil {
						w.rec.MCEgressError(w.id)
					}
					w.log.Debug("mc egress send error", "err", err)
				} else if w.rec != nil {
					w.rec.FrameForwarded(w.id, w.mcastEgr.Proto())
				}
			}
		}
	}

	if w.debug {
		w.log.Debug("bundle decoalesced", "group", groupIdx, "seq_num", b.SeqNum, "members", len(b.Members), "bundle_aware", bundleAware)
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

	// Block-control gate (opt-in): validate inter-domain announcements before
	// fan-out — PoW on the announce, coinbase↔block correlation on coinbase.
	if !w.blockGateAllows(bf, bf.Payload) {
		if w.debug {
			w.log.Debug("block frame dropped by block-control gate",
				"msg_type", bf.MsgType, "content_id", fmt.Sprintf("%x", bf.ContentID[:8]))
		}
		return
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
	if w.headerEgr != nil || w.headerMCastEgr != nil || w.headerFanout != nil {
		w.emitBlockHeader(bf)
	}

	// Gap tracking on the control flow uses a zero SubtreeID.
	if w.tracker != nil && bf.SeqNum != 0 {
		var zeroSub [32]byte
		w.tracker.Observe(uint32(shard.GroupBlockBroadcast), zeroSub, bf.HashKey, bf.SeqNum, bf.ContentID, w.curSource)
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
// payload, re-encodes it as a BRC-135 block header frame (FrameVer 0x07,
// 92B header + 80B payload = 172B), and sends it to configured header
// egress endpoints.
//
// Per BRC-135, the listener (as emitter) stamps HashKey using its own
// identity (set via SetHeaderEmitterIdentity) and a monotonic per-emitter
// SeqNum counter. The block hash is carried verbatim from the upstream
// BRC-131 ContentID. Downstream SPV consumers track gaps on the
// emitter-attributed (HashKey, SeqNum) flow.
func (w *Worker) emitBlockHeader(bf *frame.BlockFrame) {
	if bf.MsgType != frame.BlockMsgAnnounce {
		return
	}
	if len(bf.Payload) < frame.BlockHeaderSize {
		return
	}

	buf := make([]byte, frame.BlockHeaderFrameSize)
	seqNum := w.headerSeqNum.Add(1)
	if _, err := frame.EncodeBlockHeader(bf.ContentID, w.headerHashKey, seqNum, bf.Payload[:frame.BlockHeaderSize], buf); err != nil {
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
		if err := w.headerMCastEgr.SendToGroup(buf, uint16(shard.GroupBlockHeader)); err != nil {
			if w.rec != nil {
				w.rec.HeaderEgressError(w.id)
			}
			w.log.Debug("header mc egress send error", "err", err)
		} else if w.rec != nil {
			w.rec.HeaderForwarded(w.id)
		}
	}

	// Per-consumer header lane. The decode cannot fail on a buffer we just
	// encoded, but routing needs the parsed view (block hash in TxID, the
	// 80-byte header aliased as Payload) rather than a second parse per
	// consumer downstream.
	if w.headerFanout != nil {
		hf, err := frame.DecodeBlockHeader(buf)
		if err != nil {
			w.log.Debug("header fanout decode error", "err", err)
			return
		}
		if err := w.headerFanout.SendHeader(buf, hf); err != nil {
			// A consumer that did not elect the lane is reported as a
			// not-elected drop by the class router, so this is a real egress
			// failure, not the common no-subscriber case.
			if w.rec != nil {
				w.rec.HeaderEgressError(w.id)
			}
			w.log.Debug("header fanout send error", "err", err)
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
		w.tracker.Observe(uint32(shard.GroupSubtreeDataAnnounce), sf.SubtreeID, sf.HashKey, sf.SeqNum, sf.SubtreeID, w.curSource)
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

	// Courtesy ingress-set mark for BRC-134 anchor TxIDs.
	if w.txDedup != nil && w.txDedup.HasIngressMark() {
		w.txDedup.Mark(f.TxID)
	}

	// Cross-listener TxID dedup for BRC-134 anchor frames.
	if w.txDedup != nil {
		claimed, claimErr := w.txDedup.Claim(f.TxID)
		if claimErr != nil {
			if w.rec != nil {
				w.rec.TxDedupError()
			}
			if w.debug {
				w.log.Debug("txid dedup redis error (fail-open)", "err", claimErr)
			}
		} else if !claimed {
			if w.rec != nil {
				w.rec.FrameTxDeduped(w.id)
			}
			const anchorGroupIdx = uint32(0xFFF9)
			var zeroSub [32]byte
			if w.tracker != nil && f.SeqNum != 0 {
				w.tracker.Observe(anchorGroupIdx, zeroSub, f.HashKey, f.SeqNum, f.TxID, w.curSource)
			}
			return
		}
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
		w.tracker.Observe(anchorGroupIdx, zeroSub, f.HashKey, f.SeqNum, f.TxID, w.curSource)
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

	// Same block-control gate on the reassembled (fragmented) path.
	if !w.blockGateAllows(bf, payload) {
		if w.debug {
			w.log.Debug("reassembled block frame dropped by block-control gate",
				"msg_type", bf.MsgType, "content_id", fmt.Sprintf("%x", bf.ContentID[:8]))
		}
		return
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
	if w.headerEgr != nil || w.headerMCastEgr != nil || w.headerFanout != nil {
		w.emitBlockHeader(bf)
	}

	if w.tracker != nil && bf.SeqNum != 0 {
		var zeroSub [32]byte
		w.tracker.Observe(uint32(shard.GroupBlockBroadcast), zeroSub, bf.HashKey, bf.SeqNum, bf.ContentID, w.curSource)
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
		w.tracker.Observe(uint32(shard.GroupSubtreeDataAnnounce), sf.SubtreeID, sf.HashKey, sf.SeqNum, sf.SubtreeID, w.curSource)
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

	// Cross-listener TxID dedup for reassembled BRC-130 frames.
	if w.txDedup != nil {
		claimed, claimErr := w.txDedup.Claim(f.TxID)
		if claimErr != nil {
			if w.rec != nil {
				w.rec.TxDedupError()
			}
			if w.debug {
				w.log.Debug("txid dedup redis error (fail-open)", "err", claimErr)
			}
		} else if !claimed {
			if w.rec != nil {
				w.rec.FrameTxDeduped(w.id)
			}
			if w.tracker != nil && f.SeqNum != 0 {
				w.tracker.Observe(groupIdx, f.SubtreeID, f.HashKey, f.SeqNum, f.TxID, w.curSource)
			}
			return
		}
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
		w.tracker.Observe(groupIdx, f.SubtreeID, f.HashKey, f.SeqNum, f.TxID, w.curSource)
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
	// SO_REUSEADDR in addition to SO_REUSEPORT: SO_REUSEPORT only lets sockets share a port
	// when they have the SAME EUID (the kernel anti-hijack rule), so on a COLLAPSED node a
	// co-resident retry-endpoint running as a different user cannot also join this multicast
	// group. SO_REUSEADDR uses the sk_reuse bind path, which permits two UDP sockets to share
	// the port regardless of EUID — the classic multiple-receivers-per-multicast-group pattern.
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		_ = unix.Close(fd)
		return -1, fmt.Errorf("SO_REUSEADDR: %w", err)
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
