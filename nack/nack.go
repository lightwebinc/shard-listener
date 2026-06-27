package nack

import (
	"context"
	"encoding/binary"
	"log/slog"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/lightwebinc/shard-listener/discovery"
	"github.com/lightwebinc/shard-listener/metrics"
)

// TrackerConfig holds tuning parameters for the gap tracker.
type TrackerConfig struct {
	JitterMax         time.Duration // Max random hold-off before first NACK (NORM suppression window)
	BackoffBase       time.Duration // Base delay for retry backoff; doubles per failed round (default 500ms)
	BackoffMax        time.Duration // Cap on exponential backoff between retries
	MaxRetries        int           // Max failed recovery rounds before declaring unrecoverable (tier hops are free)
	GapTTL            time.Duration // Max lifetime of a gap entry (~Bitcoin block interval)
	TailTTL           time.Duration // Max idle time before a flow entry is evicted; 0 = GapTTL
	SeqResetThreshold uint64        // If seqNum <= threshold on an established flow, treat as proxy restart (default 100)
	// MaxFlows caps the per-source flow table so a burst of thousands of DISTINCT new
	// sources (a flood, before the idle-age-out sweep frees slots) cannot exhaust memory.
	// At the cap, a NEW source is not gap-tracked (its frames still forward — only NACK
	// recovery is skipped for it) until a slot frees via age-out. 0 = unbounded (legacy).
	MaxFlows int
}

// groupBlockBroadcast is the reserved group index for BRC-131 block control frames.
// Mirrors shard.GroupBlockBroadcast = 0xFFFE without importing the shard package.
const groupBlockBroadcast uint32 = 0xFFFE

// groupSubtreeDataAnnounce is the reserved group index for BRC-132 subtree data frames.
// Mirrors shard.GroupSubtreeDataAnnounce = 0xFFFB without importing the shard package.
const groupSubtreeDataAnnounce uint32 = 0xFFFB

// groupAnchorFlow is the virtual group index for BRC-134 anchor transaction frames.
// Anchors share the GroupBlockBroadcast multicast address on the wire but use a
// dedicated groupIdx for HashKey derivation so they have their own SeqNum counter.
const groupAnchorFlow uint32 = 0xFFF9

// flowLabel returns "brc131" for block control flows, "brc132" for subtree data
// flows, "brc134" for anchor transaction flows, and "brc124" for all others.
func flowLabel(groupIdx uint32) string {
	switch groupIdx {
	case groupBlockBroadcast:
		return "brc131"
	case groupSubtreeDataAnnounce:
		return "brc132"
	case groupAnchorFlow:
		return "brc134"
	default:
		return "brc124"
	}
}

// srcStr renders a source IP as a metric label value, lazily (only at the rare
// gap/NACK emit sites — never on the per-frame hot path). "" for a nil source.
func srcStr(ip net.IP) string {
	if ip == nil {
		return ""
	}
	return ip.String()
}

// flowState tracks one active per-flow sequence stream.
//
// Flows are keyed by hashKey = XXH64(senderIPv6 || groupIdx || subtreeID).
// A gap is detected whenever seqNum > lastSeqNum+1.
type flowState struct {
	lastSeqNum uint64
	groupIdx   uint32
	subtreeID  [32]byte
	flowType   string               // "brc131" or "brc124"
	source     net.IP               // multicast source address (for per-source loss attribution)
	pending    map[uint64]*gapEntry // keyed by missing seqNum
	lastSeen   time.Time
}

// gapEntry holds retry state for a single missing frame.
//
// retries and failRounds track two different things. retries counts every NACK
// attempt (including the immediate tier-escalation hops that walk retry1 →
// retry2 → retry3 on a MISS) and is used only for observability. failRounds
// counts consecutive *failed recovery rounds* — a timeout, or a MISS once the
// deepest tier has been reached — and drives both the exponential backoff
// magnitude and the unrecoverable cap. Escalating through tiers is free: it
// does not grow the backoff or consume the retry budget, so a deep topology
// gets the same number of real retries at the warm cache as a shallow one.
type gapEntry struct {
	hashKey     uint64
	seqNum      uint64 // the missing sequence number
	groupIdx    uint32
	subtreeID   [32]byte // for NACK SubtreeID field
	source      net.IP   // multicast source address (per-source loss attribution)
	retries     int      // total NACK attempts (observability only)
	failRounds  int      // consecutive failed recovery rounds (backoff + eviction cap)
	nextAttempt time.Time
	deadline    time.Time // absolute eviction deadline
	endpointIdx int       // index into registry snapshot, clamped to the deepest tier
}

// Tracker is the gap state machine. Construct with [New] and call [Start] to
// begin background GC and NACK dispatch.
type Tracker struct {
	cfg           TrackerConfig
	iface         *net.Interface
	rec           *metrics.Recorder
	log           *slog.Logger
	registry      *discovery.Registry
	respTimeout   time.Duration // deadline for ACK/MISS response (default 300ms)
	maxConcurrent int           // semaphore bound for concurrent sendNACK goroutines

	// recoverFn re-injects a frame returned by a retry endpoint over the
	// unicast NACK return channel back into the listener pipeline (set via
	// SetRecoverFunc). nil disables unicast recovery (multicast-only repair).
	recoverFn func(raw []byte)

	// nackSrc is the source address the NACK socket binds to (set via
	// SetNACKSource). Empty = wildcard (kernel picks per-route). Set this to a
	// globally routable address on a tunnelled fabric so the retry's unicast
	// return traffic routes back to this node.
	nackSrc string

	mu           sync.Mutex
	flows        map[uint64]*flowState // keyed by hashKey
	flowsRefused uint64                // new flows skipped at MaxFlows (flood guard); under mu

	// nackQueue receives gap entries ready for NACK dispatch.
	nackQueue chan *gapEntry

	// sem bounds concurrent sendNACK goroutines.
	sem chan struct{}
}

// New constructs a Tracker. retryEndpoints is the static seed list.
// registry is the dynamic endpoint registry from beacon discovery (may be nil
// to use only static seeds). iface is reserved for future multicast NACK send.
func New(cfg TrackerConfig, retryEndpoints []string, iface *net.Interface, rec *metrics.Recorder, registry *discovery.Registry) *Tracker {
	const defaultMaxConcurrent = 64
	const defaultRespTimeout = 300 * time.Millisecond
	const defaultBackoffBase = 500 * time.Millisecond

	if registry == nil {
		registry = discovery.NewRegistry()
	}
	if len(retryEndpoints) > 0 {
		registry.Seed(retryEndpoints)
	}
	// Guard against a zero base, which would make every backoff zero and spin
	// the gap through the dispatch queue without pause.
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = defaultBackoffBase
	}

	return &Tracker{
		cfg:           cfg,
		iface:         iface,
		rec:           rec,
		log:           slog.Default().With("component", "nack"),
		registry:      registry,
		respTimeout:   defaultRespTimeout,
		maxConcurrent: defaultMaxConcurrent,
		flows:         make(map[uint64]*flowState),
		nackQueue:     make(chan *gapEntry, 4096),
		sem:           make(chan struct{}, defaultMaxConcurrent),
	}
}

// SetRecoverFunc registers the callback that re-injects a frame returned by a
// retry endpoint over the unicast NACK return channel. When set, sendNACK
// drains any data frame the retry unicasts back on the NACK socket and re-feeds
// it through the listener pipeline (the gap is then auto-filled by Observe).
// Typically wired to (*listener.Worker).Reinject.
func (t *Tracker) SetRecoverFunc(f func(raw []byte)) { t.recoverFn = f }

// FlowCount returns the number of tracked per-source flows (diagnostics/tests).
func (t *Tracker) FlowCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.flows)
}

// FlowsRefused returns the cumulative count of new flows skipped at MaxFlows (the flood
// guard) — a non-zero value signals sustained source-flood pressure.
func (t *Tracker) FlowsRefused() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.flowsRefused
}

// SetNACKSource binds the NACK socket to a specific source address so NACKs and
// the retry's unicast return traffic use a globally routable identity. Leave
// unset (empty) to let the kernel pick the source per-route.
func (t *Tracker) SetNACKSource(addr string) { t.nackSrc = addr }

// Observe is called by the listener worker on every BRC-124/BRC-128 frame.
// It detects gaps by comparing seqNum against the last known seqNum for the
// flow identified by hashKey. seqNum == 0 means the proxy has not stamped the
// frame; it is ignored.
//
// Each distinct hashKey represents one flow (sender × group × subtree).
// Gaps are detected when seqNum > lastSeqNum+1 for the same flow.
//
// Processing steps:
//  1. Skip unstamped frames (seqNum == 0).
//  2. Look up (or create) the flowState for hashKey.
//  3. Auto-fill: if seqNum matches a pending gap, close it.
//  4. Ignore: seqNum <= lastSeqNum (duplicate or old retransmit).
//  5. Contiguous: seqNum == lastSeqNum+1 → advance.
//  6. Gap: seqNum > lastSeqNum+1 → register each missing seqNum.
func (t *Tracker) Observe(groupIdx uint32, subtreeID [32]byte, hashKey, seqNum uint64, txid [32]byte, source net.IP) {
	if seqNum == 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()

	fs, ok := t.flows[hashKey]
	if !ok {
		// Flood guard: at the cap, do NOT create a new flow — the frame still forwards
		// (the caller egresses independently), only gap/NACK recovery is skipped for this
		// source until an idle flow ages out and frees a slot.
		if t.cfg.MaxFlows > 0 && len(t.flows) >= t.cfg.MaxFlows {
			t.flowsRefused++
			if t.rec != nil {
				t.rec.FlowsRefused(flowLabel(groupIdx))
			}
			return
		}
		fs = &flowState{
			groupIdx:  groupIdx,
			subtreeID: subtreeID,
			flowType:  flowLabel(groupIdx),
			pending:   make(map[uint64]*gapEntry),
		}
		t.flows[hashKey] = fs
	}
	fs.lastSeen = now
	// Sources are stable per flow. Keep the latest non-empty source so a recovered
	// frame re-injected by the tracker (which carries no live source) does not
	// clobber the real source attribution.
	if source != nil {
		fs.source = source
	}

	// Step 3: auto-fill — close any pending gap whose seqNum matches.
	if _, found := fs.pending[seqNum]; found {
		delete(fs.pending, seqNum)
		if t.rec != nil {
			t.rec.GapSuppressed(fs.flowType, srcStr(fs.source))
		}
		// Fall through: update lastSeqNum if this advances the stream.
	}

	if fs.lastSeqNum == 0 {
		// First frame for this flow.
		fs.lastSeqNum = seqNum
		return
	}

	if seqNum <= fs.lastSeqNum {
		// Step 4: duplicate, old retransmit, or proxy restart.
		// Detect a proxy restart: SeqNum rolled back to a very small value on an
		// established flow (lastSeqNum significantly higher). Reset the flow so
		// the restarted proxy's sequence stream is tracked from scratch.
		threshold := t.cfg.SeqResetThreshold
		if threshold == 0 {
			threshold = 100
		}
		if seqNum <= threshold && fs.lastSeqNum > threshold {
			// Proxy restarted: evict any pending gaps (unrecoverable now) and
			// reset the flow counter.
			for _, e := range fs.pending {
				_ = e // gaps from previous proxy lifetime; drop silently
				if t.rec != nil {
					t.rec.GapUnrecovered(fs.flowType, srcStr(fs.source))
				}
			}
			fs.pending = make(map[uint64]*gapEntry)
			fs.lastSeqNum = seqNum
			return
		}
		return
	}

	if seqNum == fs.lastSeqNum+1 {
		// Step 5: contiguous.
		fs.lastSeqNum = seqNum
		return
	}

	// Step 6: gap — register each missing seqNum in (lastSeqNum, seqNum).
	for missing := fs.lastSeqNum + 1; missing < seqNum; missing++ {
		if _, exists := fs.pending[missing]; !exists {
			jitter := time.Duration(rand.Int64N(int64(t.cfg.JitterMax) + 1))
			e := &gapEntry{
				hashKey:     hashKey,
				seqNum:      missing,
				groupIdx:    groupIdx,
				subtreeID:   subtreeID,
				source:      source,
				nextAttempt: now.Add(jitter),
				deadline:    now.Add(t.cfg.GapTTL),
			}
			fs.pending[missing] = e
			if t.rec != nil {
				t.rec.GapDetected(fs.flowType, srcStr(source))
			}
		}
	}
	fs.lastSeqNum = seqNum
}

// Fill cancels a pending gap when a retransmitted frame arrives out-of-band
// and the caller has (hashKey, seqNum) but not the full frame. Observe handles
// the same fill check automatically when the retransmit is processed normally.
func (t *Tracker) Fill(hashKey, seqNum uint64) {
	if seqNum == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	if fs, ok := t.flows[hashKey]; ok {
		if _, found := fs.pending[seqNum]; found {
			delete(fs.pending, seqNum)
			if t.rec != nil {
				t.rec.GapSuppressed(fs.flowType, srcStr(fs.source))
			}
		}
	}
}

// Start launches the background NACK dispatch loop and GC sweeper.
// It returns when ctx is cancelled.
func (t *Tracker) Start(ctx context.Context) {
	go t.dispatchLoop(ctx)
	go t.gcLoop(ctx)
}

// gcLoop scans pending gaps on a regular interval, evicts expired entries,
// and enqueues entries whose nextAttempt has passed.
func (t *Tracker) gcLoop(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			t.sweepOnce(now)
		}
	}
}

func (t *Tracker) sweepOnce(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	flowTTL := t.cfg.TailTTL
	if flowTTL <= 0 {
		flowTTL = t.cfg.GapTTL
	}

	for hk, fs := range t.flows {
		for seq, e := range fs.pending {
			if now.After(e.deadline) {
				delete(fs.pending, seq)
				if t.rec != nil {
					t.rec.GapUnrecovered(fs.flowType, srcStr(e.source))
				}
				t.log.Debug("gap evicted (TTL)",
					"hash_key", hk,
					"seq_num", e.seqNum,
				)
				continue
			}
			if e.failRounds >= t.cfg.MaxRetries {
				delete(fs.pending, seq)
				if t.rec != nil {
					t.rec.GapUnrecovered(fs.flowType, srcStr(e.source))
				}
				t.log.Debug("gap evicted (retries)",
					"hash_key", hk,
					"seq_num", e.seqNum,
				)
				continue
			}
			if now.After(e.nextAttempt) {
				// Stamp nextAttempt before copying so concurrent sweeps do not
				// re-enqueue the same gap while a sendNACK goroutine is in-flight.
				e.nextAttempt = now.Add(t.respTimeout + 100*time.Millisecond)
				entry := *e // shallow copy to avoid races
				select {
				case t.nackQueue <- &entry:
				default:
					// Queue full — reset so this gap is retried next tick.
					e.nextAttempt = now
				}
			}
		}

		// Evict idle flows with no pending gaps.
		if len(fs.pending) == 0 && now.Sub(fs.lastSeen) > flowTTL {
			delete(t.flows, hk)
		}
	}
}

// dispatchLoop reads from nackQueue and launches bounded sendNACK goroutines.
func (t *Tracker) dispatchLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-t.nackQueue:
			select {
			case t.sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			go func() {
				defer func() { <-t.sem }()
				t.sendNACK(e)
			}()
		}
	}
}

// sendNACK dispatches a NACK to a retry endpoint, then waits for the first
// ACK/MISS response. On ACK the gap is cancelled; on MISS or timeout, retry
// state advances with exponential backoff.
func (t *Tracker) sendNACK(e *gapEntry) {
	snap := t.registry.Snapshot()
	if len(snap) == 0 {
		return
	}

	// Clamp at the last (highest-tier) endpoint. Once we've escalated through
	// every tier, further attempts should stay at the deepest cache rather than
	// cycling back to lower-tier endpoints that have already returned MISS.
	idx := e.endpointIdx
	if idx >= len(snap) {
		idx = len(snap) - 1
	}
	endpoint := snap[idx]

	addr, err := net.ResolveUDPAddr("udp", endpoint.Addr)
	if err != nil {
		t.log.Warn("NACK: cannot resolve retry endpoint", "endpoint", endpoint.Addr, "err", err)
		t.advanceEndpoint(e, false, 0)
		return
	}

	// Ephemeral unconnected socket: accept ACK/MISS from any source address.
	// (Connected sockets filter by exact source; SLAAC addresses on the retry
	// endpoint would cause silent discard of ACK responses.)
	//
	// Bind to nackSrc when set so the NACK (and the retry's return traffic) uses
	// a globally routable identity. Otherwise the kernel picks a per-route
	// source — on a tunnelled fabric that is a point-to-point /127 tunnel inner
	// address, and the retry's reply (a unicast retransmit) is then misrouted
	// off the tunnel and lost.
	laddr := "[::]:0"
	if t.nackSrc != "" {
		laddr = "[" + t.nackSrc + "]:0"
	}
	conn, err := net.ListenPacket("udp", laddr)
	if err != nil {
		t.log.Warn("NACK: listen failed", "endpoint", endpoint.Addr, "err", err)
		t.advanceEndpoint(e, false, 0)
		return
	}
	defer func() { _ = conn.Close() }()

	var buf [NACKSize]byte
	Encode(&NACK{
		MsgType:   MsgTypeNACK,
		HashKey:   e.hashKey,
		StartSeq:  e.seqNum,
		EndSeq:    e.seqNum,
		SubtreeID: e.subtreeID,
	}, buf[:])
	_, _ = conn.WriteTo(buf[:], addr)

	if t.rec != nil {
		t.rec.NACKDispatched(flowLabel(e.groupIdx), srcStr(e.source))
	}
	t.log.Debug("NACK dispatched",
		"endpoint", endpoint.Addr,
		"tier", endpoint.Tier,
		"hash_key", e.hashKey,
		"seq_num", e.seqNum,
		"retry", e.retries+1,
	)

	// Drain the NACK socket until respTimeout. The retry endpoint may return up
	// to two datagrams: a unicast retransmit of the missing frame (the data
	// return channel) and a small ACK/MISS/THROTTLED control response — in
	// either order. A data frame is re-injected through the listener pipeline
	// (recoverFn), which repairs the gap for downstream consumers without any
	// client logic; the control response drives tier/backoff state. The buffer
	// is sized for a full BRC frame so a unicast retransmit is not truncated.
	deadline := time.Now().Add(t.respTimeout)
	var rbuf [65536]byte
	unicastACK := false
	for {
		_ = conn.SetReadDeadline(deadline)
		nr, _, err := conn.ReadFrom(rbuf[:])
		if err != nil { // timeout / closed
			break
		}

		// A data frame (the unicast retransmit) confirms recovery: re-inject it
		// through the listener pipeline (Observe auto-fills the gap and fans it
		// out downstream), cancel, and return — regardless of whether the ACK
		// arrived first or not.
		if t.recoverFn != nil && nr >= minRetransmitFrame && binary.BigEndian.Uint32(rbuf[0:4]) == nackMagic {
			cp := make([]byte, nr)
			copy(cp, rbuf[:nr])
			t.recoverFn(cp)
			t.cancelGap(e)
			t.log.Debug("NACK: unicast retransmit recovered", "endpoint", endpoint.Addr, "seq_num", e.seqNum, "bytes", nr)
			return
		}

		if resp, derr := DecodeResponse(rbuf[:nr]); derr == nil {
			switch resp.MsgType {
			case MsgTypeACK:
				// A unicast-flagged ACK only promises a retransmit; recovery is
				// confirmed solely when the data frame itself arrives, because
				// the retransmit can be lost on the same lossy path. So do NOT
				// cancel on the ACK alone — keep draining for the data, and on
				// timeout escalate to another cache. A non-unicast
				// (multicast/legacy) ACK keeps the original trust-the-repair
				// semantics: the data-path Fill closes the gap.
				if resp.Flags&respFlagUnicastSent != 0 && t.recoverFn != nil {
					unicastACK = true
					deadline = time.Now().Add(unicastDrainWindow)
					continue
				}
				t.cancelGap(e)
				t.log.Debug("NACK: ACK received", "endpoint", endpoint.Addr, "seq_num", e.seqNum, "flags", resp.Flags)
				return
			case MsgTypeMISS:
				t.log.Debug("NACK: MISS received, advancing endpoint", "endpoint", endpoint.Addr, "seq_num", e.seqNum)
				t.advanceEndpoint(e, true, len(snap))
				return
			case MsgTypeTHROTTLED:
				t.log.Debug("NACK: THROTTLED received, holding gap", "endpoint", endpoint.Addr, "seq_num", e.seqNum, "bucket", resp.Flags&0x0F)
				t.throttleGap(e, resp.Flags)
				return
			}
			return
		}
		// Unknown datagram; keep draining until the deadline.
	}

	// Drain ended without the data frame (a recovered data frame returns inline).
	if unicastACK {
		// The cache claimed it has the frame but the unicast retransmit never
		// arrived (lost in transit). Escalate to another cache rather than
		// dropping the gap — a frame lost on this path may be repairable from a
		// node that received it over a clean path.
		t.advanceEndpoint(e, true, len(snap))
		return
	}
	t.advanceEndpoint(e, false, 0)
}

// respFlagUnicastSent mirrors the retry-endpoint ACK flag bit indicating a
// unicast retransmit was dispatched on the NACK return channel (BRC-126).
const respFlagUnicastSent byte = 0x02

// minRetransmitFrame is the smallest datagram treated as a unicast data
// retransmit rather than a (16-byte) control response. A BRC frame header is
// larger; this stays well above ResponseSize.
const minRetransmitFrame = 64

// unicastDrainWindow bounds the extra wait for the second datagram (data after
// ACK, or ACK after data) once the first has been seen.
const unicastDrainWindow = 80 * time.Millisecond

// throttleHintBase is the protocol unit for the THROTTLED backoff hint: the
// suggested hold is throttleHintBase << bucket, where bucket is the low nibble
// of the response Flags byte (BRC-126).
const throttleHintBase = 125 * time.Millisecond

// throttleGap parks a gap after a THROTTLED congestion signal. Unlike a failed
// round it neither advances the endpoint nor consumes the retry budget — the
// endpoint is healthy and a multicast repair is likely propagating — so the gap
// holds for the hinted backoff (jittered) and retries the same endpoint. GapTTL
// remains the absolute safety net; Fill cancels the gap if the repair arrives.
func (t *Tracker) throttleGap(e *gapEntry, flags byte) {
	hold := throttleHintBase << uint(flags&0x0F)
	if hold > t.cfg.BackoffMax || hold <= 0 { // <=0 guards shift overflow
		hold = t.cfg.BackoffMax
	}
	half := hold / 2
	wait := half + time.Duration(rand.Int64N(int64(half)+1))

	t.mu.Lock()
	defer t.mu.Unlock()
	fs, ok := t.flows[e.hashKey]
	if !ok {
		return
	}
	entry, ok := fs.pending[e.seqNum]
	if !ok {
		return
	}
	entry.retries++
	entry.nextAttempt = time.Now().Add(wait)
	if t.rec != nil {
		t.rec.NACKThrottled(flowLabel(e.groupIdx), srcStr(e.source))
	}
}

// advanceEndpoint updates retry state after a NACK attempt. It distinguishes
// tier escalation (free, instant progress toward the warm cache) from a failed
// recovery round (which backs off and consumes the retry budget):
//
//   - immediate=true (MISS) with tiers still to try: escalate to the next
//     endpoint and retry immediately. This is discovery, not failure, so the
//     fail-round counter resets and no backoff applies.
//   - otherwise (MISS at the deepest tier, or any timeout/error): count a
//     failed round and back off exponentially from BackoffBase, doubling per
//     round and capped at BackoffMax, with full jitter to de-synchronise
//     listeners. endpointIdx still advances so a timeout fails over toward the
//     deepest tier; the clamp in sendNACK keeps further attempts pinned there.
//
// numEndpoints is the live endpoint count (0 from error paths, which cannot
// know it and are always treated as a failed round).
func (t *Tracker) advanceEndpoint(e *gapEntry, immediate bool, numEndpoints int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	fs, ok := t.flows[e.hashKey]
	if !ok {
		return
	}
	entry, ok := fs.pending[e.seqNum]
	if !ok {
		return
	}

	entry.retries++
	entry.endpointIdx++

	exhausted := numEndpoints > 0 && entry.endpointIdx >= numEndpoints
	if immediate && !exhausted {
		// Clean tier escalation: instant retry at the next tier.
		entry.failRounds = 0
		entry.nextAttempt = time.Now()
		return
	}

	// Failed recovery round: exponential backoff seeded by consecutive failures,
	// not total attempts, so escalation depth never inflates the delay.
	entry.failRounds++
	backoff := t.cfg.BackoffBase << uint(entry.failRounds-1)
	if backoff > t.cfg.BackoffMax || backoff <= 0 { // <=0 guards shift overflow
		backoff = t.cfg.BackoffMax
	}
	// Full jitter over [backoff/2, backoff].
	half := backoff / 2
	entry.nextAttempt = time.Now().Add(half + time.Duration(rand.Int64N(int64(half)+1)))
}

// PendingGaps returns the total number of unresolved gap entries across all flows.
func (t *Tracker) PendingGaps() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	total := 0
	for _, fs := range t.flows {
		total += len(fs.pending)
	}
	return total
}

// ActiveFlows returns the number of active flows being tracked.
func (t *Tracker) ActiveFlows() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.flows)
}

// cancelGap removes a gap entry after receiving an ACK.
func (t *Tracker) cancelGap(e *gapEntry) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if fs, ok := t.flows[e.hashKey]; ok {
		if _, found := fs.pending[e.seqNum]; found {
			delete(fs.pending, e.seqNum)
			if t.rec != nil {
				t.rec.GapSuppressed(flowLabel(e.groupIdx), srcStr(e.source))
			}
		}
	}
}
