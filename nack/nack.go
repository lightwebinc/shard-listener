package nack

import (
	"context"
	"encoding/binary"
	"log/slog"
	"math/rand/v2"
	"net"
	"strings"
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
	// MaxForwardJump bounds a plausible in-flow burst: a forward SeqNum jump LARGER
	// than this on an established flow is an EMITTER CHANGE (e.g. anycast spine
	// failover between long-lived proxies with divergent in-memory counters), not
	// loss — the flow re-baselines instead of registering (and NACK-storming) a
	// phantom gap range. 0 = default 4096.
	MaxForwardJump uint64
	// TailProbe enables speculative probing of the next expected SeqNum on a flow
	// that has gone quiet. Gap detection is otherwise INFERENTIAL — frame N is only
	// known lost when N+1 arrives — so the last frames before a sender goes idle,
	// and losses on very low-rate flows, are invisible no matter how good the
	// retry path is. A probe asks the question the missing successor frame would
	// have answered. It needs no protocol addition: BRC-126 already defines a MISS
	// response, so a probe for a SeqNum that was never emitted is answered cheaply
	// and definitively.
	TailProbe bool
	// TailProbeIdleFactor multiplies the flow's smoothed inter-arrival estimate to
	// decide when silence is abnormal. Too low and healthy jitter triggers probes;
	// too high and the tail sits unrecovered. 0 = default 4.
	TailProbeIdleFactor float64
	// TailProbeMinIdle floors the idle threshold so a fast flow (sub-millisecond
	// inter-arrival) does not probe continuously. 0 = default 500ms.
	TailProbeMinIdle time.Duration
	// TailProbeMaxMisses stops probing a flow after this many consecutive MISS
	// answers — the sender is genuinely idle, not lossy. Reset by new traffic.
	// 0 = default 3.
	TailProbeMaxMisses int
}

// Tail-probe defaults. Chosen so a probe costs at most a handful of 64-byte
// NACKs per flow per idle period and then stops — the mechanism must not turn
// every idle flow into standing traffic.
const (
	defaultTailProbeIdleFactor = 4.0
	defaultTailProbeMinIdle    = 500 * time.Millisecond
	defaultTailProbeMaxMisses  = 3
)

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
		if groupIdx >= 0x1000 && groupIdx <= 0x1FFF {
			return "brc148" // BEEF object plane band (BRC-148 domain 0x1)
		}
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
	// ewmaIPG is a smoothed inter-arrival estimate (per OBSERVED frame) used to judge
	// whether a seq jump is plausible for the elapsed silence — the emitter-change
	// discriminator that works at any traffic rate. 0 until two frames seen.
	ewmaIPG time.Duration
	// contiguous counts consecutive in-order frames — how settled ewmaIPG is. A
	// re-baseline only NACK-recovers its transition tail once the rate estimate is
	// trustworthy (contiguous >= minContiguousForRecover); reset on any gap/re-baseline.
	contiguous uint64
	// probeMisses counts consecutive tail probes answered MISS. Reset by any new
	// frame, so a flow that goes quiet, is confirmed idle, then resumes, is
	// eligible to probe again on its next tail.
	probeMisses int
	// probing is the SeqNum of the in-flight tail probe (0 = none), so a slow
	// probe is not re-issued on every 100ms sweep.
	probing uint64
}

// minContiguousForRecover is how many consecutive in-order frames must have settled the
// inter-arrival estimate before a re-baseline trusts it enough to NACK-recover the
// transition tail. Below it, the estimate is too noisy to tell a real loss from emitter
// divergence, so the re-baseline drops silently (the pre-existing behavior).
const minContiguousForRecover = 16

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
	// speculative marks a tail probe: a NACK for a SeqNum that may never have been
	// emitted. It is a QUESTION, not an observed loss, so it must never count as a
	// detected gap — otherwise every idle flow inflates the unrecovered ratio with
	// phantom losses and the repair alerts fire on healthy fabrics.
	speculative bool
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
	recoverFn func(raw []byte) bool

	// nackSrc is the source address the NACK socket binds to (set via
	// SetNACKSource). Empty = wildcard (kernel picks per-route). Set this to a
	// globally routable address on a tunnelled fabric so the retry's unicast
	// return traffic routes back to this node.
	nackSrc string
	// seeds is the static retry-endpoint list, retained so SetNACKSource can
	// re-order it once this node's own address is known.
	seeds []string

	mu           sync.Mutex
	flows        map[uint64]*flowState // keyed by hashKey
	flowsRefused uint64                // new flows skipped at MaxFlows (flood guard); under mu

	// nackQueue receives gap entries ready for NACK dispatch.
	nackQueue chan *gapEntry

	// sem bounds concurrent sendNACK goroutines.
	sem chan struct{}
}

// demoteSelf moves this node's OWN retry endpoint to the END of the seed list.
// A retry co-located with the listener sits behind the SAME link and therefore
// missed exactly the frames the listener missed — it cannot repair link loss,
// only the listener's own local drops (socket overflow, where the separate retry
// process did receive). The tracker escalates ONE endpoint per retry with
// backoff, so leaving self first spends a gap's whole retry budget on the one
// endpoint guaranteed not to help, and the remote caches that DO hold the frames
// are reached only after the gap expires. Demoting rather than removing keeps
// the local-drop repair case, as a last resort.
func demoteSelf(endpoints []string, selfAddr string) []string {
	if selfAddr == "" || len(endpoints) < 2 {
		return endpoints
	}
	remote := make([]string, 0, len(endpoints))
	var self []string
	for _, ep := range endpoints {
		host, _, err := net.SplitHostPort(ep)
		if err == nil && sameHost(host, selfAddr) {
			self = append(self, ep)
			continue
		}
		remote = append(remote, ep)
	}
	return append(remote, self...)
}

// sameHost compares address literals, tolerating brackets and differing textual
// forms of the same IPv6 address.
func sameHost(a, b string) bool {
	ia, ib := net.ParseIP(strings.Trim(a, "[]")), net.ParseIP(strings.Trim(b, "[]"))
	if ia != nil && ib != nil {
		return ia.Equal(ib)
	}
	return strings.EqualFold(strings.Trim(a, "[]"), strings.Trim(b, "[]"))
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
		seeds:         retryEndpoints,
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
func (t *Tracker) SetRecoverFunc(f func(raw []byte) bool) { t.recoverFn = f }

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
func (t *Tracker) SetNACKSource(addr string) {
	t.nackSrc = addr
	// Re-seed with self demoted now that this node's own address is known (the
	// downstream embedders call this AFTER New).
	if len(t.seeds) > 0 && t.registry != nil {
		t.registry.Seed(demoteSelf(t.seeds, addr))
	}
}

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
	elapsed := now.Sub(fs.lastSeen)
	fs.lastSeen = now
	// Traffic resumed: a flow previously confirmed idle becomes eligible to probe
	// its next tail. Without this reset, a flow that goes quiet once would never
	// be probed again for the rest of its life.
	fs.probeMisses = 0
	// Sources are stable per flow. Keep the latest non-empty source so a recovered
	// frame re-injected by the tracker (which carries no live source) does not
	// clobber the real source attribution.
	if source != nil {
		fs.source = source
	}

	// Step 3: auto-fill — close any pending gap whose seqNum matches.
	if e, found := fs.pending[seqNum]; found {
		delete(fs.pending, seqNum)
		t.bookFill(fs, e)
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
		// Step 5: contiguous — update the inter-arrival estimate (the emitter-change
		// discriminator's rate input). EWMA α=1/8, integer-friendly.
		if elapsed > 0 {
			if fs.ewmaIPG == 0 {
				fs.ewmaIPG = elapsed
			} else {
				fs.ewmaIPG += (elapsed - fs.ewmaIPG) / 8
			}
		}
		fs.lastSeqNum = seqNum
		fs.contiguous++
		return
	}

	// Emitter change: a forward jump IMPLAUSIBLE for the elapsed silence means a
	// DIFFERENT proxy now stamps this flow (anycast failover between long-lived
	// emitters with divergent in-memory counters) — the intermediate range was never
	// emitted toward this listener. Re-baseline: drop pending phantoms silently
	// (artifacts of the old emitter's numbering, not losses) and track from here.
	// Plausibility is RATE-AWARE (a fixed jump cap cannot separate a real loss
	// burst from emitter divergence across traffic rates): a real outage of
	// `elapsed` at the flow's observed rate misses ≈ elapsed/ewmaIPG frames; allow
	// 4× headroom + a floor for bursty/low-rate flows. MaxForwardJump stays as the
	// absolute upper bound (and the whole guard when no rate estimate exists yet).
	missing := seqNum - fs.lastSeqNum - 1
	maxJump := t.cfg.MaxForwardJump
	if maxJump == 0 {
		maxJump = 4096
	}
	implausible := missing > maxJump
	if !implausible && fs.ewmaIPG > 0 && missing > 64 {
		expected := uint64(elapsed / fs.ewmaIPG)
		implausible = missing > 4*(expected+1)
	}
	if implausible {
		fs.pending = make(map[uint64]*gapEntry)
		// Anycast-failover recovery: the jump itself is the two emitters' divergent
		// counters, but a REAL transition outage of `elapsed` still lost ≈ elapsed/ewmaIPG
		// frames — and the NEW emitter stamped those just before `seqNum` (they were
		// RPF-dropped in the ~sub-second reconvergence) and holds them in ITS retry cache.
		// NACK that rate-plausible TAIL (bounded, so no storm) so the existing retry path
		// recovers the transition instead of it vanishing silently inside the phantom
		// range. Re-baseline the divergent remainder as before.
		if fs.ewmaIPG > 0 && fs.contiguous >= minContiguousForRecover {
			tail := uint64(elapsed / fs.ewmaIPG)
			if tail > missing {
				tail = missing
			}
			if tail > maxJump {
				tail = maxJump // safety cap; the rate estimate already bounds it
			}
			for m := seqNum - tail; m < seqNum; m++ {
				jitter := time.Duration(rand.Int64N(int64(t.cfg.JitterMax) + 1))
				fs.pending[m] = &gapEntry{
					hashKey:     hashKey,
					seqNum:      m,
					groupIdx:    groupIdx,
					subtreeID:   subtreeID,
					source:      source,
					nextAttempt: now.Add(jitter),
					deadline:    now.Add(t.cfg.GapTTL),
				}
				if t.rec != nil {
					t.rec.GapDetected(fs.flowType, srcStr(source))
				}
			}
		}
		fs.lastSeqNum = seqNum
		fs.ewmaIPG = 0    // new emitter, new cadence — re-learn
		fs.contiguous = 0 // rate estimate is stale across the emitter change
		if t.rec != nil {
			t.rec.SeqRebaselined(fs.flowType, srcStr(fs.source))
		}
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
	fs.contiguous = 0 // a real gap breaks the contiguous run
}

// bookFill records the metrics for a pending entry being closed, whatever path
// closed it (ACK response, auto-fill in Observe, or out-of-band Fill).
//
// A recovered PROBE is booked as detected AND suppressed at the same instant:
// detection is deliberately never booked at probe issue, so a MISS leaves no
// phantom loss behind, which means success is the only place the real loss it
// uncovered can be counted. Every fill path must go through here — an earlier
// version accounted only in the ACK path, so a probe whose frame returned via the
// DATA path incremented suppressed without detected and drove the repair ratio
// above 1.0. Caller must hold t.mu.
func (t *Tracker) bookFill(fs *flowState, e *gapEntry) {
	if fs.probing == e.seqNum {
		fs.probing = 0
	}
	if t.rec == nil {
		return
	}
	if e.speculative {
		fs.probeMisses = 0
		t.rec.TailProbeRecovered(fs.flowType, srcStr(fs.source))
		t.rec.GapDetected(fs.flowType, srcStr(fs.source))
	}
	t.rec.GapSuppressed(fs.flowType, srcStr(fs.source))
}

// RequestGaps registers explicit missing SeqNums on a flow and dispatches NACKs
// for them, for losses discovered by a means OTHER than a sequence discontinuity.
//
// The motivating case is an incomplete reassembly slot: fragments are cached and
// NACK-recoverable individually, but losing a trailing fragment loses the whole
// object with no successor frame to expose it, so without this the listener
// discards objects it could have asked for. These are REAL observed losses (the
// slot proves the object was in flight), so unlike a tail probe they are counted
// as detected gaps.
//
// Safe to call for a flow the tracker has never seen; it is created on demand.
func (t *Tracker) RequestGaps(hashKey uint64, groupIdx uint32, subtreeID [32]byte, seqs []uint64) {
	if len(seqs) == 0 {
		return
	}
	t.mu.Lock()
	now := time.Now()
	fs, ok := t.flows[hashKey]
	if !ok {
		if t.cfg.MaxFlows > 0 && len(t.flows) >= t.cfg.MaxFlows {
			t.mu.Unlock()
			return
		}
		fs = &flowState{
			groupIdx:  groupIdx,
			subtreeID: subtreeID,
			flowType:  flowLabel(groupIdx),
			pending:   make(map[uint64]*gapEntry),
			lastSeen:  now,
		}
		t.flows[hashKey] = fs
	}
	queued := make([]*gapEntry, 0, len(seqs))
	for _, seq := range seqs {
		if _, exists := fs.pending[seq]; exists {
			continue
		}
		jitter := time.Duration(rand.Int64N(int64(t.cfg.JitterMax) + 1))
		e := &gapEntry{
			hashKey:     hashKey,
			seqNum:      seq,
			groupIdx:    groupIdx,
			subtreeID:   subtreeID,
			source:      fs.source,
			nextAttempt: now.Add(jitter),
			deadline:    now.Add(t.cfg.GapTTL),
		}
		fs.pending[seq] = e
		cp := *e
		queued = append(queued, &cp)
		if t.rec != nil {
			t.rec.GapDetected(fs.flowType, srcStr(fs.source))
		}
	}
	t.mu.Unlock()

	// Enqueue outside the lock; a full queue simply defers to the next sweep,
	// which will pick these up via their nextAttempt.
	for _, e := range queued {
		select {
		case t.nackQueue <- e:
		default:
			return
		}
	}
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
		if e, found := fs.pending[seqNum]; found {
			delete(fs.pending, seqNum)
			t.bookFill(fs, e)
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
				// An unanswered PROBE is not a lost frame — it is an unanswered
				// question. Counting it as unrecovered would manufacture loss on
				// every quiet flow and make the repair-ratio alerts unusable.
				if e.speculative {
					t.retireProbe(fs, seq)
				} else if t.rec != nil {
					t.rec.GapUnrecovered(fs.flowType, srcStr(e.source))
				}
				t.log.Debug("gap evicted (TTL)",
					"hash_key", hk,
					"seq_num", e.seqNum,
					"speculative", e.speculative,
				)
				continue
			}
			if e.failRounds >= t.cfg.MaxRetries {
				delete(fs.pending, seq)
				if e.speculative {
					t.retireProbe(fs, seq)
				} else if t.rec != nil {
					t.rec.GapUnrecovered(fs.flowType, srcStr(e.source))
				}
				t.log.Debug("gap evicted (retries)",
					"hash_key", hk,
					"seq_num", e.seqNum,
					"speculative", e.speculative,
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

		// Tail probe: a flow with no pending gaps that has gone abnormally quiet
		// may be sitting on a lost tail that no successor frame will ever reveal.
		if len(fs.pending) == 0 {
			t.maybeProbeTail(hk, fs, now)
		}

		// Evict idle flows with no pending gaps.
		if len(fs.pending) == 0 && now.Sub(fs.lastSeen) > flowTTL {
			delete(t.flows, hk)
		}
	}
}

// maybeProbeTail issues a speculative NACK for the next expected SeqNum when a
// flow has been quiet for materially longer than its own established rhythm.
// Caller must hold t.mu.
//
// The idle threshold is derived from the flow's OWN smoothed inter-arrival
// (ewmaIPG) rather than a fixed timeout, because a "long silence" means nothing
// in absolute terms: 200ms is unremarkable on a 1-per-second flow and a total
// stall on a 10k/s one. That is also why the probe is gated on the same
// contiguity guard the re-baseline path uses — until the estimate has settled,
// silence cannot be distinguished from an unestablished rate.
func (t *Tracker) maybeProbeTail(hashKey uint64, fs *flowState, now time.Time) {
	if !t.cfg.TailProbe || fs.probing != 0 {
		return
	}
	// An unsettled rate estimate cannot tell an abnormal silence from a normal one.
	if fs.ewmaIPG <= 0 || fs.contiguous < minContiguousForRecover {
		return
	}
	maxMisses := t.cfg.TailProbeMaxMisses
	if maxMisses <= 0 {
		maxMisses = defaultTailProbeMaxMisses
	}
	// Confirmed idle: stop asking until the sender speaks again.
	if fs.probeMisses >= maxMisses {
		return
	}
	factor := t.cfg.TailProbeIdleFactor
	if factor <= 0 {
		factor = defaultTailProbeIdleFactor
	}
	minIdle := t.cfg.TailProbeMinIdle
	if minIdle <= 0 {
		minIdle = defaultTailProbeMinIdle
	}
	threshold := time.Duration(float64(fs.ewmaIPG) * factor)
	if threshold < minIdle {
		threshold = minIdle
	}
	if now.Sub(fs.lastSeen) < threshold {
		return
	}

	seq := fs.lastSeqNum + 1
	fs.probing = seq
	e := &gapEntry{
		hashKey:     hashKey,
		seqNum:      seq,
		groupIdx:    fs.groupIdx,
		subtreeID:   fs.subtreeID,
		source:      fs.source,
		nextAttempt: now,
		// A probe gets the ordinary gap lifetime: if it turns out to be a real
		// loss it should be retried like any other.
		deadline:    now.Add(t.cfg.GapTTL),
		speculative: true,
	}
	fs.pending[seq] = e
	if t.rec != nil {
		t.rec.TailProbeSent(flowLabel(fs.groupIdx), srcStr(fs.source))
	}
	t.log.Debug("tail probe issued",
		"hash_key", hashKey, "seq_num", seq,
		"idle", now.Sub(fs.lastSeen), "ewma_ipg", fs.ewmaIPG)

	entry := *e
	select {
	case t.nackQueue <- &entry:
	default:
		// Queue full — drop the probe entirely rather than let speculative work
		// displace real gap recovery. It will be reconsidered next sweep.
		delete(fs.pending, seq)
		fs.probing = 0
	}
}

// retireProbe clears in-flight probe state for a flow and counts the attempt
// against the consecutive-miss budget. Caller must hold t.mu.
func (t *Tracker) retireProbe(fs *flowState, seq uint64) {
	if fs.probing == seq {
		fs.probing = 0
	}
	fs.probeMisses++
}

// probeMissed retires a tail probe that was answered MISS: the SeqNum was never
// emitted, so the flow was simply idle. No gap is recorded — nothing was lost.
func (t *Tracker) probeMissed(e *gapEntry) {
	t.mu.Lock()
	defer t.mu.Unlock()

	fs, ok := t.flows[e.hashKey]
	if !ok {
		return
	}
	delete(fs.pending, e.seqNum)
	if fs.probing == e.seqNum {
		fs.probing = 0
	}
	fs.probeMisses++
	if t.rec != nil {
		t.rec.TailProbeMiss(flowLabel(fs.groupIdx), srcStr(fs.source))
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
			if !t.recoverFn(cp) {
				// The pipeline REJECTED the frame — a BRC-131 announce that
				// fails the block-control gate is the case. Asking again cannot
				// help: the retry endpoint holds exactly this frame and the gate
				// will reject it every time. Stop retrying, but do NOT book a
				// repair: the gap is real and permanent, so it must read as
				// unrecovered or the repair ratio reports success for data no
				// consumer ever received.
				t.abandonGap(e, "rejected")
				t.log.Debug("NACK: unicast retransmit rejected by the pipeline",
					"endpoint", endpoint.Addr, "seq_num", e.seqNum, "bytes", nr)
				return
			}
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
				if e.speculative {
					// A probe MISS is a definitive answer: that SeqNum does not
					// exist. Escalating tiers would only ask other caches about a
					// frame that was never sent, so retire the probe here.
					t.log.Debug("tail probe: MISS (flow idle, no loss)", "endpoint", endpoint.Addr, "seq_num", e.seqNum)
					t.probeMissed(e)
					return
				}
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
// abandonGap removes a gap that can never be filled, WITHOUT booking a fill.
// cancelGap is the success path (it calls bookFill and counts a suppression);
// this is the failure path, so the gap is counted unrecovered instead.
func (t *Tracker) abandonGap(e *gapEntry, reason string) {
	t.mu.Lock()
	fs, ok := t.flows[e.hashKey]
	if ok {
		delete(fs.pending, e.seqNum)
		if fs.probing == e.seqNum {
			fs.probing = 0
		}
	}
	t.mu.Unlock()
	if ok && t.rec != nil {
		// A speculative tail probe that comes back rejected was never an
		// observed loss, so it is retired silently — booking it would
		// manufacture phantom loss on an idle flow.
		if !e.speculative {
			t.rec.GapUnrecovered(fs.flowType, srcStr(fs.source))
		}
	}
	_ = reason
}

func (t *Tracker) cancelGap(e *gapEntry) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if fs, ok := t.flows[e.hashKey]; ok {
		if pend, found := fs.pending[e.seqNum]; found {
			delete(fs.pending, e.seqNum)
			// e is a shallow copy taken at dispatch; the stored entry is
			// authoritative for the speculative flag.
			t.bookFill(fs, pend)
		}
	}
}
