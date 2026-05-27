package nack

import (
	"context"
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
	BackoffMax        time.Duration // Cap on exponential backoff between retries
	MaxRetries        int           // Max NACK attempts before declaring unrecoverable
	GapTTL            time.Duration // Max lifetime of a gap entry (~Bitcoin block interval)
	TailTTL           time.Duration // Max idle time before a flow entry is evicted; 0 = GapTTL
	SeqResetThreshold uint64        // If seqNum <= threshold on an established flow, treat as proxy restart (default 100)
}

// ctrlGroupControl is the reserved group index for BRC-131 block control frames.
// Mirrors shard.CtrlGroupControl = 0xFFFE without importing the shard package.
const ctrlGroupControl uint32 = 0xFFFE

// ctrlGroupSubtreeAnnounce is the reserved group index for BRC-132 subtree data frames.
// Mirrors shard.CtrlGroupSubtreeAnnounce = 0xFFFB without importing the shard package.
const ctrlGroupSubtreeAnnounce uint32 = 0xFFFB

// ctrlGroupAnchor is the virtual group index for BRC-134 anchor transaction frames.
// Anchors share the CtrlGroupControl multicast address on the wire but use a
// dedicated groupIdx for HashKey derivation so they have their own SeqNum counter.
const ctrlGroupAnchor uint32 = 0xFFF9

// flowLabel returns "brc131" for block control flows, "brc132" for subtree data
// flows, "brc134" for anchor transaction flows, and "brc124" for all others.
func flowLabel(groupIdx uint32) string {
	switch groupIdx {
	case ctrlGroupControl:
		return "brc131"
	case ctrlGroupSubtreeAnnounce:
		return "brc132"
	case ctrlGroupAnchor:
		return "brc134"
	default:
		return "brc124"
	}
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
	pending    map[uint64]*gapEntry // keyed by missing seqNum
	lastSeen   time.Time
}

// gapEntry holds retry state for a single missing frame.
type gapEntry struct {
	hashKey     uint64
	seqNum      uint64 // the missing sequence number
	groupIdx    uint32
	subtreeID   [32]byte // for NACK SubtreeID field
	retries     int
	nextAttempt time.Time
	deadline    time.Time // absolute eviction deadline
	endpointIdx int       // round-robin index into registry snapshot
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

	mu    sync.Mutex
	flows map[uint64]*flowState // keyed by hashKey

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

	if registry == nil {
		registry = discovery.NewRegistry()
	}
	if len(retryEndpoints) > 0 {
		registry.Seed(retryEndpoints)
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
func (t *Tracker) Observe(groupIdx uint32, subtreeID [32]byte, hashKey, seqNum uint64, txid [32]byte) {
	if seqNum == 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()

	fs, ok := t.flows[hashKey]
	if !ok {
		fs = &flowState{
			groupIdx:  groupIdx,
			subtreeID: subtreeID,
			flowType:  flowLabel(groupIdx),
			pending:   make(map[uint64]*gapEntry),
		}
		t.flows[hashKey] = fs
	}
	fs.lastSeen = now

	// Step 3: auto-fill — close any pending gap whose seqNum matches.
	if _, found := fs.pending[seqNum]; found {
		delete(fs.pending, seqNum)
		if t.rec != nil {
			t.rec.GapSuppressed(fs.flowType)
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
					t.rec.GapUnrecovered(fs.flowType)
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
				nextAttempt: now.Add(jitter),
				deadline:    now.Add(t.cfg.GapTTL),
			}
			fs.pending[missing] = e
			if t.rec != nil {
				t.rec.GapDetected(fs.flowType)
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
				t.rec.GapSuppressed(fs.flowType)
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
					t.rec.GapUnrecovered(fs.flowType)
				}
				t.log.Debug("gap evicted (TTL)",
					"hash_key", hk,
					"seq_num", e.seqNum,
				)
				continue
			}
			if e.retries >= t.cfg.MaxRetries {
				delete(fs.pending, seq)
				if t.rec != nil {
					t.rec.GapUnrecovered(fs.flowType)
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
	conn, err := net.ListenPacket("udp", "[::]:0")
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
		t.rec.NACKDispatched(flowLabel(e.groupIdx))
	}
	t.log.Debug("NACK dispatched",
		"endpoint", endpoint.Addr,
		"tier", endpoint.Tier,
		"hash_key", e.hashKey,
		"seq_num", e.seqNum,
		"retry", e.retries+1,
	)

	_ = conn.SetReadDeadline(time.Now().Add(t.respTimeout))
	var respBuf [ResponseSize + 16]byte
	nr, _, err := conn.ReadFrom(respBuf[:])
	if err != nil {
		t.advanceEndpoint(e, false, 0)
		return
	}

	resp, err := DecodeResponse(respBuf[:nr])
	if err != nil {
		t.log.Debug("NACK: invalid response", "endpoint", endpoint.Addr, "err", err)
		t.advanceEndpoint(e, false, 0)
		return
	}

	switch resp.MsgType {
	case MsgTypeACK:
		t.cancelGap(e)
		t.log.Debug("NACK: ACK received, gap cancelled",
			"endpoint", endpoint.Addr,
			"seq_num", e.seqNum,
			"flags", resp.Flags,
		)
	case MsgTypeMISS:
		t.log.Debug("NACK: MISS received, advancing endpoint",
			"endpoint", endpoint.Addr,
			"seq_num", e.seqNum,
		)
		t.advanceEndpoint(e, true, len(snap))
	}
}

// advanceEndpoint updates retry state after a NACK attempt.
//
//   - immediate=true (MISS): retry now at the next endpoint, provided we have
//     not yet exhausted the endpoint list. Once endpointIdx reaches
//     numEndpoints, the gap has tried every tier; further MISS responses use
//     exponential backoff to give the deepest cache time to warm before
//     retrying. numEndpoints==0 disables this clamp (used by error paths).
//   - immediate=false (timeout/error): exponential backoff.
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
		entry.nextAttempt = time.Now()
	} else {
		backoff := time.Duration(1<<uint(entry.retries)) * 500 * time.Millisecond
		if backoff > t.cfg.BackoffMax {
			backoff = t.cfg.BackoffMax
		}
		entry.nextAttempt = time.Now().Add(backoff)
	}
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
				t.rec.GapSuppressed(flowLabel(e.groupIdx))
			}
		}
	}
}
