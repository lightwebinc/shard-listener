package nack

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/lightwebinc/bitcoin-shard-listener/discovery"
	"github.com/lightwebinc/bitcoin-shard-listener/metrics"
)

// TrackerConfig holds tuning parameters for the gap tracker.
type TrackerConfig struct {
	JitterMax  time.Duration // Max random hold-off before first NACK (NORM suppression window)
	BackoffMax time.Duration // Cap on exponential backoff between retries
	MaxRetries int           // Max NACK attempts before declaring unrecoverable
	GapTTL     time.Duration // Max lifetime of a gap entry (~Bitcoin block interval)
	TailTTL    time.Duration // Max idle time before a chain tail is evicted; 0 = GapTTL
}

// chainTail tracks one active hash chain within a group.
//
// Tails are keyed by lastCurSeq in groupState.tails for O(1) PrevSeq matching.
// An orphan tail (chainID == 0) was created when a gap was detected; it gains
// a chainID when its predecessor gap is filled (cascade merge).
type chainTail struct {
	chainID    uint64    // initial CurSeq of the chain (set when PrevSeq==0); 0 = orphan
	lastCurSeq uint64    // most recent CurSeq observed for this chain
	lastSeen   time.Time // for stale-tail GC
	gapCurSeq  uint64    // if orphan: the gap key (curSeq) that spawned this tail; 0 if attributed
}

// gapEntry holds retry state for a single missing frame.
//
// curSeq is the CurSeq of the missing frame (= PrevSeq of the frame that
// revealed the gap). It is used as the backward lookup key: LookupByCurSeq.
//
// leftBoundary, when non-zero, is the CurSeq of the last good frame before
// the gap. It enables the forward lookup: LookupByPrevSeq(leftBoundary).
// It is set at detection time when the group has exactly one active tail
// (single-sender case); otherwise it remains 0 and only the backward NACK
// is dispatched.
type gapEntry struct {
	curSeq       uint64 // backward NACK target: LookupByCurSeq
	leftBoundary uint64 // forward NACK target: LookupByPrevSeq; 0 = unknown
	chainID      uint64 // 0 = unattributed
	groupIdx     uint32
	retries      int
	nextAttempt  time.Time
	deadline     time.Time // absolute eviction deadline
	endpointIdx  int       // round-robin index into registry snapshot
}

// groupState holds all chains and pending gaps for one groupIdx.
type groupState struct {
	tails   map[uint64]*chainTail // keyed by chainTail.lastCurSeq
	pending map[uint64]*gapEntry  // keyed by gapEntry.curSeq
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

	mu     sync.Mutex
	states map[uint32]*groupState // keyed by groupIdx

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
		states:        make(map[uint32]*groupState),
		nackQueue:     make(chan *gapEntry, 4096),
		sem:           make(chan struct{}, defaultMaxConcurrent),
	}
}

// Observe is called by the listener worker on every BRC-124/BRC-128 frame.
// It detects chain breaks via tail matching and schedules NACKs.
// curSeq == 0 means the proxy has not yet stamped the frame; it is ignored.
//
// Processing steps:
//  1. Skip unstamped frames (curSeq == 0).
//  2. Fill check: if curSeq matches a pending gap key, close it.
//  3. Duplicate check: if curSeq is already a known tail, update lastSeen and return.
//  4. New chain start: if prevSeq == 0, register a new attributed tail.
//  5. Chain extension: if prevSeq matches a known tail, advance it and cascade-merge orphans.
//  6. Gap detected: prevSeq matches no tail — register a gap entry and create an orphan tail.
func (t *Tracker) Observe(groupIdx uint32, prevSeq, curSeq uint64, txid [32]byte) {
	if curSeq == 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()

	st, ok := t.states[groupIdx]
	if !ok {
		st = &groupState{
			tails:   make(map[uint64]*chainTail),
			pending: make(map[uint64]*gapEntry),
		}
		t.states[groupIdx] = st
	}

	// Step 2: fill check — close any pending gap whose key equals curSeq.
	if _, found := st.pending[curSeq]; found {
		delete(st.pending, curSeq)
		if t.rec != nil {
			t.rec.GapSuppressed()
		}
		// Do not return; this frame still participates in chain tracking below.
	}

	// Step 3: duplicate check — curSeq already recorded as a tail.
	if tail, isDup := st.tails[curSeq]; isDup {
		tail.lastSeen = now
		return
	}

	// Step 4: new chain start (PrevSeq == 0).
	if prevSeq == 0 {
		st.tails[curSeq] = &chainTail{
			chainID:    curSeq,
			lastCurSeq: curSeq,
			lastSeen:   now,
		}
		return
	}

	// Step 5: extend an existing chain (PrevSeq matches a known tail).
	if tail, found := st.tails[prevSeq]; found {
		delete(st.tails, prevSeq)
		tail.lastCurSeq = curSeq
		tail.lastSeen = now
		st.tails[curSeq] = tail
		// Cascade: merge orphan tails that were waiting for this gap to be filled.
		t.cascadeMerge(st, tail.chainID, curSeq)
		return
	}

	// Step 6: gap detected — prevSeq matches no known tail.
	gapKey := prevSeq // = CurSeq of the missing frame; backward NACK key
	if _, already := st.pending[gapKey]; !already {
		jitter := time.Duration(rand.Int64N(int64(t.cfg.JitterMax) + 1))

		// leftBoundary is knowable only when a single tail exists in this group
		// (the common single-sender case). With multiple tails we cannot
		// determine which chain the gap belongs to, so we fall back to
		// backward-only NACK.
		var leftBoundary uint64
		if len(st.tails) == 1 {
			for _, tail := range st.tails {
				leftBoundary = tail.lastCurSeq
			}
		}

		e := &gapEntry{
			curSeq:       gapKey,
			leftBoundary: leftBoundary,
			groupIdx:     groupIdx,
			nextAttempt:  now.Add(jitter),
			deadline:     now.Add(t.cfg.GapTTL),
		}
		st.pending[gapKey] = e
		if t.rec != nil {
			t.rec.GapDetected()
		}
	}

	// Register an orphan tail for the frame that revealed the gap.
	// gapCurSeq records which gap created this orphan so it can be merged
	// when the predecessor gap is eventually filled.
	st.tails[curSeq] = &chainTail{
		chainID:    0,
		lastCurSeq: curSeq,
		lastSeen:   now,
		gapCurSeq:  prevSeq,
	}
}

// cascadeMerge attributes orphan tails to chainID by following the gapCurSeq
// links. Called after a chain is extended to newTailCurSeq.
func (t *Tracker) cascadeMerge(st *groupState, chainID, newTailCurSeq uint64) {
	queue := []uint64{newTailCurSeq}
	for len(queue) > 0 {
		gapCurSeq := queue[0]
		queue = queue[1:]
		for _, orphan := range st.tails {
			if orphan.chainID == 0 && orphan.gapCurSeq == gapCurSeq {
				orphan.chainID = chainID
				orphan.gapCurSeq = 0
				queue = append(queue, orphan.lastCurSeq)
			}
		}
	}
}

// Fill cancels a pending gap when a retransmitted frame arrives via multicast
// with the given (groupIdx, curSeq). This is a lighter-weight path for callers
// that do not have the full frame; Observe handles the same fill check
// automatically when the retransmit is processed as a regular frame.
func (t *Tracker) Fill(groupIdx uint32, curSeq uint64) {
	if curSeq == 0 {
		return
	}
	t.mu.Lock()
	if st, ok := t.states[groupIdx]; ok {
		if _, found := st.pending[curSeq]; found {
			delete(st.pending, curSeq)
			if t.rec != nil {
				t.rec.GapSuppressed()
			}
		}
	}
	t.mu.Unlock()
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

	tailTTL := t.cfg.TailTTL
	if tailTTL <= 0 {
		tailTTL = t.cfg.GapTTL
	}

	for groupIdx, st := range t.states {
		for key, e := range st.pending {
			if now.After(e.deadline) {
				delete(st.pending, key)
				if t.rec != nil {
					t.rec.GapUnrecovered()
				}
				t.log.Debug("gap evicted (TTL)",
					"group", groupIdx,
					"cur_seq", e.curSeq,
				)
				continue
			}
			if e.retries >= t.cfg.MaxRetries {
				delete(st.pending, key)
				if t.rec != nil {
					t.rec.GapUnrecovered()
				}
				t.log.Debug("gap evicted (retries)",
					"group", groupIdx,
					"cur_seq", e.curSeq,
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

		// Stale tail eviction: remove tails idle beyond TailTTL.
		for curSeq, tail := range st.tails {
			if now.Sub(tail.lastSeen) > tailTTL {
				delete(st.tails, curSeq)
				t.log.Debug("tail evicted (TTL)",
					"group", groupIdx,
					"chain_id", tail.chainID,
					"last_cur_seq", tail.lastCurSeq,
				)
			}
		}

		if len(st.pending) == 0 && len(st.tails) == 0 {
			delete(t.states, groupIdx)
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

// sendNACK dispatches NACK(s) to a retry endpoint, then waits for the first
// ACK/MISS response. On ACK the gap is cancelled; on MISS or timeout, retry
// state advances with exponential backoff.
//
// When leftBoundary is known (single-sender case), two NACKs are sent in
// parallel: forward (LookupByPrevSeq) and backward (LookupByCurSeq). When
// leftBoundary is 0 (multi-sender or unattributed), only the backward NACK is
// sent. Chain reconnection via cascadeMerge populates leftBoundary on future
// retry attempts once the chain is identified.
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

	// Backward NACK: find frame whose CurSeq == e.curSeq.
	var bwdBuf [NACKSize]byte
	Encode(&NACK{MsgType: MsgTypeNACK, LookupType: LookupByCurSeq, LookupSeq: e.curSeq, ChainID: e.chainID}, bwdBuf[:])
	_, _ = conn.WriteTo(bwdBuf[:], addr)

	// Forward NACK: sent only when leftBoundary is known (single-sender or
	// after chain reconnection). Finds the frame whose PrevSeq == leftBoundary.
	if e.leftBoundary != 0 {
		var fwdBuf [NACKSize]byte
		Encode(&NACK{MsgType: MsgTypeNACK, LookupType: LookupByPrevSeq, LookupSeq: e.leftBoundary, ChainID: e.chainID}, fwdBuf[:])
		_, _ = conn.WriteTo(fwdBuf[:], addr)
	}

	if t.rec != nil {
		t.rec.NACKDispatched()
	}
	t.log.Debug("NACK dispatched",
		"endpoint", endpoint.Addr,
		"tier", endpoint.Tier,
		"left_boundary", e.leftBoundary,
		"cur_seq", e.curSeq,
		"retry", e.retries+1,
	)

	// Wait for any response (ACK or MISS). Both NACKs share the same socket,
	// so the first response received determines the action.
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
			"cur_seq", e.curSeq,
			"flags", resp.Flags,
		)
	case MsgTypeMISS:
		t.log.Debug("NACK: MISS received, advancing endpoint",
			"endpoint", endpoint.Addr,
			"cur_seq", e.curSeq,
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

	st, ok := t.states[e.groupIdx]
	if !ok {
		return
	}
	entry, ok := st.pending[e.curSeq]
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

// PendingGaps returns the total number of unresolved gap entries across all
// groups. Useful for testing and diagnostics.
func (t *Tracker) PendingGaps() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	total := 0
	for _, st := range t.states {
		total += len(st.pending)
	}
	return total
}

// GapLeftBoundary returns the leftBoundary stored for the gap entry identified
// by (groupIdx, gapCurSeq). Returns 0 if the gap does not exist. For testing.
func (t *Tracker) GapLeftBoundary(groupIdx uint32, gapCurSeq uint64) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if st, ok := t.states[groupIdx]; ok {
		if e, ok := st.pending[gapCurSeq]; ok {
			return e.leftBoundary
		}
	}
	return 0
}

// ActiveTails returns the total number of chain tails tracked across all
// groups. Useful for testing and diagnostics.
func (t *Tracker) ActiveTails() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	total := 0
	for _, st := range t.states {
		total += len(st.tails)
	}
	return total
}

// cancelGap removes a gap entry after receiving an ACK.
func (t *Tracker) cancelGap(e *gapEntry) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if st, ok := t.states[e.groupIdx]; ok {
		if _, found := st.pending[e.curSeq]; found {
			delete(st.pending, e.curSeq)
			if t.rec != nil {
				t.rec.GapSuppressed()
			}
		}
	}
}
