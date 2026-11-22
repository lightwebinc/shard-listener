package main

import (
	"context"
	"errors"
	"log/slog"
	"net/netip"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/shard-listener/config"
	"github.com/lightwebinc/shard-listener/filter"
)

// groupJoiner is the part of a receive worker that runtime auto-join drives
// (*listener.Worker).
type groupJoiner interface {
	AddGroup(group netip.Addr, sources []netip.Addr) error
	RemoveGroup(group netip.Addr, sources []netip.Addr) error
}

// startupShards returns the shard indices the workers join at startup: the
// -shard-include list, or every shard when it is empty.
func startupShards(cfg *config.Config) []uint32 {
	if len(cfg.ShardInclude) > 0 {
		return cfg.ShardInclude
	}
	all := make([]uint32, cfg.NumGroups)
	for i := range all {
		all[i] = uint32(i)
	}
	return all
}

// Retry pacing for a pilot group whose join (or leave) failed, per group:
// the first retry after autoJoinRetryBase, doubling to autoJoinRetryMax.
// Each failed attempt logs once, so the backoff also paces the log.
const (
	autoJoinRetryBase = 2 * time.Second
	autoJoinRetryMax  = 2 * time.Minute
	autoJoinTick      = time.Second
)

// autoJoin keeps the BRC-139 pilot-group subscription
// (-shard-include-from-manifest) in step with the adopted pilot set:
//
//   - wanted is the set of pilot groups the manifests claim, beyond the
//     startup shards, which the workers joined in Run and which auto-join
//     never touches or leaves;
//   - installed records, per worker, the pilot groups that worker has joined,
//     and so has to leave once they are no longer wanted;
//   - the receive filter admits the startup shards plus every wanted group,
//     replaced whole on each change (filter.SetRuntimeShards).
//
// A wanted group is asserted on every worker (AddGroup joins whatever sources
// it lacks and is otherwise a no-op), an installed one no longer wanted is
// left. A join or leave that fails is retried with backoff until it
// succeeds or the pilot set stops asking for it, so a membership refused for
// lack of source-filter capacity (ENOBUFS) is restored once capacity is freed.
// Under SSM every join and leave names the publisher set, like the startup
// data-plane joins: a (*,G) join in the SSM range is not routed.
type autoJoin struct {
	engine  *shard.Engine
	port    int
	static  map[uint16]struct{}
	sources []netip.Addr // nil under ASM
	filt    *filter.Filter
	now     func() time.Time

	mu        sync.Mutex
	workers   []groupJoiner
	installed []map[uint16]struct{}
	wanted    map[uint16]struct{}
	retry     map[uint16]*joinRetry
}

// joinRetry is the backoff state of a group whose last join or leave failed.
type joinRetry struct {
	at       time.Time
	delay    time.Duration
	attempts int
}

func newAutoJoin(engine *shard.Engine, port int, static []uint32, sources []netip.Addr, filt *filter.Filter) *autoJoin {
	a := &autoJoin{
		engine:  engine,
		port:    port,
		static:  make(map[uint16]struct{}, len(static)),
		sources: sources,
		filt:    filt,
		now:     time.Now,
		wanted:  map[uint16]struct{}{},
		retry:   map[uint16]*joinRetry{},
	}
	for _, idx := range static {
		a.static[uint16(idx)] = struct{}{}
	}
	return a
}

// setWorkers hands the receive workers to auto-join once they exist and
// installs the wanted groups on them.
func setWorkers[W groupJoiner](a *autoJoin, workers []W) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.workers = make([]groupJoiner, len(workers))
	a.installed = make([]map[uint16]struct{}, len(workers))
	for i, w := range workers {
		a.workers[i] = w
		a.installed[i] = map[uint16]struct{}{}
	}
	a.reconcileLocked()
}

// pilotsChanged applies an adopted pilot-set change (the applier's
// OnPilotGroupsChange hook): the receive filter follows at once, and the
// memberships are reconciled. A group added again starts a fresh attempt; a
// group removed stops being retried.
func (a *autoJoin) pilotsChanged(added, removed []uint16) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, idx := range added {
		if _, startup := a.static[idx]; startup {
			continue
		}
		a.wanted[idx] = struct{}{}
		delete(a.retry, idx)
	}
	for _, idx := range removed {
		if _, ok := a.wanted[idx]; ok {
			delete(a.wanted, idx)
			delete(a.retry, idx)
		}
	}
	a.updateFilterLocked()
	a.reconcileLocked()
	slog.Info("auto-join applied", "added", added, "removed", removed,
		"wanted", len(a.wanted), "sources", len(a.sources))
}

// run retries failed joins and leaves until ctx ends.
func (a *autoJoin) run(ctx context.Context) {
	t := time.NewTicker(autoJoinTick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			a.tick()
		}
	}
}

// tick retries the groups whose backoff has elapsed; nothing to do when no
// join or leave has failed.
func (a *autoJoin) tick() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.retry) > 0 {
		a.reconcileLocked()
	}
}

func (a *autoJoin) updateFilterLocked() {
	if a.filt == nil {
		return
	}
	shards := make([]uint32, 0, len(a.wanted))
	for idx := range a.wanted {
		shards = append(shards, uint32(idx))
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })
	a.filt.SetRuntimeShards(shards)
}

// reconcileLocked joins every wanted group a worker lacks and leaves every
// installed group that is no longer wanted, skipping groups still in backoff.
func (a *autoJoin) reconcileLocked() {
	now := a.now()
	// failed records each group attempted in this pass: true when any of its
	// attempts failed.
	failed := map[uint16]bool{}
	succeeded := func(idx uint16) {
		if _, seen := failed[idx]; !seen {
			failed[idx] = false
		}
	}
	for i, w := range a.workers {
		inst := a.installed[i]
		// A wanted group is asserted on every pass, held or not: AddGroup is a
		// no-op for a worker that holds every source, and repairs one whose
		// membership a failed leave cut short. installed only says what to
		// leave once the group is no longer wanted.
		for idx := range a.wanted {
			if !a.due(idx, now) {
				continue
			}
			ga, ok := a.group(idx)
			if !ok {
				continue
			}
			if err := w.AddGroup(ga, a.sources); err != nil {
				logJoinFailure(ga, err, a.retry[idx])
				failed[idx] = true
				continue
			}
			inst[idx] = struct{}{}
			succeeded(idx)
		}
		for idx := range inst {
			if _, ok := a.wanted[idx]; ok || !a.due(idx, now) {
				continue
			}
			ga, ok := a.group(idx)
			if !ok {
				delete(inst, idx)
				continue
			}
			if err := w.RemoveGroup(ga, a.sources); err != nil {
				slog.Warn("auto-join RemoveGroup failed; will retry", "group", ga.String(), "err", err)
				failed[idx] = true
				continue
			}
			delete(inst, idx)
			succeeded(idx)
		}
	}
	for idx, f := range failed {
		if f {
			a.backoff(idx, now)
			continue
		}
		if r, ok := a.retry[idx]; ok && a.settled(idx) {
			if ga, gok := a.group(idx); gok {
				slog.Info("auto-join membership recovered", "group", ga.String(), "attempts", r.attempts+1)
			}
			delete(a.retry, idx)
		}
	}
}

// due reports whether idx may be attempted now (not waiting out a backoff).
func (a *autoJoin) due(idx uint16, now time.Time) bool {
	r, ok := a.retry[idx]
	return !ok || !now.Before(r.at)
}

// settled reports whether every worker is in the state the pilot set asks
// for idx: holding it when wanted, not holding it when not.
func (a *autoJoin) settled(idx uint16) bool {
	_, want := a.wanted[idx]
	for _, inst := range a.installed {
		if _, has := inst[idx]; has != want {
			return false
		}
	}
	return true
}

func (a *autoJoin) backoff(idx uint16, now time.Time) {
	r, ok := a.retry[idx]
	if !ok {
		r = &joinRetry{delay: autoJoinRetryBase}
		a.retry[idx] = r
	} else {
		r.delay = min(2*r.delay, autoJoinRetryMax)
	}
	r.attempts++
	r.at = now.Add(r.delay)
}

func (a *autoJoin) group(idx uint16) (netip.Addr, bool) {
	addr := a.engine.Addr(uint32(idx), a.port)
	ga, ok := netip.AddrFromSlice(addr.IP.To16())
	if !ok {
		slog.Warn("auto-join: bad group address", "idx", idx)
	}
	return ga, ok
}

func logJoinFailure(ga netip.Addr, err error, r *joinRetry) {
	attempts := 1
	if r != nil {
		attempts = r.attempts + 1
	}
	var errno syscall.Errno
	if errors.As(err, &errno) && errno == syscall.ENOBUFS {
		// mld_max_msf source-filter exhaustion is the canonical at-scale
		// join failure (category-8).
		slog.Error("auto-join failed: MLD source-filter exhausted; raise net.ipv6.mld_max_msf; will retry",
			"group", ga.String(), "err", err, "errno", errno.Error(), "syscall", "setsockopt", "attempt", attempts)
		return
	}
	slog.Warn("auto-join AddGroup failed; will retry", "group", ga.String(), "err", err, "attempt", attempts)
}
