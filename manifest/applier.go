// Package manifest hosts the listener-side applier that consumes the
// BRC-139 manifest evaluator's adopted view and translates it into
// concrete actions on the listener's join state and metrics.
//
// The applier runs on a 1-second tick. It is deliberately simple: every
// tick it evicts expired registry entries, runs the evaluator, and
// publishes the new view to whichever pluggable hooks the caller
// supplied. The first evaluation is compared with the configuration the
// process runs ([Running]), not with a zero value, and divergence from it is
// reported, never acted on: the listener observes; it does not restart.
package manifest

import (
	"context"
	"log/slog"
	"maps"
	"net/netip"
	"reflect"
	"slices"
	"sort"
	"time"

	commanifest "github.com/lightwebinc/shard-common/manifest"

	"github.com/lightwebinc/shard-listener/metrics"
)

// Hooks are pluggable change handlers fired by [Applier]. Any field can
// be nil; nil means "ignore this kind of change." Each fires once per
// transition: the first evaluation is compared with [Applier.Running],
// each later one with the evaluation before it.
//
// A hook must not restart the process to apply a new value: the restarted
// process re-reads the same flags, runs the old value and sees the same
// transition again. With the evaluator built from [Running.Pin] the value
// hooks (ShardBits, source mode, domain ShardBits) never fire, because
// adoption cannot move a pinned value; disagreement is divergence.
type Hooks struct {
	// OnShardBitsChange fires when the adopted ShardBits moves away from
	// the running value, then from the previous one. The evaluator reports
	// 0 until it adopts a value; 0 never reads as a transition.
	OnShardBitsChange func(prev, next uint8)

	// OnSourceModeChange fires on a SourceModeSSM transition. The
	// evaluator reports ASM (false) until it adopts a mode, so a process
	// running SSM must pin the mode ([Running.Pin]) or this can report a
	// transition that is only the absence of adoption.
	OnSourceModeChange func(prevSSM, nextSSM bool)

	// OnPilotGroupsChange fires when the auto-join set diffs (added or
	// removed members).
	OnPilotGroupsChange func(added, removed []uint16)

	// OnSourceSetChange fires when the union of SourcesValid payloads
	// changes (added or removed members).
	OnSourceSetChange func(added, removed []netip.Addr)

	// OnSuccessorChange fires when a Successor view appears, changes,
	// or disappears. After is nil when the Successor is no longer
	// adopted (cutover or quorum loss).
	OnSuccessorChange func(before, after *commanifest.SuccessorView)

	// OnDomainShardBitsChange fires when the adopted ShardBits of an object
	// plane the process runs ([Running.DomainShardBits]) moves away from
	// its running width, then from the previous one, keyed by DomainID
	// (domain 0x0 is OnShardBitsChange). Planes the process does not run
	// never fire, and a plane the evaluator has not adopted (absent, or
	// width 0) keeps its previous width.
	OnDomainShardBitsChange func(domain, prev, next uint8)
}

// Running is the configuration the process runs with: the values it was
// started from (CLI/env). The [Applier] compares the adopted view with it,
// never with a zero value, so a manifest that agrees with the running
// configuration fires nothing.
//
// A restart re-reads the same flags and comes back with the same Running
// view: nothing adopted at run time survives it. Build the evaluator with
// [Running.Pin] so adoption never moves a value the process cannot apply,
// and announcers that disagree surface as divergence instead.
type Running struct {
	ShardBits     uint8 // transaction plane (domain 0x0) shard-bit width
	SourceModeSSM bool  // true when the data plane runs SSM

	// DomainShardBits is the shard-bit width of each BRC-148 object plane
	// the process runs, keyed by DomainID. Per-domain transitions are
	// reported only for these planes.
	DomainShardBits map[uint8]uint8
}

// Pin returns an evaluator pin that holds every running value. The CLI
// value is the local authority (BRC-139 manual pin precedence): adoption
// never moves it, and an authoritative announcer that disagrees with it is
// reported in [commanifest.Adopted.DivergenceFields].
func (r Running) Pin() commanifest.Pin {
	return commanifest.Pin{
		ShardBits:        r.ShardBits,
		HasShardBitsPin:  true,
		SourceModeSSM:    r.SourceModeSSM,
		HasSourceModePin: true,
		DomainShardBits:  maps.Clone(r.DomainShardBits),
	}
}

// adopted returns the running configuration as an adopted view: the
// baseline the first evaluation is compared with.
func (r Running) adopted() commanifest.Adopted {
	v := commanifest.Adopted{ShardBits: r.ShardBits, SourceModeSSM: r.SourceModeSSM}
	if len(r.DomainShardBits) > 0 {
		v.Domains = make(map[uint8]commanifest.DomainAdoption, len(r.DomainShardBits))
		for id, bits := range r.DomainShardBits {
			v.Domains[id] = commanifest.DomainAdoption{ShardBits: bits}
		}
	}
	return v
}

// Applier is the periodic evaluator+notifier. Field zero values are
// safe; callers should set Registry, Evaluator, and at least one Hook
// before calling [Applier.Run]. It never restarts the process:
// divergence from the running configuration is logged when it starts and
// when it clears, and counted on every tick.
type Applier struct {
	Registry  *commanifest.Registry
	Evaluator *commanifest.Evaluator
	Hooks     Hooks
	Rec       *metrics.Recorder
	Log       *slog.Logger
	Interval  time.Duration // default 1s

	// Running is the configuration this process runs with: the baseline
	// the first evaluation is compared with. See [Running].
	Running Running

	// evaluated, when set, is called after each evaluation, once its hooks,
	// metrics and logs are done, with the view the applier compared. Tests
	// wait on it instead of sleeping.
	evaluated func(commanifest.Adopted)
}

// Run loops on Interval, evicting expired entries, evaluating, and
// firing hooks on change. Blocks until ctx is cancelled.
func (a *Applier) Run(ctx context.Context) {
	interval := a.Interval
	if interval == 0 {
		interval = 1 * time.Second
	}
	log := a.Log
	if log == nil {
		log = slog.Default().With("component", "manifest-applier")
	}

	t := time.NewTicker(interval)
	defer t.Stop()

	prev := a.Running.adopted()
	var diverging []string
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		a.Registry.Evict()
		snap := a.Registry.Snapshot()
		next := a.Evaluator.Evaluate(snap)
		a.publishMetrics(next)
		next = a.settle(prev, next)
		a.fireHooks(prev, next, log)
		diverging = a.logDivergence(log, diverging, next.DivergenceFields)
		prev = next
		if a.evaluated != nil {
			a.evaluated(next)
		}
	}
}

// settle returns next as the applier compares it. A ShardBits of 0 means
// nothing adopted yet (the transaction plane runs 1-12) and keeps the
// previous value. Domains holds exactly the planes the process runs, each
// keeping its previous width until the evaluator adopts a non-zero one.
// The pilot groups and the source set are compared as evaluated.
func (a *Applier) settle(prev, next commanifest.Adopted) commanifest.Adopted {
	if next.ShardBits == 0 {
		next.ShardBits = prev.ShardBits
	}
	var doms map[uint8]commanifest.DomainAdoption
	if len(a.Running.DomainShardBits) > 0 {
		doms = make(map[uint8]commanifest.DomainAdoption, len(a.Running.DomainShardBits))
		for id := range a.Running.DomainShardBits {
			d := next.Domains[id]
			if d.ShardBits == 0 {
				d.ShardBits = prev.Domains[id].ShardBits
			}
			doms[id] = d
		}
	}
	next.Domains = doms
	return next
}

// logDivergence logs each divergence field once when it appears and once
// when it clears (the counter records every tick) and returns the fields
// now diverging. The running configuration stays in effect either way.
func (a *Applier) logDivergence(log *slog.Logger, was, now []string) []string {
	for _, f := range now {
		if !slices.Contains(was, f) {
			log.Warn("manifest divergence: authoritative announcers disagree with each other or with the running configuration",
				"field", f,
				"running_shard_bits", a.Running.ShardBits,
				"running_source_mode_ssm", a.Running.SourceModeSSM,
				"running_domain_shard_bits", a.Running.DomainShardBits,
				"effect", "none: the running configuration stays in effect, and a restart would re-read the same flags")
		}
	}
	for _, f := range was {
		if !slices.Contains(now, f) {
			log.Info("manifest divergence cleared", "field", f)
		}
	}
	return slices.Clone(now)
}

func (a *Applier) publishMetrics(v commanifest.Adopted) {
	a.Rec.ManifestSetPilotsKnown(v.PilotsKnown)
	var bits int32
	if v.QuorumMet["shard_bits"] {
		bits |= 1 << 0
	}
	if v.QuorumMet["source_mode"] {
		bits |= 1 << 1
	}
	if v.QuorumMet["successor"] {
		bits |= 1 << 2
	}
	a.Rec.ManifestSetQuorumMetBits(bits)

	for _, f := range v.DivergenceFields {
		a.Rec.ManifestDivergence(f, "peer-disagree")
	}

	state := int32(0)
	window := int64(0)
	if v.Successor != nil {
		now := time.Now().Unix()
		remaining := int64(v.Successor.TransitionEpoch) - now
		window = remaining
		if remaining > 0 {
			state = 1 // bridging
		} else {
			state = 2 // cutover-pending
		}
	}
	a.Rec.ManifestSetReshardState(state)
	a.Rec.ManifestSetReshardWindowSeconds(window)
}

func (a *Applier) fireHooks(prev, next commanifest.Adopted, log *slog.Logger) {
	if a.Hooks.OnShardBitsChange != nil && prev.ShardBits != next.ShardBits {
		log.Info("ShardBits change", "prev", prev.ShardBits, "next", next.ShardBits)
		a.Rec.ManifestAdoption("shard_bits", reasonForChange(prev.ShardBits != 0))
		a.Hooks.OnShardBitsChange(prev.ShardBits, next.ShardBits)
	}
	if a.Hooks.OnSourceModeChange != nil && prev.SourceModeSSM != next.SourceModeSSM {
		log.Info("SourceMode change", "prev_ssm", prev.SourceModeSSM, "next_ssm", next.SourceModeSSM)
		a.Rec.ManifestAdoption("source_mode", reasonForChange(prev.PilotsKnown > 0))
		a.Hooks.OnSourceModeChange(prev.SourceModeSSM, next.SourceModeSSM)
	}
	if a.Hooks.OnPilotGroupsChange != nil {
		added, removed := diffUint16(prev.PilotGroups, next.PilotGroups)
		if len(added) > 0 || len(removed) > 0 {
			log.Info("PilotGroups change", "added", added, "removed", removed)
			a.Hooks.OnPilotGroupsChange(added, removed)
		}
	}
	if a.Hooks.OnSourceSetChange != nil {
		added, removed := diffAddrs(prev.SourceSet, next.SourceSet)
		if len(added) > 0 || len(removed) > 0 {
			log.Info("SourceSet change", "added", len(added), "removed", len(removed))
			a.Hooks.OnSourceSetChange(added, removed)
		}
	}
	if a.Hooks.OnSuccessorChange != nil && !reflect.DeepEqual(prev.Successor, next.Successor) {
		log.Info("Successor change", "prev", prev.Successor, "next", next.Successor)
		a.Hooks.OnSuccessorChange(prev.Successor, next.Successor)
	}
	if a.Hooks.OnDomainShardBitsChange != nil {
		for _, d := range domainShardBitsChanges(prev.Domains, next.Domains) {
			log.Info("domain ShardBits change", "domain", d.domain, "prev", d.prev, "next", d.next)
			a.Rec.ManifestAdoption("domain_shard_bits", reasonForChange(d.prev != 0))
			a.Hooks.OnDomainShardBitsChange(d.domain, d.prev, d.next)
		}
	}
}

// domainChange is one per-domain ShardBits transition.
type domainChange struct {
	domain     uint8
	prev, next uint8
}

// domainShardBitsChanges returns the per-domain ShardBits transitions between
// two settled views, in ascending DomainID order. Both hold the same planes
// (the ones the process runs), so a domain present in only one map, which
// would read as a change from or to its zero value, does not occur.
func domainShardBitsChanges(prev, next map[uint8]commanifest.DomainAdoption) []domainChange {
	var out []domainChange
	for id := uint8(0); id <= 0x0E; id++ {
		pv := prev[id].ShardBits
		nv := next[id].ShardBits
		if pv != nv {
			out = append(out, domainChange{domain: id, prev: pv, next: nv})
		}
	}
	return out
}

// reasonForChange picks an adoption-reason label for telemetry. A change
// from zero ⇒ "bootstrap"; otherwise "quorum-shift". A pinned value equal to
// the running configuration is no change at all, so it is never counted.
func reasonForChange(hadPrev bool) string {
	if hadPrev {
		return "quorum-shift"
	}
	return "bootstrap"
}

// diffUint16 returns (added, removed) computed against the sorted
// before/after slices. Both inputs MUST be sorted ascending.
func diffUint16(before, after []uint16) (added, removed []uint16) {
	mb := make(map[uint16]struct{}, len(before))
	for _, v := range before {
		mb[v] = struct{}{}
	}
	ma := make(map[uint16]struct{}, len(after))
	for _, v := range after {
		ma[v] = struct{}{}
	}
	for _, v := range after {
		if _, ok := mb[v]; !ok {
			added = append(added, v)
		}
	}
	for _, v := range before {
		if _, ok := ma[v]; !ok {
			removed = append(removed, v)
		}
	}
	sort.Slice(added, func(i, j int) bool { return added[i] < added[j] })
	sort.Slice(removed, func(i, j int) bool { return removed[i] < removed[j] })
	return
}

// diffAddrs is the netip.Addr counterpart of diffUint16.
func diffAddrs(before, after []netip.Addr) (added, removed []netip.Addr) {
	mb := make(map[netip.Addr]struct{}, len(before))
	for _, v := range before {
		mb[v] = struct{}{}
	}
	ma := make(map[netip.Addr]struct{}, len(after))
	for _, v := range after {
		ma[v] = struct{}{}
	}
	for _, v := range after {
		if _, ok := mb[v]; !ok {
			added = append(added, v)
		}
	}
	for _, v := range before {
		if _, ok := ma[v]; !ok {
			removed = append(removed, v)
		}
	}
	sort.Slice(added, func(i, j int) bool { return added[i].Less(added[j]) })
	sort.Slice(removed, func(i, j int) bool { return removed[i].Less(removed[j]) })
	return
}
