// Package manifest hosts the listener-side applier that consumes the
// BRC-139 manifest evaluator's adopted view and translates it into
// concrete actions on the listener's join state and metrics.
//
// The applier runs on a 1-second tick. It is deliberately simple: every
// tick it evicts expired registry entries, runs the evaluator, and
// publishes the new view to whichever pluggable hooks the caller
// supplied. Restart-on-adopt vs. live-resharding is the caller's
// concern; this package only provides change notifications.
package manifest

import (
	"context"
	"log/slog"
	"net/netip"
	"reflect"
	"sort"
	"time"

	commanifest "github.com/lightwebinc/shard-common/manifest"

	"github.com/lightwebinc/shard-listener/metrics"
)

// Hooks are pluggable change handlers fired by [Applier]. Any field can
// be nil; nil means "ignore this kind of change."
type Hooks struct {
	// OnShardBitsChange fires when the evaluator's adopted ShardBits
	// differs from the previously-published value. The caller decides
	// restart-vs-bridge based on its own policy.
	OnShardBitsChange func(prev, next uint8)

	// OnSourceModeChange fires on a SourceModeSSM transition.
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
}

// Applier is the periodic evaluator+notifier. Field zero values are
// safe; callers should set Registry, Evaluator, and at least one Hook
// before calling [Applier.Run].
type Applier struct {
	Registry  *commanifest.Registry
	Evaluator *commanifest.Evaluator
	Hooks     Hooks
	Rec       *metrics.Recorder
	Log       *slog.Logger
	Interval  time.Duration // default 1s
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

	var prev commanifest.Adopted
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
		a.fireHooks(prev, next, log)
		prev = next
	}
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
}

// reasonForChange picks an adoption-reason label for telemetry. A change
// from zero ⇒ "bootstrap"; otherwise "quorum-shift". Pin-driven changes
// are handled by the evaluator (Pin entry is the value reported), so the
// applier sees those as a one-shot bootstrap on first observation.
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
