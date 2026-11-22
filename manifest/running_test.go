package manifest

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	commanifest "github.com/lightwebinc/shard-common/manifest"
	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/shard-listener/metrics"
)

func newRecorder(t *testing.T) *metrics.Recorder {
	t.Helper()
	rec, err := metrics.New("manifest-test", 1, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		rec.Shutdown(ctx)
	})
	return rec
}

// counter sums the samples of the named series whose line holds every label
// substring; an absent series reads 0.
func counter(t *testing.T, rec *metrics.Recorder, name string, labels ...string) float64 {
	t.Helper()
	srv := httptest.NewServer(rec.PromHandler())
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	var sum float64
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, name+"{") {
			continue
		}
		match := true
		for _, l := range labels {
			match = match && strings.Contains(line, l)
		}
		if f := strings.Fields(line); match && len(f) > 0 {
			if v, err := strconv.ParseFloat(f[len(f)-1], 64); err == nil {
				sum += v
			}
		}
	}
	return sum
}

// announcerManifest is an authoritative pilot manifest for shardBits that also
// declares SSM when ssm is set.
func announcerManifest(instanceID uint32, shardBits uint8, ssm bool) *frame.ShardManifest {
	m := authoritativePilotManifest(instanceID, shardBits, []uint16{0})
	if ssm {
		m.Flags |= frame.ShardManifestFlagSourceModeSSM
	}
	return m
}

// valueHookCalls counts the value-hook firings of an applier.
type valueHookCalls struct {
	mu                                sync.Mutex
	shardBits, sourceMode, domainBits int
}

func (c *valueHookCalls) hooks() Hooks {
	return Hooks{
		OnShardBitsChange:       func(uint8, uint8) { c.mu.Lock(); c.shardBits++; c.mu.Unlock() },
		OnSourceModeChange:      func(bool, bool) { c.mu.Lock(); c.sourceMode++; c.mu.Unlock() },
		OnDomainShardBitsChange: func(uint8, uint8, uint8) { c.mu.Lock(); c.domainBits++; c.mu.Unlock() },
	}
}

// evalWatch records the applier's evaluations (through Applier.evaluated), so
// a test waits for evaluation progress, bounded, instead of sleeping.
type evalWatch struct {
	mu        sync.Mutex
	n         int    // evaluations so far
	pilots    []int  // PilotsKnown, per evaluation
	diverging []bool // shard_bits reported diverging, per evaluation
}

func (w *evalWatch) observe(v commanifest.Adopted) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.n++
	w.pilots = append(w.pilots, v.PilotsKnown)
	w.diverging = append(w.diverging, slices.Contains(v.DivergenceFields, "shard_bits"))
}

// count is how many evaluations so far satisfy f.
func (w *evalWatch) count(f func(i int) bool) int {
	n := 0
	for i := range w.n {
		if f(i) {
			n++
		}
	}
	return n
}

// waitFor polls until cond holds for the evaluations so far, under the
// watch's lock, and fails the test if it does not within 10 s.
func (w *evalWatch) waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		w.mu.Lock()
		ok, n := cond(), w.n
		w.mu.Unlock()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s (%d evaluations so far)", what, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// startApplier runs a until the returned stop is called, watching its
// evaluations; stop returns the applier's log.
func startApplier(a *Applier) (w *evalWatch, stop func() string) {
	var logs bytes.Buffer
	a.Log = slog.New(slog.NewTextHandler(&logs, nil))
	w = &evalWatch{}
	a.evaluated = w.observe
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		a.Run(ctx)
		close(done)
	}()
	return w, func() string {
		cancel()
		<-done
		return logs.String()
	}
}

// A manifest that agrees with the running configuration is no transition:
// no value hook fires, nothing is counted as adopted, and nothing is logged
// as an adoption, a divergence or a restart. A zero baseline instead read the
// pinned values as adopted at every start.
func TestApplier_StableManifestEqualToRunningFiresNothing(t *testing.T) {
	running := Running{ShardBits: 2, SourceModeSSM: true, DomainShardBits: map[uint8]uint8{shard.DomainBEEF: 0}}
	reg := commanifest.NewRegistry(60 * time.Second)
	reg.Upsert(mustAddr("2001:db8::1"), announcerManifest(1, 2, true))
	reg.Upsert(mustAddr("2001:db8::2"), announcerManifest(2, 2, true))
	ev := commanifest.NewEvaluator(commanifest.EvaluatorConfig{Quorum: 2, Hysteresis: time.Nanosecond, Pin: running.Pin()})
	rec := newRecorder(t)
	calls := &valueHookCalls{}
	a := &Applier{Registry: reg, Evaluator: ev, Rec: rec, Running: running, Interval: 10 * time.Millisecond, Hooks: calls.hooks()}

	w, stop := startApplier(a)
	// Several evaluations, each of both announcers, before asserting that
	// nothing happened: zero effects with no evaluation would prove nothing.
	w.waitFor(t, "three evaluations of both announcers", func() bool {
		return w.count(func(i int) bool { return w.pilots[i] == 2 }) >= 3
	})
	logs := stop()

	calls.mu.Lock()
	defer calls.mu.Unlock()
	if calls.shardBits+calls.sourceMode+calls.domainBits != 0 {
		t.Errorf("value hooks fired for a manifest equal to the running config: shard_bits=%d source_mode=%d domain=%d",
			calls.shardBits, calls.sourceMode, calls.domainBits)
	}
	if v := counter(t, rec, "multicast_manifest_adoption_total"); v != 0 {
		t.Errorf("multicast_manifest_adoption_total = %v, want 0", v)
	}
	if v := counter(t, rec, "multicast_manifest_divergence_total"); v != 0 {
		t.Errorf("multicast_manifest_divergence_total = %v, want 0", v)
	}
	for _, word := range []string{"change", "divergence", "restart"} {
		if strings.Contains(logs, word) {
			t.Errorf("applier log mentions %q for a stable manifest:\n%s", word, logs)
		}
	}
}

// Announcers that disagree with the running configuration are divergence:
// counted on every tick, logged once when it starts and once when it clears,
// never adopted.
func TestApplier_DivergenceLoggedOnceAndCleared(t *testing.T) {
	running := Running{ShardBits: 2}
	reg := commanifest.NewRegistry(60 * time.Second)
	reg.Upsert(mustAddr("2001:db8::1"), announcerManifest(1, 3, false))
	reg.Upsert(mustAddr("2001:db8::2"), announcerManifest(2, 3, false))
	ev := commanifest.NewEvaluator(commanifest.EvaluatorConfig{Quorum: 2, Hysteresis: time.Nanosecond, Pin: running.Pin()})
	rec := newRecorder(t)
	calls := &valueHookCalls{}
	a := &Applier{Registry: reg, Evaluator: ev, Rec: rec, Running: running, Interval: 10 * time.Millisecond, Hooks: calls.hooks()}

	w, stop := startApplier(a)
	// The divergence is evaluated, on more than one tick, before the
	// announcers change.
	w.waitFor(t, "two evaluations reporting the shard_bits divergence", func() bool {
		return w.count(func(i int) bool { return w.diverging[i] }) >= 2
	})
	reg.Upsert(mustAddr("2001:db8::1"), announcerManifest(1, 2, false))
	reg.Upsert(mustAddr("2001:db8::2"), announcerManifest(2, 2, false))
	w.mu.Lock()
	switched := w.n
	w.mu.Unlock()
	w.waitFor(t, "an evaluation after the change with the divergence cleared", func() bool {
		return w.n > switched && !w.diverging[w.n-1]
	})
	logs := stop()

	if n := strings.Count(logs, "manifest divergence:"); n != 1 {
		t.Errorf("divergence warnings = %d, want 1 (once when it starts):\n%s", n, logs)
	}
	if !strings.Contains(logs, "field=shard_bits") {
		t.Errorf("the divergence warning does not name the field:\n%s", logs)
	}
	if n := strings.Count(logs, "manifest divergence cleared"); n != 1 {
		t.Errorf("divergence-cleared lines = %d, want 1:\n%s", n, logs)
	}
	if v := counter(t, rec, "multicast_manifest_divergence_total", `field="shard_bits"`); v < 2 {
		t.Errorf("divergence counted %v times, want one per tick while it lasted (>= 2)", v)
	}
	calls.mu.Lock()
	defer calls.mu.Unlock()
	if calls.shardBits != 0 {
		t.Errorf("OnShardBitsChange fired %d times; a pinned ShardBits is never adopted", calls.shardBits)
	}
}
