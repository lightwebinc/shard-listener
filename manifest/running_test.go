package manifest

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
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

// startApplier runs a until the returned stop is called; stop returns the
// applier's log.
func startApplier(a *Applier) (stop func() string) {
	var logs bytes.Buffer
	a.Log = slog.New(slog.NewTextHandler(&logs, nil))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		a.Run(ctx)
		close(done)
	}()
	return func() string {
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

	stop := startApplier(a)
	time.Sleep(150 * time.Millisecond)
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

	stop := startApplier(a)
	time.Sleep(100 * time.Millisecond) // several ticks of divergence
	reg.Upsert(mustAddr("2001:db8::1"), announcerManifest(1, 2, false))
	reg.Upsert(mustAddr("2001:db8::2"), announcerManifest(2, 2, false))
	time.Sleep(100 * time.Millisecond) // agreement
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
