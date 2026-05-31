package txdedup_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/lightwebinc/shard-listener/txdedup"
)

// countRec is a Recorder that counts each callback for assertions.
type countRec struct {
	egLocalHit, egWon, egLost, egErr   atomic.Int64
	inSet, inExisted, inErr, inDropped atomic.Int64
}

func (r *countRec) EgressClaimLocalHit() { r.egLocalHit.Add(1) }
func (r *countRec) EgressClaimWon()      { r.egWon.Add(1) }
func (r *countRec) EgressClaimLost()     { r.egLost.Add(1) }
func (r *countRec) EgressClaimError()    { r.egErr.Add(1) }
func (r *countRec) IngressMarkSet()      { r.inSet.Add(1) }
func (r *countRec) IngressMarkExisted()  { r.inExisted.Add(1) }
func (r *countRec) IngressMarkError()    { r.inErr.Add(1) }
func (r *countRec) IngressMarkDropped()  { r.inDropped.Add(1) }

func tid(b byte) [32]byte {
	var id [32]byte
	id[0] = b
	return id
}

// eventually polls cond up to 2s; fails the test if it never holds.
func eventually(t *testing.T, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition never held: %s", msg)
}

func TestNewWithConfig_EgressTTLRequired(t *testing.T) {
	if _, err := txdedup.NewWithConfig(txdedup.Config{EgressTTL: 0}); err == nil {
		t.Error("EgressTTL=0 should error")
	}
}

func TestNewWithConfig_IngressTTLRequired(t *testing.T) {
	// Configuring ingress (via prefix) without a positive IngressTTL errors.
	_, err := txdedup.NewWithConfig(txdedup.Config{
		EgressTTL:     time.Second,
		IngressPrefix: "bsp:tx:",
		IngressTTL:    0,
	})
	if err == nil {
		t.Error("IngressTTL=0 with ingress configured should error")
	}
}

func TestNewWithConfig_DeploymentIDKeyShape(t *testing.T) {
	mr := miniredis.RunT(t)
	s, err := txdedup.NewWithConfig(txdedup.Config{
		EgressRedisAddr: mr.Addr(),
		EgressPrefix:    "bsl:egr:",
		EgressTTL:       time.Second,
		DeploymentID:    "depA",
	})
	if err != nil {
		t.Fatalf("NewWithConfig: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if got := s.EgressPrefix(); got != "bsl:egr:depA:" {
		t.Errorf("egress prefix = %q, want bsl:egr:depA:", got)
	}
	if s.HasIngressMark() {
		t.Error("ingress mark should be disabled")
	}
}

func TestClaim_DeploymentsRaceIndependently(t *testing.T) {
	mr := miniredis.RunT(t)
	mk := func(dep string) *txdedup.Store {
		s, err := txdedup.NewWithConfig(txdedup.Config{
			EgressRedisAddr: mr.Addr(),
			EgressPrefix:    "bsl:egr:",
			EgressTTL:       time.Second,
			DeploymentID:    dep,
		})
		if err != nil {
			t.Fatalf("NewWithConfig(%s): %v", dep, err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	a, b := mk("depA"), mk("depB")
	id := tid(0x10)
	// Each deployment claims independently — both win.
	if ok, _ := a.Claim(id); !ok {
		t.Error("depA should win its own namespace")
	}
	if ok, _ := b.Claim(id); !ok {
		t.Error("depB should win its own namespace")
	}
}

func TestClaim_RecorderWiring(t *testing.T) {
	mr := miniredis.RunT(t)
	rec := &countRec{}
	s, err := txdedup.NewWithConfig(txdedup.Config{
		EgressRedisAddr: mr.Addr(),
		EgressPrefix:    "bsl:egr:",
		EgressTTL:       time.Second,
		DeploymentID:    "dep",
		Recorder:        rec,
	})
	if err != nil {
		t.Fatalf("NewWithConfig: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	id := tid(0x20)
	if ok, _ := s.Claim(id); !ok {
		t.Fatal("first claim should win")
	}
	// Second claim in the same process hits the local LRU tier.
	if ok, _ := s.Claim(id); ok {
		t.Fatal("second claim should be suppressed")
	}
	if rec.egWon.Load() != 1 {
		t.Errorf("EgressClaimWon = %d, want 1", rec.egWon.Load())
	}
	if rec.egLocalHit.Load() < 1 {
		t.Errorf("EgressClaimLocalHit = %d, want >= 1", rec.egLocalHit.Load())
	}
}

func TestMark_SetsIngressNamespace(t *testing.T) {
	mr := miniredis.RunT(t)
	rec := &countRec{}
	s, err := txdedup.NewWithConfig(txdedup.Config{
		EgressRedisAddr:  mr.Addr(),
		EgressTTL:        time.Second,
		IngressRedisAddr: mr.Addr(),
		IngressPrefix:    "bsp:tx:",
		IngressTTL:       time.Second,
		Recorder:         rec,
	})
	if err != nil {
		t.Fatalf("NewWithConfig: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if !s.HasIngressMark() {
		t.Fatal("ingress mark should be enabled")
	}
	if got := s.IngressPrefix(); got != "bsp:tx:" {
		t.Errorf("ingress prefix = %q", got)
	}

	s.Mark(tid(0x30))
	// Mark is async; the SETNX lands shortly after.
	eventually(t, "ingress MarkSet recorded", func() bool {
		return rec.inSet.Load() == 1
	})
}

func TestMark_LocalOnlyDrops(t *testing.T) {
	mr := miniredis.RunT(t)
	rec := &countRec{}
	// Ingress configured by prefix only (no Redis addr) → local-only, Mark
	// has nowhere to SETNX and reports a drop.
	s, err := txdedup.NewWithConfig(txdedup.Config{
		EgressRedisAddr: mr.Addr(),
		EgressTTL:       time.Second,
		IngressPrefix:   "bsp:tx:",
		IngressTTL:      time.Second,
		Recorder:        rec,
	})
	if err != nil {
		t.Fatalf("NewWithConfig: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	s.Mark(tid(0x31))
	eventually(t, "local-only Mark drop recorded", func() bool {
		return rec.inDropped.Load() == 1
	})
}

func TestLocalOnlyClaim(t *testing.T) {
	// No Redis address: egress dedup runs on the tier-1 LRU alone.
	s, err := txdedup.NewWithConfig(txdedup.Config{
		EgressTTL:      time.Second,
		EgressLocalCap: 1024,
	})
	if err != nil {
		t.Fatalf("NewWithConfig: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	id := tid(0x40)
	if ok, err := s.Claim(id); err != nil || !ok {
		t.Fatalf("first local claim: (%v, %v)", ok, err)
	}
	if ok, err := s.Claim(id); err != nil || ok {
		t.Fatalf("second local claim should suppress: (%v, %v)", ok, err)
	}
}

func TestNilStoreMethods(t *testing.T) {
	var s *txdedup.Store
	if ok, err := s.Claim(tid(0x50)); !ok || err != nil {
		t.Errorf("nil Claim = (%v,%v), want (true,nil)", ok, err)
	}
	s.Mark(tid(0x51)) // must not panic
	if s.HasIngressMark() {
		t.Error("nil HasIngressMark should be false")
	}
	if s.EgressPrefix() != "" || s.IngressPrefix() != "" {
		t.Error("nil prefixes should be empty")
	}
	if err := s.Close(); err != nil {
		t.Errorf("nil Close = %v", err)
	}
}
