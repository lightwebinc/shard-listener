package nack_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lightwebinc/bitcoin-shard-listener/discovery"
	"github.com/lightwebinc/bitcoin-shard-listener/nack"
)

// mockEndpoint is a small UDP server that replies to every NACK with a fixed
// MsgType. If respond is MsgTypeMISS for the first `missThen` requests and
// MsgTypeACK afterwards, the endpoint simulates a deep-tier cache that
// eventually warms up.
type mockEndpoint struct {
	conn      net.PacketConn
	addr      *net.UDPAddr
	count     atomic.Int64
	missCount int64 // when count <= missCount, respond MISS; else ACK
}

func newMockEndpoint(t *testing.T, missCount int64) *mockEndpoint {
	t.Helper()
	c, err := net.ListenPacket("udp", "[::1]:0")
	if err != nil {
		t.Skipf("udp loopback unavailable: %v", err)
	}
	m := &mockEndpoint{
		conn:      c,
		addr:      c.LocalAddr().(*net.UDPAddr),
		missCount: missCount,
	}
	go m.run()
	t.Cleanup(func() { _ = c.Close() })
	return m
}

func (m *mockEndpoint) run() {
	buf := make([]byte, 256)
	for {
		_, src, err := m.conn.ReadFrom(buf)
		if err != nil {
			return
		}
		n := m.count.Add(1)
		resp := nack.MsgTypeMISS
		var curSeq uint64
		if n > m.missCount {
			resp = nack.MsgTypeACK
			curSeq = 200
		}
		var out [nack.ResponseSize]byte
		nack.EncodeResponse(&nack.Response{MsgType: resp, Flags: 0x01, CurSeq: curSeq}, out[:])
		_, _ = m.conn.WriteTo(out[:], src)
	}
}

// upsertEndpoint registers a beacon-discovered endpoint with the registry at
// the given tier/preference, pointing at the loopback mock UDP listener.
func upsertEndpoint(t *testing.T, r *discovery.Registry, ep *mockEndpoint, tier, pref uint8, id uint32) {
	t.Helper()
	r.Upsert(&discovery.ADVERT{
		NACKAddr:       net.ParseIP("::1"),
		NACKPort:       uint16(ep.addr.Port),
		Tier:           tier,
		Preference:     pref,
		BeaconInterval: 60,
		InstanceID:     id,
	})
}

// TestTierEscalation_RecoversAfterDeepTierMiss is the regression test for the
// scenario where retry1 (T0/P128) and retry2 (T0/P64) have cold caches and
// retry3 (T1/P128) MISSes on the first attempt (cache not yet warm) but ACKs
// on a later attempt. The gap MUST be recovered: the tracker must not cycle
// back to retry1/retry2 — it must stay on retry3 with backoff and retry it.
func TestTierEscalation_RecoversAfterDeepTierMiss(t *testing.T) {
	r1 := newMockEndpoint(t, 1<<30) // always MISS
	r2 := newMockEndpoint(t, 1<<30) // always MISS
	r3 := newMockEndpoint(t, 1)     // MISS once, then ACK

	reg := discovery.NewRegistry()
	upsertEndpoint(t, reg, r1, 0, 128, 1) // T0/P128
	upsertEndpoint(t, reg, r2, 0, 64, 2)  // T0/P64
	upsertEndpoint(t, reg, r3, 1, 128, 3) // T1/P128

	cfg := nack.TrackerConfig{
		JitterMax:  0,
		BackoffMax: 300 * time.Millisecond, // keep test fast
		MaxRetries: 5,
		GapTTL:     10 * time.Second,
	}
	tr := nack.New(cfg, nil, nil, nil, reg)

	tr.Observe(0, 0, 100, [32]byte{})
	tr.Observe(0, 200, 300, [32]byte{}) // gap, key=200
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("setup: PendingGaps = %d, want 1", g)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	got := pollGaps(tr, 0, 5*time.Second)
	if got != 0 {
		t.Errorf("after escalation: PendingGaps = %d, want 0 (gap should have recovered after retry3 cache warmed)", got)
	}

	// Sanity: retry3 should have been hit at least twice (initial MISS + retry).
	if hits := r3.count.Load(); hits < 2 {
		t.Errorf("retry3 hit count = %d, want ≥ 2 (initial MISS then re-attempt)", hits)
	}
}

// TestTierEscalation_LowerTiersNotRetriedAfterMiss ensures we do not cycle
// back to retry1/retry2 once we've reached retry3. Lower-tier endpoints
// should each be hit at most once (one MISS each) before retry3 takes over.
func TestTierEscalation_LowerTiersNotRetriedAfterMiss(t *testing.T) {
	r1 := newMockEndpoint(t, 1<<30) // always MISS
	r2 := newMockEndpoint(t, 1<<30) // always MISS
	r3 := newMockEndpoint(t, 4)     // MISS for first 2 attempts (×2 datagrams), then ACK

	reg := discovery.NewRegistry()
	upsertEndpoint(t, reg, r1, 0, 128, 1)
	upsertEndpoint(t, reg, r2, 0, 64, 2)
	upsertEndpoint(t, reg, r3, 1, 128, 3)

	cfg := nack.TrackerConfig{
		JitterMax:  0,
		BackoffMax: 200 * time.Millisecond,
		MaxRetries: 6,
		GapTTL:     10 * time.Second,
	}
	tr := nack.New(cfg, nil, nil, nil, reg)

	tr.Observe(0, 0, 100, [32]byte{})
	tr.Observe(0, 200, 300, [32]byte{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	if got := pollGaps(tr, 0, 5*time.Second); got != 0 {
		t.Fatalf("gap not recovered: PendingGaps = %d", got)
	}

	// Each sendNACK attempt sends 2 datagrams (fwd+bwd) when leftBoundary != 0.
	// retry1 and retry2 must be visited exactly once each → at most 2 datagrams.
	if c := r1.count.Load(); c > 2 {
		t.Errorf("retry1 hit %d times, want ≤ 2 (must not re-cycle through lower tier)", c)
	}
	if c := r2.count.Load(); c > 2 {
		t.Errorf("retry2 hit %d times, want ≤ 2 (must not re-cycle through lower tier)", c)
	}
	// retry3 must be hit at least 3 times (the third attempt finally ACKs after
	// two MISSes, accounting for 2 datagrams per attempt × 3 attempts).
	if c := r3.count.Load(); c < 3 {
		t.Errorf("retry3 hit %d times, want ≥ 3", c)
	}
}
