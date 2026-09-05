package nack_test

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lightwebinc/shard-listener/metrics"
	"github.com/lightwebinc/shard-listener/nack"
)

// A gap given up on (here: TTL) is booked unrecovered WITH its reason, and a
// frame that then arrives for it — a multicast retransmit that outran the
// NACK deadline — is booked as a late fill, so unrecovered minus late-filled
// is the flow's real loss. Measured live before this existed: 32 unrecovered
// on a flow whose consumer received every object.
func TestAbandonedGap_LateFillIsBooked(t *testing.T) {
	rec, err := metrics.New("late-fill-test", 1, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		rec.Shutdown(ctx)
	}()
	cfg := nack.TrackerConfig{
		JitterMax:  0,
		BackoffMax: 5 * time.Second,
		MaxRetries: 3,
		GapTTL:     50 * time.Millisecond, // the gap's deadline; the sweep evicts it
		TailTTL:    10 * time.Second,
	}
	tr := nack.New(cfg, nil, nil, rec, nil)
	src := []byte{0xfd, 0, 0, 0x50, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 1}
	tr.Observe(0, [32]byte{}, flowA, 1, [32]byte{}, src)
	tr.Observe(0, [32]byte{}, flowA, 3, [32]byte{}, src) // gap at 2
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("PendingGaps = %d, want 1", g)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for tr.PendingGaps() != 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if g := tr.PendingGaps(); g != 0 {
		t.Fatalf("gap not evicted by TTL: PendingGaps = %d", g)
	}
	// The retransmit arrives after all.
	tr.Observe(0, [32]byte{}, flowA, 2, [32]byte{}, nil)
	// And a frame that was never a gap is not a late fill.
	tr.Observe(0, [32]byte{}, flowA, 4, [32]byte{}, src)

	srv := httptest.NewServer(rec.PromHandler())
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	out := string(body)
	for _, want := range []string{
		`bsl_gaps_unrecovered_total{flow="brc124",otel_scope_name="shard-listener",otel_scope_schema_url="",otel_scope_version="",source="fd00:50:0:6::1"} 1`,
		`bsl_gaps_abandoned_total{flow="brc124",otel_scope_name="shard-listener",otel_scope_schema_url="",otel_scope_version="",reason="ttl",source="fd00:50:0:6::1"} 1`,
		`bsl_gaps_late_filled_total{flow="brc124",otel_scope_name="shard-listener",otel_scope_schema_url="",otel_scope_version="",source="fd00:50:0:6::1"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q", want)
		}
	}
	if strings.Count(out, "bsl_gaps_late_filled_total{") != 1 {
		t.Errorf("late fill booked for a frame that was never a gap:\n%s", out)
	}
}
