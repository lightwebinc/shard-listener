package nack

import (
	"context"
	"io"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lightwebinc/shard-listener/metrics"
)

// sumSeries sums every sample of the named series in a Prometheus exposition.
func sumSeries(text, name string) float64 {
	var sum float64
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, name+"{") {
			continue
		}
		f := strings.Fields(line)
		if v, err := strconv.ParseFloat(f[len(f)-1], 64); err == nil {
			sum += v
		}
	}
	return sum
}

// A tail probe asks about a SeqNum that may never have been emitted. An ACK
// answers that question: a cache holds the frame, so the flow did lose it. If
// the repair the ACK promised never arrives, that is a real loss and must be
// booked once as detected and unrecovered, not closed as a recovery (the copy
// may be lost on the same path) and not retired as an idle flow's miss. The
// same SeqNum is not probed again once it has been given up on.
func TestTailProbeConfirmedByACK_UnrepairedIsBookedOnce(t *testing.T) {
	rec, err := metrics.New("probe-confirm-test", 1, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		rec.Shutdown(ctx)
	}()
	fr := newFakeRetry(t, true) // bare ACK, and no data ever arrives
	tr := New(probeCfg(), []string{fr.addr()}, nil, rec, nil)
	tr.Start(t.Context())
	settleFlow(tr)

	// Long enough for the probe, its retries (MaxRetries 2, backoff <= 50 ms),
	// the eviction, and several more sweeps that must not probe it again.
	time.Sleep(1500 * time.Millisecond)

	srv := httptest.NewServer(rec.PromHandler())
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	out := string(body)

	if v := sumSeries(out, "bsl_gaps_suppressed_total"); v != 0 {
		t.Errorf("gaps suppressed = %v, want 0: the promised repair never arrived", v)
	}
	if v := sumSeries(out, "bsl_tail_probes_recovered_total"); v != 0 {
		t.Errorf("tail probes recovered = %v, want 0", v)
	}
	if v := sumSeries(out, "bsl_gaps_detected_total"); v != 1 {
		t.Errorf("gaps detected = %v, want 1 (the ACK confirmed the loss)", v)
	}
	if v := sumSeries(out, "bsl_gaps_unrecovered_total"); v != 1 {
		t.Errorf("gaps unrecovered = %v, want 1", v)
	}
	tr.mu.Lock()
	misses := 0
	if fs, ok := tr.flows[42]; ok {
		misses = fs.probeMisses
	}
	tr.mu.Unlock()
	if misses != 0 {
		t.Errorf("probeMisses = %d, want 0: a confirmed loss is not an idle flow", misses)
	}
}
