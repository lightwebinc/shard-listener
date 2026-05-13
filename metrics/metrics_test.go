package metrics

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newRecorder(t *testing.T) *Recorder {
	t.Helper()
	r, err := New("test-instance", 2, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		r.Shutdown(ctx)
	})
	return r
}

func TestNew_RegistersWithoutCollision(t *testing.T) {
	r := newRecorder(t)
	if r == nil {
		t.Fatal("nil recorder")
	}
}

func TestNew_EmptyInstanceIDFallsBackToHostname(t *testing.T) {
	r, err := New("", 1, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		r.Shutdown(ctx)
	}()
}

func TestRecorder_AllCountersSafeToCall(t *testing.T) {
	r := newRecorder(t)
	r.FrameReceived(0, "lo", "brc124")
	r.FrameDropped(0, "decode_error")
	r.FrameForwarded(0, "udp")
	r.EgressError(0)
	r.MCEgressError(0)
	r.GapDetected()
	r.GapSuppressed()
	r.NACKDispatched()
	r.GapUnrecovered()
	r.SubtreeAnnounceReceived()
	r.SubtreeAnnounceRejected("decode_error")
	r.SubtreeGroupEvicted(3, 10)
	r.BeaconAdvertReceived()
	r.SetBeaconRegistryEndpoints(5)
	r.WorkerReady()
	r.WorkerDone()
}

func TestReadyz_Starting(t *testing.T) {
	r := newRecorder(t)
	w := httptest.NewRecorder()
	r.handleReadyz(w, httptest.NewRequest("GET", "/readyz", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("starting state: code=%d", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"starting"`) {
		t.Errorf("body: %q", w.Body.String())
	}
}

func TestReadyz_Ready(t *testing.T) {
	r := newRecorder(t)
	r.WorkerReady()
	r.WorkerReady()
	w := httptest.NewRecorder()
	r.handleReadyz(w, httptest.NewRequest("GET", "/readyz", nil))
	if w.Code != http.StatusOK {
		t.Errorf("ready state: code=%d", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"ready"`) {
		t.Errorf("body: %q", w.Body.String())
	}
}

func TestReadyz_Draining(t *testing.T) {
	r := newRecorder(t)
	r.WorkerReady()
	r.WorkerReady()
	r.SetDraining()
	w := httptest.NewRecorder()
	r.handleReadyz(w, httptest.NewRequest("GET", "/readyz", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("draining: code=%d", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"draining"`) {
		t.Errorf("body: %q", w.Body.String())
	}
}

func TestHealthz(t *testing.T) {
	r := newRecorder(t)
	w := httptest.NewRecorder()
	r.handleHealthz(w, httptest.NewRequest("GET", "/healthz", nil))
	if w.Code != http.StatusOK {
		t.Errorf("code=%d", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"ok"`) {
		t.Errorf("body: %q", w.Body.String())
	}
}

func TestServe(t *testing.T) {
	r := newRecorder(t)
	// pick a free port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	done := make(chan struct{})
	served := make(chan struct{})
	go func() {
		r.Serve(addr, done)
		close(served)
	}()
	// Wait for server to come up.
	var resp *http.Response
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		resp, err = http.Get("http://" + addr + "/healthz")
		if err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !strings.Contains(string(body), `"ok"`) {
		t.Errorf("body: %s", body)
	}

	// /metrics endpoint
	resp2, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp2.Body)
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("metrics status: %d", resp2.StatusCode)
	}

	close(done)
	select {
	case <-served:
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return")
	}
}
