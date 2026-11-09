package nack_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/shard-listener/metrics"
	"github.com/lightwebinc/shard-listener/nack"
)

// Emitter addresses for these tests (documentation prefix, RFC 3849).
var (
	emitterA = net.ParseIP("2001:db8::27")
	emitterB = net.ParseIP("2001:db8::28")
)

// observeRun feeds flowA the SeqNums [from, to] from src, sleeping every
// between frames. every = 0 is the cadence a backlogged receiver processes a
// receive batch at: back to back, whatever the wire rate was.
func observeRun(tr *nack.Tracker, src net.IP, from, to uint64, every time.Duration) {
	for s := from; s <= to; s++ {
		tr.Observe(0, [32]byte{}, flowA, s, [32]byte{}, src)
		if every > 0 {
			time.Sleep(every)
		}
	}
}

// promText renders rec's Prometheus exposition.
func promText(t *testing.T, rec *metrics.Recorder) string {
	t.Helper()
	srv := httptest.NewServer(rec.PromHandler())
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

// counter sums every sample of the named series whose line holds all of the
// label substrings; an absent series reads 0.
func counter(text, name string, labels ...string) float64 {
	var sum float64
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, name+"{") {
			continue
		}
		ok := true
		for _, l := range labels {
			if !strings.Contains(line, l) {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		f := strings.Fields(line)
		v, err := strconv.ParseFloat(f[len(f)-1], 64)
		if err == nil {
			sum += v
		}
	}
	return sum
}

func newRecorder(t *testing.T) *metrics.Recorder {
	t.Helper()
	rec, err := metrics.New("burst-test", 1, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		rec.Shutdown(ctx)
	})
	return rec
}

// TestBurstSameSender_EveryGapRegistered: a burst of consecutive SeqNums lost
// from the SAME emitter is loss, at any burst size within MaxForwardJump and
// at any cadence the receiver happens to process frames at. The case that
// matters is the backlogged receiver: a NIC ring or socket overflows because
// the receiver is behind, so the frames on either side of the lost burst are
// processed back to back and the elapsed time across the jump reads as
// nothing. The burst must be registered in full, never read as an emitter
// change.
func TestBurstSameSender_EveryGapRegistered(t *testing.T) {
	cadences := []struct {
		name    string
		every   time.Duration // between frames before the burst
		silence time.Duration // extra quiet before the first frame after it
	}{
		{"backlogged", 0, 0},
		{"settled-1ms-no-silence", time.Millisecond, 0},
		{"settled-1ms-20ms-silence", time.Millisecond, 20 * time.Millisecond},
	}
	for _, size := range []uint64{65, 500, 1435, 4096} {
		for _, c := range cadences {
			t.Run(fmt.Sprintf("%d/%s", size, c.name), func(t *testing.T) {
				tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second}, nil, nil, nil, nil)
				const run = 40 // settles the rate estimate (>= 16 in-order frames)
				observeRun(tr, emitterA, 1, run, c.every)
				time.Sleep(c.silence)
				observeRun(tr, emitterA, run+size+1, run+size+3, 0)
				if g := tr.PendingGaps(); uint64(g) != size {
					t.Fatalf("PendingGaps = %d, want %d: a same-emitter burst within MaxForwardJump must be registered in full", g, size)
				}
			})
		}
	}
}

// TestBurstSameSender_EveryGapNACKedAndRecovered drives a 1,435-frame burst
// through the dispatch path against a retry endpoint that answers every NACK
// the way the deployed posture does (the frame on the NACK socket, then a
// unicast-flagged ACK). Every lost SeqNum must be NACKed and recovered, with
// nothing booked unrecovered and no re-baseline.
func TestBurstSameSender_EveryGapNACKedAndRecovered(t *testing.T) {
	const size = 1435
	retry := newFrameRetry(t, 0x02, true)
	rec := newRecorder(t)
	cfg := nack.TrackerConfig{
		JitterMax:   5 * time.Millisecond,
		BackoffBase: 50 * time.Millisecond,
		BackoffMax:  200 * time.Millisecond,
		MaxRetries:  5,
		GapTTL:      10 * time.Second,
	}
	tr := nack.New(cfg, []string{retry.addr()}, nil, rec, nil)
	var recovered sync.Map
	tr.SetRecoverFunc(func(raw []byte) bool {
		hk := binary.BigEndian.Uint64(raw[40:48])
		seq := binary.BigEndian.Uint64(raw[48:56])
		recovered.Store(seq, true)
		// What Worker.Reinject does: the frame goes back through Observe with
		// no live source.
		tr.Observe(0, [32]byte{}, hk, seq, [32]byte{}, nil)
		return true
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	observeRun(tr, emitterA, 1, 256, 0)
	observeRun(tr, emitterA, 256+size+1, 256+size+64, 0)

	if got := pollGaps(tr, 0, 10*time.Second); got != 0 {
		t.Fatalf("PendingGaps = %d after recovery, want 0", got)
	}
	for s := uint64(257); s <= 256+size; s++ {
		if !retry.asked(s) {
			t.Fatalf("SeqNum %d was never NACKed (%d of %d asked)", s, retry.distinct(), size)
		}
		if _, ok := recovered.Load(s); !ok {
			t.Fatalf("SeqNum %d was never recovered", s)
		}
	}
	out := promText(t, rec)
	if v := counter(out, "bsl_gaps_detected_total"); v != size {
		t.Errorf("gaps detected = %v, want %d", v, size)
	}
	if v := counter(out, "bsl_gaps_suppressed_total"); v != size {
		t.Errorf("gaps suppressed (filled) = %v, want %d", v, size)
	}
	if v := counter(out, "bsl_gaps_unrecovered_total"); v != 0 {
		t.Errorf("gaps unrecovered = %v, want 0", v)
	}
	if v := counter(out, "bsl_seq_rebaselines_total"); v != 0 {
		t.Errorf("re-baselines = %v, want 0: the burst came from the same emitter", v)
	}
	if v := counter(out, "bsl_nacks_dispatched_total"); v < size {
		t.Errorf("NACKs dispatched = %v, want >= %d", v, size)
	}
}

// TestInterleavedLoss_LateFramesFillAndRealLossRegistered: a later block of the
// flow arrives before an earlier one (two dispatchers stamping one flow, or two
// receive queues). The jump registers the range, the late block fills it
// without a NACK, and a frame lost from inside the late block stays a gap.
func TestInterleavedLoss_LateFramesFillAndRealLossRegistered(t *testing.T) {
	tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second}, nil, nil, nil, nil)
	observeRun(tr, emitterA, 1, 100, 0)
	observeRun(tr, emitterA, 301, 400, 0) // the later block first
	observeRun(tr, emitterA, 101, 149, 0) // then the earlier one, 150 lost
	observeRun(tr, emitterA, 151, 300, 0)
	if g := tr.PendingGaps(); g != 1 {
		t.Fatalf("PendingGaps = %d, want 1 (only the frame lost inside the late block)", g)
	}
	if !tr.Pending(flowA, []uint64{150}) {
		t.Fatal("SeqNum 150 is not pending: the loss would go unrepaired")
	}
}

// TestRebaseline_KeepsEarlierGaps: a jump beyond MaxForwardJump re-baselines,
// but gaps registered before it stay pending. They are frames the previous
// emitter numbered and the retry caches hold under the same HashKey and SeqNum,
// so they are repaired or booked unrecovered like any other gap, never dropped
// without a NACK or a count.
func TestRebaseline_KeepsEarlierGaps(t *testing.T) {
	tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second, MaxForwardJump: 1000}, nil, nil, nil, nil)
	observeRun(tr, emitterA, 1, 2, 0)
	observeRun(tr, emitterA, 5, 5, 0) // gaps 3, 4
	observeRun(tr, emitterA, 5005, 5005, 0)
	if g := tr.PendingGaps(); g != 2 {
		t.Fatalf("PendingGaps = %d, want 2: the gaps before the re-baseline must survive it", g)
	}
	if !tr.Pending(flowA, []uint64{3}) || !tr.Pending(flowA, []uint64{4}) {
		t.Fatal("gaps 3 and 4 dropped by the re-baseline")
	}
}

// TestSenderChange_StillRebaselines: a genuine emitter change (anycast
// failover to a proxy whose counter for this HashKey is far away) must still
// re-baseline: no phantom range is registered, the re-baseline is counted,
// tracking continues from the new emitter's counter, and a real gap in the old
// emitter's stream is still repaired.
func TestSenderChange_StillRebaselines(t *testing.T) {
	rec := newRecorder(t)
	tr := nack.New(nack.TrackerConfig{GapTTL: 10 * time.Second}, nil, nil, rec, nil)
	observeRun(tr, emitterA, 1, 29, time.Millisecond)
	observeRun(tr, emitterA, 31, 60, time.Millisecond) // a real gap at 30
	const next = 9_000_000
	observeRun(tr, emitterB, next, next, 0)
	g := tr.PendingGaps()
	if g >= 100 {
		t.Fatalf("PendingGaps = %d: the divergent range was registered as phantom gaps", g)
	}
	if !tr.Pending(flowA, []uint64{30}) {
		t.Fatal("the old emitter's real gap at 30 was dropped by the re-baseline")
	}
	if v := counter(promText(t, rec), "bsl_seq_rebaselines_total"); v != 1 {
		t.Fatalf("re-baselines = %v, want 1", v)
	}
	observeRun(tr, emitterB, next+1, next+1, 0)
	if g2 := tr.PendingGaps(); g2 != g {
		t.Fatalf("contiguous frame after the re-baseline changed PendingGaps %d -> %d", g, g2)
	}
	observeRun(tr, emitterB, next+3, next+3, 0)
	if g3 := tr.PendingGaps(); g3 != g+1 {
		t.Fatalf("real gap after the re-baseline: PendingGaps = %d, want %d", g3, g+1)
	}
}

// frameRetry is a retry endpoint double that answers each NACK with the
// requested frame on the NACK socket (when withData) and an ACK carrying
// flags, and records which SeqNums were asked for.
type frameRetry struct {
	conn  net.PacketConn
	mu    sync.Mutex
	seen  map[uint64]int
	acked chan uint64
}

func newFrameRetry(t *testing.T, flags byte, withData bool) *frameRetry {
	t.Helper()
	c, err := net.ListenPacket("udp", "[::1]:0")
	if err != nil {
		t.Skipf("UDP loopback unavailable: %v", err)
	}
	r := &frameRetry{conn: c, seen: make(map[uint64]int), acked: make(chan uint64, 8192)}
	t.Cleanup(func() { _ = c.Close() })
	go func() {
		buf := make([]byte, 256)
		for {
			n, src, err := c.ReadFrom(buf)
			if err != nil {
				return
			}
			if n < nack.NACKSize {
				continue
			}
			req, derr := nack.Decode(buf[:n])
			if derr != nil {
				continue
			}
			r.mu.Lock()
			r.seen[req.StartSeq]++
			r.mu.Unlock()
			if withData {
				_, _ = c.WriteTo(frameFor(req.HashKey, req.StartSeq), src)
			}
			var resp [nack.ResponseSize]byte
			nack.EncodeResponse(&nack.Response{MsgType: nack.MsgTypeACK, Flags: flags, SeqNum: req.StartSeq}, resp[:])
			_, _ = c.WriteTo(resp[:], src)
			select {
			case r.acked <- req.StartSeq:
			default:
			}
		}
	}()
	return r
}

func (r *frameRetry) addr() string { return r.conn.LocalAddr().String() }

func (r *frameRetry) asked(seq uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen[seq] > 0
}

func (r *frameRetry) times(seq uint64) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen[seq]
}

func (r *frameRetry) distinct() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seen)
}

// frameFor builds a BRC-124-shaped frame carrying hashKey and seq at their
// header offsets, long enough to read as a data frame on the NACK socket.
func frameFor(hashKey, seq uint64) []byte {
	b := make([]byte, 100)
	b[0], b[1], b[2], b[3] = 0xE3, 0xE1, 0xF3, 0xE8 // MagicBSV
	b[6] = 0x02                                     // FrameVer BRC-124
	binary.BigEndian.PutUint64(b[40:48], hashKey)
	binary.BigEndian.PutUint64(b[48:56], seq)
	return b
}
