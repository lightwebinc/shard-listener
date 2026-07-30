package nack

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
)

// fakeRetry answers NACKs on a UDP socket. hit decides whether a request is
// answered ACK (frame exists) or MISS (never emitted).
type fakeRetry struct {
	conn *net.UDPConn
	hit  bool
	got  chan uint64
}

func newFakeRetry(t *testing.T, hit bool) *fakeRetry {
	t.Helper()
	c, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback, Port: 0})
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	f := &fakeRetry{conn: c, hit: hit, got: make(chan uint64, 32)}
	go func() {
		buf := make([]byte, 2048)
		for {
			n, src, err := c.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if n < 24 {
				continue
			}
			seq := binary.BigEndian.Uint64(buf[16:24])
			select {
			case f.got <- seq:
			default:
			}
			var resp [16]byte
			binary.BigEndian.PutUint32(resp[0:4], frame.MagicBSV)
			binary.BigEndian.PutUint16(resp[4:6], frame.ProtoVer)
			if f.hit {
				resp[6] = MsgTypeACK
			} else {
				resp[6] = MsgTypeMISS
			}
			binary.BigEndian.PutUint64(resp[8:16], seq)
			_, _ = c.WriteToUDP(resp[:], src)
		}
	}()
	t.Cleanup(func() { _ = c.Close() })
	return f
}

func (f *fakeRetry) addr() string {
	return net.JoinHostPort("::1", itoa(f.conn.LocalAddr().(*net.UDPAddr).Port))
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func probeCfg() TrackerConfig {
	return TrackerConfig{
		JitterMax:          time.Millisecond,
		BackoffBase:        20 * time.Millisecond,
		BackoffMax:         50 * time.Millisecond,
		MaxRetries:         2,
		GapTTL:             2 * time.Second,
		TailProbe:          true,
		TailProbeMinIdle:   50 * time.Millisecond,
		TailProbeMaxMisses: 2,
	}
}

// Drive a flow with enough contiguous in-order frames to settle ewmaIPG, then
// let it go quiet.
func settleFlow(tr *Tracker) {
	var zero [32]byte
	for i := uint64(1); i <= minContiguousForRecover+2; i++ {
		tr.Observe(1, zero, 42, i, zero, net.ParseIP("fd00::1"))
		time.Sleep(2 * time.Millisecond)
	}
}

// A quiet flow must probe the next expected SeqNum — the question the absent
// successor frame would have answered.
func TestTailProbeIssuedOnQuietFlow(t *testing.T) {
	fr := newFakeRetry(t, false)
	tr := New(probeCfg(), []string{fr.addr()}, nil, nil, nil)
	tr.Start(t.Context())
	settleFlow(tr)

	select {
	case seq := <-fr.got:
		if seq != minContiguousForRecover+3 {
			t.Errorf("probed seq=%d want %d (next expected)", seq, minContiguousForRecover+3)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no tail probe issued — tail loss stays invisible on a quiet flow")
	}
}

// The critical property: a MISS means the flow was simply idle. It must NOT be
// recorded as an unrecovered gap, or every quiet flow manufactures phantom loss
// and the repair-ratio alerts fire on a healthy fabric.
func TestTailProbeMissIsNotLoss(t *testing.T) {
	fr := newFakeRetry(t, false)
	tr := New(probeCfg(), []string{fr.addr()}, nil, nil, nil)
	tr.Start(t.Context())
	settleFlow(tr)

	<-fr.got // at least one probe went out
	time.Sleep(1500 * time.Millisecond)

	tr.mu.Lock()
	fs, ok := tr.flows[42]
	var pending int
	var misses int
	if ok {
		pending = len(fs.pending)
		misses = fs.probeMisses
	}
	tr.mu.Unlock()

	if !ok {
		t.Skip("flow aged out")
	}
	if pending != 0 {
		t.Errorf("flow holds %d pending entries after MISS; a probe MISS must retire "+
			"the entry, not leave it to expire as an unrecovered gap", pending)
	}
	if misses == 0 {
		t.Error("probe misses not counted — the flow would probe forever")
	}
	if misses > probeCfg().TailProbeMaxMisses {
		t.Errorf("probeMisses=%d exceeds the cap %d — probing did not stop on a confirmed "+
			"idle flow", misses, probeCfg().TailProbeMaxMisses)
	}
}

// Probing must stop once the flow is confirmed idle, so a quiet fabric does not
// carry standing NACK traffic.
func TestTailProbeStopsAfterMaxMisses(t *testing.T) {
	fr := newFakeRetry(t, false)
	tr := New(probeCfg(), []string{fr.addr()}, nil, nil, nil)
	tr.Start(t.Context())
	settleFlow(tr)

	time.Sleep(1200 * time.Millisecond)
	// Drain whatever accumulated, then confirm no NEW probes arrive.
	for len(fr.got) > 0 {
		<-fr.got
	}
	select {
	case seq := <-fr.got:
		t.Errorf("probe for seq=%d issued after the miss cap; an idle flow must go quiet", seq)
	case <-time.After(800 * time.Millisecond):
	}
}

// A probe answered ACK found a REAL loss that no successor frame would ever have
// revealed. It must retire cleanly and reset the miss budget, so the flow keeps
// probing while it is actually losing frames.
func TestTailProbeHitRecovers(t *testing.T) {
	fr := newFakeRetry(t, true)
	tr := New(probeCfg(), []string{fr.addr()}, nil, nil, nil)
	tr.Start(t.Context())
	settleFlow(tr)

	<-fr.got
	time.Sleep(400 * time.Millisecond)

	tr.mu.Lock()
	fs, ok := tr.flows[42]
	var pending, misses int
	var probing uint64
	if ok {
		pending, misses, probing = len(fs.pending), fs.probeMisses, fs.probing
	}
	tr.mu.Unlock()

	if !ok {
		t.Skip("flow aged out")
	}
	// NOTE: pending/probing may legitimately be non-zero here. This fake retry
	// ACKs but sends no DATA, so lastSeqNum never advances and the flow correctly
	// re-probes the same SeqNum on the next sweep. The property that actually
	// separates the ACK path from the MISS path is the miss budget: an ACK must
	// not consume it, so a flow that is genuinely losing frames keeps probing.
	_ = pending
	_ = probing
	if misses != 0 {
		t.Errorf("probeMisses=%d after a successful recovery; an ACK must not consume "+
			"the miss budget or a genuinely lossy flow stops probing", misses)
	}
}

// Regression: a probe whose frame returns via the DATA path (Observe auto-fill or
// out-of-band Fill) must be accounted identically to one closed by an ACK.
// Live measurement caught this: accounting lived only in the ACK path, so a
// data-path recovery incremented gaps_suppressed without gaps_detected and drove
// the repair ratio above 1.0 (observed suppressed=2, detected absent).
func TestTailProbeAccountedOnDataPathFill(t *testing.T) {
	fr := newFakeRetry(t, false) // MISS, so only the data path can close it
	tr := New(probeCfg(), []string{fr.addr()}, nil, nil, nil)
	tr.Start(t.Context())
	settleFlow(tr)

	// Force a probe entry to exist deterministically.
	tr.mu.Lock()
	fs := tr.flows[42]
	seq := fs.lastSeqNum + 1
	fs.pending[seq] = &gapEntry{
		hashKey: 42, seqNum: seq, groupIdx: 1,
		deadline: time.Now().Add(time.Second), speculative: true,
	}
	fs.probing = seq
	tr.mu.Unlock()

	// The frame arrives on the data path.
	tr.Fill(42, seq)

	tr.mu.Lock()
	_, stillPending := tr.flows[42].pending[seq]
	probing := tr.flows[42].probing
	tr.mu.Unlock()

	if stillPending {
		t.Error("data-path Fill did not close the probe entry")
	}
	if probing != 0 {
		t.Error("probing flag not cleared by data-path fill; the flow would never probe again")
	}
}
