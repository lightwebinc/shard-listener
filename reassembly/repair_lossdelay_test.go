package reassembly_test

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-listener/nack"
	"github.com/lightwebinc/shard-listener/reassembly"
)

// slowLossyRetry is a retry endpoint on loopback whose repair path is both
// distant and lossy: every answer is held for rtt before it is sent, and the
// first drops[seq] requests for a SeqNum are lost outright (the listener sees a
// response timeout, a failed round, and backs off). It answers like the
// deployed unicast posture: the cached frame, then a unicast-flagged ACK.
type slowLossyRetry struct {
	conn     net.PacketConn
	rtt      time.Duration
	mu       sync.Mutex
	cache    map[uint64][]byte
	drops    map[uint64]int
	attempts map[uint64]int
	served   atomic.Int32
}

func newSlowLossyRetry(t *testing.T, rtt time.Duration) *slowLossyRetry {
	t.Helper()
	c, err := net.ListenPacket("udp", "[::1]:0")
	if err != nil {
		t.Skipf("UDP loopback unavailable: %v", err)
	}
	r := &slowLossyRetry{conn: c, rtt: rtt, cache: map[uint64][]byte{}, drops: map[uint64]int{}, attempts: map[uint64]int{}}
	t.Cleanup(func() { _ = c.Close() })
	go r.serve()
	return r
}

func (r *slowLossyRetry) serve() {
	buf := make([]byte, 256)
	for {
		n, src, err := r.conn.ReadFrom(buf)
		if err != nil {
			return
		}
		req, err := nack.Decode(buf[:n])
		if err != nil {
			continue
		}
		seq := req.StartSeq
		r.mu.Lock()
		r.attempts[seq]++
		lost := r.attempts[seq] <= r.drops[seq]
		data := r.cache[seq]
		r.mu.Unlock()
		if lost || data == nil {
			continue
		}
		time.AfterFunc(r.rtt, func() {
			_, _ = r.conn.WriteTo(data, src)
			var resp [nack.ResponseSize]byte
			nack.EncodeResponse(&nack.Response{MsgType: nack.MsgTypeACK, Flags: 0x02, SeqNum: seq}, resp[:])
			_, _ = r.conn.WriteTo(resp[:], src)
			r.served.Add(1)
		})
	}
}

type lossDelayResult struct {
	completed, abandoned, repairWait, repairWaitDone int32
	served                                           int32
}

// runLossDelay drives three 20-fragment objects on one flow through a real NACK
// tracker and a reassembly buffer, losing fragments on first delivery and
// serving their repair from slowLossyRetry. Object A loses one interior
// fragment whose repair is lost twice; object B loses two interior fragments
// whose repairs are lost three times; object C loses its final fragment, which
// nothing follows, so only the slot's expiry can ask for it.
func runLossDelay(t *testing.T, repairWait bool) lossDelayResult {
	t.Helper()
	const (
		frags   = 20
		fragLen = 100
		hashKey = uint64(0xB0B)
		rtt     = 150 * time.Millisecond
		slotTTL = 250 * time.Millisecond
	)
	retry := newSlowLossyRetry(t, rtt)

	cfg := nack.TrackerConfig{
		JitterMax:   20 * time.Millisecond,
		BackoffBase: 200 * time.Millisecond,
		BackoffMax:  time.Second,
		MaxRetries:  8,
		GapTTL:      time.Minute,
	}
	tr := nack.New(cfg, []string{retry.conn.LocalAddr().String()}, nil, nil, nil)

	var res lossDelayResult
	var completed, abandoned, waited, waitDone atomic.Int32
	buf := reassembly.New(64, slotTTL, false, func([]byte, *frame.Frame) { completed.Add(1) })
	buf.SetAbandonedHook(func() { abandoned.Add(1) })
	buf.SetExtendedHook(func() { waited.Add(1) })
	buf.SetLateCompleteHook(func() { waitDone.Add(1) })
	buf.SetIncompleteHook(func(hk uint64, g uint32, sub [32]byte, missing []uint64) {
		tr.RequestGaps(hk, g, sub, missing)
	})
	if repairWait {
		buf.SetRepairWait(tr.Pending, 10*time.Second)
	}

	// The listener's fragment path: reassembly first, then the gap tracker.
	deliver := func(raw []byte) bool {
		ff, err := frame.DecodeFragment(raw)
		if err != nil {
			t.Errorf("decode: %v", err)
			return false
		}
		buf.Observe(ff)
		tr.Observe(0, [32]byte{}, ff.HashKey, ff.SeqNum, ff.TxID, nil)
		return true
	}
	tr.SetRecoverFunc(deliver)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)
	go func() {
		tk := time.NewTicker(20 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tk.C:
				buf.Tick()
			}
		}
	}()

	lostFirst := map[int]map[int]int{ // object -> fragment index -> lost repair rounds
		0: {7: 2},
		1: {3: 3, 11: 3},
		2: {frags - 1: 1},
	}
	seq := uint64(1)
	for obj := range 3 {
		txid := [32]byte{0xA0 + byte(obj)}
		for i := range frags {
			raw := make([]byte, frame.HeaderSizeV3+fragLen)
			if _, err := frame.EncodeFragment(raw, txid, [32]byte{}, hashKey, seq, frags*fragLen,
				uint16(i), frags, frame.FrameVerV2, make([]byte, fragLen)); err != nil {
				t.Fatal(err)
			}
			retry.mu.Lock()
			retry.cache[seq] = raw
			if d, ok := lostFirst[obj][i]; ok {
				retry.drops[seq] = d
			}
			retry.mu.Unlock()
			if _, ok := lostFirst[obj][i]; !ok {
				deliver(raw)
			}
			seq++
		}
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if completed.Load()+abandoned.Load() >= 3 && buf.Len() == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Let every first-round repair land (one per lost fragment), so the control
	// run shows the repairs arriving after their slots were gone.
	for time.Now().Before(deadline) && retry.served.Load() < 4 {
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)

	res.completed, res.abandoned = completed.Load(), abandoned.Load()
	res.repairWait, res.repairWaitDone = waited.Load(), waitDone.Load()
	res.served = retry.served.Load()
	return res
}

// TestRepairedObjectsCompleteUnderLossAndDelay is the item-77 proof. Repair is
// slow (150 ms per answer) and lossy (two or three rounds lost per fragment), so
// each repair finishes well after the 250 ms slot TTL. With the repair wait,
// every object completes once its fragments are repaired, the tail included.
// The control run (fixed TTL, the previous behaviour) loses all three objects
// although the same repairs are served.
func TestRepairedObjectsCompleteUnderLossAndDelay(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test")
	}
	got := runLossDelay(t, true)
	t.Logf("repair wait: %+v", got)
	if got.completed != 3 || got.abandoned != 0 {
		t.Fatalf("completed=%d abandoned=%d, want 3/0: an object whose fragments were all repaired was abandoned",
			got.completed, got.abandoned)
	}
	if got.repairWait != 3 || got.repairWaitDone != 3 {
		t.Errorf("repair_wait=%d repair_wait_completed=%d, want 3/3", got.repairWait, got.repairWaitDone)
	}
	if got.served < 4 {
		t.Errorf("retry served %d repairs, want at least 4 (one per lost fragment)", got.served)
	}

	ctrl := runLossDelay(t, false)
	t.Logf("fixed TTL: %+v", ctrl)
	if ctrl.completed != 0 || ctrl.abandoned < 3 {
		t.Errorf("control: completed=%d abandoned=%d, want 0 and at least 3; the scenario no longer outlasts the TTL",
			ctrl.completed, ctrl.abandoned)
	}
	if ctrl.served < 4 {
		t.Errorf("control: retry served %d repairs, want at least 4 (repair succeeded, reassembly did not)", ctrl.served)
	}
}
