package listener

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/teewire"

	"github.com/lightwebinc/shard-listener/metrics"
)

// retryTeeSender mirrors received data frames to a co-resident
// retry-endpoint's tee-ingest socket (its -tee-listen) over loopback UDP.
//
// This is the receive-side counterpart of the proxy's -retry-tee: the proxy
// mirrors frames this node ORIGINATES (which the retry cannot receive via
// multicast — own-source SSM joins are structurally excluded), while this tee
// mirrors frames this node RECEIVES from the fabric. Together the two feeds
// let a co-resident retry-endpoint fill its repair cache without any
// multicast join of its own (retry-endpoint -mc-join-enabled=false), which
// frees the shared wildcard data port on a collapsed node.
//
// Each mirrored frame is wrapped in a teewire envelope carrying the original
// datagram source, so the cache's per-source counters (the
// RetryCacheSourceStarved alerting contract) attribute tee-fed frames exactly
// as natively received ones. The mirror is deliberately lossy: a failed write
// is counted and logged once, never retried and never allowed to slow the
// receive loop — a missed mirror only narrows repair coverage to the other
// endpoints a listener walks on MISS.
//
// The send runs on a goroutine of the tee's own, the shape the proxy's tee
// took in shard-proxy v1.41.0. The processing goroutine copies each eligible
// datagram into a pooled buffer and queues it without blocking; the sender
// writes the queue in order. A 4 MiB BEEF record is 477 datagrams, and
// writing their copies over loopback synchronously cost the processing
// goroutine a loopback send per datagram on top of reassembly and delivery;
// the copy costs it a fraction of that. The queue is bounded (teeQueueDepth
// datagrams): a tee that falls behind drops datagrams and counts them
// (bsl_retry_tee_dropped_total), since the cache is an optimization and the
// receive path never waits on it. close drains what is queued, then closes
// the socket.
type retryTeeSender struct {
	conn *net.UDPConn
	rec  *metrics.Recorder // nil = no counters
	id   int               // worker id, the counters' label
	log  *slog.Logger

	// pool holds copy buffers of teeBufSize bytes; a larger datagram gets a
	// buffer of its own.
	pool sync.Pool

	// mu guards closed and started against a send on q: close closes the
	// channel, and a mirror that raced it would otherwise panic.
	mu      sync.Mutex
	closed  bool
	started bool
	q       chan teeMsg
	done    chan struct{}

	dropped    atomic.Uint64 // datagrams dropped because q was full or the tee was closed
	loggedDrop atomic.Bool   // one-shot drop log latch
	loggedErr  atomic.Bool   // one-shot write-failure log latch
}

const (
	// teeQueueDepth bounds the datagrams the tee holds for its sender. At a
	// 9000-byte fragment MTU and 200 MB/s it is some 45 ms of receive; the
	// sender's loopback write runs far ahead of that, so the queue absorbs
	// scheduling jitter and a full queue means the tee's peer, not the tee,
	// is the bottleneck.
	teeQueueDepth = 1024
	// teeBufSize is the pooled copy buffer: the teewire envelope plus any
	// datagram up to a jumbo-frame fragment.
	teeBufSize = 16 << 10
)

// teeMsg is one queued mirror datagram: the enveloped copy and, when the copy
// sits in a pooled buffer, the buffer to return once written.
type teeMsg struct {
	buf    []byte
	pooled *[]byte
}

// SetRetryTee enables the receive-side retry tee, dialing addr (the
// co-resident retry-endpoint's -tee-listen, e.g. "[::1]:9002"). Call after
// New and before Run. Unlike the proxy's best-effort enable, a dial failure
// here is returned: when this tee is configured it may be the only feed a
// tee-only retry-endpoint has for remote-origin frames, and starting without
// it would silently strand every cross-node NACK on this cache.
func (w *Worker) SetRetryTee(addr string) error {
	ua, err := net.ResolveUDPAddr("udp6", addr)
	if err != nil {
		return fmt.Errorf("retry-tee %q: %w", addr, err)
	}
	logger := w.log
	if logger == nil {
		logger = slog.Default()
	}
	if !ua.IP.IsLoopback() {
		logger.Warn("retry tee target is not loopback; the mirrored receive stream and its source assertions leave the node", "addr", addr)
	}
	conn, err := net.DialUDP("udp6", nil, ua)
	if err != nil {
		return fmt.Errorf("retry-tee dial %s: %w", addr, err)
	}
	w.retryTee = newRetryTeeSender(conn, w.rec, w.id, logger, teeQueueDepth)
	w.retryTee.start()
	return nil
}

// newRetryTeeSender builds a tee over conn with a queue of depth datagrams.
// The sender goroutine runs from start; a tee that is closed before it was
// started still drains its queue (see close).
func newRetryTeeSender(conn *net.UDPConn, rec *metrics.Recorder, id int, log *slog.Logger, depth int) *retryTeeSender {
	t := &retryTeeSender{
		conn: conn,
		rec:  rec,
		id:   id,
		log:  log,
		q:    make(chan teeMsg, depth),
		done: make(chan struct{}),
	}
	t.pool.New = func() any {
		b := make([]byte, teeBufSize)
		return &b
	}
	return t
}

// start launches the sender goroutine. Idempotent.
func (t *retryTeeSender) start() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.started || t.closed {
		return
	}
	t.started = true
	go t.run()
}

// run is the sender: one connected-socket Write per queued datagram, in queue
// order, until the queue is closed.
func (t *retryTeeSender) run() {
	defer close(t.done)
	for m := range t.q {
		t.write(m)
	}
}

// write sends one mirror datagram, counts the outcome and releases its buffer.
func (t *retryTeeSender) write(m teeMsg) {
	if _, err := t.conn.Write(m.buf); err != nil {
		if t.rec != nil {
			t.rec.RetryTeeError(t.id)
		}
		if t.loggedErr.CompareAndSwap(false, true) {
			t.log.Warn("retry tee write failed — the co-resident cache will MISS frames this node received; repair coverage narrows to other endpoints (logged once; see bsl_retry_tee_errors_total)",
				"err", err, "target", t.conn.RemoteAddr())
		}
	} else if t.rec != nil {
		t.rec.RetryTeeFrame(t.id)
	}
	if m.pooled != nil {
		t.pool.Put(m.pooled)
	}
}

// mirror copies raw into an envelope naming src and queues it for the sender
// without blocking. A full queue, or a closed tee, drops the datagram and
// counts it.
func (t *retryTeeSender) mirror(src netip.AddrPort, raw []byte) {
	var m teeMsg
	if need := teewire.HeaderSize + len(raw); need <= teeBufSize {
		m.pooled = t.pool.Get().(*[]byte)
		m.buf = teewire.AppendEncap((*m.pooled)[:0], src, raw)
	} else {
		m.buf = teewire.AppendEncap(make([]byte, 0, need), src, raw)
	}
	queued := false
	t.mu.Lock()
	if !t.closed {
		select {
		case t.q <- m:
			queued = true
		default:
		}
	}
	t.mu.Unlock()
	if queued {
		return
	}
	if m.pooled != nil {
		t.pool.Put(m.pooled)
	}
	t.dropped.Add(1)
	if t.rec != nil {
		t.rec.RetryTeeDropped(t.id)
	}
	if t.loggedDrop.CompareAndSwap(false, true) {
		t.log.Warn("retry tee behind; datagrams dropped: the co-resident cache will MISS frames this node received; repair coverage narrows to other endpoints (logged once; see bsl_retry_tee_dropped_total)",
			"target", t.conn.RemoteAddr())
	}
}

// close stops accepting datagrams, writes whatever is queued, then closes the
// socket. A tee whose sender was never started drains on the caller's
// goroutine. Safe to call more than once.
func (t *retryTeeSender) close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	started := t.started
	close(t.q)
	t.mu.Unlock()
	if started {
		<-t.done
	} else {
		t.run()
	}
	return t.conn.Close()
}

// teeEligible reports whether a received datagram is a cacheable data frame.
// Control datagrams (BRC-126 NACK/ADVERT, BRC-127 announces, BRC-139
// manifests — version byte ≥ 0x10) are excluded, as the retry-endpoint would
// only count them as decode-error drops. BRC-135 header frames (V7) are
// excluded because they are outside the primary NACK repair path and the
// retry-endpoint has no V7 handler.
func teeEligible(raw []byte) bool {
	if len(raw) < 7 || binary.BigEndian.Uint32(raw[0:4]) != frame.MagicBSV {
		return false
	}
	v := raw[6]
	return v >= frame.FrameVerV1 && v <= frame.FrameVerV9 && v != frame.FrameVerV7
}

// teeToRetry mirrors one eligible received datagram. Called only from the
// serve loop's processing goroutine (never from Reinject: a NACK-recovered
// frame came FROM a retry cache and must not be re-mirrored). raw is the
// worker's receive buffer, reused as soon as this returns, so the tee copies
// it before queuing. from is the datagram's kernel source address.
func (w *Worker) teeToRetry(raw []byte, from unix.Sockaddr) {
	sa, ok := from.(*unix.SockaddrInet6)
	if !ok || !teeEligible(raw) {
		return
	}
	w.retryTee.mirror(netip.AddrPortFrom(netip.AddrFrom16(sa.Addr), uint16(sa.Port)), raw)
}
