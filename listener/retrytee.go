package listener

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"net/netip"

	"golang.org/x/sys/unix"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/teewire"
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
type retryTeeSender struct {
	conn   *net.UDPConn
	encBuf []byte
	logged bool // one-shot write-failure log latch
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
	w.retryTee = &retryTeeSender{
		conn:   conn,
		encBuf: make([]byte, 0, teewire.HeaderSize+65536),
	}
	return nil
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
// serve loop goroutine (never from Reinject — a NACK-recovered frame came FROM
// a retry cache and must not be re-mirrored), so the encode buffer needs no
// locking. from is the datagram's kernel source address.
func (w *Worker) teeToRetry(raw []byte, from unix.Sockaddr) {
	sa, ok := from.(*unix.SockaddrInet6)
	if !ok || !teeEligible(raw) {
		return
	}
	t := w.retryTee
	src := netip.AddrPortFrom(netip.AddrFrom16(sa.Addr), uint16(sa.Port))
	t.encBuf = teewire.AppendEncap(t.encBuf[:0], src, raw)
	if _, err := t.conn.Write(t.encBuf); err != nil {
		if w.rec != nil {
			w.rec.RetryTeeError(w.id)
		}
		if !t.logged {
			t.logged = true
			logger := w.log
			if logger == nil {
				logger = slog.Default()
			}
			logger.Warn("retry tee write failed — the co-resident cache will MISS frames this node received; repair coverage narrows to other endpoints (logged once; see bsl_retry_tee_errors_total)",
				"err", err, "target", t.conn.RemoteAddr())
		}
		return
	}
	if w.rec != nil {
		w.rec.RetryTeeFrame(w.id)
	}
}
