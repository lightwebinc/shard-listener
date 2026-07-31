// Package egress implements the unicast forwarding sink for
// shard-listener. Frames filtered from the multicast fabric are
// delivered to a single downstream host:port over UDP or TCP.
//
// # UDP sender
//
// Each call to [Sender.Send] writes the frame (or payload) into a single
// UDP datagram on a connected socket. The socket is opened on demand and
// re-opened after a write error, so a destination that is unroutable when the
// listener starts (or that goes away later) costs failed sends, never a failed
// start-up — see [New].
//
// # TCP sender
//
// A persistent TCP connection is maintained. On write error the connection
// is closed and the next Send re-dials. The connection is owned by the
// calling goroutine; concurrent callers must use separate Sender instances
// (one per worker).
//
// # Strip-header mode
//
// When [Config.StripHeader] is true, only the raw BSV transaction bytes
// (frame.Payload) are forwarded. When false, the complete frame including
// the 92-byte BRC-124/BRC-128 header is forwarded verbatim.
package egress

import (
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
)

const (
	tcpWriteDeadline = 5 * time.Second

	// udpRedialMin/Max bound how often a Sender re-opens a connected UDP
	// socket for an unreachable destination. Without them the retry is one
	// socket+connect per FRAME on a fan-out that sees the full fabric rate.
	udpRedialMin = 250 * time.Millisecond
	udpRedialMax = 5 * time.Second
)

// Sender forwards frames to a single downstream unicast address.
type Sender struct {
	addr        string
	proto       string
	stripHeader bool
	log         *slog.Logger

	// TCP-only state (nil for UDP)
	tcpConn net.Conn

	// UDP-only state (nil for TCP)
	udpConn *net.UDPConn
	udpDst  *net.UDPAddr
	// udpErr is the last dial failure and udpRetryAt the earliest next
	// attempt, so a down destination is retried on a backoff rather than on
	// every frame.
	udpErr     error
	udpRetryAt time.Time
	udpBackoff time.Duration
}

// New constructs a Sender. Construction never fails on the network: the
// address is validated, a UDP socket is opened best-effort, and a destination
// that is not connectable yet is connected on a later Send instead.
//
// UDP construction USED to dial, and that made an unroutable destination fatal
// at start-up. A consumer's tunnel exists only on the edge currently serving
// it, so on that consumer's standby edge its address is unreachable by
// design — and any restart of a standby listener (a roll, a config reload)
// died in New with "dial udp <sda>: network is unreachable" and crash-looped.
// Dialing on demand also lets a Sender pick up a tunnel that appears AFTER
// start-up, which is exactly what a failover onto the standby edge does.
//
// A syntactically bad address is still rejected here: that is a config error
// no amount of retrying fixes, and it should fail the roll. Name resolution is
// deferred with the dial — a resolver that is briefly unavailable is a
// transient, not a config error.
func New(addr, proto string, stripHeader bool) (*Sender, error) {
	s := &Sender{
		addr:        addr,
		proto:       proto,
		stripHeader: stripHeader,
		log:         slog.Default().With("component", "egress"),
	}
	if proto == "udp" {
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return nil, fmt.Errorf("egress: UDP addr %q: %w", addr, err)
		}
		// Best-effort: connect now so a reachable destination is ready before
		// the first frame arrives. A failure is logged, not returned.
		if err := s.dialUDP(); err != nil {
			s.log.Warn("UDP egress destination not reachable at start; will connect on demand",
				"addr", addr, "err", err)
		}
	}
	return s, nil
}

// dialUDP opens the connected socket, rate-limited by backoff. It reports the
// dial error (cached between attempts) so a caller that cannot send still gets
// an error to count, without paying a syscall per frame.
func (s *Sender) dialUDP() error {
	now := time.Now()
	if s.udpBackoff > 0 && now.Before(s.udpRetryAt) {
		return s.udpErr
	}
	dst, err := net.ResolveUDPAddr("udp", s.addr)
	if err == nil {
		var conn *net.UDPConn
		if conn, err = net.DialUDP("udp", nil, dst); err == nil {
			s.udpConn, s.udpDst = conn, dst
			s.udpErr, s.udpBackoff = nil, 0
			s.log.Info("UDP egress connected", "addr", s.addr)
			return nil
		}
	}
	s.udpErr = fmt.Errorf("egress: UDP dial %q: %w", s.addr, err)
	s.udpBackoff = min(max(2*s.udpBackoff, udpRedialMin), udpRedialMax)
	s.udpRetryAt = now.Add(s.udpBackoff)
	return s.udpErr
}

// Send forwards f to the downstream. raw is the verbatim wire buffer (used
// when stripHeader is false); f.Payload is used when stripHeader is true.
// raw must be valid for the duration of this call.
func (s *Sender) Send(raw []byte, f *frame.Frame) error {
	var buf []byte
	if s.stripHeader {
		buf = f.Payload
	} else {
		buf = raw
	}
	switch s.proto {
	case "udp":
		return s.sendUDP(buf)
	case "tcp":
		return s.sendTCP(buf)
	default:
		return fmt.Errorf("egress: unknown protocol %q", s.proto)
	}
}

func (s *Sender) sendUDP(buf []byte) error {
	if s.udpConn == nil {
		if err := s.dialUDP(); err != nil {
			return err
		}
	}
	if _, err := s.udpConn.Write(buf); err != nil {
		// A connected UDP socket pins its source address and route at connect
		// time, so once the destination's tunnel is torn down every subsequent
		// write fails on the same dead socket. Drop it and let the next frame
		// re-dial (on backoff): that is what re-homes this Sender when the
		// consumer fails over onto this edge.
		s.closeUDP()
		return fmt.Errorf("egress: UDP write %q: %w", s.addr, err)
	}
	return nil
}

func (s *Sender) closeUDP() {
	if s.udpConn != nil {
		_ = s.udpConn.Close()
		s.udpConn = nil
		s.log.Info("UDP egress socket closed; will reconnect on next frame", "addr", s.addr)
	}
}

func (s *Sender) sendTCP(buf []byte) error {
	if s.tcpConn == nil {
		conn, err := net.DialTimeout("tcp", s.addr, tcpWriteDeadline)
		if err != nil {
			return fmt.Errorf("egress: TCP dial %q: %w", s.addr, err)
		}
		s.tcpConn = conn
		s.log.Info("TCP egress connected", "addr", s.addr)
	}
	if err := s.tcpConn.SetWriteDeadline(time.Now().Add(tcpWriteDeadline)); err != nil {
		s.closeTCP()
		return err
	}
	if _, err := s.tcpConn.Write(buf); err != nil {
		s.closeTCP()
		return fmt.Errorf("egress: TCP write: %w", err)
	}
	return nil
}

func (s *Sender) closeTCP() {
	if s.tcpConn != nil {
		_ = s.tcpConn.Close()
		s.tcpConn = nil
		s.log.Info("TCP egress connection closed; will reconnect on next frame", "addr", s.addr)
	}
}

// SendRaw forwards an arbitrary byte buffer to the downstream. No
// strip-header logic is applied — buf is sent verbatim.
func (s *Sender) SendRaw(buf []byte) error {
	switch s.proto {
	case "udp":
		return s.sendUDP(buf)
	case "tcp":
		return s.sendTCP(buf)
	default:
		return fmt.Errorf("egress: unknown protocol %q", s.proto)
	}
}

// SendBlock forwards a BRC-131 block control frame to the downstream.
// When stripHeader is true, only bf.Payload is sent; otherwise the full raw
// wire buffer is forwarded.
func (s *Sender) SendBlock(raw []byte, bf *frame.BlockFrame) error {
	var buf []byte
	if s.stripHeader {
		buf = bf.Payload
	} else {
		buf = raw
	}
	switch s.proto {
	case "udp":
		return s.sendUDP(buf)
	case "tcp":
		return s.sendTCP(buf)
	default:
		return fmt.Errorf("egress: unknown protocol %q", s.proto)
	}
}

// SendSubtreeData forwards a BRC-132 subtree data frame to the downstream.
// When stripHeader is true, only sf.Payload is sent; otherwise the full raw
// wire buffer is forwarded.
func (s *Sender) SendSubtreeData(raw []byte, sf *frame.SubtreeDataFrame) error {
	var buf []byte
	if s.stripHeader {
		buf = sf.Payload
	} else {
		buf = raw
	}
	switch s.proto {
	case "udp":
		return s.sendUDP(buf)
	case "tcp":
		return s.sendTCP(buf)
	default:
		return fmt.Errorf("egress: unknown protocol %q", s.proto)
	}
}

// SendBeef forwards a BRC-148 BEEF object frame (FrameVer 0x09) to the
// downstream. When stripHeader is true the BRC-149 DELIVERY RECORD is sent
// (TopicID ∥ u32 objectLen ∥ object) — never the bare object, which is not
// self-delimiting and so cannot be framed on a stream. Otherwise the full
// raw wire buffer is forwarded.
func (s *Sender) SendBeef(raw []byte, bf *frame.BEEFFrame) error {
	var buf []byte
	if s.stripHeader {
		// BRC-149 delivery record — NOT the bare object. A BEEF object is not
		// self-delimiting without a full structural parse, so a bare object on
		// a persistent TCP stream cannot be split by the receiver; the record's
		// explicit length restores framing. It also preserves the TopicID,
		// which tells the subscriber which of its elected topics matched.
		buf = objfmt.EncodeBEEFDelivery(bf.TopicID, bf.Payload)
	} else {
		buf = raw
	}
	switch s.proto {
	case "udp":
		return s.sendUDP(buf)
	case "tcp":
		return s.sendTCP(buf)
	default:
		return fmt.Errorf("egress: unknown protocol %q", s.proto)
	}
}

// Proto returns the configured egress protocol ("udp" or "tcp").
func (s *Sender) Proto() string { return s.proto }

// Close releases all underlying connections. A Sender that never managed to
// connect closes cleanly (nothing was opened).
func (s *Sender) Close() error {
	if s.udpConn != nil {
		err := s.udpConn.Close()
		s.udpConn = nil
		return err
	}
	s.closeTCP()
	return nil
}
