// Package egress implements the unicast forwarding sink for
// shard-listener. Frames filtered from the multicast fabric are
// delivered to a single downstream host:port over UDP or TCP.
//
// # UDP sender
//
// Each call to [Sender.Send] writes the frame (or payload) into a single
// UDP datagram. No connection state is maintained.
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
)

const tcpWriteDeadline = 5 * time.Second

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
}

// New constructs a Sender. For UDP, the underlying socket is opened immediately.
// For TCP, the connection is established lazily on first Send.
func New(addr, proto string, stripHeader bool) (*Sender, error) {
	s := &Sender{
		addr:        addr,
		proto:       proto,
		stripHeader: stripHeader,
		log:         slog.Default().With("component", "egress"),
	}
	if proto == "udp" {
		dst, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			return nil, fmt.Errorf("egress: resolve UDP addr %q: %w", addr, err)
		}
		conn, err := net.DialUDP("udp", nil, dst)
		if err != nil {
			return nil, fmt.Errorf("egress: dial UDP %q: %w", addr, err)
		}
		s.udpConn = conn
		s.udpDst = dst
	}
	return s, nil
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
	_, err := s.udpConn.Write(buf)
	return err
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

// Proto returns the configured egress protocol ("udp" or "tcp").
func (s *Sender) Proto() string { return s.proto }

// Close releases all underlying connections.
func (s *Sender) Close() error {
	if s.udpConn != nil {
		return s.udpConn.Close()
	}
	s.closeTCP()
	return nil
}
