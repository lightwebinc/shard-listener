// Package egress — multicast sender.
//
// MCastSender re-emits filtered frames onto a configurable IPv6 multicast
// group address space, enabling bridging between multicast domains with
// optional scope and/or address-space translation.
//
// # Address derivation (zero-alloc)
//
// At construction time bytes 0-13 of an address template are pre-filled
// (scope prefix + zero IANA boundary + 16-bit IANA group-id). Per-frame,
// only bytes 14-15 are overwritten with the 16-bit shard group index
// supplied by the caller — the same index already computed by the ingress
// worker. No heap allocations occur on the hot path.
//
// # Socket options
//
//   - IPV6_MULTICAST_IF  — binds send to the configured egress interface.
//   - IPV6_MULTICAST_HOPS — controls how far re-emitted frames travel.
//   - IPV6_MULTICAST_LOOP = 0 — prevents re-ingestion on the sending host.
//   - SO_SNDBUF = 16 MiB   — absorbs short bursts (kernel caps at wmem_max).
package egress

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"

	"golang.org/x/sys/unix"

	"github.com/lightwebinc/shard-common/frame"
)

const mcSendBuf = 16 * 1024 * 1024 // 16 MiB; kernel silently caps at wmem_max

// MCastSender forwards filtered frames to an IPv6 multicast address space
// derived from the frame's shard index.
type MCastSender struct {
	addrTemplate [16]byte // bytes 0-13 fixed; 14-15 written per-frame
	egressPort   int
	stripHeader  bool
	fd           int
	log          *slog.Logger
}

// NewMCast constructs an MCastSender and opens the underlying UDP6 socket.
//
//   - mcPrefix is the two-byte IPv6 multicast prefix for egress groups (e.g. 0xFF02).
//   - groupID is the 16-bit IANA group-id occupying bytes 12–13 (default 0x000B).
//   - shardBits is reserved for future group-subset egress; unused by current send logic.
//   - port is the UDP destination port for egress datagrams.
//   - iface is the network interface used for multicast send (IPV6_MULTICAST_IF).
//   - hopLimit is set via IPV6_MULTICAST_HOPS (1 = link-local, higher for routed domains).
//   - stripHeader mirrors the listener-wide -strip-header flag.
func NewMCast(
	mcPrefix uint16,
	groupID uint16,
	_ uint, // shardBits — reserved for future group-subset egress
	port int,
	iface *net.Interface,
	hopLimit int,
	stripHeader bool,
) (*MCastSender, error) {
	fd, err := unix.Socket(unix.AF_INET6, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("mc-egress: socket: %w", err)
	}

	if err := unix.SetsockoptInt(fd, unix.IPPROTO_IPV6, unix.IPV6_MULTICAST_IF, iface.Index); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("mc-egress: IPV6_MULTICAST_IF: %w", err)
	}
	if err := unix.SetsockoptInt(fd, unix.IPPROTO_IPV6, unix.IPV6_MULTICAST_HOPS, hopLimit); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("mc-egress: IPV6_MULTICAST_HOPS: %w", err)
	}
	// Disable loopback so re-emitted frames are not received back by sockets
	// on the sending host, including this listener's own ingress socket.
	if err := unix.SetsockoptInt(fd, unix.IPPROTO_IPV6, unix.IPV6_MULTICAST_LOOP, 0); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("mc-egress: IPV6_MULTICAST_LOOP: %w", err)
	}
	// Send buffer: absorbs short bursts; kernel silently caps at wmem_max.
	_ = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_SNDBUF, mcSendBuf)

	s := &MCastSender{
		egressPort:  port,
		stripHeader: stripHeader,
		fd:          fd,
		log:         slog.Default().With("component", "mc-egress"),
	}
	// Pre-fill fixed address bytes: scope prefix (bytes 0-1), IANA 96-bit
	// boundary zero-fill (bytes 2-11), and IANA group-id (bytes 12-13).
	// Bytes 14-15 are written per-frame.
	binary.BigEndian.PutUint16(s.addrTemplate[0:2], mcPrefix)
	// bytes 2..11 are already zero (Go zero-init)
	binary.BigEndian.PutUint16(s.addrTemplate[12:14], groupID)

	return s, nil
}

// Send forwards raw (or f.Payload when stripHeader is set) to the multicast
// group address for groupIdx.
//
// groupIdx must be the value already computed by shard.Engine.GroupIndex in
// the calling worker; it is not re-derived here. The SockaddrInet6 is built
// on the stack — no heap allocations occur.
func (s *MCastSender) Send(raw []byte, f *frame.Frame, groupIdx uint32) error {
	var buf []byte
	if s.stripHeader {
		buf = f.Payload
	} else {
		buf = raw
	}

	// Write the 16-bit group index into bytes 14-15 of the address template.
	s.addrTemplate[14] = byte(groupIdx >> 8)
	s.addrTemplate[15] = byte(groupIdx)

	sa := unix.SockaddrInet6{Port: s.egressPort}
	copy(sa.Addr[:], s.addrTemplate[:])

	return unix.Sendto(s.fd, buf, 0, &sa)
}

// SendToGroup forwards buf to the multicast group at the given control
// group index. Unlike Send, no frame decoding or strip-header logic is
// applied — buf is sent verbatim. The destination address is derived
// from the pre-filled template with groupIdx in bytes 14-15.
func (s *MCastSender) SendToGroup(buf []byte, groupIdx uint16) error {
	s.addrTemplate[14] = byte(groupIdx >> 8)
	s.addrTemplate[15] = byte(groupIdx)
	sa := unix.SockaddrInet6{Port: s.egressPort}
	copy(sa.Addr[:], s.addrTemplate[:])
	return unix.Sendto(s.fd, buf, 0, &sa)
}

// Proto returns the egress protocol identifier used in metrics labels.
func (s *MCastSender) Proto() string { return "udp-mcast" }

// Close releases the underlying socket.
func (s *MCastSender) Close() error {
	return unix.Close(s.fd)
}
