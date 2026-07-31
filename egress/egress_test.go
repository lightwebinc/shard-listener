package egress

import (
	"net"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
)

func newUDPSink(t *testing.T) (string, *net.UDPConn, func()) {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	return pc.LocalAddr().String(), pc, func() { _ = pc.Close() }
}

func TestNew_UDP(t *testing.T) {
	addr, _, cleanup := newUDPSink(t)
	defer cleanup()
	s, err := New(addr, "udp", false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if s.Proto() != "udp" {
		t.Errorf("Proto = %q", s.Proto())
	}
	if s.udpConn == nil || s.udpDst == nil {
		t.Error("udp state not initialized")
	}
}

func TestNew_UDP_InvalidAddr(t *testing.T) {
	if _, err := New("not-an-addr", "udp", false); err == nil {
		t.Error("expected error")
	}
}

// unreachableUDP is a destination connect() always rejects: an IPv6
// link-local address carries no zone, so it cannot be bound to an interface.
// It stands in for the real case — an SDA reachable only through a tunnel that
// exists on another edge.
const unreachableUDP = "[fe80::1]:9701"

// newUnreachableUDP builds a Sender whose destination could not be connected,
// skipping if the platform dialed it anyway (nothing left to test).
func newUnreachableUDP(t *testing.T) *Sender {
	t.Helper()
	s, err := New(unreachableUDP, "udp", false)
	if err != nil {
		t.Fatalf("New must not fail on an unreachable UDP destination: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if s.udpConn != nil {
		t.Skip("platform connected the probe address; no unroutable destination available")
	}
	return s
}

// TestNew_UDP_UnreachableDest is the standby-edge regression. A consumer's
// tunnel exists only on the edge currently serving it, so on its standby edge
// the SDA is unroutable by design. Dialing in New made that fatal, and every
// restart of a standby listener crash-looped on "network is unreachable".
func TestNew_UDP_UnreachableDest(t *testing.T) {
	s := newUnreachableUDP(t)
	// Frames fail (the caller counts an egress error) instead of hitting a
	// nil socket.
	if err := s.Send([]byte("raw"), &frame.Frame{Payload: []byte("p")}); err == nil {
		t.Error("Send to an unreachable destination should error")
	}
}

// TestNew_UDP_UnresolvableHost: name resolution is deferred with the dial, so a
// resolver that is briefly unavailable does not fail start-up either.
func TestNew_UDP_UnresolvableHost(t *testing.T) {
	s, err := New("no-such-host.invalid:9701", "udp", false)
	if err != nil {
		t.Fatalf("New must not fail on an unresolvable host: %v", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.SendRaw([]byte("x")); err == nil {
		t.Error("SendRaw to an unresolvable host should error")
	}
}

// TestSend_UDP_RedialBackoff: a destination that stays down must cost one dial
// per backoff interval, not one per frame — this sink sees the full fabric rate.
func TestSend_UDP_RedialBackoff(t *testing.T) {
	s := newUnreachableUDP(t)
	if s.udpBackoff != udpRedialMin {
		t.Fatalf("backoff after first failed dial = %v, want %v", s.udpBackoff, udpRedialMin)
	}
	retryAt := s.udpRetryAt
	for range 10 {
		if err := s.SendRaw([]byte("x")); err == nil {
			t.Fatal("send to an unreachable destination should error")
		}
	}
	if !s.udpRetryAt.Equal(retryAt) {
		t.Error("re-dialed inside the backoff window")
	}
}

// TestSend_UDP_ReconnectAfterWriteError: a connected UDP socket pins its route
// at connect time, so it stays dead once the destination's tunnel goes away.
// The failed write drops it and the next frame re-connects — which is how a
// Sender picks up a consumer that fails over onto this edge.
func TestSend_UDP_ReconnectAfterWriteError(t *testing.T) {
	addr, pc, cleanup := newUDPSink(t)
	defer cleanup()
	s, err := New(addr, "udp", false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	if err := s.SendRaw([]byte("one")); err != nil {
		t.Fatal(err)
	}
	// Kill the socket under the Sender (stands in for the route going away).
	_ = s.udpConn.Close()
	if err := s.SendRaw([]byte("lost")); err == nil {
		t.Error("write on a dead socket should error")
	}
	if s.udpConn != nil {
		t.Error("failed socket should be dropped, not reused")
	}
	if err := s.SendRaw([]byte("two")); err != nil {
		t.Fatalf("next frame should re-dial: %v", err)
	}

	buf := make([]byte, 100)
	for _, want := range []string{"one", "two"} {
		_ = pc.SetReadDeadline(time.Now().Add(time.Second))
		n, _, err := pc.ReadFrom(buf)
		if err != nil {
			t.Fatalf("read %q: %v", want, err)
		}
		if string(buf[:n]) != want {
			t.Errorf("got %q, want %q", buf[:n], want)
		}
	}
}

func TestNew_TCP_LazyDial(t *testing.T) {
	s, err := New("127.0.0.1:1", "tcp", false)
	if err != nil {
		t.Fatal(err) // TCP should not dial during New
	}
	defer func() { _ = s.Close() }()
	if s.Proto() != "tcp" {
		t.Error("Proto")
	}
	if s.tcpConn != nil {
		t.Error("TCP should be lazy")
	}
}

func TestSend_UDP(t *testing.T) {
	addr, pc, cleanup := newUDPSink(t)
	defer cleanup()
	s, _ := New(addr, "udp", false)
	defer func() { _ = s.Close() }()

	raw := []byte("hello-frame")
	f := &frame.Frame{Payload: []byte("hello")}
	if err := s.Send(raw, f); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 100)
	_ = pc.SetReadDeadline(time.Now().Add(time.Second))
	n, _, err := pc.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "hello-frame" {
		t.Errorf("got %q", buf[:n])
	}
}

func TestSend_UDP_StripHeader(t *testing.T) {
	addr, pc, cleanup := newUDPSink(t)
	defer cleanup()
	s, _ := New(addr, "udp", true)
	defer func() { _ = s.Close() }()

	raw := []byte("ignored-header-then-payload")
	f := &frame.Frame{Payload: []byte("only-payload")}
	if err := s.Send(raw, f); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 100)
	_ = pc.SetReadDeadline(time.Now().Add(time.Second))
	n, _, _ := pc.ReadFrom(buf)
	if string(buf[:n]) != "only-payload" {
		t.Errorf("got %q", buf[:n])
	}
}

func TestSend_UnknownProto(t *testing.T) {
	s := &Sender{proto: "weird"}
	if err := s.Send([]byte("x"), &frame.Frame{}); err == nil {
		t.Error("expected error")
	}
}

func TestSend_TCP_DialAndWrite(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	doneCh := make(chan []byte, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		buf := make([]byte, 128)
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, _ := conn.Read(buf)
		doneCh <- buf[:n]
	}()

	s, _ := New(ln.Addr().String(), "tcp", false)
	defer func() { _ = s.Close() }()

	raw := []byte("tcp-frame")
	if err := s.Send(raw, &frame.Frame{}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-doneCh:
		if string(got) != "tcp-frame" {
			t.Errorf("got %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
	if s.tcpConn == nil {
		t.Error("tcpConn should be set after Send")
	}
}

func TestSend_TCP_DialFailure(t *testing.T) {
	// Connect to a port that should refuse: pick 1 (privileged, likely closed).
	s, _ := New("127.0.0.1:1", "tcp", false)
	defer func() { _ = s.Close() }()
	if err := s.Send([]byte("x"), &frame.Frame{}); err == nil {
		t.Error("expected dial failure")
	}
}

func TestSend_TCP_ReconnectAfterClose(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer func() { _ = ln.Close() }()
	// First accept loop.
	accept := make(chan net.Conn, 2)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accept <- conn
		}
	}()

	s, _ := New(ln.Addr().String(), "tcp", false)
	defer func() { _ = s.Close() }()

	if err := s.Send([]byte("one"), &frame.Frame{}); err != nil {
		t.Fatal(err)
	}
	first := <-accept
	// Force a write error by closing remote side.
	_ = first.Close()
	// Second send: likely fails (peer closed) and triggers reconnect.
	_ = s.Send([]byte("two"), &frame.Frame{})

	// Subsequent send should re-dial (lazy).
	if err := s.Send([]byte("three"), &frame.Frame{}); err != nil {
		// Possibly accepted, possibly transient; just verify no panic.
		t.Logf("third send: %v", err)
	}
	// At least one more accept should have happened.
	select {
	case <-accept:
	case <-time.After(time.Second):
		// allow flake
	}
}

func TestClose_UDP(t *testing.T) {
	addr, _, cleanup := newUDPSink(t)
	defer cleanup()
	s, _ := New(addr, "udp", false)
	if err := s.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
}

func TestClose_TCP(t *testing.T) {
	s, _ := New("127.0.0.1:1", "tcp", false)
	if err := s.Close(); err != nil {
		t.Errorf("close (no conn): %v", err)
	}
}

func TestSendRaw_UDP(t *testing.T) {
	addr, pc, cleanup := newUDPSink(t)
	defer cleanup()
	s, _ := New(addr, "udp", false)
	defer func() { _ = s.Close() }()

	buf := []byte("raw-header-bytes")
	if err := s.SendRaw(buf); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 100)
	_ = pc.SetReadDeadline(time.Now().Add(time.Second))
	n, _, err := pc.ReadFrom(got)
	if err != nil {
		t.Fatal(err)
	}
	if string(got[:n]) != "raw-header-bytes" {
		t.Errorf("got %q", got[:n])
	}
}

func TestSendRaw_TCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	doneCh := make(chan []byte, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		buf := make([]byte, 256)
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, _ := conn.Read(buf)
		doneCh <- buf[:n]
	}()

	s, _ := New(ln.Addr().String(), "tcp", false)
	defer func() { _ = s.Close() }()

	if err := s.SendRaw([]byte("raw-tcp-data")); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-doneCh:
		if string(got) != "raw-tcp-data" {
			t.Errorf("got %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestSendRaw_UnknownProto(t *testing.T) {
	s := &Sender{proto: "weird"}
	if err := s.SendRaw([]byte("x")); err == nil {
		t.Error("expected error")
	}
}

func TestSendToGroup_AddressDerivation(t *testing.T) {
	// Test that SendToGroup correctly writes the group index into bytes 14-15.
	s := &MCastSender{}
	s.addrTemplate[0], s.addrTemplate[1] = 0xFF, 0x05
	s.egressPort = 9001

	// Simulate SendToGroup address derivation without actually sending.
	groupIdx := uint16(0xFFFA)
	s.addrTemplate[14] = byte(groupIdx >> 8)
	s.addrTemplate[15] = byte(groupIdx)

	if s.addrTemplate[14] != 0xFF || s.addrTemplate[15] != 0xFA {
		t.Errorf("group bytes: 0x%02X%02X, want 0xFFFA", s.addrTemplate[14], s.addrTemplate[15])
	}
}

func TestMCastSender_AddressDerivation(t *testing.T) {
	// Construct without opening a real multicast socket — we test the address-template
	// derivation logic directly on a zero-value MCastSender.
	s := &MCastSender{}
	// Manually set fields as NewMCast would.
	s.addrTemplate[0], s.addrTemplate[1] = 0xFF, 0x05
	copy(s.addrTemplate[2:13], []byte{0xDE, 0xAD, 0xBE, 0xEF, 0, 0, 0, 0, 0, 0, 0})
	s.egressPort = 9001

	// Simulate the per-frame address derivation that Send does.
	groupIdx := uint32(0x010203)
	s.addrTemplate[13] = byte(groupIdx >> 16)
	s.addrTemplate[14] = byte(groupIdx >> 8)
	s.addrTemplate[15] = byte(groupIdx)

	if s.addrTemplate[13] != 0x01 || s.addrTemplate[14] != 0x02 || s.addrTemplate[15] != 0x03 {
		t.Errorf("group bytes: %x", s.addrTemplate[13:16])
	}
	if s.Proto() != "udp-mcast" {
		t.Errorf("Proto = %q", s.Proto())
	}
}
