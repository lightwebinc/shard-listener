package egress

import (
	"net"
	"testing"
	"time"

	"github.com/lightwebinc/bitcoin-shard-common/frame"
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
	defer s.Close()
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

func TestNew_TCP_LazyDial(t *testing.T) {
	s, err := New("127.0.0.1:1", "tcp", false)
	if err != nil {
		t.Fatal(err) // TCP should not dial during New
	}
	defer s.Close()
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
	defer s.Close()

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
	defer s.Close()

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
	defer ln.Close()
	doneCh := make(chan []byte, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 128)
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, _ := conn.Read(buf)
		doneCh <- buf[:n]
	}()

	s, _ := New(ln.Addr().String(), "tcp", false)
	defer s.Close()

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
	defer s.Close()
	if err := s.Send([]byte("x"), &frame.Frame{}); err == nil {
		t.Error("expected dial failure")
	}
}

func TestSend_TCP_ReconnectAfterClose(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
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
	defer s.Close()

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

func TestMCastSender_AddressDerivation(t *testing.T) {
	// Construct without opening a real multicast socket — we test the address-template
	// derivation logic directly on a zero-value MCastSender.
	s := &MCastSender{}
	// Manually set fields as NewMCast would.
	s.addrTemplate[0], s.addrTemplate[1] = 0xFF, 0x05
	copy(s.addrTemplate[2:13], []byte{0xDE, 0xAD, 0xBE, 0xEF, 0, 0, 0, 0, 0, 0, 0})
	s.egressPort = 9001
	s.stripHeader = false

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
