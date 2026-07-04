package egress

import (
	"net"
	"testing"
	"time"
)

// A MultiSender must deliver every frame to ALL configured destinations — the
// receiver→delivery multi-destination fan-out.
func TestMultiSender_FansOutToAll(t *testing.T) {
	addr1, pc1, done1 := newUDPSink(t)
	defer done1()
	addr2, pc2, done2 := newUDPSink(t)
	defer done2()

	m, err := NewMulti([]string{addr1, addr2}, "udp")
	if err != nil {
		t.Fatalf("NewMulti: %v", err)
	}
	defer func() { _ = m.Close() }()
	if got := m.Dests(); got != 2 {
		t.Fatalf("Dests = %d, want 2", got)
	}

	payload := []byte("multi-dest-frame")
	if err := m.SendRaw(payload); err != nil {
		t.Fatalf("SendRaw: %v", err)
	}

	for i, pc := range []*net.UDPConn{pc1, pc2} {
		_ = pc.SetReadDeadline(time.Now().Add(time.Second))
		buf := make([]byte, len(payload))
		n, _, err := pc.ReadFrom(buf)
		if err != nil {
			t.Fatalf("dest %d: read: %v", i, err)
		}
		if string(buf[:n]) != string(payload) {
			t.Fatalf("dest %d: got %q, want %q", i, buf[:n], payload)
		}
	}
}

// Empty / whitespace-only address sets are rejected (a receiver with no delivery
// targets should fall back to the single egress at the call site, not silently
// build a no-op sink).
func TestMultiSender_RejectsEmpty(t *testing.T) {
	if _, err := NewMulti(nil, "udp"); err == nil {
		t.Fatal("NewMulti(nil) = nil error, want error")
	}
	if _, err := NewMulti([]string{"", "  "}, "udp"); err == nil {
		t.Fatal("NewMulti(blanks) = nil error, want error")
	}
}
