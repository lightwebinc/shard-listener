package discovery

import (
	"context"
	"net"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/lightwebinc/shard-listener/subtreegroup"
)

func TestSockaddrToUDP_IPv6(t *testing.T) {
	sa := &unix.SockaddrInet6{}
	copy(sa.Addr[:], net.ParseIP("fd20::24").To16())
	got := sockaddrToUDP(sa)
	if !got.IP.Equal(net.ParseIP("fd20::24")) {
		t.Errorf("got %v", got.IP)
	}
}

func TestSockaddrToUDP_Other(t *testing.T) {
	// Non-IPv6 sockaddr (e.g. IPv4) → empty UDPAddr
	got := sockaddrToUDP(&unix.SockaddrInet4{})
	if got == nil {
		t.Fatal("nil")
		return
	}
	if got.IP != nil {
		t.Errorf("expected nil IP, got %v", got.IP)
	}
}

func mustCIDR(t *testing.T, s string) *net.IPNet {
	t.Helper()
	_, n, err := net.ParseCIDR(s)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestSenderAllowed_NoFilter(t *testing.T) {
	sl := &SubtreeAnnounceListener{}
	if !sl.senderAllowed(&net.UDPAddr{IP: net.ParseIP("fd20::24")}) {
		t.Error("empty filter should allow all")
	}
}

func TestSenderAllowed_ExcludeBlocks(t *testing.T) {
	sl := &SubtreeAnnounceListener{
		SenderExclude: []*net.IPNet{mustCIDR(t, "fd20::/16")},
	}
	if sl.senderAllowed(&net.UDPAddr{IP: net.ParseIP("fd20::24")}) {
		t.Error("excluded sender should be denied")
	}
	if !sl.senderAllowed(&net.UDPAddr{IP: net.ParseIP("fe80::1")}) {
		t.Error("non-excluded sender should be allowed when include is empty")
	}
}

func TestSenderAllowed_IncludeOnly(t *testing.T) {
	sl := &SubtreeAnnounceListener{
		SenderInclude: []*net.IPNet{mustCIDR(t, "fd20::/16")},
	}
	if !sl.senderAllowed(&net.UDPAddr{IP: net.ParseIP("fd20::24")}) {
		t.Error("included sender should be allowed")
	}
	if sl.senderAllowed(&net.UDPAddr{IP: net.ParseIP("fe80::1")}) {
		t.Error("non-included sender should be denied")
	}
}

func TestSenderAllowed_ExcludeOverridesInclude(t *testing.T) {
	sl := &SubtreeAnnounceListener{
		SenderInclude: []*net.IPNet{mustCIDR(t, "fd20::/16")},
		SenderExclude: []*net.IPNet{mustCIDR(t, "fd20::24/128")},
	}
	if sl.senderAllowed(&net.UDPAddr{IP: net.ParseIP("fd20::24")}) {
		t.Error("exclude should win over include")
	}
	if !sl.senderAllowed(&net.UDPAddr{IP: net.ParseIP("fd20::25")}) {
		t.Error("non-excluded sender within include should still be allowed")
	}
}

func TestEvictLoop_RunsAndStops(t *testing.T) {
	reg := subtreegroup.New(nil, 0)
	sl := &SubtreeAnnounceListener{Registry: reg}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { sl.evictLoop(ctx); close(done) }()
	// Give the ticker enough time to fire at least once.
	time.Sleep(1100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("evictLoop did not exit on ctx cancel")
	}
}
