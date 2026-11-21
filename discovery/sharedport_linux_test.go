//go:build linux

package discovery

import (
	"net"
	"testing"

	"golang.org/x/sys/unix"
)

// On Linux a co-resident retry endpoint shares the data port across users with
// SO_REUSEADDR alone. The control sockets must bind beside it in either order.
func TestControlSockets_SharePortWithReuseAddrSocket(t *testing.T) {
	lo := loopbackIface(t)
	t.Run("announce socket", func(t *testing.T) {
		port := freeUDP6Port(t)
		if err := bindLike(t, port, unix.SO_REUSEADDR); err != nil {
			t.Fatalf("retry-endpoint bind: %v", err)
		}
		fd, err := openAnnounceSocket(lo, &net.UDPAddr{IP: net.ParseIP("ff05::b:fffc"), Port: port}, nil)
		mustOpen(t, "announce socket", err)
		_ = unix.Close(fd)
	})
	for name := range beaconShapes(lo, 0) {
		t.Run("beacon socket/"+name, func(t *testing.T) {
			port := freeUDP6Port(t)
			sh := beaconShapes(lo, port)[name]
			if err := bindLike(t, port, unix.SO_REUSEADDR); err != nil {
				t.Fatalf("retry-endpoint bind: %v", err)
			}
			conn, err := sh.bl.openGroupConn(sh.grps)
			mustOpen(t, "beacon socket", err)
			_ = conn.Close()
		})
	}
}

// A socket sharing the data port receives, by default, multicast for every
// group joined on the host at that port: the beacon and announce sockets would
// be handed the whole data plane. Both must receive their own groups only.
func TestControlSockets_ReceiveOwnGroupsOnly(t *testing.T) {
	lo := loopbackIface(t)
	wantOwnOnly := func(t *testing.T, what string, v int, err error) {
		t.Helper()
		if err != nil {
			t.Skipf("IPV6_MULTICAST_ALL not supported here: %v", err)
		}
		if v != 0 {
			t.Errorf("%s IPV6_MULTICAST_ALL = %d, want 0 (own groups only)", what, v)
		}
	}

	t.Run("announce socket", func(t *testing.T) {
		port := freeUDP6Port(t)
		fd, err := openAnnounceSocket(lo, &net.UDPAddr{IP: net.ParseIP("ff05::b:fffc"), Port: port}, nil)
		mustOpen(t, "announce socket", err)
		defer func() { _ = unix.Close(fd) }()
		v, err := unix.GetsockoptInt(fd, unix.IPPROTO_IPV6, unix.IPV6_MULTICAST_ALL)
		wantOwnOnly(t, "announce socket", v, err)
	})
	for name := range beaconShapes(lo, 0) {
		t.Run("beacon socket/"+name, func(t *testing.T) {
			port := freeUDP6Port(t)
			sh := beaconShapes(lo, port)[name]
			conn, err := sh.bl.openGroupConn(sh.grps)
			mustOpen(t, "beacon socket", err)
			defer func() { _ = conn.Close() }()
			raw, err := conn.SyscallConn()
			if err != nil {
				t.Fatal(err)
			}
			var v int
			var optErr error
			if err := raw.Control(func(fd uintptr) {
				v, optErr = unix.GetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_MULTICAST_ALL)
			}); err != nil {
				t.Fatal(err)
			}
			wantOwnOnly(t, "beacon socket", v, optErr)
		})
	}
}
