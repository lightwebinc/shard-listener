package discovery

import (
	"errors"
	"net"
	"net/netip"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// The BRC-139 manifest port and the BRC-127 announce port default to the data
// port (9001), so the beacon listener's manifest socket, the announce socket
// and the data-plane worker sockets all bind one wildcard port. Each pair must
// bind in either order: the second bind failing with EADDRINUSE stops every
// worker, or the beacon listener (and with it ADVERT discovery).

// workerOpts are the data-plane worker socket's options (listener openRawSocket).
var workerOpts = []int{unix.SO_REUSEPORT, unix.SO_REUSEADDR}

// freeUDP6Port returns a UDP port nothing is bound to right now.
func freeUDP6Port(t *testing.T) int {
	t.Helper()
	pc, err := net.ListenPacket("udp6", "[::]:0")
	if err != nil {
		t.Skipf("no IPv6 UDP here: %v", err)
	}
	port := pc.LocalAddr().(*net.UDPAddr).Port
	_ = pc.Close()
	return port
}

// bindLike binds a raw UDP6 socket to [::]:port with the given SOL_SOCKET
// options, standing in for another receiver of the port. The socket is closed
// at the end of the test.
func bindLike(t *testing.T, port int, opts ...int) error {
	t.Helper()
	fd, err := unix.Socket(unix.AF_INET6, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range opts {
		if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, o, 1); err != nil {
			_ = unix.Close(fd)
			t.Fatal(err)
		}
	}
	if err := unix.Bind(fd, &unix.SockaddrInet6{Port: port}); err != nil {
		_ = unix.Close(fd)
		return err
	}
	t.Cleanup(func() { _ = unix.Close(fd) })
	return nil
}

func loopbackIface(t *testing.T) *net.Interface {
	t.Helper()
	ifs, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range ifs {
		if i.Flags&net.FlagLoopback != 0 {
			return &i
		}
	}
	t.Skip("no loopback interface")
	return nil
}

// mustOpen fails the test on a bind collision, the defect, and skips when the
// host cannot join a multicast group at all, a property of the environment.
func mustOpen(t *testing.T, what string, err error) {
	t.Helper()
	if errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("%s cannot share the port: %v", what, err)
	}
	if err != nil {
		t.Skipf("multicast join unavailable here: %v", err)
	}
}

// beaconShapes are the three ways the beacon listener opens a port: one
// any-source group, the FF0x/FF3x pair of -control-group-compat=both, and an
// SSM roster.
func beaconShapes(lo *net.Interface, port int) map[string]struct {
	bl   *BeaconListener
	grps []*net.UDPAddr
} {
	asm := &net.UDPAddr{IP: net.ParseIP("ff05::b:fffd"), Port: port}
	ssm := &net.UDPAddr{IP: net.ParseIP("ff35::b:fffd"), Port: port}
	return map[string]struct {
		bl   *BeaconListener
		grps []*net.UDPAddr
	}{
		"any-source group": {&BeaconListener{Iface: lo}, []*net.UDPAddr{asm}},
		"compat pair":      {&BeaconListener{Iface: lo}, []*net.UDPAddr{asm, ssm}},
		"ssm roster": {
			&BeaconListener{Iface: lo, Sources: []netip.Addr{netip.MustParseAddr("2001:db8::10")}},
			[]*net.UDPAddr{ssm},
		},
	}
}

func TestOpenGroupConn_SharesPortWithDataPlaneWorker(t *testing.T) {
	lo := loopbackIface(t)
	for name := range beaconShapes(lo, 0) {
		t.Run(name+"/worker first", func(t *testing.T) {
			port := freeUDP6Port(t)
			sh := beaconShapes(lo, port)[name]
			if err := bindLike(t, port, workerOpts...); err != nil {
				t.Fatalf("worker bind: %v", err)
			}
			conn, err := sh.bl.openGroupConn(sh.grps)
			mustOpen(t, "beacon socket", err)
			_ = conn.Close()
		})
		t.Run(name+"/beacon first", func(t *testing.T) {
			port := freeUDP6Port(t)
			sh := beaconShapes(lo, port)[name]
			conn, err := sh.bl.openGroupConn(sh.grps)
			mustOpen(t, "beacon socket", err)
			defer func() { _ = conn.Close() }()
			if err := bindLike(t, port, workerOpts...); err != nil {
				t.Fatalf("data-plane worker cannot bind beside the beacon socket: %v", err)
			}
		})
	}
}

func TestOpenGroupConn_SharesPortWithAnnounceSocket(t *testing.T) {
	lo := loopbackIface(t)
	announce := func(port int) *net.UDPAddr {
		return &net.UDPAddr{IP: net.ParseIP("ff05::b:fffc"), Port: port}
	}
	for name := range beaconShapes(lo, 0) {
		t.Run(name+"/announce first", func(t *testing.T) {
			port := freeUDP6Port(t)
			sh := beaconShapes(lo, port)[name]
			fd, err := openAnnounceSocket(lo, announce(port), nil)
			mustOpen(t, "announce socket", err)
			defer func() { _ = unix.Close(fd) }()
			conn, err := sh.bl.openGroupConn(sh.grps)
			mustOpen(t, "beacon socket", err)
			_ = conn.Close()
		})
		t.Run(name+"/beacon first", func(t *testing.T) {
			port := freeUDP6Port(t)
			sh := beaconShapes(lo, port)[name]
			conn, err := sh.bl.openGroupConn(sh.grps)
			mustOpen(t, "beacon socket", err)
			defer func() { _ = conn.Close() }()
			fd, err := openAnnounceSocket(lo, announce(port), nil)
			mustOpen(t, "announce socket", err)
			_ = unix.Close(fd)
		})
	}
}
