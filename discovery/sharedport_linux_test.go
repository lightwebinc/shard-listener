//go:build linux

package discovery

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/lightwebinc/shard-listener/subtreegroup"
)

// The BRC-139 manifest port and the BRC-127 announce port default to the data
// port (9001), so the beacon and announce sockets share that port with the
// data-plane worker sockets ([::]:port with SO_REUSEPORT and SO_REUSEADDR,
// listener openRawSocket). Every pair must bind in either order: a failed
// second bind stops every worker, or the beacon listener and ADVERT discovery
// with it. And no control socket may take unicast meant for the workers. The
// tests drive the listeners through Start, as main does.

// workerOpts are the data-plane worker socket's options.
var workerOpts = []int{unix.SO_REUSEPORT, unix.SO_REUSEADDR}

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

// bindLike binds a raw UDP6 socket to [::]:port with the given options,
// standing in for another holder of the port: a data-plane worker
// (workerOpts) or a co-resident retry endpoint (SO_REUSEADDR alone, the Linux
// cross-user share). The socket is closed when the test ends.
func bindLike(t *testing.T, port int, opts ...int) (int, error) {
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
		return -1, err
	}
	t.Cleanup(func() { _ = unix.Close(fd) })
	return fd, nil
}

// boundOn counts the UDP6 sockets bound to port.
func boundOn(t *testing.T, port int) int {
	t.Helper()
	f, err := os.Open("/proc/net/udp6")
	if err != nil {
		t.Skipf("no /proc/net/udp6: %v", err)
	}
	defer func() { _ = f.Close() }()
	suffix := fmt.Sprintf(":%04X", port)
	n := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if fields := strings.Fields(sc.Text()); len(fields) > 1 && strings.HasSuffix(fields[1], suffix) {
			n++
		}
	}
	return n
}

// started runs a control listener's Start until the test ends and waits
// until at least want sockets hold the port: every socket already there plus
// every one this listener opens (one per group address). A bind collision fails the test,
// which is the defect; any other startup error (a host that cannot join a
// multicast group at all) skips it.
func started(t *testing.T, port, want int, start func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() { errs <- start(ctx) }()
	t.Cleanup(func() {
		cancel()
		// Wait for the listener to exit, which closes its sockets.
		if err := <-errs; err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("control listener exited: %v", err)
		}
	})
	check := func() {
		select {
		case err := <-errs:
			errs <- err // for the cleanup
			if errors.Is(err, syscall.EADDRINUSE) {
				t.Fatalf("control socket cannot share the port: %v", err)
			}
			t.Skipf("control listener did not start here: %v", err)
		default:
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for boundOn(t, port) < want {
		check()
		if time.Now().After(deadline) {
			t.Fatalf("%d sockets on port %d after 3s, want %d", boundOn(t, port), port, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond) // any later socket of the same listener
	check()
}

// beaconShapes are the ways the beacon listener holds a port: one any-source
// group, the FF0x/FF3x pair of -control-group-compat=both, and an SSM roster.
func beaconShapes(lo *net.Interface, port int) map[string]*BeaconListener {
	asm := &net.UDPAddr{IP: net.ParseIP("ff05::b:fffd"), Port: port}
	ssm := &net.UDPAddr{IP: net.ParseIP("ff35::b:fffd"), Port: port}
	return map[string]*BeaconListener{
		"any-source group": {Registry: NewRegistry(), Iface: lo, Groups: []*net.UDPAddr{asm}},
		"compat pair":      {Registry: NewRegistry(), Iface: lo, Groups: []*net.UDPAddr{asm, ssm}},
		"ssm roster": {Registry: NewRegistry(), Iface: lo, Groups: []*net.UDPAddr{ssm},
			Sources: []netip.Addr{netip.MustParseAddr("2001:db8::10")}},
	}
}

func announceListener(lo *net.Interface, port int) *SubtreeGroupAnnounceListener {
	return &SubtreeGroupAnnounceListener{
		Registry: subtreegroup.New(nil, 0),
		Iface:    lo,
		Groups:   []*net.UDPAddr{{IP: net.ParseIP("ff05::b:fffc"), Port: port}},
	}
}

func TestBeaconListener_SharesPortWithDataPlaneWorker(t *testing.T) {
	lo := loopbackIface(t)
	for name := range beaconShapes(lo, 0) {
		t.Run(name+"/worker first", func(t *testing.T) {
			port := freeUDP6Port(t)
			bl := beaconShapes(lo, port)[name]
			if _, err := bindLike(t, port, workerOpts...); err != nil {
				t.Fatalf("worker bind: %v", err)
			}
			started(t, port, 1+len(bl.Groups), bl.Start)
		})
		t.Run(name+"/beacon first", func(t *testing.T) {
			port := freeUDP6Port(t)
			bl := beaconShapes(lo, port)[name]
			started(t, port, len(bl.Groups), bl.Start)
			if _, err := bindLike(t, port, workerOpts...); err != nil {
				t.Fatalf("data-plane worker cannot bind beside the beacon socket: %v", err)
			}
		})
	}
}

func TestControlListeners_ShareOnePort(t *testing.T) {
	lo := loopbackIface(t)
	for name := range beaconShapes(lo, 0) {
		t.Run(name+"/announce first", func(t *testing.T) {
			port := freeUDP6Port(t)
			bl := beaconShapes(lo, port)[name]
			started(t, port, 1, announceListener(lo, port).Start)
			started(t, port, 1+len(bl.Groups), bl.Start)
		})
		t.Run(name+"/beacon first", func(t *testing.T) {
			port := freeUDP6Port(t)
			bl := beaconShapes(lo, port)[name]
			started(t, port, len(bl.Groups), bl.Start)
			started(t, port, len(bl.Groups)+1, announceListener(lo, port).Start)
		})
	}
}

func TestControlListeners_SharePortWithReuseAddrSocket(t *testing.T) {
	lo := loopbackIface(t)
	for name := range beaconShapes(lo, 0) {
		t.Run(name, func(t *testing.T) {
			port := freeUDP6Port(t)
			if _, err := bindLike(t, port, unix.SO_REUSEADDR); err != nil {
				t.Fatalf("retry-endpoint bind: %v", err)
			}
			bl := beaconShapes(lo, port)[name]
			started(t, port, 2, announceListener(lo, port).Start)
			started(t, port, 2+len(bl.Groups), bl.Start)
		})
	}
}

// Unicast to the data port belongs to the workers. Linux balances it across
// the SO_REUSEPORT group holding the port, so a control socket in that group
// would take a share of it and drop it. Every datagram, each from its own
// source port, must reach the worker.
func TestControlListeners_DoNotTakeWorkerUnicast(t *testing.T) {
	lo := loopbackIface(t)
	port := freeUDP6Port(t)
	worker, err := bindLike(t, port, workerOpts...)
	if err != nil {
		t.Fatalf("worker bind: %v", err)
	}
	started(t, port, 3, beaconShapes(lo, port)["compat pair"].Start) // worker + one socket per beacon group
	started(t, port, 4, announceListener(lo, port).Start)            // + the announce socket

	const n = 64
	dst := &net.UDPAddr{IP: net.IPv6loopback, Port: port}
	for i := range n {
		c, err := net.DialUDP("udp6", nil, dst) // a fresh source port per datagram
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Write([]byte{0xE3, 0xE1, 0xF3, 0xE8, byte(i)}); err != nil {
			t.Fatal(err)
		}
		_ = c.Close()
	}

	tv := unix.NsecToTimeval((500 * time.Millisecond).Nanoseconds())
	if err := unix.SetsockoptTimeval(worker, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv); err != nil {
		t.Fatal(err)
	}
	got := 0
	buf := make([]byte, 64)
	for got < n {
		if _, _, err := unix.Recvfrom(worker, buf, 0); err != nil {
			break
		}
		got++
	}
	if got != n {
		t.Fatalf("worker received %d of %d unicast datagrams: a control socket on the port took the rest", got, n)
	}
}
