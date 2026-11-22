//go:build linux

package listener

import (
	"bufio"
	"encoding/hex"
	"net"
	"net/netip"
	"os"
	"strings"
	"testing"

	"github.com/lightwebinc/shard-common/netjoin"
	"golang.org/x/sys/unix"
)

// sourceJoined reports whether the kernel holds an INCLUDE source filter for
// (group, source) on iface, from /proc/net/mcfilter6.
func sourceJoined(t *testing.T, iface *net.Interface, group, source netip.Addr) bool {
	t.Helper()
	f, err := os.Open("/proc/net/mcfilter6")
	if err != nil {
		t.Skipf("no /proc/net/mcfilter6: %v", err)
	}
	defer func() { _ = f.Close() }()
	g, s := group.As16(), source.As16()
	wantG, wantS := hex.EncodeToString(g[:]), hex.EncodeToString(s[:])
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		// Idx Device Multicast-Address Source-Address INC EXC
		fl := strings.Fields(sc.Text())
		if len(fl) >= 6 && fl[1] == iface.Name && fl[2] == wantG && fl[3] == wantS && fl[4] != "0" {
			return true
		}
	}
	return false
}

// A runtime join on a multi-source roster that fails on a later source must
// not keep the sources joined before it: the group is not recorded, so
// RemoveGroup would never leave them, and they would hold source-filter
// capacity for the life of the socket.
func TestWorker_AddGroupFailureLeavesNoMembership(t *testing.T) {
	lo := loopbackIface(t)
	fd, err := unix.Socket(unix.AF_INET6, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(fd) }()
	if err := unix.Bind(fd, &unix.SockaddrInet6{}); err != nil {
		t.Fatal(err)
	}
	group := netip.MustParseAddr("ff35::b:7e57")
	good := netip.MustParseAddr("2001:db8::10")

	// The host must support an (S,G) join, and show its join and leave.
	if err := netjoin.Join(fd, lo.Index, group, []netip.Addr{good}); err != nil {
		t.Skipf("SSM join unavailable here: %v", err)
	}
	if !sourceJoined(t, lo, group, good) {
		t.Skip("the kernel does not list the (S,G) join in /proc/net/mcfilter6")
	}
	if err := netjoin.Leave(fd, lo.Index, group, []netip.Addr{good}); err != nil {
		t.Fatal(err)
	}
	if sourceJoined(t, lo, group, good) {
		t.Skip("the kernel still lists a left source in /proc/net/mcfilter6")
	}

	w := &Worker{id: 0, iface: lo, joinFd: fd, joinedGroups: map[netip.Addr]struct{}{}}
	// netjoin rejects the second source, an IPv4 address, after the first joined.
	if err := w.AddGroup(group, []netip.Addr{good, netip.MustParseAddr("192.0.2.1")}); err == nil {
		t.Fatal("AddGroup with a non-IPv6 source succeeded")
	}
	if sourceJoined(t, lo, group, good) {
		t.Errorf("failed AddGroup left (%s, %s) joined, and RemoveGroup can never leave it", good, group)
	}
	if got := w.JoinedGroups(); len(got) != 0 {
		t.Errorf("failed AddGroup recorded groups %v", got)
	}
}
