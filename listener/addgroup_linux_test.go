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

	w := &Worker{id: 0, iface: lo, joinFd: fd, joinedGroups: map[netip.Addr]map[netip.Addr]struct{}{}}
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

// A leave that fails on the second source must not leave the first one gone
// while the record still holds the group: if the pilot then wants the group
// back, AddGroup would see it held and add nothing, and the first source would
// stay absent. The failed leave joins what it left again, and a returning
// pilot finds every source joined.
func TestWorker_RemoveGroupFailureKeepsMembership(t *testing.T) {
	lo := loopbackIface(t)
	fd, err := unix.Socket(unix.AF_INET6, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(fd) }()
	if err := unix.Bind(fd, &unix.SockaddrInet6{}); err != nil {
		t.Fatal(err)
	}
	group := netip.MustParseAddr("ff35::b:7e59")
	s1, s2 := netip.MustParseAddr("2001:db8::10"), netip.MustParseAddr("2001:db8::11")
	w := &Worker{id: 0, iface: lo, joinFd: fd, joinedGroups: map[netip.Addr]map[netip.Addr]struct{}{}}
	if err := w.AddGroup(group, []netip.Addr{s1, s2}); err != nil {
		t.Skipf("SSM join unavailable here: %v", err)
	}
	if !sourceJoined(t, lo, group, s1) || !sourceJoined(t, lo, group, s2) {
		t.Skip("the kernel does not list the (S,G) joins in /proc/net/mcfilter6")
	}

	real := leaveSource
	t.Cleanup(func() { leaveSource = real })
	leaveSource = func(fd, ifIndex int, g, src netip.Addr) error {
		if src == s2 {
			return unix.ENOMEM // the second leave fails
		}
		return real(fd, ifIndex, g, src)
	}
	if err := w.RemoveGroup(group, []netip.Addr{s1, s2}); err == nil {
		t.Fatal("RemoveGroup with a failing second leave succeeded")
	}
	leaveSource = real

	if err := w.AddGroup(group, []netip.Addr{s1, s2}); err != nil { // the pilot is back
		t.Fatalf("AddGroup after the failed leave: %v", err)
	}
	for _, src := range []netip.Addr{s1, s2} {
		if !sourceJoined(t, lo, group, src) {
			t.Errorf("(%s, %s) absent after the group was wanted back", src, group)
		}
	}
}
