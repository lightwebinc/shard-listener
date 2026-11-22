package discovery

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// bindGroupSocket creates the UDP6 socket for one control-plane group and
// binds it to the group address itself, on the group's port.
//
// That port is usually the data port: the BRC-139 manifest port and the
// BRC-127 announce port default to it. The data-plane workers hold the port as
// [::]:port in one SO_REUSEPORT group, and a co-resident retry endpoint may
// hold it too. Bound to its group address rather than the wildcard, this
// socket is in no reuseport group with them (membership needs the identical
// bound address), so the kernel never balances unicast meant for the workers
// onto it, and it matches only datagrams sent to its own group: no unicast, no
// data-plane multicast. net.ListenPacket would rebind a multicast address as
// the wildcard, hence the raw socket.
//
// The bind still overlaps the workers' wildcard bind, so the socket carries
// their sharing options (listener openRawSocket): SO_REUSEPORT, which FreeBSD
// needs on every socket that shares a port, and SO_REUSEADDR, the Linux
// cross-user share. Without them the second of the two binds failed with
// EADDRINUSE, whichever came second.
//
// A link- or interface-local group takes the interface as its scope.
func bindGroupSocket(iface *net.Interface, grp *net.UDPAddr) (int, error) {
	ip := grp.IP.To16()
	if ip == nil || !ip.IsMulticast() {
		return -1, fmt.Errorf("bad group address %s", grp.IP)
	}
	fd, err := unix.Socket(unix.AF_INET6, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("socket: %w", err)
	}
	for _, opt := range []struct {
		name string
		opt  int
	}{{"SO_REUSEPORT", unix.SO_REUSEPORT}, {"SO_REUSEADDR", unix.SO_REUSEADDR}} {
		if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, opt.opt, 1); err != nil {
			_ = unix.Close(fd)
			return -1, fmt.Errorf("%s: %w", opt.name, err)
		}
	}
	sa := &unix.SockaddrInet6{Port: grp.Port}
	copy(sa.Addr[:], ip)
	if ip.IsLinkLocalMulticast() || ip.IsInterfaceLocalMulticast() {
		sa.ZoneId = uint32(iface.Index)
	}
	if err := unix.Bind(fd, sa); err != nil {
		_ = unix.Close(fd)
		return -1, fmt.Errorf("bind [%s]:%d: %w", grp.IP, grp.Port, err)
	}
	return fd, nil
}
