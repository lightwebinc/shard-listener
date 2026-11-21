package discovery

import (
	"context"
	"fmt"
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

// shareDataPort applies, before bind, the options a control-plane receiver
// needs to share its UDP port with the data plane. The BRC-139 manifest port
// and the BRC-127 announce port default to the data port, so the listener's
// worker sockets hold the same wildcard port, and a co-resident retry endpoint
// may hold it too.
//
// The options are the workers' own (listener openRawSocket): SO_REUSEPORT,
// which FreeBSD needs on every socket that shares a port and Linux uses for a
// same-user share, and SO_REUSEADDR, the Linux cross-user share. A socket with
// both binds beside anything a worker binds beside; one missing either fails
// whichever of the two binds comes second with EADDRINUSE.
//
// Sharing the port has a second effect on Linux: by default a socket receives
// multicast for every group joined on the host at its port, so a control socket
// beside the workers would be handed the whole data plane. ownGroupsOnly limits
// it to the groups it joined itself.
func shareDataPort(fd int) error {
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
		return fmt.Errorf("SO_REUSEPORT: %w", err)
	}
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		return fmt.Errorf("SO_REUSEADDR: %w", err)
	}
	return ownGroupsOnly(fd)
}

// listenSharedUDP6 binds the wildcard [::]:port with shareDataPort applied.
func listenSharedUDP6(port int) (*net.UDPConn, error) {
	lc := net.ListenConfig{Control: func(_, _ string, c syscall.RawConn) error {
		var optErr error
		if err := c.Control(func(fd uintptr) { optErr = shareDataPort(int(fd)) }); err != nil {
			return err
		}
		return optErr
	}}
	pc, err := lc.ListenPacket(context.Background(), "udp6", fmt.Sprintf("[::]:%d", port))
	if err != nil {
		return nil, err
	}
	uc, ok := pc.(*net.UDPConn)
	if !ok {
		_ = pc.Close()
		return nil, fmt.Errorf("unexpected conn type %T", pc)
	}
	return uc, nil
}
