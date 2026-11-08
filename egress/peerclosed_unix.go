//go:build unix

package egress

import (
	"errors"
	"net"
	"syscall"
)

// peerClosed reports whether the bridge has closed its end of c. A bridge never
// writes on a lane, so a non-blocking peek that finds end of stream or a reset
// means the connection is finished, and an object written into it would be lost.
// Bytes from the bridge are unexpected but leave the connection open. The peek
// reads the socket directly: a net.Conn read under an expired deadline returns a
// timeout without looking at the socket, so it cannot see a pending EOF.
func peerClosed(c net.Conn) bool {
	sc, ok := c.(syscall.Conn)
	if !ok {
		return false
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return true
	}
	closed := false
	var b [1]byte
	ctlErr := rc.Read(func(fd uintptr) bool {
		n, _, rerr := syscall.Recvfrom(int(fd), b[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
		switch {
		case rerr == nil:
			closed = n == 0 // 0 bytes without an error is the bridge's FIN
		case errors.Is(rerr, syscall.EAGAIN), errors.Is(rerr, syscall.EWOULDBLOCK), errors.Is(rerr, syscall.EINTR):
			closed = false // open, nothing to read
		default:
			closed = true // reset or another socket error
		}
		return true // never wait for readability
	})
	return closed || ctlErr != nil
}
