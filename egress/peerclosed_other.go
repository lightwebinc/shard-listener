//go:build !unix

package egress

import "net"

// peerClosed cannot peek the socket on this platform; a closed bridge is found
// by the next write's error instead.
func peerClosed(net.Conn) bool { return false }
