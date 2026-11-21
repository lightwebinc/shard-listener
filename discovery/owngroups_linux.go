//go:build linux

package discovery

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// ownGroupsOnly turns IPV6_MULTICAST_ALL off, so the socket receives multicast
// only for the groups it joined, not for every group another socket on the
// host joined at the same port. A kernel without the option (before 4.20)
// keeps the old delivery; the socket still works.
func ownGroupsOnly(fd int) error {
	err := unix.SetsockoptInt(fd, unix.IPPROTO_IPV6, unix.IPV6_MULTICAST_ALL, 0)
	if err != nil && !errors.Is(err, unix.ENOPROTOOPT) {
		return fmt.Errorf("IPV6_MULTICAST_ALL: %w", err)
	}
	return nil
}
