//go:build !linux

package discovery

// ownGroupsOnly is a no-op off Linux. FreeBSD has no IPV6_MULTICAST_ALL: once
// a socket has joined a group it receives multicast only for its own groups.
func ownGroupsOnly(int) error { return nil }
