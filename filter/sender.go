// Sender ACL: shared exclude→include CIDR filter applied to both the BRC-127
// announcement listener and the data-plane workers. A nil *SenderACL accepts
// every source — callers should keep the pointer nil when no CIDRs are
// configured to make the per-frame check a single nil compare.
package filter

import "net"

// SenderACL holds optional include/exclude IPv6/IPv4 CIDR lists. Exclude is
// evaluated first; if Include is empty, every non-excluded source is allowed.
type SenderACL struct {
	Include []*net.IPNet
	Exclude []*net.IPNet
}

// NewSenderACL returns nil when both lists are empty (so the caller can keep
// the pointer nil and skip the per-packet check entirely). Otherwise it
// returns a *SenderACL that copies the slices verbatim.
func NewSenderACL(include, exclude []*net.IPNet) *SenderACL {
	if len(include) == 0 && len(exclude) == 0 {
		return nil
	}
	return &SenderACL{Include: include, Exclude: exclude}
}

// Allow reports whether ip should be accepted. Exclude wins over Include.
// An empty Include list means "accept everything not excluded".
// A nil receiver accepts every source.
func (a *SenderACL) Allow(ip net.IP) bool {
	if a == nil {
		return true
	}
	for _, cidr := range a.Exclude {
		if cidr.Contains(ip) {
			return false
		}
	}
	if len(a.Include) == 0 {
		return true
	}
	for _, cidr := range a.Include {
		if cidr.Contains(ip) {
			return true
		}
	}
	return false
}
