package filter_test

import (
	"net"
	"testing"

	"github.com/lightwebinc/bitcoin-shard-listener/filter"
)

func mustCIDR(t *testing.T, s string) *net.IPNet {
	t.Helper()
	_, n, err := net.ParseCIDR(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return n
}

func TestSenderACL_NilAllowsAll(t *testing.T) {
	var acl *filter.SenderACL
	if !acl.Allow(net.ParseIP("fd20::24")) {
		t.Error("nil ACL must accept every source")
	}
}

func TestSenderACL_EmptyListsReturnsNil(t *testing.T) {
	if filter.NewSenderACL(nil, nil) != nil {
		t.Error("NewSenderACL(nil, nil) should return nil so callers can skip the check")
	}
}

func TestSenderACL_ExcludeBlocks(t *testing.T) {
	acl := filter.NewSenderACL(nil, []*net.IPNet{mustCIDR(t, "fd20::/16")})
	if acl.Allow(net.ParseIP("fd20::24")) {
		t.Error("excluded sender must be denied")
	}
	if !acl.Allow(net.ParseIP("fe80::1")) {
		t.Error("non-excluded sender must be allowed when include is empty")
	}
}

func TestSenderACL_IncludeOnly(t *testing.T) {
	acl := filter.NewSenderACL([]*net.IPNet{mustCIDR(t, "fd20::/16")}, nil)
	if !acl.Allow(net.ParseIP("fd20::24")) {
		t.Error("included sender must be allowed")
	}
	if acl.Allow(net.ParseIP("fe80::1")) {
		t.Error("non-included sender must be denied")
	}
}

func TestSenderACL_ExcludeWinsOverInclude(t *testing.T) {
	acl := filter.NewSenderACL(
		[]*net.IPNet{mustCIDR(t, "fd20::/16")},
		[]*net.IPNet{mustCIDR(t, "fd20::24/128")},
	)
	if acl.Allow(net.ParseIP("fd20::24")) {
		t.Error("exclude must win over include")
	}
	if !acl.Allow(net.ParseIP("fd20::25")) {
		t.Error("non-excluded sender within include must still be allowed")
	}
}

func TestSenderACL_IPv4Mapped(t *testing.T) {
	// 10.0.0.0/8 expressed as IPv4 CIDR should still match an IPv4-mapped
	// IPv6 address arriving on a dual-stack socket.
	acl := filter.NewSenderACL([]*net.IPNet{mustCIDR(t, "10.0.0.0/8")}, nil)
	if !acl.Allow(net.ParseIP("10.1.2.3")) {
		t.Error("IPv4 source must match its IPv4 CIDR")
	}
	if !acl.Allow(net.ParseIP("::ffff:10.1.2.3")) {
		t.Error("IPv4-mapped IPv6 must match the IPv4 CIDR")
	}
}
