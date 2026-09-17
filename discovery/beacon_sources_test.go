package discovery

import (
	"net"
	"net/netip"
	"testing"
)

// The ADVERT port and the BRC-139 manifest port share the beacon GROUP but are
// published by different senders, so under SSM each must join its own (S,G)
// roster — a single shared list would filter one sender out completely.
func TestBeaconListener_SourcesForPerPort(t *testing.T) {
	advertSrc := netip.MustParseAddr("fd00::1")
	manifestSrc := netip.MustParseAddr("fd00::2")
	bl := &BeaconListener{
		Sources:       []netip.Addr{advertSrc},
		SourcesByPort: map[int][]netip.Addr{9001: {manifestSrc}},
	}
	ip := net.ParseIP("ff05::b")

	if got := bl.sourcesFor(&net.UDPAddr{IP: ip, Port: 9300}); len(got) != 1 || got[0] != advertSrc {
		t.Errorf("ADVERT port sources = %v, want %v", got, advertSrc)
	}
	if got := bl.sourcesFor(&net.UDPAddr{IP: ip, Port: 9001}); len(got) != 1 || got[0] != manifestSrc {
		t.Errorf("manifest port sources = %v, want %v", got, manifestSrc)
	}

	// No override configured (the ASM case) keeps the shared list.
	bl.SourcesByPort = nil
	if got := bl.sourcesFor(&net.UDPAddr{IP: ip, Port: 9001}); len(got) != 1 || got[0] != advertSrc {
		t.Errorf("fallback sources = %v, want %v", got, advertSrc)
	}
}
