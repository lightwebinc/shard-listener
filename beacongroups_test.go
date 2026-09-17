package main

import (
	"net"
	"testing"

	"github.com/lightwebinc/shard-listener/config"
)

// At stock defaults shard-manifest announces on the beacon GROUP at port 9001
// while retry-endpoint ADVERTs arrive on 9300. A listener that opened only the
// ADVERT port never saw a manifest, so the manifest consumer could never reach
// quorum however many announcers were running.
func TestBeaconGroups_ManifestPortJoinedSeparately(t *testing.T) {
	ip := net.ParseIP("ff05::b")

	off := beaconGroups(&config.Config{BeaconPort: 9300, AutoConfigBeaconPort: 9001}, ip)
	if len(off) != 1 || off[0].Port != 9300 {
		t.Fatalf("manifest consumer off: groups = %v, want only the ADVERT port", off)
	}

	on := beaconGroups(&config.Config{
		BeaconPort:           9300,
		AutoConfigBeaconPort: 9001,
		AutoConfigEnabled:    true,
	}, ip)
	if len(on) != 2 {
		t.Fatalf("groups = %d, want 2 (ADVERT + manifest)", len(on))
	}
	if on[0].Port != 9300 || on[1].Port != 9001 {
		t.Errorf("ports = %d,%d, want 9300,9001", on[0].Port, on[1].Port)
	}
	for i, g := range on {
		if !g.IP.Equal(ip) {
			t.Errorf("group %d address = %s, want the beacon group %s", i, g.IP, ip)
		}
	}
}

// Equal ports means one socket: the receive loop demuxes ADVERT vs manifest on
// the MsgType byte, so a second bind to the same (group, port) would only
// fail.
func TestBeaconGroups_EqualPortsShareOneSocket(t *testing.T) {
	got := beaconGroups(&config.Config{
		BeaconPort:           9001,
		AutoConfigBeaconPort: 9001,
		AutoConfigEnabled:    true,
	}, net.ParseIP("ff05::b"))
	if len(got) != 1 {
		t.Fatalf("groups = %d, want 1 when the ports coincide", len(got))
	}
}
