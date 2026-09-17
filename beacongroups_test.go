package main

import (
	"net"
	"testing"

	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/shard-listener/config"
)

// At stock defaults shard-manifest announces on the beacon GROUP at port 9001
// while retry-endpoint ADVERTs arrive on 9300. A listener that opened only the
// ADVERT port never saw a manifest, so the manifest consumer could never reach
// quorum however many announcers were running.
func TestBeaconGroups_ManifestPortJoinedSeparately(t *testing.T) {
	ip := net.ParseIP("ff05::b")

	off := beaconGroups(&config.Config{BeaconPort: 9300, AutoConfigBeaconPort: 9001}, []net.IP{ip})
	if len(off) != 1 || off[0].Port != 9300 {
		t.Fatalf("manifest consumer off: groups = %v, want only the ADVERT port", off)
	}

	on := beaconGroups(&config.Config{
		BeaconPort:           9300,
		AutoConfigBeaconPort: 9001,
		AutoConfigEnabled:    true,
	}, []net.IP{ip})
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
	}, []net.IP{net.ParseIP("ff05::b")})
	if len(got) != 1 {
		t.Fatalf("groups = %d, want 1 when the ports coincide", len(got))
	}
}

// -control-group-compat=both makes each port carry BOTH the any-source and
// the source-specific form of the beacon group, so an upgraded listener
// hears a retry endpoint on either side of the flag day. Entries sharing a
// port share one socket (BeaconListener.Start buckets them), so this is 4
// joins on 2 sockets, not 4 binds.
func TestBeaconGroups_DualPrefixDuringTransition(t *testing.T) {
	asm := net.ParseIP("ff05::b:fffd")
	ssm := net.ParseIP("ff35::b:fffd")

	got := beaconGroups(&config.Config{
		BeaconPort:           9300,
		AutoConfigBeaconPort: 9001,
		AutoConfigEnabled:    true,
	}, []net.IP{asm, ssm})
	if len(got) != 4 {
		t.Fatalf("groups = %d, want 4 (2 prefixes x 2 ports)", len(got))
	}
	want := []struct {
		ip   net.IP
		port int
	}{{asm, 9300}, {ssm, 9300}, {asm, 9001}, {ssm, 9001}}
	for i, w := range want {
		if !got[i].IP.Equal(w.ip) || got[i].Port != w.port {
			t.Errorf("group %d = [%s]:%d, want [%s]:%d", i, got[i].IP, got[i].Port, w.ip, w.port)
		}
	}
}

// THE DEFECT, end to end through the code path main.go runs: -source-mode
// ssm must put the beacon socket on the source-specific group BRC-126 and
// BRC-129 name (FF35::B:FFFD at site scope), not the any-source FF05 one
// this always used to derive.
func TestBeaconGroups_SSMDerivesSourceSpecificAddress(t *testing.T) {
	cfg := &config.Config{
		BeaconPort:         9300,
		MCGroupID:          shard.DefaultGroupID,
		SourceMode:         "ssm",
		BeaconScope:        "site",
		ControlGroupCompat: config.ControlGroupDerived,
	}
	prefixes, err := cfg.BeaconGroupPrefixes()
	if err != nil {
		t.Fatal(err)
	}
	ips := make([]net.IP, 0, len(prefixes))
	for _, p := range prefixes {
		ips = append(ips, shard.GroupAddr(p, cfg.MCGroupID, shard.GroupBeacon))
	}

	got := beaconGroups(cfg, ips)
	if len(got) != 1 {
		t.Fatalf("groups = %d, want 1", len(got))
	}
	want := net.ParseIP("ff35::b:fffd")
	if !got[0].IP.Equal(want) {
		t.Errorf("beacon join address = %s, want %s", got[0].IP, want)
	}
}

// The shipped receiver default spans the flag day: an upgraded listener
// joins the group un-upgraded retry endpoints still advertise into as well
// as the conformant one, so rolling listeners first can never go deaf.
func TestBeaconGroups_ReceiverDefaultJoinsBoth(t *testing.T) {
	cfg := &config.Config{
		BeaconPort:         9300,
		MCGroupID:          shard.DefaultGroupID,
		SourceMode:         "ssm",
		BeaconScope:        "site",
		ControlGroupCompat: config.ControlGroupBoth,
	}
	prefixes, err := cfg.BeaconGroupPrefixes()
	if err != nil {
		t.Fatal(err)
	}
	if len(prefixes) != 2 || prefixes[0] != 0xFF05 || prefixes[1] != 0xFF35 {
		t.Fatalf("prefixes = %#04x, want [0xff05 0xff35]", prefixes)
	}
}
