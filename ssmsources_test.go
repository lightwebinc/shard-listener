package main

import (
	"context"
	"net/netip"
	"slices"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/shard-listener/config"
)

// ssmTestConfig is an SSM site-scope config with two publishers (the proxies)
// and a manifest announcer on a different address.
func ssmTestConfig() *config.Config {
	return &config.Config{
		MCPrefix:             0xFF35,
		MCGroupID:            shard.DefaultGroupID,
		NumGroups:            4,
		SSMBootstrapManifest: []string{"2001:db8::50"},
		SSMPublishersStatic:  []string{"2001:db8::10", "2001:db8::11"},
		SSMBootstrapRefresh:  time.Minute,
	}
}

func groupAddr(t *testing.T, cfg *config.Config, idx shard.GroupIdx) netip.Addr {
	t.Helper()
	ga, ok := netip.AddrFromSlice(shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, idx).To16())
	if !ok {
		t.Fatalf("group %#x: not an IPv6 address", uint16(idx))
	}
	return ga
}

func addrs(ss ...string) []netip.Addr {
	out := make([]netip.Addr, 0, len(ss))
	for _, s := range ss {
		out = append(out, netip.MustParseAddr(s))
	}
	return out
}

// BRC-133: block-broadcast frames share the emitting proxy's source, so
// 0xFFFE is (S,G)-joined against the publisher set, the same set as the
// data-plane groups, never against the manifest announcers (which never
// emit there: an (S,G) join on them filters every block frame out).
func TestBuildSSMSources_BlockBroadcastJoinsPublishers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := ssmTestConfig()

	gs, _, manifestSrcs, _, err := buildSSMSources(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("buildSSMSources: %v", err)
	}
	publishers := addrs("2001:db8::10", "2001:db8::11")

	if got := gs[groupAddr(t, cfg, shard.GroupBlockBroadcast)]; !slices.Equal(got, publishers) {
		t.Errorf("block broadcast (0xFFFE) sources = %v, want the publishers %v", got, publishers)
	}
	for idx := shard.GroupIdx(0); idx < shard.GroupIdx(cfg.NumGroups); idx++ {
		if got := gs[groupAddr(t, cfg, idx)]; !slices.Equal(got, publishers) {
			t.Errorf("shard group %d sources = %v, want the publishers %v", idx, got, publishers)
		}
	}
	// The announcer list keeps its one job: the beacon group's manifest port.
	if want := addrs("2001:db8::50"); !slices.Equal(manifestSrcs, want) {
		t.Errorf("manifest sources = %v, want %v", manifestSrcs, want)
	}
}

// With no publisher list, block broadcast gets no source list at all (it is
// joined without a filter, like the data plane), not the announcers'.
func TestBuildSSMSources_BlockBroadcastIgnoresManifestList(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := ssmTestConfig()
	cfg.SSMPublishersStatic = nil

	gs, _, _, _, err := buildSSMSources(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("buildSSMSources: %v", err)
	}
	if got, ok := gs[groupAddr(t, cfg, shard.GroupBlockBroadcast)]; ok {
		t.Errorf("block broadcast (0xFFFE) sources = %v, want none without -ssm-publishers-static", got)
	}
}

// A collapsed node never (S,G)-joins its own source, on 0xFFFE as on the
// data plane.
func TestBuildSSMSources_BlockBroadcastExcludesOwnSource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := ssmTestConfig()
	cfg.LocalSource = "2001:db8::10"

	gs, _, _, _, err := buildSSMSources(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("buildSSMSources: %v", err)
	}
	if got, want := gs[groupAddr(t, cfg, shard.GroupBlockBroadcast)], addrs("2001:db8::11"); !slices.Equal(got, want) {
		t.Errorf("block broadcast (0xFFFE) sources = %v, want %v (own source excluded)", got, want)
	}
}
