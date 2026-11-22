package main

import (
	"bytes"
	"context"
	"log/slog"
	"net/netip"
	"slices"
	"strings"
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

// captureLogs routes the default logger into a buffer for the test.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
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

	src, err := buildSSMSources(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("buildSSMSources: %v", err)
	}
	publishers := addrs("2001:db8::10", "2001:db8::11")

	if got := src.groups[groupAddr(t, cfg, shard.GroupBlockBroadcast)]; !slices.Equal(got, publishers) {
		t.Errorf("block broadcast (0xFFFE) sources = %v, want the publishers %v", got, publishers)
	}
	for idx := shard.GroupIdx(0); idx < shard.GroupIdx(cfg.NumGroups); idx++ {
		if got := src.groups[groupAddr(t, cfg, idx)]; !slices.Equal(got, publishers) {
			t.Errorf("shard group %d sources = %v, want the publishers %v", idx, got, publishers)
		}
	}
	// The announcer list keeps its one job: the beacon group's manifest port.
	if want := addrs("2001:db8::50"); !slices.Equal(src.manifest, want) {
		t.Errorf("manifest sources = %v, want %v", src.manifest, want)
	}
}

// With no publisher list, block broadcast gets no source list at all (it is
// joined without a filter, like the data plane), not the announcers'.
func TestBuildSSMSources_BlockBroadcastIgnoresManifestList(t *testing.T) {
	logs := captureLogs(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := ssmTestConfig()
	cfg.SSMPublishersStatic = nil

	src, err := buildSSMSources(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("buildSSMSources: %v", err)
	}
	if got, ok := src.groups[groupAddr(t, cfg, shard.GroupBlockBroadcast)]; ok {
		t.Errorf("block broadcast (0xFFFE) sources = %v, want none without -ssm-publishers-static", got)
	}
	if !strings.Contains(logs.String(), "no publisher sources") {
		t.Errorf("startup log %q lacks the missing-publisher warning", logs.String())
	}
}

// A publisher list that resolves only to -local-source leaves no remote
// publisher. The groups keep the data plane's fallback (no source entry),
// and the warning names that cause instead of reporting a missing list.
func TestBuildSSMSources_LocalSourceOnlyPublishers(t *testing.T) {
	logs := captureLogs(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := ssmTestConfig()
	cfg.SSMPublishersStatic = []string{"2001:db8::10"}
	cfg.LocalSource = "2001:db8::10"

	src, err := buildSSMSources(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("buildSSMSources: %v", err)
	}
	for _, idx := range []shard.GroupIdx{shard.GroupBlockBroadcast, 0} {
		if got, ok := src.groups[groupAddr(t, cfg, idx)]; ok {
			t.Errorf("group %#x sources = %v, want none (own source only)", uint16(idx), got)
		}
	}
	out := logs.String()
	if !strings.Contains(out, "resolves only to -local-source") || strings.Contains(out, "no publisher sources") {
		t.Errorf("startup log %q must name the -local-source cause, not a missing list", out)
	}
}

// A collapsed node never (S,G)-joins its own source, on 0xFFFE as on the
// data plane.
func TestBuildSSMSources_BlockBroadcastExcludesOwnSource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := ssmTestConfig()
	cfg.LocalSource = "2001:db8::10"

	src, err := buildSSMSources(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("buildSSMSources: %v", err)
	}
	if got, want := src.groups[groupAddr(t, cfg, shard.GroupBlockBroadcast)], addrs("2001:db8::11"); !slices.Equal(got, want) {
		t.Errorf("block broadcast (0xFFFE) sources = %v, want %v (own source excluded)", got, want)
	}
	// The same set feeds runtime auto-join (autoJoin).
	if got, want := src.publishers, addrs("2001:db8::11"); !slices.Equal(got, want) {
		t.Errorf("publishers = %v, want %v (own source excluded)", got, want)
	}
}

// The proxies emit BRC-132 subtree data (0xFFFB) and BRC-127 subtree group
// announcements (0xFFFC). With no subtree-announce list of its own, both join
// the publisher set: left without sources they were joined (*,G), which
// PIM-SSM does not route, so neither class ever arrived.
func TestBuildSSMSources_SubtreeAnnounceDefaultsToPublishers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := ssmTestConfig() // no -ssm-bootstrap-subtree-announce
	cfg.LocalSource = "2001:db8::10"

	src, err := buildSSMSources(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("buildSSMSources: %v", err)
	}
	want := addrs("2001:db8::11") // the publishers, own source excluded
	for _, idx := range []shard.GroupIdx{shard.GroupSubtreeDataAnnounce, shard.GroupSubtreeGroupAnnounce} {
		if got := src.groups[groupAddr(t, cfg, idx)]; !slices.Equal(got, want) {
			t.Errorf("group %#x sources = %v, want the publishers %v", uint16(idx), got, want)
		}
	}
	// The BRC-127 announce listener joins 0xFFFC from this roster.
	if !slices.Equal(src.subtreeAnnounce, want) {
		t.Errorf("subtree-announce roster = %v, want the publishers %v", src.subtreeAnnounce, want)
	}
}

// An explicit subtree-announce list stays authoritative: it names every
// emitter, and the publisher set is not mixed in.
func TestBuildSSMSources_SubtreeAnnounceExplicitList(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := ssmTestConfig()
	cfg.SSMBootstrapSubtreeAnn = []string{"2001:db8::60"}

	src, err := buildSSMSources(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("buildSSMSources: %v", err)
	}
	want := addrs("2001:db8::60")
	for _, idx := range []shard.GroupIdx{shard.GroupSubtreeDataAnnounce, shard.GroupSubtreeGroupAnnounce} {
		if got := src.groups[groupAddr(t, cfg, idx)]; !slices.Equal(got, want) {
			t.Errorf("group %#x sources = %v, want the explicit list %v", uint16(idx), got, want)
		}
	}
	if !slices.Equal(src.subtreeAnnounce, want) {
		t.Errorf("subtree-announce roster = %v, want %v", src.subtreeAnnounce, want)
	}
}
