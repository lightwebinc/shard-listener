package config

import (
	"net"
	"strings"
	"testing"
	"time"
)

func TestSplitComma(t *testing.T) {
	if got := splitComma(""); got != nil {
		t.Fatalf("empty: want nil, got %v", got)
	}
	got := splitComma("a, b,c ,  d")
	want := []string{"a", "b", "c", "d"}
	if len(got) != len(want) {
		t.Fatalf("len mismatch: %v vs %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("[%d] %q != %q", i, got[i], want[i])
		}
	}
}

func TestEnvStr(t *testing.T) {
	t.Setenv("CFG_X", "value")
	if got := envStr("CFG_X", "def"); got != "value" {
		t.Errorf("envStr: got %q", got)
	}
	t.Setenv("CFG_X", "")
	if got := envStr("CFG_X", "def"); got != "def" {
		t.Errorf("default not used: %q", got)
	}
}

func TestEnvInt(t *testing.T) {
	t.Setenv("CFG_N", "42")
	if got := envInt("CFG_N", 7); got != 42 {
		t.Errorf("got %d", got)
	}
	t.Setenv("CFG_N", "notanumber")
	if got := envInt("CFG_N", 7); got != 7 {
		t.Errorf("invalid should fall back to default: %d", got)
	}
}

func TestEnvBool(t *testing.T) {
	t.Setenv("CFG_B", "true")
	if !envBool("CFG_B", false) {
		t.Error("envBool true")
	}
	t.Setenv("CFG_B", "garbage")
	if envBool("CFG_B", false) {
		t.Error("invalid should fall back to default")
	}
}

func TestEnvDuration(t *testing.T) {
	t.Setenv("CFG_D", "250ms")
	if got := envDuration("CFG_D", time.Second); got != 250*time.Millisecond {
		t.Errorf("got %v", got)
	}
	t.Setenv("CFG_D", "bad")
	if got := envDuration("CFG_D", time.Second); got != time.Second {
		t.Errorf("invalid should fall back to default: %v", got)
	}
}

func TestParseSubtreeList(t *testing.T) {
	ids, err := parseSubtreeList("")
	if err != nil || ids != nil {
		t.Fatalf("empty: got %v err=%v", ids, err)
	}
	hexA := strings.Repeat("aa", 32)
	hexB := strings.Repeat("bb", 32)
	ids, err = parseSubtreeList(hexA + ",0x" + hexB)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("len=%d", len(ids))
	}
	if ids[0] != [32]byte{0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa} {
		t.Errorf("first id mismatch")
	}
	if _, err := parseSubtreeList("zzzz"); err == nil {
		t.Error("expected error on bad hex")
	}
	if _, err := parseSubtreeList("aabb"); err == nil {
		t.Error("expected error on wrong length")
	}
}

func TestParseGroupList(t *testing.T) {
	if g, err := parseGroupList(""); err != nil || g != nil {
		t.Fatalf("empty: %v %v", g, err)
	}
	hexA := strings.Repeat("11", 16)
	g, err := parseGroupList(hexA)
	if err != nil {
		t.Fatal(err)
	}
	if len(g) != 1 || g[0][0] != 0x11 || g[0][15] != 0x11 {
		t.Errorf("unexpected: %v", g)
	}
	if _, err := parseGroupList("badhex"); err == nil {
		t.Error("expected length error")
	}
}

func TestParseIPNetList(t *testing.T) {
	if l, err := parseIPNetList(""); err != nil || l != nil {
		t.Fatalf("empty: %v %v", l, err)
	}
	got, err := parseIPNetList("fd20::24, fd20::/16, 10.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("len=%d", len(got))
	}
	// Plain IPv6 -> /128
	ones, bits := got[0].Mask.Size()
	if ones != 128 || bits != 128 {
		t.Errorf("ipv6 mask: %d/%d", ones, bits)
	}
	// CIDR -> /16
	ones, _ = got[1].Mask.Size()
	if ones != 16 {
		t.Errorf("cidr mask: %d", ones)
	}
	// Plain IPv4 -> /32
	ones, bits = got[2].Mask.Size()
	if ones != 32 || bits != 32 {
		t.Errorf("ipv4 mask: %d/%d", ones, bits)
	}
	if _, err := parseIPNetList("not-an-ip"); err == nil {
		t.Error("expected error")
	}
	if _, err := parseIPNetList("10.0.0.0/zzz"); err == nil {
		t.Error("expected CIDR error")
	}
}

func TestParseIPNetListContains(t *testing.T) {
	nets, err := parseIPNetList("fd20::/16")
	if err != nil {
		t.Fatal(err)
	}
	if !nets[0].Contains(net.ParseIP("fd20::24")) {
		t.Error("contains failed")
	}
	if nets[0].Contains(net.ParseIP("fe80::1")) {
		t.Error("should not contain")
	}
}

func TestScopesMap(t *testing.T) {
	for _, name := range []string{"link", "site", "org", "global"} {
		if _, ok := Scopes[name]; !ok {
			t.Errorf("missing scope %s", name)
		}
	}
}

func TestDefaultSubtreeGroupTTL(t *testing.T) {
	if DefaultSubtreeGroupTTL != 900*time.Second {
		t.Errorf("DefaultSubtreeGroupTTL = %v", DefaultSubtreeGroupTTL)
	}
}
