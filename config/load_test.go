package config

import (
	"flag"
	"io"
	"net"
	"os"
	"strings"
	"testing"
)

// testIface returns a real interface name usable for flag resolution.
// Prefers the loopback so the test is portable across hosts.
func testIface(t *testing.T) string {
	t.Helper()
	ifaces, err := net.Interfaces()
	if err != nil || len(ifaces) == 0 {
		t.Skip("no network interfaces available")
	}
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagLoopback != 0 {
			return ifc.Name
		}
	}
	return ifaces[0].Name
}

// loadWithArgs resets the global flag state and invokes Load with the given
// CLI args. Restores os.Args and flag.CommandLine afterwards. Not safe for
// t.Parallel — relies on process-global flag state.
func loadWithArgs(t *testing.T, args ...string) (*Config, error) {
	t.Helper()
	origArgs := os.Args
	origCL := flag.CommandLine
	t.Cleanup(func() {
		os.Args = origArgs
		flag.CommandLine = origCL
	})
	flag.CommandLine = flag.NewFlagSet("test", flag.ContinueOnError)
	flag.CommandLine.SetOutput(io.Discard)
	os.Args = append([]string{"shard-listener"}, args...)
	return Load()
}

// withIface prepends a valid -iface flag so interface resolution succeeds.
func withIface(t *testing.T, args ...string) []string {
	return append([]string{"-iface=" + testIface(t)}, args...)
}

func TestLoad_Minimal(t *testing.T) {
	c, err := loadWithArgs(t, withIface(t)...)
	if err != nil {
		t.Fatalf("minimal load: %v", err)
	}
	if c.SourceMode != "asm" {
		t.Errorf("default source-mode = %q, want asm", c.SourceMode)
	}
	if c.ShardBits != 2 || c.NumGroups != 4 {
		t.Errorf("shard defaults: bits=%d groups=%d", c.ShardBits, c.NumGroups)
	}
	if c.MCPrefix != 0xFF05 {
		t.Errorf("site scope prefix = 0x%04X, want 0xFF05", c.MCPrefix)
	}
	if c.MCGroupID != 0x000B {
		t.Errorf("default group-id = 0x%04X, want 0x000B", c.MCGroupID)
	}
	if len(c.AnnounceScopes) != 1 || c.AnnounceScopes[0] != "site" {
		t.Errorf("default announce scopes = %v", c.AnnounceScopes)
	}
	if c.DeploymentID == "" || c.NodeID != c.DeploymentID {
		t.Errorf("deployment/node id defaulting: dep=%q node=%q", c.DeploymentID, c.NodeID)
	}
}

func TestLoad_ShardBitsBounds(t *testing.T) {
	for _, bits := range []string{"0", "13"} {
		if _, err := loadWithArgs(t, withIface(t, "-shard-bits="+bits)...); err == nil {
			t.Errorf("shard-bits=%s should error", bits)
		}
	}
	if _, err := loadWithArgs(t, withIface(t, "-shard-bits=12")...); err != nil {
		t.Errorf("shard-bits=12 should be valid: %v", err)
	}
}

func TestLoad_IfaceNotFound(t *testing.T) {
	if _, err := loadWithArgs(t, "-iface=definitely-not-a-real-iface0"); err == nil {
		t.Error("nonexistent iface should error")
	}
}

func TestLoad_UnknownScope(t *testing.T) {
	if _, err := loadWithArgs(t, withIface(t, "-scope=bogus")...); err == nil {
		t.Error("unknown scope should error")
	}
}

func TestLoad_SourceModeSSM(t *testing.T) {
	// SSM with no bootstrap list must fail closed.
	if _, err := loadWithArgs(t, withIface(t, "-source-mode=ssm")...); err == nil {
		t.Error("ssm without bootstrap should error")
	}
	// SSM with a beacon bootstrap succeeds and switches the prefix to FF3x.
	c, err := loadWithArgs(t, withIface(t,
		"-source-mode=ssm", "-scope=site", "-ssm-bootstrap-beacon=2001:db8::1")...)
	if err != nil {
		t.Fatalf("ssm with bootstrap: %v", err)
	}
	if c.SourceMode != "ssm" {
		t.Errorf("source-mode = %q", c.SourceMode)
	}
	if len(c.SSMBootstrapBeacon) != 1 {
		t.Errorf("bootstrap beacon = %v", c.SSMBootstrapBeacon)
	}
	// SSM rejects link scope.
	if _, err := loadWithArgs(t, withIface(t,
		"-source-mode=ssm", "-scope=link", "-ssm-bootstrap-beacon=2001:db8::1")...); err == nil {
		t.Error("ssm with link scope should error")
	}
}

func TestLoad_SSMRefreshNonPositive(t *testing.T) {
	if _, err := loadWithArgs(t, withIface(t, "-ssm-bootstrap-refresh=0")...); err == nil {
		t.Error("ssm-bootstrap-refresh=0 should error")
	}
}

func TestLoad_SSMPublishersStaticCap(t *testing.T) {
	many := make([]string, 17)
	for i := range many {
		many[i] = "2001:db8::1"
	}
	args := withIface(t, "-source-mode=ssm", "-scope=site",
		"-ssm-publishers-static="+strings.Join(many, ","))
	if _, err := loadWithArgs(t, args...); err == nil {
		t.Error(">16 static publishers without manifest discovery should error")
	}
}

func TestLoad_InvalidSourceMode(t *testing.T) {
	if _, err := loadWithArgs(t, withIface(t, "-source-mode=bogus")...); err == nil {
		t.Error("invalid source-mode should error")
	}
}

func TestLoad_InvalidGroupID(t *testing.T) {
	if _, err := loadWithArgs(t, withIface(t, "-mc-group-id=zzz")...); err == nil {
		t.Error("invalid group-id should error")
	}
}

func TestLoad_EgressProto(t *testing.T) {
	if _, err := loadWithArgs(t, withIface(t, "-egress-proto=sctp")...); err == nil {
		t.Error("invalid egress-proto should error")
	}
	if _, err := loadWithArgs(t, withIface(t, "-egress-proto=tcp")...); err != nil {
		t.Errorf("tcp egress-proto should be valid: %v", err)
	}
}

func TestLoad_MCEgress(t *testing.T) {
	c, err := loadWithArgs(t, withIface(t, "-mc-egress-enabled")...)
	if err != nil {
		t.Fatalf("mc-egress enabled: %v", err)
	}
	// Defaults inherit from ingress params.
	if c.MCEgressScope != c.MCScope || c.MCEgressGroupID != c.MCGroupID {
		t.Errorf("mc-egress defaults not inherited: scope=%q gid=0x%04X",
			c.MCEgressScope, c.MCEgressGroupID)
	}
	if c.MCEgressPort != c.ListenPort {
		t.Errorf("mc-egress port = %d, want listen-port %d", c.MCEgressPort, c.ListenPort)
	}
	if c.MCEgressIface == nil {
		t.Error("mc-egress iface should default to ingress iface")
	}
	// Unknown egress scope errors.
	if _, err := loadWithArgs(t, withIface(t,
		"-mc-egress-enabled", "-mc-egress-scope=bogus")...); err == nil {
		t.Error("unknown mc-egress-scope should error")
	}
}

func TestLoad_HeaderEgress(t *testing.T) {
	if _, err := loadWithArgs(t, withIface(t,
		"-header-egress-enabled", "-header-egress-proto=sctp")...); err == nil {
		t.Error("invalid header-egress-proto should error")
	}
	c, err := loadWithArgs(t, withIface(t, "-header-mc-egress-enabled")...)
	if err != nil {
		t.Fatalf("header-mc-egress enabled: %v", err)
	}
	if c.HeaderMCEgressGroupID != c.MCGroupID || c.HeaderMCEgressPort != c.ListenPort {
		t.Errorf("header mc-egress defaults not inherited")
	}
	if _, err := loadWithArgs(t, withIface(t,
		"-header-mc-egress-enabled", "-header-mc-egress-scope=bogus")...); err == nil {
		t.Error("unknown header-mc-egress-scope should error")
	}
}

func TestLoad_ShardInclude(t *testing.T) {
	c, err := loadWithArgs(t, withIface(t, "-shard-bits=2", "-shard-include=0,3")...)
	if err != nil {
		t.Fatalf("shard-include: %v", err)
	}
	if len(c.ShardInclude) != 2 || c.ShardInclude[0] != 0 || c.ShardInclude[1] != 3 {
		t.Errorf("shard-include parsed = %v", c.ShardInclude)
	}
	// Out-of-range index.
	if _, err := loadWithArgs(t, withIface(t, "-shard-bits=2", "-shard-include=4")...); err == nil {
		t.Error("shard-include >= numGroups should error")
	}
	// Non-numeric.
	if _, err := loadWithArgs(t, withIface(t, "-shard-include=x")...); err == nil {
		t.Error("non-numeric shard-include should error")
	}
}

func TestLoad_BadListsError(t *testing.T) {
	cases := map[string][]string{
		"subtree-include": {"-subtree-include=zz"},
		"subtree-groups":  {"-subtree-groups=badhex"},
		"announce-scope":  {"-announce-scope=bogus"},
		"sender-include":  {"-sender-include=not-an-ip"},
		"sender-exclude":  {"-sender-exclude=10.0.0.0/zz"},
	}
	for name, extra := range cases {
		if _, err := loadWithArgs(t, withIface(t, extra...)...); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestLoad_DeprecatedTxidDedupAlias(t *testing.T) {
	c, err := loadWithArgs(t, withIface(t,
		"-txid-dedup-addr=127.0.0.1:6379",
		"-txid-dedup-prefix=old:",
		"-txid-dedup-ttl=30s")...)
	if err != nil {
		t.Fatalf("deprecated alias load: %v", err)
	}
	if c.EgressDedupRedisAddr != "127.0.0.1:6379" {
		t.Errorf("addr alias not applied: %q", c.EgressDedupRedisAddr)
	}
	if c.EgressDedupPrefix != "old:" {
		t.Errorf("prefix alias not applied: %q", c.EgressDedupPrefix)
	}
	if c.EgressDedupTTL2 != 30_000_000_000 {
		t.Errorf("ttl alias not applied: %v", c.EgressDedupTTL2)
	}
}

func TestLoad_AutoConfigValidation(t *testing.T) {
	if _, err := loadWithArgs(t, withIface(t, "-manifest-bootstrap=bogus")...); err == nil {
		t.Error("unknown manifest-bootstrap should error")
	}
	if _, err := loadWithArgs(t, withIface(t, "-pilot-quorum=0")...); err == nil {
		t.Error("pilot-quorum=0 should error")
	}
	// live-resharding requires egress-dedup-cap > 0.
	if _, err := loadWithArgs(t, withIface(t,
		"-manifest-consumer-enabled", "-live-resharding")...); err == nil {
		t.Error("live-resharding without egress-dedup-cap should error")
	}
	// With cap but too-small ttl.
	if _, err := loadWithArgs(t, withIface(t,
		"-manifest-consumer-enabled", "-live-resharding",
		"-egress-dedup-cap=1000", "-egress-dedup-ttl=1s")...); err == nil {
		t.Error("live-resharding with small ttl should error")
	}
	// Valid live-resharding.
	if _, err := loadWithArgs(t, withIface(t,
		"-manifest-consumer-enabled", "-live-resharding",
		"-egress-dedup-cap=1000", "-egress-dedup-ttl=5s")...); err != nil {
		t.Errorf("valid live-resharding config rejected: %v", err)
	}
}
