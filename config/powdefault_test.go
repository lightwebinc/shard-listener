package config

import (
	"flag"
	"os"
	"testing"
)

// The block-control gate defaults ON. This is a security-relevant default that
// several render surfaces (ansible, helm, fleet) mirror by hand, so pin it here
// — a silent flip back to false would disable PoW validation fleet-wide and no
// other test would notice.
func TestRequireBlockPoWDefaultsOn(t *testing.T) {
	oldArgs, oldCL := os.Args, flag.CommandLine
	t.Cleanup(func() { os.Args, flag.CommandLine = oldArgs, oldCL })
	flag.CommandLine = flag.NewFlagSet("test", flag.ContinueOnError)
	os.Args = []string{"shard-listener", "-iface", "lo"}
	t.Setenv("REQUIRE_BLOCK_POW", "")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !c.RequireBlockPoW {
		t.Fatal("RequireBlockPoW default = false, want true (block-control gate must default ON)")
	}
	// A floor of 0 means "self-consistency only" — the header must meet the
	// difficulty it claims, with no floor on what it may claim. An operator
	// raises it per network; the default must not silently impose one.
	if c.MinPoWBits != 0 {
		t.Fatalf("MinPoWBits default = %#x, want 0", c.MinPoWBits)
	}
}

// The env var must still be able to turn it off, or an operator has no escape
// hatch short of a code change.
func TestRequireBlockPoWEnvCanDisable(t *testing.T) {
	oldArgs, oldCL := os.Args, flag.CommandLine
	t.Cleanup(func() { os.Args, flag.CommandLine = oldArgs, oldCL })
	flag.CommandLine = flag.NewFlagSet("test", flag.ContinueOnError)
	os.Args = []string{"shard-listener", "-iface", "lo"}
	t.Setenv("REQUIRE_BLOCK_POW", "false")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.RequireBlockPoW {
		t.Fatal("REQUIRE_BLOCK_POW=false did not disable the gate")
	}
}
