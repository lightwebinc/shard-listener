// Package config loads and validates runtime configuration for
// bitcoin-shard-listener. Parameters are accepted from CLI flags first;
// environment variables serve as fallbacks; hard-coded defaults apply when
// neither is present.
//
// # Environment variable mapping
//
//	Flag                  Env var              Default          Description
//	-iface                MULTICAST_IF         eth0             NIC for multicast joins and NACK send
//	-listen-port          LISTEN_PORT          9001             UDP port for incoming multicast frames
//	-shard-bits           SHARD_BITS           2                Must match proxy (1–24)
//	-scope                MC_SCOPE             site             Multicast scope
//	-mc-base-addr         MC_BASE_ADDR                          Base IPv6 for group address space
//	-shard-include        SHARD_INCLUDE                         Comma-separated shard indices/ranges (empty=all)
//	-subtree-include      SUBTREE_INCLUDE                       Hex subtree IDs to allow (empty=all)
//	-subtree-exclude      SUBTREE_EXCLUDE                       Hex subtree IDs to drop (empty=none)
//	-egress-addr          EGRESS_ADDR          127.0.0.1:9100   Downstream unicast host:port
//	-egress-proto         EGRESS_PROTO         udp              udp | tcp
//	-strip-header         STRIP_HEADER         false            Send payload-only
//	-mc-egress-enabled    MC_EGRESS_ENABLED    false            Enable multicast egress
//	-mc-egress-iface      MC_EGRESS_IFACE      (=iface)         Output NIC for multicast send
//	-mc-egress-port       MC_EGRESS_PORT       (=listen-port)   Egress group UDP port
//	-mc-egress-scope      MC_EGRESS_SCOPE      (=scope)         Multicast scope for egress groups
//	-mc-egress-base-addr  MC_EGRESS_BASE_ADDR  (=mc-base-addr)  Base IPv6 for egress address space
//	-mc-egress-hoplimit   MC_EGRESS_HOPLIMIT   1                IPV6_MULTICAST_HOPS
//	-retry-endpoints      RETRY_ENDPOINTS                       Comma-separated host:port retry nodes
//	-nack-jitter-max      NACK_JITTER_MAX      200ms            Max NACK suppression jitter
//	-nack-backoff-max     NACK_BACKOFF_MAX      5s               Cap on exponential backoff per gap
//	-nack-max-retries     NACK_MAX_RETRIES      5                Max NACK attempts per gap
//	-nack-gap-ttl         NACK_GAP_TTL         10m              Max gap state lifetime
//	-beacon-enabled       BEACON_ENABLED       true             Enable ADVERT beacon listener
//	-beacon-port          BEACON_PORT          9300             UDP port for beacon reception
//	-beacon-scope         BEACON_SCOPE         site             Multicast scope for beacon groups
//	-workers              NUM_WORKERS          NumCPU           Receive goroutine count
//	-debug                DEBUG                false            Per-frame logging
//	-metrics-addr         METRICS_ADDR         :9200            Prometheus / healthz / readyz
//	-drain-timeout        DRAIN_TIMEOUT        0s               Pre-shutdown drain window
//	-instance             INSTANCE_ID          hostname         OTel service.instance.id
//	-otlp-endpoint        OTLP_ENDPOINT                         OTLP gRPC push (empty=disabled)
//	-otlp-interval        OTLP_INTERVAL        30s              OTLP metric export interval
package config

import (
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Scopes maps human-readable scope names to their RFC 4291 IPv6 multicast prefixes.
var Scopes = map[string]uint16{
	"link":   0xFF02,
	"site":   0xFF05,
	"org":    0xFF08,
	"global": 0xFF0E,
}

// Config holds all runtime parameters. Fields are read-only after [Load] returns.
type Config struct {
	// Network
	Iface          *net.Interface // Interface for multicast joins and NACK send
	ListenPort     int
	EgressAddr     string
	EgressProto    string // "udp" or "tcp"
	StripHeader    bool
	RetryEndpoints []string // host:port list for NACK dispatch

	// Sharding
	ShardBits      uint
	NumGroups      uint32
	MCScope        string
	MCPrefix       uint16
	MCBaseAddr     string
	MCMiddleBytes  [11]byte
	ShardInclude   []uint32   // empty = all
	SubtreeInclude [][32]byte // empty = all allowed
	SubtreeExclude [][32]byte // empty = none excluded

	// NACK
	NACKJitterMax  time.Duration
	NACKBackoffMax time.Duration
	NACKMaxRetries int
	NACKGapTTL     time.Duration

	// Multicast egress (domain bridging)
	MCEgressEnabled     bool
	MCEgressIface       *net.Interface
	MCEgressPort        int
	MCEgressScope       string
	MCEgressPrefix      uint16
	MCEgressBaseAddr    string
	MCEgressMiddleBytes [11]byte
	MCEgressHopLimit    int

	// Beacon discovery (BRC-126)
	BeaconEnabled bool
	BeaconPort    int
	BeaconScope   string // multicast scope for beacon group joins

	// Runtime
	NumWorkers   int
	Debug        bool
	DrainTimeout time.Duration

	// Observability
	MetricsAddr  string
	InstanceID   string
	OTLPEndpoint string
	OTLPInterval time.Duration
}

// Load parses flags and environment variables, validates all values, and
// returns a populated [Config]. It calls [flag.Parse] internally.
func Load() (*Config, error) {
	c := &Config{}

	ifaceFlag := flag.String("iface", envStr("MULTICAST_IF", "eth0"),
		"network interface for multicast group joins and NACK send")
	flag.IntVar(&c.ListenPort, "listen-port", envInt("LISTEN_PORT", 9001),
		"UDP port to receive multicast frames on")
	flag.StringVar(&c.MCScope, "scope", envStr("MC_SCOPE", "site"),
		"multicast scope: link | site | org | global")
	flag.StringVar(&c.MCBaseAddr, "mc-base-addr", envStr("MC_BASE_ADDR", ""),
		"base IPv6 address for assigned multicast address space (bytes 2–12)")
	shardIncludeFlag := flag.String("shard-include", envStr("SHARD_INCLUDE", ""),
		"comma-separated shard indices/ranges to subscribe (empty = all)")
	subtreeIncludeFlag := flag.String("subtree-include", envStr("SUBTREE_INCLUDE", ""),
		"comma-separated hex subtree IDs to allow (V2 only; empty = all)")
	subtreeExcludeFlag := flag.String("subtree-exclude", envStr("SUBTREE_EXCLUDE", ""),
		"comma-separated hex subtree IDs to drop (V2 only; empty = none)")
	flag.StringVar(&c.EgressAddr, "egress-addr", envStr("EGRESS_ADDR", "127.0.0.1:9100"),
		"downstream unicast host:port")
	flag.StringVar(&c.EgressProto, "egress-proto", envStr("EGRESS_PROTO", "udp"),
		"egress protocol: udp | tcp")
	flag.BoolVar(&c.StripHeader, "strip-header", envBool("STRIP_HEADER", false),
		"send payload-only (no frame header) to egress")
	flag.BoolVar(&c.MCEgressEnabled, "mc-egress-enabled", envBool("MC_EGRESS_ENABLED", false),
		"enable multicast egress (domain bridging)")
	mcEgressIfaceFlag := flag.String("mc-egress-iface", envStr("MC_EGRESS_IFACE", ""),
		"network interface for multicast egress send (default: same as -iface)")
	flag.IntVar(&c.MCEgressPort, "mc-egress-port", envInt("MC_EGRESS_PORT", 0),
		"UDP destination port for egress multicast groups (default: same as -listen-port)")
	flag.StringVar(&c.MCEgressScope, "mc-egress-scope", envStr("MC_EGRESS_SCOPE", ""),
		"multicast scope for egress groups: link | site | org | global (default: same as -scope)")
	flag.StringVar(&c.MCEgressBaseAddr, "mc-egress-base-addr", envStr("MC_EGRESS_BASE_ADDR", ""),
		"base IPv6 for egress multicast group address space (default: same as -mc-base-addr)")
	flag.IntVar(&c.MCEgressHopLimit, "mc-egress-hoplimit", envInt("MC_EGRESS_HOPLIMIT", 1),
		"IPv6 multicast hop limit for egress datagrams (IPV6_MULTICAST_HOPS)")
	retryFlag := flag.String("retry-endpoints", envStr("RETRY_ENDPOINTS", ""),
		"comma-separated host:port of multicast-retry caching nodes")
	flag.DurationVar(&c.NACKJitterMax, "nack-jitter-max", envDuration("NACK_JITTER_MAX", 200*time.Millisecond),
		"max random hold-off before NACK dispatch (NORM suppression window)")
	flag.DurationVar(&c.NACKBackoffMax, "nack-backoff-max", envDuration("NACK_BACKOFF_MAX", 5*time.Second),
		"cap on exponential backoff per gap")
	flag.IntVar(&c.NACKMaxRetries, "nack-max-retries", envInt("NACK_MAX_RETRIES", 5),
		"max NACK attempts per gap before declaring unrecoverable")
	flag.DurationVar(&c.NACKGapTTL, "nack-gap-ttl", envDuration("NACK_GAP_TTL", 10*time.Minute),
		"max time to hold a gap entry before evicting (~Bitcoin block interval)")
	flag.BoolVar(&c.BeaconEnabled, "beacon-enabled", envBool("BEACON_ENABLED", true),
		"enable ADVERT beacon listener for dynamic endpoint discovery")
	flag.IntVar(&c.BeaconPort, "beacon-port", envInt("BEACON_PORT", 9300),
		"UDP port for receiving ADVERT beacons")
	flag.StringVar(&c.BeaconScope, "beacon-scope", envStr("BEACON_SCOPE", "site"),
		"multicast scope for beacon group joins: link | site | org | global")
	flag.IntVar(&c.NumWorkers, "workers", envInt("NUM_WORKERS", runtime.NumCPU()),
		"number of worker goroutines (0 = runtime.NumCPU)")
	flag.BoolVar(&c.Debug, "debug", envBool("DEBUG", false),
		"enable per-frame debug logging")
	flag.DurationVar(&c.DrainTimeout, "drain-timeout", envDuration("DRAIN_TIMEOUT", 0),
		"pre-drain delay before closing sockets; /readyz returns 503 during this window (0 = disabled)")
	flag.StringVar(&c.MetricsAddr, "metrics-addr", envStr("METRICS_ADDR", ":9200"),
		"HTTP bind address for /metrics, /healthz, /readyz")
	flag.StringVar(&c.InstanceID, "instance", envStr("INSTANCE_ID", ""),
		"OTel service.instance.id (default: hostname)")
	flag.StringVar(&c.OTLPEndpoint, "otlp-endpoint", envStr("OTLP_ENDPOINT", ""),
		"OTLP gRPC endpoint for metric push (empty = disabled)")
	flag.DurationVar(&c.OTLPInterval, "otlp-interval", envDuration("OTLP_INTERVAL", 30*time.Second),
		"OTLP metric export interval (ignored when OTLP_ENDPOINT is empty)")

	bits := flag.Uint("shard-bits", uint(envInt("SHARD_BITS", 2)),
		"txid prefix bit width used as the shard key (1–24); must match proxy")

	flag.Parse()

	// Validate shard bit width.
	if *bits < 1 || *bits > 24 {
		return nil, fmt.Errorf("shard-bits must be in [1, 24], got %d", *bits)
	}
	c.ShardBits = *bits
	c.NumGroups = 1 << c.ShardBits

	// Resolve multicast scope.
	prefix, ok := Scopes[c.MCScope]
	if !ok {
		return nil, fmt.Errorf("unknown scope %q; valid values: link, site, org, global", c.MCScope)
	}
	c.MCPrefix = prefix

	// Parse base IPv6 middle bytes if provided.
	if c.MCBaseAddr != "" {
		ip := net.ParseIP(c.MCBaseAddr)
		if ip == nil {
			return nil, fmt.Errorf("invalid base IPv6 address %q", c.MCBaseAddr)
		}
		ip16 := ip.To16()
		if ip16 == nil || ip.To4() != nil {
			return nil, fmt.Errorf("base address must be IPv6, got %q", c.MCBaseAddr)
		}
		copy(c.MCMiddleBytes[:], ip16[2:13])
	}

	// Validate egress protocol.
	if c.EgressProto != "udp" && c.EgressProto != "tcp" {
		return nil, fmt.Errorf("egress-proto must be udp or tcp, got %q", c.EgressProto)
	}

	// Default workers.
	if c.NumWorkers <= 0 {
		c.NumWorkers = runtime.NumCPU()
	}

	// Resolve interface.
	iface, err := net.InterfaceByName(*ifaceFlag)
	if err != nil {
		return nil, fmt.Errorf("interface %q not found: %w", *ifaceFlag, err)
	}
	c.Iface = iface

	// Resolve multicast egress parameters (only when enabled).
	// Placed after c.Iface is set so the default iface fallback is valid.
	if c.MCEgressEnabled {
		// Scope: default to ingress scope.
		if c.MCEgressScope == "" {
			c.MCEgressScope = c.MCScope
		}
		egressPrefix, ok := Scopes[c.MCEgressScope]
		if !ok {
			return nil, fmt.Errorf("mc-egress-scope %q unknown; valid values: link, site, org, global", c.MCEgressScope)
		}
		c.MCEgressPrefix = egressPrefix

		// Base address: default to ingress middle bytes.
		if c.MCEgressBaseAddr == "" {
			c.MCEgressMiddleBytes = c.MCMiddleBytes
		} else {
			ip := net.ParseIP(c.MCEgressBaseAddr)
			if ip == nil {
				return nil, fmt.Errorf("invalid mc-egress-base-addr %q", c.MCEgressBaseAddr)
			}
			ip16 := ip.To16()
			if ip16 == nil || ip.To4() != nil {
				return nil, fmt.Errorf("mc-egress-base-addr must be IPv6, got %q", c.MCEgressBaseAddr)
			}
			copy(c.MCEgressMiddleBytes[:], ip16[2:13])
		}

		// Port: default to listen port.
		if c.MCEgressPort == 0 {
			c.MCEgressPort = c.ListenPort
		}

		// Interface: default to ingress interface (c.Iface already resolved above).
		if *mcEgressIfaceFlag == "" {
			c.MCEgressIface = c.Iface
		} else {
			mcIface, err := net.InterfaceByName(*mcEgressIfaceFlag)
			if err != nil {
				return nil, fmt.Errorf("mc-egress-iface %q not found: %w", *mcEgressIfaceFlag, err)
			}
			c.MCEgressIface = mcIface
		}
	}

	// Parse retry endpoints.
	for _, ep := range splitComma(*retryFlag) {
		if ep != "" {
			c.RetryEndpoints = append(c.RetryEndpoints, ep)
		}
	}

	// Parse shard include list.
	if *shardIncludeFlag != "" {
		for _, s := range splitComma(*shardIncludeFlag) {
			if s == "" {
				continue
			}
			idx, err := strconv.ParseUint(s, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid shard-include value %q: %w", s, err)
			}
			if uint32(idx) >= c.NumGroups {
				return nil, fmt.Errorf("shard-include %d >= numGroups %d", idx, c.NumGroups)
			}
			c.ShardInclude = append(c.ShardInclude, uint32(idx))
		}
	}

	// Parse subtree include/exclude as hex strings.
	if c.SubtreeInclude, err = parseSubtreeList(*subtreeIncludeFlag); err != nil {
		return nil, fmt.Errorf("subtree-include: %w", err)
	}
	if c.SubtreeExclude, err = parseSubtreeList(*subtreeExcludeFlag); err != nil {
		return nil, fmt.Errorf("subtree-exclude: %w", err)
	}

	return c, nil
}

func parseSubtreeList(s string) ([][32]byte, error) {
	var out [][32]byte
	for _, h := range splitComma(s) {
		if h == "" {
			continue
		}
		b, err := hex.DecodeString(strings.TrimPrefix(h, "0x"))
		if err != nil || len(b) != 32 {
			return nil, fmt.Errorf("invalid 32-byte hex subtree ID %q", h)
		}
		var id [32]byte
		copy(id[:], b)
		out = append(out, id)
	}
	return out, nil
}

func splitComma(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, strings.TrimSpace(p))
	}
	return out
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
