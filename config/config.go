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
//	-shard-bits           SHARD_BITS           2                Must match proxy (1–15)
//	-scope                MC_SCOPE             site             Multicast scope
//	-mc-group-id          MC_GROUP_ID          0x000B           IANA group-id (default Bitcoin = 0x000B)
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
//	-mc-egress-group-id   MC_EGRESS_GROUP_ID   (=mc-group-id)   IANA group-id for egress groups
//	-mc-egress-hoplimit   MC_EGRESS_HOPLIMIT   1                IPV6_MULTICAST_HOPS
//	-header-egress-enabled       HEADER_EGRESS_ENABLED       false            Enable unicast block header retransmission
//	-header-egress-addr          HEADER_EGRESS_ADDR          127.0.0.1:9101   Downstream unicast host:port for headers
//	-header-egress-proto         HEADER_EGRESS_PROTO         udp              udp | tcp
//	-header-mc-egress-enabled    HEADER_MC_EGRESS_ENABLED    false            Enable multicast block header retransmission
//	-header-mc-egress-iface      HEADER_MC_EGRESS_IFACE      (=iface)         Output NIC for multicast header send
//	-header-mc-egress-port       HEADER_MC_EGRESS_PORT       (=listen-port)   Egress group UDP port for headers
//	-header-mc-egress-scope      HEADER_MC_EGRESS_SCOPE      (=scope)         Multicast scope for header egress
//	-header-mc-egress-group-id   HEADER_MC_EGRESS_GROUP_ID   (=mc-group-id)   IANA group-id for header egress
//	-header-mc-egress-hoplimit   HEADER_MC_EGRESS_HOPLIMIT   1                IPV6_MULTICAST_HOPS for headers
//	-retry-endpoints      RETRY_ENDPOINTS                       Comma-separated host:port retry nodes
//	-nack-jitter-max      NACK_JITTER_MAX      200ms            Max NACK suppression jitter
//	-nack-backoff-max     NACK_BACKOFF_MAX      5s               Cap on exponential backoff per gap
//	-nack-max-retries     NACK_MAX_RETRIES      5                Max NACK attempts per gap
//	-nack-gap-ttl         NACK_GAP_TTL         10m              Max gap state lifetime
//	-beacon-enabled       BEACON_ENABLED       true             Enable ADVERT beacon listener
//	-beacon-port          BEACON_PORT          9300             UDP port for beacon reception
//	-beacon-scope         BEACON_SCOPE         site             Multicast scope for beacon groups
//	-subtree-groups       SUBTREE_GROUPS                        Comma-separated 32-char hex GroupIDs to subscribe
//	-subtree-group-default-ttl SUBTREE_GROUP_DEFAULT_TTL 900s  Default TTL for group announcements
//	-announce-scope       ANNOUNCE_SCOPE       site             Multicast scope(s) for announcement group joins
//	-sender-include       SENDER_INCLUDE                        IPv6/IPv4 addresses/CIDRs of trusted senders (announcements + data frames)
//	-sender-exclude       SENDER_EXCLUDE                        IPv6/IPv4 addresses/CIDRs to reject (checked before include)
//	-workers              NUM_WORKERS          NumCPU           Receive goroutine count
//	-debug                DEBUG                false            Per-frame logging
//	-verify-payload-hash  VERIFY_PAYLOAD_HASH  false            Verify SHA256d(payload)==TxID on V2 frames; drop on mismatch
//	-subtree-data-enabled SUBTREE_DATA_ENABLED false            Enable BRC-132 subtree data reception (join 0xFFFB group)
//	-subtree-data-verify-merkle SUBTREE_DATA_VERIFY_MERKLE false Optional post-reassembly Merkle root verification (expensive)
//	-egress-dedup-cap     EGRESS_DEDUP_CAP     0                Egress dedup capacity (0 = disabled)
//	-egress-dedup-ttl     EGRESS_DEDUP_TTL     2s               Egress dedup TTL (max age of a remembered key)
//	-txid-dedup-addr      TXID_DEDUP_ADDR                       Redis address for cross-listener TxID dedup (empty = disabled)
//	-txid-dedup-prefix    TXID_DEDUP_PREFIX    bsl:txid:        Redis key prefix for TxID dedup entries
//	-txid-dedup-ttl       TXID_DEDUP_TTL       60s              TTL for TxID dedup Redis entries
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

// DefaultSubtreeGroupTTL is the default announcement TTL applied when the
// sender transmits TTL=0 and no -subtree-group-default-ttl is configured.
const DefaultSubtreeGroupTTL = 900 * time.Second

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
	MCGroupID      uint16
	ShardInclude   []uint32   // empty = all
	SubtreeInclude [][32]byte // empty = all allowed
	SubtreeExclude [][32]byte // empty = none excluded

	// NACK
	NACKJitterMax  time.Duration
	NACKBackoffMax time.Duration
	NACKMaxRetries int
	NACKGapTTL     time.Duration

	// Multicast egress (domain bridging)
	MCEgressEnabled  bool
	MCEgressIface    *net.Interface
	MCEgressPort     int
	MCEgressScope    string
	MCEgressPrefix   uint16
	MCEgressGroupID  uint16
	MCEgressHopLimit int

	// Block header egress (SPV)
	HeaderEgressEnabled    bool
	HeaderEgressAddr       string
	HeaderEgressProto      string // "udp" or "tcp"
	HeaderMCEgressEnabled  bool
	HeaderMCEgressIface    *net.Interface
	HeaderMCEgressPort     int
	HeaderMCEgressScope    string
	HeaderMCEgressPrefix   uint16
	HeaderMCEgressGroupID  uint16
	HeaderMCEgressHopLimit int

	// Beacon discovery (BRC-126)
	BeaconEnabled bool
	BeaconPort    int
	BeaconScope   string // multicast scope for beacon group joins

	// Subtree group announcements (BRC-127)
	SubtreeGroups          [][16]byte // parsed GroupIDs to subscribe
	SubtreeGroupDefaultTTL time.Duration
	AnnounceScopes         []string     // e.g. ["site", "org"]
	SenderInclude          []*net.IPNet // nil/empty = accept all non-excluded
	SenderExclude          []*net.IPNet // checked before include

	// BRC-132 subtree data
	SubtreeDataEnabled      bool // join CtrlGroupSubtreeAnnounce (0xFFFB)
	SubtreeDataVerifyMerkle bool // optional post-reassembly Merkle root check

	// Runtime
	NumWorkers        int
	Debug             bool
	VerifyPayloadHash bool
	EgressDedupCap    int           // 0 = disabled
	EgressDedupTTL    time.Duration // max age of a remembered key

	// Egress TxID dedup (per-deployment): HA listener siblings sharing a
	// DeploymentID race to win the SETNX claim under EgressDedupPrefix +
	// DeploymentID + ":" + hex(txid). Only the winner forwards downstream.
	// Listeners with different DeploymentID values race independently, so
	// each deployment forwards at most once.
	//
	// TxidDedupAddr/Prefix/TTL are preserved as DEPRECATED aliases for
	// EgressDedupRedisAddr/Prefix/TTL when the new flags are not set.
	TxidDedupAddr   string        // DEPRECATED alias for EgressDedupRedisAddr
	TxidDedupPrefix string        // DEPRECATED alias for EgressDedupPrefix
	TxidDedupTTL    time.Duration // DEPRECATED alias for EgressDedupTTL

	DeploymentID         string
	NodeID               string
	EgressDedupRedisAddr string
	EgressDedupPrefix    string
	EgressDedupTTL2      time.Duration // separate field so deprecation logic can compare
	EgressDedupLocalCap  int

	// Optional courtesy SETNX into the local proxy's ingress namespace so
	// the proxy knows the TxID has been seen on the multicast network (e.g.
	// arrived via a cross-site bridge or other path the proxy did not see).
	//
	// IngressSetRedisAddr empty disables the courtesy mark.
	IngressSetRedisAddr string
	IngressSetPrefix    string
	IngressSetTTL       time.Duration
	IngressSetLocalCap  int

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
	groupIDFlag := flag.String("mc-group-id", envStr("MC_GROUP_ID", "0x000B"),
		"IANA group-id (bytes 12–13 of the IPv6 multicast address); default 0x000B (IANA Bitcoin)")
	shardIncludeFlag := flag.String("shard-include", envStr("SHARD_INCLUDE", ""),
		"comma-separated shard indices/ranges to subscribe (empty = all)")
	subtreeIncludeFlag := flag.String("subtree-include", envStr("SUBTREE_INCLUDE", ""),
		"comma-separated hex subtree IDs to allow (BRC-124/BRC-128 only; empty = all)")
	subtreeExcludeFlag := flag.String("subtree-exclude", envStr("SUBTREE_EXCLUDE", ""),
		"comma-separated hex subtree IDs to drop (BRC-124/BRC-128 only; empty = none)")
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
	egressGroupIDFlag := flag.String("mc-egress-group-id", envStr("MC_EGRESS_GROUP_ID", ""),
		"IANA group-id for egress multicast groups (default: same as -mc-group-id)")
	flag.IntVar(&c.MCEgressHopLimit, "mc-egress-hoplimit", envInt("MC_EGRESS_HOPLIMIT", 1),
		"IPv6 multicast hop limit for egress datagrams (IPV6_MULTICAST_HOPS)")
	flag.BoolVar(&c.HeaderEgressEnabled, "header-egress-enabled", envBool("HEADER_EGRESS_ENABLED", false),
		"enable unicast block header retransmission (stripped BRC-131, 172 bytes per block)")
	flag.StringVar(&c.HeaderEgressAddr, "header-egress-addr", envStr("HEADER_EGRESS_ADDR", "127.0.0.1:9101"),
		"downstream unicast host:port for block header stream")
	flag.StringVar(&c.HeaderEgressProto, "header-egress-proto", envStr("HEADER_EGRESS_PROTO", "udp"),
		"block header egress protocol: udp | tcp")
	flag.BoolVar(&c.HeaderMCEgressEnabled, "header-mc-egress-enabled", envBool("HEADER_MC_EGRESS_ENABLED", false),
		"enable multicast block header retransmission to CtrlGroupBlockHeader (0xFFFA)")
	headerMCEgressIfaceFlag := flag.String("header-mc-egress-iface", envStr("HEADER_MC_EGRESS_IFACE", ""),
		"network interface for multicast header egress send (default: same as -iface)")
	flag.IntVar(&c.HeaderMCEgressPort, "header-mc-egress-port", envInt("HEADER_MC_EGRESS_PORT", 0),
		"UDP destination port for header egress multicast group (default: same as -listen-port)")
	flag.StringVar(&c.HeaderMCEgressScope, "header-mc-egress-scope", envStr("HEADER_MC_EGRESS_SCOPE", ""),
		"multicast scope for header egress: link | site | org | global (default: same as -scope)")
	headerMCEgressGroupIDFlag := flag.String("header-mc-egress-group-id", envStr("HEADER_MC_EGRESS_GROUP_ID", ""),
		"IANA group-id for header egress multicast (default: same as -mc-group-id)")
	flag.IntVar(&c.HeaderMCEgressHopLimit, "header-mc-egress-hoplimit", envInt("HEADER_MC_EGRESS_HOPLIMIT", 1),
		"IPv6 multicast hop limit for header egress datagrams (IPV6_MULTICAST_HOPS)")
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
	subtreeGroupsFlag := flag.String("subtree-groups", envStr("SUBTREE_GROUPS", ""),
		"comma-separated 32-char hex group IDs to subscribe (BRC-127)")
	flag.DurationVar(&c.SubtreeGroupDefaultTTL, "subtree-group-default-ttl",
		envDuration("SUBTREE_GROUP_DEFAULT_TTL", DefaultSubtreeGroupTTL),
		"default TTL applied when an announcement carries TTL=0")
	announceScopeFlag := flag.String("announce-scope", envStr("ANNOUNCE_SCOPE", "site"),
		"multicast scope(s) for subtree announcement group joins: link | site | org | global (comma-separated)")
	senderIncludeFlag := flag.String("sender-include", envStr("SENDER_INCLUDE", ""),
		"comma-separated IPv6/IPv4 addresses/CIDRs of trusted senders; applied to both BRC-127 announcements and data-plane frames (empty = accept all)")
	senderExcludeFlag := flag.String("sender-exclude", envStr("SENDER_EXCLUDE", ""),
		"comma-separated IPv6/IPv4 addresses/CIDRs to reject; checked before include and applied to both BRC-127 announcements and data-plane frames")
	flag.IntVar(&c.NumWorkers, "workers", envInt("NUM_WORKERS", runtime.NumCPU()),
		"number of worker goroutines (0 = runtime.NumCPU)")
	flag.BoolVar(&c.Debug, "debug", envBool("DEBUG", false),
		"enable per-frame debug logging")
	flag.BoolVar(&c.VerifyPayloadHash, "verify-payload-hash", envBool("VERIFY_PAYLOAD_HASH", false),
		"verify SHA256d(payload) == TxID on BRC-124/BRC-128 frames; drop on mismatch")
	flag.BoolVar(&c.SubtreeDataEnabled, "subtree-data-enabled", envBool("SUBTREE_DATA_ENABLED", false),
		"enable BRC-132 subtree data reception: join CtrlGroupSubtreeAnnounce (0xFFFB) group")
	flag.BoolVar(&c.SubtreeDataVerifyMerkle, "subtree-data-verify-merkle", envBool("SUBTREE_DATA_VERIFY_MERKLE", false),
		"optional post-reassembly Merkle root verification for BRC-132 frames (expensive at 1M nodes)")
	flag.IntVar(&c.EgressDedupCap, "egress-dedup-cap", envInt("EGRESS_DEDUP_CAP", 0),
		"egress duplicate-suppression capacity (0 = disabled); typical value: workers × tps × dedup-ttl")
	flag.DurationVar(&c.EgressDedupTTL, "egress-dedup-ttl", envDuration("EGRESS_DEDUP_TTL", 2*time.Second),
		"egress dedup TTL: max age of a remembered (groupIdx, subtreeID, SeqNum) tuple")
	flag.StringVar(&c.TxidDedupAddr, "txid-dedup-addr", envStr("TXID_DEDUP_ADDR", ""),
		"DEPRECATED: use -egress-dedup-redis-addr. Redis address for cross-listener TxID dedup")
	flag.StringVar(&c.TxidDedupPrefix, "txid-dedup-prefix", envStr("TXID_DEDUP_PREFIX", ""),
		"DEPRECATED: use -egress-dedup-prefix. Redis key prefix for TxID dedup entries")
	flag.DurationVar(&c.TxidDedupTTL, "txid-dedup-ttl", envDuration("TXID_DEDUP_TTL", 0),
		"DEPRECATED: use -egress-dedup-ttl. TTL for TxID dedup Redis entries")

	flag.StringVar(&c.DeploymentID, "deployment-id", envStr("DEPLOYMENT_ID", ""),
		"per-deployment dedup identifier; HA siblings must share the same value (default: hostname)")
	flag.StringVar(&c.NodeID, "node-id", envStr("NODE_ID", ""),
		"per-node informational identifier used in metrics labels (default: hostname)")
	flag.StringVar(&c.EgressDedupRedisAddr, "egress-dedup-redis-addr", envStr("EGRESS_DEDUP_REDIS_ADDR", ""),
		"Redis address for per-deployment egress TxID dedup; empty = local-only LRU")
	flag.StringVar(&c.EgressDedupPrefix, "egress-dedup-prefix", envStr("EGRESS_DEDUP_PREFIX", "bsl:egr:"),
		"Redis key prefix for per-deployment egress dedup; deployment-id is appended")
	flag.DurationVar(&c.EgressDedupTTL2, "egress-dedup-ttl-redis", envDuration("EGRESS_DEDUP_TTL_REDIS", 60*time.Second),
		"TTL for egress-dedup Redis entries (per-deployment); must exceed max propagation delay")
	flag.IntVar(&c.EgressDedupLocalCap, "egress-dedup-local-cap", envInt("EGRESS_DEDUP_LOCAL_CAP", 1<<20),
		"tier-1 local LRU capacity for the egress TxID dedup gate (0 = disable feature)")

	flag.StringVar(&c.IngressSetRedisAddr, "ingress-set-redis-addr", envStr("INGRESS_SET_REDIS_ADDR", ""),
		"Redis address for courtesy SETNX into the local proxy's ingress namespace (empty = disabled)")
	flag.StringVar(&c.IngressSetPrefix, "ingress-set-prefix", envStr("INGRESS_SET_PREFIX", "bsp:tx:"),
		"Redis key prefix for ingress-set courtesy marks; MUST match the local proxy's -txid-dedup-prefix")
	flag.DurationVar(&c.IngressSetTTL, "ingress-set-ttl", envDuration("INGRESS_SET_TTL", 10*time.Minute),
		"TTL for ingress-set courtesy marks; SHOULD match the local proxy's -txid-dedup-ttl")
	flag.IntVar(&c.IngressSetLocalCap, "ingress-set-local-cap", envInt("INGRESS_SET_LOCAL_CAP", 1<<20),
		"tier-1 local LRU capacity for the ingress-mark dedup (0 = disable local LRU)")
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
		"txid prefix bit width used as the shard key (1–15); must match proxy")

	flag.Parse()

	// Validate shard bit width. Top of the 16-bit shard space is reserved for
	// control-plane groups (0xFFFC–0xFFFE), so practical bits is bounded at 15.
	if *bits < 1 || *bits > 15 {
		return nil, fmt.Errorf("shard-bits must be in [1, 15], got %d", *bits)
	}
	c.ShardBits = *bits
	c.NumGroups = 1 << c.ShardBits

	// Resolve multicast scope.
	prefix, ok := Scopes[c.MCScope]
	if !ok {
		return nil, fmt.Errorf("unknown scope %q; valid values: link, site, org, global", c.MCScope)
	}
	c.MCPrefix = prefix

	// Parse IANA group-id (default 0x000B = IANA Bitcoin allocation).
	gid, err := parseGroupID(*groupIDFlag)
	if err != nil {
		return nil, fmt.Errorf("invalid -mc-group-id %q: %w", *groupIDFlag, err)
	}
	c.MCGroupID = gid

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

		// Group-id: default to ingress group-id.
		if *egressGroupIDFlag == "" {
			c.MCEgressGroupID = c.MCGroupID
		} else {
			egid, err := parseGroupID(*egressGroupIDFlag)
			if err != nil {
				return nil, fmt.Errorf("invalid -mc-egress-group-id %q: %w", *egressGroupIDFlag, err)
			}
			c.MCEgressGroupID = egid
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

	// Validate unicast header egress protocol.
	if c.HeaderEgressEnabled {
		if c.HeaderEgressProto != "udp" && c.HeaderEgressProto != "tcp" {
			return nil, fmt.Errorf("header-egress-proto must be udp or tcp, got %q", c.HeaderEgressProto)
		}
	}

	// Resolve multicast header egress parameters (only when enabled).
	if c.HeaderMCEgressEnabled {
		if c.HeaderMCEgressScope == "" {
			c.HeaderMCEgressScope = c.MCScope
		}
		hdrPrefix, ok := Scopes[c.HeaderMCEgressScope]
		if !ok {
			return nil, fmt.Errorf("header-mc-egress-scope %q unknown; valid values: link, site, org, global", c.HeaderMCEgressScope)
		}
		c.HeaderMCEgressPrefix = hdrPrefix

		if *headerMCEgressGroupIDFlag == "" {
			c.HeaderMCEgressGroupID = c.MCGroupID
		} else {
			hgid, err := parseGroupID(*headerMCEgressGroupIDFlag)
			if err != nil {
				return nil, fmt.Errorf("invalid -header-mc-egress-group-id %q: %w", *headerMCEgressGroupIDFlag, err)
			}
			c.HeaderMCEgressGroupID = hgid
		}

		if c.HeaderMCEgressPort == 0 {
			c.HeaderMCEgressPort = c.ListenPort
		}

		if *headerMCEgressIfaceFlag == "" {
			c.HeaderMCEgressIface = c.Iface
		} else {
			hdrIface, err := net.InterfaceByName(*headerMCEgressIfaceFlag)
			if err != nil {
				return nil, fmt.Errorf("header-mc-egress-iface %q not found: %w", *headerMCEgressIfaceFlag, err)
			}
			c.HeaderMCEgressIface = hdrIface
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

	// Parse subtree group IDs (32-char hex = 16 bytes each).
	if c.SubtreeGroups, err = parseGroupList(*subtreeGroupsFlag); err != nil {
		return nil, fmt.Errorf("subtree-groups: %w", err)
	}

	// Parse announce scope(s).
	for _, s := range splitComma(*announceScopeFlag) {
		if s == "" {
			continue
		}
		if _, ok := Scopes[s]; !ok {
			return nil, fmt.Errorf("announce-scope %q unknown; valid values: link, site, org, global", s)
		}
		c.AnnounceScopes = append(c.AnnounceScopes, s)
	}
	if len(c.AnnounceScopes) == 0 {
		c.AnnounceScopes = []string{"site"}
	}

	// Parse sender include/exclude CIDRs.
	if c.SenderInclude, err = parseIPNetList(*senderIncludeFlag); err != nil {
		return nil, fmt.Errorf("sender-include: %w", err)
	}
	if c.SenderExclude, err = parseIPNetList(*senderExcludeFlag); err != nil {
		return nil, fmt.Errorf("sender-exclude: %w", err)
	}

	// Deprecation: when -egress-dedup-redis-addr is empty but the deprecated
	// -txid-dedup-addr is set, alias the old values into the new fields. This
	// preserves behaviour for operators who have not yet migrated. An info
	// log is emitted at startup (in main.go) when the alias is taken.
	if c.EgressDedupRedisAddr == "" && c.TxidDedupAddr != "" {
		c.EgressDedupRedisAddr = c.TxidDedupAddr
	}
	if c.TxidDedupPrefix != "" {
		// Operator set the deprecated flag explicitly — honour it.
		c.EgressDedupPrefix = c.TxidDedupPrefix
	}
	if c.TxidDedupTTL > 0 {
		c.EgressDedupTTL2 = c.TxidDedupTTL
	}

	// Default DeploymentID / NodeID to hostname when unset.
	if c.DeploymentID == "" {
		if h, hErr := os.Hostname(); hErr == nil && h != "" {
			c.DeploymentID = h
		} else {
			c.DeploymentID = "unknown"
		}
	}
	if c.NodeID == "" {
		c.NodeID = c.DeploymentID
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

func parseGroupList(s string) ([][16]byte, error) {
	var out [][16]byte
	for _, h := range splitComma(s) {
		if h == "" {
			continue
		}
		b, err := hex.DecodeString(strings.TrimPrefix(h, "0x"))
		if err != nil || len(b) != 16 {
			return nil, fmt.Errorf("invalid 32-char hex group ID %q (want 16 bytes)", h)
		}
		var id [16]byte
		copy(id[:], b)
		out = append(out, id)
	}
	return out, nil
}

func parseIPNetList(s string) ([]*net.IPNet, error) {
	var out []*net.IPNet
	for _, addr := range splitComma(s) {
		if addr == "" {
			continue
		}
		var ipNet *net.IPNet
		var err error
		if strings.Contains(addr, "/") {
			_, ipNet, err = net.ParseCIDR(addr)
			if err != nil {
				return nil, fmt.Errorf("invalid CIDR %q: %w", addr, err)
			}
		} else {
			ip := net.ParseIP(addr)
			if ip == nil {
				return nil, fmt.Errorf("invalid IP address %q", addr)
			}
			bits := 128
			if ip.To4() != nil {
				bits = 32
			}
			ipNet = &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)}
		}
		out = append(out, ipNet)
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

// parseGroupID accepts either a hex literal (0x000B, 000B) or a decimal
// integer in the range [0, 0xFFFF].
func parseGroupID(s string) (uint16, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty value")
	}
	base := 10
	low := strings.ToLower(s)
	if strings.HasPrefix(low, "0x") {
		s = s[2:]
		base = 16
	} else if _, err := strconv.ParseUint(s, 10, 16); err != nil {
		base = 16
	}
	n, err := strconv.ParseUint(s, base, 16)
	if err != nil {
		return 0, err
	}
	return uint16(n), nil
}
