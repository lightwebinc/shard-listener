// Package config loads and validates runtime configuration for
// shard-listener. Parameters are accepted from CLI flags first;
// environment variables serve as fallbacks; hard-coded defaults apply when
// neither is present.
//
// # Environment variable mapping
//
//	Flag                  Env var              Default          Description
//	-iface                MULTICAST_IF         eth0             NIC for multicast joins and NACK send
//	-listen-port          LISTEN_PORT          9001             UDP port for incoming multicast frames
//	-mode                 LISTENER_MODE        collapsed        Role split (P3b): collapsed | receiver | delivery
//	-shard-bits           SHARD_BITS           2                Must match proxy (1–12)
//	-scope                MC_SCOPE             site             Multicast scope
//	-mc-group-id          MC_GROUP_ID          0x000B           IANA group-id (default Bitcoin = 0x000B)
//	-local-source         LOCAL_SOURCE                          Co-located proxy BIND_SOURCE excluded from (S,G) joins
//	-shard-include        SHARD_INCLUDE                         Comma-separated shard indices/ranges (empty=all)
//	-subtree-include      SUBTREE_INCLUDE                       Hex subtree IDs to allow (empty=all)
//	-subtree-exclude      SUBTREE_EXCLUDE                       Hex subtree IDs to drop (empty=none)
//	-egress-addr          EGRESS_ADDR          127.0.0.1:9100   Downstream unicast host:port
//	-delivery-addrs       DELIVERY_ADDRS                        receiver mode: comma-separated delivery host:port fan-out set (empty=use -egress-addr)
//	-egress-proto         EGRESS_PROTO         udp              udp | tcp
//	-strip-header         STRIP_HEADER         true             Send payload-only (drop frame header)
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
//	-nack-backoff-base    NACK_BACKOFF_BASE    500ms            Base retry backoff (doubles per failed round)
//	-nack-backoff-max     NACK_BACKOFF_MAX      5s               Cap on exponential backoff per gap
//	-nack-max-retries     NACK_MAX_RETRIES      5                Max failed recovery rounds per gap
//	-nack-gap-ttl         NACK_GAP_TTL         10m              Max gap state lifetime
//	-nack-max-flows       NACK_MAX_FLOWS       100000           Cap on tracked per-source flows (0 = unbounded)
//	-nack-max-forward-jump NACK_MAX_FORWARD_JUMP 4096           Forward SeqNum jump beyond which a flow re-baselines
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
//	-verify-payload-hash  VERIFY_PAYLOAD_HASH  false            Verify canonical TxID on V2 frames (EF-aware); drop on mismatch
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

	"github.com/lightwebinc/shard-common/shard"
)

// splitCSV trims and returns non-empty comma-separated tokens.
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

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
	// Mode selects the listener's role in the receiver/delivery split (P3b,
	// mirrors the proxy -mode). "collapsed" (default) = today's monolith: join
	// fabric (S,G), demux/gap/NACK, and fan out to consumers in one process.
	// "receiver" = the multicast-facing half (join + gap/NACK) that forwards raw
	// frames (envelope-preserving) to the delivery tier: with -delivery-addrs it
	// fans every demuxed frame out to that host:port set (egress.MultiSender);
	// with none it degenerates to collapsed's single -egress-addr. "delivery" = the consumer-facing
	// half: NO multicast join and NO gap/NACK (the receiver owns those); a
	// unicast-ingest front reads raw frames off the wire and runs only the egress
	// (fan-out) sink. Delivery is edge-only + tunnel-out.
	Mode string

	// Network
	Iface      *net.Interface // Interface for multicast joins and NACK send
	ListenPort int
	EgressAddr string
	// DeliveryAddrs is the receiver→delivery fan-out target set (`-mode receiver`):
	// each demuxed frame is forwarded (envelope-preserving) to every host:port here.
	// Empty in receiver mode falls back to the single EgressAddr (== collapsed egress).
	DeliveryAddrs  []string
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

	// SSM. See SSM Support Plan.
	// SourceMode: "asm" (default) | "ssm"
	// SSMBootstrap{Beacon,Manifest,SubtreeGroupAnnounce}: per-control-group
	//   bootstrap source lists (IPv6 literals or DNS names) used to (S,G)
	//   join the matching control group. Resolved via shared
	//   bootstrap.Resolver at startup.
	// SSMPublishersStatic: lab/CI escape hatch for the data-plane source
	//   set. Production uses manifest-driven discovery.
	// SSMBootstrapRefresh: DNS re-resolve interval (default 30s).
	// LocalSource: the co-located proxy's BIND_SOURCE on a collapsed node.
	//   Excluded from every roster-driven (S,G) join — joining the node's
	//   own source on the PIM interface installs an iif==oif mroute and
	//   loops originated frames until hop-limit death (~60x amplification,
	//   measured in a geo SSM lab). Consequence: this listener
	//   does not receive own-node frames via multicast.
	SourceMode             string
	LocalSource            string
	SSMBootstrapBeacon     []string
	SSMBootstrapManifest   []string
	SSMBootstrapSubtreeAnn []string
	SSMPublishersStatic    []string
	SSMBootstrapRefresh    time.Duration

	// NACK
	NACKJitterMax   time.Duration
	NACKBackoffBase time.Duration
	NACKBackoffMax  time.Duration
	NACKMaxRetries  int
	NACKGapTTL      time.Duration
	NACKMaxFlows    int
	// BRC-126 tail probe: closes the tail-loss blind spot without a protocol
	// addition by reusing the existing MISS response.
	NACKTailProbe           bool
	NACKTailProbeIdleFactor float64
	NACKTailProbeMinIdle    time.Duration
	NACKTailProbeMaxMisses  int
	NACKMaxForwardJump      int

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

	// Auto-shard-config (BRC-139 manifest consumer). All fields are
	// opt-in. When AutoConfigEnabled is false, the listener does not
	// decode manifests off the beacon socket and the other fields are
	// ignored.
	AutoConfigEnabled        bool
	AutoConfigBootstrap      string        // "optional" (default) | "required"
	AutoConfigPilotQuorum    int           // default 2
	AutoConfigHysteresis     time.Duration // 0 ⇒ 2 × AnnounceInterval (per BRC-139)
	AutoJoinFromManifest     bool          // listener-only: union(-shard-include, pilot_groups)
	AutoConfigLiveResharding bool          // opt-in bridging mode (default: restart on adopt)
	AutoConfigBridgingWindow time.Duration // 0 ⇒ honour pilot TransitionEpoch verbatim

	// BRC-148 BEEF object plane (listener side). The plane is joined when
	// topics and/or explicit groups are configured; otherwise the listener
	// ignores the band entirely.
	BEEFTopics         []string // elected topic names and/or 64-hex TopicIDs; derives joins + topic filter
	BEEFGroups         []uint32 // explicit plane-relative group indices (aggregator: joined with no topic restriction)
	BEEFShardBits      uint     // plane shard-bit width (0-12, 0 = single group); must match the proxy
	BEEFMaxObjectBytes int      // reassembly bound for OrigFrameVer 0x09 (matches the proxies' ingress bound)
	BEEFVersions       []string // accepted encodings: beef|beefv2|atomic (empty = all)
	BEEFVerifyContent  bool     // debug: verify ContentID == SHA-256d(object) before fan-out

	// RebucketRelay marks this listener as an intentional re-bucket relay (it runs
	// a different ShardBits than the bundles it receives and re-buckets them). When
	// false (a bare edge listener), any re-bucketing raises the bsl_rebucket_unguarded
	// alarm + a one-shot WARN: a generation mismatch is almost always a misconfig.
	RebucketRelay bool

	// Subtree group announcements (BRC-127)
	SubtreeGroups          [][16]byte // parsed GroupIDs to subscribe
	SubtreeGroupDefaultTTL time.Duration
	AnnounceScopes         []string     // e.g. ["site", "org"]
	SenderInclude          []*net.IPNet // nil/empty = accept all non-excluded
	SenderExclude          []*net.IPNet // checked before include

	// BRC-132 subtree data
	SubtreeDataEnabled      bool // join GroupSubtreeDataAnnounce (0xFFFB)
	SubtreeDataVerifyMerkle bool // optional post-reassembly Merkle root check

	// Runtime
	NumWorkers        int
	Debug             bool
	VerifyPayloadHash bool
	EgressDedupCap    int           // 0 = disabled
	EgressDedupTTL    time.Duration // max age of a remembered key

	// Block-control gate (opt-in). Inter-domain BRC-131 announces reach the
	// listener by multicast without passing our proxy, so the listener
	// independently validates before fan-out: PoW on the announce, and
	// coinbase↔block correlation on BRC-133. Off by default.
	RequireBlockPoW bool
	MinPoWBits      uint32 // PoW difficulty floor (compact nBits); 0 = self-consistency only

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
	EgressDedupBackend   string // redis|aerospike|memory|none (empty infers redis if addr set, else none)
	EgressDedupRedisAddr string
	EgressDedupAeroHosts []string
	EgressDedupAeroNS    string
	EgressDedupAeroSet   string
	EgressDedupPrefix    string
	EgressDedupTTL2      time.Duration // separate field so deprecation logic can compare
	EgressDedupLocalCap  int

	// Optional courtesy SETNX into the local proxy's ingress namespace so
	// the proxy knows the TxID has been seen on the multicast network (e.g.
	// arrived via a cross-site bridge or other path the proxy did not see).
	// The two stores are addressed independently: either may use a different
	// backend or endpoint.
	//
	// IngressSetBackend none AND IngressSetRedisAddr empty disables the mark.
	IngressSetBackend   string
	IngressSetRedisAddr string
	IngressSetAeroHosts []string
	IngressSetAeroNS    string
	IngressSetAeroSet   string
	IngressSetPrefix    string
	IngressSetTTL       time.Duration
	IngressSetLocalCap  int

	DrainTimeout time.Duration

	// Observability
	MetricsAddr   string
	InstanceID    string
	OTLPEndpoint  string
	OTLPInterval  time.Duration
	LogFormat     string  // "text" (default) | "json"
	LogLevel      string  // debug|info|warn|error
	TraceSampling float64 // 0..1 head sampling ratio; 0 disables tracing
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
		"multicast scope: link | site | org | global (site|global also accepted in SSM mode)")
	groupIDFlag := flag.String("mc-group-id", envStr("MC_GROUP_ID", "0x000B"),
		"IANA group-id (bytes 12–13 of the IPv6 multicast address); default 0x000B (IANA Bitcoin)")
	flag.StringVar(&c.SourceMode, "source-mode", envStr("SOURCE_MODE", "asm"),
		"multicast addressing model: asm | ssm")
	flag.StringVar(&c.Mode, "mode", envStr("LISTENER_MODE", "collapsed"),
		"role split (P3b): collapsed (default; join+demux+gap/NACK+fan-out) | receiver (mcast half) | delivery (consumer half: unicast-ingest + fan-out, no mcast join, no gap/NACK)")
	ssmBootstrapBeacon := flag.String("ssm-bootstrap-beacon", envStr("SSM_BOOTSTRAP_BEACON", ""),
		"CSV of retry-endpoint sources for SSM join of the beacon group")
	ssmBootstrapManifest := flag.String("ssm-bootstrap-manifest", envStr("SSM_BOOTSTRAP_MANIFEST", ""),
		"CSV of shard-manifest sources for SSM join of the manifest/block-broadcast group")
	ssmBootstrapSubtreeAnn := flag.String("ssm-bootstrap-subtree-announce", envStr("SSM_BOOTSTRAP_SUBTREE_ANNOUNCE", ""),
		"CSV of subtree-announce emitter sources for SSM join of that control group")
	ssmPublishersStatic := flag.String("ssm-publishers-static", envStr("SSM_PUBLISHERS_STATIC", ""),
		"lab/CI: CSV of data-plane publisher IPv6 sources (production uses manifest discovery)")
	flag.StringVar(&c.LocalSource, "local-source", envStr("LOCAL_SOURCE", ""),
		"co-located proxy's BIND_SOURCE; excluded from all (S,G) joins (own-source loop guard)")
	flag.DurationVar(&c.SSMBootstrapRefresh, "ssm-bootstrap-refresh", envDuration("SSM_BOOTSTRAP_REFRESH", 30*time.Second),
		"DNS re-resolve interval for SSM bootstrap entries")
	shardIncludeFlag := flag.String("shard-include", envStr("SHARD_INCLUDE", ""),
		"comma-separated shard indices/ranges to subscribe (empty = all)")
	subtreeIncludeFlag := flag.String("subtree-include", envStr("SUBTREE_INCLUDE", ""),
		"comma-separated hex subtree IDs to allow (BRC-124/BRC-128 only; empty = all)")
	subtreeExcludeFlag := flag.String("subtree-exclude", envStr("SUBTREE_EXCLUDE", ""),
		"comma-separated hex subtree IDs to drop (BRC-124/BRC-128 only; empty = none)")
	flag.StringVar(&c.EgressAddr, "egress-addr", envStr("EGRESS_ADDR", "127.0.0.1:9100"),
		"downstream unicast host:port")
	deliveryAddrsFlag := flag.String("delivery-addrs", envStr("DELIVERY_ADDRS", ""),
		"receiver mode (-mode receiver): comma-separated delivery host:port set to fan every demuxed frame out to (envelope-preserving); empty falls back to -egress-addr")
	flag.StringVar(&c.EgressProto, "egress-proto", envStr("EGRESS_PROTO", "udp"),
		"egress protocol: udp | tcp")
	flag.BoolVar(&c.StripHeader, "strip-header", envBool("STRIP_HEADER", true),
		"drop the multicast frame header, sending payload-only (raw BSV tx) to egress "+
			"(default; set false only when the downstream re-reads frames, e.g. domain bridging)")
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
		"enable unicast block header retransmission (BRC-135, 172 bytes per block)")
	flag.StringVar(&c.HeaderEgressAddr, "header-egress-addr", envStr("HEADER_EGRESS_ADDR", "127.0.0.1:9101"),
		"downstream unicast host:port for block header stream")
	flag.StringVar(&c.HeaderEgressProto, "header-egress-proto", envStr("HEADER_EGRESS_PROTO", "udp"),
		"block header egress protocol: udp | tcp")
	flag.BoolVar(&c.HeaderMCEgressEnabled, "header-mc-egress-enabled", envBool("HEADER_MC_EGRESS_ENABLED", false),
		"enable multicast block header retransmission to GroupBlockHeader (0xFFFA)")
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
	flag.DurationVar(&c.NACKBackoffBase, "nack-backoff-base", envDuration("NACK_BACKOFF_BASE", 500*time.Millisecond),
		"base delay for retry backoff; doubles per failed recovery round (not per tier-escalation hop)")
	flag.DurationVar(&c.NACKBackoffMax, "nack-backoff-max", envDuration("NACK_BACKOFF_MAX", 5*time.Second),
		"cap on exponential backoff per gap")
	flag.IntVar(&c.NACKMaxRetries, "nack-max-retries", envInt("NACK_MAX_RETRIES", 5),
		"max failed recovery rounds per gap before declaring unrecoverable (tier-escalation hops are free)")
	flag.DurationVar(&c.NACKGapTTL, "nack-gap-ttl", envDuration("NACK_GAP_TTL", 10*time.Minute),
		"max time to hold a gap entry before evicting (~Bitcoin block interval)")
	flag.BoolVar(&c.NACKTailProbe, "nack-tail-probe", envBool("NACK_TAIL_PROBE", true),
		"BRC-126: speculatively NACK the next expected SeqNum on a flow that has gone quiet. Gap detection is otherwise inferential (frame N is only known lost when N+1 arrives), so the last frames before a sender idles are invisible. Uses the existing MISS response — no protocol change")
	flag.Float64Var(&c.NACKTailProbeIdleFactor, "nack-tail-probe-idle-factor", envFloat("NACK_TAIL_PROBE_IDLE_FACTOR", 4.0),
		"BRC-126: multiple of the flow's own smoothed inter-arrival before silence counts as abnormal")
	flag.DurationVar(&c.NACKTailProbeMinIdle, "nack-tail-probe-min-idle", envDuration("NACK_TAIL_PROBE_MIN_IDLE", 500*time.Millisecond),
		"BRC-126: floor on the tail-probe idle threshold so fast flows do not probe continuously")
	flag.IntVar(&c.NACKTailProbeMaxMisses, "nack-tail-probe-max-misses", envInt("NACK_TAIL_PROBE_MAX_MISSES", 3),
		"BRC-126: stop probing a flow after this many consecutive MISS answers (it is genuinely idle); reset by new traffic")
	flag.IntVar(&c.NACKMaxFlows, "nack-max-flows", envInt("NACK_MAX_FLOWS", 100000),
		"cap on tracked per-source flows (flood guard; new sources past the cap still forward but skip NACK recovery until idle flows age out); 0 = unbounded")
	flag.IntVar(&c.NACKMaxForwardJump, "nack-max-forward-jump", envInt("NACK_MAX_FORWARD_JUMP", 4096),
		"forward SeqNum jump beyond which a flow re-baselines (emitter change, e.g. anycast failover) instead of registering a phantom gap range; 0 = default 4096")
	flag.BoolVar(&c.BeaconEnabled, "beacon-enabled", envBool("BEACON_ENABLED", true),
		"enable ADVERT beacon listener for dynamic endpoint discovery")
	flag.IntVar(&c.BeaconPort, "beacon-port", envInt("BEACON_PORT", 9300),
		"UDP port for receiving ADVERT beacons")
	flag.StringVar(&c.BeaconScope, "beacon-scope", envStr("BEACON_SCOPE", "site"),
		"multicast scope for beacon group joins: link | site | org | global")
	flag.BoolVar(&c.AutoConfigEnabled, "manifest-consumer-enabled", envBool("MANIFEST_CONSUMER_ENABLED", false),
		"opt-in BRC-139 manifest consumer for auto-shard-config (off by default)")
	flag.StringVar(&c.AutoConfigBootstrap, "manifest-bootstrap", envStr("MANIFEST_BOOTSTRAP", "optional"),
		"manifest bootstrap behavior: 'optional' (default) | 'required' (refuse data-plane bind until quorum)")
	flag.IntVar(&c.AutoConfigPilotQuorum, "pilot-quorum", envInt("PILOT_QUORUM", 2),
		"min distinct authoritative announcers required for adoption; 1 allowed but logs a warning")
	flag.DurationVar(&c.AutoConfigHysteresis, "pilot-hysteresis", envDuration("PILOT_HYSTERESIS", 0),
		"hysteresis window before adoption; 0 ⇒ 2 × AnnounceInterval of the candidate manifest")
	flag.BoolVar(&c.AutoJoinFromManifest, "shard-include-from-manifest", envBool("SHARD_INCLUDE_FROM_MANIFEST", false),
		"additive auto-join: effective subscription = union(-shard-include, pilot_groups)")
	flag.BoolVar(&c.AutoConfigLiveResharding, "live-resharding", envBool("LIVE_RESHARDING", false),
		"opt-in BRC-139 live-resharding bridging mode (default: restart on ShardBits adoption)")
	flag.DurationVar(&c.AutoConfigBridgingWindow, "bridging-window", envDuration("BRIDGING_WINDOW", 0),
		"local floor on bridging duration; 0 ⇒ honour pilot TransitionEpoch verbatim")
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
		"enable per-frame debug logging; deprecated alias for -log-level=debug")
	flag.StringVar(&c.LogFormat, "log-format", envStr("LOG_FORMAT", "text"),
		"log output format: text (default, stderr) | json (stdout, for fleet aggregation)")
	flag.StringVar(&c.LogLevel, "log-level", envStr("LOG_LEVEL", "info"),
		"log level: debug|info|warn|error (overridden to debug when -debug is set)")
	flag.Float64Var(&c.TraceSampling, "trace-sampling", envFloat("TRACE_SAMPLING", 0),
		"distributed-trace head sampling ratio 0..1 (0 = tracing off; exports via -otlp-endpoint)")
	flag.BoolVar(&c.VerifyPayloadHash, "verify-payload-hash", envBool("VERIFY_PAYLOAD_HASH", false),
		"verify the canonical TxID on BRC-124/BRC-128 frames (SHA256d(payload) for BRC-12 raw, objfmt.TxID for BRC-30 EF payloads); drop on mismatch")
	flag.BoolVar(&c.RequireBlockPoW, "require-block-pow", envBool("REQUIRE_BLOCK_POW", true),
		"gate BRC-131 announces on header proof-of-work + correlate BRC-133 coinbase with a validated block before fan-out (validates inter-domain block control); default ON — everything downstream of a block announce, including BRC-135 header egress, inherits this gate")
	minPoWBits := flag.String("min-pow-bits", envStr("MIN_POW_BITS", "0"),
		"PoW difficulty floor for -require-block-pow in Bitcoin compact nBits form (e.g. 0x1d00ffff); 0 = header self-consistency only")
	flag.BoolVar(&c.SubtreeDataEnabled, "subtree-data-enabled", envBool("SUBTREE_DATA_ENABLED", false),
		"enable BRC-132 subtree data reception: join GroupSubtreeDataAnnounce (0xFFFB) group")
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
	flag.StringVar(&c.EgressDedupBackend, "egress-dedup-backend", envStr("EGRESS_DEDUP_BACKEND", ""),
		"tier-2 egress dedup backend: redis|aerospike|memory|none (empty infers redis when -egress-dedup-redis-addr set, else none)")
	flag.StringVar(&c.EgressDedupRedisAddr, "egress-dedup-redis-addr", envStr("EGRESS_DEDUP_REDIS_ADDR", ""),
		"Redis-protocol address (Redis/Valkey/Dragonfly) for per-deployment egress TxID dedup; empty = local-only LRU")
	egAeroHosts := flag.String("egress-dedup-aerospike-hosts", envStr("EGRESS_DEDUP_AEROSPIKE_HOSTS", ""),
		"Aerospike seed nodes host:port (comma-separated) for egress dedup; required when -egress-dedup-backend=aerospike")
	flag.StringVar(&c.EgressDedupAeroNS, "egress-dedup-aerospike-namespace", envStr("EGRESS_DEDUP_AEROSPIKE_NAMESPACE", "cache"),
		"Aerospike namespace for egress dedup")
	flag.StringVar(&c.EgressDedupAeroSet, "egress-dedup-aerospike-set", envStr("EGRESS_DEDUP_AEROSPIKE_SET", "bsl-egr"),
		"Aerospike set for egress dedup")
	flag.StringVar(&c.EgressDedupPrefix, "egress-dedup-prefix", envStr("EGRESS_DEDUP_PREFIX", "bsl:egr:"),
		"Redis key prefix for per-deployment egress dedup; deployment-id is appended")
	flag.DurationVar(&c.EgressDedupTTL2, "egress-dedup-ttl-redis", envDuration("EGRESS_DEDUP_TTL_REDIS", 60*time.Second),
		"TTL for egress-dedup Redis entries (per-deployment); must exceed max propagation delay")
	flag.IntVar(&c.EgressDedupLocalCap, "egress-dedup-local-cap", envInt("EGRESS_DEDUP_LOCAL_CAP", 1<<20),
		"tier-1 local LRU capacity for the egress TxID dedup gate (0 = disable feature)")

	flag.StringVar(&c.IngressSetBackend, "ingress-set-backend", envStr("INGRESS_SET_BACKEND", ""),
		"tier-2 ingress-mark backend: redis|aerospike|memory|none (empty infers redis when -ingress-set-redis-addr set, else none)")
	flag.StringVar(&c.IngressSetRedisAddr, "ingress-set-redis-addr", envStr("INGRESS_SET_REDIS_ADDR", ""),
		"Redis-protocol address for courtesy SETNX into the local proxy's ingress namespace (empty = disabled)")
	ingAeroHosts := flag.String("ingress-set-aerospike-hosts", envStr("INGRESS_SET_AEROSPIKE_HOSTS", ""),
		"Aerospike seed nodes host:port (comma-separated) for ingress mark; required when -ingress-set-backend=aerospike")
	flag.StringVar(&c.IngressSetAeroNS, "ingress-set-aerospike-namespace", envStr("INGRESS_SET_AEROSPIKE_NAMESPACE", "cache"),
		"Aerospike namespace for ingress mark")
	flag.StringVar(&c.IngressSetAeroSet, "ingress-set-aerospike-set", envStr("INGRESS_SET_AEROSPIKE_SET", "bsp-tx"),
		"Aerospike set for ingress mark")
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
		"txid prefix bit width used as the shard key (1–12); must match proxy")

	beefTopicsFlag := flag.String("beef-topics", envStr("BEEF_TOPICS", ""),
		"comma-separated BRC-148 overlay topics to elect (names or 64-hex TopicIDs); derives plane joins + the topic filter")
	beefGroupsFlag := flag.String("beef-groups", envStr("BEEF_GROUPS", ""),
		"comma-separated plane-relative BRC-148 group indices to join (aggregator mode — no topic restriction)")
	beefBits := flag.Uint("beef-shard-bits", uint(envInt("BEEF_SHARD_BITS", 0)),
		"BRC-148 BEEF plane shard-bit width (0-12, 0 = single group); must match proxy")
	beefMaxObject := flag.Int("beef-max-object-bytes", envInt("BEEF_MAX_OBJECT_BYTES", 1<<20),
		"BRC-148/149 per-object byte bound applied to fragment reassembly for OrigFrameVer 0x09 (declared OrigPayloadLen); MUST match the ingress proxies' -beef-max-object-bytes")
	beefVersionsFlag := flag.String("beef-versions", envStr("BEEF_VERSIONS", ""),
		"accepted BEEF encodings, comma of beef|beefv2|atomic (empty = all)")
	flag.BoolVar(&c.BEEFVerifyContent, "beef-verify-content", envBool("BEEF_VERIFY_CONTENT", false),
		"verify ContentID == SHA-256d(object) before fan-out (debug/test support)")
	flag.BoolVar(&c.RebucketRelay, "rebucket-relay", envBool("REBUCKET_RELAY", false),
		"mark this listener an intentional re-bucket relay (runs a different ShardBits than the bundles it receives); when false, re-bucketing raises the bsl_rebucket_unguarded alarm")

	flag.Parse()

	// Parse the PoW difficulty floor (Bitcoin compact nBits): hex (0x…/bare) or decimal.
	{
		s := strings.TrimSpace(*minPoWBits)
		base := 10
		if strings.HasPrefix(strings.ToLower(s), "0x") {
			s, base = s[2:], 16
		}
		v, perr := strconv.ParseUint(s, base, 32)
		if perr != nil {
			return nil, fmt.Errorf("invalid -min-pow-bits %q: %w", *minPoWBits, perr)
		}
		c.MinPoWBits = uint32(v)
	}

	// Validate shard bit width. BRC-129 zones the 16-bit shard space: shard
	// group indices are 0x0000–0x0FFF, so bits is bounded at 12.
	if *bits < 1 || *bits > 12 {
		return nil, fmt.Errorf("shard-bits must be in [1, 12], got %d", *bits)
	}
	c.ShardBits = *bits
	c.NumGroups = 1 << c.ShardBits

	// BRC-148 BEEF plane parsing/validation (v1 caps the width at 12).
	if *beefBits > 12 {
		return nil, fmt.Errorf("beef-shard-bits must be in [0, 12], got %d", *beefBits)
	}
	c.BEEFShardBits = *beefBits
	if *beefMaxObject < 1 {
		return nil, fmt.Errorf("beef-max-object-bytes must be positive, got %d", *beefMaxObject)
	}
	c.BEEFMaxObjectBytes = *beefMaxObject
	for _, t := range strings.Split(*beefTopicsFlag, ",") {
		if t = strings.TrimSpace(t); t != "" {
			c.BEEFTopics = append(c.BEEFTopics, t)
		}
	}
	for _, g := range strings.Split(*beefGroupsFlag, ",") {
		g = strings.TrimSpace(g)
		if g == "" {
			continue
		}
		v, perr := strconv.ParseUint(g, 0, 32)
		if perr != nil {
			return nil, fmt.Errorf("invalid -beef-groups entry %q: %w", g, perr)
		}
		if v >= 1<<c.BEEFShardBits {
			return nil, fmt.Errorf("beef group index %d outside plane width 2^%d", v, c.BEEFShardBits)
		}
		c.BEEFGroups = append(c.BEEFGroups, uint32(v))
	}
	for _, tok := range strings.Split(*beefVersionsFlag, ",") {
		tok = strings.ToLower(strings.TrimSpace(tok))
		if tok == "" {
			continue
		}
		switch tok {
		case "beef", "beefv2", "atomic":
			c.BEEFVersions = append(c.BEEFVersions, tok)
		default:
			return nil, fmt.Errorf("invalid -beef-versions token %q (beef|beefv2|atomic)", tok)
		}
	}

	// Validate the receiver/delivery role split (P3b).
	switch strings.ToLower(c.Mode) {
	case "", "collapsed":
		c.Mode = "collapsed"
	case "receiver":
		c.Mode = "receiver"
	case "delivery":
		c.Mode = "delivery"
	default:
		return nil, fmt.Errorf("invalid -mode %q (collapsed|receiver|delivery)", c.Mode)
	}

	// Resolve multicast scope + source-mode → upper-16-bit prefix.
	switch strings.ToLower(c.SourceMode) {
	case "asm":
		c.SourceMode = "asm"
		prefix, ok := Scopes[c.MCScope]
		if !ok {
			return nil, fmt.Errorf("unknown scope %q; valid values: link, site, org, global", c.MCScope)
		}
		c.MCPrefix = prefix
	case "ssm":
		c.SourceMode = "ssm"
		scope, err := shard.ParseScope(c.MCScope)
		if err != nil {
			return nil, fmt.Errorf("source-mode=ssm requires -scope site|global: %w", err)
		}
		prefix, err := shard.Prefix(shard.SourceModeSSM, scope)
		if err != nil {
			return nil, err
		}
		c.MCPrefix = prefix
	default:
		return nil, fmt.Errorf("invalid source-mode %q (asm|ssm)", c.SourceMode)
	}

	c.SSMBootstrapBeacon = splitCSV(*ssmBootstrapBeacon)
	c.SSMBootstrapManifest = splitCSV(*ssmBootstrapManifest)
	c.SSMBootstrapSubtreeAnn = splitCSV(*ssmBootstrapSubtreeAnn)
	c.SSMPublishersStatic = splitCSV(*ssmPublishersStatic)
	c.DeliveryAddrs = splitCSV(*deliveryAddrsFlag)
	if c.SSMBootstrapRefresh <= 0 {
		return nil, fmt.Errorf("ssm-bootstrap-refresh must be > 0")
	}
	if c.SourceMode == "ssm" {
		if len(c.SSMBootstrapBeacon) == 0 &&
			len(c.SSMBootstrapManifest) == 0 &&
			len(c.SSMBootstrapSubtreeAnn) == 0 &&
			len(c.SSMPublishersStatic) == 0 {
			return nil, fmt.Errorf("source-mode=ssm requires at least one of -ssm-bootstrap-{beacon,manifest,subtree-announce} or -ssm-publishers-static")
		}
		if len(c.SSMPublishersStatic) > 16 && len(c.SSMBootstrapManifest) == 0 {
			return nil, fmt.Errorf("ssm-publishers-static has %d entries; production at this size must use manifest discovery (set -ssm-bootstrap-manifest)", len(c.SSMPublishersStatic))
		}
	}

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

	// Parse Aerospike seed nodes for the two dedup stores.
	for _, h := range strings.Split(*egAeroHosts, ",") {
		if h = strings.TrimSpace(h); h != "" {
			c.EgressDedupAeroHosts = append(c.EgressDedupAeroHosts, h)
		}
	}
	for _, h := range strings.Split(*ingAeroHosts, ",") {
		if h = strings.TrimSpace(h); h != "" {
			c.IngressSetAeroHosts = append(c.IngressSetAeroHosts, h)
		}
	}

	// Infer tier-2 backend kinds when not set explicitly: redis if an address
	// was provided (back-compat), else none (tier-1 LRU only / mark disabled).
	if c.EgressDedupBackend == "" {
		if c.EgressDedupRedisAddr != "" {
			c.EgressDedupBackend = "redis"
		} else {
			c.EgressDedupBackend = "none"
		}
	}
	if c.IngressSetBackend == "" {
		if c.IngressSetRedisAddr != "" {
			c.IngressSetBackend = "redis"
		} else {
			c.IngressSetBackend = "none"
		}
	}
	for _, b := range []string{c.EgressDedupBackend, c.IngressSetBackend} {
		switch b {
		case "redis", "aerospike", "memory", "none":
		default:
			return nil, fmt.Errorf("dedup backend %q unknown; valid: redis, aerospike, memory, none", b)
		}
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

	// Auto-shard-config validation. Defaults applied first, then
	// invariants checked.
	switch c.AutoConfigBootstrap {
	case "optional", "required":
	default:
		return nil, fmt.Errorf("manifest-bootstrap %q unknown; valid: optional, required", c.AutoConfigBootstrap)
	}
	if c.AutoConfigPilotQuorum < 1 {
		return nil, fmt.Errorf("pilot-quorum must be >= 1, got %d", c.AutoConfigPilotQuorum)
	}
	if c.AutoConfigEnabled && c.AutoConfigLiveResharding && c.EgressDedupCap == 0 {
		return nil, fmt.Errorf("live-resharding requires egress-dedup-cap > 0 (dedup absorbs bridging-window duplicates)")
	}
	if c.AutoConfigEnabled && c.AutoConfigLiveResharding && c.EgressDedupTTL < 4*time.Second {
		// Loose floor — full check (>= 2 × bridging window) happens at
		// bridging-mode entry, since the bridging window is not known
		// until the pilot publishes a Successor block.
		return nil, fmt.Errorf("live-resharding requires egress-dedup-ttl >= 4s (got %s); production deployments should size to >= 2 × bridgingWindow", c.EgressDedupTTL)
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

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
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
