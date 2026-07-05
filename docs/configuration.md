# shard-listener — Configuration Reference

All parameters are accepted as CLI flags. Environment variables serve as
fallbacks; hard-coded defaults apply when neither is present.

## Network

### `-iface` / `MULTICAST_IF` (default: `eth0`)

Network interface for multicast group joins and NACK send. Must be the same
interface the multicast fabric is reachable on.

### `-listen-port` / `LISTEN_PORT` (default: `9001`)

UDP port to receive multicast frames on. Must match the proxy's egress port.

### `-scope` / `MC_SCOPE` (default: `site`)

Multicast scope nibble. Must match the proxy's `-scope`.

| Value | Prefix | Reach |
|----------|--------|-----------------------------------------------------|
| `link` | `FF02` | Same L2 segment only |
| `site` | `FF05` | Site-local; crosses routers within a site (default) |
| `org` | `FF08` | Organisation-wide |
| `global` | `FF0E` | Internet-wide |

### `-mc-group-id` / `MC_GROUP_ID` (default: `0x000B`)

IANA group-id occupying bytes 12–13 of every IPv6 multicast group address.
The default `0x000B` corresponds to the IANA-assigned Bitcoin allocation
`FF0X::B`. Must match the proxy's `-mc-group-id`. Operators MAY override
(e.g. `0xCAFE`) for testing or private deployments.

---

## Mode (receiver / delivery split)

### `-mode` / `LISTENER_MODE` (default: `collapsed`)

Role in the receiver/delivery split (mirrors the proxy's `-mode`):

| Value | Behaviour |
|-------------|--------------------------------------------------------------|
| `collapsed` | Default monolith: join fabric `(S,G)`, demux/gap/NACK, and fan out to consumers in one process. |
| `receiver` | Multicast-facing half: joins + gap/NACK, forwards raw frames (envelope-preserving) to the delivery tier. |
| `delivery` | Consumer-facing half: **no** multicast join, **no** gap/NACK (the receiver owns those); a unicast-ingest front reads raw frames off the wire and runs only the egress (fan-out) sink. |

### `-delivery-addrs` / `DELIVERY_ADDRS` (default: empty)

Receiver mode only: comma-separated delivery `host:port` set. Every demuxed
frame is fanned out (envelope-preserving — `HashKey`/`SeqNum` intact) to each
entry. Empty falls back to the single `-egress-addr` (equivalent to collapsed
egress).

---

## SSM (RFC 4607)

See the [SSM Support Plan](https://github.com/lightwebinc/bsv-multicast/blob/main/DESIGN.md#source-specific-multicast-ssm)
for the full design. ASM remains the default; SSM is fully opt-in.

### `-source-mode` / `SOURCE_MODE` (default: `asm`)

Multicast addressing model: `asm` (FF0x scope; default) or `ssm`
(FF3x scope per RFC 4607). In SSM mode the prefix is derived via
`shard.Prefix(SSM, scope)` — FF35 for site, FF3E for global. RFC 8815
deprecates ASM at global scope and the validator rejects it. All three
join sites (data plane, beacon, subtree-announce) branch between
`IPV6_JOIN_GROUP` (ASM) and `MCAST_JOIN_SOURCE_GROUP` (SSM, one per
source) via the shared `shard-common/netjoin` helper.

### `-ssm-bootstrap-manifest` / `SSM_BOOTSTRAP_MANIFEST` (default: `""`)

CSV of `shard-manifest` source IPs (IPv6 literals or DNS names; a
headless-Service name is the expected production form). Resolved via
the shared `bootstrap.Resolver` at startup and re-resolved every
`ssm-bootstrap-refresh`. Used as the SSM source set for the manifest
group `(S,G)` join.

### `-ssm-bootstrap-beacon` / `SSM_BOOTSTRAP_BEACON` (default: `""`)

CSV of retry-endpoint source IPs (the retry-endpoint is the beacon
emitter — see BRC-126). Used as the SSM source set for the beacon
group `(S,G)` join under Posture C.

### `-ssm-bootstrap-subtree-announce` / `SSM_BOOTSTRAP_SUBTREE_ANNOUNCE` (default: `""`)

CSV of source IPs that emit subtree-announce frames. Used as the SSM
source set for both `GroupSubtreeDataAnnounce` (0xFFFB) and
`GroupSubtreeGroupAnnounce` (0xFFFC).

### `-ssm-publishers-static` / `SSM_PUBLISHERS_STATIC` (default: `""`)

**Lab / CI escape hatch.** Pre-declared data-plane publisher source
list applied to every shard group. Production must use manifest-driven
discovery (set `-ssm-bootstrap-manifest`); fails closed when
`source-mode=ssm` AND the list has > 16 entries AND no manifest
bootstrap is configured.

### `-ssm-bootstrap-refresh` / `SSM_BOOTSTRAP_REFRESH` (default: `30s`)

DNS re-resolve interval for the bootstrap lists. The `bootstrap.Resolver`
retains the last good AAAA set on transient refresh failures so a brief
DNS outage doesn't drop active joins.

### `-local-source` / `LOCAL_SOURCE` (default: `""`)

The co-located proxy's `BIND_SOURCE` on a collapsed node. This source is
excluded from every roster-driven `(S,G)` join — joining the node's own source
on the PIM interface would install an `iif==oif` mroute and loop originated
frames until hop-limit death. When set, this listener does not receive
own-node frames via multicast; mirror locally where own-source completeness
matters.

### Fail-closed validation

When `-source-mode=ssm`, at least one of the four lists above MUST be
non-empty or the process fails to start. This prevents silent ASM
fallback when SSM was intended.

---

## Sharding

### `-shard-bits` / `SHARD_BITS` (default: `2`)

Txid prefix bit width used as the shard key. Must exactly match the proxy's
`-shard-bits`. Determines how many multicast groups exist (2ᴺ).

| Bits | Groups |
|------|--------|
| 1 | 2 |
| 2 | 4 |
| 8 | 256 |
| 12 | 4 096 |
| 15 | 32 768 (max; top of 16-bit space reserved for control) |

### `-shard-include` / `SHARD_INCLUDE`

Comma-separated list of shard indices to subscribe to and forward. Empty (the
default) means subscribe to all groups. Example: `0,1,3`.

### `-subtree-include` / `SUBTREE_INCLUDE`

Comma-separated list of 32-byte hex SubtreeIDs to allow (BRC-124/BRC-128 frames only).
Empty means accept all subtrees.

### `-subtree-exclude` / `SUBTREE_EXCLUDE`

Comma-separated list of 32-byte hex SubtreeIDs to drop. Applied after include.
Empty means exclude nothing.

### BRC-142 bundle handling

BRC-142 coalescing bundles (FrameVer `0x08`) are handled automatically — there is
**no CLI flag**. Bundles are decoalesced/re-bucketed and delivered by the normal
egress path (see [architecture — BRC-142 Bundle Processing](architecture.md#brc-142-bundle-processing)).
When a bundle's ShardBits generation differs from this listener's, it is
re-bucketed to the local generation; the re-bucketed datagram size cap is the
hardcoded public-internet MTU baseline (`rebucketMaxBytes = 1500` in `listener.go`),
not configurable.

---

## Egress (unicast downstream)

### `-egress-addr` / `EGRESS_ADDR` (default: `127.0.0.1:9100`)

Downstream unicast `host:port`. Frames passing the filter are forwarded here.

### `-egress-proto` / `EGRESS_PROTO` (default: `udp`)

Egress protocol: `udp` or `tcp`.

- **UDP** — one datagram per frame; no connection state.
- **TCP** — persistent connection; reconnects automatically on error.

### `-strip-header` / `STRIP_HEADER` (default: `true`)

When `true` (the default), only the raw BSV transaction payload is forwarded
(no frame header) — this is what a downstream node/miner expects, since it
reads a standard `tx` (BRC-12) or Extended Format (BRC-30) serialisation, not a
multicast frame. When `false`, the complete 92-byte BRC-124/BRC-128 frame is
forwarded verbatim; set this only when the downstream re-reads frames itself
(for example domain bridging into another multicast fabric).

---

## Multicast Egress (domain bridging)

When multicast egress is enabled, every frame that passes the shard/subtree
filter is re-emitted onto an IPv6 multicast address space in addition to the
normal unicast downstream. This enables bridging between multicast domains with
optional scope and/or address-space translation.

The re-emitted frame uses the **same shard index** as the ingress group, but
the destination address is computed with independently configurable scope,
middle bytes, and port. The underlying socket sets `IPV6_MULTICAST_LOOP=0` so
re-emitted frames are not received back by sockets on the sending host.

### `-mc-egress-enabled` / `MC_EGRESS_ENABLED` (default: `false`)

Set to `true` to enable multicast egress. All other `-mc-egress-*` flags are
ignored when this is `false`.

### `-mc-egress-iface` / `MC_EGRESS_IFACE` (default: same as `-iface`)

Network interface for multicast send (`IPV6_MULTICAST_IF`). Defaults to the
same interface used for ingress. Set to a different interface when bridging
between two separate fabric segments.

### `-mc-egress-port` / `MC_EGRESS_PORT` (default: same as `-listen-port`)

UDP destination port written into egress multicast datagrams. Receivers on the
downstream domain must listen on this port.

### `-mc-egress-scope` / `MC_EGRESS_SCOPE` (default: same as `-scope`)

Multicast scope for the egress group address space. Use a narrower scope (e.g.
`link`) to confine re-emitted frames to an L2 segment, or a wider scope for
routed delivery.

| Value | Prefix | Reach |
|----------|--------|-----------------------------------------------------|
| `link` | `FF02` | Same L2 segment only |
| `site` | `FF05` | Site-local; crosses routers within a site |
| `org` | `FF08` | Organisation-wide |
| `global` | `FF0E` | Internet-wide |

### `-mc-egress-group-id` / `MC_EGRESS_GROUP_ID` (default: same as `-mc-group-id`)

IANA group-id (bytes 12–13) for egress multicast group addresses.
Leave unset to re-emit on the same group-id as ingress (only the scope
changes). Set to a different prefix to bridge between unrelated address spaces.

### `-mc-egress-hoplimit` / `MC_EGRESS_HOPLIMIT` (default: `1`)

IPv6 multicast hop limit (`IPV6_MULTICAST_HOPS`). The default of `1` confines
re-emitted frames to the directly attached network. Increase for routed
multicast delivery (requires PIM or similar on intermediate routers).

> **Firewall:** the egress interface OUTPUT chain must accept
> `ip6 daddr ff00::/8 udp dport <mc-egress-port>`. The `listener-infra`
> Ansible role nft template should be extended with this rule when mc egress
> is in use.

> **Same address-space warning:** if `-mc-egress-scope` and
> `-mc-egress-group-id` match the ingress address space, re-emitted frames
> will be visible to all other listeners joined to those groups on the same
> fabric. `IPV6_MULTICAST_LOOP=0` prevents the sending host from re-ingesting
> its own frames, but other hosts on the segment will receive duplicates unless
> they are intentional downstream consumers.

---

## Block Header Egress (BRC-135)

When BRC-131 block control frames are received, the listener can extract the 80-byte block
header from `BlockAnnounce` frames and re-emit it as a 172-byte BRC-135 frame
(FrameVer `0x07`; 92-byte header + 80-byte payload). This provides a lightweight SPV
consumer path without requiring consumers to process full block announcement payloads.
Header egress runs independently of the normal unicast egress (`-egress-addr`); both can
be active simultaneously.

### `-header-egress-enabled` / `HEADER_EGRESS_ENABLED` (default: `false`)

Enable unicast block header retransmission. When `true`, BRC-131 `BlockAnnounce` frames
trigger extraction and re-encoding of the 80-byte block header as a BRC-135 frame.

### `-header-egress-addr` / `HEADER_EGRESS_ADDR` (default: `127.0.0.1:9101`)

Downstream unicast `host:port` for block headers. Headers are sent as 172-byte
BRC-135 frames (92-byte header + 80-byte block header payload).

### `-header-egress-proto` / `HEADER_EGRESS_PROTO` (default: `udp`)

Transport for unicast header egress: `udp` or `tcp`. TCP reconnects automatically on error.

### `-header-mc-egress-enabled` / `HEADER_MC_EGRESS_ENABLED` (default: `false`)

Enable multicast block header retransmission. When `true`, the BRC-135 header frames
are re-emitted to `GroupBlockHeader` (`FF0X::B:FFFA`). SPV consumers join this group
rather than `GroupBlockBroadcast` (`FF0X::B:FFFE`) to receive headers only.

### `-header-mc-egress-iface` / `HEADER_MC_EGRESS_IFACE` (default: same as `-iface`)

Network interface for multicast header send (`IPV6_MULTICAST_IF`).

### `-header-mc-egress-port` / `HEADER_MC_EGRESS_PORT` (default: same as `-listen-port`)

UDP destination port for multicast header datagrams.

### `-header-mc-egress-scope` / `HEADER_MC_EGRESS_SCOPE` (default: same as `-scope`)

Multicast scope for the header egress group. Use a narrower scope than the data plane if
SPV consumers are on a separate L2 segment.

### `-header-mc-egress-group-id` / `HEADER_MC_EGRESS_GROUP_ID` (default: same as `-mc-group-id`)

IANA group-id (bytes 12–13 of the IPv6 multicast address) for the header egress group.
Override when consumers join headers on a different `FF0X::<gid>:FFFA` address than the
data-plane fabric (e.g. a tenant-isolated SPV segment). Accepts decimal (`11`) or hex
(`0x000B`).

### `-header-mc-egress-hoplimit` / `HEADER_MC_EGRESS_HOPLIMIT` (default: `1`)

`IPV6_MULTICAST_HOPS` for header egress datagrams. The default `1` confines headers to the
directly attached segment.

---

## NACK / Gap Recovery

Gap tracking is performed for BRC-124/BRC-128 frames where `SeqNum` (bytes 48–55) is
non-zero. `HashKey` (bytes 40–47) is a stable per-flow identifier computed as
`XXH64(senderIPv6 ∥ groupIdx ∥ subtreeID)`; `SeqNum` is a monotonic per-flow
counter starting at 1. Both are stamped in-place by the proxy; a zero `SeqNum`
means the frame has not been stamped and gap tracking is skipped.

When a gap is detected the listener sends a 64-byte NACK datagram (carrying
`HashKey`, `StartSeq`/`EndSeq`, and `SubtreeID`) to the current endpoint in
the sorted registry.

### `-retry-endpoints` / `RETRY_ENDPOINTS`

Comma-separated `host:port` list of multicast retry caching nodes to send NACK
datagrams to. Empty disables NACK dispatch (gaps are still detected and
counted). Example: `[2001:db8::1]:9300,[2001:db8::2]:9300`.

### `-nack-jitter-max` / `NACK_JITTER_MAX` (default: `200ms`)

Maximum random hold-off before the first NACK is dispatched (NORM suppression
window). Prevents NACK implosion when many listeners detect the same gap.

### `-nack-backoff-base` / `NACK_BACKOFF_BASE` (default: `500ms`)

Base delay for the retry backoff; doubles per failed recovery round.
Tier-escalation hops (MISS → next endpoint) do not consume backoff rounds.

### `-nack-backoff-max` / `NACK_BACKOFF_MAX` (default: `5s`)

Cap on exponential backoff between successive NACK retries for the same gap.

### `-nack-max-retries` / `NACK_MAX_RETRIES` (default: `5`)

Maximum NACK attempts per gap. After this is exceeded the gap is declared
unrecoverable and evicted (`bsl_gaps_unrecovered_total` incremented).

> **Multi-endpoint deployments:** each MISS response advances to the next
> endpoint, consuming one retry. With beacon discovery enabled and 3 retry
> endpoints (3 beacon + 3 static seeds = 6 registry entries), set
> `NACK_MAX_RETRIES=8` to ensure all entries are tried before eviction.

### `-nack-gap-ttl` / `NACK_GAP_TTL` (default: `10m`)

Maximum lifetime of a gap entry before it is evicted regardless of retry
count. Set to approximately one Bitcoin block interval to avoid accumulating
stale state across block boundaries.

### `-nack-max-flows` / `NACK_MAX_FLOWS` (default: `100000`)

Cap on tracked per-source flows (flood guard). New sources past the cap still
forward normally but skip NACK recovery until idle flows age out. `0` =
unbounded.

### `-nack-max-forward-jump` / `NACK_MAX_FORWARD_JUMP` (default: `4096`)

Forward `SeqNum` jump beyond which a flow re-baselines (emitter change, e.g.
anycast failover between proxies with divergent counters) instead of
registering — and NACK-storming — a phantom gap range. `0` selects the
default 4096. See
[architecture — Gap tracking](architecture.md#gap-tracking-nack--norm-inspired)
for the rate-aware plausibility check and the v1.7.1 transition-tail recovery.

---

## Egress Deduplication

When both an inline frame **and** its retransmit arrive at the listener, the
downstream consumer would otherwise receive the same transaction twice. Egress
dedup suppresses the second delivery.

### `-egress-dedup-cap` / `EGRESS_DEDUP_CAP` (default: `0`)

Capacity of the egress dedup set (number of `(groupIdx, subtreeID, seqNum)`
entries). `0` disables dedup entirely. A value of `65536` is sufficient for
~10 minutes of sustained traffic at 100 TPS with 10% gap rate.

### `-egress-dedup-ttl` / `EGRESS_DEDUP_TTL` (default: `2s`)

TTL for entries in the egress dedup set. Frames with the same `SeqNum` seen
within this window are suppressed. Size to cover the maximum expected
retransmit delay: a late retransmit can arrive up to `nack-backoff-max` + one
sweep interval (5.1 s with defaults) after the inline frame, so raise the TTL
above the 2 s default when full retransmit-window suppression matters
(live-resharding additionally requires ≥ 4 s). Entries also evict on capacity
overflow regardless of TTL.

> **Interaction with gap tracker:** even when a duplicate is suppressed by
> egress dedup, `nack.Tracker.Observe` is still called so gap-fill bookkeeping
> stays accurate.

---

## Cross-Listener TxID Deduplication

When multiple listeners process the same multicast fabric (e.g. for redundancy
behind a downstream load balancer), each will forward the same TxID once to its
own downstream. Cross-listener dedup arbitrates a single forwarder per TxID
through a shared Redis backend: the first listener to claim a TxID in Redis
forwards egress; the others drop the frame.

Local egress dedup (above) operates on `(groupIdx, subtreeID, SeqNum)` within a
single listener; cross-listener dedup is keyed on `TxID` across all listeners.
The two mechanisms are independent and can be combined.

### `-txid-dedup-addr` / `TXID_DEDUP_ADDR` (default: empty)

Redis address (`host:port`) for cross-listener TxID dedup. Empty disables the
feature entirely; the listener runs without checking Redis.

### `-txid-dedup-prefix` / `TXID_DEDUP_PREFIX` (default: `bsl:txid:`)

Redis key prefix prepended to every `TxID` claim. Useful for namespacing
multiple independent listener fleets sharing one Redis instance.

### `-txid-dedup-ttl` / `TXID_DEDUP_TTL` (default: `60s`)

TTL for `TxID` dedup Redis entries. Must exceed the maximum propagation delay
across all listeners (including retransmit jitter) so that a late-arriving
retransmit on another listener still finds the original claim.

---

## Beacon Discovery

### `-beacon-enabled` / `BEACON_ENABLED` (default: `true`)

When true, join the beacon multicast group and dynamically discover retry
endpoints from ADVERT datagrams broadcast by `retry-endpoint` instances.
Discovered endpoints are merged into the NACK dispatch registry alongside any
static seeds from `-retry-endpoints`.

The registry is sorted by **(Tier ASC, Preference DESC)**. Beacon-discovered
entries sort before static seeds (seeds use Tier=0xFF). Endpoints are evicted
after 3 × their advertised interval without a refresh.

### `-beacon-port` / `BEACON_PORT` (default: `9300`)

UDP port for receiving ADVERT beacon datagrams. Must match the
`-nack-port` / `NACK_PORT` of the retry endpoints.

### `-beacon-scope` / `BEACON_SCOPE` (default: `site`)

Multicast scope for the beacon group join. Must match the `-beacon-scope`
used by the retry endpoints.

| Value | Prefix | Reach |
|--------|--------|---------------------------------------------------|
| `link` | `FF02` | Same L2 segment only |
| `site` | `FF05` | Site-local; crosses routers within a site |
| `org` | `FF08` | Organisation-wide |
| `global` | `FF0E` | Internet-wide |

> **Firewall:** the listener's nftables input chain must accept UDP traffic on
> `beacon-port` from the beacon multicast prefix (`ff00::/8`) on the fabric
> interface. The `listener-infra` Ansible role already includes this rule.

---

## Auto-Shard-Config (BRC-139)

The listener can consume BRC-139 ShardManifest announcements off the same
beacon group, applying the normative consumer profile (Authoritative
quorum, hysteresis, ±1 ShardBits shift bound, manual-pin precedence).
Default off; opt-in via `-manifest-consumer-enabled`. See the
[Automatic Shard Configuration Plan](https://github.com/lightwebinc/bsv-multicast/blob/main/DESIGN.md#automatic-shard-configuration)
for the system-level design.

### `-manifest-consumer-enabled` / `MANIFEST_CONSUMER_ENABLED` (default: `false`)

Master switch. When false, the beacon listener drops any
`MsgType=0x40` (ShardManifest) datagram it sees and behavior matches
today exactly. When true, manifests are decoded, upserted into a
`shard-common/manifest.Registry`, and evaluated on a 1 s tick.

### `-manifest-bootstrap` / `MANIFEST_BOOTSTRAP` (default: `optional`)

`optional` ⇒ start with CLI/env values; pilot adjustments applied
opportunistically. `required` ⇒ refuse data-plane bind until quorum is
reached for `ShardBits` (and `SourceModeSSM` when SSM). `/readyz`
returns 503 throughout the wait.

### `-pilot-quorum` / `PILOT_QUORUM` (default: `2`)

Minimum distinct authoritative announcers (keyed on `(SrcIPv6,
InstanceID)`) required for adoption. `1` is permitted but logs a
warning at startup; production deployments should keep `≥ 2`.

### `-pilot-hysteresis` / `PILOT_HYSTERESIS` (default: `0`)

Duration a candidate value must hold quorum before adoption. `0`
selects `2 × AnnounceInterval` of the candidate manifest (BRC-139
default per §Safety).

### `-shard-include-from-manifest` / `SHARD_INCLUDE_FROM_MANIFEST` (default: `false`)

When set, the listener's effective subscription =
`union(-shard-include, pilot_groups)`, where `pilot_groups` is the
union of `Flags.GroupsValid` payloads from `Flags.PilotOnly=1`
manifests that satisfy quorum. Pilot-added groups are leaved when no
pilot still claims them; static `-shard-include` entries are NEVER
leaved.

### `-live-resharding` / `LIVE_RESHARDING` (default: `false`)

Opt-in BRC-139 bridging mode. When false (default), a `ShardBits` or
`SourceModeSSM` adoption flips `/readyz` and exits non-zero so the
orchestrator can roll the pod. When true, the listener tracks a
Successor view but the actual bridging coordination is driven by the
pilot's Successor block + the worker's runtime `AddGroup`/`RemoveGroup`
path. Enabling this requires `-egress-dedup-cap > 0` and
`-egress-dedup-ttl ≥ 4s` (validated at startup) so the listener's egress
dedup can absorb the duplicate frames a bridging proxy emits.

### `-bridging-window` / `BRIDGING_WINDOW` (default: `0`)

Local floor on the bridging duration. `0` ⇒ honour the pilot's
`TransitionEpoch` verbatim; nonzero ⇒ `MAX(pilot, this)`.

---

## Subtree Group Announcements (BRC-127)

When configured, the listener joins the `GroupSubtreeGroupAnnounce`
(`0xFFFC`) control-plane multicast group and receives `SubtreeGroupAnnounce`
datagrams from block assemblers (via the proxy TCP ingress). Announced
SubtreeIDs are added to a dynamic registry with TTL-based eviction. The
filter treats registry membership as an additional pass condition alongside
static `-subtree-include`.

### `-subtree-groups` / `SUBTREE_GROUPS`

Comma-separated 32-char hex GroupIDs to subscribe to. Each GroupID
identifies a logical subtree group whose membership is announced
dynamically. Empty (the default) disables BRC-127 group filtering entirely.

Example: `bfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbf`

### `-subtree-group-default-ttl` / `SUBTREE_GROUP_DEFAULT_TTL` (default: `900s`)

Default TTL applied to group announcements when the sender transmits
`TTL=0`. After this duration without a refresh, the SubtreeID is evicted
from the registry and will no longer pass the filter.

### `-announce-scope` / `ANNOUNCE_SCOPE` (default: `site`)

Multicast scope(s) for the announcement group join. Comma-separated if
joining multiple scopes. Must match the scope used by the proxy's
multicast egress for the control-plane group.

| Value | Prefix | Reach |
|--------|--------|---------------------------------------------------|
| `link` | `FF02` | Same L2 segment only |
| `site` | `FF05` | Site-local; crosses routers within a site |
| `org` | `FF08` | Organisation-wide |
| `global` | `FF0E` | Internet-wide |

### `-sender-include` / `SENDER_INCLUDE`

Comma-separated IPv6/IPv4 addresses or CIDRs of trusted senders. Applied to
**both** the BRC-127 announcement listener **and** the data-plane workers:
datagrams whose source address is not in the include set (and is not
matched by `-sender-exclude`) are dropped before decode. Data-plane drops
are counted as `bsl_frames_dropped_total{reason="sender_filter"}`.
Empty means accept all senders not matched by `-sender-exclude`.

This forms the trust boundary for pure dynamic-subtree-group filtering:
without static `-subtree-include`, only frames from authorized senders can
reach the registry-based subtree gate.

### `-sender-exclude` / `SENDER_EXCLUDE`

Comma-separated IPv6/IPv4 addresses or CIDRs to reject. Checked before
`-sender-include` and applied to both announcements and data-plane frames.
Empty means exclude nothing.

> **Upgrade note:** prior releases applied `-sender-include` /
> `-sender-exclude` only to BRC-127 announcements. They now also gate
> data-plane frames. If you already configured an include list, ensure
> your proxy / source IP is covered before upgrading.

---

## BRC-132 Subtree Data Reception

BRC-132 carries subtree-level Merkle data (hashes or full nodes) for a given Bitcoin block
subtree. Subtree data frames arrive on `GroupSubtreeDataAnnounce` (`FF0X::B:FFFB`), which
the listener joins only when enabled. They bypass shard/subtree filtering and are forwarded
directly to the configured egress endpoint. Gap tracking runs on a per-subtree flow so that
NACK retransmission can recover lost fragments independently for each subtree.

### `-subtree-data-enabled` / `SUBTREE_DATA_ENABLED` (default: `false`)

Enable BRC-132 subtree data reception. When `true`, the listener joins
`GroupSubtreeDataAnnounce` (`0xFFFB`) in addition to its shard groups and `GroupBlockBroadcast`.
When `false` (the default), the group is not joined and BRC-132 frames are never received.

### `-subtree-data-verify-merkle` / `SUBTREE_DATA_VERIFY_MERKLE` (default: `false`)

Enable optional post-reassembly Merkle root verification for BRC-132 fragments. When `true`,
after all fragments of a subtree data payload are reassembled, the listener verifies that the
reassembled payload is consistent with the SubtreeID (Merkle root). Applies only to
fragmented subtree data; inline unfragmented frames are not verified. This check is
computationally expensive and should be disabled unless data integrity verification is required.

---

## Runtime

### `-workers` / `NUM_WORKERS` (default: `runtime.NumCPU()`)

Number of SO_REUSEPORT receive worker goroutines.

> **Critical — multicast receive:** Linux does **not** load-balance multicast
> datagrams across SO_REUSEPORT sockets. Every socket that has joined a
> multicast group receives a copy of each datagram, so `num_workers > 1`
> causes every frame to be processed and forwarded that many times, inflating
> all metrics and producing duplicate egress datagrams.
> **Always set `NUM_WORKERS=1` when receiving multicast.**
>
> SO_REUSEPORT load balancing applies to unicast UDP only (e.g. the E2E test
> path or a future unicast ingress mode). For the normal multicast-receive
> deployment path, a single worker is correct.

### `-debug` / `DEBUG` (default: `false`)

Enable per-frame debug logging (decode errors, forwarded frames, gap events).

### `-verify-payload-hash` / `VERIFY_PAYLOAD_HASH` (default: `false`)
 
When `true`, verify that the TxID field in BRC-124/BRC-128 frames matches the
SHA256d hash of the payload. Frames with mismatched TxIDs are dropped before
egress and gap tracking, and `bsl_frames_invalid_payload_total` is incremented.
BRC-12 legacy frames are forwarded verbatim regardless of this setting.

### `-require-block-pow` / `REQUIRE_BLOCK_POW` (default: `false`)

Validate block control frames before fan-out. Inter-domain block announcements
reach the listener over the multicast fabric without passing our proxy, so the
listener must independently validate them — this is the permissionless gate
(validate the artifact, not the emitter):

- **BRC-131 block announce** — the in-frame 80-byte header must satisfy proof
  of work: `hash(header) ≤ target(nBits)`, and that target must be at least as
  hard as `-min-pow-bits`. Failing frames are dropped
  (`bsl_frames_dropped_total{reason="block_pow"}`) and not gap-tracked.
- **BRC-133 coinbase** — gated by correlation: the listener records the
  coinbase TxID of every PoW-valid announce and forwards a coinbase frame only
  if its TxID matches one (`reason="coinbase_uncorrelated"` otherwise).

This is anti-spam at fan-out, not consensus validation (no chain context); the
consuming node does full validation. Off by default. BRC-134 anchors are
deliberately ungated.

### `-min-pow-bits` / `MIN_POW_BITS` (default: `0`)

PoW difficulty floor for `-require-block-pow`, in Bitcoin compact `nBits` form
(e.g. `0x1d00ffff`). `0` checks only that the header is self-consistent, which
a forger satisfies by claiming trivial difficulty — set a real floor in
production.

### `-coinbase-corr-cap` / `COINBASE_CORR_CAP` (default: `4096`)

Maximum correlated coinbase TxIDs retained for BRC-133 correlation. `0` disables
coinbase correlation (PoW on announces still applies). Block announcements are
low-rate, so the set stays small.

### `-coinbase-corr-ttl` / `COINBASE_CORR_TTL` (default: `10m`)

Maximum age of a correlated coinbase TxID before it expires from the set.

### `-drain-timeout` / `DRAIN_TIMEOUT` (default: `0`)

Pre-shutdown drain window. When non-zero, `/readyz` returns 503 immediately
on signal receipt while workers continue forwarding for this duration. Useful
for rolling restarts behind a load balancer.

---

## Observability

### `-metrics-addr` / `METRICS_ADDR` (default: `:9200`)

HTTP bind address for:
- `GET /metrics` — Prometheus scrape endpoint
- `GET /healthz` — always `200 OK` while the process is running
- `GET /readyz` — `200` when all workers are ready; `503` while starting or draining

### `-instance` / `INSTANCE_ID` (default: hostname)

OTel `service.instance.id` resource attribute. Useful in federated deployments
to identify individual listener instances.

### `-otlp-endpoint` / `OTLP_ENDPOINT`

gRPC endpoint for OTLP metric push (e.g. `otel-collector:4317`). Empty
disables push export; Prometheus scraping always works regardless.

### `-otlp-interval` / `OTLP_INTERVAL`

Metric export interval for the OTLP push exporter. Default `30s`. Ignored when
`OTLP_ENDPOINT` is empty. Tune down for tighter observability or up to reduce
collector load.

### `-log-format` / `LOG_FORMAT` (default: `text`)

Structured-log output format: `text` (human-readable, stderr; dev default) or
`json` (one JSON object per line on stdout, for fleet aggregation via a
node-local collector). Every line carries the `service.{name,instance.id,version}`
identity triple shared with the OTLP metrics resource attributes. See the
[Unified Logging Plan](https://github.com/lightwebinc/shard-common/blob/main/docs/logging.md).

### `-log-level` / `LOG_LEVEL` (default: `info`)

`debug` | `info` | `warn` | `error`. Runtime-togglable without a restart via
`POST /loglevel?level=<lvl>` on the metrics server and via SIGHUP (toggles
debug↔configured level). `-debug` is retained as a deprecated alias for
`-log-level=debug`.

### `-trace-sampling` / `TRACE_SAMPLING` (default: `0`)

Distributed-trace head sampling ratio `0.0`–`1.0`. `0` installs a no-op tracer
(zero cost). When `> 0` with an `-otlp-endpoint`, control-plane flows (NACK →
recovery, manifest adoption) are traced and exported via the collector; the
receive hot path is never traced.

### `-deployment-id` / `DEPLOYMENT_ID` (default: hostname)

Logical deployment identifier. All HA listener siblings sharing a Redis
must set the same value to dedup their downstream egress. Listeners with
distinct `DEPLOYMENT_ID` values race independently, so each deployment
forwards at most once. Default = hostname makes every listener its own
deployment of one out of the box.

### `-node-id` / `NODE_ID` (default: hostname)

Informational identifier surfaced in metrics labels. Does not participate
in the dedup decision.

The egress dedup and the courtesy ingress mark each use a modular
`shard-common/cache` backend, selected independently. See
[`shard-common/docs/cache-backend.md`](https://github.com/lightwebinc/shard-common/blob/main/docs/cache-backend.md)
for the interface and backend matrix.

### `-egress-dedup-backend` / `EGRESS_DEDUP_BACKEND` (default: inferred)

Tier-2 backend for the egress dedup store: `redis|aerospike|memory|none`. Empty
infers `redis` when `-egress-dedup-redis-addr` is set, else `none` (tier-1 LRU
only). Aerospike seed nodes are set with `-egress-dedup-aerospike-hosts`
(comma-separated `host:port`), namespace `-egress-dedup-aerospike-namespace`
(default `cache`), set `-egress-dedup-aerospike-set` (default `bsl-egr`).

### `-egress-dedup-redis-addr` / `EGRESS_DEDUP_REDIS_ADDR`

Redis-protocol address (Redis/Valkey/Dragonfly) for the per-deployment egress
TxID dedup. Empty falls back to a tier-1 in-process LRU only — useful for
single-listener deployments or when the backend is unavailable.

Final key shape: `<EGRESS_DEDUP_PREFIX><DEPLOYMENT_ID>:<hex-txid>`.

### `-egress-dedup-prefix` / `EGRESS_DEDUP_PREFIX` (default `bsl:egr:`)

Key prefix for the egress dedup namespace. The current `-deployment-id` is
appended to this prefix.

### `-egress-dedup-ttl-redis` / `EGRESS_DEDUP_TTL_REDIS` (default `60s`)

TTL for egress-dedup Redis entries. Must exceed the maximum propagation
delay across HA siblings. Distinct from the legacy `-egress-dedup-ttl`
(the local egress sliding-window dedup TTL for `(groupIdx, subtreeID, SeqNum)`).

### `-egress-dedup-local-cap` / `EGRESS_DEDUP_LOCAL_CAP` (default `1048576`)

Tier-1 local LRU capacity for the egress dedup gate. Set to 0 to disable
the per-deployment dedup feature entirely.

### `-ingress-set-backend` / `INGRESS_SET_BACKEND` (default: inferred)

Tier-2 backend for the courtesy ingress mark: `redis|aerospike|memory|none`.
Empty infers `redis` when `-ingress-set-redis-addr` is set, else `none`
(mark disabled). Aerospike knobs mirror the egress store:
`-ingress-set-aerospike-hosts` / `-ingress-set-aerospike-namespace` (default
`cache`) / `-ingress-set-aerospike-set` (default `bsp-tx`).

### `-ingress-set-redis-addr` / `INGRESS_SET_REDIS_ADDR`

Optional Redis-protocol address for the courtesy SETNX into the local proxy's
ingress namespace. Lets the local proxy know that a TxID is already on
the multicast network even when the proxy itself never saw the upstream
delivery (cross-site bridged TxIDs, side-channel ingress, etc.). Empty
disables the courtesy mark; the egress dedup gate continues to function.

### `-ingress-set-prefix` / `INGRESS_SET_PREFIX` (default `bsp:tx:`)

Key prefix for the courtesy mark. **MUST** match the local proxy's
`-txid-dedup-prefix` exactly; otherwise the proxy is unaware of marks.

### `-ingress-set-ttl` / `INGRESS_SET_TTL` (default `10m`)

TTL for ingress-set marks. SHOULD match the local proxy's
`-txid-dedup-ttl` so cross-bridge dedup windows align.

### `-ingress-set-local-cap` / `INGRESS_SET_LOCAL_CAP` (default `1048576`)

Tier-1 LRU capacity for the ingress mark (avoids redundant async Redis
writes for repeated TxIDs).

### Deprecated TxID dedup flags

The old `-txid-dedup-addr` / `-txid-dedup-prefix` / `-txid-dedup-ttl`
remain accepted as aliases for the corresponding `-egress-dedup-*` flags
and trigger a startup warning. They will be removed in a future release.
A single-listener deployment that previously set these flags continues to
work without changes (deployment-id defaults to hostname).

---

## Example: minimal

```
shard-listener \
  -iface eth0 \
  -shard-bits 2 \
  -egress-addr 127.0.0.1:9100
```

## Example: shard filter + NACK with beacon discovery

```
shard-listener \
  -iface eth0 \
  -shard-bits 8 \
  -shard-include 0,1,2,3 \
  -egress-addr consumer.local:9100 \
  -egress-proto tcp \
  -retry-endpoints retry1.local:9300,retry2.local:9300,retry3.local:9300 \
  -beacon-enabled true \
  -beacon-port 9300 \
  -nack-jitter-max 100ms \
  -nack-max-retries 8 \
  -metrics-addr :9200
```

## Helm chart

Every flag documented in this file is exposed under `.config` in the corresponding Helm chart's `values.yaml`. See the chart repository for installation snippets and the `values.schema.json` for validation rules.

Chart: [`lightwebinc/shard-listener-helm`](https://github.com/lightwebinc/shard-listener-helm) — supports `workloadType=Deployment | DaemonSet`; hardcodes `NUM_WORKERS=1` to avoid SO_REUSEPORT multicast duplication.
