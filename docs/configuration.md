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

---

## Egress (unicast downstream)

### `-egress-addr` / `EGRESS_ADDR` (default: `127.0.0.1:9100`)

Downstream unicast `host:port`. Frames passing the filter are forwarded here.

### `-egress-proto` / `EGRESS_PROTO` (default: `udp`)

Egress protocol: `udp` or `tcp`.

- **UDP** — one datagram per frame; no connection state.
- **TCP** — persistent connection; reconnects automatically on error.

### `-strip-header` / `STRIP_HEADER` (default: `false`)

When `true`, only the raw BSV transaction payload is forwarded (no frame
header). When `false`, the complete 92-byte BRC-124/BRC-128 frame is forwarded verbatim.

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

## Block Header Egress (BRC-131)

When BRC-131 block control frames are received, the listener can extract the 80-byte block
header from `BlockAnnounce` frames and re-emit it as a stripped 172-byte BRC-131 frame
(92-byte header + 80-byte payload). This provides a lightweight SPV consumer path without
requiring consumers to process full block announcement payloads. Header egress runs
independently of the normal unicast egress (`-egress-addr`); both can be active simultaneously.

### `-header-egress-enabled` / `HEADER_EGRESS_ENABLED` (default: `false`)

Enable unicast block header retransmission. When `true`, `BlockAnnounce` frames trigger
extraction and re-encoding of the 80-byte block header as a stripped BRC-131 frame.

### `-header-egress-addr` / `HEADER_EGRESS_ADDR` (default: `127.0.0.1:9101`)

Downstream unicast `host:port` for stripped block headers. Headers are sent as 172-byte
BRC-131 frames (92-byte header + 80-byte block header payload).

### `-header-egress-proto` / `HEADER_EGRESS_PROTO` (default: `udp`)

Transport for unicast header egress: `udp` or `tcp`. TCP reconnects automatically on error.

### `-header-mc-egress-enabled` / `HEADER_MC_EGRESS_ENABLED` (default: `false`)

Enable multicast block header retransmission. When `true`, stripped block header frames
are re-emitted to `CtrlGroupBlockHeader` (`FF0X::B:FFFA`). SPV consumers join this group
rather than `CtrlGroupControl` (`FF0X::B:FFFE`) to receive headers only.

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
counted). Example: `10.0.0.1:9002,10.0.0.2:9002`.

### `-nack-jitter-max` / `NACK_JITTER_MAX` (default: `200ms`)

Maximum random hold-off before the first NACK is dispatched (NORM suppression
window). Prevents NACK implosion when many listeners detect the same gap.

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

---

## Egress Deduplication

When both an inline frame **and** its retransmit arrive at the listener, the
downstream consumer would otherwise receive the same transaction twice. Egress
dedup suppresses the second delivery.

### `-egress-dedup-cap` / `EGRESS_DEDUP_CAP` (default: `0`)

Capacity of the egress dedup set (number of `(groupIdx, subtreeID, seqNum)`
entries). `0` disables dedup entirely. A value of `65536` is sufficient for
~10 minutes of sustained traffic at 100 TPS with 10% gap rate.

### `-egress-dedup-ttl` / `EGRESS_DEDUP_TTL` (default: `5s`)

TTL for entries in the egress dedup set. Frames with the same `SeqNum` seen
within this window are suppressed. Set to at least the maximum expected
retransmit delay (typically `nack-backoff-max` + one sweep interval = 5.1 s).
Entries also evict on capacity overflow regardless of TTL.

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

## Subtree Group Announcements (BRC-127)

When configured, the listener joins the `CtrlGroupSubtreeGroupAnnounce`
(`0xFFFC`) control-plane multicast group and receives `SubtreeAnnounce`
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
subtree. Subtree data frames arrive on `CtrlGroupSubtreeAnnounce` (`FF0X::B:FFFB`), which
the listener joins only when enabled. They bypass shard/subtree filtering and are forwarded
directly to the configured egress endpoint. Gap tracking runs on a per-subtree flow so that
NACK retransmission can recover lost fragments independently for each subtree.

### `-subtree-data-enabled` / `SUBTREE_DATA_ENABLED` (default: `false`)

Enable BRC-132 subtree data reception. When `true`, the listener joins
`CtrlGroupSubtreeAnnounce` (`0xFFFB`) in addition to its shard groups and `CtrlGroupControl`.
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

### `-deployment-id` / `DEPLOYMENT_ID` (default: hostname)

Logical deployment identifier. All HA listener siblings sharing a Redis
must set the same value to dedup their downstream egress. Listeners with
distinct `DEPLOYMENT_ID` values race independently, so each deployment
forwards at most once. Default = hostname makes every listener its own
deployment of one out of the box.

### `-node-id` / `NODE_ID` (default: hostname)

Informational identifier surfaced in metrics labels. Does not participate
in the dedup decision.

### `-egress-dedup-redis-addr` / `EGRESS_DEDUP_REDIS_ADDR`

Redis address for the per-deployment egress TxID dedup. Empty falls back
to a tier-1 in-process LRU only — useful for single-listener deployments
or when Redis is unavailable.

Final Redis key shape: `<EGRESS_DEDUP_PREFIX><DEPLOYMENT_ID>:<hex-txid>`.

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

### `-ingress-set-redis-addr` / `INGRESS_SET_REDIS_ADDR`

Optional Redis address for the courtesy SETNX into the local proxy's
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
