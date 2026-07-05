# shard-listener — Architecture

## Overview

`shard-listener` sits downstream of `shard-proxy` in the BSV
transaction distribution pipeline. The proxy multicasts BRC-124/BRC-128 transaction frames
and BRC-131/BRC-132/BRC-134 control-plane frames onto an IPv6 multicast fabric; the listener joins
the relevant groups, filters transaction frames by shard index and/or subtree ID, forwards
matching frames to a configurable unicast downstream over UDP or TCP and/or re-emits them
via multicast egress (domain bridging), and performs NORM-inspired NACK-based gap recovery.

BRC-specific wire formats live
in [bsv-multicast/docs/](https://github.com/lightwebinc/bsv-multicast/tree/main/docs).

```
BSV senders
   │ (TCP or UDP ingress)
   ▼
shard-proxy
   │ BRC-124/BRC-128 frames → FF05::B:<shard>      (data plane)
   │ BRC-131/BRC-134 frames → FF0E::B:FFFE          (GroupBlockBroadcast, always global)
   │ BRC-132 frames         → FF05::B:FFFB          (GroupSubtreeDataAnnounce)
   │ BRC-127 datagrams      → FF05::B:FFFC          (GroupSubtreeGroupAnnounce)
   ▼
Multicast fabric (site-scoped FF05::/16)
   │
   ├── FF05::B:<shard>   BRC-124/BRC-128 transaction frames
   ├── FF0E::B:FFFE      BRC-131 block control + BRC-134 anchor (always joined; global scope)
   ├── FF05::B:FFFB      BRC-132 subtree data (when -subtree-data-enabled)
   ├── FF05::B:FFFC      BRC-127 subtree group announcements (when -subtree-groups set)
   └── FF05::B:FFFD      BRC-126 ADVERT beacon
       │
       └── shard-listener
              ├──▶ unicast UDP/TCP → downstream consumers
              ├──▶ multicast egress (optional) → bridged domain
              ├──▶ header egress BRC-135 (produced from BRC-131 BlockAnnounce, optional)
              └──▶ NACK gap tracking (shard flows + control-plane flows)
```

## SSM (RFC 4607) mode

When `-source-mode=ssm` the listener joins every multicast group as
`(S,G)` via `MCAST_JOIN_SOURCE_GROUP` instead of `(*,G)` via
`IPV6_JOIN_GROUP`. The branch lives in the shared
`shard-common/netjoin` package; the data-plane Worker, BeaconListener,
and SubtreeGroupAnnounceListener all call the same `netjoin.Join(fd, ifIdx,
group, sources)` helper which selects the syscall based on the source
list length.

Source lists per group come from two places:

| Group                          | Source set                                                                                    |
| ------------------------------ | --------------------------------------------------------------------------------------------- |
| Data groups (`FF35::B:idx`)    | `-ssm-publishers-static` (lab/CI) or manifest-derived publisher union (production)             |
| Beacon (`:FFFD`)               | `-ssm-bootstrap-beacon` — the retry-endpoint pod IPs (retry-endpoint emits NACK ADVERTs)       |
| Manifest / BlockBroadcast      | `-ssm-bootstrap-manifest` — the shard-manifest pod IPs                                         |
| SubtreeGroupAnnounce (`:FFFB/FFFC`) | `-ssm-bootstrap-subtree-announce`                                                              |

Bootstrap lists are resolved via the shared `bootstrap.Resolver`
(`shard-common/bootstrap`): DNS names or IPv6 literals; fail-closed
startup; last-good retention on transient refresh failures. The
addressing prefix switches from `FF0x` (ASM) to `FF3x` (SSM) per
`shard.Prefix(SourceModeSSM, scope)` — FF35 for site, FF3E for global
(RFC 8815 rejects global-scope ASM).

See the [SSM Support Plan](https://github.com/lightwebinc/bsv-multicast/blob/main/DESIGN.md#source-specific-multicast-ssm)
for fabric prerequisites (PIM-SSM, MLDv2, raised `mld_max_msf`).

## Receive workers

Each worker:
1. Opens a UDP socket with `SO_REUSEPORT` on the configured listen port.
2. Joins all configured multicast groups on the configured interface (shard groups +
   `GroupBlockBroadcast` always; `GroupSubtreeDataAnnounce` when `-subtree-data-enabled`;
   `GroupSubtreeGroupAnnounce` when `-subtree-groups` is set).
3. Dispatches each received datagram via `processFrame`, which branches on the frame
   version byte before decode:
   - `FrameVerV4` (0x04) → `processBlockFrame` (BRC-131)
   - `FrameVerV5` (0x05) → `processSubtreeDataFrame` (BRC-132)
   - `FrameVerV6` (0x06) → `processAnchorFrame` (BRC-134)
   - `FrameVerV3` (0x03) → fragment reassembly buffer (`Buffer.Observe`)
   - Otherwise → BRC-12/BRC-124/BRC-128 hot path: `frame.Decode`, `shard.Engine.GroupIndex`,
     `filter.Allow`, `egress.Send`, optionally `mcastEgr.Send`
4. Calls `nack.Tracker.Observe` for BRC-124/BRC-128 frames with non-zero `SeqNum`,
   and for BRC-131/BRC-132 frames on their respective control-plane flow keys.

**SO_REUSEPORT and multicast:** Linux does **not** load-balance multicast
datagrams across SO_REUSEPORT sockets — every socket that has joined the group
receives a full copy of each datagram. Running more than one worker therefore
causes every frame to be processed and forwarded multiple times.
**`NUM_WORKERS` must be set to `1` for multicast-receive deployments.**

SO_REUSEPORT load balancing applies to unicast UDP only. The E2E test suite
exploits this property by injecting frames as unicast to `[::]:listen-port`,
allowing multiple worker sockets to be tested in isolation.

## BRC-124/BRC-128 frame format (92 bytes)

The canonical 92-byte header layout is specified in
[BRC-124](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-124-frame-format.md)
(and [BRC-128](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-128-ef-frame-format.md)
for Extended Format payloads) and implemented in `shard-common/frame/frame.go`.
The listener consumes four header fields: `TxID` (shard derivation +
optional payload-hash verification), `HashKey` and `SeqNum` (gap tracking),
and `SubtreeID` (subtree filtering).

`HashKey` is a stable per-flow identifier computed by the proxy as
`XXH64(senderIPv6 ∥ groupIdx ∥ subtreeID)`. It is constant for all frames
in a given (sender, group, subtree) flow. `SeqNum` is a monotonic per-flow
counter starting at 1. Both are stamped in-place by the proxy before multicast
forwarding. Senders (generators) set both to zero. Gap tracking is skipped
when `SeqNum` is zero.

## Gap tracking (NACK / NORM-inspired)

State key: `HashKey`. Per-flow state:
- `lastSeqNum`: the highest `SeqNum` seen so far for this flow.
- `pending`: map from missing `SeqNum` to a `gapEntry`.

Scoping by `HashKey` (which incorporates sender IP, group index, and subtree
ID) ensures sequence chains from different flows are tracked independently;
a gap in one flow does not affect another flow's tail.

**Gap detection** (`nack.Tracker.Observe(hashKey, seqNum)`):
- If `seqNum > lastSeqNum + 1`: a gap exists (frames with `SeqNum` in
  `(lastSeqNum, seqNum)` are missing). A `gapEntry` is added to `pending`
  for each missing sequence number.
- If the incoming `seqNum` matches a pending gap key, the gap is auto-closed
  (retransmit arrived inline). `bsl_gaps_suppressed_total` is incremented.
- Out-of-order or retransmitted frames (`seqNum <= lastSeqNum`) are silently
  accepted; they never create new gap entries and never regress `lastSeqNum`.
- `lastSeqNum` only advances forward.

**Re-baseline on emitter change** (v1.7.0/v1.7.1): a forward jump that is
implausible for the elapsed silence means a *different* emitter now stamps the
flow (e.g. anycast failover between long-lived proxies whose in-memory counters
diverged) — the intermediate range was never emitted toward this listener.
Implausibility is judged two ways: the jump exceeds `-nack-max-forward-jump`
(default 4096, the absolute bound), or it exceeds 4× the frames expected for
the elapsed time at the flow's observed rate (a smoothed inter-arrival EWMA,
`ewmaIPG`, α=1/8; only applied to jumps > 64 frames). On re-baseline the
pending phantom gaps are dropped silently (never NACKed),
`bsl_seq_rebaselines_total` is incremented, and tracking restarts at the new
`SeqNum`. Since v1.7.1, if the rate estimate is settled (≥ 16 contiguous
in-order frames), the rate-plausible *transition tail* (≈ elapsed/`ewmaIPG`
frames just before the new `SeqNum`, bounded) is still NACK-recovered — the new
emitter stamped those frames during reconvergence and holds them in its retry
cache, so a real sub-second outage is repaired instead of vanishing inside the
phantom range.

**Gap fill** (`nack.Tracker.Fill(hashKey, seqNum)`):
- Called when a retransmit arrives via a NACK ACK response. Deletes the
  matching `pending` entry and increments `bsl_gaps_suppressed_total`.

**Sweeper** — fires every 100 ms:
- Entries past `deadline` (detected + `nack-gap-ttl`) are evicted;
  `bsl_gaps_unrecovered_total` is incremented.
- Entries past `nextAttempt` with `retries < nack-max-retries` are enqueued
  on `nackQueue`. `nextAttempt` is advanced immediately before enqueue to
  prevent the same gap from being re-dispatched before a response arrives.
- `nackQueue` consumers send 64-byte NACK datagrams (carrying `HashKey`,
  `StartSeq`, `EndSeq`, and `SubtreeID`) to the current endpoint in the
  sorted registry.

**NACK escalation** on endpoint response:
- **ACK**: gap is cancelled (`Fill`); `bsl_gaps_suppressed_total` incremented.
- **MISS**: endpoint index is advanced immediately (no backoff). The next sweep
  dispatch targets the next endpoint in the sorted registry snapshot.
- **THROTTLED** (`MsgType 0x13`): honest-congestion backoff hint from the
  endpoint. The gap is held on the **same** endpoint for the hinted interval
  (`125 ms << bucket`, bucket carried in the response Flags low nibble); the
  endpoint index is **not** advanced and the failed-round counter is **not**
  incremented (no escalation, no retry-budget consumption). Only the
  sequence/chain/group tiers emit it, and only when the endpoint runs with
  `-rl-throttle-response`.
- **Timeout** (no response within `respTimeout`): exponential backoff applied;
  endpoint index unchanged.

## Unicast NACK recovery (re-inject into fan-out)

By default a repaired frame returns over the **multicast** data plane: the retry
endpoint re-multicasts it and the listener's normal receive path closes the gap.
On a fabric where multicast re-injection is blocked for a remote receiver — e.g.
PIM-SSM RPF only lets the *source* node inject into its own `(S,G)` tree — the
listener can instead recover frames over the **unicast NACK return channel**:

- `Tracker.SetRecoverFunc(func([]byte))` registers a re-inject callback, wired to
  `(*listener.Worker).Reinject`. `Reinject` feeds the frame through the normal
  pipeline (shard filter → own-traffic exclusion → gap tracking → fan-out egress),
  serialised against the receive loop by `Worker.procMu`, so a recovered frame
  reaches downstream consumers with no client-side logic.
- `sendNACK` drains the NACK socket: a retry that has the frame **unicasts it back**
  to the NACK source (plus a `unicast_sent`-flagged ACK). The tracker re-injects the
  data frame and cancels the gap. Classification is by size + magic (a control
  response is 16 bytes; a data frame is a full BRC frame).
- **The ACK alone never confirms recovery** for a unicast retransmit — only the
  arriving data frame does. A `unicast_sent` ACK whose data is lost in transit (the
  retransmit can re-cross the same lossy path) does **not** cancel the gap; it
  **escalates to the next cache**, so a frame lost on one path is repaired from a
  node that received it over a clean path. (A non-unicast/legacy ACK keeps the
  original trust-the-multicast-repair semantics.)
- `Tracker.SetNACKSource(addr)` binds the NACK socket to a routable source address.
  This is **required on a tunnelled fabric**: an unbound socket picks a per-route
  source that can be a point-to-point `/127` tunnel inner address, and the retry's
  unicast reply is then misrouted off the tunnel and lost. Bind a globally routable
  `/128` so the return traffic routes back.

## Beacon discovery

Retry endpoints multicast 56-byte ADVERT datagrams to the beacon group
(`ff05::B:FFFD` for site scope, UDP port 9300 by default). Each ADVERT
carries the endpoint's NACKAddr (unicast IPv6), tier, preference, and flags.

The `discovery.BeaconListener` goroutine joins the beacon group and upserts
endpoints into the `discovery.Registry` on each received ADVERT. The registry
is sorted by **(Tier ASC, Preference DESC)**; beacon-discovered entries sort
before static seeds (seeds use Tier=0xFF).

Endpoints are evicted automatically after 3 × BeaconInterval without a refresh.
The NACK tracker holds a snapshot of the registry at dispatch time, so evictions
take effect at the next gap sweep without locking.

Beacon discovery is enabled by default (`-beacon-enabled`). Static seeds
(`-retry-endpoints`) provide a fallback when no beacons have been received yet
or after eviction.

## Filter

Filtering is pure (no I/O) and allocation-free on the hot path:

| Config | Behaviour |
|-----------------------------|---------------------------------------|
| `shard-include` empty | all shard indices accepted |
| `shard-include` non-empty | only listed indices accepted |
| `subtree-include` empty | all SubtreeIDs accepted |
| `subtree-include` non-empty | only listed IDs accepted |
| `subtree-exclude` | listed IDs dropped; overrides include |

## BRC-12 (legacy) frame support

`frame.Decode` accepts both BRC-12 (44-byte header) and BRC-124/BRC-128 (92-byte header) frames.
BRC-12 frames are decoded with zero-valued `HashKey`, `SeqNum`, and `SubtreeID`.
Shard filtering applies to BRC-12 frames normally; subtree filtering has no effect
(zero `SubtreeID` passes all include/exclude checks). Gap tracking is skipped
for BRC-12 frames because `SeqNum` is zero.

## Control Group Address Table

Control-group indices, canonical addresses, and the virtual flow indices
(0xFFF8/0xFFF9) are specified in
[BRC-129](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-129-multicast-addressing.md).

The listener always joins `GroupBlockBroadcast` (0xFFFE, always global scope)
and `GroupBeacon` (0xFFFD). It joins `GroupSubtreeDataAnnounce` (0xFFFB) only
when `-subtree-data-enabled=true`, and `GroupSubtreeGroupAnnounce` (0xFFFC)
when `-subtree-groups` is configured. `GroupBlockHeader` (0xFFFA) is
egress-only (see block header egress below).

## BRC-131 Block Control Frame Processing

`GroupBlockBroadcast` (0xFFFE) is joined at startup unconditionally. BRC-131 frames
received on this group are dispatched to `processBlockFrame`:

1. Calls `frame.DecodeBlock` to validate and extract block fields.
2. Bypasses the shard/subtree filter (block frames carry no TxID; filtering would be meaningless).
3. Forwards the raw frame via `egress.Sender.SendBlock` to the configured downstream.
4. Calls `nack.Tracker.Observe(uint32(GroupBlockBroadcast), zeroSubtreeID, bf.HashKey, bf.SeqNum, bf.ContentID)`
   for gap tracking on the block control flow.

**Block header egress (BRC-135):** when `-header-egress-enabled=true`, `processBlockFrame`
additionally calls `emitBlockHeader` for `BlockAnnounce` (MsgType 0x01) frames.
`emitBlockHeader` extracts the first 80 bytes of the payload (raw block header) and
re-encodes them as a 172-byte BRC-135 frame (FrameVer `0x07`, 92-byte header + 80-byte
payload) via `frame.EncodeBlockHeader`. The frame is sent to the configured unicast
header egress endpoint.

Per BRC-135, the listener stamps the frame with its **own emitter identity**:

- `HashKey = XXH64(listenerIPv6 ∥ 0xFFFE ∥ zeros[32])` — computed once at startup
  from the configured `-iface` primary IPv6 address (see `primaryIPv6` in `main.go`).
- `SeqNum` — a monotonic per-worker counter (`atomic.Uint64`) starting at 1,
  incremented on every emission.
- `BlockHash` (TxID slot) — copied verbatim from the upstream BRC-131 `ContentID`.

Downstream SPV consumers track gaps on the emitter-attributed `(HashKey, SeqNum)`
flow; if multiple listeners emit headers for the same block, each appears as an
independent flow (matching the "redundant emitters" recovery model in BRC-135 §6).

When `-header-mc-egress-enabled=true`, the BRC-135 frame is also re-emitted to
`GroupBlockHeader` (0xFFFA), allowing SPV consumers to join only that group.

**Reassembly:** fragmented BRC-131 payloads arrive as BRC-130 fragments with `OrigFrameVer=0x04`.
The reassembly buffer's `BlockCallback` is called when all fragments arrive; the completed payload
is delivered via `DeliverReassembledBlock`, which re-encodes it as a valid wire buffer using
`frame.EncodeBlock` before forwarding.

## BRC-132 Subtree Data Frame Processing

`GroupSubtreeDataAnnounce` (0xFFFB) is joined only when `-subtree-data-enabled=true`.
BRC-132 frames on this group are dispatched to `processSubtreeDataFrame`:

1. Calls `frame.DecodeSubtreeData` to validate and extract subtree fields.
2. Bypasses the shard/subtree filter.
3. Forwards the raw frame via `egress.Sender.SendSubtreeData` to the configured downstream.
4. Calls `nack.Tracker.Observe(uint32(GroupSubtreeDataAnnounce), sf.SubtreeID, sf.HashKey, sf.SeqNum, sf.SubtreeID)`
   for gap tracking. Each distinct `SubtreeID` is sequenced independently.

The listener forwards the raw payload without parsing. `MsgType` `0x01` = hashes-only
(32 bytes per node), `0x02` = full-nodes (48 bytes per node); both are forwarded verbatim.

**Reassembly:** fragmented BRC-132 payloads arrive as BRC-130 fragments with `OrigFrameVer=0x05`.
The reassembly buffer's `SubtreeDataCallback` is called on completion. Optional post-reassembly
Merkle root verification is applied if `-subtree-data-verify-merkle=true`. The completed payload
is delivered via `DeliverReassembledSubtreeData`, which re-encodes it via `frame.EncodeSubtreeData`
before forwarding.

## BRC-134 Anchor Transaction Frame Processing

`FrameVerV6` (0x06) anchor frames arrive on `GroupBlockBroadcast` (0xFFFE). The processor:

1. Calls `frame.IsAnchorFrame` to detect, then `frame.DecodeAnchor` to validate.
2. Bypasses the shard/subtree filter (anchors carry no shard semantics).
3. Forwards the raw frame via `egress.Sender.SendAnchor` to the configured downstream.
4. Calls `nack.Tracker.Observe` with a **virtual anchor groupIdx `0xFFF9`** so anchor gap
   tracking has an independent flow label (`brc134`) separate from BRC-131 block control.

The virtual `0xFFF9` is not a real multicast address — it matches the proxy's HashKey
derivation for anchor frames to keep flow identity consistent end to end. See
[bsv-multicast/docs/brc-134-anchor-transactions.md](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-134-anchor-transactions.md).

## BRC-142 Bundle Processing

`FrameVerV8` (0x08) coalescing bundles pack several small transactions into one
datagram to cut fabric pps. `frame.IsBundle(raw)` dispatches them to
`Worker.processBundle` before the ordinary `frame.Decode` path (`Decode` rejects
V8). A bundle is one `(group, subtree)` flow: it carries its own `GroupIdx`,
`SubtreeID`, `HashKey`, and per-flow `SeqNum`, and the shard/subtree filter,
gap-tracking (`nack.Tracker.Observe` with a zero TxID), cross-listener egress
dedup, and local dedup all run at **bundle granularity** in `deliverBundle`.

### Re-bucketing / ShardBits generation alignment

A bundle's `GroupIdx` names a group at the **bundle's own ShardBits generation**
(header `ShardBits` field). If that differs from this listener's live generation,
delivering as-is would filter/route against the wrong groups and over-deliver a
coarser parent bundle to a finer subscriber. So when `b.ShardBits != 0 &&
uint(b.ShardBits) != w.engine.ShardBits()`, `processBundle` re-buckets first via
`shard-common/bundle.Rebucketer` (`Rebucket(sender, b)`): the parent is split and
each member re-coalesced into its correct **local-generation** group, producing
child bundles that are then delivered through the normal `deliverBundle` path. The
re-bucketer is built with `carryTxID=true` so member TxIDs (which may be EF, not
recomputable as SHA256d) survive the split. Children are re-stamped on a fresh
flow keyed by the parent `HashKey` for monotonic per-flow SeqNums.

Own-traffic exclusion is skipped for re-bucketed (cross-generation) flows: it keys
on `HashKey = hash(originalSenderIP, group, subtree)`, which cannot be recomputed
at the local groups from the opaque parent `HashKey` — a documented re-shard
boundary limitation, not a per-tx regression on the common (same-generation) path.
Each re-bucket increments `bsl_bundles_rebucketed_total` (labelled by worker).

### Edge-decoalesce vs consumer-decoalesce (`egress.BundleSink`)

Whether a bundle stays intact end-to-end depends on the egress sink implementing
the optional `egress.BundleSink` capability (`SendBundle(raw, b)`) on top of
`egress.EgressSink`:

- **Plain sink (edge-decoalesce, default):** the sink does **not** implement
  `BundleSink`, so `deliverBundle` splits the bundle with `bundle.Decoalesce(b)`
  and forwards individual BRC-124 frames, each re-stamped with a fresh monotonic
  per-flow egress SeqNum (keyed by the bundle `HashKey`) so downstream consumers
  retain gap detection. The default forwarding contract is unchanged.
- **Bundle-aware sink (consumer-decoalesce):** the fan-out (`fanout.Sink`)
  implements `BundleSink`, so the worker hands it the whole bundle. The fan-out
  then decides **per consumer**: a `BundleCapable` consumer whose own sink is an
  `egress.BundleSink` receives the whole bundle intact (one datagram, many
  transactions — the coalescing saving reaches that consumer's last hop); every
  other consumer gets the bundle decoalesced into re-stamped BRC-124 frames.
  Decoalescing happens at most once per bundle and is shared across all
  non-capable consumers. The multicast re-emit path (`mcastEgr`), when present,
  always receives decoalesced members on its own re-stamp stream.

`egress.DeliverBundle(sink, raw, b)` is the helper a pass-through wrapper uses to
forward a bundle toward a bundle-aware inner sink while still decoalescing
correctly if the inner chain turns out not to be bundle-aware.

## Fragment Reassembly Callbacks

Three callback types are registered on the reassembly buffer (`reassembly.Buffer`):

| Callback | Frame version | Triggered by | Delivers to |
|---|---|---|---|
| `Callback` | V2 (BRC-124/BRC-128) | Fragment set complete | BRC-124/128 egress path; optional SHA256d verification |
| `BlockCallback` | V4 (BRC-131) | Fragment set complete | `DeliverReassembledBlock` |
| `SubtreeDataCallback` | V5 (BRC-132) | Fragment set complete | Merkle verify if enabled → `DeliverReassembledSubtreeData` |

## Egress deduplication

When `-egress-dedup-cap` is non-zero, each worker maintains a fixed-capacity
TTL-bounded set of recently-seen `(groupIdx, subtreeID, seqNum)` keys.
Before forwarding a BRC-124/BRC-128 frame with a non-zero `SeqNum`, the worker
checks the set:

- **First occurrence** — key is inserted; frame is forwarded normally.
- **Duplicate** — key is already present (inline frame and its retransmit both
  arrived); frame is discarded; `bsl_frames_deduped_total` is incremented.
  `nack.Tracker.Observe` still runs so gap-fill bookkeeping stays accurate.

The set is a ring-buffer + hash-map with O(1) insert and lookup. Entries expire
after `-egress-dedup-ttl` (default 2 s). When the capacity is reached the
oldest entry is evicted regardless of TTL. BRC-12 frames and unstamped
BRC-124/BRC-128 frames (`SeqNum == 0`) bypass dedup entirely.

> **Multicast receive:** set `NUM_WORKERS=1` when receiving multicast. Each
> additional worker holds an independent dedup set; duplicates from multiple
> workers are not cross-suppressed.

## Egress

### Unicast egress

A single `egress.Sender` per worker delivers frames to `egress-addr`:

| `egress-proto` | Behaviour |
|----------------|-------------------------------------------------------|
| `udp` | `net.DialUDP` on startup; `Write` per frame |
| `tcp` | lazy connect on first frame; reconnect on write error |

`strip-header=true` (default) sends only the raw BSV transaction bytes (frame
payload) — what a downstream node/miner expects; `strip-header=false` sends the
complete 92-byte BRC-124/BRC-128 frame verbatim, for downstreams that re-read
frames (e.g. domain bridging).

### Multicast egress (domain bridging)

When `-mc-egress-enabled=true`, each worker also holds an `egress.MCastSender`
that re-emits every filtered frame onto a configurable IPv6 multicast address
space. This enables bridging between multicast domains with optional scope
and/or address-space translation.

| Flag | Purpose |
|------|---------------------------------------------------|
| `-mc-egress-iface` | Outbound interface (`IPV6_MULTICAST_IF`) |
| `-mc-egress-port` | Destination UDP port (default: same as `-listen-port`) |
| `-mc-egress-scope` | Scope for egress groups (default: same as `-scope`) |
| `-mc-egress-group-id` | IANA group-id (bytes 12–13) for egress groups (default: same as `-mc-group-id`) |
| `-mc-egress-hoplimit` | `IPV6_MULTICAST_HOPS` (default 1) |

Multicast egress fires independently of unicast egress — both paths execute
for every accepted frame. `strip-header` applies to both egress modes.

The per-frame address derivation is zero-alloc: bytes 0–13 are fixed at
construction (scope prefix, zero IANA boundary, 16-bit group-id); only bytes
14–15 (shard group index) are overwritten per datagram.

### Egress sink seam (`egress.EgressSink`)

The worker forwards through the `egress.EgressSink` interface rather than a
concrete `*egress.Sender`. The default single-destination `Sender` satisfies it,
so the stock listener is unchanged; an alternative sink can be injected via
`listener.New` to receive every frame the worker would otherwise unicast. The
interface mirrors `Sender`'s surface exactly (`Send`, `SendBlock`,
`SendSubtreeData`, `SendRaw`, `Proto`, `Close`) and leaks no routing or policy
detail — analogous to the proxy's pluggable-ingress seam. A downstream build
can plug a multi-consumer sink into this seam without forking.

### Multi-consumer fan-out (`fanout` package)

The `fanout` package is the generic, addressing-agnostic capability for serving
**many consumers with heterogeneous subscriptions from one decode pipeline**.
Running one listener process per consumer is the wrong answer: Linux delivers
each multicast datagram to *every* `SO_REUSEPORT` socket, so N processes pay full
ingress + decode N times before any consumer is served. Instead the worker
decodes once and `fanout.Sink` (an `EgressSink`) delivers to the matching subset
of a **consumer table**:

- **Shard dimension** — inverted into a reverse index (`shardIdx → consumers`),
  so a transaction frame's per-frame cost is `O(consumers-on-that-shard)`, not
  `O(all-consumers)`. Consumers with no shard restriction are an `allShards` set.
- **Subtree dimension** — the residual predicate reuses `filter.Filter.Allow`.
- **Control frames** (BRC-131/132, BRC-135 headers) broadcast to all consumers,
  matching the worker's bypass-filter semantics for those classes.
- **Own-traffic exclusion (opt-in)** — a consumer entry with a non-zero
  `OwnIngressIP` does not receive its own transactions back: a frame (or
  bundle) whose proxy-stamped `HashKey` matches
  `XXH64(OwnIngressIP ∥ groupIdx ∥ SubtreeID)` is skipped for that consumer
  (and surfaced to the optional `IngressObserver` so a downstream build can
  meter the upstream volume). Zero `OwnIngressIP` (the default) keeps full
  delivery.

`fanout.Sink.Apply(consumers)` atomically swaps the table and rebuilds the index;
this is the open end of an external control-plane contract (join-set union +
consumer table). Subscription management, placement, metering, and billing are
out of scope here and belong to whatever downstream build supplies the table.

## Logging & Tracing

The listener uses the shared `shard-common/logging` package: `run` calls
`logging.Init` once, installing a process-wide `slog` default carrying the
`service.{name,instance.id,version}` identity triple (shared with the OTLP
metrics resource attributes). `-log-format json` emits one JSON object per line
on stdout for fleet aggregation; `-log-level` is runtime-togglable via
`POST /loglevel` and SIGHUP. At startup the listener emits a one-shot
`host.inventory` event (OS/CPU/mem/NIC incl. IPv4+IPv6, multicast sysctls) and
a `bsl_host_info` gauge.

**Category-8 OS/NIC logging:** the pilot-driven auto-join path in `main.go`
classifies `AddGroup` failures — an `ENOBUFS` from exceeding `net.ipv6.mld_max_msf`
(the kernel's per-socket source-filter cap, the canonical fleet-scale SSM join
failure) is logged at Error with the errno and a remediation hint, distinct from
generic join failures.

**Tracing** is opt-in (`-trace-sampling > 0` + `-otlp-endpoint`) and
control-plane only — the receive/reassembly hot paths take no span. See the
[Unified Logging Plan](https://github.com/lightwebinc/shard-common/blob/main/docs/logging.md).

## Testing

Worker sockets bind to `[::]:listen-port`, which accepts **both multicast and
unicast** datagrams. The E2E test suite (`test/run-e2e.sh`) exploits this: it
injects frames as plain unicast UDP (`[::1]:listen-port`) using
`send-test-frames` from the proxy repo, bypassing the proxy and the multicast
fabric entirely. This makes E2E tests self-contained and reliable on any Linux
host without requiring kernel multicast loopback support on the loopback
interface.

In production the socket receives multicast frames exclusively; the unicast
receive path is an implementation property of the `[::]` bind address, not an
intended ingress path.
