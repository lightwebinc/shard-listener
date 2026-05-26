# bitcoin-shard-listener — Architecture

## Overview

`bitcoin-shard-listener` sits downstream of `bitcoin-shard-proxy` in the BSV
transaction distribution pipeline. The proxy multicasts BRC-124/BRC-128 transaction frames
and BRC-131/BRC-132/BRC-134 control-plane frames onto an IPv6 multicast fabric; the listener joins
the relevant groups, filters transaction frames by shard index and/or subtree ID, forwards
matching frames to a configurable unicast downstream over UDP or TCP and/or re-emits them
via multicast egress (domain bridging), and performs NORM-inspired NACK-based gap recovery.

Foundational concepts (shard hierarchy, anycast ingress, frame versions) live in
[multicast-skills/architecture.md](../../../multicast-skills/architecture.md) and
[multicast-skills/protocol.md](../../../multicast-skills/protocol.md); BRC-specific wire formats
in [bitcoin-multicast/docs/](../../../bitcoin-multicast/docs/).

```
BSV senders
   │ (TCP or UDP ingress)
   ▼
bitcoin-shard-proxy
   │ BRC-124/BRC-128 frames → FF05::B:<shard>      (data plane)
   │ BRC-131/BRC-134 frames → FF0E::B:FFFE          (CtrlGroupControl, always global)
   │ BRC-132 frames         → FF05::B:FFFB          (CtrlGroupSubtreeAnnounce)
   │ BRC-127 datagrams      → FF05::B:FFFC          (CtrlGroupSubtreeGroupAnnounce)
   ▼
Multicast fabric (site-scoped FF05::/16)
   │
   ├── FF05::B:<shard>   BRC-124/BRC-128 transaction frames
   ├── FF0E::B:FFFE      BRC-131 block control + BRC-134 anchor (always joined; global scope)
   ├── FF05::B:FFFB      BRC-132 subtree data (when -subtree-data-enabled)
   ├── FF05::B:FFFC      BRC-127 subtree group announcements (when -subtree-groups set)
   └── FF05::B:FFFD      BRC-126 ADVERT beacon
       │
       └── bitcoin-shard-listener
              ├──▶ unicast UDP/TCP → downstream consumers
              ├──▶ multicast egress (optional) → bridged domain
              ├──▶ header egress BRC-135 (produced from BRC-131 BlockAnnounce, optional)
              └──▶ NACK gap tracking (shard flows + control-plane flows)
```

## Receive workers

Each worker:
1. Opens a UDP socket with `SO_REUSEPORT` on the configured listen port.
2. Joins all configured multicast groups on the configured interface (shard groups +
   `CtrlGroupControl` always; `CtrlGroupSubtreeAnnounce` when `-subtree-data-enabled`;
   `CtrlGroupSubtreeGroupAnnounce` when `-subtree-groups` is set).
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

All multi-byte integers are big-endian. Layout is defined in
`bitcoin-shard-common/frame/frame.go`.

```text
Offset  Size  Align  Field          Value / notes
------  ----  -----  -----          -------------
     0     4   —     Network magic  0xE3E1F3E8
     4     2   —     Protocol ver   0x02BF
     6     1   —     Frame version  0x02 (BRC-124/BRC-128)
     7     1   —     Reserved       0x00
     8    32   8B    TxID           raw 256-bit txid (internal byte order)
    40     8   8B    HashKey        stable per-flow XXH64 identifier; 0 = unset
    48     8   8B    SeqNum         monotonic per-flow counter; 0 = unset
    56    32   8B    SubtreeID      32-byte batch identifier; zeros = unset
    88     4   —     PayloadLen     uint32 BE
    92     *   —     Payload        raw serialised BSV transaction (BRC-12 or BRC-30 EF for BRC-128)
```

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
- **Timeout** (no response within `respTimeout`): exponential backoff applied;
  endpoint index unchanged.

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

| Constant | Index | Canonical Address (group-id `0x000B`) | Purpose |
|---|---|---|---|
| `CtrlGroupBlockHeader` | 0xFFFA | egress-scope `FF0X::<egress-gid>:FFFA` | Block header egress channel (BRC-135) |
| `CtrlGroupSubtreeAnnounce` | 0xFFFB | FF05::B:FFFB (data-plane scope) | BRC-132 subtree data frames |
| `CtrlGroupSubtreeGroupAnnounce` | 0xFFFC | FF05::B:FFFC (data-plane scope) | BRC-127 subtree group announcements |
| `CtrlGroupBeacon` | 0xFFFD | FF05::B:FFFD (site) / FF0E::B:FFFD (global) | ADVERT beacon (BRC-126 discovery) |
| `CtrlGroupControl` | 0xFFFE | **FF0E::B:FFFE (always global)** | BRC-131 block control + BRC-134 anchor frames |
| _(virtual)_ | 0xFFF9 | — | BRC-134 anchor flow identity for gap tracking |

The listener always joins `CtrlGroupControl` and `CtrlGroupBeacon`. It joins
`CtrlGroupSubtreeAnnounce` only when `-subtree-data-enabled=true`, and
`CtrlGroupSubtreeGroupAnnounce` when `-subtree-groups` is configured.

## BRC-131 Block Control Frame Processing

`CtrlGroupControl` (0xFFFE) is joined at startup unconditionally. BRC-131 frames
received on this group are dispatched to `processBlockFrame`:

1. Calls `frame.DecodeBlock` to validate and extract block fields.
2. Bypasses the shard/subtree filter (block frames carry no TxID; filtering would be meaningless).
3. Forwards the raw frame via `egress.Sender.SendBlock` to the configured downstream.
4. Calls `nack.Tracker.Observe(uint32(CtrlGroupControl), zeroSubtreeID, bf.HashKey, bf.SeqNum, bf.ContentID)`
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
`CtrlGroupBlockHeader` (0xFFFA), allowing SPV consumers to join only that group.

**Reassembly:** fragmented BRC-131 payloads arrive as BRC-130 fragments with `OrigFrameVer=0x04`.
The reassembly buffer's `BlockCallback` is called when all fragments arrive; the completed payload
is delivered via `DeliverReassembledBlock`, which re-encodes it as a valid wire buffer using
`frame.EncodeBlock` before forwarding.

## BRC-132 Subtree Data Frame Processing

`CtrlGroupSubtreeAnnounce` (0xFFFB) is joined only when `-subtree-data-enabled=true`.
BRC-132 frames on this group are dispatched to `processSubtreeDataFrame`:

1. Calls `frame.DecodeSubtreeData` to validate and extract subtree fields.
2. Bypasses the shard/subtree filter.
3. Forwards the raw frame via `egress.Sender.SendSubtreeData` to the configured downstream.
4. Calls `nack.Tracker.Observe(uint32(CtrlGroupSubtreeAnnounce), sf.SubtreeID, sf.HashKey, sf.SeqNum, sf.SubtreeID)`
   for gap tracking. Each distinct `SubtreeID` is sequenced independently.

The listener forwards the raw payload without parsing. `MsgType` `0x01` = hashes-only
(32 bytes per node), `0x02` = full-nodes (48 bytes per node); both are forwarded verbatim.

**Reassembly:** fragmented BRC-132 payloads arrive as BRC-130 fragments with `OrigFrameVer=0x05`.
The reassembly buffer's `SubtreeDataCallback` is called on completion. Optional post-reassembly
Merkle root verification is applied if `-subtree-data-verify-merkle=true`. The completed payload
is delivered via `DeliverReassembledSubtreeData`, which re-encodes it via `frame.EncodeSubtreeData`
before forwarding.

## BRC-134 Anchor Transaction Frame Processing

`FrameVerV6` (0x06) anchor frames arrive on `CtrlGroupControl` (0xFFFE). The processor:

1. Calls `frame.IsAnchorFrame` to detect, then `frame.DecodeAnchor` to validate.
2. Bypasses the shard/subtree filter (anchors carry no shard semantics).
3. Forwards the raw frame via `egress.Sender.SendAnchor` to the configured downstream.
4. Calls `nack.Tracker.Observe` with a **virtual anchor groupIdx `0xFFF9`** so anchor gap
   tracking has an independent flow label (`brc134`) separate from BRC-131 block control.

The virtual `0xFFF9` is not a real multicast address — it matches the proxy's HashKey
derivation for anchor frames to keep flow identity consistent end to end. See
[bitcoin-multicast/docs/brc-134-anchor-transactions.md](../../../bitcoin-multicast/docs/brc-134-anchor-transactions.md).

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
after `-egress-dedup-ttl` (default 5 s). When the capacity is reached the
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

`strip-header=true` sends only the raw BSV transaction bytes (frame payload);
`strip-header=false` (default) sends the complete 92-byte BRC-124/BRC-128 frame verbatim.

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
| `-mc-egress-base-addr` | IPv6 bytes 2–12 for egress group space |
| `-mc-egress-hoplimit` | `IPV6_MULTICAST_HOPS` (default 1) |

Multicast egress fires independently of unicast egress — both paths execute
for every accepted frame. `strip-header` applies to both egress modes.

The per-frame address derivation is zero-alloc: bytes 0–12 are fixed at
construction; only bytes 13–15 (group index) are overwritten per datagram.

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
