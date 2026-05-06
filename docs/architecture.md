# bitcoin-shard-listener — Architecture

## Overview

`bitcoin-shard-listener` sits downstream of `bitcoin-shard-proxy` in the BSV
transaction distribution pipeline. The proxy multicasts BRC-124 frames onto an
IPv6 multicast fabric; the listener joins the relevant groups, filters frames
by shard index and/or subtree ID, forwards matching frames to a configurable
unicast downstream over UDP or TCP and/or re-emits them via multicast egress
(domain bridging), and performs NORM-inspired NACK-based gap recovery.

```
BSV senders
   │ (TCP or UDP ingress)
   ▼
bitcoin-shard-proxy
   │ BRC-124 frames, PrevSeq/CurSeq stamped in-place at bytes 40–55 (XXH64)
   │ IPv6 multicast  FF05::<group-index>
   ▼
Multicast fabric (site-scoped FF05::/16)
   │
   ├── direct subscribers (miners, exchanges, …)
   │
   └── bitcoin-shard-listener
          │ filter → egress
          ├──▶ unicast UDP/TCP → downstream consumers
          └──▶ multicast egress (optional) → bridged domain
```

## Receive workers

Each worker:
1. Opens a UDP socket with `SO_REUSEPORT` on the configured listen port.
2. Joins all configured multicast groups on the configured interface.
3. Calls `frame.Decode`, `shard.Engine.GroupIndex`, `filter.Allow`,
   `egress.Send`, and optionally `mcastEgr.Send` in the hot path for every
   received datagram.
4. Calls `nack.Tracker.Observe` for BRC-124 frames with non-zero `CurSeq`.

**SO_REUSEPORT and multicast:** Linux does **not** load-balance multicast
datagrams across SO_REUSEPORT sockets — every socket that has joined the group
receives a full copy of each datagram. Running more than one worker therefore
causes every frame to be processed and forwarded multiple times.
**`NUM_WORKERS` must be set to `1` for multicast-receive deployments.**

SO_REUSEPORT load balancing applies to unicast UDP only. The E2E test suite
exploits this property by injecting frames as unicast to `[::]:listen-port`,
allowing multiple worker sockets to be tested in isolation.

## BRC-124 frame format (92 bytes)

All multi-byte integers are big-endian. Layout is defined in
`bitcoin-shard-common/frame/frame.go`.

```text
Offset  Size  Align  Field          Value / notes
------  ----  -----  -----          -------------
     0     4   —     Network magic  0xE3E1F3E8
     4     2   —     Protocol ver   0x02BF
     6     1   —     Frame version  0x02 (BRC-124)
     7     1   —     Reserved       0x00
     8    32   8B    TxID           raw 256-bit txid (internal byte order)
    40     8   8B    PrevSeq        XXH64 of previous chain state; 0 = unset
    48     8   8B    CurSeq         XXH64 of current chain state; 0 = unset
    56    32   8B    SubtreeID      32-byte batch identifier; zeros = unset
    88     4   —     PayloadLen     uint32 BE
    92     *   —     Payload        raw serialised BSV transaction
```

`PrevSeq` and `CurSeq` form a hash chain: each frame's `PrevSeq` equals the
`CurSeq` of its predecessor in the sender's per-(sender IP, group) sequence.
Both are computed by the proxy (`bitcoin-shard-proxy`) as
`XXH64(senderIPv6 ∥ groupIdx ∥ monotonicCounter)` and stamped in-place before
multicast forwarding. Senders (generators) set both to zero. Gap tracking is
skipped when `CurSeq` is zero.

## Gap tracking (NACK / NORM-inspired)

State key: `groupIdx`. Per-group state:
- `lastCurSeq`: the highest `CurSeq` seen so far for this group.
- `pending`: map from `CurSeq` of the last-in-gap frame to a `gapEntry`.

**Gap detection** (`nack.Tracker.Observe(groupIdx, prevSeq, curSeq, txID)`):
- If `prevSeq != 0` and `prevSeq > lastCurSeq`: a gap exists (frames with
  `CurSeq` in `(lastCurSeq, prevSeq]` are missing). A `gapEntry` is added to
  `pending` keyed by `prevSeq`.
- If the incoming `curSeq` matches a pending gap key, the gap is auto-closed
  (retransmit arrived inline). `bsl_gaps_suppressed_total` is incremented.
- Out-of-order or retransmitted frames (`prevSeq < lastCurSeq`) are silently
  accepted; they never create new gap entries and never regress `lastCurSeq`.
- `lastCurSeq` only advances forward.

**Gap fill** (`nack.Tracker.Fill(groupIdx, curSeq)`):
- Called when a retransmit arrives via a NACK ACK response. Deletes the
  matching `pending` entry and increments `bsl_gaps_suppressed_total`.

**Sweeper** — fires every 100 ms:
- Entries past `deadline` (detected + `nack-gap-ttl`) are evicted;
  `bsl_gaps_unrecovered_total` is incremented.
- Entries past `nextAttempt` with `retries < nack-max-retries` are enqueued
  on `nackQueue`. `nextAttempt` is advanced immediately before enqueue to
  prevent the same gap from being re-dispatched before a response arrives.
- `nackQueue` consumers send 24-byte NACK datagrams (forward by `PrevSeq` +
  backward by `CurSeq`) to the current endpoint in the sorted registry.

**NACK escalation** on endpoint response:
- **ACK**: gap is cancelled (`Fill`); `bsl_gaps_suppressed_total` incremented.
- **MISS**: endpoint index is advanced immediately (no backoff). The next sweep
  dispatch targets the next endpoint in the sorted registry snapshot.
- **Timeout** (no response within `respTimeout`): exponential backoff applied;
  endpoint index unchanged.

## Beacon discovery

Retry endpoints multicast 56-byte ADVERT datagrams to the beacon group
(`ff05::ff:fffd` for site scope, UDP port 9300 by default). Each ADVERT
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

## V1 frame support

`frame.Decode` accepts both v1 (44-byte header) and BRC-124 (92-byte header) frames.
v1 frames are decoded with zero-valued `PrevSeq`, `CurSeq`, and `SubtreeID`.
Shard filtering applies to v1 frames normally; subtree filtering has no effect
(zero `SubtreeID` passes all include/exclude checks). Gap tracking is skipped
for v1 frames because `CurSeq` is zero.

## Egress

### Unicast egress

A single `egress.Sender` per worker delivers frames to `egress-addr`:

| `egress-proto` | Behaviour |
|----------------|-------------------------------------------------------|
| `udp` | `net.DialUDP` on startup; `Write` per frame |
| `tcp` | lazy connect on first frame; reconnect on write error |

`strip-header=true` sends only the raw BSV transaction bytes (frame payload);
`strip-header=false` (default) sends the complete 92-byte BRC-124 frame verbatim.

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
