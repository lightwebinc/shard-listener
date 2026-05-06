# bitcoin-shard-listener

Multicast subscriber and forwarder for the BSV transaction sharding pipeline.
Receives BRC-124 frames from the `bitcoin-shard-proxy` multicast fabric, applies
shard and subtree filters, forwards matching frames to a configurable downstream
consumer over unicast UDP/TCP and/or multicast egress (domain bridging), and
performs NORM-inspired NACK-based gap recovery via PrevSeq/CurSeq hash-chain
tracking with BRC-TBD-retransmission beacon-discovered retry endpoints and
tier-based escalation.

## Features

- **SO_REUSEPORT** multi-worker receive with kernel-level source affinity
- **Shard filter** — subscribe to a subset of shard groups (empty = all)
- **Subtree filter** — include/exclude by 32-byte SubtreeID (BRC-124 frames)
- **Gap tracking** — per-group PrevSeq/CurSeq hash-chain gap detection (BRC-124)
- **NACK dispatch** — 24-byte NACK datagrams (LookupType + LookupSeq) with 16-byte ACK/MISS response handling
- **Beacon discovery** — dynamic retry endpoint registry via BRC-TBD-retransmission ADVERT beacons
- **Tier escalation** — MISS → immediate advance to next endpoint; ACK → gap cancelled
- **Semaphore-bounded dispatch** — concurrent NACK goroutines with configurable limit
- **Egress UDP or TCP** with optional strip-header mode (payload-only)
- **Multicast egress** — optional domain bridging; re-emits filtered frames onto a separate multicast address space with configurable scope, interface, port, and hop limit
- **Prometheus + OTLP metrics**, `/healthz`, `/readyz`
- **Graceful shutdown** with configurable drain window

## Quick start

```sh
# Subscribe to all groups; forward to localhost:9100 over UDP
bitcoin-shard-listener \
  -iface eth0 \
  -shard-bits 2 \
  -egress-addr 127.0.0.1:9100
```

## Build

```sh
make build       # -> build/bitcoin-shard-listener
make test        # unit tests (race detector)
make test-e2e    # end-to-end tests (see Testing below)
make docker      # build Docker image
```

## Testing

Unit tests cover the filter, NACK tracker, egress, and frame-decode paths:

```sh
make test
```

The E2E suite (`test/run-e2e.sh`) starts a listener instance, injects frames
as unicast UDP directly to the listener's bound port, and verifies delivery via
a `sink-test-frames` UDP sink. Three scenarios are exercised sequentially:

1. **Basic delivery** — all frames forwarded; verified by sink count and
   `bsl_frames_forwarded_total` Prometheus metric.
2. **Shard filter** — `-shard-include 0` passes only the group-0 frame.
3. **Strip-header** — `-strip-header` forwards raw payload bytes; sink counts
   raw datagrams.

The suite requires `bitcoin-shard-proxy` checked out at `../bitcoin-shard-proxy`
(for `send-test-frames`). `make test-e2e` builds all binaries fresh before
running:

```sh
make test-e2e
```

## Documentation

- [Architecture](docs/architecture.md)
- [Configuration reference](docs/configuration.md)
- [Protocol specification](https://github.com/lightwebinc/bitcoin-shard-common/blob/main/docs/protocol.md)
- [BRC-TBD-retransmission (Retransmission Protocol)](https://github.com/lightwebinc/bitcoin-multicast/blob/main/docs/brc-tbd-retransmission-protocol.md)
- [NACK Retransmission Flow](https://github.com/lightwebinc/bitcoin-multicast/blob/main/docs/nack-retransmission-flow.md)

## Dependencies

- [`github.com/lightwebinc/bitcoin-shard-common`](https://github.com/lightwebinc/bitcoin-shard-common) — `frame`, `shard`, `sequence` packages
- Prometheus client + OpenTelemetry SDK (same versions as proxy)
- `golang.org/x/net/ipv6` — multicast group join
- `golang.org/x/sys/unix` — `SO_REUSEPORT`

## License

See [LICENSE](LICENSE).
