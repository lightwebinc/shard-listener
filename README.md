# shard-listener

[![CI](https://github.com/lightwebinc/shard-listener/actions/workflows/ci.yml/badge.svg)](https://github.com/lightwebinc/shard-listener/actions/workflows/ci.yml)
[![CodeQL](https://github.com/lightwebinc/shard-listener/actions/workflows/codeql.yml/badge.svg)](https://github.com/lightwebinc/shard-listener/actions/workflows/codeql.yml)
[![Release](https://img.shields.io/github/v/release/lightwebinc/shard-listener)](https://github.com/lightwebinc/shard-listener/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/lightwebinc/shard-listener.svg)](https://pkg.go.dev/github.com/lightwebinc/shard-listener)
[![Go Report Card](https://goreportcard.com/badge/github.com/lightwebinc/shard-listener)](https://goreportcard.com/report/github.com/lightwebinc/shard-listener)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Multicast subscriber and forwarder for the BSV transaction sharding pipeline.
Receives BRC-124/BRC-128 frames from the `shard-proxy` multicast fabric, applies
shard and subtree filters, forwards matching frames to a configurable downstream
consumer over unicast UDP/TCP and/or multicast egress (domain bridging), and
performs NORM-inspired NACK-based gap recovery via HashKey/SeqNum per-flow
sequence tracking with BRC-126 beacon-discovered retry endpoints and
tier-based escalation.

```
FF05::<shard>:9001  ──multicast──►  shard-listener  ──UDP/TCP──►  downstream :9100
[Control Groups]    ──multicast──►  (BRC-127 SubtreeAnnounce) └─multicast►  FF02::<shard>
                                           │  shard + subtree filter
                                     gap detected
                                           │
                          NACK (BRC-126) ──▼──────►  [nack-addr]:9300
                                           ◄─── ACK / MISS
```

## Features

- **SO_REUSEPORT** multi-worker receive with kernel-level source affinity
- **Shard filter** — subscribe to a subset of shard groups (empty = all)
- **Subtree filter** — include/exclude by 32-byte SubtreeID (BRC-124/BRC-128 frames)
- **BRC-127 subtree group announcements** — dynamic group-based filtering via multicast SubtreeAnnounce datagrams with TTL eviction and sender ACLs
- **Gap tracking** — per-flow HashKey/SeqNum monotonic counter gap detection (BRC-124/BRC-128)
- **NACK dispatch** — 64-byte NACK datagrams (HashKey + StartSeq/EndSeq + SubtreeID) with 16-byte ACK/MISS response handling
- **Beacon discovery** — dynamic retry endpoint registry via BRC-126 ADVERT beacons
- **Tier escalation** — MISS → immediate advance to next endpoint; ACK → gap cancelled
- **Semaphore-bounded dispatch** — concurrent NACK goroutines with configurable limit
- **Egress UDP or TCP** with optional strip-header mode (payload-only)
- **Multicast egress** — optional domain bridging; re-emits filtered frames onto a separate multicast address space with configurable scope, interface, port, and hop limit
- **BRC-135 header egress** — optional; re-emits the 80-byte block header carried in BRC-131 BlockAnnounce frames as a 172-byte BRC-135 frame on a dedicated egress group (`0xFFFA`) for SPV-style consumers
- **Per-deployment egress TxID dedup** — optional shared store (Redis SETNX) to suppress duplicate egress when multiple listeners cover the same shard subset; honours optional ingress courtesy marks from `shard-proxy`
- **Prometheus + OTLP metrics**, `/healthz`, `/readyz`
- **Graceful shutdown** with configurable drain window

## Quick start

```sh
# Subscribe to all groups; forward to localhost:9100 over UDP
shard-listener \
  -iface eth0 \
  -shard-bits 2 \
  -egress-addr 127.0.0.1:9100
```

## Build

```sh
make build       # -> build/shard-listener
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

The suite requires `shard-proxy` checked out at `../shard-proxy`
(for `send-test-frames`). `make test-e2e` builds all binaries fresh before
running:

```sh
make test-e2e
```

## Documentation

- [Architecture](docs/architecture.md)
- [Configuration reference](docs/configuration.md)
- [Protocol specification](https://github.com/lightwebinc/shard-common/blob/main/docs/protocol.md)
- [BRC-126 (Retransmission Protocol)](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-126-retransmission-protocol.md)
- [NACK Retransmission Flow](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/nack-retransmission-flow.md)

## Dependencies

- [`github.com/lightwebinc/shard-common`](https://github.com/lightwebinc/shard-common) — `frame`, `shard` packages
- Prometheus client + OpenTelemetry SDK (same versions as proxy)
- `golang.org/x/net/ipv6` — multicast group join
- `golang.org/x/sys/unix` — `SO_REUSEPORT`

## Container image

The Dockerfile produces a `gcr.io/distroless/static:nonroot` image with the
single static binary at `/usr/local/bin/shard-listener`. Configure via
Helm `values.yaml`, container environment variables, or CLI flags.

## Helm chart

A Kubernetes Helm chart is published from a dedicated chart repository:

- Repository: [`lightwebinc/shard-listener-helm`](https://github.com/lightwebinc/shard-listener-helm)
- HTTPS:
  ```
  helm repo add bsl https://lightwebinc.github.io/shard-listener-helm
  helm install listener bsl/shard-listener
  ```
- OCI: `helm install listener oci://ghcr.io/lightwebinc/charts/shard-listener --version 0.1.0`

Supports `workloadType=Deployment` (default) and `workloadType=DaemonSet`. Every flag accepted by this binary is exposed under `.config` in the chart's `values.yaml`. The chart hardcodes `NUM_WORKERS=1` to avoid SO_REUSEPORT multicast duplication. See the chart README for the full reference.

## License

See [LICENSE](LICENSE).
