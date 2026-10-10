# shard-listener

[![CI](https://github.com/lightwebinc/shard-listener/actions/workflows/ci.yml/badge.svg)](https://github.com/lightwebinc/shard-listener/actions/workflows/ci.yml)
[![CodeQL](https://github.com/lightwebinc/shard-listener/actions/workflows/codeql.yml/badge.svg)](https://github.com/lightwebinc/shard-listener/actions/workflows/codeql.yml)
[![Release](https://img.shields.io/github/v/release/lightwebinc/shard-listener)](https://github.com/lightwebinc/shard-listener/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/lightwebinc/shard-listener.svg)](https://pkg.go.dev/github.com/lightwebinc/shard-listener)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

> Part of the [**BSV Layered Multicast**](https://github.com/lightwebinc/bsv-multicast) open-source project — see the main repository for the full architecture, design docs, and BRC specifications.

Multicast subscriber and forwarder for the BSV transaction sharding pipeline.
Receives BRC-124/BRC-128 frames from the `shard-proxy` multicast fabric, applies
shard and subtree filters, forwards matching frames to a configurable downstream
consumer over unicast UDP/TCP and/or multicast egress (domain bridging), and
performs NORM-inspired NACK-based gap recovery via HashKey/SeqNum per-flow
sequence tracking with BRC-126 beacon-discovered retry endpoints and
tier-based escalation.

```
FF05::B:<shard>:9001 ──multicast──►  shard-listener  ──UDP/TCP──►  downstream :9100
[Control Groups]    ──multicast──►  (BRC-127 SubtreeGroupAnnounce) └─multicast►  FF05::B:<shard>
                                           │  shard + subtree filter
                                     gap detected
                                           │
                          NACK (BRC-126) ──▼──────►  [retry-endpoint]:9300
                                           ◄─── ACK / MISS
```

Feature list: [docs/architecture.md#features](docs/architecture.md#features).

## Quick start

```sh
# Subscribe to all groups; forward to localhost:9100 over UDP (ASM, default)
shard-listener \
  -iface eth0 \
  -shard-bits 2 \
  -egress-addr 127.0.0.1:9100
```

SSM (Posture C — requires PIM-SSM in the fabric and raised
`net.ipv6.mld_max_msf`):

```sh
shard-listener \
  -iface eth0 \
  -shard-bits 2 \
  -scope site \
  -source-mode ssm \
  -ssm-bootstrap-manifest shard-manifest-headless.svc.cluster.local \
  -ssm-bootstrap-beacon   retry-endpoint-headless.svc.cluster.local \
  -egress-addr 127.0.0.1:9100
```

## Build

```sh
make build       # -> build/shard-listener
make test        # unit tests (race detector)
make test-e2e    # end-to-end tests (see docs/testing.md)
make docker      # build Docker image
```

## Documentation

- [Architecture](docs/architecture.md)
- [Configuration reference](docs/configuration.md) — every flag, plus the
  `bsl_` metrics reference
- [Testing](docs/testing.md)
- [Unified Component Logging](https://github.com/lightwebinc/shard-common/blob/main/docs/logging.md) — `-log-format json` structured logs, `host.inventory`, `-trace-sampling`, runtime `/loglevel`
- [Protocol specification](https://github.com/lightwebinc/shard-common/blob/main/docs/protocol.md)
- [BRC-126 (Retransmission Protocol)](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-126-retransmission-protocol.md)

## Dependencies

- [`github.com/lightwebinc/shard-common`](https://github.com/lightwebinc/shard-common) — `frame`, `shard` packages
- Prometheus client (`client_golang`, `common`) + OpenTelemetry SDK, OTLP gRPC and Prometheus exporters
- `golang.org/x/sys` — `SO_REUSEPORT`, multicast socket options
- `github.com/alicebob/miniredis/v2` — in-process Redis for egress dedup tests

## Container image

The Dockerfile produces a `gcr.io/distroless/static:nonroot` image with the
single static binary at `/usr/local/bin/shard-listener`. Configure via
Helm `values.yaml`, container environment variables, or CLI flags.

## Helm chart

A Kubernetes Helm chart is published from a dedicated chart repository:

- Repository: [`charts/shard-listener`](https://github.com/lightwebinc/charts/tree/main/charts/shard-listener)
- Install: `helm install shard-listener oci://ghcr.io/lightwebinc/charts/shard-listener` (add `--version` to pin a released chart)

Supports `workloadType=Deployment` (default) and `workloadType=DaemonSet`. Every flag accepted by this binary is exposed under `.config` in the chart's `values.yaml`. The chart hardcodes `NUM_WORKERS=1` to avoid SO_REUSEPORT multicast duplication. See the chart README for the full reference.

## Releases

Releases and release notes live on [GitHub Releases](https://github.com/lightwebinc/shard-listener/releases); there is no CHANGELOG.

## License

See [LICENSE](LICENSE).
