# Testing

Unit tests cover the filter, NACK tracker, egress, and frame-decode paths:

```sh
make test
```

The E2E suite (`test/run-e2e.sh`) starts a listener instance, injects frames
as unicast UDP directly to the listener's bound port, and verifies delivery via
a `sink-test-frames` UDP sink. Four scenarios are exercised sequentially:

1. **Basic delivery** — all frames forwarded; verified by sink count and
   `bsl_frames_forwarded_total` Prometheus metric.
2. **Shard filter** — `-shard-include 0` passes only the group-0 frame.
3. **Strip-header** — on by default, forwards raw payload bytes (the BSV tx);
   `-strip-header=false` forwards the full frame. Sink counts raw datagrams.
4. **BRC-130 fragmentation** — fragmented payloads are reassembled and
   delivered.

The suite requires `shard-proxy` checked out at `../shard-proxy`
(for `send-test-frames`). `make test-e2e` builds all binaries fresh before
running:

```sh
make test-e2e
```

See also [architecture.md#testing](architecture.md#testing) for why unicast injection works.
