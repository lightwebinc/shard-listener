#!/bin/sh
#
# Listener E2E test suite.
#
# Frames are injected by send-test-frames as unicast UDP directly to the
# listener's bound port ([::1]:<port>).  The listener socket is bound to
# [::] so it receives unicast and multicast alike — this lets the suite run
# on Linux GitHub Actions runners where loopback IPv6 multicast is
# unreliable, while still exercising the full decode→filter→forward pipeline.
#
# Tests run sequentially; each test uses distinct ports to avoid TIME_WAIT.

SHARD_BITS=${SHARD_BITS:-2}
RECV_TIMEOUT=${RECV_TIMEOUT:-15s}
NUM_GROUPS=$(( 1 << SHARD_BITS ))

echo "=== E2E test suite: shard_bits=$SHARD_BITS num_groups=$NUM_GROUPS ==="

PASSED=0
FAILED=0

# ── Test 1: basic delivery ────────────────────────────────────────────────────
# Inject one frame per shard group directly into the listener's port.
# All NUM_GROUPS frames should pass the (unconfigured) filter and arrive at
# the sink.  Verify via sink exit code and bsl_frames_forwarded_total metric.
echo ""
echo "--- Test 1: basic delivery (expect $NUM_GROUPS frames) ---"

bitcoin-shard-listener \
    -iface lo -scope link -shard-bits "$SHARD_BITS" \
    -listen-port 9001 \
    -egress-addr "127.0.0.1:9102" -egress-proto udp \
    -metrics-addr ":9200" -workers 1 -debug &
L1=$!

sink-test-frames -port 9102 -count "$NUM_GROUPS" -timeout "$RECV_TIMEOUT" &
S1=$!

sleep 1

send-test-frames \
    -addr "[::1]:9001" \
    -shard-bits "$SHARD_BITS" -spread -count 1 -interval 50

wait "$S1" && S1_EXIT=0 || S1_EXIT=$?

sleep 1
FORWARDED1=$(curl -sf "http://127.0.0.1:9200/metrics" \
    | grep '^bsl_frames_forwarded_total{' \
    | awk '{sum += $2} END {print int(sum)}' 2>/dev/null) || FORWARDED1=0

kill "$L1" 2>/dev/null || true
wait "$L1" 2>/dev/null || true

if [ "$S1_EXIT" -eq 0 ] && [ "${FORWARDED1:-0}" -ge "$NUM_GROUPS" ]; then
    echo "=== PASS: basic delivery (forwarded=${FORWARDED1:-0}) ==="
    PASSED=$(( PASSED + 1 ))
else
    echo "=== FAIL: basic delivery (sink_exit=$S1_EXIT forwarded=${FORWARDED1:-0}) ==="
    FAILED=$(( FAILED + 1 ))
fi

# ── Test 2: shard filter ──────────────────────────────────────────────────────
# Listener configured with -shard-include 0.  Of the NUM_GROUPS injected
# frames (one per group), only the group-0 frame should reach the sink.
echo ""
echo "--- Test 2: shard filter (shard-include=0, expect 1 frame) ---"

bitcoin-shard-listener \
    -iface lo -scope link -shard-bits "$SHARD_BITS" \
    -listen-port 9002 \
    -shard-include 0 \
    -egress-addr "127.0.0.1:9103" -egress-proto udp \
    -metrics-addr ":9201" -workers 1 -debug &
L2=$!

sink-test-frames -port 9103 -count 1 -timeout "$RECV_TIMEOUT" &
S2=$!

sleep 1

send-test-frames \
    -addr "[::1]:9002" \
    -shard-bits "$SHARD_BITS" -spread -count 1 -interval 50

wait "$S2" && S2_EXIT=0 || S2_EXIT=$?

kill "$L2" 2>/dev/null || true
wait "$L2" 2>/dev/null || true

if [ "$S2_EXIT" -eq 0 ]; then
    echo "=== PASS: shard filter ==="
    PASSED=$(( PASSED + 1 ))
else
    echo "=== FAIL: shard filter (sink_exit=$S2_EXIT) ==="
    FAILED=$(( FAILED + 1 ))
fi

# ── Test 3: strip-header ──────────────────────────────────────────────────────
# Listener configured with -strip-header.  The sink receives raw payload
# bytes (no frame header).  Use -raw to count datagrams without frame decode.
echo ""
echo "--- Test 3: strip-header (expect $NUM_GROUPS raw datagrams) ---"

bitcoin-shard-listener \
    -iface lo -scope link -shard-bits "$SHARD_BITS" \
    -listen-port 9003 \
    -strip-header \
    -egress-addr "127.0.0.1:9104" -egress-proto udp \
    -metrics-addr ":9202" -workers 1 -debug &
L3=$!

sink-test-frames -port 9104 -count "$NUM_GROUPS" -raw -timeout "$RECV_TIMEOUT" &
S3=$!

sleep 1

send-test-frames \
    -addr "[::1]:9003" \
    -shard-bits "$SHARD_BITS" -spread -count 1 -interval 50

wait "$S3" && S3_EXIT=0 || S3_EXIT=$?

kill "$L3" 2>/dev/null || true
wait "$L3" 2>/dev/null || true

if [ "$S3_EXIT" -eq 0 ]; then
    echo "=== PASS: strip-header ==="
    PASSED=$(( PASSED + 1 ))
else
    echo "=== FAIL: strip-header (sink_exit=$S3_EXIT) ==="
    FAILED=$(( FAILED + 1 ))
fi

# ── Test 4: BRC-130 fragmentation ────────────────────────────────────────────
# Inject fragmented payloads (frag-mtu=300, payload-size=500 → 4 fragments).
# The listener should reassemble all NUM_GROUPS transactions and forward them.
echo ""
echo "--- Test 4: BRC-130 fragmentation (expect $NUM_GROUPS reassembled frames) ---"

bitcoin-shard-listener \
    -iface lo -scope link -shard-bits "$SHARD_BITS" \
    -listen-port 9004 \
    -egress-addr "127.0.0.1:9105" -egress-proto udp \
    -metrics-addr ":9203" -workers 1 -debug &
L4=$!

sink-test-frames -port 9105 -count "$NUM_GROUPS" -timeout "$RECV_TIMEOUT" &
S4=$!

sleep 1

send-test-frames \
    -addr "[::1]:9004" \
    -shard-bits "$SHARD_BITS" -spread -count 1 -interval 50 \
    -frag-mtu 300 -payload-size 500

wait "$S4" && S4_EXIT=0 || S4_EXIT=$?

kill "$L4" 2>/dev/null || true
wait "$L4" 2>/dev/null || true

if [ "$S4_EXIT" -eq 0 ]; then
    echo "=== PASS: BRC-130 fragmentation ==="
    PASSED=$(( PASSED + 1 ))
else
    echo "=== FAIL: BRC-130 fragmentation (sink_exit=$S4_EXIT) ==="
    FAILED=$(( FAILED + 1 ))
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "=== E2E results: $PASSED passed, $FAILED failed ==="
[ "$FAILED" -eq 0 ]
