#!/bin/sh
#
# Listener E2E test suite.
#
# Frames are injected by send-test-frames as unicast UDP directly to the
# listener's bound port ([::1]:<port>).  The listener socket is bound to
# [::] so it receives unicast and multicast alike, this lets the suite run
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

shard-listener \
    -iface lo -scope link -shard-bits "$SHARD_BITS" \
    -listen-port 9001 \
    -egress-addr "127.0.0.1:9102" -egress-proto udp -strip-header=false \
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

shard-listener \
    -iface lo -scope link -shard-bits "$SHARD_BITS" \
    -listen-port 9002 \
    -shard-include 0 \
    -egress-addr "127.0.0.1:9103" -egress-proto udp -strip-header=false \
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

shard-listener \
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

shard-listener \
    -iface lo -scope link -shard-bits "$SHARD_BITS" \
    -listen-port 9004 \
    -egress-addr "127.0.0.1:9105" -egress-proto udp -strip-header=false \
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

# ── Test 5: reference lane ────────────────────────────────────────────────────
# Listener with -lane-tx, -lane-block and -lane-header, each to its own TCP
# sink. Transaction frames reach the tx lane as bare payloads; native BRC-131
# BlockAnnounce frames put their bare 80-byte header on the header lane and,
# carrying no whole BRC-144 block, are dropped from the block lane as
# not strippable. Nothing reaches a lane it was not routed to.
echo ""
echo "--- Test 5: reference lane (tx, header, block) ---"

TX_COUNT=$NUM_GROUPS
TX_SIZE=28
HDR_COUNT=3

sink-test-frames -tcp -port 9106 -bytes $(( TX_COUNT * TX_SIZE )) -timeout "$RECV_TIMEOUT" &
S5T=$!
sink-test-frames -tcp -port 9107 -bytes $(( HDR_COUNT * 80 )) -timeout "$RECV_TIMEOUT" &
S5H=$!
sink-test-frames -tcp -port 9108 -bytes 1 -timeout 0 &
S5B=$!

sleep 1

shard-listener \
    -iface lo -scope link -shard-bits "$SHARD_BITS" \
    -listen-port 9005 -require-block-pow=false \
    -lane-tx "127.0.0.1:9106" -lane-header "127.0.0.1:9107" -lane-block "127.0.0.1:9108" \
    -metrics-addr ":9204" -workers 2 -debug &
L5=$!

sleep 1

send-test-frames \
    -addr "[::1]:9005" \
    -shard-bits "$SHARD_BITS" -spread -count 1 -interval 50 -payload-size "$TX_SIZE"
send-test-frames \
    -addr "[::1]:9005" -block-announce -count "$HDR_COUNT" -interval 50

wait "$S5T" && S5T_EXIT=0 || S5T_EXIT=$?
wait "$S5H" && S5H_EXIT=0 || S5H_EXIT=$?

sleep 1
M5=$(curl -sf "http://127.0.0.1:9204/metrics") || M5=""
lane_metric() {
    printf '%s\n' "$M5" | grep "^$1{" | grep "$2" | awk '{sum += $2} END {print int(sum)}'
}
TX_OBJ=$(lane_metric bsl_lane_objects_total 'lane="tx"')
HDR_OBJ=$(lane_metric bsl_lane_objects_total 'lane="header"')
BLK_OBJ=$(lane_metric bsl_lane_objects_total 'lane="block"')
BLK_DROP=$(lane_metric bsl_lane_drops_total 'reason="not_strippable"')
EGR_ERR=$(lane_metric bsl_egress_errors_total 'worker=')

kill "$L5" "$S5B" 2>/dev/null || true
wait "$L5" 2>/dev/null || true
wait "$S5B" 2>/dev/null || true

if [ "$S5T_EXIT" -eq 0 ] && [ "$S5H_EXIT" -eq 0 ] \
    && [ "${TX_OBJ:-0}" -eq "$TX_COUNT" ] && [ "${HDR_OBJ:-0}" -eq "$HDR_COUNT" ] \
    && [ "${BLK_OBJ:-0}" -eq 0 ] && [ "${BLK_DROP:-0}" -eq "$HDR_COUNT" ]; then
    echo "=== PASS: reference lane (tx=$TX_OBJ header=$HDR_OBJ block_not_strippable=$BLK_DROP) ==="
    PASSED=$(( PASSED + 1 ))
else
    echo "=== FAIL: reference lane (tx_sink=$S5T_EXIT header_sink=$S5H_EXIT tx=${TX_OBJ:-0} header=${HDR_OBJ:-0} block=${BLK_OBJ:-0} block_not_strippable=${BLK_DROP:-0} egress_errors=${EGR_ERR:-0}) ==="
    FAILED=$(( FAILED + 1 ))
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "=== E2E results: $PASSED passed, $FAILED failed ==="
[ "$FAILED" -eq 0 ]
