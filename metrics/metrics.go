// Package metrics initialises an OpenTelemetry MeterProvider backed by both
// a Prometheus exporter (for scraping) and an optional OTLP gRPC exporter
// (for push-based delivery to any OTel-compatible backend).
//
// All instrument handles are allocated once at [New] time. Record methods use
// them directly — no map lookups on the critical path.
package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	prometheusexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/exemplar"
	"go.opentelemetry.io/otel/sdk/resource"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/lightwebinc/shard-common/logging"
)

// ServiceName is the OTel service.name resource attribute value.
const ServiceName = "shard-listener"

// Version is set at build time via -ldflags "-X metrics.Version=<ver>".
var Version = "dev"

// Recorder holds all pre-allocated metric handles and readiness state.
//
// Hot-path counters (framesReceived/Forwarded/Dropped/Deduped/... ) use
// prometheus client_golang directly because the OTel SDK Add path adds
// ~⅓ of total CPU on a packet-rate-bound workload (measured on shard-proxy
// at 256 B). Cold-path
// counters stay on OTel.
type Recorder struct {
	provider   *sdkmetric.MeterProvider
	promReg    promclient.Gatherer
	promOtel   *promclient.Registry // shared with OTel exporter; hot-path counters registered here too
	levelVar   *slog.LevelVar
	numWorkers int
	startTime  time.Time
	readyCount atomic.Int32
	draining   atomic.Bool
	shutdownFn func(context.Context) error

	// ── Hot-path counters — direct prometheus client_golang ──
	promFramesReceived       *promclient.CounterVec // worker, iface, version
	promFramesDropped        *promclient.CounterVec // worker, reason
	promBundlesRebucketed    *promclient.CounterVec // worker
	promRebucketUnguarded    *promclient.CounterVec // worker
	promFramesForwarded      *promclient.CounterVec // worker, proto
	promFramesInvalidPayload *promclient.CounterVec // worker
	promFramesDeduped        *promclient.CounterVec // worker
	promFramesTxDeduped      *promclient.CounterVec // worker
	promEgressErrors         *promclient.CounterVec // worker
	promMCEgressErrors       *promclient.CounterVec // worker

	// BRC-130 reassembly counters — cold path (only fires on > MTU frames).
	frameReassemblyStarted      metric.Int64Counter
	frameReassemblyCompleted    metric.Int64Counter
	frameReassemblyAbandoned    metric.Int64Counter
	frameReassemblyHashMismatch metric.Int64Counter
	txDedupErrors               metric.Int64Counter // Redis errors during TxID claim

	// Egress / ingress TxID dedup outcomes (txidset.Store callbacks)
	egressClaimLocalHit metric.Int64Counter
	egressClaimWon      metric.Int64Counter
	egressClaimLost     metric.Int64Counter
	egressClaimError    metric.Int64Counter
	ingressMarkSet      metric.Int64Counter
	ingressMarkExisted  metric.Int64Counter
	ingressMarkError    metric.Int64Counter
	ingressMarkDropped  metric.Int64Counter

	// Block header egress counters
	headerForwarded    metric.Int64Counter
	headerEgressErrors metric.Int64Counter

	// NACK / gap counters
	tailProbesSent      metric.Int64Counter
	tailProbesRecovered metric.Int64Counter
	tailProbesMiss      metric.Int64Counter
	gapsDetected        metric.Int64Counter
	gapsSuppressed      metric.Int64Counter // cancelled by retransmit fill or ACK response
	flowsRefused        metric.Int64Counter // new flows skipped at the MaxFlows flood-guard cap
	seqRebaselines      metric.Int64Counter // flows re-baselined on an implausible SeqNum jump (emitter change)
	nacksDispatched     metric.Int64Counter
	nacksThrottled      metric.Int64Counter // held after a THROTTLED congestion signal
	nacksUnrecovered    metric.Int64Counter // retries exhausted or TTL exceeded

	// BRC-127 subtree group announce counters
	subtreeGroupAnnouncesReceived metric.Int64Counter
	subtreeGroupAnnouncesRejected metric.Int64Counter
	subtreeGroupEvictions         metric.Int64Counter

	// Subtree group registry size (updated by evict loop)
	subtreeGroupEntries atomic.Int64

	// Beacon discovery counter
	beaconAdvertsReceived metric.Int64Counter

	// Beacon registry endpoint count (updated by evict loop)
	beaconRegistryEndpoints atomic.Int64

	// BRC-139 ShardManifest counters and gauges. Updated by the
	// auto-config consumer subsystem (shard-common/manifest).
	manifestReceived      metric.Int64Counter
	manifestPilotsKnown   atomic.Int64
	manifestQuorumMetBits atomic.Int32 // bitmap: bit0 shard_bits, bit1 source_mode, bit2 successor
	// Unix seconds of the most recent divergence per field. Read by an
	// observable gauge, so a stale timestamp is the signal — a field that
	// stops diverging keeps its last value rather than disappearing.
	lastDivergenceMu       sync.Mutex
	lastDivergence         map[string]int64
	manifestDivergence     metric.Int64Counter
	manifestAdoption       metric.Int64Counter
	manifestReshardState   atomic.Int32 // 0 steady, 1 bridging, 2 cutover-pending
	manifestReshardWindow  atomic.Int64 // seconds until TransitionEpoch (negative briefly during cutover)
	manifestReshardEmitDup metric.Int64Counter
}

// New constructs and returns a Recorder.
func New(instanceID string, numWorkers int, otlpEndpoint string, otlpInterval time.Duration) (*Recorder, error) {
	if instanceID == "" {
		h, err := os.Hostname()
		if err != nil {
			h = "unknown"
		}
		instanceID = h
	}

	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			attribute.String("service.name", ServiceName),
			attribute.String("service.instance.id", instanceID),
			attribute.String("service.version", Version),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("metrics: build resource: %w", err)
	}

	reg := promclient.NewRegistry()
	promExp, err := prometheusexporter.New(prometheusexporter.WithRegisterer(reg))
	if err != nil {
		return nil, fmt.Errorf("metrics: prometheus exporter: %w", err)
	}

	runtimeReg := promclient.NewRegistry()
	runtimeReg.MustRegister(collectors.NewGoCollector())
	runtimeReg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	mpOpts := []sdkmetric.Option{
		sdkmetric.WithReader(promExp),
		sdkmetric.WithResource(res),
		// Exemplars require a trace context per measurement and add ~11%
		// of cumulative CPU on the hot path (aggregate.Builder.filter).
		// shard-listener doesn't emit traces, so they are never useful.
		sdkmetric.WithExemplarFilter(exemplar.AlwaysOffFilter),
	}

	var shutdownFuncs []func(context.Context) error

	if otlpEndpoint != "" {
		otlpExp, oerr := otlpmetricgrpc.New(
			context.Background(),
			otlpmetricgrpc.WithEndpoint(otlpEndpoint),
			otlpmetricgrpc.WithInsecure(),
		)
		if oerr != nil {
			return nil, fmt.Errorf("metrics: OTLP exporter: %w", oerr)
		}
		mpOpts = append(mpOpts, sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(otlpExp, sdkmetric.WithInterval(otlpInterval)),
		))
		shutdownFuncs = append(shutdownFuncs, otlpExp.Shutdown)
		slog.Info("OTLP exporter enabled", "endpoint", otlpEndpoint, "interval", otlpInterval)
	}

	mp := sdkmetric.NewMeterProvider(mpOpts...)
	shutdownFuncs = append(shutdownFuncs, mp.Shutdown)

	r := &Recorder{
		provider:       mp,
		promReg:        promclient.Gatherers{reg, runtimeReg},
		numWorkers:     numWorkers,
		startTime:      time.Now(),
		lastDivergence: make(map[string]int64),
		shutdownFn: func(ctx context.Context) error {
			var last error
			for _, fn := range shutdownFuncs {
				if err := fn(ctx); err != nil {
					last = err
				}
			}
			return last
		},
	}

	meter := mp.Meter(ServiceName)

	// Hot-path counters: direct prometheus client_golang. Same names and
	// label sets as before so dashboards keep working.
	r.promFramesReceived = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsl_frames_received_total",
		Help: "Multicast frames received",
	}, []string{"worker", "network_interface_name", "version"})
	r.promFramesDropped = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsl_frames_dropped_total",
		Help: "Frames dropped before egress",
	}, []string{"worker", "reason"})
	r.promBundlesRebucketed = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsl_bundles_rebucketed_total",
		Help: "BRC-142 bundles re-bucketed to the local ShardBits generation before delivery (cross-generation/re-shard)",
	}, []string{"worker"})
	r.promRebucketUnguarded = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsl_rebucket_unguarded_total",
		Help: "Bundles re-bucketed on a listener NOT configured as a re-bucket relay (-rebucket-relay) — a generation-mismatch alarm: parent-stream recovery is active, but re-multicast children are unrecoverable without a child-generation retry. Alert on rate > 0.",
	}, []string{"worker"})
	r.promFramesForwarded = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsl_frames_forwarded_total",
		Help: "Frames forwarded to downstream unicast",
	}, []string{"worker", "proto"})
	r.promFramesInvalidPayload = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsl_frames_invalid_payload_total",
		Help: "BRC-124/BRC-128 frames dropped because SHA256d(payload) != TxID",
	}, []string{"worker"})
	r.promFramesDeduped = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsl_frames_deduped_total",
		Help: "BRC-124/BRC-128 retransmits suppressed before egress (egress dedup)",
	}, []string{"worker"})
	r.promFramesTxDeduped = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsl_frames_tx_deduped_total",
		Help: "Frames suppressed by Redis TxID claim (cross-listener dedup)",
	}, []string{"worker"})
	r.promEgressErrors = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsl_egress_errors_total",
		Help: "Errors sending to downstream",
	}, []string{"worker"})
	r.promMCEgressErrors = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsl_mc_egress_errors_total",
		Help: "Errors sending to multicast egress",
	}, []string{"worker"})
	for _, c := range []promclient.Collector{
		r.promFramesReceived, r.promFramesDropped, r.promFramesForwarded,
		r.promFramesInvalidPayload, r.promFramesDeduped, r.promFramesTxDeduped,
		r.promEgressErrors, r.promMCEgressErrors, r.promBundlesRebucketed,
		r.promRebucketUnguarded,
	} {
		if regErr := reg.Register(c); regErr != nil {
			return nil, fmt.Errorf("metrics: register hot-path counter: %w", regErr)
		}
	}
	r.promOtel = reg

	if r.frameReassemblyStarted, err = meter.Int64Counter("bsl_reassembly_started_total",
		metric.WithDescription("BRC-130 reassembly slots opened (first fragment of a TxID received)")); err != nil {
		return nil, err
	}
	if r.frameReassemblyCompleted, err = meter.Int64Counter("bsl_reassembly_completed_total",
		metric.WithDescription("BRC-130 reassemblies that produced a complete payload")); err != nil {
		return nil, err
	}
	if r.frameReassemblyAbandoned, err = meter.Int64Counter("bsl_reassembly_abandoned_total",
		metric.WithDescription("BRC-130 reassembly slots evicted before completion (TTL expired or buffer full)")); err != nil {
		return nil, err
	}
	if r.frameReassemblyHashMismatch, err = meter.Int64Counter("bsl_reassembly_hash_mismatch_total",
		metric.WithDescription("Completed reassemblies dropped because SHA256d(payload) != TxID")); err != nil {
		return nil, err
	}
	if r.txDedupErrors, err = meter.Int64Counter("bsl_txid_dedup_errors_total",
		metric.WithDescription("Redis errors during TxID dedup claim (fail-open: frame was forwarded)")); err != nil {
		return nil, err
	}
	if r.egressClaimLocalHit, err = meter.Int64Counter("bsl_egress_claim_local_hit_total",
		metric.WithDescription("Tier-1 local LRU short-circuit on per-deployment egress claim")); err != nil {
		return nil, err
	}
	if r.egressClaimWon, err = meter.Int64Counter("bsl_egress_claim_won_total",
		metric.WithDescription("Per-deployment egress claim SETNX wins (frame forwarded)")); err != nil {
		return nil, err
	}
	if r.egressClaimLost, err = meter.Int64Counter("bsl_egress_claim_lost_total",
		metric.WithDescription("Per-deployment egress claim SETNX losses (HA sibling already forwarded)")); err != nil {
		return nil, err
	}
	if r.egressClaimError, err = meter.Int64Counter("bsl_egress_claim_errors_total",
		metric.WithDescription("Redis errors during egress claim (fail-open: frame was forwarded)")); err != nil {
		return nil, err
	}
	if r.ingressMarkSet, err = meter.Int64Counter("bsl_ingress_mark_set_total",
		metric.WithDescription("Courtesy SETNX into the proxy's ingress namespace that created a new key")); err != nil {
		return nil, err
	}
	if r.ingressMarkExisted, err = meter.Int64Counter("bsl_ingress_mark_existed_total",
		metric.WithDescription("Courtesy SETNX into the proxy's ingress namespace where the key already existed")); err != nil {
		return nil, err
	}
	if r.ingressMarkError, err = meter.Int64Counter("bsl_ingress_mark_errors_total",
		metric.WithDescription("Redis errors during ingress-set courtesy mark (best-effort)")); err != nil {
		return nil, err
	}
	if r.ingressMarkDropped, err = meter.Int64Counter("bsl_ingress_mark_dropped_total",
		metric.WithDescription("Ingress-set courtesy marks dropped because the async queue was full or local-only mode is active")); err != nil {
		return nil, err
	}
	if r.headerForwarded, err = meter.Int64Counter("bsl_header_forwarded_total",
		metric.WithDescription("Block headers extracted and forwarded to header egress")); err != nil {
		return nil, err
	}
	if r.headerEgressErrors, err = meter.Int64Counter("bsl_header_egress_errors_total",
		metric.WithDescription("Errors sending to header egress downstream")); err != nil {
		return nil, err
	}
	if r.tailProbesSent, err = meter.Int64Counter("bsl_tail_probes_sent_total",
		metric.WithDescription("speculative NACKs for the next expected SeqNum on an idle flow (BRC-126 tail-loss probe)")); err != nil {
		return nil, err
	}
	if r.tailProbesRecovered, err = meter.Int64Counter("bsl_tail_probes_recovered_total",
		metric.WithDescription("tail probes that returned a real frame — loss that no successor frame would ever have revealed")); err != nil {
		return nil, err
	}
	if r.tailProbesMiss, err = meter.Int64Counter("bsl_tail_probes_miss_total",
		metric.WithDescription("tail probes answered MISS — the flow was genuinely idle, not lossy")); err != nil {
		return nil, err
	}
	if r.gapsDetected, err = meter.Int64Counter("bsl_gaps_detected_total",
		metric.WithDescription("Sequence gaps detected (missing frames)")); err != nil {
		return nil, err
	}
	if r.gapsSuppressed, err = meter.Int64Counter("bsl_gaps_suppressed_total",
		metric.WithDescription("Gaps cancelled by retransmit fill or ACK response")); err != nil {
		return nil, err
	}
	if r.flowsRefused, err = meter.Int64Counter("bsl_nack_flows_refused_total",
		metric.WithDescription("New per-source flows skipped at the MaxFlows flood-guard cap")); err != nil {
		return nil, err
	}
	if r.seqRebaselines, err = meter.Int64Counter("bsl_seq_rebaselines_total",
		metric.WithDescription("Flows re-baselined on an implausible SeqNum jump (emitter change, e.g. anycast failover)")); err != nil {
		return nil, err
	}
	if r.nacksDispatched, err = meter.Int64Counter("bsl_nacks_dispatched_total",
		metric.WithDescription("NACK datagrams sent to retry endpoints")); err != nil {
		return nil, err
	}
	if r.nacksThrottled, err = meter.Int64Counter("bsl_nacks_throttled_total",
		metric.WithDescription("Gaps held after a THROTTLED congestion signal")); err != nil {
		return nil, err
	}
	if r.nacksUnrecovered, err = meter.Int64Counter("bsl_gaps_unrecovered_total",
		metric.WithDescription("Gaps evicted after retries exhausted or TTL exceeded")); err != nil {
		return nil, err
	}

	if r.subtreeGroupAnnouncesReceived, err = meter.Int64Counter("bsl_subtree_group_announces_received_total",
		metric.WithDescription("Valid SubtreeGroupAnnounce datagrams processed (BRC-127)")); err != nil {
		return nil, err
	}
	if r.subtreeGroupAnnouncesRejected, err = meter.Int64Counter("bsl_subtree_group_announces_rejected_total",
		metric.WithDescription("SubtreeGroupAnnounce datagrams rejected before registry update")); err != nil {
		return nil, err
	}
	if r.subtreeGroupEvictions, err = meter.Int64Counter("bsl_subtree_group_evictions_total",
		metric.WithDescription("Subtree group registry entries removed by TTL expiry")); err != nil {
		return nil, err
	}
	if _, err = meter.Int64ObservableGauge("bsl_subtree_group_entries",
		metric.WithDescription("Live (groupID, subtreeID) pairs in the subtree group registry"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(r.subtreeGroupEntries.Load())
			return nil
		}),
	); err != nil {
		return nil, err
	}

	if r.beaconAdvertsReceived, err = meter.Int64Counter("bsl_beacon_adverts_received_total",
		metric.WithDescription("Valid ADVERT beacon datagrams upserted into the endpoint registry")); err != nil {
		return nil, err
	}

	if r.manifestReceived, err = meter.Int64Counter("multicast_manifest_received_total",
		metric.WithDescription("BRC-139 ShardManifest datagrams accepted and upserted into the manifest registry")); err != nil {
		return nil, err
	}
	if r.manifestDivergence, err = meter.Int64Counter("multicast_manifest_divergence_total",
		metric.WithDescription("Authoritative-peer disagreements observed by the auto-config evaluator (BRC-139 §Divergence telemetry)")); err != nil {
		return nil, err
	}
	if r.manifestAdoption, err = meter.Int64Counter("multicast_manifest_adoption_total",
		metric.WithDescription("Times the auto-config evaluator newly adopted a value (bootstrap, quorum-shift, pin-removed)")); err != nil {
		return nil, err
	}
	if r.manifestReshardEmitDup, err = meter.Int64Counter("multicast_manifest_resharding_emit_duplicates_total",
		metric.WithDescription("Listener-side duplicates absorbed by egress dedup during a live re-shard bridging window")); err != nil {
		return nil, err
	}
	if _, err = meter.Int64ObservableGauge("multicast_manifest_pilots_known",
		metric.WithDescription("Distinct authoritative announcers currently within TTL"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(r.manifestPilotsKnown.Load())
			return nil
		}),
	); err != nil {
		return nil, err
	}
	if _, err = meter.Int64ObservableGauge("multicast_manifest_last_divergence_epoch",
		metric.WithDescription("Unix seconds of the most recent divergence observed per field; absent until a field first diverges"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			r.lastDivergenceMu.Lock()
			defer r.lastDivergenceMu.Unlock()
			for field, ts := range r.lastDivergence {
				o.Observe(ts, metric.WithAttributes(attribute.String("field", field)))
			}
			return nil
		}),
	); err != nil {
		return nil, err
	}
	if _, err = meter.Int64ObservableGauge("multicast_manifest_quorum_met_bits",
		metric.WithDescription("Bit0=shard_bits, bit1=source_mode, bit2=successor quorum-met flags"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(r.manifestQuorumMetBits.Load()))
			return nil
		}),
	); err != nil {
		return nil, err
	}
	if _, err = meter.Int64ObservableGauge("multicast_manifest_resharding_state",
		metric.WithDescription("0=steady, 1=bridging, 2=cutover-pending"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(r.manifestReshardState.Load()))
			return nil
		}),
	); err != nil {
		return nil, err
	}
	if _, err = meter.Int64ObservableGauge("multicast_manifest_resharding_window_seconds",
		metric.WithDescription("Seconds until TransitionEpoch; negative briefly during cutover; 0 when not bridging"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(r.manifestReshardWindow.Load())
			return nil
		}),
	); err != nil {
		return nil, err
	}
	if _, err = meter.Int64ObservableGauge("bsl_beacon_registry_endpoints",
		metric.WithDescription("Number of endpoints currently in the beacon discovery registry"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(r.beaconRegistryEndpoints.Load())
			return nil
		}),
	); err != nil {
		return nil, err
	}

	if _, err = meter.Float64ObservableGauge("bsl_uptime_seconds",
		metric.WithDescription("Seconds elapsed since the listener process started"),
		metric.WithUnit("s"),
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			o.Observe(time.Since(r.startTime).Seconds())
			return nil
		}),
	); err != nil {
		return nil, err
	}

	return r, nil
}

// FrameReceived records receipt of a multicast frame.
// version should be "brc12" (legacy 44-byte) or "brc124" (BRC-124/BRC-128, 92-byte).
func (r *Recorder) FrameReceived(workerID int, iface, version string) {
	r.promFramesReceived.WithLabelValues(strconv.Itoa(workerID), iface, version).Inc()
}

// FrameDropped records a dropped frame.
// reason: "decode_error", "shard_filter", "subtree_exclude",
// "subtree_include_miss", "sender_filter", "frag_decode_error",
// "no_reassembly_buffer".
func (r *Recorder) FrameDropped(workerID int, reason string) {
	r.promFramesDropped.WithLabelValues(strconv.Itoa(workerID), reason).Inc()
}

// FrameForwarded records a successfully forwarded frame.
func (r *Recorder) FrameForwarded(workerID int, proto string) {
	r.promFramesForwarded.WithLabelValues(strconv.Itoa(workerID), proto).Inc()
}

// BundleRebucketed records one BRC-142 bundle re-bucketed to the local ShardBits
// generation (its members re-coalesced into local-generation groups) before
// delivery — the cross-generation / re-shard relay operation.
func (r *Recorder) BundleRebucketed(workerID int) {
	r.promBundlesRebucketed.WithLabelValues(strconv.Itoa(workerID)).Inc()
}

// RebucketUnguarded records a re-bucket on a listener not configured as a
// relay (-rebucket-relay) — the generation-mismatch alarm signal.
func (r *Recorder) RebucketUnguarded(workerID int) {
	r.promRebucketUnguarded.WithLabelValues(strconv.Itoa(workerID)).Inc()
}

// FrameInvalidPayload records a BRC-124/BRC-128 frame dropped because
// SHA256d(payload) did not match the frame's TxID. Only emitted when
// payload-hash verification is enabled on the worker.
func (r *Recorder) FrameInvalidPayload(workerID int) {
	r.promFramesInvalidPayload.WithLabelValues(strconv.Itoa(workerID)).Inc()
}

// ReassemblyStarted records the opening of a new BRC-130 reassembly slot.
func (r *Recorder) ReassemblyStarted() {
	r.frameReassemblyStarted.Add(context.Background(), 1)
}

// ReassemblyCompleted records a BRC-130 reassembly that produced a complete payload.
func (r *Recorder) ReassemblyCompleted() {
	r.frameReassemblyCompleted.Add(context.Background(), 1)
}

// ReassemblyAbandoned records a BRC-130 reassembly slot evicted before
// completion because its TTL expired or the buffer was full.
func (r *Recorder) ReassemblyAbandoned() {
	r.frameReassemblyAbandoned.Add(context.Background(), 1)
}

// ReassemblyHashMismatch records a completed reassembly that was dropped
// because SHA256d(payload) != TxID.
func (r *Recorder) ReassemblyHashMismatch() {
	r.frameReassemblyHashMismatch.Add(context.Background(), 1)
}

// FrameDeduped records a BRC-124/BRC-128 retransmit suppressed before egress
// because the (groupIdx, subtreeID, SeqNum) tuple was already forwarded
// recently. The gap-state suppression metric (GapSuppressed) is a separate
// signal: it tracks gap-tracker fills, not egress dedup.
func (r *Recorder) FrameDeduped(workerID int) {
	r.promFramesDeduped.WithLabelValues(strconv.Itoa(workerID)).Inc()
}

// FrameTxDeduped records a frame suppressed by the Redis TxID claim gate
// (cross-listener deduplication). The frame was already claimed by another
// listener in the same Redis-backed dedup group.
func (r *Recorder) FrameTxDeduped(workerID int) {
	r.promFramesTxDeduped.WithLabelValues(strconv.Itoa(workerID)).Inc()
}

// TxDedupError records a Redis error during a TxID claim attempt. The frame
// was forwarded anyway (fail-open behaviour).
func (r *Recorder) TxDedupError() {
	r.txDedupErrors.Add(context.Background(), 1)
}

// EgressClaimLocalHit records a tier-1 local-LRU short-circuit on egress claim.
func (r *Recorder) EgressClaimLocalHit() { r.egressClaimLocalHit.Add(context.Background(), 1) }

// EgressClaimWon records a tier-2 SETNX win (frame proceeds to egress).
func (r *Recorder) EgressClaimWon() { r.egressClaimWon.Add(context.Background(), 1) }

// EgressClaimLost records a tier-2 SETNX loss (sibling listener forwarded).
func (r *Recorder) EgressClaimLost() { r.egressClaimLost.Add(context.Background(), 1) }

// EgressClaimError records a Redis error during egress claim (fail-open).
func (r *Recorder) EgressClaimError() { r.egressClaimError.Add(context.Background(), 1) }

// IngressMarkSet records a courtesy SETNX into the proxy's namespace that
// created a new key (this listener was first to observe the TxID via multicast).
func (r *Recorder) IngressMarkSet() { r.ingressMarkSet.Add(context.Background(), 1) }

// IngressMarkExisted records a courtesy SETNX where the key already existed
// (the local proxy or a sibling listener had already claimed the TxID).
func (r *Recorder) IngressMarkExisted() { r.ingressMarkExisted.Add(context.Background(), 1) }

// IngressMarkError records a Redis error during async ingress-set mark.
func (r *Recorder) IngressMarkError() { r.ingressMarkError.Add(context.Background(), 1) }

// IngressMarkDropped records an ingress-set courtesy mark dropped because the
// async work queue was full or no Redis was configured.
func (r *Recorder) IngressMarkDropped() { r.ingressMarkDropped.Add(context.Background(), 1) }

// EgressError records a send failure to downstream.
func (r *Recorder) EgressError(workerID int) {
	r.promEgressErrors.WithLabelValues(strconv.Itoa(workerID)).Inc()
}

// MCEgressError records a send failure on the multicast egress path.
func (r *Recorder) MCEgressError(workerID int) {
	r.promMCEgressErrors.WithLabelValues(strconv.Itoa(workerID)).Inc()
}

// HeaderForwarded records a block header extracted and forwarded to header egress.
func (r *Recorder) HeaderForwarded(workerID int) {
	r.headerForwarded.Add(context.Background(), 1, metric.WithAttributes(
		attribute.Int("worker", workerID),
	))
}

// HeaderEgressError records a send failure on the header egress path.
func (r *Recorder) HeaderEgressError(workerID int) {
	r.headerEgressErrors.Add(context.Background(), 1, metric.WithAttributes(
		attribute.Int("worker", workerID),
	))
}

// GapDetected records a newly detected sequence gap.
// flow identifies the frame type: "brc131" for block control frames, "brc124" otherwise.
// source is the multicast source address the loss is attributed to (per-link
// delivery health); "" when unknown. Source cardinality is the spine/source count.
func (r *Recorder) GapDetected(flow, source string) {
	r.gapsDetected.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("flow", flow), attribute.String("source", source)))
}

// SeqRebaselined counts a flow re-baselined on an implausible forward SeqNum jump —
// an emitter change (e.g. anycast spine failover between long-lived proxies whose
// in-memory flow counters diverged), NOT data loss: the intermediate range was never
// emitted toward this listener, so it must not be registered (or NACKed) as gaps.
func (r *Recorder) SeqRebaselined(flow, source string) {
	r.seqRebaselines.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("flow", flow), attribute.String("source", source)))
}

// FlowsRefused counts a new per-source flow skipped at the MaxFlows flood-guard cap.
func (r *Recorder) FlowsRefused(flow string) {
	r.flowsRefused.Add(context.Background(), 1, metric.WithAttributes(attribute.String("flow", flow)))
}

// GapSuppressed records a gap cancelled by a retransmit fill or ACK response.
// flow/source as in GapDetected.
func (r *Recorder) GapSuppressed(flow, source string) {
	r.gapsSuppressed.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("flow", flow), attribute.String("source", source)))
}

// NACKDispatched records a NACK datagram sent to a retry endpoint.
// flow/source as in GapDetected.
func (r *Recorder) NACKDispatched(flow, source string) {
	r.nacksDispatched.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("flow", flow), attribute.String("source", source)))
}

// NACKThrottled records a gap parked after a THROTTLED congestion signal.
// flow/source as in GapDetected.
func (r *Recorder) NACKThrottled(flow, source string) {
	r.nacksThrottled.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("flow", flow), attribute.String("source", source)))
}

// GapUnrecovered records a gap evicted after retries exhausted or TTL exceeded.
// flow/source as in GapDetected. This is the per-source PERMANENT-loss signal the
// topology spectral health weights consume.
func (r *Recorder) GapUnrecovered(flow, source string) {
	r.nacksUnrecovered.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("flow", flow), attribute.String("source", source)))
}

// SubtreeGroupAnnounceReceived records a valid SubtreeGroupAnnounce datagram processed.
func (r *Recorder) SubtreeGroupAnnounceReceived() {
	r.subtreeGroupAnnouncesReceived.Add(context.Background(), 1)
}

// SubtreeGroupAnnounceRejected records a rejected SubtreeGroupAnnounce datagram.
// reason: "too_short", "decode_error", "sender_filter".
func (r *Recorder) SubtreeGroupAnnounceRejected(reason string) {
	r.subtreeGroupAnnouncesRejected.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("reason", reason),
	))
}

// SubtreeGroupEvicted records entries removed from the subtree group registry
// by TTL expiry and updates the live-entry gauge.
func (r *Recorder) SubtreeGroupEvicted(evicted, remaining int) {
	if evicted > 0 {
		r.subtreeGroupEvictions.Add(context.Background(), int64(evicted))
	}
	r.subtreeGroupEntries.Store(int64(remaining))
}

// BeaconAdvertReceived records a valid ADVERT beacon upserted into the registry.
func (r *Recorder) BeaconAdvertReceived() {
	r.beaconAdvertsReceived.Add(context.Background(), 1)
}

// SetBeaconRegistryEndpoints updates the beacon registry endpoint count gauge.
func (r *Recorder) SetBeaconRegistryEndpoints(n int) {
	r.beaconRegistryEndpoints.Store(int64(n))
}

// ManifestReceived records a valid BRC-139 ShardManifest upserted into
// the manifest registry. nil-safe.
func (r *Recorder) ManifestReceived() {
	if r == nil {
		return
	}
	r.manifestReceived.Add(context.Background(), 1)
}

// ManifestSetPilotsKnown updates the pilots-known gauge from the
// evaluator's most-recent snapshot. nil-safe.
func (r *Recorder) ManifestSetPilotsKnown(n int) {
	if r == nil {
		return
	}
	r.manifestPilotsKnown.Store(int64(n))
}

// ManifestSetQuorumMetBits encodes per-field quorum-met flags into a
// single bitmap gauge. Bit positions: 0=shard_bits, 1=source_mode,
// 2=successor.
func (r *Recorder) ManifestSetQuorumMetBits(bits int32) {
	if r == nil {
		return
	}
	r.manifestQuorumMetBits.Store(bits)
}

// ManifestDivergence increments the divergence counter for one field
// observation. kind: "peer-disagree", "pin-disagree", or "crc-fail".
func (r *Recorder) ManifestDivergence(field, kind string) {
	if r == nil {
		return
	}
	r.manifestDivergence.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("field", field),
		attribute.String("kind", kind),
	))
	r.lastDivergenceMu.Lock()
	r.lastDivergence[field] = time.Now().Unix()
	r.lastDivergenceMu.Unlock()
}

// ManifestAdoption increments the adoption counter when the evaluator
// newly adopts a value. reason: "bootstrap", "quorum-shift",
// "pin-removed".
func (r *Recorder) ManifestAdoption(field, reason string) {
	if r == nil {
		return
	}
	r.manifestAdoption.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("field", field),
		attribute.String("reason", reason),
	))
}

// ManifestSetReshardState updates the re-sharding state gauge.
// 0=steady, 1=bridging, 2=cutover-pending.
func (r *Recorder) ManifestSetReshardState(state int32) {
	if r == nil {
		return
	}
	r.manifestReshardState.Store(state)
}

// ManifestSetReshardWindowSeconds updates the seconds-until-cutover
// gauge. May be negative briefly during cutover.
func (r *Recorder) ManifestSetReshardWindowSeconds(s int64) {
	if r == nil {
		return
	}
	r.manifestReshardWindow.Store(s)
}

// ManifestReshardEmitDuplicate records one egress-dedup duplicate
// absorbed during a live re-shard bridging window.
func (r *Recorder) ManifestReshardEmitDuplicate() {
	if r == nil {
		return
	}
	r.manifestReshardEmitDup.Add(context.Background(), 1)
}

// WorkerReady signals a worker has entered its receive loop.
func (r *Recorder) WorkerReady() { r.readyCount.Add(1) }

// WorkerDone signals a worker has exited its receive loop.
func (r *Recorder) WorkerDone() { r.readyCount.Add(-1) }

// SetDraining marks the recorder as draining; /readyz returns 503.
func (r *Recorder) SetDraining() { r.draining.Store(true) }

// Shutdown flushes all pending OTLP exports and releases SDK resources.
func (r *Recorder) Shutdown(ctx context.Context) {
	if err := r.shutdownFn(ctx); err != nil {
		slog.Warn("metrics shutdown error", "err", err)
	}
}

// Serve starts the HTTP metrics server on addr.
func (r *Recorder) Serve(addr string, done <-chan struct{}) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(r.promReg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", r.handleHealthz)
	mux.HandleFunc("/readyz", r.handleReadyz)
	if r.levelVar != nil {
		mux.HandleFunc("/loglevel", logging.LevelHandler(r.levelVar))
	}

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		slog.Info("metrics server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server error", "err", err)
		}
	}()
	<-done
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Warn("metrics server shutdown error", "err", err)
	}
}

func (r *Recorder) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `{"status":"ok","uptime_seconds":%.1f}`, time.Since(r.startTime).Seconds())
}

func (r *Recorder) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	ready := int(r.readyCount.Load())
	total := r.numWorkers
	w.Header().Set("Content-Type", "application/json")
	if r.draining.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, `{"status":"draining","workers_ready":%d,"workers_total":%d}`, ready, total)
		return
	}
	if ready >= total && total > 0 {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"status":"ready","workers_ready":%d,"workers_total":%d}`, ready, total)
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = fmt.Fprintf(w, `{"status":"starting","workers_ready":%d,"workers_total":%d}`, ready, total)
}

// TailProbeSent records a speculative NACK issued for the next expected SeqNum
// on a flow that has gone quiet. Unlike a gap, a probe is a QUESTION, not an
// observed loss — it must never be counted as a detected gap or the unrecovered
// ratio inflates with phantom losses on every idle flow.
func (r *Recorder) TailProbeSent(flow, source string) {
	r.tailProbesSent.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("flow", flow), attribute.String("source", source)))
}

// TailProbeRecovered records a probe that returned a real frame: tail loss that
// no successor frame would ever have revealed.
func (r *Recorder) TailProbeRecovered(flow, source string) {
	r.tailProbesRecovered.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("flow", flow), attribute.String("source", source)))
}

// TailProbeMiss records a probe answered MISS — the flow was simply idle. This
// is the expected steady-state outcome and is the cost of the mechanism.
func (r *Recorder) TailProbeMiss(flow, source string) {
	r.tailProbesMiss.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("flow", flow), attribute.String("source", source)))
}
