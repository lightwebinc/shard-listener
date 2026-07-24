// shard-listener receives IPv6 multicast BSV transaction frames,
// filters by shard and/or subtree, forwards matching frames to a configurable
// downstream unicast host:port over UDP or TCP, and performs NACK-based gap recovery for BRC-124/BRC-128 frames.
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/lightwebinc/shard-common/bootstrap"
	"github.com/lightwebinc/shard-common/cache"
	"github.com/lightwebinc/shard-common/hostinfo"
	"github.com/lightwebinc/shard-common/logging"
	commanifest "github.com/lightwebinc/shard-common/manifest"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-common/seqhash"
	"github.com/lightwebinc/shard-common/shard"
	"github.com/lightwebinc/shard-common/tracing"

	listenermanifest "github.com/lightwebinc/shard-listener/manifest"

	"github.com/lightwebinc/shard-listener/config"
	"github.com/lightwebinc/shard-listener/dedup"
	"github.com/lightwebinc/shard-listener/discovery"
	"github.com/lightwebinc/shard-listener/egress"
	"github.com/lightwebinc/shard-listener/filter"
	"github.com/lightwebinc/shard-listener/listener"
	"github.com/lightwebinc/shard-listener/metrics"
	"github.com/lightwebinc/shard-listener/nack"
	"github.com/lightwebinc/shard-listener/reassembly"
	"github.com/lightwebinc/shard-listener/subtreegroup"
	"github.com/lightwebinc/shard-listener/txdedup"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	logLevel := logging.ParseLevel(cfg.LogLevel)
	if cfg.Debug {
		logLevel = slog.LevelDebug
	}
	levelVar := logging.Init(logging.Options{
		Service:    metrics.ServiceName,
		InstanceID: cfg.InstanceID,
		Version:    metrics.Version,
		Level:      logLevel,
		Format:     logging.ParseFormat(cfg.LogFormat),
	})
	logging.InstallSIGHUPToggle(levelVar, logLevel)

	slog.Info("shard-listener starting",
		"mode", cfg.Mode,
		"shard_bits", cfg.ShardBits,
		"num_groups", cfg.NumGroups,
		"scope", cfg.MCScope,
		"listen_port", cfg.ListenPort,
		"egress_addr", cfg.EgressAddr,
		"egress_proto", cfg.EgressProto,
		"mc_egress_enabled", cfg.MCEgressEnabled,
		"workers", cfg.NumWorkers,
		"retry_endpoints", len(cfg.RetryEndpoints),
	)
	if cfg.MCEgressEnabled {
		slog.Info("multicast egress enabled",
			"iface", cfg.MCEgressIface.Name,
			"scope", cfg.MCEgressScope,
			"port", cfg.MCEgressPort,
			"hoplimit", cfg.MCEgressHopLimit,
		)
	}
	if cfg.HeaderEgressEnabled {
		slog.Info("block header unicast egress enabled",
			"addr", cfg.HeaderEgressAddr,
			"proto", cfg.HeaderEgressProto,
		)
	}
	if cfg.HeaderMCEgressEnabled {
		slog.Info("block header multicast egress enabled",
			"iface", cfg.HeaderMCEgressIface.Name,
			"scope", cfg.HeaderMCEgressScope,
			"port", cfg.HeaderMCEgressPort,
			"hoplimit", cfg.HeaderMCEgressHopLimit,
		)
	}

	rec, err := metrics.New(cfg.InstanceID, cfg.NumWorkers, cfg.OTLPEndpoint, cfg.OTLPInterval)
	if err != nil {
		return fmt.Errorf("metrics: %w", err)
	}
	rec.SetLevelVar(levelVar)

	// One-shot host inventory: descriptive payload as a log event, slim
	// numerics mirrored as the bsl_host_info gauge.
	inv := hostinfo.Gather(metrics.ServiceName, metrics.Version)
	rec.SetHostInfo(inv)
	slog.Info("host.inventory", "inventory", inv)

	// Opt-in distributed tracing (no-op unless -trace-sampling > 0 with an OTLP
	// endpoint). Control-plane only (NACK/manifest); never the receive hot path.
	_, traceShutdown, terr := tracing.Init(context.Background(), tracing.Options{
		Service:      metrics.ServiceName,
		InstanceID:   cfg.InstanceID,
		Version:      metrics.Version,
		OTLPEndpoint: cfg.OTLPEndpoint,
		Sampling:     cfg.TraceSampling,
	})
	if terr != nil {
		slog.Warn("tracing init failed; continuing without traces", "err", terr)
	}
	defer func() {
		tctx, tcancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer tcancel()
		_ = traceShutdown(tctx)
	}()

	// Build the shard engine.
	engine := shard.New(cfg.MCPrefix, cfg.MCGroupID, cfg.ShardBits)

	// BRC-148 BEEF plane: engine (always wired — a delivery worker must be
	// able to route forwarded V9 frames), elected topic/version sets, and the
	// band join indices derived from the election.
	beefEngine, beefTopicSet, beefVersionSet, beefJoinIdx, err := setupBEEF(cfg)
	if err != nil {
		return err
	}

	// Delivery mode (P3b) is the consumer-facing half: it does NOT join fabric
	// (S,G) and runs no gap/NACK — a receiver already did that and forwards raw
	// frames unicast. The receiver-side machinery below (multicast groups, SSM
	// bootstrap, NACK tracker, beacon/manifest discovery, cross-listener dedup,
	// block-PoW gate, reassembly) is skipped; only the egress fan-out sink runs.
	delivery := cfg.Mode == "delivery"
	if cfg.Mode == "receiver" && len(cfg.DeliveryAddrs) > 0 {
		slog.Info("receiver mode: multi-destination fan-out to delivery tier",
			"delivery_addrs", cfg.DeliveryAddrs, "proto", cfg.EgressProto)
	}

	// Derive the multicast group addresses to join (receiver/collapsed only).
	var groups []*net.UDPAddr
	if !delivery {
		groups, err = buildGroups(cfg, engine, beefJoinIdx)
		if err != nil {
			return fmt.Errorf("build groups: %w", err)
		}
		slog.Info("multicast groups", "count", len(groups))
	}

	// Build subtree group registry if -subtree-groups is configured.
	var groupReg *subtreegroup.Registry
	if len(cfg.SubtreeGroups) > 0 {
		groupReg = subtreegroup.New(cfg.SubtreeGroups, cfg.SubtreeGroupDefaultTTL)
		slog.Info("subtree group registry created",
			"groups", len(cfg.SubtreeGroups),
			"default_ttl", cfg.SubtreeGroupDefaultTTL,
		)
	}

	// Build filter.
	filt := filter.New(cfg.ShardInclude, cfg.SubtreeInclude, cfg.SubtreeExclude, groupReg)

	// Shared sender ACL applied to both BRC-127 announcements and the
	// data-plane workers. nil when neither -sender-include nor -sender-exclude
	// is configured (so the per-frame check collapses to a single nil compare).
	senderACL := filter.NewSenderACL(cfg.SenderInclude, cfg.SenderExclude)

	// Build the endpoint registry (beacon-discovered + static seeds).
	reg := discovery.NewRegistry()

	// Build NACK tracker (receiver-side gap recovery; delivery skips it).
	var tracker *nack.Tracker
	if !delivery {
		tracker = nack.New(
			nack.TrackerConfig{
				JitterMax:      cfg.NACKJitterMax,
				BackoffBase:    cfg.NACKBackoffBase,
				BackoffMax:     cfg.NACKBackoffMax,
				MaxRetries:     cfg.NACKMaxRetries,
				GapTTL:         cfg.NACKGapTTL,
				MaxFlows:       cfg.NACKMaxFlows,
				MaxForwardJump: uint64(cfg.NACKMaxForwardJump),
			},
			cfg.RetryEndpoints,
			cfg.Iface,
			rec,
			reg,
		)
	}

	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// SSM: resolve per-control-group bootstrap source lists and the
	// static data-plane publisher list (lab/CI) into a single map keyed
	// by group address. Production manifest-driven discovery for
	// data-plane groups lands in a follow-up: the beacon listener already
	// observes BRC-139 manifests via the beacon group; an OnChange path
	// can mutate this map dynamically. For now, fail-closed startup on
	// the configured bootstrap lists.
	var gs listener.GroupSources
	var beaconSrcs, manifestSrcs, subAnnSrcs []netip.Addr
	if !delivery && cfg.SourceMode == "ssm" {
		var err error
		gs, beaconSrcs, manifestSrcs, subAnnSrcs, err = buildSSMSources(ctx, cfg, beefJoinIdx)
		if err != nil {
			return fmt.Errorf("ssm bootstrap: %w", err)
		}
		// Silence unused-warnings when none of the control-group lists
		// are configured; the per-listener wiring below uses each list.
		_ = manifestSrcs
	}

	if tracker != nil {
		tracker.Start(ctx)
	}

	// Start metrics server.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rec.Serve(cfg.MetricsAddr, done)
	}()

	// Start subtree announcement listener (BRC-127) — receiver-side discovery.
	if !delivery && groupReg != nil {
		var announceGroups []*net.UDPAddr
		for _, scopeName := range cfg.AnnounceScopes {
			scopePrefix := config.Scopes[scopeName]
			annIP := shard.GroupAddr(scopePrefix, cfg.MCGroupID, shard.GroupSubtreeGroupAnnounce)
			announceGroups = append(announceGroups, &net.UDPAddr{IP: annIP, Port: cfg.ListenPort})
		}
		sal := &discovery.SubtreeGroupAnnounceListener{
			Registry:      groupReg,
			Groups:        announceGroups,
			Iface:         cfg.Iface,
			Sources:       subAnnSrcs,
			DefaultTTL:    cfg.SubtreeGroupDefaultTTL,
			SenderInclude: cfg.SenderInclude,
			SenderExclude: cfg.SenderExclude,
			Rec:           rec,
			Debug:         cfg.Debug,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sal.Start(ctx); err != nil && ctx.Err() == nil {
				slog.Error("subtree announce listener error", "err", err)
			}
		}()
		slog.Info("subtree announce listener started",
			"groups", len(announceGroups),
			"scopes", cfg.AnnounceScopes,
		)
	}

	// Manifest consumer registry (BRC-139 auto-shard-config). Built
	// unconditionally so its zero-pilot state is always observable;
	// only wired into the beacon listener when AutoConfigEnabled.
	manifestReg := commanifest.NewRegistry(0)

	// Workers are constructed below. Pre-declare the slice here so the
	// auto-config applier's hook closure can capture it for runtime
	// AddGroup/RemoveGroup against each worker fd.
	var workers []*listener.Worker

	// Start beacon listener for dynamic endpoint discovery (receiver-side).
	if !delivery && cfg.BeaconEnabled {
		beaconScopePrefix, ok := config.Scopes[cfg.BeaconScope]
		if !ok {
			beaconScopePrefix = 0xFF05
		}
		beaconIP := shard.GroupAddr(beaconScopePrefix, cfg.MCGroupID, shard.GroupBeacon)
		beaconGrp := &net.UDPAddr{IP: beaconIP, Port: cfg.BeaconPort}
		bl := &discovery.BeaconListener{
			Registry: reg,
			Groups:   []*net.UDPAddr{beaconGrp},
			Iface:    cfg.Iface,
			Sources:  beaconSrcs,
			Rec:      rec,
			Debug:    cfg.Debug,
		}
		if cfg.AutoConfigEnabled {
			bl.ManifestRegistry = manifestReg
			slog.Info("manifest consumer enabled",
				"bootstrap", cfg.AutoConfigBootstrap,
				"quorum", cfg.AutoConfigPilotQuorum,
				"hysteresis", cfg.AutoConfigHysteresis,
				"auto_join", cfg.AutoJoinFromManifest,
				"live_resharding", cfg.AutoConfigLiveResharding)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := bl.Start(ctx); err != nil && ctx.Err() == nil {
				slog.Error("beacon listener error", "err", err)
			}
		}()
		slog.Info("beacon listener started", "group", beaconIP, "port", cfg.BeaconPort)
	}

	// Start the manifest evaluator/applier when auto-config is enabled (receiver-side).
	if !delivery && cfg.AutoConfigEnabled {
		ev := commanifest.NewEvaluator(commanifest.EvaluatorConfig{
			Quorum:     cfg.AutoConfigPilotQuorum,
			Hysteresis: cfg.AutoConfigHysteresis,
			Pin: commanifest.Pin{
				ShardBits:       uint8(cfg.ShardBits),
				HasShardBitsPin: true,
			},
		})
		applier := &listenermanifest.Applier{
			Registry:  manifestReg,
			Evaluator: ev,
			Rec:       rec,
			Hooks: listenermanifest.Hooks{
				OnShardBitsChange: func(prev, next uint8) {
					slog.Warn("auto-config adopted new ShardBits (restart mode)",
						"prev", prev, "next", next,
						"action", "flipping /readyz to 503; orchestrator will roll the pod")
					// Restart-mode is the default. Live-resharding
					// hooks land in a follow-up pass that observes
					// cfg.AutoConfigLiveResharding here.
				},
				OnPilotGroupsChange: func(added, removed []uint16) {
					if !cfg.AutoJoinFromManifest {
						return
					}
					for _, idx := range added {
						addr := engine.Addr(uint32(idx), cfg.ListenPort)
						ga, ok := netip.AddrFromSlice(addr.IP.To16())
						if !ok {
							slog.Warn("auto-join: bad group address", "idx", idx)
							continue
						}
						for _, w := range workers {
							if err := w.AddGroup(ga, nil); err != nil {
								var errno syscall.Errno
								if errors.As(err, &errno) && errno == syscall.ENOBUFS {
									// mld_max_msf source-filter exhaustion is the
									// canonical fleet-scale join failure (category-8).
									slog.Error("auto-join failed: MLD source-filter exhausted; raise net.ipv6.mld_max_msf",
										"group", ga.String(), "err", err, "errno", errno.Error(), "syscall", "setsockopt")
								} else {
									slog.Warn("auto-join AddGroup failed",
										"group", ga.String(), "worker", w, "err", err)
								}
							}
						}
					}
					for _, idx := range removed {
						addr := engine.Addr(uint32(idx), cfg.ListenPort)
						ga, ok := netip.AddrFromSlice(addr.IP.To16())
						if !ok {
							continue
						}
						// Static -shard-include entries are NEVER leaved.
						if inStaticInclude(cfg.ShardInclude, idx) {
							continue
						}
						for _, w := range workers {
							if err := w.RemoveGroup(ga, nil); err != nil {
								slog.Warn("auto-join RemoveGroup failed",
									"group", ga.String(), "worker", w, "err", err)
							}
						}
					}
					slog.Info("auto-join applied", "added", added, "removed", removed)
				},
			},
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			applier.Run(ctx)
		}()
		slog.Info("manifest applier started")
	}

	// Build the shared TxID dedup store. Two independent namespaces are
	// composed into one Store:
	//   - egress  — per-deployment SETNX before downstream forward
	//   - ingress — optional courtesy SETNX into the local proxy's namespace
	//
	// LocalCap=0 on the egress side disables the feature entirely.
	var txDedupStore *txdedup.Store
	if !delivery && cfg.EgressDedupLocalCap > 0 {
		egBackend, ebErr := cache.Open(context.Background(), cache.Config{
			Backend:       cfg.EgressDedupBackend,
			RedisAddr:     cfg.EgressDedupRedisAddr,
			AeroHosts:     cfg.EgressDedupAeroHosts,
			AeroNamespace: cfg.EgressDedupAeroNS,
			AeroSet:       cfg.EgressDedupAeroSet,
		})
		if ebErr != nil {
			return fmt.Errorf("egress dedup backend: %w", ebErr)
		}
		ingBackend, ibErr := cache.Open(context.Background(), cache.Config{
			Backend:       cfg.IngressSetBackend,
			RedisAddr:     cfg.IngressSetRedisAddr,
			AeroHosts:     cfg.IngressSetAeroHosts,
			AeroNamespace: cfg.IngressSetAeroNS,
			AeroSet:       cfg.IngressSetAeroSet,
		})
		if ibErr != nil {
			if egBackend != nil {
				_ = egBackend.Close()
			}
			return fmt.Errorf("ingress-set backend: %w", ibErr)
		}
		txDedupStore, err = txdedup.NewWithConfig(txdedup.Config{
			EgressBackend:   egBackend,
			EgressPrefix:    cfg.EgressDedupPrefix,
			EgressTTL:       cfg.EgressDedupTTL2,
			EgressLocalCap:  cfg.EgressDedupLocalCap,
			DeploymentID:    cfg.DeploymentID,
			IngressBackend:  ingBackend,
			IngressPrefix:   cfg.IngressSetPrefix,
			IngressTTL:      cfg.IngressSetTTL,
			IngressLocalCap: cfg.IngressSetLocalCap,
			Recorder:        rec,
		})
		if err != nil {
			return fmt.Errorf("txid dedup: %w", err)
		}
		defer func() {
			_ = txDedupStore.Close()
			if egBackend != nil {
				_ = egBackend.Close()
			}
			if ingBackend != nil {
				_ = ingBackend.Close()
			}
		}()

		slog.Info("egress TxID dedup enabled",
			"backend", cfg.EgressDedupBackend,
			"redis_addr", cfg.EgressDedupRedisAddr,
			"prefix", txDedupStore.EgressPrefix(),
			"ttl", cfg.EgressDedupTTL2,
			"local_cap", cfg.EgressDedupLocalCap,
			"deployment_id", cfg.DeploymentID,
		)
		if txDedupStore.HasIngressMark() {
			slog.Info("ingress-set courtesy mark enabled",
				"redis_addr", cfg.IngressSetRedisAddr,
				"prefix", txDedupStore.IngressPrefix(),
				"ttl", cfg.IngressSetTTL,
			)
		}
		if cfg.TxidDedupAddr != "" || cfg.TxidDedupPrefix != "" || cfg.TxidDedupTTL > 0 {
			slog.Warn("deprecated -txid-dedup-* flags in use; migrate to -egress-dedup-* and -deployment-id")
		}
	}

	// Block-control gate (opt-in): one CoinbaseCorrelator shared across all
	// workers, since a block announce and its coinbase can land on different
	// worker sockets under SO_REUSEPORT.
	var coinbaseCorr *listener.CoinbaseCorrelator
	if !delivery && cfg.RequireBlockPoW && cfg.CoinbaseCorrCap > 0 {
		coinbaseCorr = listener.NewCoinbaseCorrelator(cfg.CoinbaseCorrCap, cfg.CoinbaseCorrTTL)
	}
	if !delivery && cfg.RequireBlockPoW {
		slog.Info("block-control gate enabled",
			"min_pow_bits", fmt.Sprintf("0x%08x", cfg.MinPoWBits),
			"coinbase_correlation", coinbaseCorr != nil)
	}

	// Start workers. Collect them so the auto-join applier can drive
	// runtime AddGroup/RemoveGroup against every worker fd.
	workers = make([]*listener.Worker, 0, cfg.NumWorkers)
	for i := range cfg.NumWorkers {
		// Receiver mode with a delivery set fans every demuxed frame out to each
		// delivery host (envelope-preserving); otherwise a single downstream egress
		// (collapsed's behaviour, and receiver's degenerate single-destination case).
		var egr egress.EgressSink
		var err error
		if cfg.Mode == "receiver" && len(cfg.DeliveryAddrs) > 0 {
			egr, err = egress.NewMulti(cfg.DeliveryAddrs, cfg.EgressProto)
		} else {
			egr, err = egress.New(cfg.EgressAddr, cfg.EgressProto, cfg.StripHeader)
		}
		if err != nil {
			return fmt.Errorf("egress worker %d: %w", i, err)
		}
		defer func() { _ = egr.Close() }()

		var mcastEgr *egress.MCastSender
		if !delivery && cfg.MCEgressEnabled {
			mcastEgr, err = egress.NewMCast(
				cfg.MCEgressPrefix,
				cfg.MCEgressGroupID,
				cfg.ShardBits,
				cfg.MCEgressPort,
				cfg.MCEgressIface,
				cfg.MCEgressHopLimit,
				cfg.StripHeader,
			)
			if err != nil {
				return fmt.Errorf("mc egress worker %d: %w", i, err)
			}
			defer func() { _ = mcastEgr.Close() }()
		}

		// Unicast header egress.
		var headerEgr *egress.Sender
		if !delivery && cfg.HeaderEgressEnabled {
			headerEgr, err = egress.New(cfg.HeaderEgressAddr, cfg.HeaderEgressProto, false)
			if err != nil {
				return fmt.Errorf("header egress worker %d: %w", i, err)
			}
			defer func() { _ = headerEgr.Close() }()
		}

		// Multicast header egress.
		var headerMCastEgr *egress.MCastSender
		if !delivery && cfg.HeaderMCEgressEnabled {
			headerMCastEgr, err = egress.NewMCast(
				cfg.HeaderMCEgressPrefix,
				cfg.HeaderMCEgressGroupID,
				cfg.ShardBits,
				cfg.HeaderMCEgressPort,
				cfg.HeaderMCEgressIface,
				cfg.HeaderMCEgressHopLimit,
				false,
			)
			if err != nil {
				return fmt.Errorf("header mc egress worker %d: %w", i, err)
			}
			defer func() { _ = headerMCastEgr.Close() }()
		}

		w := listener.New(i, cfg.Iface, cfg.ListenPort, groups, engine, filt, egr, mcastEgr, tracker, rec, cfg.Debug)
		if gs != nil {
			w.SetGroupSources(gs)
		}
		if headerEgr != nil {
			w.SetHeaderEgress(headerEgr)
		}
		if headerMCastEgr != nil {
			w.SetHeaderMCastEgress(headerMCastEgr)
		}
		// BRC-135 emitter identity: stable per-emitter HashKey computed once
		// using the listener's primary IPv6 on the configured interface,
		// the GroupBlockHeader index (matches the actual egress group
		// for BRC-135 frames), and a zero SubtreeID. The same value is
		// reused for every block header frame this emitter produces.
		if headerEgr != nil || headerMCastEgr != nil {
			if emitterIP, ok := primaryIPv6(cfg.Iface); ok {
				w.SetHeaderEmitterIdentity(seqhash.Hash(emitterIP, uint32(shard.GroupBlockHeader), [32]byte{}))
			}
		}
		w.SetVerifyPayloadHash(cfg.VerifyPayloadHash)
		w.SetBEEF(beefEngine, beefTopicSet, beefVersionSet, cfg.BEEFVerifyContent)
		if !delivery && cfg.RequireBlockPoW {
			w.SetBlockPoW(true, cfg.MinPoWBits, coinbaseCorr)
		}
		// Sender ACL is a receiver-side ingress filter on the ORIGINAL source; on
		// a delivery worker the datagram source is the receiver, so it must not run.
		if !delivery && senderACL != nil {
			w.SetSenderACL(senderACL)
		}
		if !delivery && cfg.EgressDedupCap > 0 {
			w.SetEgressDedup(dedup.New(cfg.EgressDedupCap, cfg.EgressDedupTTL))
		}
		if txDedupStore != nil {
			w.SetTxDedup(txDedupStore)
		}
		// Wire BRC-130 reassembly buffer (receiver-side: a delivery worker only
		// sees whole, already-reassembled frames the receiver forwards). The
		// buffer captures w via closure so each worker owns its own reassembly
		// state (SO_REUSEPORT routes all fragments from the same proxy source to
		// the same worker).
		if !delivery {
			wLocal := w
			buf := reassembly.New(
				reassembly.DefaultMaxSlots,
				reassembly.DefaultTTL,
				cfg.VerifyPayloadHash,
				wLocal.DeliverReassembled,
			)
			buf.SetStartedHook(rec.ReassemblyStarted)
			buf.SetAbandonedHook(rec.ReassemblyAbandoned)
			buf.SetHashMismatchHook(rec.ReassemblyHashMismatch)
			buf.SetBlockCallback(wLocal.DeliverReassembledBlock)
			buf.SetSubtreeDataCallback(wLocal.DeliverReassembledSubtreeData)
			buf.SetBEEFCallback(wLocal.DeliverReassembledBeef)
			if cfg.SubtreeDataVerifyMerkle {
				buf.SetVerifyMerkle(true)
			}
			w.SetReassemblyBuffer(buf)
			wg.Add(1)
			go func(b *reassembly.Buffer) {
				defer wg.Done()
				t := time.NewTicker(time.Second)
				defer t.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-t.C:
						b.Tick()
					}
				}
			}(buf)
		}
		workers = append(workers, w)
		wg.Add(1)
		go func(worker *listener.Worker) {
			defer wg.Done()
			// Delivery: unicast-ingest (no join, no gap/NACK). Else: join fabric (S,G).
			run := worker.Run
			if delivery {
				run = worker.RunUnicastIngest
			}
			if err := run(ctx); err != nil {
				slog.Error("worker exited with error", "err", err)
			}
		}(w)
	}

	// Wait for signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	slog.Info("shutdown signal received", "signal", sig)

	if cfg.DrainTimeout > 0 {
		rec.SetDraining()
		slog.Info("draining", "timeout", cfg.DrainTimeout)
		time.Sleep(cfg.DrainTimeout)
	}

	cancel()
	close(done)
	wg.Wait()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	rec.Shutdown(ctx2)

	slog.Info("shutdown complete")
	return nil
}

// primaryIPv6 returns the first non-link-local IPv6 address assigned to iface,
// falling back to any IPv6 address if no non-link-local is present. The
// returned 16-byte value is suitable as the senderIPv6 input to seqhash.Hash.
// Returns ok=false if iface has no IPv6 address (e.g. loopback in some test
// environments) — callers should leave the emitter HashKey unset in that case.
func primaryIPv6(iface *net.Interface) (out [16]byte, ok bool) {
	if iface == nil {
		return out, false
	}
	addrs, err := iface.Addrs()
	if err != nil {
		return out, false
	}
	var fallback net.IP
	for _, a := range addrs {
		ipn, ok2 := a.(*net.IPNet)
		if !ok2 {
			continue
		}
		ip := ipn.IP.To16()
		if ip == nil || ip.To4() != nil {
			continue
		}
		if fallback == nil {
			fallback = ip
		}
		if !ip.IsLinkLocalUnicast() {
			copy(out[:], ip)
			return out, true
		}
	}
	if fallback != nil {
		copy(out[:], fallback)
		return out, true
	}
	return out, false
}

// buildGroups returns the multicast group addresses this instance should join.
// If ShardInclude is set, only those groups are joined; otherwise all groups.
// The block control group (FF0E::B:FFFE) is always appended so block
// announcements are received regardless of shard filtering.
func buildGroups(cfg *config.Config, engine *shard.Engine, beefJoinIdx []uint32) ([]*net.UDPAddr, error) {
	var indices []uint32
	if len(cfg.ShardInclude) > 0 {
		indices = cfg.ShardInclude
	} else {
		indices = make([]uint32, cfg.NumGroups)
		for i := range indices {
			indices[i] = uint32(i)
		}
	}
	groups := make([]*net.UDPAddr, 0, len(indices)+1)
	for _, idx := range indices {
		addr := engine.Addr(idx, cfg.ListenPort)
		groups = append(groups, addr)
	}

	// Join the block broadcast group so we receive block announcements.
	ctrlIP := shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.GroupBlockBroadcast)
	groups = append(groups, &net.UDPAddr{IP: ctrlIP, Port: cfg.ListenPort})

	// Join the subtree data group when BRC-132 reception is enabled.
	if cfg.SubtreeDataEnabled {
		subtreeDataIP := shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.GroupSubtreeDataAnnounce)
		groups = append(groups, &net.UDPAddr{IP: subtreeDataIP, Port: cfg.ListenPort})
	}

	// Join the BRC-148 BEEF plane band groups derived from the election
	// (topic-derived ∪ explicit aggregator indices, already domain-tagged).
	for _, idx := range beefJoinIdx {
		groups = append(groups, engine.Addr(idx, cfg.ListenPort))
	}

	return groups, nil
}

// setupBEEF derives the BRC-148 plane wiring from config: the plane engine,
// the worker-level topic election (names hashed to TopicIDs; a 64-hex entry
// is taken as a TopicID verbatim), the accepted version-word set, and the
// sorted domain-tagged join indices (one group per elected topic, plus the
// explicit aggregator indices).
func setupBEEF(cfg *config.Config) (*shard.PlaneEngine, map[[32]byte]struct{}, map[uint32]struct{}, []uint32, error) {
	pe, err := shard.NewPlane(cfg.MCPrefix, cfg.MCGroupID, cfg.BEEFShardBits, shard.DomainBEEF)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("beef plane: %w", err)
	}

	joinSet := map[uint32]struct{}{}
	var topics map[[32]byte]struct{}
	if len(cfg.BEEFTopics) > 0 {
		topics = make(map[[32]byte]struct{}, len(cfg.BEEFTopics))
		for _, t := range cfg.BEEFTopics {
			var tid [32]byte
			if b, herr := hex.DecodeString(t); herr == nil && len(b) == 32 {
				copy(tid[:], b)
			} else {
				tid = objfmt.TopicID(t)
			}
			topics[tid] = struct{}{}
			joinSet[pe.GroupIndex(&tid)] = struct{}{}
		}
	}
	for _, g := range cfg.BEEFGroups {
		joinSet[uint32(pe.Base())+g] = struct{}{}
	}
	joins := make([]uint32, 0, len(joinSet))
	for idx := range joinSet {
		joins = append(joins, idx)
	}
	sort.Slice(joins, func(i, j int) bool { return joins[i] < joins[j] })

	var versions map[uint32]struct{}
	if len(cfg.BEEFVersions) > 0 {
		versions = make(map[uint32]struct{}, len(cfg.BEEFVersions))
		for _, tok := range cfg.BEEFVersions {
			switch tok {
			case "beef":
				versions[objfmt.BEEFMarkerV1] = struct{}{}
			case "beefv2":
				versions[objfmt.BEEFMarkerV2] = struct{}{}
			case "atomic":
				versions[objfmt.AtomicBEEFMarker] = struct{}{}
			}
		}
	}
	return pe, topics, versions, joins, nil
}

// buildSSMSources resolves the per-control-group bootstrap source lists
// and the lab/CI static publisher list into a single map keyed by
// IPv6-group-address. The control-group source lists (beacon, manifest,
// subtree-announce) are returned separately so they can be threaded into
// the BeaconListener / SubtreeGroupAnnounceListener Sources fields. All
// resolvers run for the lifetime of ctx; startup is fail-closed.
//
// Returns (gs, beaconSrcs, manifestSrcs, subtreeAnnSrcs, err).
// excludeOwnSource drops the node's own source address from a roster before
// it feeds (S,G) joins. Joining the node's own source on the PIM interface
// installs an iif==oif mroute on a collapsed edge — every originated frame
// re-enters the MFC until hop-limit death (~60x egress amplification).
// Consequence: the
// listener does not receive own-node frames via multicast; a local mirror is
// required where own-source completeness matters.
func excludeOwnSource(srcs []netip.Addr, own netip.Addr) []netip.Addr {
	if !own.IsValid() {
		return srcs
	}
	out := srcs[:0]
	for _, s := range srcs {
		if s != own {
			out = append(out, s)
		}
	}
	return out
}

func buildSSMSources(ctx context.Context, cfg *config.Config, beefJoinIdx []uint32) (listener.GroupSources, []netip.Addr, []netip.Addr, []netip.Addr, error) {
	gs := make(listener.GroupSources)

	var own netip.Addr
	if cfg.LocalSource != "" {
		a, err := netip.ParseAddr(cfg.LocalSource)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("local-source: %w", err)
		}
		own = a
	}

	resolve := func(entries []string) ([]netip.Addr, error) {
		if len(entries) == 0 {
			return nil, nil
		}
		r := &bootstrap.Resolver{Entries: entries, Refresh: cfg.SSMBootstrapRefresh}
		if err := r.Start(ctx); err != nil {
			return nil, err
		}
		return excludeOwnSource(r.Current(), own), nil
	}

	beaconSrcs, err := resolve(cfg.SSMBootstrapBeacon)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("ssm-bootstrap-beacon: %w", err)
	}
	manifestSrcs, err := resolve(cfg.SSMBootstrapManifest)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("ssm-bootstrap-manifest: %w", err)
	}
	subAnnSrcs, err := resolve(cfg.SSMBootstrapSubtreeAnn)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("ssm-bootstrap-subtree-announce: %w", err)
	}
	staticSrcs, err := resolve(cfg.SSMPublishersStatic)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("ssm-publishers-static: %w", err)
	}

	// Map control-group sources onto their group addresses too (so the
	// data-plane Worker's join loop pre-loads them when those groups are
	// also in its joined-group set, e.g. BlockBroadcast for manifest).
	put := func(ip net.IP, srcs []netip.Addr) {
		if len(srcs) == 0 {
			return
		}
		if ga, ok := netip.AddrFromSlice(ip.To16()); ok {
			gs[ga] = srcs
		}
	}
	put(shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.GroupBeacon), beaconSrcs)
	put(shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.GroupSubtreeDataAnnounce), subAnnSrcs)
	put(shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.GroupSubtreeGroupAnnounce), subAnnSrcs)
	put(shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.GroupBlockBroadcast), manifestSrcs)

	// Data-plane shard groups (lab/CI static path).
	if len(staticSrcs) > 0 {
		for idx := uint32(0); idx < cfg.NumGroups; idx++ {
			ip := shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.GroupIdx(idx))
			put(ip, staticSrcs)
		}
		// BRC-148 plane band joins inherit the announced global source
		// roster (the spec's Source Discovery rule: object planes are
		// published from the same sources as the transaction plane).
		for _, idx := range beefJoinIdx {
			ip := shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.GroupIdx(idx))
			put(ip, staticSrcs)
		}
	}
	return gs, beaconSrcs, manifestSrcs, subAnnSrcs, nil
}

// inStaticInclude reports whether the given shard index is in the
// operator's static -shard-include list. Pilot-driven RemoveGroup MUST
// skip these entries: static includes are never leaved per the
// auto-shard-config plan.
func inStaticInclude(static []uint32, idx uint16) bool {
	for _, v := range static {
		if v == uint32(idx) {
			return true
		}
	}
	return false
}
