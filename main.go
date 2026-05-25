// bitcoin-shard-listener receives IPv6 multicast BSV transaction frames,
// filters by shard and/or subtree, forwards matching frames to a configurable
// downstream unicast host:port over UDP or TCP, and performs NACK-based gap recovery for BRC-124/BRC-128 frames.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/lightwebinc/bitcoin-shard-common/seqhash"
	"github.com/lightwebinc/bitcoin-shard-common/shard"

	"github.com/lightwebinc/bitcoin-shard-listener/config"
	"github.com/lightwebinc/bitcoin-shard-listener/dedup"
	"github.com/lightwebinc/bitcoin-shard-listener/discovery"
	"github.com/lightwebinc/bitcoin-shard-listener/egress"
	"github.com/lightwebinc/bitcoin-shard-listener/filter"
	"github.com/lightwebinc/bitcoin-shard-listener/listener"
	"github.com/lightwebinc/bitcoin-shard-listener/metrics"
	"github.com/lightwebinc/bitcoin-shard-listener/nack"
	"github.com/lightwebinc/bitcoin-shard-listener/reassembly"
	"github.com/lightwebinc/bitcoin-shard-listener/subtreegroup"
	"github.com/lightwebinc/bitcoin-shard-listener/txdedup"
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

	logLevel := slog.LevelInfo
	if cfg.Debug {
		logLevel = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})))

	slog.Info("bitcoin-shard-listener starting",
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

	// Build the shard engine.
	engine := shard.New(cfg.MCPrefix, cfg.MCGroupID, cfg.ShardBits)

	// Derive the multicast group addresses to join.
	groups, err := buildGroups(cfg, engine)
	if err != nil {
		return fmt.Errorf("build groups: %w", err)
	}
	slog.Info("multicast groups", "count", len(groups))

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

	// Build NACK tracker.
	tracker := nack.New(
		nack.TrackerConfig{
			JitterMax:  cfg.NACKJitterMax,
			BackoffMax: cfg.NACKBackoffMax,
			MaxRetries: cfg.NACKMaxRetries,
			GapTTL:     cfg.NACKGapTTL,
		},
		cfg.RetryEndpoints,
		cfg.Iface,
		rec,
		reg,
	)

	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tracker.Start(ctx)

	// Start metrics server.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rec.Serve(cfg.MetricsAddr, done)
	}()

	// Start subtree announcement listener (BRC-127).
	if groupReg != nil {
		var announceGroups []*net.UDPAddr
		for _, scopeName := range cfg.AnnounceScopes {
			scopePrefix := config.Scopes[scopeName]
			annIP := shard.ControlGroupAddr(scopePrefix, cfg.MCGroupID, shard.CtrlGroupSubtreeGroupAnnounce)
			announceGroups = append(announceGroups, &net.UDPAddr{IP: annIP, Port: cfg.ListenPort})
		}
		sal := &discovery.SubtreeAnnounceListener{
			Registry:      groupReg,
			Groups:        announceGroups,
			Iface:         cfg.Iface,
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

	// Start beacon listener for dynamic endpoint discovery.
	if cfg.BeaconEnabled {
		beaconScopePrefix, ok := config.Scopes[cfg.BeaconScope]
		if !ok {
			beaconScopePrefix = 0xFF05
		}
		beaconIP := shard.ControlGroupAddr(beaconScopePrefix, cfg.MCGroupID, shard.CtrlGroupBeacon)
		beaconGrp := &net.UDPAddr{IP: beaconIP, Port: cfg.BeaconPort}
		bl := &discovery.BeaconListener{
			Registry: reg,
			Groups:   []*net.UDPAddr{beaconGrp},
			Iface:    cfg.Iface,
			Rec:      rec,
			Debug:    cfg.Debug,
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

	// Build the shared TxID dedup store. Two independent namespaces are
	// composed into one Store:
	//   - egress  — per-deployment SETNX before downstream forward
	//   - ingress — optional courtesy SETNX into the local proxy's namespace
	//
	// LocalCap=0 on the egress side disables the feature entirely.
	var txDedupStore *txdedup.Store
	if cfg.EgressDedupLocalCap > 0 {
		txDedupStore, err = txdedup.NewWithConfig(txdedup.Config{
			EgressRedisAddr:  cfg.EgressDedupRedisAddr,
			EgressPrefix:     cfg.EgressDedupPrefix,
			EgressTTL:        cfg.EgressDedupTTL2,
			EgressLocalCap:   cfg.EgressDedupLocalCap,
			DeploymentID:     cfg.DeploymentID,
			IngressRedisAddr: cfg.IngressSetRedisAddr,
			IngressPrefix:    cfg.IngressSetPrefix,
			IngressTTL:       cfg.IngressSetTTL,
			IngressLocalCap:  cfg.IngressSetLocalCap,
			Recorder:         rec,
		})
		if err != nil {
			return fmt.Errorf("txid dedup: %w", err)
		}
		defer func() { _ = txDedupStore.Close() }()

		slog.Info("egress TxID dedup enabled",
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

	// Start workers.
	for i := range cfg.NumWorkers {
		egr, err := egress.New(cfg.EgressAddr, cfg.EgressProto, cfg.StripHeader)
		if err != nil {
			return fmt.Errorf("egress worker %d: %w", i, err)
		}
		defer func() { _ = egr.Close() }()

		var mcastEgr *egress.MCastSender
		if cfg.MCEgressEnabled {
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
		if cfg.HeaderEgressEnabled {
			headerEgr, err = egress.New(cfg.HeaderEgressAddr, cfg.HeaderEgressProto, false)
			if err != nil {
				return fmt.Errorf("header egress worker %d: %w", i, err)
			}
			defer func() { _ = headerEgr.Close() }()
		}

		// Multicast header egress.
		var headerMCastEgr *egress.MCastSender
		if cfg.HeaderMCEgressEnabled {
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
		if headerEgr != nil {
			w.SetHeaderEgress(headerEgr)
		}
		if headerMCastEgr != nil {
			w.SetHeaderMCastEgress(headerMCastEgr)
		}
		// BRC-135 emitter identity: stable per-emitter HashKey computed once
		// using the listener's primary IPv6 on the configured interface,
		// the CtrlGroupControl index, and a zero SubtreeID. The same value
		// is reused for every block header frame this emitter produces.
		if headerEgr != nil || headerMCastEgr != nil {
			if emitterIP, ok := primaryIPv6(cfg.Iface); ok {
				w.SetHeaderEmitterIdentity(seqhash.Hash(emitterIP, uint32(shard.CtrlGroupControl), [32]byte{}))
			}
		}
		w.SetVerifyPayloadHash(cfg.VerifyPayloadHash)
		if senderACL != nil {
			w.SetSenderACL(senderACL)
		}
		if cfg.EgressDedupCap > 0 {
			w.SetEgressDedup(dedup.New(cfg.EgressDedupCap, cfg.EgressDedupTTL))
		}
		if txDedupStore != nil {
			w.SetTxDedup(txDedupStore)
		}
		// Wire BRC-130 reassembly buffer. The buffer captures w via closure so
		// each worker owns its own reassembly state (SO_REUSEPORT routes all
		// fragments from the same proxy source to the same worker).
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
		if cfg.SubtreeDataVerifyMerkle {
			buf.SetVerifyMerkle(true)
		}
		w.SetReassemblyBuffer(buf)
		wg.Add(1)
		go func(worker *listener.Worker) {
			defer wg.Done()
			if err := worker.Run(ctx); err != nil {
				slog.Error("worker exited with error", "err", err)
			}
		}(w)
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
func buildGroups(cfg *config.Config, engine *shard.Engine) ([]*net.UDPAddr, error) {
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

	// Join the block control group so we receive block announcements.
	ctrlIP := shard.ControlGroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.CtrlGroupControl)
	groups = append(groups, &net.UDPAddr{IP: ctrlIP, Port: cfg.ListenPort})

	// Join the subtree data group when BRC-132 reception is enabled.
	if cfg.SubtreeDataEnabled {
		subtreeDataIP := shard.ControlGroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.CtrlGroupSubtreeAnnounce)
		groups = append(groups, &net.UDPAddr{IP: subtreeDataIP, Port: cfg.ListenPort})
	}

	return groups, nil
}
