package discovery

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/netip"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/manifest"
	"github.com/lightwebinc/shard-common/netjoin"

	"github.com/lightwebinc/shard-listener/metrics"
)

// BeaconListener joins the beacon multicast groups and upserts received
// ADVERTs into the Registry. Call Start to begin listening; cancel the
// context to stop.
//
// When Sources is non-empty the beacon groups are joined as SSM (S,G)
// against the supplied source list (typically the retry-endpoint pods'
// IPv6 from sources.bootstrap.beacon). When Sources is empty the
// listener uses the stdlib ASM path (net.ListenMulticastUDP).
//
// Groups entries that share a UDP port share ONE socket, with a join per
// address on it. Two wildcard binds on the same port collide, and the
// BRC-126/129 transition config — the any-source FF0x beacon group and the
// source-specific FF3x one, both on -beacon-port — is exactly such a pair.
type BeaconListener struct {
	Registry *Registry
	Groups   []*net.UDPAddr    // beacon group addresses to join
	Iface    *net.Interface    // multicast interface
	Sources  []netip.Addr      // optional SSM source list applied to every group in Groups
	Rec      *metrics.Recorder // nil = no metrics
	Debug    bool

	// SourcesByPort overrides Sources for one group port. The ADVERT port
	// and the BRC-139 manifest port sit on the SAME beacon group address but
	// are published by different senders (retry endpoints vs shard-manifest
	// announcers), so under SSM each needs its own (S,G) roster — one shared
	// list would filter the other sender out entirely. Ports absent from the
	// map fall back to Sources.
	SourcesByPort map[int][]netip.Addr

	// ManifestRegistry, when non-nil, receives every BRC-139
	// ShardManifest datagram (MsgType 0x40) decoded off the beacon
	// socket. ADVERTs (MsgType 0x20) continue to flow into Registry.
	// When nil, manifest datagrams are silently dropped (logged at
	// Debug).
	ManifestRegistry *manifest.Registry
}

// Start listens for ADVERT beacons on all configured groups.
// It also starts a background eviction goroutine (1 s tick).
// Blocks until ctx is cancelled.
func (bl *BeaconListener) Start(ctx context.Context) error {
	// Start eviction goroutine
	go bl.evictLoop(ctx)

	buckets := bucketByPort(bl.Groups)
	errCh := make(chan error, len(buckets))
	for _, grps := range buckets {
		go func() {
			errCh <- bl.listenGroup(ctx, grps)
		}()
	}

	// Wait for context cancellation or first fatal error
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

// bucketByPort groups the configured beacon addresses by UDP port,
// preserving first-seen port order and the address order within a port.
// One bucket becomes one socket: a second wildcard bind on a port already
// held fails, so the FF0x/FF3x pair the BRC-126/129 transition puts on
// -beacon-port has to be two joins on one socket, not two sockets.
func bucketByPort(groups []*net.UDPAddr) [][]*net.UDPAddr {
	idx := make(map[int]int, len(groups))
	buckets := make([][]*net.UDPAddr, 0, len(groups))
	for _, grp := range groups {
		i, seen := idx[grp.Port]
		if !seen {
			i = len(buckets)
			idx[grp.Port] = i
			buckets = append(buckets, nil)
		}
		buckets[i] = append(buckets[i], grp)
	}
	return buckets
}

// openGroupConn opens one UDP6 listener on the bucket's shared port and
// joins every address in it. A single ASM address keeps the stdlib path
// (net.ListenMulticastUDP: bind + IPV6_JOIN_GROUP, byte-for-byte what
// earlier releases did). Everything else — any SSM roster, or more than
// one address on the port, which is what -control-group-compat=both
// produces — binds the wildcard once and adds each membership via netjoin
// (MCAST_JOIN_SOURCE_GROUP per source under SSM, IPV6_JOIN_GROUP under
// ASM).
func (bl *BeaconListener) openGroupConn(grps []*net.UDPAddr) (*net.UDPConn, error) {
	if len(grps) == 0 {
		return nil, fmt.Errorf("beacon listen: empty group bucket")
	}
	port := grps[0].Port
	srcs := bl.sourcesFor(grps[0])
	if len(srcs) == 0 && len(grps) == 1 {
		return net.ListenMulticastUDP("udp6", bl.Iface, grps[0])
	}
	// Bind the wildcard on the shared port (so we receive datagrams sent
	// to any joined group address) and add the memberships explicitly.
	pc, err := net.ListenPacket("udp6", fmt.Sprintf("[::]:%d", port))
	if err != nil {
		return nil, fmt.Errorf("beacon listen %d: %w", port, err)
	}
	uc, ok := pc.(*net.UDPConn)
	if !ok {
		_ = pc.Close()
		return nil, fmt.Errorf("beacon listen: unexpected conn type %T", pc)
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		_ = uc.Close()
		return nil, fmt.Errorf("beacon listen: SyscallConn: %w", err)
	}
	for _, grp := range grps {
		ga, ok := netip.AddrFromSlice(grp.IP.To16())
		if !ok {
			_ = uc.Close()
			return nil, fmt.Errorf("beacon listen: bad group address %s", grp.IP)
		}
		var joinErr error
		if cerr := raw.Control(func(fd uintptr) {
			joinErr = netjoin.Join(int(fd), bl.Iface.Index, ga, srcs)
		}); cerr != nil {
			_ = uc.Close()
			return nil, fmt.Errorf("beacon listen: Control: %w", cerr)
		}
		if joinErr != nil {
			_ = uc.Close()
			return nil, fmt.Errorf("beacon join %s (%d sources): %w", grp.IP, len(srcs), joinErr)
		}
	}
	return uc, nil
}

// sourcesFor returns the SSM source roster to join grp with: the per-port
// override when one is configured for grp.Port, else the shared Sources. An
// empty result selects the ASM path.
func (bl *BeaconListener) sourcesFor(grp *net.UDPAddr) []netip.Addr {
	if srcs, ok := bl.SourcesByPort[grp.Port]; ok {
		return srcs
	}
	return bl.Sources
}

// listenGroup services one socket: the bucket of beacon addresses that
// share a UDP port. grp is the bucket's first address and is used only to
// label logs — a datagram arriving on a socket joined to both the FF0x and
// FF3x forms of the same group cannot be attributed to one of them without
// IPV6_RECVPKTINFO, and nothing downstream needs that attribution.
func (bl *BeaconListener) listenGroup(ctx context.Context, grps []*net.UDPAddr) error {
	conn, err := bl.openGroupConn(grps)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	grp := grps[0]

	// Set a read buffer size
	_ = conn.SetReadBuffer(1 << 16) // 64 KiB

	// Buffer must accommodate the larger of ADVERT and ShardManifest
	// payloads. ShardManifest with a full bitmap (ShardBits=12) plus
	// sources and a Successor block fits well under 2 KiB.
	buf := make([]byte, 2048)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// Short deadline so we re-check ctx periodically
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, src, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			// On context cancel, the socket will be closed
			select {
			case <-ctx.Done():
				return nil
			default:
				log.Printf("discovery: beacon read error on %s: %v", grp.IP, err)
				continue
			}
		}

		// Demux on MsgType byte at offset 6 per BRC-126 / BRC-139.
		if n < 7 {
			continue
		}
		switch buf[6] {
		case MsgTypeADVERT:
			bl.handleADVERT(buf[:n], grp)
		case frame.MsgTypeShardManifest:
			bl.handleManifest(buf[:n], src, grp)
		default:
			if bl.Debug {
				log.Printf("discovery: ignoring unknown MsgType 0x%02X on %s", buf[6], grp.IP)
			}
		}
	}
}

// handleADVERT decodes a BRC-126 ADVERT and upserts the corresponding
// retry-endpoint entry. Draining endpoints are skipped.
func (bl *BeaconListener) handleADVERT(buf []byte, grp *net.UDPAddr) {
	advert, err := DecodeADVERT(buf)
	if err != nil {
		if bl.Debug {
			log.Printf("discovery: ignoring invalid ADVERT from %s: %v", grp.IP, err)
		}
		return
	}
	if advert.Flags&FlagDraining != 0 {
		if bl.Debug {
			log.Printf("discovery: ignoring draining endpoint %s (instance %08X)", advert.NACKAddr, advert.InstanceID)
		}
		return
	}
	bl.Registry.Upsert(advert)
	if bl.Rec != nil {
		bl.Rec.BeaconAdvertReceived()
	}
	if bl.Debug {
		log.Printf("discovery: upserted endpoint [%s]:%d tier=%d pref=%d instance=%08X",
			advert.NACKAddr, advert.NACKPort, advert.Tier, advert.Preference, advert.InstanceID)
	}
}

// handleManifest decodes a BRC-139 ShardManifest and upserts it into the
// configured manifest registry. Drops with a debug-log when no registry
// is configured (auto-config disabled).
func (bl *BeaconListener) handleManifest(buf []byte, src *net.UDPAddr, grp *net.UDPAddr) {
	if bl.ManifestRegistry == nil {
		if bl.Debug {
			log.Printf("discovery: manifest received on %s but no registry configured", grp.IP)
		}
		return
	}
	m, err := frame.DecodeShardManifest(buf)
	if err != nil {
		if bl.Debug {
			log.Printf("discovery: ignoring invalid manifest from %s: %v", src.IP, err)
		}
		return
	}
	srcAddr, ok := netip.AddrFromSlice(src.IP.To16())
	if !ok {
		if bl.Debug {
			log.Printf("discovery: bad src address from %s", src.IP)
		}
		return
	}
	bl.ManifestRegistry.Upsert(srcAddr, m)
	if bl.Rec != nil {
		bl.Rec.ManifestReceived()
	}
	if bl.Debug {
		log.Printf("discovery: upserted manifest from [%s] instance=%08X shardBits=%d flags=0x%02X",
			src.IP, m.InstanceID, m.ShardBits, m.Flags)
	}
}

func (bl *BeaconListener) evictLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bl.Registry.Evict()
			if bl.Rec != nil {
				bl.Rec.SetBeaconRegistryEndpoints(bl.Registry.Len())
			}
		}
	}
}
