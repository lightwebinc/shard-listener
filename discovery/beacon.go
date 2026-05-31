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
type BeaconListener struct {
	Registry *Registry
	Groups   []*net.UDPAddr    // beacon group addresses to join
	Iface    *net.Interface    // multicast interface
	Sources  []netip.Addr      // optional SSM source list applied to every group in Groups
	Rec      *metrics.Recorder // nil = no metrics
	Debug    bool

	// ManifestRegistry, when non-nil, receives every BRC-137
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

	errCh := make(chan error, len(bl.Groups))
	for _, grp := range bl.Groups {
		grp := grp
		go func() {
			errCh <- bl.listenGroup(ctx, grp)
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

// openGroupConn opens a UDP6 listener on grp.Port and joins grp. When
// bl.Sources is empty the join is ASM (IPV6_JOIN_GROUP via the stdlib
// helper); when non-empty it is SSM (one MCAST_JOIN_SOURCE_GROUP per
// source via netjoin).
func (bl *BeaconListener) openGroupConn(grp *net.UDPAddr) (*net.UDPConn, error) {
	if len(bl.Sources) == 0 {
		return net.ListenMulticastUDP("udp6", bl.Iface, grp)
	}
	// SSM path: open a regular UDP6 socket bound to the wildcard on
	// grp.Port (so we receive datagrams sent to the group address) and
	// add the (S,G) filters via netjoin.
	pc, err := net.ListenPacket("udp6", fmt.Sprintf("[::]:%d", grp.Port))
	if err != nil {
		return nil, fmt.Errorf("ssm listen %d: %w", grp.Port, err)
	}
	uc, ok := pc.(*net.UDPConn)
	if !ok {
		_ = pc.Close()
		return nil, fmt.Errorf("ssm listen: unexpected conn type %T", pc)
	}
	ga, ok := netip.AddrFromSlice(grp.IP.To16())
	if !ok {
		_ = uc.Close()
		return nil, fmt.Errorf("ssm listen: bad group address %s", grp.IP)
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		_ = uc.Close()
		return nil, fmt.Errorf("ssm listen: SyscallConn: %w", err)
	}
	var joinErr error
	if cerr := raw.Control(func(fd uintptr) {
		joinErr = netjoin.Join(int(fd), bl.Iface.Index, ga, bl.Sources)
	}); cerr != nil {
		_ = uc.Close()
		return nil, fmt.Errorf("ssm listen: Control: %w", cerr)
	}
	if joinErr != nil {
		_ = uc.Close()
		return nil, fmt.Errorf("ssm join (%d sources): %w", len(bl.Sources), joinErr)
	}
	return uc, nil
}

func (bl *BeaconListener) listenGroup(ctx context.Context, grp *net.UDPAddr) error {
	conn, err := bl.openGroupConn(grp)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

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

		// Demux on MsgType byte at offset 6 per BRC-126 / BRC-137.
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

// handleManifest decodes a BRC-137 ShardManifest and upserts it into the
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
