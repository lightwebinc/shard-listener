package discovery

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/lightwebinc/shard-listener/metrics"
)

// BeaconListener joins the beacon multicast groups and upserts received
// ADVERTs into the Registry. Call Start to begin listening; cancel the
// context to stop.
type BeaconListener struct {
	Registry *Registry
	Groups   []*net.UDPAddr    // beacon group addresses to join
	Iface    *net.Interface    // multicast interface
	Rec      *metrics.Recorder // nil = no metrics
	Debug    bool
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

func (bl *BeaconListener) listenGroup(ctx context.Context, grp *net.UDPAddr) error {
	conn, err := net.ListenMulticastUDP("udp6", bl.Iface, grp)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	// Set a read buffer size
	_ = conn.SetReadBuffer(1 << 16) // 64 KiB

	buf := make([]byte, ADVERTSize+64) // extra room for future extensions

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// Short deadline so we re-check ctx periodically
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, _, err := conn.ReadFromUDP(buf)
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

		advert, err := DecodeADVERT(buf[:n])
		if err != nil {
			if bl.Debug {
				log.Printf("discovery: ignoring invalid ADVERT from %s: %v", grp.IP, err)
			}
			continue
		}

		// Ignore ADVERT with Draining flag
		if advert.Flags&FlagDraining != 0 {
			if bl.Debug {
				log.Printf("discovery: ignoring draining endpoint %s (instance %08X)", advert.NACKAddr, advert.InstanceID)
			}
			continue
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
