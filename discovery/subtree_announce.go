package discovery

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"time"

	"golang.org/x/sys/unix"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-listener/filter"
	"github.com/lightwebinc/shard-listener/metrics"
	"github.com/lightwebinc/shard-listener/subtreegroup"
)

// SubtreeAnnounceListener joins the BRC-127 subtree announcement multicast
// group on one or more configured scopes and populates a [subtreegroup.Registry]
// with received SubtreeAnnounce datagrams. Call Start to begin listening;
// cancel the context to stop.
type SubtreeAnnounceListener struct {
	Registry      *subtreegroup.Registry
	Groups        []*net.UDPAddr    // control group addresses to join
	Iface         *net.Interface    // multicast interface
	DefaultTTL    time.Duration     // applied when announcement TTL == 0
	SenderInclude []*net.IPNet      // nil/empty = accept all non-excluded sources
	SenderExclude []*net.IPNet      // checked before include
	Rec           *metrics.Recorder // nil = no metrics
	Debug         bool
}

// Start listens for SubtreeAnnounce datagrams on all configured groups.
// It also starts a background eviction goroutine (1 s tick).
// Blocks until ctx is cancelled.
func (sl *SubtreeAnnounceListener) Start(ctx context.Context) error {
	go sl.evictLoop(ctx)

	errCh := make(chan error, len(sl.Groups))
	for _, grp := range sl.Groups {
		grp := grp
		go func() {
			errCh <- sl.listenGroup(ctx, grp)
		}()
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (sl *SubtreeAnnounceListener) listenGroup(ctx context.Context, grp *net.UDPAddr) error {
	fd, err := openAnnounceSocket(sl.Iface, grp)
	if err != nil {
		return err
	}

	// Periodic wakeup so we can check ctx cancellation.
	tv := unix.NsecToTimeval((2 * time.Second).Nanoseconds())
	_ = unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv)

	go func() {
		<-ctx.Done()
		_ = unix.Close(fd)
	}()

	buf := make([]byte, frame.SubtreeAnnounceSize+64)

	for {
		n, from, err := unix.Recvfrom(fd, buf, 0)
		if err != nil {
			if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
				if ctx.Err() != nil {
					return nil
				}
				continue
			}
			if err == unix.EBADF || err == unix.EINVAL {
				return nil
			}
			if err == unix.EINTR {
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			slog.Warn("subtree_announce: read error", "group", grp.IP, "err", err)
			continue
		}

		if n < frame.SubtreeAnnounceSize {
			if sl.Rec != nil {
				sl.Rec.SubtreeAnnounceRejected("too_short")
			}
			if sl.Debug {
				slog.Debug("subtree_announce: datagram too short", "n", n)
			}
			continue
		}

		src := sockaddrToUDP(from)
		if !sl.senderAllowed(src) {
			if sl.Rec != nil {
				sl.Rec.SubtreeAnnounceRejected("sender_filter")
			}
			if sl.Debug {
				slog.Debug("subtree_announce: sender rejected by filter", "src", src.IP)
			}
			continue
		}

		ann, err := frame.DecodeSubtreeAnnounce(buf[:n])
		if err != nil {
			if sl.Rec != nil {
				sl.Rec.SubtreeAnnounceRejected("decode_error")
			}
			if sl.Debug {
				slog.Debug("subtree_announce: decode error", "err", err)
			}
			continue
		}

		ttl := sl.DefaultTTL
		if ann.TTL > 0 {
			ttl = time.Duration(ann.TTL) * time.Second
		}
		sl.Registry.Add(ann.GroupID, ann.SubtreeID, ttl)
		if sl.Rec != nil {
			sl.Rec.SubtreeAnnounceReceived()
		}

		if sl.Debug {
			slog.Debug("subtree_announce: added entry",
				"group", hex.EncodeToString(ann.GroupID[:]),
				"subtree", hex.EncodeToString(ann.SubtreeID[:]),
				"ttl", ttl,
				"src", src.IP,
			)
		}
	}
}

// openAnnounceSocket creates a UDP6 socket with SO_REUSEPORT, binds to
// [::]:port, and joins the specified multicast group. SO_REUSEPORT is
// required so the announce listener coexists with the data worker, which
// also binds the same port with SO_REUSEPORT.
func openAnnounceSocket(iface *net.Interface, grp *net.UDPAddr) (int, error) {
	fd, err := unix.Socket(unix.AF_INET6, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("socket: %w", err)
	}
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
		_ = unix.Close(fd)
		return -1, fmt.Errorf("SO_REUSEPORT: %w", err)
	}
	sa := &unix.SockaddrInet6{Port: grp.Port}
	if err := unix.Bind(fd, sa); err != nil {
		_ = unix.Close(fd)
		return -1, fmt.Errorf("bind [::]::%d: %w", grp.Port, err)
	}
	mreq := &unix.IPv6Mreq{Interface: uint32(iface.Index)}
	copy(mreq.Multiaddr[:], grp.IP.To16())
	if err := unix.SetsockoptIPv6Mreq(fd, unix.IPPROTO_IPV6, unix.IPV6_JOIN_GROUP, mreq); err != nil {
		_ = unix.Close(fd)
		return -1, fmt.Errorf("join group %s: %w", grp.IP, err)
	}
	return fd, nil
}

// sockaddrToUDP converts a unix.Sockaddr to *net.UDPAddr.
func sockaddrToUDP(sa unix.Sockaddr) *net.UDPAddr {
	if sa6, ok := sa.(*unix.SockaddrInet6); ok {
		return &net.UDPAddr{IP: net.IP(sa6.Addr[:])}
	}
	return &net.UDPAddr{}
}

// senderAllowed applies exclude → include filtering on the UDP source.
// Returns true if the announcement should be processed. Delegates to
// [filter.SenderACL] so the same logic backs the data-plane worker check.
func (sl *SubtreeAnnounceListener) senderAllowed(src *net.UDPAddr) bool {
	acl := filter.SenderACL{Include: sl.SenderInclude, Exclude: sl.SenderExclude}
	return acl.Allow(src.IP)
}

func (sl *SubtreeAnnounceListener) evictLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			evicted := sl.Registry.Evict()
			if sl.Rec != nil {
				sl.Rec.SubtreeGroupEvicted(evicted, sl.Registry.Len())
			}
		}
	}
}
