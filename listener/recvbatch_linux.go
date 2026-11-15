package listener

import (
	"context"
	"encoding/binary"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

// recvBatchSize is how many datagrams one recvmmsg may return. One syscall,
// one pool round trip and one channel hand-off then cover a whole batch
// instead of one datagram: measured on the lab edge, the per-datagram path
// (recvfrom, pool, channel send, processor wake) capped a worker near 20,000
// datagrams a second while the processing goroutine sat idle.
const recvBatchSize = 32

// mmsghdr mirrors struct mmsghdr: a msghdr and the bytes the kernel wrote.
// On amd64 msghdr is 56 bytes and the struct pads to 64; the size is asserted
// in the tests.
type mmsghdr struct {
	Hdr unix.Msghdr
	Len uint32
	_   [4]byte
}

// recvBatch is one recvmmsg's worth of datagrams: pooled buffers, the raw
// source sockaddrs the kernel filled, and the headers that point at both.
type recvBatch struct {
	bufs  [recvBatchSize][]byte
	names [recvBatchSize]unix.RawSockaddrInet6
	iovs  [recvBatchSize]unix.Iovec
	hdrs  [recvBatchSize]mmsghdr
	n     int
	// ctl holds each message's control data: with receive GRO on, the kernel
	// says there how long the datagrams coalesced into the message are.
	ctl [recvBatchSize][groCtlSize]byte

	// refs counts who still reads the batch's buffers: the processing
	// goroutine while it walks the batch, and each fragment a fragment worker
	// has not stored yet. At zero the batch goes back to pool.
	refs atomic.Int32
	pool *batchPool
}

func (b *recvBatch) retain() { b.refs.Add(1) }

func (b *recvBatch) release() {
	if b.refs.Add(-1) == 0 && b.pool != nil {
		b.pool.put(b)
	}
}

// recvBatchHold is how many batches, beyond those the receive queue holds,
// fragments waiting for a fragment worker may keep from reuse.
const recvBatchHold = 32

// batchPool is one worker's receive batches. It makes them as they are first
// needed and never more than max. A fragment a fragment worker has not stored
// yet keeps its whole batch from reuse (recvBatchSize buffers of maxDatagram
// bytes), so with no bound a fragment worker that fell behind could hold a
// batch for every fragment queued for it. At the bound the reader waits for a
// batch to come back and the socket buffer takes the burst, which is what
// happens when the processing goroutine falls behind.
type batchPool struct {
	free chan *recvBatch
	made int // batches made so far; the reader goroutine's alone
	max  int
}

func newBatchPool(max int) *batchPool {
	return &batchPool{free: make(chan *recvBatch, max), max: max}
}

// get returns a batch free for use: one that came back, a new one while fewer
// than max exist, or else the next to come back. It returns nil if ctx ends
// first. Only the reader goroutine calls it.
func (p *batchPool) get(ctx context.Context) *recvBatch {
	select {
	case b := <-p.free:
		return b
	default:
	}
	if p.made < p.max {
		p.made++
		b := newRecvBatch()
		b.pool = p
		return b
	}
	select {
	case b := <-p.free:
		return b
	case <-ctx.Done():
		return nil
	}
}

// put takes a batch back. It never waits: free has room for every batch made.
func (p *batchPool) put(b *recvBatch) { p.free <- b }

func newRecvBatch() *recvBatch {
	b := &recvBatch{}
	for i := range b.bufs {
		b.bufs[i] = make([]byte, maxDatagram)
		b.iovs[i].Base = &b.bufs[i][0]
		b.iovs[i].SetLen(maxDatagram)
		b.hdrs[i].Hdr.Name = (*byte)(unsafe.Pointer(&b.names[i]))
		b.hdrs[i].Hdr.Namelen = uint32(unsafe.Sizeof(b.names[i]))
		b.hdrs[i].Hdr.Iov = &b.iovs[i]
		b.hdrs[i].Hdr.SetIovlen(1)
		b.hdrs[i].Hdr.Control = &b.ctl[i][0]
	}
	return b
}

// Receive GRO (SetRecvGRO).
//
// A sender that hands the kernel runs of equal datagrams (UDP GSO, as the
// proxy's group sends are) puts them on the wire back to back, and a receiving
// socket with UDP_GRO set gets such a run back as ONE message: the datagrams'
// payloads concatenated, with their common length in a control message. The
// bytes are the same and so is their order, but the kernel queues, accounts
// and hands over one buffer where it handled several, and the read loop makes
// a seventh of the trips for a plane of 9000-byte fragments (seven fit under
// the 64 KiB a message may carry). The receive loop splits the message back
// into its datagrams, so nothing past this file sees a difference.
//
// The option is the receiving socket's own. Multicast delivery hands a
// coalesced run to each socket that asked for it and splits it again for one
// that did not, so listeners with and without it share a host.

// groCtlSize is the control buffer per message: room for the one cmsg the
// kernel adds (a cmsghdr and the 4-byte segment size, aligned).
const groCtlSize = 32

// enableGRO asks for coalesced runs on fd. An error means the kernel has no
// UDP_GRO; the socket then delivers single datagrams as before.
func enableGRO(fd int) error {
	return unix.SetsockoptInt(fd, unix.IPPROTO_UDP, unix.UDP_GRO, 1)
}

// segSize is the length of the datagrams coalesced into message i, or 0 when
// the message is a single datagram (no UDP_GRO control message came with it).
func (b *recvBatch) segSize(i int) int {
	ctl := b.ctl[i][:b.hdrs[i].Hdr.Controllen]
	for len(ctl) >= unix.SizeofCmsghdr {
		h := (*unix.Cmsghdr)(unsafe.Pointer(&ctl[0]))
		if h.Len < unix.SizeofCmsghdr || int(h.Len) > len(ctl) {
			return 0
		}
		if h.Level == unix.IPPROTO_UDP && h.Type == unix.UDP_GRO && h.Len >= unix.SizeofCmsghdr+4 {
			// The kernel writes an int in host byte order.
			return int(binary.NativeEndian.Uint32(ctl[unix.SizeofCmsghdr:]))
		}
		next := (int(h.Len) + unix.SizeofPtr - 1) &^ (unix.SizeofPtr - 1)
		if next >= len(ctl) {
			return 0
		}
		ctl = ctl[next:]
	}
	return 0
}

// message is message i's bytes and the length of the datagrams coalesced
// into it: 0 when it is a single datagram. A coalesced run's datagrams are
// seg bytes each, in arrival order, but the last, which may be shorter.
func (b *recvBatch) message(i int) (msg []byte, seg int) {
	n := int(b.hdrs[i].Len)
	if n <= 0 {
		return nil, 0
	}
	msg = b.bufs[i][:n]
	if seg = b.segSize(i); seg >= n {
		seg = 0
	}
	return msg, seg
}

// recv fills the batch from fd: at least one datagram (MSG_WAITFORONE: the
// call returns as soon as one is there, with whatever else is queued, up to
// recvBatchSize), honoring the socket's receive timeout like recvfrom.
func (b *recvBatch) recv(fd int) (int, error) {
	for i := range b.hdrs {
		b.hdrs[i].Hdr.Namelen = uint32(unsafe.Sizeof(b.names[i]))
		b.hdrs[i].Hdr.SetControllen(groCtlSize)
		b.hdrs[i].Len = 0
	}
	n, _, errno := unix.Syscall6(unix.SYS_RECVMMSG, uintptr(fd), uintptr(unsafe.Pointer(&b.hdrs[0])), uintptr(recvBatchSize), uintptr(unix.MSG_WAITFORONE), 0, 0)
	if errno != 0 {
		return 0, errno
	}
	b.n = int(n)
	return b.n, nil
}

// sockaddr converts the kernel-filled name of datagram i to what the sender
// filter and gap tracker read.
func (b *recvBatch) sockaddr(i int) unix.Sockaddr {
	raw := &b.names[i]
	if raw.Family != unix.AF_INET6 {
		return nil
	}
	sa := &unix.SockaddrInet6{ZoneId: raw.Scope_id}
	// Port is in network byte order in the raw struct.
	sa.Port = int(raw.Port>>8) | int(raw.Port&0xff)<<8
	sa.Addr = raw.Addr
	return sa
}

// recvPipelined is the Linux receive side of the pipelined serve loop: one
// recvmmsg per batch, one hand-off per batch.
func (w *Worker) recvPipelined(ctx context.Context, fd int, tee bool) error {
	if w.recvGRO {
		if err := enableGRO(fd); err != nil {
			w.log.Warn("receive GRO not available: reading single datagrams", "err", err)
		}
	}
	queue := make(chan *recvBatch, (w.recvQueue+recvBatchSize-1)/recvBatchSize)
	// Every batch that can be in use at once: queued, read into, walked by the
	// processing goroutine, and those held for fragment workers.
	pool := newBatchPool(cap(queue) + 2 + recvBatchHold)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for b := range queue {
			b.refs.Store(1)
			for i := 0; i < b.n; i++ {
				msg, seg := b.message(i)
				if msg == nil {
					continue
				}
				from := b.sockaddr(i)
				if seg > 0 {
					// A coalesced run: its datagrams, in order.
					for len(msg) > seg {
						w.handleDatagram(msg[:seg], from, tee, b)
						msg = msg[seg:]
					}
				}
				w.handleDatagram(msg, from, tee, b)
			}
			b.release()
		}
	}()
	defer func() {
		close(queue)
		<-done
	}()
	for {
		b := pool.get(ctx)
		if b == nil {
			return nil
		}
		n, err := b.recv(fd)
		if err != nil {
			pool.put(b)
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
			w.log.Error("recvmmsg error", "err", err)
			continue
		}
		if n == 0 {
			pool.put(b)
			continue
		}
		queue <- b
	}
}
