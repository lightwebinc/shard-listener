package listener

import (
	"context"
	"sync"
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
}

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
	}
	return b
}

// recv fills the batch from fd: at least one datagram (MSG_WAITFORONE: the
// call returns as soon as one is there, with whatever else is queued, up to
// recvBatchSize), honouring the socket's receive timeout like recvfrom.
func (b *recvBatch) recv(fd int) (int, error) {
	for i := range b.hdrs {
		b.hdrs[i].Hdr.Namelen = uint32(unsafe.Sizeof(b.names[i]))
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
	pool := sync.Pool{New: func() any { return newRecvBatch() }}
	queue := make(chan *recvBatch, (w.recvQueue+recvBatchSize-1)/recvBatchSize)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for b := range queue {
			for i := 0; i < b.n; i++ {
				if n := int(b.hdrs[i].Len); n > 0 {
					w.handleDatagram(b.bufs[i][:n], b.sockaddr(i), tee)
				}
			}
			pool.Put(b)
		}
	}()
	defer func() {
		close(queue)
		<-done
	}()
	for {
		b := pool.Get().(*recvBatch)
		n, err := b.recv(fd)
		if err != nil {
			pool.Put(b)
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
			pool.Put(b)
			continue
		}
		queue <- b
	}
}
