//go:build !linux

package listener

import (
	"context"
	"sync"

	"golang.org/x/sys/unix"
)

// recvPipelined off Linux: no recvmmsg, so the pipelined loop reads one
// datagram at a time (servePipelinedSingle).
func (w *Worker) recvPipelined(ctx context.Context, fd int, tee bool) error {
	return w.servePipelinedSingle(ctx, fd, tee)
}

// recvDatagram is one queued datagram: a pooled buffer, the bytes read into it
// and the kernel source address.
type recvDatagram struct {
	buf  *[]byte
	n    int
	from unix.Sockaddr
}

// servePipelinedSingle is the pipelined loop without recvmmsg: one datagram
// per read and per hand-off. Linux uses recvPipelined's batches instead.
func (w *Worker) servePipelinedSingle(ctx context.Context, fd int, tee bool) error {
	pool := sync.Pool{New: func() any { b := make([]byte, maxDatagram); return &b }}
	queue := make(chan recvDatagram, w.recvQueue)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for d := range queue {
			w.handleDatagram((*d.buf)[:d.n], d.from, tee)
			pool.Put(d.buf)
		}
	}()
	defer func() {
		close(queue)
		<-done
	}()
	for {
		bp := pool.Get().(*[]byte)
		n, from, err := unix.Recvfrom(fd, *bp, 0)
		if err != nil {
			pool.Put(bp)
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
			w.log.Error("recvfrom error", "err", err)
			continue
		}
		if n == 0 {
			pool.Put(bp)
			continue
		}
		queue <- recvDatagram{buf: bp, n: n, from: from}
	}
}
