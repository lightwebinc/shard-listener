//go:build !linux

package listener

import (
	"context"
	"sync"
	"sync/atomic"

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
	buf  []byte
	n    int
	from unix.Sockaddr

	// refs counts who still reads buf: the processing goroutine, and a
	// fragment worker that has not stored the fragment yet. At zero the
	// datagram goes back to pool.
	refs atomic.Int32
	pool *sync.Pool
}

func (d *recvDatagram) retain() { d.refs.Add(1) }

func (d *recvDatagram) release() {
	if d.refs.Add(-1) == 0 {
		d.pool.Put(d)
	}
}

// servePipelinedSingle is the pipelined loop without recvmmsg: one datagram
// per read and per hand-off. Linux uses recvPipelined's batches instead.
func (w *Worker) servePipelinedSingle(ctx context.Context, fd int, tee bool) error {
	var pool sync.Pool
	pool.New = func() any { return &recvDatagram{buf: make([]byte, maxDatagram), pool: &pool} }
	queue := make(chan *recvDatagram, w.recvQueue)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for d := range queue {
			d.refs.Store(1)
			w.handleDatagram(d.buf[:d.n], d.from, tee, d)
			d.release()
		}
	}()
	defer func() {
		close(queue)
		<-done
	}()
	for {
		d := pool.Get().(*recvDatagram)
		n, from, err := unix.Recvfrom(fd, d.buf, 0)
		if err != nil {
			pool.Put(d)
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
			pool.Put(d)
			continue
		}
		d.n, d.from = n, from
		queue <- d
	}
}
