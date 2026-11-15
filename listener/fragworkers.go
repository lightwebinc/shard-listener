package listener

import (
	"context"
	"encoding/binary"
	"sync"

	"github.com/lightwebinc/shard-common/frame"

	"github.com/lightwebinc/shard-listener/reassembly"
)

// Fragment workers (SetReassemblyBuffers with more than one buffer).
//
// All of one topic's fragments reach one receive worker, and on its processing
// goroutine reassembly is most of the work: every fragment's bytes are copied
// into its object's buffer, and that buffer is allocated there. What else the
// goroutine does per fragment (the decode, the gap tracker) is small and must
// run in arrival order. Reassembly need not: a fragment carries its index, so
// an object assembles the same whatever order its fragments are stored in, as
// long as they all reach ONE buffer.
//
// So there are several buffers, each drained by a goroutine of its own, and a
// fragment goes to the buffer its object selects. The processing goroutine
// keeps the decode and the gap tracker; the copy, the allocation and the
// completed object's delivery run on the fragment worker. Only BEEF objects
// (FrameVer 0x09) are handed over: they are the plane's bulk, and their
// delivery touches nothing the processing goroutine serializes. Every other
// class is stored inline as before, in the buffer its key selects.
//
// Nothing is dropped here. A fragment worker that falls behind fills its
// queue, the processing goroutine waits on it, the receive queue fills behind
// that, and the kernel buffer absorbs the burst, exactly as without workers.

// fragQueueDepth is the fragments queued for one fragment worker: at a
// 9000-byte fragment MTU, a few megabytes of datagram buffers held at most.
const fragQueueDepth = 512

// datagramHold keeps a received datagram's buffer from being reused while a
// fragment worker still reads it. The receive loops pool their buffers, and a
// fragment's bytes are a view into one.
type datagramHold interface {
	retain()
	release()
}

// fragJob is one fragment for a fragment worker and, when its bytes sit in a
// pooled receive buffer, the hold to release once they are stored.
type fragJob struct {
	ff   *frame.FragFrame
	hold datagramHold
}

// SetReassemblyBuffers attaches several reassembly buffers. A fragment goes to
// the buffer its object's identifier selects, so each object assembles in one
// of them; with the receive queue on (SetRecvQueueDepth) each buffer also gets
// a fragment worker (see the comment at the top of this file). One buffer is
// SetReassemblyBuffer. Every buffer must carry the same configuration and
// callbacks. Must be called before Run.
func (w *Worker) SetReassemblyBuffers(bufs []*reassembly.Buffer) {
	switch len(bufs) {
	case 0:
		w.reassemBuf, w.reassemBufs = nil, nil
	case 1:
		w.reassemBuf, w.reassemBufs = bufs[0], nil
	default:
		w.reassemBuf, w.reassemBufs = bufs[0], bufs
	}
}

// fragShard is the buffer a fragment's object assembles in. Every fragment of
// an object carries the same identifier (TxID, or the ContentID for a BEEF
// object), so they all select the same one.
func (w *Worker) fragShard(ff *frame.FragFrame) int {
	return int(binary.LittleEndian.Uint64(ff.TxID[:8]) % uint64(len(w.reassemBufs)))
}

// bufferFor is the reassembly buffer for ff.
func (w *Worker) bufferFor(ff *frame.FragFrame) *reassembly.Buffer {
	if w.reassemBufs == nil {
		return w.reassemBuf
	}
	return w.reassemBufs[w.fragShard(ff)]
}

// startFragWorkers starts one goroutine per reassembly buffer and returns the
// function that stops them and waits. It does nothing with a single buffer.
// The queues are published under procMu, which is what the processing
// goroutine and Reinject hold when they read them.
func (w *Worker) startFragWorkers(ctx context.Context) (stop func()) {
	if len(w.reassemBufs) < 2 {
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	qs := make([]chan fragJob, len(w.reassemBufs))
	var wg sync.WaitGroup
	for i := range qs {
		qs[i] = make(chan fragJob, fragQueueDepth)
		wg.Add(1)
		go func(q chan fragJob, buf *reassembly.Buffer) {
			defer wg.Done()
			for {
				select {
				case j := <-q:
					buf.Observe(j.ff)
					if j.hold != nil {
						j.hold.release()
					}
				case <-ctx.Done():
					drainFragQueue(q)
					return
				}
			}
		}(qs[i], w.reassemBufs[i])
	}
	w.procMu.Lock()
	w.fragQ, w.fragCtx = qs, ctx
	w.procMu.Unlock()
	return func() {
		cancel()
		wg.Wait()
	}
}

// dispatchFragment hands a BEEF fragment to its fragment worker and reports
// whether it did. It is called under procMu. False (no workers running, or
// another class) leaves the fragment to the caller.
func (w *Worker) dispatchFragment(ff *frame.FragFrame) bool {
	if w.fragQ == nil || ff.OrigFrameVer != frame.FrameVerV9 {
		return false
	}
	j := fragJob{ff: ff, hold: w.curHold}
	if j.hold != nil {
		j.hold.retain()
	}
	q := w.fragQ[w.fragShard(ff)]
	select {
	case q <- j:
		// The workers may have stopped, and drained, while this was queued:
		// nothing would read it then, so give its buffer back here.
		if w.fragCtx.Err() != nil {
			drainFragQueue(q)
		}
	case <-w.fragCtx.Done():
		// Shutting down: the fragment is not stored.
		if j.hold != nil {
			j.hold.release()
		}
	}
	return true
}

// drainFragQueue releases the receive buffers of the fragments still queued
// on q, without storing them: the workers have stopped, and the receive loop
// is waiting for its buffers.
func drainFragQueue(q chan fragJob) {
	for {
		select {
		case j := <-q:
			if j.hold != nil {
				j.hold.release()
			}
		default:
			return
		}
	}
}
