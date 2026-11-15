package listener

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"

	"github.com/lightwebinc/shard-listener/filter"
	"github.com/lightwebinc/shard-listener/reassembly"
)

// reusedBuf stands in for a pooled receive buffer: while anyone holds it the
// bytes are the datagram's, and at the last release they are overwritten, as
// the receive loop's next read would. A fragment worker that read its
// fragment after releasing it would assemble 0xFF bytes, and the ContentID
// check would refuse the object.
type reusedBuf struct {
	buf  []byte
	refs atomic.Int32
	free chan *reusedBuf
}

func (r *reusedBuf) retain() { r.refs.Add(1) }

func (r *reusedBuf) release() {
	if r.refs.Add(-1) == 0 {
		for i := range r.buf {
			r.buf[i] = 0xFF
		}
		r.free <- r
	}
}

// beefObject is one test object and its fragments on the wire.
type beefObject struct {
	id      [32]byte // ContentID: SHA-256d of payload
	payload []byte
	frags   [][]byte
}

func makeBeefObject(t *testing.T, rng *rand.Rand, topic [32]byte, size, fragSize int, seq *uint64) beefObject {
	t.Helper()
	o := beefObject{payload: make([]byte, size)}
	rng.Read(o.payload)
	first := sha256.Sum256(o.payload)
	o.id = sha256.Sum256(first[:])
	total := (size + fragSize - 1) / fragSize
	for i := 0; i < total; i++ {
		end := min((i+1)*fragSize, size)
		*seq++
		buf := make([]byte, frame.HeaderSizeV3+fragSize)
		n, err := frame.EncodeFragment(buf, o.id, topic, 0xABCD, *seq, uint32(size), uint16(i), uint16(total), frame.FrameVerV9, o.payload[i*fragSize:end])
		if err != nil {
			t.Fatal(err)
		}
		o.frags = append(o.frags, buf[:n])
	}
	return o
}

// fragWorkerRig is a worker with several reassembly buffers whose completed
// BEEF objects are collected by ContentID.
type fragWorkerRig struct {
	w    *Worker
	mu   sync.Mutex
	got  map[[32]byte][]byte
	done atomic.Int32
}

func newFragWorkerRig(t *testing.T, buffers int) *fragWorkerRig {
	t.Helper()
	addr, _, cleanup := newSink(t)
	t.Cleanup(cleanup)
	r := &fragWorkerRig{w: newWorker(t, addr, filter.New(nil, nil, nil, nil)), got: map[[32]byte][]byte{}}
	bufs := make([]*reassembly.Buffer, buffers)
	for i := range bufs {
		bufs[i] = reassembly.New(1024, time.Minute, true, nil) // verifyHash: the ContentID check
		bufs[i].SetBEEFCallback(func(p []byte, bf *frame.BEEFFrame) {
			r.mu.Lock()
			r.got[bf.ContentID] = bytes.Clone(p)
			r.mu.Unlock()
			r.done.Add(1)
		})
	}
	r.w.SetReassemblyBuffers(bufs)
	r.w.SetRecvQueueDepth(64)
	return r
}

// feed pushes every datagram through handleDatagram the way the pipelined
// receive loop does: from a few reused buffers, each held for the call.
func (r *fragWorkerRig) feed(t *testing.T, datagrams [][]byte) {
	t.Helper()
	const pooled = 4
	free := make(chan *reusedBuf, pooled)
	for i := 0; i < pooled; i++ {
		free <- &reusedBuf{buf: make([]byte, 2048), free: free}
	}
	for _, d := range datagrams {
		var rb *reusedBuf
		select {
		case rb = <-free:
		case <-time.After(5 * time.Second):
			t.Fatal("no receive buffer came back: a hold was not released")
		}
		n := copy(rb.buf, d)
		rb.refs.Store(1)
		r.w.handleDatagram(rb.buf[:n], nil, false, rb)
		rb.release()
	}
}

func (r *fragWorkerRig) wait(t *testing.T, objects int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for int(r.done.Load()) < objects {
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d objects reassembled", r.done.Load(), objects)
		}
		time.Sleep(time.Millisecond)
	}
}

func (r *fragWorkerRig) check(t *testing.T, objs []beefObject) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, o := range objs {
		if !bytes.Equal(r.got[o.id], o.payload) {
			t.Fatalf("object %d: reassembled bytes differ", i)
		}
	}
}

// TestFragWorkers_InterleavedObjects: many objects' fragments, interleaved
// and out of order, through four fragment workers and reused receive buffers.
// Every object must assemble byte for byte.
func TestFragWorkers_InterleavedObjects(t *testing.T) {
	r := newFragWorkerRig(t, 4)
	stop := r.w.startFragWorkers(context.Background())
	defer stop()
	if r.w.fragQ == nil {
		t.Fatal("fragment workers did not start")
	}

	rng := rand.New(rand.NewSource(1))
	var topic [32]byte
	topic[0] = 0x42
	var seq uint64
	var objs []beefObject
	var wire [][]byte
	shards := map[int]bool{}
	for i := 0; i < 64; i++ {
		o := makeBeefObject(t, rng, topic, 20_000+rng.Intn(20_000), 1024, &seq)
		objs = append(objs, o)
		wire = append(wire, o.frags...)
		shards[int(binary.LittleEndian.Uint64(o.id[:8])%4)] = true
	}
	if len(shards) != 4 {
		t.Fatalf("objects reached %d of 4 buffers", len(shards))
	}
	rng.Shuffle(len(wire), func(i, j int) { wire[i], wire[j] = wire[j], wire[i] })

	r.feed(t, wire)
	r.wait(t, len(objs))
	r.check(t, objs)
}

// TestFragWorkers_OneBufferStoresInline: a single buffer starts no workers
// and the fragment is stored before handleDatagram returns.
func TestFragWorkers_OneBufferStoresInline(t *testing.T) {
	r := newFragWorkerRig(t, 1)
	stop := r.w.startFragWorkers(context.Background())
	defer stop()
	if r.w.fragQ != nil {
		t.Fatal("one buffer must not start fragment workers")
	}
	rng := rand.New(rand.NewSource(2))
	var seq uint64
	o := makeBeefObject(t, rng, [32]byte{1}, 5000, 1024, &seq)
	r.feed(t, o.frags)
	if r.done.Load() != 1 {
		t.Fatal("object not complete when the last fragment's call returned")
	}
	r.check(t, []beefObject{o})
}

// TestFragWorkers_OtherClassesInline: a class that is not a BEEF object is
// stored inline, in the buffer its identifier selects, so it still completes
// with workers running.
func TestFragWorkers_OtherClassesInline(t *testing.T) {
	r := newFragWorkerRig(t, 4)
	var mu sync.Mutex
	var got []byte
	for _, b := range r.w.reassemBufs {
		b.SetBlockCallback(func(p []byte, _ *frame.BlockFrame) {
			mu.Lock()
			got = bytes.Clone(p)
			mu.Unlock()
		})
	}
	stop := r.w.startFragWorkers(context.Background())
	defer stop()

	payload := bytes.Repeat([]byte("block"), 700)
	var id [32]byte
	id[0], id[9] = 7, 9
	const fragSize = 1024
	total := (len(payload) + fragSize - 1) / fragSize
	for i := 0; i < total; i++ {
		end := min((i+1)*fragSize, len(payload))
		buf := make([]byte, frame.HeaderSizeV3+fragSize)
		n, err := frame.EncodeFragment(buf, id, [32]byte{}, 0x77, uint64(i+1), uint32(len(payload)), uint16(i), uint16(total), frame.FrameVerV4, payload[i*fragSize:end])
		if err != nil {
			t.Fatal(err)
		}
		r.w.handleDatagram(buf[:n], nil, false, nil)
	}
	mu.Lock()
	defer mu.Unlock()
	if !bytes.Equal(got, payload) {
		t.Fatal("block object not reassembled inline")
	}
}

// TestFragWorkers_ReinjectedFragment: a fragment recovered over the NACK
// return channel has no pooled buffer behind it and still reaches its worker.
func TestFragWorkers_ReinjectedFragment(t *testing.T) {
	r := newFragWorkerRig(t, 4)
	stop := r.w.startFragWorkers(context.Background())
	defer stop()
	rng := rand.New(rand.NewSource(3))
	var seq uint64
	o := makeBeefObject(t, rng, [32]byte{3}, 9000, 1024, &seq)
	r.feed(t, o.frags[:len(o.frags)-1])
	if !r.w.Reinject(bytes.Clone(o.frags[len(o.frags)-1])) {
		t.Fatal("reinjected fragment refused")
	}
	r.wait(t, 1)
	r.check(t, []beefObject{o})
}

// TestFragWorkers_StopReleasesHolds: fragments still queued when the workers
// stop give their receive buffers back, and a fragment offered afterwards is
// released at once rather than queued forever.
func TestFragWorkers_StopReleasesHolds(t *testing.T) {
	r := newFragWorkerRig(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	stop := r.w.startFragWorkers(ctx)
	rng := rand.New(rand.NewSource(4))
	var seq uint64
	o := makeBeefObject(t, rng, [32]byte{4}, 64_000, 1024, &seq)
	r.feed(t, o.frags)
	cancel()
	stop()

	free := make(chan *reusedBuf, 1)
	rb := &reusedBuf{buf: make([]byte, 2048), free: free}
	n := copy(rb.buf, o.frags[0])
	rb.refs.Store(1)
	r.w.handleDatagram(rb.buf[:n], nil, false, rb)
	rb.release()
	select {
	case <-free:
	case <-time.After(2 * time.Second):
		t.Fatal("a fragment offered after the workers stopped kept its buffer")
	}
}
