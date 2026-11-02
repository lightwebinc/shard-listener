package listener

import (
	"context"
	"log/slog"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/teewire"

	"github.com/lightwebinc/shard-listener/filter"
)

// newTeeSink is newSink on IPv6 loopback: the stand-in for a retry-endpoint's
// -tee-listen socket.
func newTeeSink(t *testing.T) (string, <-chan []byte, func()) {
	t.Helper()
	return newTeeSinkDepth(t, 16)
}

// newTeeSinkDepth is newTeeSink with a receive channel of the given depth, for
// tests that send more datagrams than they drain at once.
func newTeeSinkDepth(t *testing.T, depth int) (string, <-chan []byte, func()) {
	t.Helper()
	pc, err := net.ListenPacket("udp6", "[::1]:0")
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	ch := make(chan []byte, depth)
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 65536)
		for {
			_ = pc.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			n, _, err := pc.ReadFrom(buf)
			if n > 0 {
				cp := make([]byte, n)
				copy(cp, buf[:n])
				select {
				case ch <- cp:
				default:
				}
			}
			if err != nil {
				select {
				case <-done:
					return
				default:
				}
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				return
			}
		}
	}()
	return pc.LocalAddr().String(), ch, func() {
		close(done)
		_ = pc.Close()
	}
}

// startIngestWorker builds a worker with the retry tee wired to teeAddr and
// runs the given ingest entry point until test cleanup.
func startIngestWorker(t *testing.T, ingestPort int, teeAddr string, run func(*Worker, context.Context) error) *Worker {
	t.Helper()
	sinkAddr, _, sinkCleanup := newSink(t)
	t.Cleanup(sinkCleanup)
	w := newWorker(t, sinkAddr, filter.New(nil, nil, nil, nil))
	w.port = ingestPort
	if err := w.SetRetryTee(teeAddr); err != nil {
		t.Fatalf("SetRetryTee: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- run(w, ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			_ = err // the loop's exit error after cancel is expected
		case <-time.After(2 * time.Second):
			t.Error("ingest loop did not exit on cancel")
		}
	})
	time.Sleep(50 * time.Millisecond) // let the socket bind
	return w
}

func dialIngest(t *testing.T, port int) *net.UDPConn {
	t.Helper()
	ua, err := net.ResolveUDPAddr("udp6", net.JoinHostPort("::1", strconv.Itoa(port)))
	if err != nil {
		t.Fatal(err)
	}
	c, err := net.DialUDP("udp6", nil, ua)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func waitTee(t *testing.T, ch <-chan []byte) []byte {
	t.Helper()
	select {
	case pkt := <-ch:
		return pkt
	case <-time.After(2 * time.Second):
		t.Fatal("no tee datagram within 2s")
		return nil
	}
}

func assertNoTee(t *testing.T, ch <-chan []byte) {
	t.Helper()
	select {
	case pkt := <-ch:
		t.Fatalf("unexpected tee datagram (%d bytes)", len(pkt))
	case <-time.After(300 * time.Millisecond):
	}
}

func TestRetryTee_MirrorsReceivedFrameWithSource(t *testing.T) {
	teeAddr, teeCh, teeCleanup := newTeeSink(t)
	defer teeCleanup()

	const ingestPort = 39041
	startIngestWorker(t, ingestPort, teeAddr, func(w *Worker, ctx context.Context) error {
		return w.RunUnicastIngest(ctx)
	})

	client := dialIngest(t, ingestPort)
	raw := buildSequencedFrame(t, sha256d([]byte("tee-tx")), []byte("tee-tx"), 1)
	if _, err := client.Write(raw); err != nil {
		t.Fatal(err)
	}

	pkt := waitTee(t, teeCh)
	if !teewire.IsEncap(pkt) {
		t.Fatalf("tee datagram is not enveloped (leading bytes %x)", pkt[:4])
	}
	src, payload, err := teewire.Decap(pkt)
	if err != nil {
		t.Fatalf("Decap: %v", err)
	}
	if len(payload) != len(raw) {
		t.Fatalf("payload %d bytes, want %d", len(payload), len(raw))
	}
	for i := range raw {
		if payload[i] != raw[i] {
			t.Fatalf("payload differs from wire frame at offset %d", i)
		}
	}
	want := client.LocalAddr().(*net.UDPAddr)
	if src.Port() != uint16(want.Port) {
		t.Errorf("envelope source port = %d, want %d", src.Port(), want.Port)
	}
	wantIP, _ := netip.AddrFromSlice(want.IP.To16())
	if src.Addr() != wantIP {
		t.Errorf("envelope source = %s, want %s", src.Addr(), wantIP)
	}
}

func TestRetryTee_SkipsControlAndHeaderFrames(t *testing.T) {
	teeAddr, teeCh, teeCleanup := newTeeSink(t)
	defer teeCleanup()

	const ingestPort = 39042
	startIngestWorker(t, ingestPort, teeAddr, func(w *Worker, ctx context.Context) error {
		return w.RunUnicastIngest(ctx)
	})
	client := dialIngest(t, ingestPort)

	// BRC-126-shaped control datagram: fabric magic, MsgType 0x10 at the
	// version offset. Must not be mirrored.
	nack := make([]byte, 64)
	nack[0], nack[1], nack[2], nack[3] = 0xE3, 0xE1, 0xF3, 0xE8
	nack[6] = 0x10
	if _, err := client.Write(nack); err != nil {
		t.Fatal(err)
	}

	// BRC-135 header frame (V7): excluded from the tee.
	hdr := make([]byte, frame.HeaderSize+80)
	hdr[0], hdr[1], hdr[2], hdr[3] = 0xE3, 0xE1, 0xF3, 0xE8
	hdr[6] = frame.FrameVerV7
	if _, err := client.Write(hdr); err != nil {
		t.Fatal(err)
	}

	// Not even the magic: junk. Must not be mirrored.
	if _, err := client.Write([]byte{1, 2, 3, 4, 5, 6, 7, 8}); err != nil {
		t.Fatal(err)
	}

	// A data frame after all of it must be the FIRST thing mirrored.
	raw := buildSequencedFrame(t, sha256d([]byte("after")), []byte("after"), 1)
	if _, err := client.Write(raw); err != nil {
		t.Fatal(err)
	}

	pkt := waitTee(t, teeCh)
	_, payload, err := teewire.Decap(pkt)
	if err != nil {
		t.Fatalf("Decap: %v", err)
	}
	if len(payload) != len(raw) {
		t.Fatalf("first mirrored datagram is %d bytes, want the %d-byte data frame — a control/header/junk datagram leaked into the tee", len(payload), len(raw))
	}
	assertNoTee(t, teeCh)
}

func TestRetryTee_LocalMirrorPathDoesNotTee(t *testing.T) {
	// RunUnicastIngestOn is the collapsed-edge local mirror: own-origin
	// frames the proxy's -retry-tee already mirrors. Teeing them again
	// would double-store under a loopback source label.
	teeAddr, teeCh, teeCleanup := newTeeSink(t)
	defer teeCleanup()

	const mirrorPort = 39043
	startIngestWorker(t, mirrorPort, teeAddr, func(w *Worker, ctx context.Context) error {
		return w.RunUnicastIngestOn(ctx, mirrorPort)
	})
	client := dialIngest(t, mirrorPort)

	raw := buildSequencedFrame(t, sha256d([]byte("own")), []byte("own"), 1)
	if _, err := client.Write(raw); err != nil {
		t.Fatal(err)
	}
	assertNoTee(t, teeCh)
}

func TestTeeEligible(t *testing.T) {
	mk := func(ver byte) []byte {
		b := make([]byte, 92)
		b[0], b[1], b[2], b[3] = 0xE3, 0xE1, 0xF3, 0xE8
		b[6] = ver
		return b
	}
	for ver := byte(0x01); ver <= 0x09; ver++ {
		want := ver != frame.FrameVerV7
		if got := teeEligible(mk(ver)); got != want {
			t.Errorf("teeEligible(V%d) = %v, want %v", ver, got, want)
		}
	}
	for _, ver := range []byte{0x00, 0x10, 0x11, 0x20, 0x30, 0x40, 0xFF} {
		if teeEligible(mk(ver)) {
			t.Errorf("teeEligible(type 0x%02X) = true, want false", ver)
		}
	}
	if teeEligible([]byte{0xE3, 0xE1, 0xF3}) {
		t.Error("teeEligible(short) = true, want false")
	}
	junk := mk(frame.FrameVerV2)
	junk[0] = 0x00
	if teeEligible(junk) {
		t.Error("teeEligible(bad magic) = true, want false")
	}
}

// dialTee connects a tee socket to the sink at addr, the way SetRetryTee does.
func dialTee(t *testing.T, addr string) *net.UDPConn {
	t.Helper()
	ua, err := net.ResolveUDPAddr("udp6", addr)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialUDP("udp6", nil, ua)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

// teeSource is a kernel source address of the shape handleDatagram passes.
func teeSource(port int) *unix.SockaddrInet6 {
	sa := &unix.SockaddrInet6{Port: port}
	sa.Addr[15] = 1 // ::1
	return sa
}

// decapSeq decodes a mirrored datagram and returns its frame's SeqNum and the
// envelope's source port.
func decapSeq(t *testing.T, pkt []byte) (uint64, uint16) {
	t.Helper()
	src, payload, err := teewire.Decap(pkt)
	if err != nil {
		t.Fatalf("Decap: %v", err)
	}
	f, err := frame.Decode(payload)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return f.SeqNum, src.Port()
}

// With the sender keeping up, every eligible frame is mirrored exactly once:
// the hand-off to the tee goroutine loses nothing on its own.
func TestRetryTee_AllFramesArriveWhenQueueKeepsUp(t *testing.T) {
	const n = 200
	teeAddr, teeCh, teeCleanup := newTeeSinkDepth(t, n)
	defer teeCleanup()

	const ingestPort = 39044
	w := startIngestWorker(t, ingestPort, teeAddr, func(w *Worker, ctx context.Context) error {
		return w.RunUnicastIngest(ctx)
	})
	client := dialIngest(t, ingestPort)
	for i := 1; i <= n; i++ {
		raw := buildSequencedFrame(t, sha256d([]byte("all")), []byte("all"), uint64(i))
		if _, err := client.Write(raw); err != nil {
			t.Fatal(err)
		}
	}

	seen := make(map[uint64]int, n)
	for i := 0; i < n; i++ {
		seq, _ := decapSeq(t, waitTee(t, teeCh))
		seen[seq]++
	}
	for i := uint64(1); i <= n; i++ {
		if seen[i] != 1 {
			t.Fatalf("frame %d mirrored %d times, want once", i, seen[i])
		}
	}
	assertNoTee(t, teeCh)
	if got := w.retryTee.dropped.Load(); got != 0 {
		t.Fatalf("tee dropped %d datagrams with an idle sender", got)
	}
}

// A full queue drops the datagram and counts it; the queued ones still go
// out, in order and with their source, once the sender runs. The sender is
// held back by not starting it, the one deterministic way to fill the queue.
func TestRetryTee_FullQueueDropsAndCounts(t *testing.T) {
	teeAddr, teeCh, teeCleanup := newTeeSinkDepth(t, 16)
	defer teeCleanup()

	const depth = 4
	tee := newRetryTeeSender(dialTee(t, teeAddr), nil, 0, slog.Default(), depth)
	w := &Worker{retryTee: tee}
	from := teeSource(4321)
	const sent = 10
	for i := 1; i <= sent; i++ {
		w.teeToRetry(buildSequencedFrame(t, sha256d([]byte("full")), []byte("full"), uint64(i)), from)
	}
	if got := tee.dropped.Load(); got != sent-depth {
		t.Fatalf("dropped = %d, want %d (queue depth %d, %d offered)", got, sent-depth, depth, sent)
	}
	assertNoTee(t, teeCh) // nothing is written before the sender runs

	tee.start()
	for i := 1; i <= depth; i++ {
		seq, port := decapSeq(t, waitTee(t, teeCh))
		if seq != uint64(i) {
			t.Fatalf("mirrored frame %d has SeqNum %d: the queue reordered or lost the first frames", i, seq)
		}
		if port != 4321 {
			t.Fatalf("envelope source port = %d, want 4321", port)
		}
	}
	assertNoTee(t, teeCh)

	// Once drained the queue takes datagrams again, and they are not counted.
	w.teeToRetry(buildSequencedFrame(t, sha256d([]byte("full")), []byte("full"), 99), from)
	if seq, _ := decapSeq(t, waitTee(t, teeCh)); seq != 99 {
		t.Fatalf("post-drain frame has SeqNum %d, want 99", seq)
	}
	if got := tee.dropped.Load(); got != sent-depth {
		t.Fatalf("dropped = %d after the queue drained, want still %d", got, sent-depth)
	}
	if err := tee.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// A closed tee drops and counts; it must not panic on the closed queue.
	w.teeToRetry(buildSequencedFrame(t, sha256d([]byte("full")), []byte("full"), 100), from)
	if got := tee.dropped.Load(); got != sent-depth+1 {
		t.Fatalf("dropped = %d after close, want %d", got, sent-depth+1)
	}
	assertNoTee(t, teeCh)
}

// close writes what is queued before the socket goes: the cache gets the tail
// of the receive stream on shutdown, started sender or not.
func TestRetryTee_CloseDrainsQueue(t *testing.T) {
	teeAddr, teeCh, teeCleanup := newTeeSinkDepth(t, 16)
	defer teeCleanup()

	tee := newRetryTeeSender(dialTee(t, teeAddr), nil, 0, slog.Default(), 8)
	w := &Worker{retryTee: tee}
	from := teeSource(5555)
	for i := 1; i <= 3; i++ {
		w.teeToRetry(buildSequencedFrame(t, sha256d([]byte("drain")), []byte("drain"), uint64(i)), from)
	}
	if err := tee.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	for i := 1; i <= 3; i++ {
		if seq, _ := decapSeq(t, waitTee(t, teeCh)); seq != uint64(i) {
			t.Fatalf("drained frame %d has SeqNum %d", i, seq)
		}
	}
	if got := tee.dropped.Load(); got != 0 {
		t.Fatalf("close dropped %d queued datagrams instead of draining them", got)
	}
	if err := tee.close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}
