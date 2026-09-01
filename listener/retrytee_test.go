package listener

import (
	"context"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/teewire"

	"github.com/lightwebinc/shard-listener/filter"
)

// newTeeSink is newSink on IPv6 loopback: the stand-in for a retry-endpoint's
// -tee-listen socket.
func newTeeSink(t *testing.T) (string, <-chan []byte, func()) {
	t.Helper()
	pc, err := net.ListenPacket("udp6", "[::1]:0")
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	ch := make(chan []byte, 16)
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
		case <-errCh:
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
