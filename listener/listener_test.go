package listener

import (
	"net"
	"testing"
	"time"

	"github.com/lightwebinc/bitcoin-shard-common/frame"
	"github.com/lightwebinc/bitcoin-shard-common/shard"

	"github.com/lightwebinc/bitcoin-shard-listener/egress"
	"github.com/lightwebinc/bitcoin-shard-listener/filter"
)

// newSink starts a UDP listener on loopback and returns its addr + a channel of received payloads.
func newSink(t *testing.T) (string, <-chan []byte, func()) {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
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

// loopbackIface returns the loopback interface (lo) suitable for non-multicast tests.
func loopbackIface(t *testing.T) *net.Interface {
	t.Helper()
	ifs, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range ifs {
		if i.Flags&net.FlagLoopback != 0 {
			ii := i
			return &ii
		}
	}
	t.Skip("no loopback interface")
	return nil
}

func buildBRC124Frame(t *testing.T, txid [32]byte, payload []byte) []byte {
	t.Helper()
	f := &frame.Frame{
		Version: frame.FrameVerV2,
		TxID:    txid,
		Payload: payload,
	}
	buf := make([]byte, frame.HeaderSize+len(payload))
	n, err := frame.Encode(f, buf)
	if err != nil {
		t.Fatal(err)
	}
	return buf[:n]
}

func buildBRC12Frame(t *testing.T, txid [32]byte, payload []byte) []byte {
	t.Helper()
	// Manually build a 44-byte BRC-12 (legacy) header.
	buf := make([]byte, frame.HeaderSizeLegacy+len(payload))
	buf[0], buf[1], buf[2], buf[3] = 0xE3, 0xE1, 0xF3, 0xE8 // magic
	buf[4], buf[5] = 0x02, 0xBF                             // proto version
	buf[6] = frame.FrameVerV1
	buf[7] = 0
	copy(buf[8:40], txid[:])
	// payLen big-endian @40
	l := uint32(len(payload))
	buf[40] = byte(l >> 24)
	buf[41] = byte(l >> 16)
	buf[42] = byte(l >> 8)
	buf[43] = byte(l)
	copy(buf[44:], payload)
	return buf
}

func newWorker(t *testing.T, addr string, filt *filter.Filter) *Worker {
	t.Helper()
	eng := shard.New(0xFF05, [11]byte{}, 2)
	egr, err := egress.New(addr, "udp", false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = egr.Close() })
	iface := loopbackIface(t)
	return New(0, iface, 9999, nil, eng, filt, egr, nil, nil, nil, false)
}

func TestProcessFrame_ForwardsBRC124(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	filt := filter.New(nil, nil, nil, nil)
	w := newWorker(t, addr, filt)

	raw := buildBRC124Frame(t, [32]byte{1, 2, 3}, []byte("payload"))
	w.processFrame(raw)

	select {
	case got := <-ch:
		if len(got) != len(raw) {
			t.Fatalf("got %d bytes want %d", len(got), len(raw))
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for forwarded frame")
	}
}

func TestProcessFrame_ForwardsBRC12(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	filt := filter.New(nil, nil, nil, nil)
	w := newWorker(t, addr, filt)

	raw := buildBRC12Frame(t, [32]byte{4, 5, 6}, []byte("legacy"))
	w.processFrame(raw)

	select {
	case got := <-ch:
		if len(got) != len(raw) {
			t.Fatalf("got %d bytes want %d", len(got), len(raw))
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestProcessFrame_DecodeError(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	filt := filter.New(nil, nil, nil, nil)
	w := newWorker(t, addr, filt)

	w.processFrame([]byte{0x00, 0x01, 0x02}) // too short

	select {
	case <-ch:
		t.Fatal("should not forward invalid frame")
	case <-time.After(150 * time.Millisecond):
		// expected
	}
}

func TestProcessFrame_FilterDrops(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	// shard-include only allows group 0, but txid 0xFFFFFFFF... → high group.
	filt := filter.New([]uint32{0}, nil, nil, nil)
	w := newWorker(t, addr, filt)

	var txid [32]byte
	// Set top bits so groupIdx != 0 with shardBits=2 (groupIdx = top 2 bits).
	txid[0] = 0xC0 // top 2 bits = 11 → groupIdx = 3
	raw := buildBRC124Frame(t, txid, []byte("dropme"))
	w.processFrame(raw)

	select {
	case <-ch:
		t.Fatal("filtered frame should not be forwarded")
	case <-time.After(150 * time.Millisecond):
		// expected
	}
}

func TestProcessFrame_StripHeader(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	eng := shard.New(0xFF05, [11]byte{}, 2)
	egr, err := egress.New(addr, "udp", true) // strip-header
	if err != nil {
		t.Fatal(err)
	}
	defer egr.Close()
	iface := loopbackIface(t)
	filt := filter.New(nil, nil, nil, nil)
	w := New(0, iface, 9999, nil, eng, filt, egr, nil, nil, nil, false)

	payload := []byte("just-payload")
	raw := buildBRC124Frame(t, [32]byte{}, payload)
	w.processFrame(raw)

	select {
	case got := <-ch:
		if string(got) != string(payload) {
			t.Fatalf("strip mode: got %q want %q", got, payload)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestNew_Construction(t *testing.T) {
	eng := shard.New(0xFF05, [11]byte{}, 2)
	filt := filter.New(nil, nil, nil, nil)
	iface := loopbackIface(t)
	w := New(7, iface, 9001, nil, eng, filt, nil, nil, nil, nil, true)
	if w == nil {
		t.Fatal("nil worker")
	}
	if w.id != 7 || w.port != 9001 || !w.debug {
		t.Errorf("fields not preserved: %+v", w)
	}
}
