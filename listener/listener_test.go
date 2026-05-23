package listener

import (
	"crypto/sha256"
	"net"
	"testing"
	"time"

	"github.com/lightwebinc/bitcoin-shard-common/frame"
	"github.com/lightwebinc/bitcoin-shard-common/shard"
	"golang.org/x/sys/unix"

	"github.com/lightwebinc/bitcoin-shard-listener/dedup"
	"github.com/lightwebinc/bitcoin-shard-listener/egress"
	"github.com/lightwebinc/bitcoin-shard-listener/filter"
)

// sha256d returns the BSV double-SHA256 of buf.
func sha256d(buf []byte) [32]byte {
	first := sha256.Sum256(buf)
	return sha256.Sum256(first[:])
}

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

func buildSequencedFrame(t *testing.T, txid [32]byte, payload []byte, seqNum uint64) []byte {
	t.Helper()
	f := &frame.Frame{
		Version: frame.FrameVerV2,
		TxID:    txid,
		SeqNum:  seqNum,
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
	eng := shard.New(0xFF05, shard.DefaultGroupID, 2)
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
	eng := shard.New(0xFF05, shard.DefaultGroupID, 2)
	egr, err := egress.New(addr, "udp", true) // strip-header
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = egr.Close() }()
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

func TestProcessFrame_EgressDedup_SuppressDuplicate(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	filt := filter.New(nil, nil, nil, nil)
	w := newWorker(t, addr, filt)
	w.SetEgressDedup(dedup.New(64, time.Second))

	payload := []byte("dup-payload")
	txid := sha256d(payload)
	raw := buildSequencedFrame(t, txid, payload, 0xDEADBEEF_CAFEBABE)

	// First delivery: must forward.
	w.processFrame(raw)
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("first delivery should forward")
	}

	// Second delivery (retransmit): must be suppressed.
	w.processFrame(raw)
	select {
	case <-ch:
		t.Fatal("duplicate delivery must be suppressed by egress dedup")
	case <-time.After(150 * time.Millisecond):
		// expected
	}
}

func TestProcessFrame_EgressDedup_Disabled_ForwardsBoth(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	filt := filter.New(nil, nil, nil, nil)
	w := newWorker(t, addr, filt)
	// No dedup set — both copies must reach downstream.

	raw := buildBRC124Frame(t, [32]byte{0x11}, []byte("no-dedup"))
	w.processFrame(raw)
	w.processFrame(raw)

	count := 0
	deadline := time.After(300 * time.Millisecond)
	for {
		select {
		case <-ch:
			count++
		case <-deadline:
			goto done
		}
	}
done:
	if count != 2 {
		t.Errorf("without dedup: expected 2 forwards, got %d", count)
	}
}

func TestProcessFrame_VerifyDisabled_ForwardsCorrupted(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	filt := filter.New(nil, nil, nil, nil)
	w := newWorker(t, addr, filt)
	// Default: verifyPayloadHash == false. Mismatched TxID must still forward.

	raw := buildBRC124Frame(t, [32]byte{0xAA, 0xBB, 0xCC}, []byte("payload"))
	w.processFrame(raw)

	select {
	case got := <-ch:
		if len(got) != len(raw) {
			t.Fatalf("got %d bytes want %d", len(got), len(raw))
		}
	case <-time.After(time.Second):
		t.Fatal("verify-disabled: expected forward despite TxID mismatch")
	}
}

func TestProcessFrame_VerifyEnabled_RejectsCorrupted(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	filt := filter.New(nil, nil, nil, nil)
	w := newWorker(t, addr, filt)
	w.SetVerifyPayloadHash(true)

	// TxID does NOT match SHA256d(payload).
	raw := buildBRC124Frame(t, [32]byte{0xAA, 0xBB, 0xCC}, []byte("corrupt"))
	w.processFrame(raw)

	select {
	case <-ch:
		t.Fatal("verify-enabled: corrupted frame must be dropped")
	case <-time.After(150 * time.Millisecond):
		// expected: silently dropped before egress
	}
}

func TestProcessFrame_VerifyEnabled_AcceptsValid(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	filt := filter.New(nil, nil, nil, nil)
	w := newWorker(t, addr, filt)
	w.SetVerifyPayloadHash(true)

	payload := []byte("the-real-payload")
	txid := sha256d(payload)
	raw := buildBRC124Frame(t, txid, payload)
	w.processFrame(raw)

	select {
	case got := <-ch:
		if len(got) != len(raw) {
			t.Fatalf("got %d bytes want %d", len(got), len(raw))
		}
	case <-time.After(time.Second):
		t.Fatal("verify-enabled: valid frame must forward")
	}
}

func TestProcessFrame_VerifyEnabled_BRC12Bypassed(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	filt := filter.New(nil, nil, nil, nil)
	w := newWorker(t, addr, filt)
	w.SetVerifyPayloadHash(true)

	// BRC-12 frames are forwarded verbatim regardless of verify flag.
	raw := buildBRC12Frame(t, [32]byte{0x01, 0x02}, []byte("legacy"))
	w.processFrame(raw)

	select {
	case got := <-ch:
		if len(got) != len(raw) {
			t.Fatalf("got %d bytes want %d", len(got), len(raw))
		}
	case <-time.After(time.Second):
		t.Fatal("BRC-12 must be forwarded even when verify is enabled")
	}
}

// buildBlockAnnounceFrame constructs a BRC-131 BlockAnnounce frame with a valid
// 80-byte block header as the first portion of the payload. contentIDByte sets
// byte 0 of the ContentID. hashKey and seqNum are stamped into the BRC-131 header.
func buildBlockAnnounceFrame(t *testing.T, contentIDByte byte, hashKey, seqNum uint64) []byte {
	t.Helper()
	// Build a BlockAnnouncePayload: 80B header + 32B coinbase TxID + 4B subtree count (0).
	var hdr [80]byte
	hdr[0] = 0x01 // block version byte
	var coinbaseTxID [32]byte
	coinbaseTxID[0] = 0xCC
	announce := &frame.BlockAnnouncePayload{
		Header:       hdr,
		CoinbaseTxID: coinbaseTxID,
	}
	payload := frame.EncodeBlockAnnounce(announce)

	var contentID [32]byte
	contentID[0] = contentIDByte
	bf := &frame.BlockFrame{
		MsgType:   frame.BlockMsgAnnounce,
		ContentID: contentID,
		HashKey:   hashKey,
		SeqNum:    seqNum,
		Payload:   payload,
	}
	buf := make([]byte, frame.HeaderSize+len(payload))
	if _, err := frame.EncodeBlock(bf, buf); err != nil {
		t.Fatal(err)
	}
	return buf
}

// buildBlockCoinbaseFrame constructs a BRC-131 BlockMsgCoinbase frame.
func buildBlockCoinbaseFrame(t *testing.T, contentIDByte byte) []byte {
	t.Helper()
	payload := []byte("raw-coinbase-tx")
	var contentID [32]byte
	contentID[0] = contentIDByte
	bf := &frame.BlockFrame{
		MsgType:   frame.BlockMsgCoinbase,
		ContentID: contentID,
		Payload:   payload,
	}
	buf := make([]byte, frame.HeaderSize+len(payload))
	if _, err := frame.EncodeBlock(bf, buf); err != nil {
		t.Fatal(err)
	}
	return buf
}

func TestProcessBlockFrame_HeaderEgress_Unicast(t *testing.T) {
	// Main egress sink.
	mainAddr, _, mainCleanup := newSink(t)
	defer mainCleanup()

	// Header egress sink.
	hdrAddr, hdrCh, hdrCleanup := newSink(t)
	defer hdrCleanup()

	filt := filter.New(nil, nil, nil, nil)
	w := newWorker(t, mainAddr, filt)

	hdrEgr, err := egress.New(hdrAddr, "udp", false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hdrEgr.Close() })
	w.SetHeaderEgress(hdrEgr)

	raw := buildBlockAnnounceFrame(t, 0xAA, 0xDEADBEEF, 42)
	w.processBlockFrame(raw)

	// Expect a 172-byte stripped BRC-131 on the header egress.
	select {
	case got := <-hdrCh:
		expectedLen := frame.HeaderSize + frame.BlockHeaderSize // 92 + 80 = 172
		if len(got) != expectedLen {
			t.Fatalf("header egress: got %d bytes, want %d", len(got), expectedLen)
		}
		// Decode and verify fields are preserved.
		decoded, err := frame.DecodeBlock(got)
		if err != nil {
			t.Fatalf("decode stripped header: %v", err)
		}
		if decoded.MsgType != frame.BlockMsgAnnounce {
			t.Errorf("MsgType = 0x%02X, want 0x01", decoded.MsgType)
		}
		if decoded.ContentID[0] != 0xAA {
			t.Errorf("ContentID[0] = 0x%02X, want 0xAA", decoded.ContentID[0])
		}
		if decoded.HashKey != 0xDEADBEEF {
			t.Errorf("HashKey = %x, want 0xDEADBEEF", decoded.HashKey)
		}
		if decoded.SeqNum != 42 {
			t.Errorf("SeqNum = %d, want 42", decoded.SeqNum)
		}
		if len(decoded.Payload) != frame.BlockHeaderSize {
			t.Errorf("Payload len = %d, want %d", len(decoded.Payload), frame.BlockHeaderSize)
		}
		// Verify the block header content is preserved.
		if decoded.Payload[0] != 0x01 {
			t.Errorf("block header version byte = 0x%02X, want 0x01", decoded.Payload[0])
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for header egress")
	}
}

func TestProcessBlockFrame_HeaderEgress_SkipsCoinbase(t *testing.T) {
	mainAddr, _, mainCleanup := newSink(t)
	defer mainCleanup()

	hdrAddr, hdrCh, hdrCleanup := newSink(t)
	defer hdrCleanup()

	filt := filter.New(nil, nil, nil, nil)
	w := newWorker(t, mainAddr, filt)

	hdrEgr, err := egress.New(hdrAddr, "udp", false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hdrEgr.Close() })
	w.SetHeaderEgress(hdrEgr)

	// Send a coinbase frame — should NOT produce header egress.
	raw := buildBlockCoinbaseFrame(t, 0xBB)
	w.processBlockFrame(raw)

	select {
	case <-hdrCh:
		t.Fatal("coinbase frames must not produce header egress")
	case <-time.After(150 * time.Millisecond):
		// expected
	}
}

func TestProcessBlockFrame_HeaderEgress_Disabled(t *testing.T) {
	mainAddr, _, mainCleanup := newSink(t)
	defer mainCleanup()

	filt := filter.New(nil, nil, nil, nil)
	w := newWorker(t, mainAddr, filt)
	// No header egress set — should not panic or produce errors.

	raw := buildBlockAnnounceFrame(t, 0xCC, 0, 1)
	w.processBlockFrame(raw)
	// No assertion needed — just verify no panic.
}

func TestDeliverReassembledBlock_HeaderEgress(t *testing.T) {
	mainAddr, _, mainCleanup := newSink(t)
	defer mainCleanup()

	hdrAddr, hdrCh, hdrCleanup := newSink(t)
	defer hdrCleanup()

	filt := filter.New(nil, nil, nil, nil)
	w := newWorker(t, mainAddr, filt)

	hdrEgr, err := egress.New(hdrAddr, "udp", false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hdrEgr.Close() })
	w.SetHeaderEgress(hdrEgr)

	// Build a BlockAnnounce payload.
	var hdr [80]byte
	hdr[0] = 0x02
	announce := &frame.BlockAnnouncePayload{Header: hdr}
	payload := frame.EncodeBlockAnnounce(announce)

	var contentID [32]byte
	contentID[0] = 0xDD
	bf := &frame.BlockFrame{
		MsgType:   frame.BlockMsgAnnounce,
		ContentID: contentID,
		HashKey:   0xFEED,
		SeqNum:    7,
		Payload:   payload,
	}

	w.DeliverReassembledBlock(payload, bf)

	select {
	case got := <-hdrCh:
		if len(got) != frame.HeaderSize+frame.BlockHeaderSize {
			t.Fatalf("reassembled header egress: got %d bytes, want %d", len(got), frame.HeaderSize+frame.BlockHeaderSize)
		}
		decoded, err := frame.DecodeBlock(got)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.ContentID[0] != 0xDD {
			t.Errorf("ContentID[0] = 0x%02X, want 0xDD", decoded.ContentID[0])
		}
		if decoded.HashKey != 0xFEED {
			t.Errorf("HashKey = %x, want 0xFEED", decoded.HashKey)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for reassembled header egress")
	}
}

func TestNew_Construction(t *testing.T) {
	eng := shard.New(0xFF05, shard.DefaultGroupID, 2)
	filt := filter.New(nil, nil, nil, nil)
	iface := loopbackIface(t)
	w := New(7, iface, 9001, nil, eng, filt, nil, nil, nil, nil, true)
	if w == nil {
		t.Fatal("nil worker")
		return
	}
	if w.id != 7 || w.port != 9001 || !w.debug {
		t.Errorf("fields not preserved: %+v", w)
	}
}

// ── Sender ACL (data-plane) ───────────────────────────────────────────────────

func TestSockaddrIP_IPv6(t *testing.T) {
	sa := &unix.SockaddrInet6{
		Addr: [16]byte{0xfd, 0x20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x24},
	}
	got := sockaddrIP(sa)
	want := net.ParseIP("fd20::24")
	if !got.Equal(want) {
		t.Errorf("sockaddrIP = %v, want %v", got, want)
	}
}

func TestSockaddrIP_NonInet6_ReturnsNil(t *testing.T) {
	if got := sockaddrIP(&unix.SockaddrInet4{}); got != nil {
		t.Errorf("non-inet6 sockaddr should yield nil, got %v", got)
	}
}

func TestWorker_SetSenderACL_PreservedOnWorker(t *testing.T) {
	addr, _, cleanup := newSink(t)
	defer cleanup()
	filt := filter.New(nil, nil, nil, nil)
	w := newWorker(t, addr, filt)
	if w.senderACL != nil {
		t.Fatal("default worker must have nil senderACL")
	}
	_, cidr, err := net.ParseCIDR("fd20::/16")
	if err != nil {
		t.Fatal(err)
	}
	acl := filter.NewSenderACL([]*net.IPNet{cidr}, nil)
	w.SetSenderACL(acl)
	if w.senderACL == nil {
		t.Fatal("senderACL must be set after SetSenderACL")
	}
	if !w.senderACL.Allow(net.ParseIP("fd20::24")) {
		t.Error("included sender must be allowed")
	}
	if w.senderACL.Allow(net.ParseIP("fe80::1")) {
		t.Error("non-included sender must be denied")
	}
}

// ── BRC-134 anchor transaction frames (FrameVerV6) ────────────────────────────

// buildAnchorFrame constructs a BRC-134 anchor transaction frame with the given
// TxID byte, seqNum, and payload. It uses frame.Encode (FrameVerV2 layout) then
// patches the version byte to 0x06.
func buildAnchorFrame(t *testing.T, txid [32]byte, payload []byte, seqNum uint64) []byte {
	t.Helper()
	f := &frame.Frame{
		Version: frame.FrameVerV2,
		TxID:    txid,
		SeqNum:  seqNum,
		Payload: payload,
	}
	buf := make([]byte, frame.HeaderSize+len(payload))
	n, err := frame.Encode(f, buf)
	if err != nil {
		t.Fatal(err)
	}
	buf[6] = frame.FrameVerV6 // promote to anchor version
	return buf[:n]
}

func TestProcessFrame_RoutesBRC134AnchorViaDispatch(t *testing.T) {
	// processFrame must detect FrameVerV6 and route to processAnchorFrame,
	// which forwards to egress. Verify the frame arrives at the sink.
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	filt := filter.New(nil, nil, nil, nil)
	w := newWorker(t, addr, filt)

	raw := buildAnchorFrame(t, [32]byte{0xAA}, []byte("anchor-payload"), 0)
	w.processFrame(raw)

	select {
	case got := <-ch:
		if len(got) != len(raw) {
			t.Fatalf("got %d bytes, want %d", len(got), len(raw))
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for anchor frame forwarded via processFrame dispatch")
	}
}

func TestProcessAnchorFrame_ForwardsToEgress(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	filt := filter.New(nil, nil, nil, nil)
	w := newWorker(t, addr, filt)

	raw := buildAnchorFrame(t, [32]byte{0x11, 0x22}, []byte("anchor-tx"), 0)
	w.processAnchorFrame(raw)

	select {
	case got := <-ch:
		if len(got) != len(raw) {
			t.Fatalf("got %d bytes, want %d", len(got), len(raw))
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for anchor frame forwarded to egress")
	}
}

func TestProcessAnchorFrame_BypassesShardFilter(t *testing.T) {
	// A shard filter allowing only group 0 must not suppress anchor frames.
	// Anchor frames bypass all shard/subtree filtering.
	addr, ch, cleanup := newSink(t)
	defer cleanup()

	// shard-include only allows group 0; an anchor TxID with top bits 0xC0
	// would hash to group 3 with shardBits=2 — filtered for ordinary tx.
	filt := filter.New([]uint32{0}, nil, nil, nil)
	w := newWorker(t, addr, filt)

	var txid [32]byte
	txid[0] = 0xC0 // would be shard group 3; irrelevant for anchor routing
	raw := buildAnchorFrame(t, txid, []byte("anchor"), 0)
	w.processAnchorFrame(raw)

	select {
	case got := <-ch:
		if len(got) != len(raw) {
			t.Fatalf("got %d bytes, want %d", len(got), len(raw))
		}
	case <-time.After(time.Second):
		t.Fatal("anchor frame must bypass shard filter — timeout waiting for frame")
	}
}

func TestProcessAnchorFrame_DecodeError_Drops(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	filt := filter.New(nil, nil, nil, nil)
	w := newWorker(t, addr, filt)

	// Bad magic — DecodeAnchor will fail; frame must be dropped silently.
	bad := make([]byte, frame.HeaderSize)
	bad[6] = frame.FrameVerV6
	w.processAnchorFrame(bad)

	select {
	case <-ch:
		t.Fatal("invalid anchor frame must not be forwarded")
	case <-time.After(150 * time.Millisecond):
		// expected
	}
}

func TestProcessAnchorFrame_StripHeader(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	eng := shard.New(0xFF05, shard.DefaultGroupID, 2)
	egr, err := egress.New(addr, "udp", true) // strip-header mode
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = egr.Close() })
	iface := loopbackIface(t)
	filt := filter.New(nil, nil, nil, nil)
	w := New(0, iface, 9999, nil, eng, filt, egr, nil, nil, nil, false)

	payload := []byte("raw-anchor-tx-bytes")
	raw := buildAnchorFrame(t, [32]byte{0x33}, payload, 0)
	w.processAnchorFrame(raw)

	select {
	case got := <-ch:
		if string(got) != string(payload) {
			t.Fatalf("strip mode: got %q, want %q", got, payload)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for strip-header anchor frame")
	}
}
