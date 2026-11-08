package egress

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
)

// laneVectors is a subset of the TPNIL multicast frame corpus
// (testdata/lane-vectors.json): frames with the bytes their lane must carry.
type laneVectors struct {
	Vectors []struct {
		ID       string         `json:"id"`
		Input    map[string]any `json:"input"`
		Expected map[string]any `json:"expected"`
	} `json:"vectors"`
}

// loadLaneVectors reads the corpus subset into id -> field -> hex string.
func loadLaneVectors(t *testing.T) map[string]map[string]string {
	t.Helper()
	raw, err := os.ReadFile("testdata/lane-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v laneVectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]string{}
	for _, e := range v.Vectors {
		m := map[string]string{}
		for k, x := range e.Input {
			if s, ok := x.(string); ok {
				m["input."+k] = s
			}
		}
		for k, x := range e.Expected {
			if s, ok := x.(string); ok {
				m[k] = s
			}
		}
		out[e.ID] = m
	}
	return out
}

// unhex decodes s or fails the test.
func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// tcpBridge is a stand-in bridge: it accepts connections and collects every
// byte written to it.
type tcpBridge struct {
	ln    net.Listener
	mu    sync.Mutex
	buf   bytes.Buffer
	conns int
	open  []net.Conn
	wg    sync.WaitGroup
}

// newTCPBridge listens on addr and collects every byte written to it.
func newTCPBridge(t *testing.T, addr string) *tcpBridge {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	b := &tcpBridge{ln: ln}
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			b.mu.Lock()
			b.conns++
			b.open = append(b.open, c)
			b.mu.Unlock()
			b.wg.Add(1)
			go func() {
				defer b.wg.Done()
				defer func() { _ = c.Close() }()
				chunk := make([]byte, 4096)
				for {
					n, err := c.Read(chunk)
					b.mu.Lock()
					b.buf.Write(chunk[:n])
					b.mu.Unlock()
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(b.stop)
	return b
}

// addr is the bridge's listening address.
func (b *tcpBridge) addr() string { return b.ln.Addr().String() }

// bytes returns a copy of everything received so far.
func (b *tcpBridge) bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf.Bytes()...)
}

// connCount is the number of connections accepted.
func (b *tcpBridge) connCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.conns
}

// stop closes the listener and every accepted connection, as a bridge
// process exiting would.
func (b *tcpBridge) stop() {
	_ = b.ln.Close()
	b.mu.Lock()
	for _, c := range b.open {
		_ = c.Close()
	}
	b.mu.Unlock()
}

// waitFor polls until got() equals want or the deadline passes.
func waitFor(t *testing.T, what string, want []byte, got func() []byte) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if bytes.Equal(got(), want) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s lane carried\n got  %x\n want %x", what, got(), want)
}

// laneRec records the lane counters for assertions.
type laneRec struct {
	mu         sync.Mutex
	objects    map[string]int
	bytes      map[string]int
	drops      map[string]int // lane/reason
	reconnects map[string]int
}

// newLaneRec returns an empty recorder.
func newLaneRec() *laneRec {
	return &laneRec{objects: map[string]int{}, bytes: map[string]int{}, drops: map[string]int{}, reconnects: map[string]int{}}
}

// LaneObject implements LaneRecorder.
func (r *laneRec) LaneObject(l string, n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.objects[l]++
	r.bytes[l] += n
}

// LaneDrop implements LaneRecorder.
func (r *laneRec) LaneDrop(l, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.drops[l+"/"+reason]++
}

// LaneReconnect implements LaneRecorder.
func (r *laneRec) LaneReconnect(l string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reconnects[l]++
}

// get reads one counter.
func (r *laneRec) get(m map[string]int, k string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return m[k]
}

// coinbaseTx is a minimal well-formed raw transaction: one input spending the
// null outpoint, one output.
func coinbaseTx() []byte {
	var b bytes.Buffer
	b.Write([]byte{0x01, 0x00, 0x00, 0x00})                         // version
	b.WriteByte(0x01)                                               // input count
	b.Write(bytes.Repeat([]byte{0x00}, 32))                         // prev txid
	b.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF})                         // prev index
	b.Write([]byte{0x02, 0x51, 0x51})                               // script
	b.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF})                         // sequence
	b.WriteByte(0x01)                                               // output count
	b.Write([]byte{0x00, 0xF2, 0x05, 0x2A, 0x01, 0x00, 0x00, 0x00}) // value
	b.Write([]byte{0x01, 0x51})                                     // script
	b.Write([]byte{0x00, 0x00, 0x00, 0x00})                         // locktime
	return b.Bytes()
}

// laneBlock builds a BRC-144 block object with two subtree roots and the given
// inline coinbase.
func laneBlock(t *testing.T, coinbase []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	b.Write(bytes.Repeat([]byte{0x22}, frame.BlockHeaderSize))
	var u [8]byte
	for _, v := range []uint64{1, 4096, 2} { // TransactionCount, SizeInBytes, SubtreeCount
		binary.BigEndian.PutUint64(u[:], v)
		b.Write(u[:])
	}
	b.Write(bytes.Repeat([]byte{0x31}, 32))
	b.Write(bytes.Repeat([]byte{0x32}, 32))
	b.Write(coinbase)
	binary.BigEndian.PutUint64(u[:], 800001) // Height
	b.Write(u[:])
	binary.BigEndian.PutUint64(u[:], 4) // CoinbaseBUMPLen
	b.Write(u[:])
	b.Write([]byte{0x74, 0x74, 0x74, 0x74})
	return b.Bytes()
}

// TestLanes_RouteEachClassToItsOwnLane drives one object of every class
// through a router with all five lanes and checks each bridge receives exactly
// its own class, in its lane form, and nothing else.
func TestLanes_RouteEachClassToItsOwnLane(t *testing.T) {
	vec := loadLaneVectors(t)
	bridges := map[string]*tcpBridge{}
	for _, n := range []string{"tx", "subtree", "block", "header", "beef"} {
		bridges[n] = newTCPBridge(t, "127.0.0.1:0")
	}
	rec := newLaneRec()
	l, err := NewLanes(LaneAddrs{
		Tx: bridges["tx"].addr(), Subtree: bridges["subtree"].addr(), Block: bridges["block"].addr(),
		Header: bridges["header"].addr(), BEEF: bridges["beef"].addr(),
	}, rec)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()

	// Transaction: BRC-124 example and BRC-128 EF payload.
	var wantTx []byte
	for _, id := range []string{"brc124.frame.example", "brc128.ef-payload"} {
		raw := unhex(t, vec[id]["frameHex"])
		f, err := frame.Decode(raw)
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		if err := l.Send(raw, f); err != nil {
			t.Fatalf("Send %s: %v", id, err)
		}
		wantTx = append(wantTx, unhex(t, vec[id]["input.payloadHex"])...)
	}

	// Subtree: a BRC-143 object framed the way the proxy frames it.
	wantSub := subtreeObject(t, 3)
	rawSub, err := objfmt.MulticastBytes(objfmt.ClassSubtree, wantSub)
	if err != nil {
		t.Fatal(err)
	}
	sf, err := frame.DecodeSubtreeData(rawSub)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.SendSubtreeData(rawSub, sf); err != nil {
		t.Fatalf("SendSubtreeData: %v", err)
	}

	// Block: a BRC-144 object carried verbatim in a BRC-131 frame.
	wantBlock := laneBlock(t, coinbaseTx())
	rawBlock, err := objfmt.MulticastBytes(objfmt.ClassBlock, wantBlock)
	if err != nil {
		t.Fatal(err)
	}
	bf, err := frame.DecodeBlock(rawBlock)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.SendBlock(rawBlock, bf); err != nil {
		t.Fatalf("SendBlock: %v", err)
	}

	// Header: the BRC-135 corpus frame; the lane carries its bare 80 bytes.
	rawHdr := unhex(t, vec["brc135.header-frame"]["frameHex"])
	hf, err := frame.DecodeBlockHeader(rawHdr)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := l.SendHeaderN(rawHdr, hf); err != nil || n != 1 {
		t.Fatalf("SendHeaderN = %d, %v; want 1, nil", n, err)
	}
	wantHdr := unhex(t, vec["brc135.header-frame"]["input.headerHex"])

	// BEEF: the BRC-148 corpus frames; the lane carries BRC-149 delivery records.
	var wantBeef []byte
	for _, id := range []string{"brc149.record-frame-delivery.one-topic", "brc149.record-frame-delivery.with-label"} {
		raw := unhex(t, vec[id]["frameHex"])
		bfr, err := frame.DecodeBEEF(raw)
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		if err := l.SendBeef(raw, bfr); err != nil {
			t.Fatalf("SendBeef %s: %v", id, err)
		}
		wantBeef = append(wantBeef, unhex(t, vec[id]["deliveryHex"])...)
	}

	waitFor(t, "tx", wantTx, bridges["tx"].bytes)
	waitFor(t, "subtree", wantSub, bridges["subtree"].bytes)
	waitFor(t, "block", wantBlock, bridges["block"].bytes)
	waitFor(t, "header", wantHdr, bridges["header"].bytes)
	waitFor(t, "beef", wantBeef, bridges["beef"].bytes)

	for name, want := range map[string]int{"tx": 2, "subtree": 1, "block": 1, "header": 1, "beef": 2} {
		if got := rec.get(rec.objects, name); got != want {
			t.Errorf("%s objects = %d, want %d", name, got, want)
		}
		if got := bridges[name].connCount(); got != 1 {
			t.Errorf("%s connections = %d, want exactly 1", name, got)
		}
	}
}

// TestLanes_AnchorsAndRawReachNoLane checks the two inputs no lane carries:
// a BRC-134 anchor frame and a class-less raw buffer.
func TestLanes_AnchorsAndRawReachNoLane(t *testing.T) {
	vec := loadLaneVectors(t)
	tx := newTCPBridge(t, "127.0.0.1:0")
	rec := newLaneRec()
	l, err := NewLanes(LaneAddrs{Tx: tx.addr()}, rec)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()

	raw := unhex(t, vec["brc124.frame.example"]["frameHex"])
	f, err := frame.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	anchor := *f
	anchor.Version = frame.FrameVerV6
	if err := l.Send(raw, &anchor); !IsNotElected(err) {
		t.Fatalf("anchor Send = %v, want ErrNotElected", err)
	}
	if err := l.SendRaw(raw); !IsNotElected(err) {
		t.Fatalf("SendRaw = %v, want ErrNotElected", err)
	}
	if got := rec.get(rec.drops, "anchor/no_lane"); got != 1 {
		t.Fatalf("anchor no_lane drops = %d, want 1", got)
	}
	time.Sleep(50 * time.Millisecond)
	if got := tx.bytes(); len(got) != 0 {
		t.Fatalf("tx lane carried %d bytes of anchor/raw input", len(got))
	}
}

// TestLanes_UnsetLaneCountsDrops checks a class with no lane is withheld as an
// election outcome, counted, and never sent anywhere.
func TestLanes_UnsetLaneCountsDrops(t *testing.T) {
	vec := loadLaneVectors(t)
	hdrBridge := newTCPBridge(t, "127.0.0.1:0")
	rec := newLaneRec()
	l, err := NewLanes(LaneAddrs{Header: hdrBridge.addr()}, rec)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()

	raw := unhex(t, vec["brc124.frame.example"]["frameHex"])
	f, err := frame.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := l.Send(raw, f); !IsNotElected(err) {
			t.Fatalf("Send with no tx lane = %v, want ErrNotElected", err)
		}
	}
	rawB := unhex(t, vec["brc149.record-frame-delivery.one-topic"]["frameHex"])
	bfr, err := frame.DecodeBEEF(rawB)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.SendBeef(rawB, bfr); !IsNotElected(err) {
		t.Fatalf("SendBeef with no beef lane = %v, want ErrNotElected", err)
	}
	if got := rec.get(rec.drops, "tx/no_lane"); got != 3 {
		t.Errorf("tx no_lane drops = %d, want 3", got)
	}
	if got := rec.get(rec.drops, "beef/no_lane"); got != 1 {
		t.Errorf("beef no_lane drops = %d, want 1", got)
	}
	if !l.HasHeader() {
		t.Error("HasHeader = false with a header lane set")
	}
	if got := hdrBridge.bytes(); len(got) != 0 {
		t.Errorf("header lane carried %d bytes of other classes", len(got))
	}
}

// TestLanes_HeaderRejectsWrongSize checks a header view whose payload is not
// 80 bytes never reaches the lane.
func TestLanes_HeaderRejectsWrongSize(t *testing.T) {
	hdrBridge := newTCPBridge(t, "127.0.0.1:0")
	rec := newLaneRec()
	l, err := NewLanes(LaneAddrs{Header: hdrBridge.addr()}, rec)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	if err := l.SendHeader(nil, &frame.Frame{Payload: make([]byte, 79)}); err == nil || IsNotElected(err) {
		t.Fatalf("SendHeader(79 bytes) = %v, want a size error", err)
	}
	if got := rec.get(rec.drops, "header/bad_size"); got != 1 {
		t.Fatalf("header bad_size drops = %d, want 1", got)
	}
}

// TestLanes_RedialAfterBridgeRestart checks a lane reconnects to a restarted
// bridge on the same address and counts the reconnect. Objects written into
// the dead connection are lost: the lane is at most once.
func TestLanes_RedialAfterBridgeRestart(t *testing.T) {
	first := newTCPBridge(t, "127.0.0.1:0")
	addr := first.addr()
	rec := newLaneRec()
	l, err := NewLanes(LaneAddrs{Header: addr}, rec)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()

	hdr := &frame.Frame{Payload: bytes.Repeat([]byte{0x01}, frame.BlockHeaderSize)}
	if err := l.SendHeader(nil, hdr); err != nil {
		t.Fatalf("first SendHeader: %v", err)
	}
	waitFor(t, "header", hdr.Payload, first.bytes)

	// Restart the bridge: close the listener and its connection, then listen
	// again on the same address.
	first.stop()
	first.wg.Wait()
	second := newTCPBridge(t, addr)

	next := &frame.Frame{Payload: bytes.Repeat([]byte{0x02}, frame.BlockHeaderSize)}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && rec.get(rec.reconnects, "header") == 0 {
		_ = l.SendHeader(nil, next)
		time.Sleep(10 * time.Millisecond)
	}
	if got := rec.get(rec.reconnects, "header"); got < 1 {
		t.Fatalf("header reconnects = %d, want >= 1", got)
	}
	if err := l.SendHeader(nil, next); err != nil {
		t.Fatalf("SendHeader after redial: %v", err)
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := second.bytes(); len(got) >= frame.BlockHeaderSize {
			if !bytes.Equal(got[:frame.BlockHeaderSize], next.Payload) {
				t.Fatalf("restarted bridge got %x", got[:frame.BlockHeaderSize])
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("restarted bridge received nothing")
}

// TestLanes_CloseIsIdempotent checks the shared router can be closed by every
// worker's deferred Close.
func TestLanes_CloseIsIdempotent(t *testing.T) {
	b := newTCPBridge(t, "127.0.0.1:0")
	l, err := NewLanes(LaneAddrs{Tx: b.addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := l.Close(); err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("Close #%d: %v", i, err)
		}
	}
}

// TestNewLanes_RejectsBadAddress checks a malformed lane address fails at
// construction, before any traffic.
func TestNewLanes_RejectsBadAddress(t *testing.T) {
	if _, err := NewLanes(LaneAddrs{Block: "no-port"}, nil); err == nil {
		t.Fatal("NewLanes accepted a lane address with no port")
	}
}

// TestLanes_DownBridgeBacksOff checks a lane whose bridge refuses does not dial
// on every object: inside the redial backoff objects drop as bridge_down.
func TestLanes_DownBridgeBacksOff(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // nothing listens: every dial is refused

	rec := newLaneRec()
	l, err := NewLanes(LaneAddrs{Header: addr}, rec)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()

	hdr := &frame.Frame{Payload: make([]byte, frame.BlockHeaderSize)}
	for i := 0; i < 20; i++ {
		if err := l.SendHeader(nil, hdr); err == nil {
			t.Fatalf("send %d to a refused bridge succeeded", i)
		}
	}
	if got := rec.get(rec.drops, "header/send_error"); got != 1 {
		t.Errorf("send_error drops = %d, want exactly 1 (one dial, then backoff)", got)
	}
	if got := rec.get(rec.drops, "header/bridge_down"); got != 19 {
		t.Errorf("bridge_down drops = %d, want 19", got)
	}

	// After the backoff a bridge that came up is dialed again.
	b := newTCPBridge(t, addr)
	time.Sleep(laneRedialMin + 50*time.Millisecond)
	if err := l.SendHeader(nil, hdr); err != nil {
		t.Fatalf("send after backoff with the bridge up: %v", err)
	}
	waitFor(t, "header", hdr.Payload, b.bytes)
}
