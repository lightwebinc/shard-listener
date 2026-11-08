//go:build unix

package egress

import (
	"bytes"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
)

// waitPeerClosed polls until ln's connection reports the bridge's close, so a
// test does not race the FIN reaching the lane socket.
func waitPeerClosed(t *testing.T, ln *lane) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ln.mu.Lock()
		closed := ln.s.tcpConn == nil || peerClosed(ln.s.tcpConn)
		ln.mu.Unlock()
		if closed {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the lane never saw the bridge close its connection")
}

// TestLanes_RedialAfterBridgeRestart checks a lane reconnects to a restarted
// bridge on the same address and counts the reconnect. The bridge's close is
// seen before the next write, so the first object after the restart is
// delivered rather than written into the dead connection.
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

	waitPeerClosed(t, l.header)

	next := &frame.Frame{Payload: bytes.Repeat([]byte{0x02}, frame.BlockHeaderSize)}
	if err := l.SendHeader(nil, next); err != nil {
		t.Fatalf("first SendHeader after the restart: %v", err)
	}
	if got := rec.get(rec.reconnects, "header"); got != 1 {
		t.Fatalf("header reconnects = %d, want 1", got)
	}
	if got := rec.get(rec.drops, "header/send_error"); got != 0 {
		t.Fatalf("header send_error drops = %d, want 0 (nothing written into the closed connection)", got)
	}
	deadline := time.Now().Add(3 * time.Second)
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
