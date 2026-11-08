package egress

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
)

// LaneAddrs names the reference lane's per-class destinations, each a
// host:port reached over TCP. An empty field leaves that class with no lane.
type LaneAddrs struct {
	Tx      string // transaction payloads (BRC-12 raw or BRC-30 EF)
	Subtree string // BRC-143 subtree objects
	Block   string // BRC-144 block objects
	Header  string // bare 80-byte block headers (BRC-135 payload)
	BEEF    string // BRC-149 delivery records
}

// Any reports whether at least one lane is configured.
func (a LaneAddrs) Any() bool {
	return a.Tx != "" || a.Subtree != "" || a.Block != "" || a.Header != "" || a.BEEF != ""
}

// LaneRecorder receives the reference lane's operational counters. They are
// per lane, never per consumer: a reference lane serves one bridge per class.
type LaneRecorder interface {
	LaneObject(lane string, bytes int)
	LaneDrop(lane, reason string)
	LaneReconnect(lane string)
}

// laneRedialMin/Max bound how often a down lane dials its bridge again. A
// bridge that blackholes connections costs a dial timeout per attempt, and the
// lane mutex is shared by every worker, so without a backoff each object of
// that class would stall the workers behind it.
const (
	laneRedialMin = 250 * time.Millisecond
	laneRedialMax = 5 * time.Second
)

// Lanes is the reference lane: a class-routing [EgressSink] that delivers each
// object class, header-stripped, to its own single bridge over one TCP
// connection. It is generic fan-out to ONE consumer per class, configured by
// static flags. It does no per-consumer routing, election, metering, or
// delivery limits, and it does not retransmit: what a lane carries is what
// the listener holds, at most once.
//
// One Lanes value is shared by every worker. Each lane is one [Sender]
// guarded by its own mutex, so a bridge sees exactly one connection per lane,
// with whole objects written in turn. The Sender's lazy dial and redial on
// write error apply unchanged.
//
// Routing:
//
//   - BRC-124/128 transaction frames → tx lane, as the frame payload.
//     BRC-134 anchor frames go to no lane.
//   - BRC-132 subtree data → subtree lane, as BRC-143 ([objfmt.StripBytes]).
//   - BRC-131 block frames → block lane, as BRC-144 ([objfmt.StripBytes]); a
//     frame carrying no whole block is dropped as not strippable.
//   - BRC-148 BEEF frames → beef lane, as the BRC-149 delivery record.
//   - BRC-135 headers ([HeaderSink]) → header lane, as the bare 80 bytes.
//
// A class with no lane returns a wrapped [ErrNotElected] and is counted as a
// "no_lane" drop. While a lane's bridge is down, objects of that class are
// dropped as "bridge_down" without a dial until the lane's redial backoff
// (250 ms doubling to 5 s) has passed.
type Lanes struct {
	tx, subtree, block, header, beef *lane
	rec                              LaneRecorder
	closeOnce                        sync.Once
	closeErr                         error
}

var (
	_ EgressSink       = (*Lanes)(nil)
	_ HeaderFanoutSink = (*Lanes)(nil)
)

// lane is one class's connection to its bridge. mu serializes writers and
// guards every field.
type lane struct {
	name    string
	mu      sync.Mutex
	s       *Sender
	dialed  bool          // a connection has been made before, so the next one is a reconnect
	backoff time.Duration // current redial backoff; 0 while the lane is up
	retryAt time.Time     // earliest next dial while the lane is down
}

// NewLanes builds the reference lane. rec may be nil.
func NewLanes(addrs LaneAddrs, rec LaneRecorder) (*Lanes, error) {
	l := &Lanes{rec: rec}
	for _, c := range []struct {
		addr string
		name string
		dst  **lane
	}{
		{addrs.Tx, "tx", &l.tx},
		{addrs.Subtree, "subtree", &l.subtree},
		{addrs.Block, "block", &l.block},
		{addrs.Header, "header", &l.header},
		{addrs.BEEF, "beef", &l.beef},
	} {
		if c.addr == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(c.addr); err != nil {
			return nil, fmt.Errorf("lane %s: address %q: %w", c.name, c.addr, err)
		}
		s, err := New(c.addr, "tcp", true)
		if err != nil {
			return nil, fmt.Errorf("lane %s: %w", c.name, err)
		}
		*c.dst = &lane{name: c.name, s: s}
	}
	return l, nil
}

// HasHeader reports whether a header lane is configured.
func (l *Lanes) HasHeader() bool { return l.header != nil }

// drop counts one undelivered object.
func (l *Lanes) drop(name, reason string) {
	if l.rec != nil {
		l.rec.LaneDrop(name, reason)
	}
}

// notElected counts a class with no lane and returns the election outcome.
func (l *Lanes) notElected(name string) error {
	l.drop(name, "no_lane")
	return fmt.Errorf("reference lane: no %s lane: %w", name, ErrNotElected)
}

// write sends one whole object on ln and books it. A connection the bridge has
// closed (a restart, a graceful stop) is redialed before the write rather than
// written into. A down lane inside its redial backoff drops the object without
// dialing.
func (l *Lanes) write(ln *lane, buf []byte) error {
	ln.mu.Lock()
	if ln.s.tcpConn != nil && peerClosed(ln.s.tcpConn) {
		ln.s.closeTCP()
	}
	wasDown := ln.s.tcpConn == nil
	if now := time.Now(); wasDown && ln.backoff > 0 && now.Before(ln.retryAt) {
		wait := ln.retryAt.Sub(now)
		ln.mu.Unlock()
		l.drop(ln.name, "bridge_down")
		return fmt.Errorf("reference lane: %s bridge down, next dial in %s", ln.name, wait.Round(time.Millisecond))
	}
	err := ln.s.sendTCP(buf)
	up := ln.s.tcpConn != nil
	reconnect := wasDown && up && ln.dialed
	if up {
		ln.dialed = true
		ln.backoff = 0
	} else {
		ln.backoff = min(max(2*ln.backoff, laneRedialMin), laneRedialMax)
		ln.retryAt = time.Now().Add(ln.backoff)
	}
	ln.mu.Unlock()

	if l.rec == nil {
		return err
	}
	if reconnect {
		l.rec.LaneReconnect(ln.name)
	}
	if err != nil {
		l.rec.LaneDrop(ln.name, "send_error")
		return err
	}
	l.rec.LaneObject(ln.name, len(buf))
	return nil
}

// Send routes a transaction frame to the tx lane. BRC-134 anchors are not
// delivered on any lane.
func (l *Lanes) Send(_ []byte, f *frame.Frame) error {
	if f.Version == frame.FrameVerV6 {
		return l.notElected("anchor")
	}
	if l.tx == nil {
		return l.notElected("tx")
	}
	return l.write(l.tx, f.Payload)
}

// SendBlock routes a block frame to the block lane as a BRC-144 object.
func (l *Lanes) SendBlock(raw []byte, _ *frame.BlockFrame) error {
	if l.block == nil {
		return l.notElected("block")
	}
	obj, err := objfmt.StripBytes(objfmt.ClassBlock, raw)
	if err != nil {
		l.drop("block", "not_strippable")
		return fmt.Errorf("reference lane: strip block: %w", err)
	}
	return l.write(l.block, obj)
}

// SendSubtreeData routes a subtree data frame to the subtree lane as a BRC-143
// object.
func (l *Lanes) SendSubtreeData(raw []byte, _ *frame.SubtreeDataFrame) error {
	if l.subtree == nil {
		return l.notElected("subtree")
	}
	obj, err := objfmt.StripBytes(objfmt.ClassSubtree, raw)
	if err != nil {
		l.drop("subtree", "not_strippable")
		return fmt.Errorf("reference lane: strip subtree: %w", err)
	}
	return l.write(l.subtree, obj)
}

// SendBeef routes a BEEF object frame to the beef lane as a BRC-149 delivery
// record.
func (l *Lanes) SendBeef(_ []byte, bf *frame.BEEFFrame) error {
	if l.beef == nil {
		return l.notElected("beef")
	}
	return l.write(l.beef, objfmt.EncodeBEEFDelivery(bf.TopicID, bf.Payload))
}

// SendHeader routes a BRC-135 block header to the header lane as the bare
// 80-byte header, the fixed-size form a header subscriber reads.
func (l *Lanes) SendHeader(_ []byte, hf *frame.Frame) error {
	if l.header == nil {
		return l.notElected("header")
	}
	if len(hf.Payload) != frame.BlockHeaderSize {
		l.drop("header", "bad_size")
		return fmt.Errorf("reference lane: header payload is %d bytes, want %d", len(hf.Payload), frame.BlockHeaderSize)
	}
	return l.write(l.header, hf.Payload)
}

// SendHeaderN is [SendHeader] with the delivered count the worker books: 1 when
// the header crossed the lane, 0 otherwise.
func (l *Lanes) SendHeaderN(raw []byte, hf *frame.Frame) (int, error) {
	err := l.SendHeader(raw, hf)
	switch {
	case err == nil:
		return 1, nil
	case IsNotElected(err):
		return 0, nil
	default:
		return 0, err
	}
}

// SendRaw is not a lane: the class of an arbitrary buffer is unknown, so it is
// never guessed. Headers reach the header lane through [SendHeader].
func (l *Lanes) SendRaw([]byte) error { return l.notElected("raw") }

// Proto reports "tcp": every lane is TCP.
func (l *Lanes) Proto() string { return "tcp" }

// Close closes every lane once; later calls return the first result.
func (l *Lanes) Close() error {
	l.closeOnce.Do(func() {
		for _, ln := range []*lane{l.tx, l.subtree, l.block, l.header, l.beef} {
			if ln == nil {
				continue
			}
			ln.mu.Lock()
			if err := ln.s.Close(); err != nil && l.closeErr == nil {
				l.closeErr = err
			}
			ln.mu.Unlock()
		}
	})
	return l.closeErr
}
