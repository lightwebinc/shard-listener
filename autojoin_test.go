package main

import (
	"net"
	"net/netip"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/shard-listener/config"
	"github.com/lightwebinc/shard-listener/egress"
	"github.com/lightwebinc/shard-listener/filter"
	"github.com/lightwebinc/shard-listener/listener"
)

// fakeJoiner records the runtime joins and leaves a worker is asked for, and
// fails a group's next joins (ENOBUFS, source-filter exhaustion) or leaves on
// request. A failed leave behaves like netjoin.Leave: the first source is gone,
// the rest are still held.
type fakeJoiner struct {
	joined     map[netip.Addr][]netip.Addr // current memberships
	left       map[netip.Addr][]netip.Addr // sources of each group's last leave
	addCalls   map[netip.Addr]int
	failJoins  map[netip.Addr]int
	failLeaves map[netip.Addr]int
}

func newFakeJoiner() *fakeJoiner {
	return &fakeJoiner{
		joined:     map[netip.Addr][]netip.Addr{},
		left:       map[netip.Addr][]netip.Addr{},
		addCalls:   map[netip.Addr]int{},
		failJoins:  map[netip.Addr]int{},
		failLeaves: map[netip.Addr]int{},
	}
}

func (f *fakeJoiner) AddGroup(g netip.Addr, s []netip.Addr) error {
	f.addCalls[g]++
	if f.failJoins[g] > 0 {
		f.failJoins[g]--
		return syscall.ENOBUFS
	}
	f.joined[g] = s
	return nil
}

func (f *fakeJoiner) RemoveGroup(g netip.Addr, s []netip.Addr) error {
	if f.failLeaves[g] > 0 {
		f.failLeaves[g]--
		if cur := f.joined[g]; len(cur) > 1 {
			f.joined[g] = cur[1:]
		}
		return syscall.ENOMEM
	}
	f.left[g] = s
	delete(f.joined, g)
	return nil
}

// testAutoJoin builds an autoJoin on a fake clock; advance moves the clock.
func testAutoJoin(engine *shard.Engine, static []uint32, sources []netip.Addr, filt *filter.Filter, workers ...*fakeJoiner) (a *autoJoin, advance func(time.Duration)) {
	a = newAutoJoin(engine, 9001, static, sources, filt)
	clock := time.Unix(1_000_000, 0)
	a.now = func() time.Time { return clock }
	setWorkers(a, workers)
	return a, func(d time.Duration) { clock = clock.Add(d) }
}

func engineGroup(t *testing.T, e *shard.Engine, idx uint32) netip.Addr {
	t.Helper()
	ga, ok := netip.AddrFromSlice(e.Addr(idx, 9001).IP.To16())
	if !ok {
		t.Fatalf("group %d: not an IPv6 address", idx)
	}
	return ga
}

// Under SSM a pilot group joined at runtime must be (S,G)-joined on the
// publisher set, like the data-plane groups joined at startup: a (*,G) join
// in the SSM range is not routed, so the group would stay silent. A leave
// names the same sources, and a startup group is never touched.
func TestAutoJoin_SSMJoinsPublishers(t *testing.T) {
	engine := shard.New(0xFF35, shard.DefaultGroupID, 2)
	publishers := addrs("2001:db8::11")
	w1, w2 := newFakeJoiner(), newFakeJoiner()
	a, _ := testAutoJoin(engine, []uint32{0}, publishers, nil, w1, w2)

	a.pilotsChanged([]uint16{0, 1, 2}, nil)
	for i, w := range []*fakeJoiner{w1, w2} {
		for _, idx := range []uint32{1, 2} {
			if got, ok := w.joined[engineGroup(t, engine, idx)]; !ok || !slices.Equal(got, publishers) {
				t.Errorf("worker %d: group %d joined=%v with sources %v, want the publishers %v", i, idx, ok, got, publishers)
			}
		}
		if n := w.addCalls[engineGroup(t, engine, 0)]; n != 0 {
			t.Errorf("worker %d: startup group 0 got %d runtime joins, want none", i, n)
		}
	}

	a.pilotsChanged(nil, []uint16{0, 1})
	for i, w := range []*fakeJoiner{w1, w2} {
		if got, ok := w.left[engineGroup(t, engine, 1)]; !ok || !slices.Equal(got, publishers) {
			t.Errorf("worker %d: group 1 left=%v with sources %v, want the publishers %v", i, ok, got, publishers)
		}
		if _, ok := w.left[engineGroup(t, engine, 0)]; ok {
			t.Errorf("worker %d: startup group 0 was left", i)
		}
		if _, ok := w.joined[engineGroup(t, engine, 2)]; !ok {
			t.Errorf("worker %d: group 2, still wanted, was left", i)
		}
	}
}

// Under ASM there is no roster, and runtime joins stay any-source.
func TestAutoJoin_ASMJoinsAnySource(t *testing.T) {
	engine := shard.New(0xFF05, shard.DefaultGroupID, 2)
	w := newFakeJoiner()
	a, _ := testAutoJoin(engine, nil, nil, nil, w)

	a.pilotsChanged([]uint16{1}, nil)
	if got, ok := w.joined[engineGroup(t, engine, 1)]; !ok || got != nil {
		t.Errorf("group 1 joined=%v with sources %v, want an any-source join", ok, got)
	}
}

// With no -shard-include the workers join every shard at startup, so a pilot
// naming one of them adds nothing, and its expiry must not leave that startup
// membership. With an explicit -shard-include, a pilot-only group is joined
// and left with its pilot.
func TestAutoJoin_StartupShardsAreNeverLeft(t *testing.T) {
	engine := shard.New(0xFF35, shard.DefaultGroupID, 2)
	publishers := addrs("2001:db8::11")

	w := newFakeJoiner()
	a, _ := testAutoJoin(engine, startupShards(&config.Config{NumGroups: 4}), publishers, nil, w)
	a.pilotsChanged([]uint16{1, 3}, nil)
	a.pilotsChanged(nil, []uint16{1, 3})
	if len(w.addCalls) != 0 || len(w.left) != 0 {
		t.Errorf("every shard is a startup group: got joins %v and leaves %v, want none", w.addCalls, w.left)
	}

	w = newFakeJoiner()
	a, _ = testAutoJoin(engine, startupShards(&config.Config{NumGroups: 4, ShardInclude: []uint32{0}}), publishers, nil, w)
	a.pilotsChanged([]uint16{0, 1}, nil)
	a.pilotsChanged(nil, []uint16{0, 1})
	if _, ok := w.left[engineGroup(t, engine, 1)]; !ok {
		t.Error("pilot-only group 1 was not left on expiry")
	}
	if _, ok := w.left[engineGroup(t, engine, 0)]; ok {
		t.Error("-shard-include group 0 was left on expiry")
	}
}

// A join refused for lack of source-filter capacity is retried with backoff,
// so freeing capacity restores the membership while the pilot still wants it.
func TestAutoJoin_RetriesFailedJoin(t *testing.T) {
	engine := shard.New(0xFF35, shard.DefaultGroupID, 2)
	w := newFakeJoiner()
	g1 := engineGroup(t, engine, 1)
	w.failJoins[g1] = 1
	a, advance := testAutoJoin(engine, []uint32{0}, addrs("2001:db8::11"), nil, w)

	a.pilotsChanged([]uint16{1}, nil)
	if _, ok := w.joined[g1]; ok || w.addCalls[g1] != 1 {
		t.Fatalf("after the refused join: joined=%v calls=%d, want not joined after 1 call", ok, w.addCalls[g1])
	}

	advance(autoJoinRetryBase / 2)
	a.tick()
	if w.addCalls[g1] != 1 {
		t.Fatalf("retried after %v, before the %v backoff", autoJoinRetryBase/2, autoJoinRetryBase)
	}

	advance(autoJoinRetryBase)
	a.tick()
	if _, ok := w.joined[g1]; !ok {
		t.Fatalf("group 1 not joined after the retry (calls=%d)", w.addCalls[g1])
	}

	advance(autoJoinRetryMax)
	a.tick()
	if w.addCalls[g1] != 2 {
		t.Errorf("join calls = %d, want 2: a settled group is not retried", w.addCalls[g1])
	}
}

// A pilot that expires while its join is still failing ends the retries.
func TestAutoJoin_ExpiryCancelsRetry(t *testing.T) {
	engine := shard.New(0xFF35, shard.DefaultGroupID, 2)
	w := newFakeJoiner()
	g1 := engineGroup(t, engine, 1)
	w.failJoins[g1] = 1000
	a, advance := testAutoJoin(engine, []uint32{0}, addrs("2001:db8::11"), nil, w)

	a.pilotsChanged([]uint16{1}, nil)
	a.pilotsChanged(nil, []uint16{1})
	for range 5 {
		advance(autoJoinRetryMax)
		a.tick()
	}
	if w.addCalls[g1] != 1 {
		t.Errorf("join calls = %d after the pilot expired, want 1", w.addCalls[g1])
	}
}

// The receive filter follows the pilot set: with -shard-include 0, traffic of
// a pilot-only shard 1 is delivered after adoption and dropped again after the
// pilot expires; shard 0 is delivered throughout, and the subtree rules keep
// applying to the pilot shard.
func TestAutoJoin_FilterFollowsPilots(t *testing.T) {
	sink, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sink.Close() }()
	egr, err := egress.New(sink.LocalAddr().String(), "udp", false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = egr.Close() }()
	lo, err := net.InterfaceByName("lo")
	if err != nil {
		t.Skipf("no loopback interface: %v", err)
	}

	excluded := [32]byte{0xEE}
	filt := filter.New([]uint32{0}, nil, [][32]byte{excluded}, nil)
	engine := shard.New(0xFF35, shard.DefaultGroupID, 2)
	worker := listener.New(0, lo, 0, nil, engine, filt, egr, nil, nil, nil, false)
	a, _ := testAutoJoin(engine, []uint32{0}, addrs("2001:db8::11"), filt, newFakeJoiner())

	// TxID top bits pick the shard at ShardBits 2: 0x00 is shard 0, 0x40 shard 1.
	frameFor := func(first byte, subtree [32]byte) []byte {
		f := &frame.Frame{Version: frame.FrameVerV2, TxID: [32]byte{first, 0x01}, SubtreeID: subtree, Payload: []byte("tx")}
		buf := make([]byte, frame.HeaderSize+len(f.Payload))
		n, err := frame.Encode(f, buf)
		if err != nil {
			t.Fatal(err)
		}
		return buf[:n]
	}
	delivered := func(raw []byte) bool {
		worker.Reinject(raw)
		_ = sink.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		buf := make([]byte, 2048)
		_, _, err := sink.ReadFrom(buf)
		return err == nil
	}
	shard0, shard1 := frameFor(0x00, [32]byte{}), frameFor(0x40, [32]byte{})

	if delivered(shard1) {
		t.Fatal("shard 1 delivered before any pilot claimed it")
	}
	a.pilotsChanged([]uint16{1}, nil)
	if !delivered(shard1) {
		t.Error("pilot-only shard 1 dropped after adoption")
	}
	if delivered(frameFor(0x40, excluded)) {
		t.Error("a subtree-excluded frame on pilot shard 1 was delivered")
	}
	a.pilotsChanged(nil, []uint16{1})
	if delivered(shard1) {
		t.Error("shard 1 still delivered after the pilot expired")
	}
	if !delivered(shard0) {
		t.Error("startup shard 0 dropped")
	}
}

// A leave that fails part-way must not strand the group if the pilot then
// wants it back: auto-join asserts the group on the worker again (a no-op
// when the worker still holds every source) instead of trusting its own
// record of a membership the failed leave cut short.
func TestAutoJoin_PilotReturnAfterFailedLeave(t *testing.T) {
	engine := shard.New(0xFF35, shard.DefaultGroupID, 2)
	publishers := addrs("2001:db8::11", "2001:db8::12")
	w := newFakeJoiner()
	g1 := engineGroup(t, engine, 1)
	w.failLeaves[g1] = 1
	a, _ := testAutoJoin(engine, []uint32{0}, publishers, nil, w)

	a.pilotsChanged([]uint16{1}, nil)
	a.pilotsChanged(nil, []uint16{1}) // the leave fails after the first source
	a.pilotsChanged([]uint16{1}, nil) // the pilot is back
	if got := w.joined[g1]; !slices.Equal(got, publishers) {
		t.Errorf("group 1 holds sources %v after the pilot returned, want every publisher %v", got, publishers)
	}
}
