package dedup

import (
	"testing"
	"time"
)

func key(g uint32, sub byte, seq uint64) Key {
	var s [32]byte
	s[0] = sub
	return Key{GroupIdx: g, SubtreeID: s, SeqNum: seq}
}

func TestSeenAndAdd_FirstInsertIsMiss(t *testing.T) {
	s := New(8, time.Second)
	if s.SeenAndAdd(key(0, 0, 1)) {
		t.Fatal("first insert should report miss")
	}
}

func TestSeenAndAdd_SecondInsertIsHit(t *testing.T) {
	s := New(8, time.Second)
	k := key(0, 0, 1)
	_ = s.SeenAndAdd(k)
	if !s.SeenAndAdd(k) {
		t.Fatal("second insert should report hit")
	}
}

func TestSeenAndAdd_DistinctKeysAreMisses(t *testing.T) {
	s := New(8, time.Second)
	if s.SeenAndAdd(key(0, 0, 1)) {
		t.Fatal("k1 first insert hit?")
	}
	if s.SeenAndAdd(key(1, 0, 1)) {
		t.Fatal("different group should not collide")
	}
	if s.SeenAndAdd(key(0, 1, 1)) {
		t.Fatal("different subtree should not collide")
	}
	if s.SeenAndAdd(key(0, 0, 2)) {
		t.Fatal("different seqNum should not collide")
	}
}

// SeqNum counts per flow, so two senders publishing to one group and subtree
// reuse each other's sequence numbers. The HashKey (the flow identity, which
// carries the sender) must keep them apart: otherwise the second sender's
// frame is suppressed as a duplicate of the first's and never forwarded.
func TestSeenAndAdd_DistinctSendersAreMisses(t *testing.T) {
	s := New(8, time.Second)
	a, b := key(0, 0, 1), key(0, 0, 1)
	a.HashKey, b.HashKey = 0xA1, 0xB2
	if s.SeenAndAdd(a) {
		t.Fatal("first sender's frame: first insert hit?")
	}
	if s.SeenAndAdd(b) {
		t.Fatal("second sender's frame with the same (group, subtree, seq) must not collide")
	}
	if !s.SeenAndAdd(a) {
		t.Fatal("first sender's retransmit must still be a hit")
	}
}

func TestSeenAndAdd_CapacityEviction(t *testing.T) {
	s := New(2, time.Hour)
	_ = s.SeenAndAdd(key(0, 0, 1))
	_ = s.SeenAndAdd(key(0, 0, 2))
	_ = s.SeenAndAdd(key(0, 0, 3)) // evicts key 1
	if s.SeenAndAdd(key(0, 0, 1)) {
		t.Fatal("evicted key should be a miss again")
	}
	if !s.SeenAndAdd(key(0, 0, 3)) {
		t.Fatal("recently inserted key should be a hit")
	}
}

func TestSeenAndAdd_TTLExpiry(t *testing.T) {
	s := New(8, 10*time.Millisecond)
	k := key(0, 0, 1)
	if s.SeenAndAdd(k) {
		t.Fatal("first insert hit?")
	}
	time.Sleep(20 * time.Millisecond)
	if s.SeenAndAdd(k) {
		t.Fatal("expired key must be reported as miss")
	}
}

func TestSeenAndAdd_DisabledSet(t *testing.T) {
	s := New(0, time.Second)
	for i := uint64(0); i < 5; i++ {
		if s.SeenAndAdd(key(0, 0, i)) {
			t.Fatalf("disabled set must always miss (i=%d)", i)
		}
		if s.SeenAndAdd(key(0, 0, i)) {
			t.Fatalf("disabled set must always miss on dup (i=%d)", i)
		}
	}
}
