package txdedup_test

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/lightwebinc/bitcoin-shard-listener/txdedup"
)

func newStore(t *testing.T, mr *miniredis.Miniredis) *txdedup.Store {
	t.Helper()
	s, err := txdedup.New(mr.Addr(), "test:txid:", time.Second)
	if err != nil {
		t.Fatalf("txdedup.New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func txid(b byte) [32]byte {
	var id [32]byte
	id[0] = b
	return id
}

func TestClaim_FirstCallerWins(t *testing.T) {
	mr := miniredis.RunT(t)
	s := newStore(t, mr)

	claimed, err := s.Claim(txid(0x01))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !claimed {
		t.Fatal("expected first Claim to return true")
	}
}

func TestClaim_SecondCallerSuppressed(t *testing.T) {
	mr := miniredis.RunT(t)
	s1 := newStore(t, mr)
	s2 := newStore(t, mr)

	id := txid(0x02)

	if ok, err := s1.Claim(id); err != nil || !ok {
		t.Fatalf("s1.Claim: got (%v, %v), want (true, nil)", ok, err)
	}
	if ok, err := s2.Claim(id); err != nil || ok {
		t.Fatalf("s2.Claim: got (%v, %v), want (false, nil)", ok, err)
	}
}

func TestClaim_DistinctTxIDsNotSuppressed(t *testing.T) {
	mr := miniredis.RunT(t)
	s := newStore(t, mr)

	for i := range 10 {
		id := txid(byte(i))
		ok, err := s.Claim(id)
		if err != nil {
			t.Fatalf("Claim(%d): unexpected error: %v", i, err)
		}
		if !ok {
			t.Fatalf("Claim(%d): expected true for distinct TxID", i)
		}
	}
}

func TestClaim_TTLExpiry(t *testing.T) {
	mr := miniredis.RunT(t)

	s, err := txdedup.New(mr.Addr(), "test:txid:", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("txdedup.New: %v", err)
	}
	defer func() { _ = s.Close() }()

	id := txid(0x03)

	if ok, err := s.Claim(id); err != nil || !ok {
		t.Fatalf("first Claim: got (%v, %v)", ok, err)
	}
	if ok, err := s.Claim(id); err != nil || ok {
		t.Fatalf("second Claim (within TTL): got (%v, %v), want (false, nil)", ok, err)
	}

	// Advance both wall-clock (for the local-LRU tier) and miniredis clock
	// (for the Redis tier) past TTL. The Store layers a TTL'd local set in
	// front of Redis, so both must time out for the next Claim to win.
	time.Sleep(150 * time.Millisecond)
	mr.FastForward(200 * time.Millisecond)

	// After TTL, the key has expired — new claim should succeed.
	if ok, err := s.Claim(id); err != nil || !ok {
		t.Fatalf("Claim after TTL: got (%v, %v), want (true, nil)", ok, err)
	}
}

func TestNew_RejectsNonPositiveTTL(t *testing.T) {
	mr := miniredis.RunT(t)
	for _, ttl := range []time.Duration{0, -1 * time.Second} {
		if _, err := txdedup.New(mr.Addr(), "test:txid:", ttl); err == nil {
			t.Errorf("ttl=%s: expected error, got nil", ttl)
		}
	}
}

func TestClaim_RedisDown_FailOpen(t *testing.T) {
	mr := miniredis.RunT(t)
	s := newStore(t, mr)

	mr.Close()

	ok, err := s.Claim(txid(0x04))
	if err == nil {
		t.Fatal("expected error when Redis is down")
	}
	if !ok {
		t.Fatal("fail-open: Claim should return true even on error")
	}
}
