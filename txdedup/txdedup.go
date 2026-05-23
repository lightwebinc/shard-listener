// Package txdedup provides a Redis-backed transaction-ID deduplicator for
// cross-listener egress deduplication.
//
// When multiple listeners receive the same multicast frame, only the first one
// to claim a TxID in Redis forwards it downstream. All subsequent listeners
// that see the same TxID suppress their egress.
//
// The mechanism uses Redis SET NX EX (set-if-not-exists with TTL): the first
// writer returns true (claimed = forward); any later writer returns false
// (already claimed = suppress).
//
// Fail-open: if Redis is unreachable, Claim returns (true, err) so the caller
// always forwards rather than silently drops.
package txdedup

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Store is a Redis-backed TxID claim set.
type Store struct {
	client *redis.Client
	prefix string
	ttl    time.Duration
}

// New constructs a Store, connects to addr, and verifies connectivity with a
// Ping. prefix is prepended to every Redis key (e.g. "bsl:txid:"). ttl
// controls how long a claimed TxID is held; it must be long enough that all
// listeners have received and processed the frame.
func New(addr, prefix string, ttl time.Duration) (*Store, error) {
	if ttl <= 0 {
		return nil, fmt.Errorf("txdedup: ttl must be > 0 (got %s); zero or negative TTL would create persistent keys", ttl)
	}

	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("txdedup: redis ping %s: %w", addr, err)
	}

	return &Store{
		client: client,
		prefix: prefix,
		ttl:    ttl,
	}, nil
}

// Claim attempts to claim txid. It returns (true, nil) when this caller is the
// first to claim the TxID (the frame should be forwarded). It returns
// (false, nil) when another listener already claimed it (suppress egress).
//
// On any Redis error it returns (true, err): the caller should forward the
// frame (fail-open) and record the error.
func (s *Store) Claim(txid [32]byte) (bool, error) {
	key := s.prefix + hex.EncodeToString(txid[:])
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	ok, err := s.client.SetNX(ctx, key, 1, s.ttl).Result()
	if err != nil {
		return true, err
	}
	return ok, nil
}

// Close releases the underlying Redis client.
func (s *Store) Close() error {
	return s.client.Close()
}
