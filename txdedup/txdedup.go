// Package txdedup provides per-deployment Redis-backed TxID egress dedup
// for bitcoin-shard-listener, plus optional courtesy marking of the proxy's
// ingress namespace ("network seen" set).
//
// # Two Redis namespaces, one Store per role
//
// The unified design splits dedup into two independent keys:
//
//   - bsp:tx:<hex-txid>       — proxy ingress dedup; SETNX'd by proxy on send
//     and optionally by listener on receive (courtesy mark). Used by the proxy
//     to know whether a TxID is already on the multicast network.
//
//   - bsl:egr:<dep-id>:<hex>  — listener per-deployment egress dedup; SETNX'd
//     by each listener before forwarding downstream. HA siblings (same dep-id)
//     race; only one forwards. Listeners in different deployments race
//     independently, so each deployment forwards at most once.
//
// Both Stores are *txidset.Store from bitcoin-shard-common. This package
// composes them with deployment-id key shaping and exposes a single Store
// type that the listener worker interacts with.
//
// # Backwards compatibility
//
// The old txdedup.Store had a single Claim(txid) method backed by a single
// Redis key. New(addr, prefix, ttl) is preserved but now constructs a Store
// in "single-deployment" mode where the egress prefix is used verbatim
// (no deployment-id appended). Existing tests and main.go continue to work.
//
// # Local-LRU fallback
//
// When the egress Redis address is empty, the Store still operates against
// an in-process LRU keyed by TxID. This is the single-server topology.
package txdedup

import (
	"fmt"
	"time"

	"github.com/lightwebinc/shard-common/txidset"
)

// Recorder is the metrics-callback interface implemented by the listener's
// metrics.Recorder. It surfaces both Claim and Mark outcomes so the operator
// can observe deployment-egress decisions independently from network-mark
// activity. Pass nil to disable metric recording.
type Recorder interface {
	EgressClaimLocalHit()
	EgressClaimWon()
	EgressClaimLost()
	EgressClaimError()
	IngressMarkSet()
	IngressMarkExisted()
	IngressMarkError()
	IngressMarkDropped()
}

// Store wraps two underlying *txidset.Store instances:
//
//   - egress  — per-deployment claim; key = egressPrefix + dep + ":" + hex.
//   - ingress — optional courtesy mark of the proxy's namespace. nil when
//     the listener is not configured to inform the local proxy.
type Store struct {
	egress       *txidset.Store
	egressPrefix string // already includes trailing ':<dep>:' when configured

	ingress       *txidset.Store
	ingressPrefix string
}

// Config configures a Store.
type Config struct {
	// Egress (per-deployment dedup before downstream forward).
	//
	// EgressRedisAddr empty → tier-1 local-only.
	// EgressPrefix is appended with ":<DeploymentID>:" before use, so the
	// final Redis key shape is "<EgressPrefix><DeploymentID>:<hex-txid>".
	// DeploymentID empty falls back to a single-deployment legacy key shape
	// "<EgressPrefix><hex-txid>" — preserves behaviour of the old single-key
	// txdedup.
	EgressRedisAddr string
	EgressPrefix    string
	EgressTTL       time.Duration
	EgressLocalCap  int
	DeploymentID    string

	// Ingress mark (courtesy SETNX to proxy's namespace). Empty
	// IngressRedisAddr disables the courtesy mark entirely.
	IngressRedisAddr string
	IngressPrefix    string
	IngressTTL       time.Duration
	IngressLocalCap  int

	// Optional metrics recorder. May be nil.
	Recorder Recorder
}

// NewWithConfig constructs a Store from the unified configuration. Either or
// both underlying stores may be disabled by leaving the corresponding
// RedisAddr empty AND LocalCap=0; in that case the respective operation
// becomes a no-op.
//
// At least one tier must be active for the egress side (otherwise Claim is
// trivially "always win"). Callers wanting to disable the feature should
// avoid attaching the Store to the listener worker entirely.
func NewWithConfig(cfg Config) (*Store, error) {
	if cfg.EgressTTL <= 0 {
		return nil, fmt.Errorf("txdedup: EgressTTL must be > 0")
	}
	if cfg.EgressLocalCap <= 0 {
		cfg.EgressLocalCap = txidset.DefaultLocalCapacity
	}
	if cfg.EgressPrefix == "" {
		cfg.EgressPrefix = "bsl:egr:"
	}

	egPrefix := cfg.EgressPrefix
	if cfg.DeploymentID != "" {
		egPrefix = cfg.EgressPrefix + cfg.DeploymentID + ":"
	}

	egRec := egressRecAdapter{rec: cfg.Recorder}
	eg, err := txidset.New(txidset.Config{
		RedisAddr:     cfg.EgressRedisAddr,
		TTL:           cfg.EgressTTL,
		LocalCapacity: cfg.EgressLocalCap,
		Recorder:      egRec,
	})
	if err != nil {
		return nil, fmt.Errorf("txdedup: egress store: %w", err)
	}

	s := &Store{egress: eg, egressPrefix: egPrefix}

	if cfg.IngressRedisAddr != "" || cfg.IngressPrefix != "" {
		if cfg.IngressTTL <= 0 {
			_ = eg.Close()
			return nil, fmt.Errorf("txdedup: IngressTTL must be > 0 when ingress mark is configured")
		}
		if cfg.IngressLocalCap <= 0 {
			cfg.IngressLocalCap = txidset.DefaultLocalCapacity
		}
		if cfg.IngressPrefix == "" {
			cfg.IngressPrefix = "bsp:tx:"
		}
		inRec := ingressRecAdapter{rec: cfg.Recorder}
		ing, err := txidset.New(txidset.Config{
			RedisAddr:     cfg.IngressRedisAddr,
			TTL:           cfg.IngressTTL,
			LocalCapacity: cfg.IngressLocalCap,
			Recorder:      inRec,
		})
		if err != nil {
			_ = eg.Close()
			return nil, fmt.Errorf("txdedup: ingress store: %w", err)
		}
		s.ingress = ing
		s.ingressPrefix = cfg.IngressPrefix
	}

	return s, nil
}

// New constructs a Store with the pre-deployment-id single-key shape.
// addr empty → local-only. ttl must be > 0. This preserves the original
// New(addr, prefix, ttl) signature; new callers should prefer
// [NewWithConfig].
func New(addr, prefix string, ttl time.Duration) (*Store, error) {
	return NewWithConfig(Config{
		EgressRedisAddr: addr,
		EgressPrefix:    prefix,
		EgressTTL:       ttl,
		EgressLocalCap:  txidset.DefaultLocalCapacity,
	})
}

// Claim races to win the per-deployment egress claim for txid. Returns:
//
//   - (true, nil)  — caller is the first; forward downstream.
//   - (false, nil) — sibling already claimed; suppress.
//   - (true, err)  — Redis error; fail-open (forward).
func (s *Store) Claim(txid [32]byte) (bool, error) {
	if s == nil || s.egress == nil {
		return true, nil
	}
	return s.egress.Claim(s.egressPrefix, txid)
}

// Mark performs a best-effort async SETNX against the configured ingress
// namespace. Used to inform the proxy that this listener observed a TxID
// (e.g. for cross-site bridged TxIDs the local proxy never saw). No-op when
// the ingress mark is not configured.
func (s *Store) Mark(txid [32]byte) {
	if s == nil || s.ingress == nil {
		return
	}
	s.ingress.Mark(s.ingressPrefix, txid)
}

// HasIngressMark reports whether courtesy ingress marking is configured.
// Used by the listener worker to gate the per-frame Mark call cheaply.
func (s *Store) HasIngressMark() bool {
	return s != nil && s.ingress != nil
}

// EgressPrefix returns the effective egress prefix (including deployment-id
// segment when configured). For diagnostic logging only.
func (s *Store) EgressPrefix() string {
	if s == nil {
		return ""
	}
	return s.egressPrefix
}

// IngressPrefix returns the effective ingress mark prefix; empty when the
// mark is not configured. For diagnostic logging only.
func (s *Store) IngressPrefix() string {
	if s == nil {
		return ""
	}
	return s.ingressPrefix
}

// Close releases both underlying Stores. Safe to call once.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	var first error
	if s.egress != nil {
		first = s.egress.Close()
	}
	if s.ingress != nil {
		if err := s.ingress.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// egressRecAdapter routes txidset.Recorder callbacks for the egress Store
// to the listener's Recorder. The prefix label from txidset is dropped:
// the listener already distinguishes egress vs ingress via separate methods.
type egressRecAdapter struct{ rec Recorder }

func (a egressRecAdapter) ClaimLocalHit(string) {
	if a.rec != nil {
		a.rec.EgressClaimLocalHit()
	}
}
func (a egressRecAdapter) ClaimWon(string) {
	if a.rec != nil {
		a.rec.EgressClaimWon()
	}
}
func (a egressRecAdapter) ClaimLost(string) {
	if a.rec != nil {
		a.rec.EgressClaimLost()
	}
}
func (a egressRecAdapter) ClaimError(string) {
	if a.rec != nil {
		a.rec.EgressClaimError()
	}
}
func (a egressRecAdapter) MarkSet(string)     {}
func (a egressRecAdapter) MarkExisted(string) {}
func (a egressRecAdapter) MarkError(string)   {}
func (a egressRecAdapter) MarkDropped(string) {}

// ingressRecAdapter routes txidset.Recorder callbacks for the ingress Store
// (courtesy mark) to the listener's Recorder.
type ingressRecAdapter struct{ rec Recorder }

func (a ingressRecAdapter) ClaimLocalHit(string) {} // Mark never calls Claim*
func (a ingressRecAdapter) ClaimWon(string)      {}
func (a ingressRecAdapter) ClaimLost(string)     {}
func (a ingressRecAdapter) ClaimError(string)    {}
func (a ingressRecAdapter) MarkSet(string) {
	if a.rec != nil {
		a.rec.IngressMarkSet()
	}
}
func (a ingressRecAdapter) MarkExisted(string) {
	if a.rec != nil {
		a.rec.IngressMarkExisted()
	}
}
func (a ingressRecAdapter) MarkError(string) {
	if a.rec != nil {
		a.rec.IngressMarkError()
	}
}
func (a ingressRecAdapter) MarkDropped(string) {
	if a.rec != nil {
		a.rec.IngressMarkDropped()
	}
}
