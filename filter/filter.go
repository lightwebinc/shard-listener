// Package filter implements allocation-free shard and subtree filtering for
// shard-listener. All logic is pure; no I/O is performed.
//
// A frame passes the filter if and only if:
//  1. Its group index is in the shard set: the shard-include set plus any
//     shards added at runtime (SetRuntimeShards), or every shard when the
//     shard-include set is empty.
//  2. Its SubtreeID is NOT in the subtree-exclude set.
//  3. Its SubtreeID is in the subtree-include set OR in any subscribed subtree
//     group (via groupReg), OR both sets are empty (accept all).
//
// BRC-12 frames have a zero SubtreeID. If subtree-include is non-empty and no
// group registry is set, a BRC-12 frame will pass only if [32]byte{} is in the
// include set.
package filter

import (
	"sync/atomic"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-listener/subtreegroup"
)

// Filter holds the compiled include/exclude sets. Construct with [New].
type Filter struct {
	shardStatic map[uint32]struct{} // the -shard-include set; nil = all shards accepted
	// shards is the effective shard set Allow checks: shardStatic plus the
	// runtime additions (SetRuntimeShards), swapped whole so a worker never
	// sees a half-built set. nil = all shards accepted.
	shards         atomic.Pointer[map[uint32]struct{}]
	subtreeInclude map[[32]byte]struct{}  // nil = no static include
	subtreeExclude map[[32]byte]struct{}  // nil = no subtrees excluded
	groupReg       *subtreegroup.Registry // nil = group filtering disabled
}

// New constructs a Filter from the parsed config lists.
//   - shardInclude: nil or empty means accept all shard indices.
//   - subtreeInclude: nil or empty means no static subtree allowlist.
//   - subtreeExclude: nil or empty means exclude nothing.
//   - groupReg: nil disables dynamic subtree-group filtering.
//
// When both subtreeInclude and groupReg are nil/empty, all non-excluded
// subtrees are accepted (open mode).
func New(shardInclude []uint32, subtreeInclude, subtreeExclude [][32]byte, groupReg *subtreegroup.Registry) *Filter {
	f := &Filter{}
	if len(shardInclude) > 0 {
		f.shardStatic = make(map[uint32]struct{}, len(shardInclude))
		for _, idx := range shardInclude {
			f.shardStatic[idx] = struct{}{}
		}
		f.SetRuntimeShards(nil)
	}
	if len(subtreeInclude) > 0 {
		f.subtreeInclude = make(map[[32]byte]struct{}, len(subtreeInclude))
		for _, id := range subtreeInclude {
			f.subtreeInclude[id] = struct{}{}
		}
	}
	if len(subtreeExclude) > 0 {
		f.subtreeExclude = make(map[[32]byte]struct{}, len(subtreeExclude))
		for _, id := range subtreeExclude {
			f.subtreeExclude[id] = struct{}{}
		}
	}
	f.groupReg = groupReg
	return f
}

// Allow returns whether the frame should be forwarded to egress.
// If the frame is denied, reason is one of "shard_filter",
// "subtree_exclude", or "subtree_include_miss".
// groupIdx is derived from the frame's TxID by the caller.
func (f *Filter) Allow(groupIdx uint32, fr *frame.Frame) (bool, string) {
	if shards := f.shards.Load(); shards != nil {
		if _, ok := (*shards)[groupIdx]; !ok {
			return false, "shard_filter"
		}
	}
	if f.subtreeExclude != nil {
		if _, ok := f.subtreeExclude[fr.SubtreeID]; ok {
			return false, "subtree_exclude"
		}
	}
	// Accept if: static include matches, OR group registry matches, OR both are
	// nil/empty (open mode).
	if f.subtreeInclude != nil || f.groupReg != nil {
		_, inStatic := f.subtreeInclude[fr.SubtreeID]
		inGroup := f.groupReg != nil && f.groupReg.Contains(fr.SubtreeID)
		if !inStatic && !inGroup {
			return false, "subtree_include_miss"
		}
	}
	return true, ""
}

// SetRuntimeShards replaces the shards admitted beyond the -shard-include set:
// the BRC-139 pilot shards the listener subscribes to at runtime
// (-shard-include-from-manifest). The -shard-include set is always kept, and a
// filter that admits every shard (no -shard-include) is left as it is; the
// subtree rules are untouched. Safe to call while workers run Allow.
func (f *Filter) SetRuntimeShards(extra []uint32) {
	if f.shardStatic == nil {
		return
	}
	m := make(map[uint32]struct{}, len(f.shardStatic)+len(extra))
	for idx := range f.shardStatic {
		m[idx] = struct{}{}
	}
	for _, idx := range extra {
		m[idx] = struct{}{}
	}
	f.shards.Store(&m)
}
