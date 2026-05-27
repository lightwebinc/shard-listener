// Package filter implements allocation-free shard and subtree filtering for
// shard-listener. All logic is pure; no I/O is performed.
//
// A frame passes the filter if and only if:
//  1. Its group index is in the shard-include set (or the set is empty = all).
//  2. Its SubtreeID is NOT in the subtree-exclude set.
//  3. Its SubtreeID is in the subtree-include set OR in any subscribed subtree
//     group (via groupReg), OR both sets are empty (accept all).
//
// BRC-12 frames have a zero SubtreeID. If subtree-include is non-empty and no
// group registry is set, a BRC-12 frame will pass only if [32]byte{} is in the
// include set.
package filter

import (
	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-listener/subtreegroup"
)

// Filter holds the compiled include/exclude sets. Construct with [New].
type Filter struct {
	shardInclude   map[uint32]struct{}    // nil = all shards accepted
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
		f.shardInclude = make(map[uint32]struct{}, len(shardInclude))
		for _, idx := range shardInclude {
			f.shardInclude[idx] = struct{}{}
		}
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
	if f.shardInclude != nil {
		if _, ok := f.shardInclude[groupIdx]; !ok {
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
