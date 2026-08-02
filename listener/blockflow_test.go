package listener

import (
	"testing"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/shard"
)

// TestBlockFlowIdx pins the coinbase flow separation. Both message types
// egress to GroupBlockBroadcast, but they are distinct proxy-stamped flows, so
// gap metrics must not conflate them.
func TestBlockFlowIdx(t *testing.T) {
	if got := blockFlowIdx(frame.BlockMsgCoinbase); got != uint32(shard.GroupCoinbaseFlow) {
		t.Errorf("coinbase flow idx = 0x%X, want 0x%X", got, uint32(shard.GroupCoinbaseFlow))
	}
	if got := blockFlowIdx(frame.BlockMsgAnnounce); got != uint32(shard.GroupBlockBroadcast) {
		t.Errorf("announce flow idx = 0x%X, want 0x%X", got, uint32(shard.GroupBlockBroadcast))
	}
}
