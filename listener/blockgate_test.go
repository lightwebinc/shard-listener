package listener

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/pow"
	"github.com/lightwebinc/shard-listener/filter"
)

const regtestBits = 0x207fffff

// minedAnnounce builds a BRC-131 BlockAnnounce frame whose in-frame header
// carries valid regtest PoW, with the given coinbase TxID in the payload.
func minedAnnounce(t *testing.T, coinbase [32]byte) []byte {
	t.Helper()
	var hdr [80]byte
	binary.LittleEndian.PutUint32(hdr[72:76], regtestBits)
	for nonce := uint32(0); nonce < 1_000_000; nonce++ {
		binary.LittleEndian.PutUint32(hdr[76:80], nonce)
		if pow.CheckHeader(hdr[:], nil) {
			break
		}
		if nonce == 999_999 {
			t.Fatal("could not mine regtest header")
		}
	}
	payload := frame.EncodeBlockAnnounce(&frame.BlockAnnouncePayload{Header: hdr, CoinbaseTxID: coinbase})
	bf := &frame.BlockFrame{MsgType: frame.BlockMsgAnnounce, ContentID: [32]byte{0xB1}, Payload: payload}
	buf := make([]byte, frame.HeaderSize+len(payload))
	if _, err := frame.EncodeBlock(bf, buf); err != nil {
		t.Fatal(err)
	}
	return buf
}

// coinbaseFrame builds a BRC-133 coinbase frame whose ContentID is txid.
func coinbaseFrame(t *testing.T, txid [32]byte) []byte {
	t.Helper()
	bf := &frame.BlockFrame{MsgType: frame.BlockMsgCoinbase, ContentID: txid, Payload: []byte("raw-cb")}
	buf := make([]byte, frame.HeaderSize+len(bf.Payload))
	if _, err := frame.EncodeBlock(bf, buf); err != nil {
		t.Fatal(err)
	}
	return buf
}

func forwarded(ch <-chan []byte) bool {
	select {
	case <-ch:
		return true
	case <-time.After(300 * time.Millisecond):
		return false
	}
}

func TestBlockGate_PoWAndCoinbaseCorrelation(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	w := newWorker(t, addr, filter.New(nil, nil, nil, nil))
	corr := NewCoinbaseCorrelator(128, time.Minute)
	w.SetBlockPoW(true, 0, corr)

	cb := [32]byte{0xCC, 0x01}

	// Invalid-PoW announce (zero header ⇒ nBits 0) is dropped.
	junk := func() []byte {
		var hdr [80]byte // nBits = 0 ⇒ invalid target
		payload := frame.EncodeBlockAnnounce(&frame.BlockAnnouncePayload{Header: hdr, CoinbaseTxID: cb})
		bf := &frame.BlockFrame{MsgType: frame.BlockMsgAnnounce, ContentID: [32]byte{0xBA}, Payload: payload}
		buf := make([]byte, frame.HeaderSize+len(payload))
		if _, err := frame.EncodeBlock(bf, buf); err != nil {
			t.Fatal(err)
		}
		return buf
	}()
	w.processBlockFrame(junk)
	if forwarded(ch) {
		t.Fatal("invalid-PoW block announce must be dropped")
	}

	// A coinbase with no validated block yet is dropped (uncorrelated).
	w.processBlockFrame(coinbaseFrame(t, cb))
	if forwarded(ch) {
		t.Fatal("uncorrelated coinbase must be dropped")
	}

	// Valid-PoW announce forwards and records its coinbase TxID.
	w.processBlockFrame(minedAnnounce(t, cb))
	if !forwarded(ch) {
		t.Fatal("valid-PoW block announce must forward")
	}
	if !corr.Has(cb) {
		t.Fatal("validated block's coinbase TxID must be recorded")
	}

	// Now the correlated coinbase forwards; an unrelated one still drops.
	w.processBlockFrame(coinbaseFrame(t, cb))
	if !forwarded(ch) {
		t.Fatal("correlated coinbase must forward")
	}
	w.processBlockFrame(coinbaseFrame(t, [32]byte{0xDE, 0xAD}))
	if forwarded(ch) {
		t.Fatal("coinbase with no matching block must be dropped")
	}
}

func TestBlockGate_OffForwardsEverything(t *testing.T) {
	addr, ch, cleanup := newSink(t)
	defer cleanup()
	w := newWorker(t, addr, filter.New(nil, nil, nil, nil))
	// Gate off (default): even a zero-PoW announce forwards.
	var hdr [80]byte
	payload := frame.EncodeBlockAnnounce(&frame.BlockAnnouncePayload{Header: hdr})
	bf := &frame.BlockFrame{MsgType: frame.BlockMsgAnnounce, ContentID: [32]byte{0x01}, Payload: payload}
	buf := make([]byte, frame.HeaderSize+len(payload))
	if _, err := frame.EncodeBlock(bf, buf); err != nil {
		t.Fatal(err)
	}
	w.processBlockFrame(buf)
	if !forwarded(ch) {
		t.Fatal("gate off: block announce must forward regardless of PoW")
	}
}

func TestCoinbaseCorrelator_TTLExpiry(t *testing.T) {
	c := NewCoinbaseCorrelator(8, 10*time.Millisecond)
	k := [32]byte{0x42}
	c.Add(k)
	if !c.Has(k) {
		t.Fatal("entry should be present immediately")
	}
	time.Sleep(20 * time.Millisecond)
	if c.Has(k) {
		t.Fatal("entry should expire after TTL")
	}
}
