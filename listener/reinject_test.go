package listener

import (
	"testing"

	"github.com/lightwebinc/shard-listener/filter"
)

// Reinject must report whether the pipeline ACCEPTED the frame. A BRC-131
// announce rejected by the block-control gate can never be recovered — the retry
// endpoint holds exactly that frame and the gate rejects it every time — so the
// tracker needs the signal to stop retrying AND to avoid booking a repair for
// data no consumer received.
func TestReinject_ReportsGateRejection(t *testing.T) {
	mainAddr, _, cleanup := newSink(t)
	defer cleanup()

	w := newWorker(t, mainAddr, filter.New(nil, nil, nil, nil))
	// The helper builds an all-but-empty 80-byte header: nBits 0 expands to no
	// valid target, so the gate cannot pass it.
	w.SetBlockPoW(true, 0)

	if accepted := w.Reinject(buildBlockAnnounceFrame(t, 0xAA, 1, 1)); accepted {
		t.Fatal("Reinject reported acceptance for a frame the block gate rejected")
	}
}

// With the gate off the same frame is accepted, so a genuine repair is still
// booked as one.
func TestReinject_ReportsAcceptance(t *testing.T) {
	mainAddr, _, cleanup := newSink(t)
	defer cleanup()

	w := newWorker(t, mainAddr, filter.New(nil, nil, nil, nil))
	if accepted := w.Reinject(buildBlockAnnounceFrame(t, 0xAA, 1, 1)); !accepted {
		t.Fatal("Reinject reported rejection for a frame the pipeline accepted")
	}
}

// A transaction frame has no gate, so it is accepted by construction.
func TestReinject_TxFrameAccepted(t *testing.T) {
	mainAddr, _, cleanup := newSink(t)
	defer cleanup()

	w := newWorker(t, mainAddr, filter.New(nil, nil, nil, nil))
	if accepted := w.Reinject(buildBRC124Frame(t, [32]byte{}, []byte("tx"))); !accepted {
		t.Fatal("Reinject rejected an ordinary transaction frame")
	}
}
