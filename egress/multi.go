package egress

import (
	"errors"
	"strings"

	"github.com/lightwebinc/shard-common/frame"
)

// MultiSender fans a receiver's decoded frames out to several delivery-tier
// destinations at once. It is the multi-destination egress that distinguishes
// `-mode receiver` (the multicast-facing half of the receiver/delivery split)
// from collapsed's single downstream: a receiver joins the fabric (S,G), runs
// gap/NACK, then forwards every raw frame to each configured delivery host, and
// the delivery hosts fan out per-consumer.
//
// Each destination gets EVERY frame verbatim (the full stamped envelope —
// HashKey/SeqNum/submitter-id — is preserved so delivery-side own-traffic
// exclusion and metering still work). A send failure to one destination does
// not stop the others; the joined error is returned.
type MultiSender struct {
	senders []*Sender
	proto   string
}

// NewMulti builds a MultiSender over the given host:port destinations, all
// sharing the same proto ("udp"|"tcp"). stripHeader is forced OFF: the
// receiver→delivery handoff must preserve the full envelope.
func NewMulti(addrs []string, proto string) (*MultiSender, error) {
	m := &MultiSender{proto: proto}
	for _, a := range addrs {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		s, err := New(a, proto, false)
		if err != nil {
			_ = m.Close()
			return nil, err
		}
		m.senders = append(m.senders, s)
	}
	if len(m.senders) == 0 {
		return nil, errors.New("egress: no valid delivery addresses")
	}
	return m, nil
}

func (m *MultiSender) fanout(fn func(*Sender) error) error {
	var errs []error
	for _, s := range m.senders {
		if err := fn(s); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Send forwards a BRC-12 / BRC-124 / BRC-128 (or BRC-134 anchor) frame to every destination.
func (m *MultiSender) Send(raw []byte, f *frame.Frame) error {
	return m.fanout(func(s *Sender) error { return s.Send(raw, f) })
}

// SendBlock forwards a BRC-131 block control frame to every destination.
func (m *MultiSender) SendBlock(raw []byte, bf *frame.BlockFrame) error {
	return m.fanout(func(s *Sender) error { return s.SendBlock(raw, bf) })
}

// SendSubtreeData forwards a BRC-132 subtree data frame to every destination.
func (m *MultiSender) SendSubtreeData(raw []byte, sf *frame.SubtreeDataFrame) error {
	return m.fanout(func(s *Sender) error { return s.SendSubtreeData(raw, sf) })
}

// SendRaw forwards an arbitrary buffer verbatim to every destination.
func (m *MultiSender) SendRaw(buf []byte) error {
	return m.fanout(func(s *Sender) error { return s.SendRaw(buf) })
}

// Proto returns the shared egress protocol label.
func (m *MultiSender) Proto() string { return m.proto }

// Dests returns the number of delivery destinations.
func (m *MultiSender) Dests() int { return len(m.senders) }

// Close releases every underlying sender.
func (m *MultiSender) Close() error {
	return m.fanout(func(s *Sender) error { return s.Close() })
}

var _ EgressSink = (*MultiSender)(nil)
