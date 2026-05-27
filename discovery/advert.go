// Package discovery implements ADVERT beacon decoding and retry endpoint
// registry management for shard-listener (BRC-126).
package discovery

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
)

// ADVERT wire format constants.
const (
	// ADVERTSize is the fixed size of an ADVERT beacon datagram.
	ADVERTSize = 56

	// MsgTypeADVERT is the message type byte for ADVERT beacons.
	MsgTypeADVERT byte = 0x20

	advertMagic    uint32 = 0xE3E1F3E8
	advertProtoVer uint16 = 0x02BF
)

// ADVERT flag bits (BRC-126).
const (
	FlagHasParent           uint16 = 0x0002
	FlagDraining            uint16 = 0x0004
	FlagUnicastRetransmit   uint16 = 0x0008
	FlagMulticastRetransmit uint16 = 0x0010
)

// ErrBadADVERT is returned when a received datagram does not decode as a valid ADVERT.
var ErrBadADVERT = errors.New("discovery: invalid ADVERT datagram")

// ADVERT is the in-memory representation of a decoded ADVERT beacon.
type ADVERT struct {
	Scope          byte   // 0x05=site, 0x08=org, 0x0E=global, 0xFF=all
	NACKAddr       net.IP // 16-byte IPv6 unicast address
	NACKPort       uint16
	Tier           uint8
	Preference     uint8
	BeaconInterval uint16 // seconds
	Flags          uint16
	InstanceID     uint32
}

// EncodeADVERT serialises a into buf (must be at least [ADVERTSize] bytes).
func EncodeADVERT(a *ADVERT, buf []byte) {
	_ = buf[ADVERTSize-1] // bounds check
	binary.BigEndian.PutUint32(buf[0:4], advertMagic)
	binary.BigEndian.PutUint16(buf[4:6], advertProtoVer)
	buf[6] = MsgTypeADVERT
	buf[7] = a.Scope
	copy(buf[8:24], a.NACKAddr.To16())
	binary.BigEndian.PutUint16(buf[24:26], a.NACKPort)
	buf[26] = a.Tier
	buf[27] = a.Preference
	binary.BigEndian.PutUint16(buf[28:30], a.BeaconInterval)
	binary.BigEndian.PutUint16(buf[30:32], a.Flags)
	binary.BigEndian.PutUint32(buf[32:36], a.InstanceID)
	// bytes 36-39: reserved
	binary.BigEndian.PutUint32(buf[36:40], 0)
	// bytes 40-55: reserved (future use)
	for i := 40; i < 56; i++ {
		buf[i] = 0
	}
}

// DecodeADVERT parses an ADVERT beacon from buf.
// Returns [ErrBadADVERT] if the datagram is too short, magic is wrong,
// or MsgType is not ADVERT.
func DecodeADVERT(buf []byte) (*ADVERT, error) {
	if len(buf) < ADVERTSize {
		return nil, ErrBadADVERT
	}
	if binary.BigEndian.Uint32(buf[0:4]) != advertMagic {
		return nil, ErrBadADVERT
	}
	if buf[6] != MsgTypeADVERT {
		return nil, ErrBadADVERT
	}

	nackAddr := make(net.IP, 16)
	copy(nackAddr, buf[8:24])

	// Validate NACKAddr is not all zeros (unset)
	allZero := true
	for _, b := range nackAddr {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return nil, fmt.Errorf("%w: NACKAddr is all zeros", ErrBadADVERT)
	}

	return &ADVERT{
		Scope:          buf[7],
		NACKAddr:       nackAddr,
		NACKPort:       binary.BigEndian.Uint16(buf[24:26]),
		Tier:           buf[26],
		Preference:     buf[27],
		BeaconInterval: binary.BigEndian.Uint16(buf[28:30]),
		Flags:          binary.BigEndian.Uint16(buf[30:32]),
		InstanceID:     binary.BigEndian.Uint32(buf[32:36]),
	}, nil
}
