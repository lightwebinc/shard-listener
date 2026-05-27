// Package nack implements NORM-inspired multicast gap recovery for
// shard-listener. This file defines the 64-byte NACK request wire
// format and the 16-byte ACK/MISS response wire format.
//
// # NACK datagram (UDP, 64 bytes, 8-byte aligned)
//
//	Offset  Size  Field
//	------  ----  -----
//	     0     4  Magic (0xE3E1F3E8) — BSV mainnet magic
//	     4     2  ProtoVer (0x02BF)
//	     6     1  MsgType = 0x10 (NACK)
//	     7     1  Flags (reserved, must be 0x00)
//	     8     8  HashKey (uint64 BE) — stable per-flow identifier (XXH64 of sender+group+subtree)
//	    16     8  StartSeq (uint64 BE) — first missing sequence number (inclusive)
//	    24     8  EndSeq (uint64 BE) — last missing sequence number (inclusive; == StartSeq for single-frame)
//	    32    32  SubtreeID — 32-byte BRC-124 subtree identifier; all-zero = no subtree
//
// HashKey identifies the flow (sender × group × subtree), computed by the
// shard proxy as XXH64(senderIPv6 || groupIdx || subtreeID). It namespaces
// the SeqNum monotonic counter and forms the first half of the cache key
// HashKey||SeqNum at the retry endpoint.
//
// StartSeq == EndSeq is the single-frame retrieval case. StartSeq < EndSeq
// is reserved for future range queries.
//
// # Response datagram (MISS/ACK, UDP, 16 bytes)
//
//	Offset  Size  Field
//	------  ----  -----
//	     0     4  Magic (0xE3E1F3E8)
//	     4     2  ProtoVer (0x02BF)
//	     6     1  MsgType = 0x11 (MISS) or 0x12 (ACK)
//	     7     1  Flags (ACK: 0x01=multicast_sent, 0x02=unicast_sent)
//	     8     8  SeqNum of the retrieved frame (0 for MISS)
package nack

import (
	"encoding/binary"
	"errors"
)

const (
	// NACKSize is the fixed size of a NACK datagram in bytes.
	NACKSize = 64

	// MsgTypeNACK identifies a gap-retransmission request.
	MsgTypeNACK byte = 0x10

	// MsgTypeMISS identifies a "frame not in cache" response from a retry endpoint.
	MsgTypeMISS byte = 0x11

	// MsgTypeACK identifies a "frame found, retransmit dispatched" response
	// from a retry endpoint.
	MsgTypeACK byte = 0x12

	// ResponseSize is the fixed size of a MISS or ACK response datagram.
	ResponseSize = 16

	nackMagic    uint32 = 0xE3E1F3E8
	nackProtoVer uint16 = 0x02BF
)

// Sentinel errors.
var (
	// ErrBadNACK is returned when a received datagram does not decode as a valid NACK.
	ErrBadNACK = errors.New("nack: invalid datagram")

	// ErrBadResponse is returned when a received datagram does not decode as a
	// valid MISS or ACK response.
	ErrBadResponse = errors.New("nack: invalid response datagram")
)

// NACK is the in-memory representation of a 64-byte NACK datagram.
type NACK struct {
	MsgType   byte     // MsgTypeNACK
	HashKey   uint64   // stable per-flow identifier (XXH64 of sender+group+subtree)
	StartSeq  uint64   // first missing sequence number (inclusive)
	EndSeq    uint64   // last missing sequence number (inclusive; == StartSeq for single-frame)
	SubtreeID [32]byte // BRC-124 subtree namespace; all-zero = no subtree
}

// Encode serialises n into buf (must be at least [NACKSize] bytes).
func Encode(n *NACK, buf []byte) {
	_ = buf[NACKSize-1] // bounds check
	binary.BigEndian.PutUint32(buf[0:4], nackMagic)
	binary.BigEndian.PutUint16(buf[4:6], nackProtoVer)
	buf[6] = n.MsgType
	buf[7] = 0x00 // Flags (reserved)
	binary.BigEndian.PutUint64(buf[8:16], n.HashKey)
	binary.BigEndian.PutUint64(buf[16:24], n.StartSeq)
	binary.BigEndian.PutUint64(buf[24:32], n.EndSeq)
	copy(buf[32:64], n.SubtreeID[:])
}

// Decode parses a NACK datagram from buf.
// Returns [ErrBadNACK] if the datagram is too short, magic is wrong, or
// MsgType is not MsgTypeNACK.
func Decode(buf []byte) (*NACK, error) {
	if len(buf) < NACKSize {
		return nil, ErrBadNACK
	}
	if binary.BigEndian.Uint32(buf[0:4]) != nackMagic {
		return nil, ErrBadNACK
	}
	mt := buf[6]
	if mt != MsgTypeNACK {
		return nil, ErrBadNACK
	}
	n := &NACK{
		MsgType:  mt,
		HashKey:  binary.BigEndian.Uint64(buf[8:16]),
		StartSeq: binary.BigEndian.Uint64(buf[16:24]),
		EndSeq:   binary.BigEndian.Uint64(buf[24:32]),
	}
	copy(n.SubtreeID[:], buf[32:64])
	return n, nil
}

// Response is the in-memory representation of a 16-byte MISS or ACK response.
type Response struct {
	MsgType byte   // MsgTypeMISS or MsgTypeACK
	Flags   byte   // ACK flags: 0x01=multicast_sent, 0x02=unicast_sent
	SeqNum  uint64 // SeqNum of the retrieved frame; 0 for MISS
}

// EncodeResponse serialises r into buf (must be at least [ResponseSize] bytes).
func EncodeResponse(r *Response, buf []byte) {
	_ = buf[ResponseSize-1] // bounds check
	binary.BigEndian.PutUint32(buf[0:4], nackMagic)
	binary.BigEndian.PutUint16(buf[4:6], nackProtoVer)
	buf[6] = r.MsgType
	buf[7] = r.Flags
	binary.BigEndian.PutUint64(buf[8:16], r.SeqNum)
}

// DecodeResponse parses a MISS or ACK response from buf.
// Returns [ErrBadResponse] if the datagram is too short, magic is wrong,
// or MsgType is not MISS or ACK.
func DecodeResponse(buf []byte) (*Response, error) {
	if len(buf) < ResponseSize {
		return nil, ErrBadResponse
	}
	if binary.BigEndian.Uint32(buf[0:4]) != nackMagic {
		return nil, ErrBadResponse
	}
	mt := buf[6]
	if mt != MsgTypeMISS && mt != MsgTypeACK {
		return nil, ErrBadResponse
	}
	return &Response{
		MsgType: mt,
		Flags:   buf[7],
		SeqNum:  binary.BigEndian.Uint64(buf[8:16]),
	}, nil
}
