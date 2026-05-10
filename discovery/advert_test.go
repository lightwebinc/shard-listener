package discovery

import (
	"net"
	"testing"
)

func testADVERT() *ADVERT {
	return &ADVERT{
		Scope:          0x05,
		NACKAddr:       net.ParseIP("fd20::41"),
		NACKPort:       9300,
		Tier:           0,
		Preference:     128,
		BeaconInterval: 60,
		Flags:          FlagMulticastRetransmit,
		InstanceID:     0xDEADBEEF,
	}
}

func TestDecodeADVERT_valid(t *testing.T) {
	a := testADVERT()
	var buf [ADVERTSize]byte
	EncodeADVERT(a, buf[:])

	got, err := DecodeADVERT(buf[:])
	if err != nil {
		t.Fatalf("DecodeADVERT: %v", err)
	}
	if got.Scope != a.Scope {
		t.Errorf("Scope = 0x%02X, want 0x%02X", got.Scope, a.Scope)
	}
	if !got.NACKAddr.Equal(a.NACKAddr) {
		t.Errorf("NACKAddr = %v, want %v", got.NACKAddr, a.NACKAddr)
	}
	if got.NACKPort != a.NACKPort {
		t.Errorf("NACKPort = %d, want %d", got.NACKPort, a.NACKPort)
	}
	if got.Tier != a.Tier {
		t.Errorf("Tier = %d, want %d", got.Tier, a.Tier)
	}
	if got.Preference != a.Preference {
		t.Errorf("Preference = %d, want %d", got.Preference, a.Preference)
	}
	if got.BeaconInterval != a.BeaconInterval {
		t.Errorf("BeaconInterval = %d, want %d", got.BeaconInterval, a.BeaconInterval)
	}
	if got.Flags != a.Flags {
		t.Errorf("Flags = 0x%04X, want 0x%04X", got.Flags, a.Flags)
	}
	if got.InstanceID != a.InstanceID {
		t.Errorf("InstanceID = 0x%08X, want 0x%08X", got.InstanceID, a.InstanceID)
	}
}

func TestDecodeADVERT_tierPreference(t *testing.T) {
	a := testADVERT()
	a.Tier = 3
	a.Preference = 200
	var buf [ADVERTSize]byte
	EncodeADVERT(a, buf[:])

	// Verify byte positions directly
	if buf[26] != 3 {
		t.Errorf("byte 26 (Tier) = %d, want 3", buf[26])
	}
	if buf[27] != 200 {
		t.Errorf("byte 27 (Preference) = %d, want 200", buf[27])
	}

	got, err := DecodeADVERT(buf[:])
	if err != nil {
		t.Fatalf("DecodeADVERT: %v", err)
	}
	if got.Tier != 3 {
		t.Errorf("Tier = %d, want 3", got.Tier)
	}
	if got.Preference != 200 {
		t.Errorf("Preference = %d, want 200", got.Preference)
	}
}

func TestDecodeADVERT_tooShort(t *testing.T) {
	_, err := DecodeADVERT(make([]byte, ADVERTSize-1))
	if err == nil {
		t.Error("expected error for short buffer")
	}
}

func TestDecodeADVERT_badMagic(t *testing.T) {
	var buf [ADVERTSize]byte
	EncodeADVERT(testADVERT(), buf[:])
	buf[0] = 0xFF
	_, err := DecodeADVERT(buf[:])
	if err == nil {
		t.Error("expected error for bad magic")
	}
}

func TestDecodeADVERT_badMsgType(t *testing.T) {
	var buf [ADVERTSize]byte
	EncodeADVERT(testADVERT(), buf[:])
	buf[6] = 0x99
	_, err := DecodeADVERT(buf[:])
	if err == nil {
		t.Error("expected error for bad MsgType")
	}
}

func TestDecodeADVERT_flags(t *testing.T) {
	for _, tc := range []struct {
		flags uint16
		name  string
	}{
		{FlagHasParent, "HasParent"},
		{FlagDraining, "Draining"},
		{FlagUnicastRetransmit, "UnicastRetransmit"},
		{FlagMulticastRetransmit, "MulticastRetransmit"},
		{FlagHasParent | FlagDraining | FlagMulticastRetransmit, "combined"},
	} {
		a := testADVERT()
		a.Flags = tc.flags
		var buf [ADVERTSize]byte
		EncodeADVERT(a, buf[:])
		got, err := DecodeADVERT(buf[:])
		if err != nil {
			t.Fatalf("%s: DecodeADVERT: %v", tc.name, err)
		}
		if got.Flags != tc.flags {
			t.Errorf("%s: Flags = 0x%04X, want 0x%04X", tc.name, got.Flags, tc.flags)
		}
	}
}

func TestEncodeADVERT_size(t *testing.T) {
	if ADVERTSize != 56 {
		t.Errorf("ADVERTSize = %d, want 56", ADVERTSize)
	}
}

func TestEncodeADVERT_scope(t *testing.T) {
	for _, scope := range []byte{0x05, 0x08, 0x0E, 0xFF} {
		a := testADVERT()
		a.Scope = scope
		var buf [ADVERTSize]byte
		EncodeADVERT(a, buf[:])
		got, err := DecodeADVERT(buf[:])
		if err != nil {
			t.Fatalf("scope=0x%02X: %v", scope, err)
		}
		if got.Scope != scope {
			t.Errorf("Scope = 0x%02X, want 0x%02X", got.Scope, scope)
		}
	}
}

func TestDecodeADVERT_zeroNACKAddr(t *testing.T) {
	a := testADVERT()
	a.NACKAddr = net.IPv6zero
	var buf [ADVERTSize]byte
	EncodeADVERT(a, buf[:])
	_, err := DecodeADVERT(buf[:])
	if err == nil {
		t.Error("expected error for all-zero NACKAddr")
	}
}
