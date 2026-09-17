package discovery

import (
	"net"
	"testing"
)

// During the BRC-126/129 transition the listener joins the any-source and
// the source-specific form of the beacon group on the SAME UDP port. Two
// wildcard binds on one port collide, so those two addresses must end up in
// one bucket — one socket, two joins.
func TestBucketByPort_FF0xAndFF3xShareOneSocket(t *testing.T) {
	asm := net.ParseIP("ff05::b:fffd")
	ssm := net.ParseIP("ff35::b:fffd")
	groups := []*net.UDPAddr{
		{IP: asm, Port: 9300},
		{IP: ssm, Port: 9300},
		{IP: asm, Port: 9001},
		{IP: ssm, Port: 9001},
	}

	buckets := bucketByPort(groups)
	if len(buckets) != 2 {
		t.Fatalf("buckets = %d, want 2 (one socket per port)", len(buckets))
	}
	for i, want := range []int{9300, 9001} {
		if len(buckets[i]) != 2 {
			t.Fatalf("bucket %d has %d addresses, want 2", i, len(buckets[i]))
		}
		for _, grp := range buckets[i] {
			if grp.Port != want {
				t.Errorf("bucket %d holds port %d, want %d", i, grp.Port, want)
			}
		}
		if !buckets[i][0].IP.Equal(asm) || !buckets[i][1].IP.Equal(ssm) {
			t.Errorf("bucket %d addresses = %s,%s; want the legacy prefix first",
				i, buckets[i][0].IP, buckets[i][1].IP)
		}
	}
}

// The single-prefix case (compat asm-only or derived, or any ASM
// deployment) must still be one address per socket, unchanged.
func TestBucketByPort_SinglePrefix(t *testing.T) {
	ip := net.ParseIP("ff05::b:fffd")
	buckets := bucketByPort([]*net.UDPAddr{
		{IP: ip, Port: 9300},
		{IP: ip, Port: 9001},
	})
	if len(buckets) != 2 {
		t.Fatalf("buckets = %d, want 2", len(buckets))
	}
	for i, b := range buckets {
		if len(b) != 1 {
			t.Errorf("bucket %d has %d addresses, want 1", i, len(b))
		}
	}
}

func TestBucketByPort_Empty(t *testing.T) {
	if got := bucketByPort(nil); len(got) != 0 {
		t.Errorf("buckets = %d, want 0", len(got))
	}
}
