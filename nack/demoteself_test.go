package nack

import "testing"

// A retry co-located with the listener shares its loss, so it must be the LAST
// escalation target, not the first. The tracker advances one endpoint per retry
// with backoff, so a self-first order burns the whole retry budget on the one
// endpoint that provably cannot repair link loss — measured live as
// gaps_unrecovered tracking gaps_detected 1:1.
func TestDemoteSelf(t *testing.T) {
	self := "fd00:50:0:6::1"
	in := []string{
		"[fd00:50:0:6::1]:9300", // self — must sink to last
		"[fd00:50:0:7::1]:9300",
		"[fd00:50:0:2::1]:9300",
	}
	got := demoteSelf(in, self)
	if len(got) != 3 {
		t.Fatalf("length changed: %v", got)
	}
	if got[len(got)-1] != "[fd00:50:0:6::1]:9300" {
		t.Errorf("self not demoted to last: %v", got)
	}
	// Remote order is preserved (the roster's own preference ordering survives).
	if got[0] != "[fd00:50:0:7::1]:9300" || got[1] != "[fd00:50:0:2::1]:9300" {
		t.Errorf("remote order not preserved: %v", got)
	}
	// Self is DEMOTED, never dropped — it still repairs listener-local drops.
	var found bool
	for _, e := range got {
		if e == "[fd00:50:0:6::1]:9300" {
			found = true
		}
	}
	if !found {
		t.Error("self endpoint removed; it must remain as a last resort")
	}
}

// Degenerate inputs must not reorder or panic.
func TestDemoteSelfNoops(t *testing.T) {
	in := []string{"[fd00::1]:9300", "[fd00::2]:9300"}
	if got := demoteSelf(in, ""); &got[0] != &in[0] {
		t.Error("empty selfAddr should return the input untouched")
	}
	single := []string{"[fd00::1]:9300"}
	if got := demoteSelf(single, "fd00::1"); len(got) != 1 {
		t.Error("single-endpoint list altered")
	}
	// Textual variants of the same address still match.
	got := demoteSelf([]string{"[fd00:0:0:0:0:0:0:1]:9300", "[fd00::9]:9300"}, "fd00::1")
	if got[len(got)-1] != "[fd00:0:0:0:0:0:0:1]:9300" {
		t.Errorf("expanded-form self not matched: %v", got)
	}
}
