package attrs

// SenderIdentityKeys is a SECURITY-relevant list in two consumers — the ingest
// splitter strips it before re-labelling a resource as a described object's,
// and the application-facing receivers strip the reserved subset of it at first
// receipt — so it is pinned in BOTH directions here, like IdentityKeys: nothing
// the builder can emit may be missing from it, and nothing may be in it that is
// neither emitted nor an explicitly documented sender-only sibling.

import (
	"slices"
	"testing"
)

func TestSenderIdentityKeysCoverEveryBuilderIdentityKey(t *testing.T) {
	sender := SenderIdentityKeys()
	if !slices.IsSorted(sender) {
		t.Errorf("SenderIdentityKeys is not sorted: %v", sender)
	}
	for _, k := range IdentityKeys() {
		if !slices.Contains(sender, k) {
			t.Errorf("the builder emits %q and SenderIdentityKeys lacks it: a sender's %q would survive "+
				"onto every object the ingest splitter re-labels", k, k)
		}
	}
	for _, k := range sender {
		if slices.Contains(IdentityKeys(), k) || slices.Contains(senderOnlyIdentityKeys, k) {
			continue
		}
		t.Errorf("SenderIdentityKeys lists %q, which the builder never emits and senderOnlyIdentityKeys "+
			"does not document: add it there with the reason, or remove it", k)
	}
}

// Every key a workload or a line must NEVER set has to be one this package
// recognises as identity — otherwise the receipt-time strip (which is keyed on
// the reserved set) and the split-time strip (keyed on this one) would disagree
// about what an identity attribute even is.
func TestReservedIdentityIsASubsetOfSenderIdentity(t *testing.T) {
	sender := SenderIdentityKeys()
	for _, k := range ReservedIdentityKeys() {
		if !slices.Contains(sender, k) {
			t.Errorf("%q is reserved identity but is not in SenderIdentityKeys: the two strips disagree "+
				"about whether it names the resource's own object", k)
		}
	}
}

// The list is package state handed out by value; a consumer appending to it
// must not extend the original (senderIdentityAttrs in otlpingest does exactly
// that kind of append).
func TestSenderIdentityKeysReturnsACopy(t *testing.T) {
	a := SenderIdentityKeys()
	a[0] = "mutated"
	if b := SenderIdentityKeys(); b[0] == "mutated" {
		t.Fatalf("mutating the result leaked into the package list: %v", b)
	}
}
