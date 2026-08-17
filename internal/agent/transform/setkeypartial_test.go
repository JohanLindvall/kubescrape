package transform

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// A FAILED assignment must leave NO partial mutation. SetKey adds the key with
// PutEmpty before converting the value, so a conversion error used to leave an
// Empty-valued attribute behind — and the ingest `admit` hook is FAIL-OPEN, so
// it then forwarded that stray attribute while reporting that the hook did
// nothing. Regression for SetKey's remove-on-error.
func TestAdmitFailedAssignmentLeavesNoPartial(t *testing.T) {
	w := hookWrapper(t, `
ingest: |
  def admit(resource):
      resource["team"] = {}
      return True
`)
	m := pcommon.NewMap()
	// The script errors on the unconvertible ({}) assignment; admit fails OPEN.
	if !w.AdmitResource(m) {
		t.Error("a hook script error must fail open (admit)")
	}
	if _, ok := m.Get("team"); ok {
		t.Errorf("a failed assignment left a partial attribute on the resource: %v", m.AsRaw())
	}
}

// The sharper half of the same contract: a failed assignment to an EXISTING key
// must leave that key's value ALONE. The first fix returned the error and left
// an Empty value; the second removed the key, which DELETED a good pre-existing
// value — data loss, reachable because the admit hook is fail-open and receives
// the real resource map, so the payload forwards with the attribute gone.
func TestFailedAssignmentPreservesAnExistingValue(t *testing.T) {
	w := hookWrapper(t, `
ingest: |
  def admit(resource):
      resource["service.name"] = {}
      return True
`)
	m := pcommon.NewMap()
	m.PutStr("service.name", "checkout")
	m.PutStr("keep", "me")

	if !w.AdmitResource(m) {
		t.Error("a hook script error must fail open (admit)")
	}
	v, ok := m.Get("service.name")
	if !ok {
		t.Fatalf("a failed assignment DELETED a pre-existing attribute: %v", m.AsRaw())
	}
	if v.Str() != "checkout" {
		t.Errorf("service.name = %q after a failed assignment, want the untouched %q", v.Str(), "checkout")
	}
	if got, _ := m.Get("keep"); got.Str() != "me" {
		t.Errorf("an unrelated attribute was disturbed: %v", m.AsRaw())
	}
}

// An out-of-range int is the non-type-mismatch failure, and the pre-check has
// to model it too or it silently diverges from fromStarlark.
func TestOutOfRangeIntLeavesTheExistingValue(t *testing.T) {
	w := hookWrapper(t, `
ingest: |
  def admit(resource):
      resource["n"] = 1 << 200
      return True
`)
	m := pcommon.NewMap()
	m.PutStr("n", "original")
	if !w.AdmitResource(m) {
		t.Error("a hook script error must fail open (admit)")
	}
	if got, ok := m.Get("n"); !ok || got.Str() != "original" {
		t.Errorf("an out-of-range int assignment disturbed the existing value: %v", m.AsRaw())
	}
}
