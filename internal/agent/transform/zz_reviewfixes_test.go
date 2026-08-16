package transform

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// int() of a long string is one uninterruptible, quadratic Starlark step, so it
// must be refused before the parse. A short string still works. Regression for
// boundedInt / maxIntStringLen.
func TestBoundedIntRejectsLongString(t *testing.T) {
	if err := runBody(t, `x = int("123456789")`); err != nil {
		t.Fatalf("int of a short numeric string errored: %v", err)
	}
	// "9" * 100000 builds a 100k-char string (within the string cap); int() of
	// it must be refused as over the character limit.
	err := runBody(t, `x = int("9" * 100000)`)
	mustContain(t, err, "character")
}

// A FAILED assignment inside a fail-open hook must leave NO partial mutation
// (the old PutEmpty-then-convert left an Empty-valued attribute the admit hook
// then forwarded). Regression for SetKey's remove-on-error.
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
