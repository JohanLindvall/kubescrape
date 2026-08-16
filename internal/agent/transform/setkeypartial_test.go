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
