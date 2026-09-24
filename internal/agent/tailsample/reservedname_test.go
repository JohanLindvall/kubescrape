package tailsample

import (
	"strings"
	"testing"
)

// NoPolicyLabel ("none") is the metric label the buffering layer renders for
// the unattributed default drop, so a policy of that name would conflate its
// decisions with every no-opinion drop. It must be refused at compile.
// Regression.
func TestPolicyNamedNoneIsRefused(t *testing.T) {
	// The value is WIRE-visible — kubescrape_tail_sampling_traces_total's help
	// text names policy="none", and a dashboard selects it — so it is pinned as
	// a literal here: the constant ties the refusal to the label, this ties
	// both to what operators were told.
	if NoPolicyLabel != "none" {
		t.Fatalf("NoPolicyLabel = %q; the documented default-drop label is \"none\"", NoPolicyLabel)
	}
	_, err := New(Config{Policies: []PolicyConfig{{
		Name: NoPolicyLabel, Type: TypeAlwaysSample,
	}}})
	if err == nil {
		t.Fatalf("a policy named %q was accepted; it collides with the default-drop metric label", NoPolicyLabel)
	}
	if !strings.Contains(err.Error(), "reserved") {
		t.Errorf("error %q should explain that %q is reserved", err, NoPolicyLabel)
	}
}
