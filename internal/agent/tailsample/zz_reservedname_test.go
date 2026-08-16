package tailsample

import (
	"strings"
	"testing"
)

// "none" is the metric label the buffering layer renders for the unattributed
// default drop, so a policy of that name would conflate its decisions with
// every no-opinion drop. It must be refused at compile. Regression.
func TestPolicyNamedNoneIsRefused(t *testing.T) {
	_, err := New(Config{Policies: []PolicyConfig{{
		Name: "none", Type: TypeAlwaysSample,
	}}})
	if err == nil {
		t.Fatal("a policy named \"none\" was accepted; it collides with the default-drop metric label")
	}
	if !strings.Contains(err.Error(), "reserved") {
		t.Errorf("error %q should explain that \"none\" is reserved", err)
	}
}
