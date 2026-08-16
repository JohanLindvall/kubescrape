package attrs_test

// This external test package imports internal/agent/route and
// internal/agent/transform (which both depend on internal/agent/attrs, so the
// non-test attrs package cannot import them) to pin attrs.ReservedPlumbing's
// literal keys to the real markers. If route.ScriptMarker or
// transform.DropMarker ever changes, this fails rather than silently leaving
// the pod-annotation / logAttributes surface unprotected.

import (
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/route"
	"github.com/JohanLindvall/kubescrape/internal/agent/transform"
)

func TestReservedPlumbingMatchesTheMarkers(t *testing.T) {
	for _, k := range []string{route.ScriptMarker, transform.DropMarker} {
		if !attrs.ReservedPlumbing(k) {
			t.Errorf("attrs.ReservedPlumbing(%q) = false; the marker is not covered by the reserved set", k)
		}
	}
	// And the enumerated set is exactly those two, so a new marker forces a
	// deliberate addition here rather than silently escaping the strip.
	got := attrs.ReservedPlumbingKeys()
	want := map[string]bool{route.ScriptMarker: true, transform.DropMarker: true}
	if len(got) != len(want) {
		t.Fatalf("ReservedPlumbingKeys() = %v; want exactly %v", got, want)
	}
	for _, k := range got {
		if !want[k] {
			t.Errorf("ReservedPlumbingKeys() has unexpected key %q — pin it to its marker here", k)
		}
	}
}
