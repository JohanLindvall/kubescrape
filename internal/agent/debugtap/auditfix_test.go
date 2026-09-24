package debugtap

import (
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// A filter's two halves are attribute keys and values, not paths: the wildcard
// has to cross '/', or the two commonest slash-bearing attributes the agent
// stamps — a full image reference and a prefixed Kubernetes label key — are
// unreachable and the stream looks like "nothing is being exported".
func TestAttrFilterWildcardsCrossSlashes(t *testing.T) {
	attrs := pcommon.NewMap()
	attrs.PutStr("container.image.name", "docker.io/library/nginx:1.25")
	attrs.PutStr("k8s.pod.label.app.kubernetes.io/name", "web")
	attrs.PutStr("k8s.namespace.name", "team-a")

	for _, tc := range []struct {
		key, value string
		want       bool
	}{
		{"container.image.name", "*nginx*", true},
		{"container.image.name", "*/library/*", true},
		{"k8s.pod.label.*", "web", true},
		{"k8s.pod.label.*/name", "*", true},
		{"k8s.namespace.name", "team-*", true}, // slash-free operands are unchanged
		{"container.image.name", "*redis*", false},
		{"k8s.pod.label.*", "api", false},
		{"k8s.node.*", "*", false},
	} {
		f := newAttrFilter(tc.key, tc.value)
		if got := f.matches(attrs); got != tc.want {
			t.Errorf("attr=%s=%s matched %v, want %v", tc.key, tc.value, got, tc.want)
		}
	}
}

// The neutralization must not change how a pattern is PARSED: character
// classes and escapes still mean what path.Match says they mean.
func TestGlobMatchKeepsPatternSyntax(t *testing.T) {
	for _, tc := range []struct {
		pat, s string
		want   bool
	}{
		{"[abc]/x", "a/x", true},
		{"[abc]/x", "d/x", false},
		{`a\/b`, "a/b", true},
		{"a?b", "a/b", true}, // '?' is a single character, separator or not
		{"", "", true},
		{"*", "", true},
	} {
		if got := compileGlob(tc.pat).match(tc.s); got != tc.want {
			t.Errorf("glob %q matching %q = %v, want %v", tc.pat, tc.s, got, tc.want)
		}
	}
}

// The VALUE half of a filter is rendered on the exporting goroutine, once per
// key-matching filter per resource per export, and pcommon.Value.AsString
// JSON-marshals a Map or Slice in full (base64 for Bytes) — a cost the pattern
// ceilings (maxAttrFilters, maxAttrFilterBytes) cannot bound, because the
// PAYLOAD chooses it. Such a resource attribute can only come from a push on
// the unauthenticated ingest listeners, so those kinds never match and are
// never rendered.
func TestFilterValueRefusesUnboundedAttributeKinds(t *testing.T) {
	attrs := pcommon.NewMap()
	attrs.PutStr("k8s.namespace.name", "team-a")
	attrs.PutInt("k8s.pod.restart_count", 3)
	attrs.PutBool("sender.ready", true)
	attrs.PutDouble("sender.ratio", 0.5)
	attrs.PutEmpty("sender.empty")
	attrs.PutEmptyMap("sender.blob").PutStr("image", "nginx")
	attrs.PutEmptySlice("sender.list").AppendEmpty().SetStr("nginx")
	attrs.PutEmptyBytes("sender.bytes").FromRaw([]byte("nginx"))

	for _, tc := range []struct {
		key, value string
		want       bool
	}{
		// Scalars still match, in every kind the agent's own resources use.
		{"k8s.namespace.name", "team-*", true},
		{"k8s.pod.restart_count", "3", true},
		{"sender.ready", "true", true},
		{"sender.ratio", "0.5", true},
		{"sender.empty", "", true},
		// The structured kinds are never rendered, so nothing their CONTENT
		// would satisfy can match — including a filter whose key selects only
		// them, which is what makes this a bound rather than a coincidence.
		{"sender.blob", "*nginx*", false},
		{"sender.list", "*nginx*", false},
		{"sender.bytes", "*", false},
		{"sender.b*", "*nginx*", false},
	} {
		f := newAttrFilter(tc.key, tc.value)
		if got := f.matches(attrs); got != tc.want {
			t.Errorf("attr=%s=%s matched %v, want %v", tc.key, tc.value, got, tc.want)
		}
	}
}

// The bound, measured rather than asserted from the switch: a filter whose key
// matches a structured attribute must cost the same however large the sender
// made it. Before the skip, the wide-key filter below JSON-marshalled the whole
// map on every call.
func TestWideFilterDoesNotRenderStructuredValues(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector changes escape analysis and adds bookkeeping allocations")
	}
	attrs := pcommon.NewMap()
	// The map FIRST: Range stops at the first match, so a scalar in front of it
	// would make this pass without the skip ever being reached.
	blob := attrs.PutEmptyMap("sender.blob")
	for i := range 4096 {
		blob.PutStr(fmt.Sprintf("k%d", i), strings.Repeat("v", 64))
	}
	attrs.PutStr("k8s.namespace.name", "team-a")
	// A key glob that matches everything and a value glob that matches
	// everything: the shape maxAttrFilters was written against, reached through
	// the value side. It must not walk into the map at all.
	f := newAttrFilter("*", "*")
	if got := testing.AllocsPerRun(20, func() {
		if !f.matches(attrs) {
			t.Fatal("a match-everything filter must match the scalar attribute")
		}
	}); got > 1 {
		t.Errorf("a wide filter over a 4096-entry map attribute allocates %.1f times, want <= 1 "+
			"(rendering the map is proportional to what the sender chose)", got)
	}
}
