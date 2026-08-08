package debugtap

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
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
		f := attrFilter{Key: tc.key, Value: tc.value}
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
		if got := globMatch(tc.pat, tc.s); got != tc.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", tc.pat, tc.s, got, tc.want)
		}
	}
}
