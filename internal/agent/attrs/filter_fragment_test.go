package attrs

import "testing"

// A regex containing a comma splits into fragments that RE2 compiles as literal
// braces — an enable filter would then silently drop every intended attribute.
// The unbalanced-bracket check must reject it loudly at startup.
func TestFilterCommaFragmentRejected(t *testing.T) {
	if _, err := NewFilter(`k8s\.pod\.label\..{1,3}`, ""); err == nil {
		t.Fatal("comma-split regex fragment compiled silently")
	}
	// Comma-free regexes (including escaped brackets and classes) still work.
	f, err := NewFilter(`k8s\.pod\..+|service\..{2}`, `secret\[.\]`)
	if err != nil {
		t.Fatalf("valid patterns rejected: %v", err)
	}
	if !f.Keep("k8s.pod.name") || f.Keep("other") {
		t.Fatal("filter semantics broken")
	}
}
