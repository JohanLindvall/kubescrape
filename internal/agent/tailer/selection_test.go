package tailer

import "testing"

// Namespace selection is read from the CRI FILENAME at discovery, so a
// non-matching file is never opened, tracked or read. A logs.rules drop can
// express the same intent but only after paying the read, parse and enrich —
// it saves egress, not node CPU.
func TestSourceNamespaceSelection(t *testing.T) {
	cases := []struct {
		name string
		src  Source
		ns   string
		want bool
	}{
		{"no config accepts all", Source{Containerd: true}, "anything", true},
		{"allowlist match", Source{Containerd: true, Namespaces: []string{"team-*"}}, "team-a", true},
		{"allowlist miss", Source{Containerd: true, Namespaces: []string{"team-*"}}, "kube-system", false},
		{"exact allow", Source{Containerd: true, Namespaces: []string{"prod"}}, "prod", true},
		{"denylist wins over allow", Source{
			Containerd:        true,
			Namespaces:        []string{"team-*"},
			ExcludeNamespaces: []string{"team-scratch"},
		}, "team-scratch", false},
		{"denylist only", Source{Containerd: true, ExcludeNamespaces: []string{"kube-*"}}, "kube-system", false},
		{"denylist only, unrelated ns", Source{Containerd: true, ExcludeNamespaces: []string{"kube-*"}}, "prod", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cs := compileSources([]Source{tc.src}, "/var/log/containers", true)[0]
			if got := cs.wantNamespace(tc.ns); got != tc.want {
				t.Fatalf("wantNamespace(%q) = %v, want %v", tc.ns, got, tc.want)
			}
		})
	}
}

// Pod labels are only known once metadata resolves, so the selector is applied
// there: the file is tracked but no data is ever read from it.
func TestSourceLabelSelector(t *testing.T) {
	cs := compileSources([]Source{{
		Containerd: true,
		Selector:   map[string]string{"logging": "true", "tier": "web"},
	}}, "/var/log/containers", true)[0]

	if !cs.wantLabels(map[string]string{"logging": "true", "tier": "web", "extra": "x"}) {
		t.Error("a pod matching every selector key must be collected")
	}
	if cs.wantLabels(map[string]string{"logging": "true"}) {
		t.Error("a pod missing a selector key must not be collected")
	}
	if cs.wantLabels(map[string]string{"logging": "false", "tier": "web"}) {
		t.Error("a pod with a differing value must not be collected")
	}
	if cs.wantLabels(nil) {
		t.Error("a pod with no labels must not match a non-empty selector")
	}

	// No selector accepts everything, including unlabeled pods.
	none := compileSources([]Source{{Containerd: true}}, "/var/log/containers", true)[0]
	if !none.wantLabels(nil) {
		t.Error("an empty selector must accept every pod")
	}
}

// Kubernetes selector semantics: the key must be PRESENT, not merely compare
// equal. A bare `lbls[k] != v` reads a MISSING key as "", so a selector entry
// with an empty value matched every pod lacking the label entirely — the
// opposite of the intent, and indistinguishable from a pod that genuinely
// carries `key: ""`.
//
// This is the case the suite above did not cover, which is why the matcher
// carried the bug while the metadata service's twin (services.selects) had the
// presence check and a comment explaining it from the start.
func TestSourceSelectorEmptyValueRequiresThePresentLabel(t *testing.T) {
	cs := compileSources([]Source{{
		Containerd: true,
		Selector:   map[string]string{"tenant": ""},
	}}, "/var/log/containers", true)[0]

	if cs.wantLabels(nil) {
		t.Error("a pod with NO labels must not match selector {tenant: \"\"}")
	}
	if cs.wantLabels(map[string]string{"other": "x"}) {
		t.Error("a pod lacking the selector key must not match selector {tenant: \"\"}")
	}
	if !cs.wantLabels(map[string]string{"tenant": ""}) {
		t.Error("a pod that genuinely carries tenant=\"\" must match")
	}
	if cs.wantLabels(map[string]string{"tenant": "acme"}) {
		t.Error("a pod with a different value must not match")
	}

	// The same distinction inside a multi-key selector, where the empty-valued
	// entry is the one that must still be checked for presence.
	both := compileSources([]Source{{
		Containerd: true,
		Selector:   map[string]string{"app": "api", "tenant": ""},
	}}, "/var/log/containers", true)[0]
	if both.wantLabels(map[string]string{"app": "api"}) {
		t.Error("the empty-valued selector key must still require presence")
	}
	if !both.wantLabels(map[string]string{"app": "api", "tenant": ""}) {
		t.Error("both keys present and equal must match")
	}
}

// A malformed namespace pattern must fail startup. path.Match returns
// ErrBadPattern for EVERY input when the pattern is bad, and wantNamespace
// reads that as "no match" — so an unvalidated typo silently collects NOTHING
// for the source, with no warning, no metric and -check-config green.
func TestInvalidNamespacePatternRejected(t *testing.T) {
	for _, s := range []Source{
		{Name: "a", Include: []string{"/var/log/containers/*.log"}, Containerd: true, Namespaces: []string{"prod["}},
		{Name: "b", Include: []string{"/var/log/containers/*.log"}, Containerd: true, ExcludeNamespaces: []string{"kube-["}},
	} {
		if _, err := ValidateSources([]Source{s}); err == nil {
			t.Errorf("source %q: an invalid namespace pattern must be rejected at startup", s.Name)
		}
	}
	// A valid one still passes.
	if _, err := ValidateSources([]Source{{
		Name: "ok", Include: []string{"/var/log/containers/*.log"}, Containerd: true,
		Namespaces: []string{"team-*", "prod"},
	}}); err != nil {
		t.Errorf("valid patterns rejected: %v", err)
	}
}
