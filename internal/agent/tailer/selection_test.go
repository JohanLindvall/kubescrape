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
