package main

import (
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/agent/tailer"
)

// `namespaces`, `excludeNamespaces` and `selector` select by POD identity, and a
// plain source has no pods: the namespace filters read the CRI FILENAME at
// discovery and the selector reads pod labels at resolve time, neither of which
// a plain file has. So on a plain source all three do NOTHING — every matched
// file collected, -check-config green, and the operator's only evidence that the
// filter worked is the absence of an error.
//
// Named rather than refused, and the second half of this test is the load-bearing
// half: ONE -config is shared by the DaemonSet, the events/Azure singleton and
// the trace tier, and refusing here aborted the two workloads that never tail a
// file at all — over a key that is inert in their config by construction.
func TestPlainSourcePodSelectionKeysWarn(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  tailer.Source
	}{
		{"namespaces", tailer.Source{Name: "apps", Include: []string{"/var/log/apps/**/*.log"}, Namespaces: []string{"prod"}}},
		{"excludeNamespaces", tailer.Source{Name: "apps", Include: []string{"/var/log/apps/**/*.log"}, ExcludeNamespaces: []string{"team-scratch"}}},
		{"selector", tailer.Source{Name: "apps", Include: []string{"/var/log/apps/**/*.log"}, Selector: map[string]string{"logging": "true"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := agentConfig{Logs: &tailer.SourcesConfig{Sources: []tailer.Source{tc.src}}}

			got := warnText(cfg)
			if !strings.Contains(got, tc.name) {
				t.Fatalf("the ignored key is not named, so nothing tells the operator the filter is inert: %q", got)
			}
			if !strings.Contains(got, "IGNORED") || !strings.Contains(got, "containerd: true") {
				t.Fatalf("the warning must say the key does nothing and what to do instead: %q", got)
			}
			if !strings.Contains(got, `"apps"`) {
				t.Fatalf("the warning must name the source it is about: %q", got)
			}

			// The load-bearing half. A shared ConfigMap must stay startable by
			// every workload, including the two that never tail a file, so an
			// inert key may not fail validation on any of them.
			if err := validateConfig(cfg, ""); err != nil {
				t.Fatalf("an inert key refused startup for every workload sharing this ConfigMap: %v", err)
			}
			if _, err := compileSources(cfg.Logs); err != nil {
				t.Fatalf("a real start refused an inert key: %v", err)
			}

			// The same keys ON a containerd source are the whole point of them,
			// and must not warn either.
			ok := tc.src
			ok.Containerd = true
			okCfg := agentConfig{Logs: &tailer.SourcesConfig{Sources: []tailer.Source{ok}}}
			if err := validateConfig(okCfg, ""); err != nil {
				t.Fatalf("rejected %s on a containerd source, where it is honoured: %v", tc.name, err)
			}
			if w := warnText(okCfg); w != "" {
				t.Fatalf("warned about %s where it is honoured: %q", tc.name, w)
			}
		})
	}

	// A plain source that asks for nothing pod-shaped is silent — this must not
	// become "plain sources are second class".
	plain := agentConfig{Logs: &tailer.SourcesConfig{Sources: []tailer.Source{{
		Name: "apps", Include: []string{"/var/log/apps/**/*.log"},
		Exclude:    []string{"/var/log/apps/debug/*.log"},
		Attributes: map[string]string{"service.name": "apps"}, IgnoreOlder: "24h",
	}}}}
	if err := validateConfig(plain, ""); err != nil {
		t.Fatalf("rejected an ordinary plain source: %v", err)
	}
	if w := warnText(plain); w != "" {
		t.Fatalf("warned about an ordinary plain source: %q", w)
	}
}
