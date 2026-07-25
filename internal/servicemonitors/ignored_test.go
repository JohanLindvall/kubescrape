package servicemonitors

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// kubescrape implements a documented SUBSET of the monitor spec. That is fine;
// applying part of a user's CR without saying so is not — they would see
// targets appear and never learn their relabelings or sampleLimit did nothing.
func TestIgnoredFieldsReported(t *testing.T) {
	u := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "m1", "namespace": "default"},
		"spec": map[string]any{
			"selector": map[string]any{"matchLabels": map[string]any{"app": "web"}},
			"endpoints": []any{map[string]any{
				"port":     "metrics",
				"interval": "10s",
				"basicAuth": map[string]any{
					"username": map[string]any{"name": "s", "key": "u"},
				},
				"proxyUrl":    "http://proxy:3128",
				"honorLabels": true,
				"relabelings": []any{map[string]any{"action": "replace"}},
				"tlsConfig": map[string]any{
					"serverName": "svc.local",
					"ca":         map[string]any{"secret": map[string]any{"name": "ca", "key": "ca.crt"}},
				},
				"metricRelabelings": []any{
					map[string]any{"action": "drop", "sourceLabels": []any{"__name__"}, "regex": "x"},
					map[string]any{"action": "labelmap", "regex": "y"},
				},
			}},
		},
	}}
	m, err := Parse(u)
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(IgnoredFields(m.Endpoints), ",")
	for _, want := range []string{
		"basicAuth", "proxyUrl", "honorLabels", "relabelings",
		"tlsConfig.serverName", "tlsConfig.ca", "metricRelabelings.action=labelmap",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("ignored fields %q missing %q", got, want)
		}
	}
	// Interpreted fields must NOT be reported as ignored.
	for _, unwanted := range []string{"port", "interval", "metricRelabelings.action=drop"} {
		if strings.Contains(got, unwanted) {
			t.Errorf("ignored fields %q wrongly includes the interpreted %q", got, unwanted)
		}
	}
	// And the interval is actually carried.
	if m.Endpoints[0].Interval != "10s" {
		t.Errorf("endpoint interval = %q, want 10s", m.Endpoints[0].Interval)
	}
}

// A monitor using only interpreted fields reports nothing.
func TestNoIgnoredFieldsWhenFullySupported(t *testing.T) {
	u := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "m2", "namespace": "default"},
		"spec": map[string]any{
			"selector":  map[string]any{"matchLabels": map[string]any{"app": "web"}},
			"endpoints": []any{map[string]any{"port": "metrics", "path": "/m", "scheme": "https"}},
		},
	}}
	m, err := Parse(u)
	if err != nil {
		t.Fatal(err)
	}
	if got := IgnoredFields(m.Endpoints); len(got) != 0 {
		t.Fatalf("ignored = %v, want none", got)
	}
}
