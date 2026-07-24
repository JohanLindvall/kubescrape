package servicemonitors

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func unstr(kind string, spec map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "monitoring.coreos.com/v1",
		"kind":       kind,
		"metadata":   map[string]any{"name": "m1", "namespace": "mon"},
		"spec":       spec,
	}}
}

func TestParsePodMonitor(t *testing.T) {
	u := unstr("PodMonitor", map[string]any{
		"selector": map[string]any{"matchLabels": map[string]any{"app": "x"}},
		"podMetricsEndpoints": []any{map[string]any{
			"port": "metrics", "path": "/m", "scheme": "https",
			"tlsConfig":         map[string]any{"insecureSkipVerify": true},
			"bearerTokenSecret": map[string]any{"name": "tok", "key": "token"},
			"metricRelabelings": []any{
				map[string]any{"action": "drop", "sourceLabels": []any{"__name__"}, "regex": "go_.*"},
				map[string]any{"action": "replace", "regex": "ignored"}, // unsupported action: skipped
			},
		}},
	})
	m, err := ParsePodMonitor(u)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Endpoints) != 1 {
		t.Fatalf("endpoints: %+v", m.Endpoints)
	}
	ep := m.Endpoints[0]
	if ep.Port != "metrics" || !ep.InsecureSkipVerify || ep.BearerSecret != "mon/tok/token" {
		t.Fatalf("endpoint: %+v", ep)
	}
	if len(ep.MetricRelabelings) != 1 || ep.MetricRelabelings[0].Action != "drop" {
		t.Fatalf("relabelings (replace must be skipped): %+v", ep.MetricRelabelings)
	}
	if nss := m.PodNamespaces(); len(nss) != 1 || nss[0] != "mon" {
		t.Fatalf("namespaces default to the monitor's own: %v", nss)
	}
}

func TestIndexPodMonitorLifecycle(t *testing.T) {
	x := NewIndex()
	u := unstr("PodMonitor", map[string]any{
		"selector":            map[string]any{},
		"podMetricsEndpoints": []any{map[string]any{"port": "m"}},
	})
	if err := x.UpsertPodMonitor(u); err != nil {
		t.Fatal(err)
	}
	if len(x.PodMonitors()) != 1 {
		t.Fatal("not indexed")
	}
	x.DeletePodMonitor("mon", "m1")
	if len(x.PodMonitors()) != 0 {
		t.Fatal("not deleted")
	}
}
