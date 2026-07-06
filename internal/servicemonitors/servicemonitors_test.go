package servicemonitors

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
)

func monitorObj(namespace, name string, spec map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "monitoring.coreos.com/v1",
		"kind":       "ServiceMonitor",
		"metadata": map[string]any{
			"namespace": namespace,
			"name":      name,
		},
		"spec": spec,
	}}
}

func TestParse(t *testing.T) {
	u := monitorObj("monitoring", "web", map[string]any{
		"selector": map[string]any{
			"matchLabels": map[string]any{"app": "web"},
		},
		"namespaceSelector": map[string]any{
			"matchNames": []any{"default", "prod"},
		},
		"endpoints": []any{
			map[string]any{"port": "http-metrics", "path": "/stats", "scheme": "https"},
			map[string]any{"targetPort": int64(9090)},
			map[string]any{"targetPort": "metrics"},
		},
	})
	m, err := Parse(u)
	if err != nil {
		t.Fatal(err)
	}
	if m.Namespace != "monitoring" || m.Name != "web" {
		t.Fatalf("monitor = %+v", m)
	}
	if !m.Selector.Matches(labels.Set{"app": "web"}) || m.Selector.Matches(labels.Set{"app": "db"}) {
		t.Errorf("selector = %v", m.Selector)
	}
	if got := m.ServiceNamespaces(); len(got) != 2 || got[0] != "default" || got[1] != "prod" {
		t.Errorf("namespaces = %v", got)
	}
	if len(m.Endpoints) != 3 {
		t.Fatalf("endpoints = %+v", m.Endpoints)
	}
	if ep := m.Endpoints[0]; ep.Port != "http-metrics" || ep.Path != "/stats" || ep.Scheme != "https" || ep.TargetPort != nil {
		t.Errorf("endpoint[0] = %+v", ep)
	}
	if ep := m.Endpoints[1]; ep.TargetPort == nil || ep.TargetPort.IntValue() != 9090 {
		t.Errorf("endpoint[1] = %+v", ep)
	}
	if ep := m.Endpoints[2]; ep.TargetPort == nil || ep.TargetPort.String() != "metrics" {
		t.Errorf("endpoint[2] = %+v", ep)
	}
}

func TestParseNamespaceDefaults(t *testing.T) {
	m, err := Parse(monitorObj("monitoring", "own-ns", map[string]any{
		"selector":  map[string]any{"matchLabels": map[string]any{"a": "b"}},
		"endpoints": []any{map[string]any{"port": "http"}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if got := m.ServiceNamespaces(); len(got) != 1 || got[0] != "monitoring" {
		t.Errorf("default namespaces = %v; want the monitor's own", got)
	}

	m, err = Parse(monitorObj("monitoring", "any-ns", map[string]any{
		"selector":          map[string]any{"matchLabels": map[string]any{"a": "b"}},
		"namespaceSelector": map[string]any{"any": true},
		"endpoints":         []any{map[string]any{"port": "http"}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if got := m.ServiceNamespaces(); got != nil {
		t.Errorf("any namespaces = %v; want nil (all)", got)
	}
}

func TestParseMatchExpressions(t *testing.T) {
	m, err := Parse(monitorObj("ns", "expr", map[string]any{
		"selector": map[string]any{
			"matchExpressions": []any{
				map[string]any{"key": "tier", "operator": "In", "values": []any{"frontend", "backend"}},
			},
		},
		"endpoints": []any{map[string]any{"port": "http"}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !m.Selector.Matches(labels.Set{"tier": "frontend"}) || m.Selector.Matches(labels.Set{"tier": "cache"}) {
		t.Errorf("selector = %v", m.Selector)
	}
}

func TestParseErrors(t *testing.T) {
	if _, err := Parse(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": "ns", "name": "empty"},
	}}); err == nil {
		t.Error("missing spec: want error")
	}
	if _, err := Parse(monitorObj("ns", "bad", map[string]any{
		"selector": map[string]any{
			"matchExpressions": []any{
				map[string]any{"key": "k", "operator": "Bogus"},
			},
		},
	})); err == nil {
		t.Error("invalid selector operator: want error")
	}
}

func TestIndex(t *testing.T) {
	ix := NewIndex()
	if err := ix.Upsert(monitorObj("ns", "a", map[string]any{
		"selector":  map[string]any{"matchLabels": map[string]any{"app": "a"}},
		"endpoints": []any{map[string]any{"port": "http"}},
	})); err != nil {
		t.Fatal(err)
	}
	if err := ix.Upsert(monitorObj("ns", "b", map[string]any{
		"selector":  map[string]any{"matchLabels": map[string]any{"app": "b"}},
		"endpoints": []any{map[string]any{"port": "http"}},
	})); err != nil {
		t.Fatal(err)
	}
	if got := ix.All(); len(got) != 2 {
		t.Fatalf("All() = %d monitors", len(got))
	}

	// Upsert replaces in place.
	if err := ix.Upsert(monitorObj("ns", "a", map[string]any{
		"selector":  map[string]any{"matchLabels": map[string]any{"app": "a2"}},
		"endpoints": []any{map[string]any{"port": "http"}},
	})); err != nil {
		t.Fatal(err)
	}
	if got := ix.All(); len(got) != 2 {
		t.Fatalf("All() after replace = %d monitors", len(got))
	}

	ix.Delete("ns", "a")
	got := ix.All()
	if len(got) != 1 || got[0].Name != "b" {
		t.Fatalf("All() after delete = %+v", got)
	}
}
