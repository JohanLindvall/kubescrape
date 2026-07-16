package servicemonitors

import (
	"slices"
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

// A monitor UPDATED to an unparseable spec must stop being served — silently
// keeping the previous version forever would diverge from the manifest
// (prometheus-operator generates no config for an invalid monitor).
func TestIndexUnparseableUpdateRemoves(t *testing.T) {
	ix := NewIndex()
	if err := ix.Upsert(monitorObj("ns", "a", map[string]any{
		"selector":  map[string]any{"matchLabels": map[string]any{"app": "a"}},
		"endpoints": []any{map[string]any{"port": "http"}},
	})); err != nil {
		t.Fatal(err)
	}
	if err := ix.Upsert(monitorObj("ns", "a", map[string]any{
		"selector": map[string]any{"matchExpressions": []any{
			map[string]any{"key": "app", "operator": "Bogus"},
		}},
	})); err == nil {
		t.Fatal("unparseable update accepted")
	}
	if got := ix.All(); len(got) != 0 {
		t.Fatalf("stale monitor still served after unparseable update: %d", len(got))
	}
}

// All must return monitors in namespace/name order: map iteration order must
// not decide which monitor a URL-deduped target is attributed to, or the kept
// target's Monitor field would flip between cache rebuilds.
func TestAllIsDeterministicallyOrdered(t *testing.T) {
	ix := NewIndex()
	spec := map[string]any{
		"selector":  map[string]any{"matchLabels": map[string]any{"app": "web"}},
		"endpoints": []any{map[string]any{"port": "metrics"}},
	}
	for _, nn := range [][2]string{{"zz", "m1"}, {"aa", "m2"}, {"aa", "m1"}, {"mm", "x"}} {
		if err := ix.Upsert(monitorObj(nn[0], nn[1], spec)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 20; i++ { // map order varies per run; make one run enough
		got := ix.All()
		var keys []string
		for _, m := range got {
			keys = append(keys, m.Namespace+"/"+m.Name)
		}
		want := []string{"aa/m1", "aa/m2", "mm/x", "zz/m1"}
		if !slices.Equal(keys, want) {
			t.Fatalf("All() order = %v, want %v", keys, want)
		}
	}
}
