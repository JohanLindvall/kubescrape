package servicemonitors

// The PodMonitor half of this package is a parallel implementation of the
// ServiceMonitor half, and every guard added so far went to the ServiceMonitor
// side: the unparseable-update removal, the deterministic ordering and the
// namespace convention were each asserted once, on the copy that did not need
// it twice. These are the mirrors.

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func podMonitorObj(namespace, name string, spec map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "monitoring.coreos.com/v1",
		"kind":       "PodMonitor",
		"metadata":   map[string]any{"namespace": namespace, "name": name},
		"spec":       spec,
	}}
}

// Mirror of TestIndexUnparseableUpdateRemoves. A PodMonitor edited into an
// unparseable spec must be REMOVED, not left serving its previous endpoints —
// and those endpoints carry the bearerTokenSecret refs AuthSecretRefs
// allowlists, so a stale one keeps /v1/scrape-auth willing to serve a Secret
// the live spec no longer names.
func TestPodMonitorUnparseableUpdateRemoves(t *testing.T) {
	ix := NewIndex()
	if err := ix.UpsertPodMonitor(podMonitorObj("ns", "a", map[string]any{
		"selector": map[string]any{"matchLabels": map[string]any{"app": "a"}},
		"podMetricsEndpoints": []any{map[string]any{
			"port":              "metrics",
			"bearerTokenSecret": map[string]any{"name": "tok", "key": "token"},
		}},
	})); err != nil {
		t.Fatal(err)
	}
	if _, ok := ix.AuthSecretRefs()["ns/tok/token"]; !ok {
		t.Fatal("setup: the endpoint's secret ref should be allowlisted")
	}

	if err := ix.UpsertPodMonitor(podMonitorObj("ns", "a", map[string]any{
		"selector": map[string]any{"matchExpressions": []any{
			map[string]any{"key": "app", "operator": "Bogus"},
		}},
	})); err == nil {
		t.Fatal("unparseable update accepted")
	}
	if got := ix.PodMonitors(); len(got) != 0 {
		t.Fatalf("stale podmonitor still served after unparseable update: %d", len(got))
	}
	// The allowlist must shrink with it, or the refused spec keeps a Secret
	// reachable through /v1/scrape-auth.
	if _, ok := ix.AuthSecretRefs()["ns/tok/token"]; ok {
		t.Error("the removed podmonitor's secret ref is still allowlisted")
	}
}

// Mirror of TestAllIsDeterministicallyOrdered. When two PodMonitors select the
// same pod and mint the same URL, handleNodeTargets' URL dedup keeps the FIRST
// — so map-iteration order must not decide which monitor's name, auth and
// relabelings ride on the surviving target. The PodMonitors() copy carries the
// fullest comment about that and had no test.
func TestPodMonitorsAreDeterministicallyOrdered(t *testing.T) {
	ix := NewIndex()
	for _, spec := range []struct{ ns, name string }{
		{"zeta", "b"}, {"alpha", "b"}, {"alpha", "a"}, {"mid", "z"},
	} {
		if err := ix.UpsertPodMonitor(podMonitorObj(spec.ns, spec.name, map[string]any{
			"selector":            map[string]any{},
			"podMetricsEndpoints": []any{map[string]any{"port": "metrics"}},
		})); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"alpha/a", "alpha/b", "mid/z", "zeta/b"}
	// Repeat: one pass could agree with map order by luck.
	for range 20 {
		got := make([]string, 0, len(want))
		for _, m := range ix.PodMonitors() {
			got = append(got, m.Namespace+"/"+m.Name)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("PodMonitors() = %v, want %v", got, want)
			}
		}
	}
}

// Mirror of the ServiceMonitor namespace-selector coverage. `any: true` is
// exactly what a fleet-wide PodMonitor sets, and a regression there fails
// CLOSED and silently — no error, no metric, just pods that stop being scraped.
func TestPodMonitorNamespaceSelector(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec map[string]any
		want []string // nil means "every namespace"
	}{
		{
			name: "default is the monitor's own namespace",
			spec: map[string]any{"selector": map[string]any{}},
			want: []string{"mon"},
		},
		{
			name: "any selects every namespace",
			spec: map[string]any{
				"selector":          map[string]any{},
				"namespaceSelector": map[string]any{"any": true},
			},
			want: nil,
		},
		{
			name: "matchNames restricts to the listed set",
			spec: map[string]any{
				"selector":          map[string]any{},
				"namespaceSelector": map[string]any{"matchNames": []any{"prod", "staging"}},
			},
			want: []string{"prod", "staging"},
		},
		{
			// any WINS over matchNames, matching the ServiceMonitor side.
			name: "any beats matchNames",
			spec: map[string]any{
				"selector": map[string]any{},
				"namespaceSelector": map[string]any{
					"any": true, "matchNames": []any{"prod"},
				},
			},
			want: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, err := ParsePodMonitor(podMonitorObj("mon", "pm", tc.spec))
			if err != nil {
				t.Fatal(err)
			}
			got := m.PodNamespaces()
			if tc.want == nil {
				if got != nil {
					t.Fatalf("PodNamespaces() = %v, want nil (every namespace)", got)
				}
				return
			}
			if len(got) != len(tc.want) {
				t.Fatalf("PodNamespaces() = %v, want %v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("PodNamespaces() = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// The two kinds must answer the namespace question IDENTICALLY — the two
// methods are byte-identical today and nothing pins that they stay so.
func TestBothMonitorKindsAgreeOnNamespaces(t *testing.T) {
	for _, tc := range []struct {
		name string
		ns   map[string]any
	}{
		{"unset", nil},
		{"any", map[string]any{"any": true}},
		{"matchNames", map[string]any{"matchNames": []any{"a", "b"}}},
		{"both", map[string]any{"any": true, "matchNames": []any{"a"}}},
		{"empty matchNames", map[string]any{"matchNames": []any{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			smSpec := map[string]any{"selector": map[string]any{}, "endpoints": []any{}}
			pmSpec := map[string]any{"selector": map[string]any{}, "podMetricsEndpoints": []any{}}
			if tc.ns != nil {
				smSpec["namespaceSelector"] = tc.ns
				pmSpec["namespaceSelector"] = tc.ns
			}
			sm, err := Parse(monitorObj("mon", "m", smSpec))
			if err != nil {
				t.Fatal(err)
			}
			pm, err := ParsePodMonitor(podMonitorObj("mon", "m", pmSpec))
			if err != nil {
				t.Fatal(err)
			}
			svcNS, podNS := sm.ServiceNamespaces(), pm.PodNamespaces()
			if (svcNS == nil) != (podNS == nil) || len(svcNS) != len(podNS) {
				t.Fatalf("the two kinds disagree: ServiceNamespaces()=%v PodNamespaces()=%v", svcNS, podNS)
			}
			for i := range svcNS {
				if svcNS[i] != podNS[i] {
					t.Fatalf("the two kinds disagree: %v vs %v", svcNS, podNS)
				}
			}
		})
	}
}
