package servicemonitors

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func unstr(kind string, spec map[string]any) *unstructured.Unstructured {
	return crObject(kind, "mon", "m1", "", spec)
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
	if ep.Port != "metrics" || !ep.InsecureSkipVerify || ep.AuthSecret != "mon/tok/token" {
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

// `scheme: HTTPS` is CRD-valid on both kinds (the enum is
// http;https;HTTP;HTTPS) and the upper-case spelling is the one the CRD's own
// documentation shows. Carried verbatim it fell through scrape's `!= "https"`
// default to plain http, and the agent attaches the endpoint's credential
// without looking at the scheme — so the bearer token crossed the pod network in
// cleartext with the tlsConfig unused. The parse door folds it for both kinds.
//
// Reverse-patch check: carrying ep.Scheme verbatim in toEndpoint fails both
// arms.
func TestUpperCaseSchemeIsFoldedAtTheParseDoor(t *testing.T) {
	ep := map[string]any{
		"port": "metrics", "scheme": "HTTPS",
		"bearerTokenSecret": map[string]any{"name": "tok", "key": "token"},
	}
	sm, err := Parse(unstr("ServiceMonitor", map[string]any{"selector": map[string]any{}, "endpoints": []any{ep}}))
	if err != nil {
		t.Fatal(err)
	}
	pm, err := ParsePodMonitor(unstr("PodMonitor", map[string]any{"selector": map[string]any{}, "podMetricsEndpoints": []any{ep}}))
	if err != nil {
		t.Fatal(err)
	}
	for kind, got := range map[string]Endpoint{"ServiceMonitor": sm.Endpoints[0], "PodMonitor": pm.Endpoints[0]} {
		if got.Scheme != "https" {
			t.Errorf("%s: scheme HTTPS parsed as %q; the credential %q would be sent over plain http",
				kind, got.Scheme, got.AuthSecret)
		}
	}
	// Anything else is carried verbatim (and scraped over http, as before):
	// only the two recognised spellings are folded.
	if got := canonicalScheme("Http"); got != "http" {
		t.Errorf("canonicalScheme(Http) = %q", got)
	}
	if got := canonicalScheme("HTTPSX"); got != "HTTPSX" {
		t.Errorf("an unrecognised scheme was rewritten: %q", got)
	}
}
