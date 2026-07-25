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
				"oauth2":   map[string]any{"clientId": map[string]any{"secret": map[string]any{"name": "s", "key": "id"}}},
				"basicAuth": map[string]any{
					"username": map[string]any{"name": "s", "key": "u"},
					"password": map[string]any{"name": "s", "key": "p"},
				},
				"proxyUrl":    "http://proxy:3128",
				"honorLabels": true,
				"relabelings": []any{map[string]any{"action": "replace"}},
				"tlsConfig": map[string]any{
					"serverName": "svc.local",
					// A configMap-backed CA is NOT resolvable (the agent reads
					// secret keys only), so it must be reported.
					"ca": map[string]any{"configMap": map[string]any{"name": "ca", "key": "ca.crt"}},
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
		"oauth2", "proxyUrl", "honorLabels", "relabelings",
		"tlsConfig.ca.configMap", "metricRelabelings.action=labelmap",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("ignored fields %q missing %q", got, want)
		}
	}
	// Interpreted fields must NOT be reported as ignored — basicAuth, a
	// secret-backed CA/cert and serverName are all honoured now.
	for _, unwanted := range []string{
		"port", "interval", "metricRelabelings.action=drop",
		"basicAuth", "tlsConfig.serverName", "authorization",
	} {
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

// The auth material an endpoint references must be carried onto the endpoint
// AND authorized for serving: /v1/scrape-auth refuses any ref no monitor names,
// which is what keeps -scrape-auth-secrets from being a general secret-read API.
func TestAuthMaterialParsedAndAuthorized(t *testing.T) {
	u := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "etcd", "namespace": "monitoring"},
		"spec": map[string]any{
			"selector": map[string]any{"matchLabels": map[string]any{"app": "etcd"}},
			"endpoints": []any{map[string]any{
				"port": "metrics",
				"basicAuth": map[string]any{
					"username": map[string]any{"name": "creds", "key": "user"},
					"password": map[string]any{"name": "creds", "key": "pass"},
				},
				"tlsConfig": map[string]any{
					"ca":         map[string]any{"secret": map[string]any{"name": "etcd-tls", "key": "ca.crt"}},
					"cert":       map[string]any{"secret": map[string]any{"name": "etcd-tls", "key": "tls.crt"}},
					"keySecret":  map[string]any{"name": "etcd-tls", "key": "tls.key"},
					"serverName": "etcd.local",
				},
			}},
		},
	}}
	ix := NewIndex()
	if err := ix.Upsert(u); err != nil {
		t.Fatal(err)
	}
	ep := ix.Endpoints("monitoring", "etcd")[0]
	for _, tc := range []struct{ got, want string }{
		{ep.BasicAuthUser, "monitoring/creds/user"},
		{ep.BasicAuthPass, "monitoring/creds/pass"},
		{ep.TLSCA, "monitoring/etcd-tls/ca.crt"},
		{ep.TLSCert, "monitoring/etcd-tls/tls.crt"},
		{ep.TLSKey, "monitoring/etcd-tls/tls.key"},
		{ep.TLSServerName, "etcd.local"},
	} {
		if tc.got != tc.want {
			t.Errorf("ref = %q, want %q", tc.got, tc.want)
		}
	}
	refs := ix.AuthSecretRefs()
	for _, want := range []string{
		"monitoring/creds/user", "monitoring/creds/pass",
		"monitoring/etcd-tls/ca.crt", "monitoring/etcd-tls/tls.crt", "monitoring/etcd-tls/tls.key",
	} {
		if _, ok := refs[want]; !ok {
			t.Errorf("%q not authorized for serving; the agent could not fetch it", want)
		}
	}
}
