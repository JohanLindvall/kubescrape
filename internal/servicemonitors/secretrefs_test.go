package servicemonitors

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// everySecretField is a CR endpoint body naming EVERY secret-bearing field a
// monitor endpoint can carry, each with a distinct secret name so a missing one
// is identifiable rather than merely absent.
func everySecretField() map[string]any {
	return map[string]any{
		"port":              "metrics",
		"bearerTokenSecret": map[string]any{"name": "bearer", "key": "token"},
		"basicAuth": map[string]any{
			"username": map[string]any{"name": "basic", "key": "user"},
			"password": map[string]any{"name": "basic", "key": "pass"},
		},
		"authorization": map[string]any{
			"type":        "Bearer",
			"credentials": map[string]any{"name": "authz", "key": "creds"},
		},
		"tlsConfig": map[string]any{
			"ca":        map[string]any{"secret": map[string]any{"name": "tls", "key": "ca.crt"}},
			"cert":      map[string]any{"secret": map[string]any{"name": "tls", "key": "tls.crt"}},
			"keySecret": map[string]any{"name": "tls", "key": "tls.key"},
		},
	}
}

func serviceMonitorCR(ns, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": name, "namespace": ns},
		"spec": map[string]any{
			"selector":  map[string]any{"matchLabels": map[string]any{"app": "x"}},
			"endpoints": []any{everySecretField()},
		},
	}}
}

func podMonitorCR(ns, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": name, "namespace": ns},
		"spec": map[string]any{
			"selector":            map[string]any{"matchLabels": map[string]any{"app": "x"}},
			"podMetricsEndpoints": []any{everySecretField()},
		},
	}}
}

// The three places that used to write out the secret-bearing field list by hand
// — the ServiceMonitor parser's namespacing loop, the PodMonitor parser's
// verbatim copy of it, and AuthSecretRefs' harvesting loop — must agree, field
// for field. They do so now by construction (Endpoint.secretRefs), and this is
// what fails if anyone re-opens a hand-written list:
//
//   - a ref NAMESPACED but not ALLOWLISTED makes /v1/scrape-auth 404 it;
//   - a ref ALLOWLISTED but not NAMESPACED can never match an allowlist entry
//     (the entries are "ns/name/key", the ref would be "name/key").
//
// Either way the target scrapes unauthenticated and the only signal is up=0.
func TestEverySecretRefIsNamespacedAndAllowlisted(t *testing.T) {
	for _, tc := range []struct {
		kind   string
		upsert func(*Index, *unstructured.Unstructured) error
		cr     *unstructured.Unstructured
		eps    func(*Index) []Endpoint
	}{
		{
			kind:   "ServiceMonitor",
			upsert: (*Index).Upsert,
			cr:     serviceMonitorCR("monitoring", "sm"),
			eps:    func(ix *Index) []Endpoint { return ix.Endpoints("monitoring", "sm") },
		},
		{
			kind:   "PodMonitor",
			upsert: (*Index).UpsertPodMonitor,
			cr:     podMonitorCR("monitoring", "pm"),
			eps:    func(ix *Index) []Endpoint { return ix.PodMonitors()[0].Endpoints },
		},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			ix := NewIndex()
			if err := tc.upsert(ix, tc.cr); err != nil {
				t.Fatal(err)
			}
			eps := tc.eps(ix)
			if len(eps) != 1 {
				t.Fatalf("endpoints = %d, want 1", len(eps))
			}
			ep := eps[0]
			refs := ix.AuthSecretRefs()

			set := 0
			for _, p := range ep.secretRefs() {
				if *p == "" {
					continue
				}
				set++
				// Namespaced with the MONITOR's namespace: that prefix is what
				// confines a monitor to secrets in its own namespace, and what
				// makes the allowlist entry matchable at all.
				if !strings.HasPrefix(*p, "monitoring/") {
					t.Errorf("%s ref %q is not namespaced; it can never match an allowlist entry", tc.kind, *p)
				}
				if strings.Count(*p, "/") != 2 {
					t.Errorf("%s ref %q is not ns/name/key", tc.kind, *p)
				}
				if _, ok := refs[*p]; !ok {
					t.Errorf("%s ref %q is carried on the endpoint but NOT allowlisted: /v1/scrape-auth would 404 it and the target scrapes unauthenticated", tc.kind, *p)
				}
			}
			// Every field the shared list names must have been populated by the
			// CR above; a new secret-bearing field with no fixture here would
			// otherwise pass this test vacuously.
			if set != len(ep.secretRefs()) {
				t.Errorf("%s populated %d of %d secret fields: everySecretField() must name every one, or a new field goes untested",
					tc.kind, set, len(ep.secretRefs()))
			}
			if set == 0 {
				t.Fatalf("%s parsed no secret refs at all", tc.kind)
			}
		})
	}
}

// AuthSecretRefs must read the STORED endpoints' fields, not a loop variable's
// copy of them. secretRefs returns pointers, so ranging by value would take the
// addresses of a per-iteration copy — harmless for reading today, and exactly
// the kind of aliasing bug that turns into "the allowlist is empty" the moment
// someone writes through the same accessor.
func TestAuthSecretRefsReadsTheStoredEndpoints(t *testing.T) {
	ix := NewIndex()
	if err := ix.Upsert(serviceMonitorCR("monitoring", "sm")); err != nil {
		t.Fatal(err)
	}
	refs := ix.AuthSecretRefs()
	for _, want := range []string{
		"monitoring/bearer/token",
		"monitoring/basic/user", "monitoring/basic/pass",
		"monitoring/authz/creds",
		"monitoring/tls/ca.crt", "monitoring/tls/tls.crt", "monitoring/tls/tls.key",
	} {
		if _, ok := refs[want]; !ok {
			t.Errorf("%q missing from the allowlist", want)
		}
	}
	if len(refs) != 7 {
		t.Errorf("allowlist has %d entries, want 7 (%v)", len(refs), refs)
	}
}
