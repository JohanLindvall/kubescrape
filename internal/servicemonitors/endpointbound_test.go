package servicemonitors

import (
	"encoding/json"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// monitorWithEndpoint builds a ServiceMonitor around one endpoint declaration.
func monitorWithEndpoint(t *testing.T, ep map[string]any) *Monitor {
	t.Helper()
	m, err := Parse(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "bomb", "namespace": "tenant"},
		"spec": map[string]any{
			"selector":          map[string]any{},
			"namespaceSelector": map[string]any{"any": true},
			"endpoints":         []any{ep},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// THE ATTACK, and the one the measurement was taken from: a tenant with edit
// rights in ONE namespace creates a single cluster-wide ServiceMonitor
// (`selector: {}` + `namespaceSelector.any: true`, which the default
// -monitor-namespaces honours) whose endpoint carries a 1 MiB `path` and no
// metricRelabelings at all — so neither relabel ceiling is approached and the
// CR is well inside etcd's object limit. The path is copied into BOTH t.URL and
// t.Path of every target the monitor resolves to; before the bound, ONE such
// endpoint yielded ONE target of 2,097,625 bytes, embedded once per matched pod
// in a document re-derived and re-marshalled on every agent poll.
//
// Reverse-patch check: dropping the enforceFieldBounds call from toEndpoint
// restores Path (1,048,577 bytes retained on the endpoint) and this fails.
func TestOversizePathIsRefusedAtTheParseDoor(t *testing.T) {
	m := monitorWithEndpoint(t, map[string]any{
		"port": "http", "path": "/" + strings.Repeat("a", 1<<20),
	})
	ep := m.Endpoints[0]
	if ep.Refused == "" {
		t.Fatalf("a 1 MiB path was accepted; Refused is empty")
	}
	if ep.Path != "" {
		t.Errorf("the refused path is retained on the endpoint (%d bytes): a refused value must not be kept", len(ep.Path))
	}
	if !slices.Contains(ep.Ignored, "path"+oversizeSuffix) {
		t.Errorf("the refusal is not reported through Ignored (so it never reaches "+
			"kubescrape_monitor_fields_ignored_total or the per-upsert warning): %v", ep.Ignored)
	}
	// The whole endpoint is unresolvable, belt and braces: a caller that has
	// not learned to read Refused must not fall back to scraping the DEFAULT
	// path — where the pod very often already has a target this endpoint's
	// rules and credentials would then merge into.
	if ep.Port != "" || ep.TargetPort != nil {
		t.Errorf("a refused endpoint still names a port (%q/%v): it must resolve to nothing", ep.Port, ep.TargetPort)
	}
	if b, err := json.Marshal(ep); err != nil {
		t.Fatal(err)
	} else if len(b) > 4<<10 {
		t.Errorf("the refused endpoint still marshals to %d bytes", len(b))
	}
}

// Every bounded field, at its own door, through the real CRD shape. The [high]
// finding's whole point is that closing one multiplier leaves its identical
// siblings open, so the test is the family and not the one member that was
// measured.
func TestEveryOversizeEndpointStringRefusesTheEndpoint(t *testing.T) {
	big := strings.Repeat("a", 1<<20)
	for _, tc := range []struct {
		field string
		ep    map[string]any
	}{
		{"path", map[string]any{"path": "/" + big}},
		{"interval", map[string]any{"interval": big}},
		{"scrapeTimeout", map[string]any{"scrapeTimeout": big}},
		{"tlsConfig.serverName", map[string]any{"tlsConfig": map[string]any{"serverName": big}}},
		{"authorization.type", map[string]any{"authorization": map[string]any{"type": big}}},
		{"authorization.credentials", map[string]any{"authorization": map[string]any{
			"credentials": map[string]any{"name": big, "key": "k"}}}},
		{"basicAuth.username", map[string]any{"basicAuth": map[string]any{
			"username": map[string]any{"name": big, "key": "k"}}}},
		{"basicAuth.password", map[string]any{"basicAuth": map[string]any{
			"password": map[string]any{"name": big, "key": "k"}}}},
		{"bearerTokenSecret", map[string]any{"bearerTokenSecret": map[string]any{"name": big, "key": "k"}}},
		{"tlsConfig.ca", map[string]any{"tlsConfig": map[string]any{
			"ca": map[string]any{"secret": map[string]any{"name": big, "key": "k"}}}}},
		{"tlsConfig.cert", map[string]any{"tlsConfig": map[string]any{
			"cert": map[string]any{"secret": map[string]any{"name": big, "key": "k"}}}}},
		{"tlsConfig.keySecret", map[string]any{"tlsConfig": map[string]any{
			"keySecret": map[string]any{"name": big, "key": "k"}}}},
	} {
		t.Run(tc.field, func(t *testing.T) {
			ep := tc.ep
			ep["port"] = "http"
			e := monitorWithEndpoint(t, ep).Endpoints[0]
			if !strings.Contains(e.Refused, tc.field) {
				t.Fatalf("a 1 MiB %s was accepted: Refused=%q", tc.field, e.Refused)
			}
			if !slices.Contains(e.Ignored, tc.field+oversizeSuffix) {
				t.Errorf("refusal of %s is not reported through Ignored: %v", tc.field, e.Ignored)
			}
			// Nothing the tenant wrote is retained, on ANY field: the refusal
			// clears the whole group so the index holds none of it.
			for _, f := range e.boundedFields() {
				if len(*f.value) > 0 {
					t.Errorf("%s is still retained (%d bytes) on a refused endpoint", f.name, len(*f.value))
				}
			}
		})
	}
}

// The structural half, the merge_guard_test.go move: every tenant-supplied
// STRING of Endpoint that scrape stamps onto a target must have a door in
// boundedFields. A new one that is forgotten is unbounded and fails nowhere
// until somebody sends a megabyte through it — which is exactly how `path`
// came to be unbounded beside a chain that was not.
func TestEveryTenantSuppliedEndpointStringIsBounded(t *testing.T) {
	// The exemptions, each with the reason it cannot carry size to a target.
	exempt := map[string]string{
		"Port":    "a resolution INPUT: the target carries the resolved port NUMBER, and an absurd name resolves to nothing",
		"Scheme":  "normalised by scrape.defaultSchemePath to one of two constants before it reaches a target",
		"Refused": "this mechanism's own verdict, built here from a fixed list of field names — never tenant text",
	}
	var e Endpoint
	bounded := map[uintptr]string{}
	for _, f := range e.boundedFields() {
		bounded[reflect.ValueOf(f.value).Pointer()] = f.name
	}
	v := reflect.ValueOf(&e).Elem()
	typ := v.Type()
	for i := range typ.NumField() {
		f := typ.Field(i)
		if f.Type.Kind() != reflect.String {
			continue
		}
		if _, ok := exempt[f.Name]; ok {
			continue
		}
		if _, ok := bounded[v.Field(i).Addr().Pointer()]; !ok {
			t.Errorf("Endpoint.%s is tenant-supplied and stamped onto every target the endpoint resolves to, "+
				"but boundedFields does not hold it to a ceiling: add it there (or to this test's exempt list "+
				"with the reason its size cannot reach a target)", f.Name)
		}
	}
	for name := range exempt {
		if _, ok := typ.FieldByName(name); !ok {
			t.Errorf("exempt names %q, which is no longer an Endpoint field", name)
		}
	}
}

// An ordinary monitor is untouched: the ceilings are far above every legitimate
// value, and a bound that refused real configuration would be a worse outage
// than the one it prevents.
func TestOrdinaryEndpointStringsAreNotRefused(t *testing.T) {
	m := monitorWithEndpoint(t, map[string]any{
		"port": "http", "path": "/actuator/prometheus?full=true",
		"interval": "30s", "scrapeTimeout": "10s",
		"tlsConfig": map[string]any{
			"serverName": strings.Repeat("a.", 100) + "svc.cluster.local", // 217 bytes
			"ca":         map[string]any{"secret": map[string]any{"name": "ca", "key": "ca.crt"}},
		},
		"authorization": map[string]any{"type": "Bearer",
			"credentials": map[string]any{"name": "creds", "key": "token"}},
	})
	ep := m.Endpoints[0]
	if ep.Refused != "" {
		t.Fatalf("an ordinary endpoint was refused: %q (ignored=%v)", ep.Refused, ep.Ignored)
	}
	if ep.Path != "/actuator/prometheus?full=true" || ep.TLSCA != "tenant/ca/ca.crt" {
		t.Errorf("ordinary endpoint mangled: %+v", ep)
	}
}

// The LIST is bounded too, and it is the dimension every other ceiling in this
// package was measured underneath: each endpoint STRING is bounded, each
// relabel chain is bounded, each report is bounded — and 100,000 minimal
// endpoints in one 1.3 MB CR (inside etcd's object limit, writable by any
// namespace-scoped tenant with the default -monitor-namespaces) still retained
// ~38 MB in the singleton for the life of the CR, multiplied into one
// monitor→services memo entry per (matched Service, endpoint), and were walked
// per pod per Service on every node-targets derivation — 1.69 s for ONE 110-pod
// node at a tenth of that scale. The response stays small, so nothing
// downstream could report it: the symptoms are the singleton's RSS and every
// agent's targets poll timing out.
func TestAMonitorsEndpointListIsBoundedAtTheParseDoor(t *testing.T) {
	eps := make([]any, 0, maxEndpointsPerMonitor+500)
	for i := range maxEndpointsPerMonitor + 500 {
		eps = append(eps, map[string]any{"port": "p" + strconv.Itoa(i)})
	}
	m, err := Parse(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "bomb", "namespace": "tenant"},
		"spec": map[string]any{
			"selector":          map[string]any{},
			"namespaceSelector": map[string]any{"any": true},
			"endpoints":         eps,
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Endpoints) != maxEndpointsPerMonitor {
		t.Fatalf("kept %d endpoints, want the %d-endpoint ceiling", len(m.Endpoints), maxEndpointsPerMonitor)
	}
	// The PREFIX, in the operator's own order — like the aggregate relabel
	// ceilings, and for the same reason: refusing the CR would take every
	// target its earlier endpoints contribute with it.
	if m.Endpoints[0].Port != "p0" || m.Endpoints[maxEndpointsPerMonitor-1].Port != "p"+strconv.Itoa(maxEndpointsPerMonitor-1) {
		t.Errorf("the kept endpoints are not the head of the list: first=%q last=%q",
			m.Endpoints[0].Port, m.Endpoints[maxEndpointsPerMonitor-1].Port)
	}
	// Reported, or the refusal is exactly the silent partial application the
	// Ignored machinery exists to prevent: it must reach
	// kubescrape_monitor_fields_ignored_total and the per-upsert warning.
	ig := IgnoredFields(m.Endpoints)
	if !slices.Contains(ig, "endpoints"+cappedSuffix) {
		t.Errorf("the refusal is not reported: IgnoredFields = %v", ig)
	}
	// Once, however many endpoints carry it.
	n := 0
	for _, f := range ig {
		if f == "endpoints"+cappedSuffix {
			n++
		}
	}
	if n != 1 {
		t.Errorf("the refusal is reported %d times in %v, want once", n, ig)
	}
}

// The PodMonitor arm shares the skeleton, so it shares the bound — and reports
// it under the CRD's own name for the list, which is not the ServiceMonitor's.
func TestAPodMonitorsEndpointListIsBoundedToo(t *testing.T) {
	eps := make([]any, 0, maxEndpointsPerMonitor+10)
	for i := range maxEndpointsPerMonitor + 10 {
		eps = append(eps, map[string]any{"port": "p" + strconv.Itoa(i)})
	}
	m, err := ParsePodMonitor(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "bomb", "namespace": "tenant"},
		"spec": map[string]any{
			"selector":            map[string]any{},
			"namespaceSelector":   map[string]any{"any": true},
			"podMetricsEndpoints": eps,
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Endpoints) != maxEndpointsPerMonitor {
		t.Fatalf("kept %d endpoints, want the %d-endpoint ceiling", len(m.Endpoints), maxEndpointsPerMonitor)
	}
	if ig := IgnoredFields(m.Endpoints); !slices.Contains(ig, "podMetricsEndpoints"+cappedSuffix) {
		t.Errorf("the refusal is not reported under the PodMonitor's own field name: %v", ig)
	}
}

// A monitor of an ordinary size is untouched and reports nothing: the ceiling
// is on the pathological, and a spurious "(capped)" entry would be a warning
// per upsert about a monitor that is entirely honoured.
func TestAnOrdinaryEndpointListIsNotCapped(t *testing.T) {
	eps := make([]any, 0, 8)
	for i := range 8 {
		eps = append(eps, map[string]any{"port": "p" + strconv.Itoa(i)})
	}
	m, err := Parse(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "sm", "namespace": "tenant"},
		"spec":     map[string]any{"selector": map[string]any{}, "endpoints": eps},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Endpoints) != 8 {
		t.Fatalf("kept %d of 8 endpoints", len(m.Endpoints))
	}
	if ig := IgnoredFields(m.Endpoints); len(ig) != 0 {
		t.Errorf("an ordinary monitor reports %v", ig)
	}
}
