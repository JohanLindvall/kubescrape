package servicemonitors

import (
	"encoding/json"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
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
	// VisibleFields, not NumField: the auth/TLS strings are PROMOTED from the
	// embedded kubemeta.ScrapeAuth, and a top-level walk would silently stop
	// covering them — the credential refs among them.
	walked := 0
	for _, f := range reflect.VisibleFields(typ) {
		if f.Anonymous || f.Type.Kind() != reflect.String {
			continue
		}
		walked++
		if _, ok := exempt[f.Name]; ok {
			continue
		}
		if _, ok := bounded[v.FieldByIndex(f.Index).Addr().Pointer()]; !ok {
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
	// The embedded group is walked, not skipped: its ten fields include nine
	// strings, and a walk that reached none of them would pass vacuously.
	if _, ok := typ.FieldByName("AuthCredentials"); !ok || walked < len(e.boundedFields())+len(exempt) {
		t.Errorf("walked %d string fields; the promoted auth/TLS strings were not reached", walked)
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

// The cap must bind BEFORE the typed decode, or it bounds what is retained but
// not what each delivery costs: FromUnstructured over a 100,000-endpoint CR
// measured ~216 ms, 37 MB and ~100k allocations per Parse, to keep 128 of
// them. With the raw list cut first the decode is ~400 allocations whatever
// the list's length, which is what the ceiling below pins — 20,000 endpoints
// cost ~20,000 allocations when the cut ran after the decode. Both kinds share
// the skeleton, so both are measured.
//
// The fixture's elements all alias ONE map, which keeps building it cheap;
// the decode cannot tell.
func TestAMonitorsEndpointListIsCutBeforeTheDecode(t *testing.T) {
	if testrace.Enabled {
		t.Skip("allocation budgets are meaningless under -race")
	}
	const n = 20_000
	for _, kind := range []string{"ServiceMonitor", "PodMonitor"} {
		t.Run(kind, func(t *testing.T) {
			ep := map[string]any{"port": "metrics"}
			raw := make([]any, n)
			for i := range raw {
				raw[i] = ep
			}
			u := crObject(kind, "tenant", "bomb", "1", map[string]any{
				"selector":          map[string]any{},
				"namespaceSelector": map[string]any{"any": true},
				endpointsKey(kind):  raw,
			})
			parse := func() []Endpoint {
				if kind == "PodMonitor" {
					m, err := ParsePodMonitor(u)
					if err != nil {
						t.Fatal(err)
					}
					return m.Endpoints
				}
				m, err := Parse(u)
				if err != nil {
					t.Fatal(err)
				}
				return m.Endpoints
			}
			eps := parse()
			if len(eps) != maxEndpointsPerMonitor {
				t.Fatalf("kept %d endpoints, want %d", len(eps), maxEndpointsPerMonitor)
			}
			if ig := IgnoredFields(eps); !slices.Contains(ig, endpointsKey(kind)+cappedSuffix) {
				t.Errorf("the pre-decode cut is not reported: %v", ig)
			}
			// The object is the informer's cache entry: the cut must not
			// write through to it.
			if got, _, _ := unstructured.NestedSlice(u.Object, "spec", endpointsKey(kind)); len(got) != n {
				t.Errorf("Parse shortened the informer's own endpoint list to %d entries", len(got))
			}
			const ceiling = 1000
			if allocs := testing.AllocsPerRun(5, func() { _ = parse() }); allocs > ceiling {
				t.Errorf("parsing a %d-endpoint %s allocates %.0f times, want <= %d: the "+
					"endpoint list is being decoded whole before the cap cuts it", n, kind, allocs, ceiling)
			}
		})
	}
}

// A consequence of cutting before the decode, pinned so it is a decision: a
// malformed element in the REFUSED tail is never decoded, so it cannot fail the
// monitor — the kept prefix is served, and the list is reported capped. The
// tail is refused either way; failing the parse would also drop the prefix.
func TestAMalformedEndpointPastTheCapDoesNotRejectTheMonitor(t *testing.T) {
	raw := make([]any, 0, maxEndpointsPerMonitor+1)
	for i := range maxEndpointsPerMonitor {
		raw = append(raw, map[string]any{"port": "p" + strconv.Itoa(i)})
	}
	raw = append(raw, map[string]any{"port": map[string]any{"not": "a string"}})
	m, err := Parse(crObject("ServiceMonitor", "tenant", "sm", "1", map[string]any{
		"selector": map[string]any{}, "endpoints": raw,
	}))
	if err != nil {
		t.Fatalf("a malformed endpoint in the refused tail rejected the whole monitor: %v", err)
	}
	if len(m.Endpoints) != maxEndpointsPerMonitor {
		t.Fatalf("kept %d endpoints, want %d", len(m.Endpoints), maxEndpointsPerMonitor)
	}
	if ig := IgnoredFields(m.Endpoints); !slices.Contains(ig, "endpoints"+cappedSuffix) {
		t.Errorf("the cut is not reported: %v", ig)
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

// selectorMonitor builds a monitor of the given kind around a selector and a
// namespaceSelector, with one ordinary endpoint.
func selectorMonitor(kind string, selector, nsSelector map[string]any) *unstructured.Unstructured {
	return crObject(kind, "tenant", "sel", "", map[string]any{
		"selector":          selector,
		"namespaceSelector": nsSelector,
		endpointsKey(kind):  []any{map[string]any{"port": "http"}},
	})
}

// parseKind parses u with the kind's parser, reporting only the error.
func parseKind(kind string, u *unstructured.Unstructured) error {
	if kind == "PodMonitor" {
		_, err := ParsePodMonitor(u)
		return err
	}
	_, err := Parse(u)
	return err
}

// THE ATTACK, one door over from the endpoint list: a monitor's two SELECTORS
// are tenant-authored and neither is bounded upstream (no maxItems in the CRD;
// apimachinery validates each key and value but not the count). Measured: a
// ~1.4 MiB CR of ~25,000 `DoesNotExist` requirements parsed without error and
// cost 744 µs per Selector.Matches — per Service in the server's
// monitor→services rebuild under its lock, and per pod in every PodMonitor
// node-targets derivation — while a 150,000-entry matchNames list was retained
// whole and scanned per pod.
//
// The MONITOR is refused, never trimmed: dropping a requirement WIDENS what it
// selects, the opposite of the endpoint cap's fail-safe.
//
// Reverse-patch check: removing the checkSelectorBounds call accepts all three
// bombs and this fails.
func TestAMonitorsSelectorsAreBoundedAtTheParseDoor(t *testing.T) {
	requirements := make([]any, 0, 25000)
	for i := range 25000 {
		requirements = append(requirements, map[string]any{
			"key": "k" + strconv.Itoa(i) + ".example.com/l", "operator": "DoesNotExist",
		})
	}
	values := make([]any, 0, maxSelectorValues+1)
	for i := range maxSelectorValues + 1 {
		values = append(values, "v"+strconv.Itoa(i))
	}
	names := make([]any, 0, maxSelectorNamespaces+1)
	for i := range maxSelectorNamespaces + 1 {
		names = append(names, "ns"+strconv.Itoa(i))
	}
	bombs := map[string]struct{ selector, nsSelector map[string]any }{
		"requirements": {map[string]any{"matchExpressions": requirements}, map[string]any{"any": true}},
		"values": {map[string]any{"matchExpressions": []any{
			map[string]any{"key": "app", "operator": "NotIn", "values": values},
		}}, map[string]any{"any": true}},
		"matchNames":      {map[string]any{}, map[string]any{"matchNames": names}},
		"matchName bytes": {map[string]any{}, map[string]any{"matchNames": []any{strings.Repeat("n", 1<<20)}}},
	}
	for _, kind := range []string{"ServiceMonitor", "PodMonitor"} {
		for name, b := range bombs {
			t.Run(kind+"/"+name, func(t *testing.T) {
				if err := parseKind(kind, selectorMonitor(kind, b.selector, b.nsSelector)); err == nil {
					t.Errorf("the %s bomb was accepted; the monitor must be refused", name)
				}
			})
		}
	}
}

// …and the ceilings are far above anything real: a kube-prometheus-stack
// shaped selector, a platform monitor's namespace list and a set-based
// expression all parse.
func TestOrdinaryMonitorSelectorsParse(t *testing.T) {
	names := make([]any, 0, 40)
	for i := range 40 {
		names = append(names, "team-"+strconv.Itoa(i))
	}
	for _, kind := range []string{"ServiceMonitor", "PodMonitor"} {
		u := selectorMonitor(kind, map[string]any{
			"matchLabels": map[string]any{
				"app.kubernetes.io/name": "kube-state-metrics", "app.kubernetes.io/instance": "kps", "release": "kps",
			},
			"matchExpressions": []any{
				map[string]any{"key": "tier", "operator": "In", "values": []any{"backend", "frontend"}},
			},
		}, map[string]any{"matchNames": names})
		if err := parseKind(kind, u); err != nil {
			t.Errorf("%s: an ordinary selector was refused: %v", kind, err)
		}
	}
}
