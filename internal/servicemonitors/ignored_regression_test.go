package servicemonitors

import (
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Fields kubescrape cannot honour must be REPORTED, never silently dropped:
// the file-path TLS arms (a target then falls back to the system trust store
// and every scrape fails), a custom metricRelabelings separator (the agent
// joins with ';', so honouring the rule would invert a keep into a drop), and
// the monitor-level guard rails like sampleLimit.
func TestIgnoredFieldsCoverUninterpreted(t *testing.T) {
	u := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": "ns", "name": "m"},
		"spec": map[string]any{
			"sampleLimit": int64(10000),
			"targetLimit": int64(5),
			"jobLabel":    "app",
			"selector":    map[string]any{},
			"endpoints": []any{map[string]any{
				"port": "http",
				"tlsConfig": map[string]any{
					"caFile":   "/etc/prometheus/secrets/etcd/ca.crt",
					"certFile": "/etc/prometheus/secrets/etcd/tls.crt",
					"keyFile":  "/etc/prometheus/secrets/etcd/tls.key",
				},
				"metricRelabelings": []any{map[string]any{
					"action":       "keep",
					"sourceLabels": []any{"__name__", "namespace"},
					"separator":    "@",
					"regex":        "kube_pod_info@prod",
				}},
			}},
		},
	}}
	m, err := Parse(u)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got := strings.Join(IgnoredFields(m.Endpoints), ",")
	for _, want := range []string{
		"tlsConfig.caFile", "tlsConfig.certFile", "tlsConfig.keyFile",
		"metricRelabelings.separator=@", "sampleLimit", "targetLimit", "jobLabel",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("IgnoredFields missing %q; got %q", want, got)
		}
	}
	// The separator rule must be SUPPRESSED, not applied against the wrong
	// joined string.
	if n := len(m.Endpoints[0].MetricRelabelings); n != 0 {
		t.Errorf("custom-separator rule was kept (%d rules); it must be skipped", n)
	}
}

// filterRunning lives on the ENDPOINT (PodMetricsEndpoint), not on the spec.
// It was decoded at the spec level for a while, where it could never bind — so
// the one field the Ignored machinery singles out as most likely to surprise
// (its default is true, and `false` asks for the OPPOSITE of what
// scrape.Scrapeable does) was the one silently dropped. Pin the placement.
func TestFilterRunningIsReportedFromTheEndpoint(t *testing.T) {
	pm := func(ep map[string]any) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"metadata": map[string]any{"namespace": "ns", "name": "pm"},
			"spec": map[string]any{
				"selector":            map[string]any{},
				"podMetricsEndpoints": []any{ep},
			},
		}}
	}
	m, err := ParsePodMonitor(pm(map[string]any{"port": "metrics", "filterRunning": false}))
	if err != nil {
		t.Fatalf("ParsePodMonitor: %v", err)
	}
	if got := IgnoredFields(m.Endpoints); !strings.Contains(strings.Join(got, ","), "filterRunning") {
		t.Errorf("filterRunning: false must be reported as ignored; got %q", got)
	}
	// The default (true) matches what kubescrape already does, so it is not a
	// partial application and must stay quiet.
	m, err = ParsePodMonitor(pm(map[string]any{"port": "metrics", "filterRunning": true}))
	if err != nil {
		t.Fatalf("ParsePodMonitor: %v", err)
	}
	if got := IgnoredFields(m.Endpoints); strings.Contains(strings.Join(got, ","), "filterRunning") {
		t.Errorf("filterRunning: true agrees with Scrapeable and must not be reported; got %q", got)
	}
	// Placed on the SPEC (where it does not belong) it must not be reported —
	// that spelling is not what the CRD accepts, so claiming we ignored it
	// would be as wrong as silently dropping the endpoint one.
	u := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": "ns", "name": "pm"},
		"spec": map[string]any{
			"selector":            map[string]any{},
			"filterRunning":       false,
			"podMetricsEndpoints": []any{map[string]any{"port": "metrics"}},
		},
	}}
	m, err = ParsePodMonitor(u)
	if err != nil {
		t.Fatalf("ParsePodMonitor: %v", err)
	}
	if got := IgnoredFields(m.Endpoints); strings.Contains(strings.Join(got, ","), "filterRunning") {
		t.Errorf("spec-level filterRunning is not a CRD field; got %q", got)
	}

	// filterRunning is an endpoint field on the ServiceMonitor CRD too, and
	// endpointSpec is shared, so the ServiceMonitor arm must report it
	// identically. Nothing exercised that side.
	sm := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": "ns", "name": "sm"},
		"spec": map[string]any{
			"selector":  map[string]any{},
			"endpoints": []any{map[string]any{"port": "http", "filterRunning": false}},
		},
	}}
	smm, err := Parse(sm)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := IgnoredFields(smm.Endpoints); !strings.Contains(strings.Join(got, ","), "filterRunning") {
		t.Errorf("ServiceMonitor endpoint filterRunning: false must be reported; got %q", got)
	}
}

// portNumber is a PodMonitor endpoint field (container port as a NUMBER). It is
// not honoured, so an endpoint naming ONLY portNumber resolves to no targets —
// which must not be silent.
func TestPodMonitorPortNumberIsReported(t *testing.T) {
	u := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": "ns", "name": "pm"},
		"spec": map[string]any{
			"selector":            map[string]any{},
			"podMetricsEndpoints": []any{map[string]any{"portNumber": int64(9090)}},
		},
	}}
	m, err := ParsePodMonitor(u)
	if err != nil {
		t.Fatalf("ParsePodMonitor: %v", err)
	}
	if got := IgnoredFields(m.Endpoints); !strings.Contains(strings.Join(got, ","), "portNumber") {
		t.Errorf("portNumber must be reported as uninterpreted; got %q", got)
	}
	// And it still yields no targets — the phantom-target guard is unchanged.
	if len(m.Endpoints) != 1 || m.Endpoints[0].Port != "" || m.Endpoints[0].TargetPort != nil {
		t.Errorf("portNumber must not be interpreted as a port: %+v", m.Endpoints[0])
	}
}

// The ten fields that were not parsed at all, exercised through the REAL decode
// path (unstructured → DefaultUnstructuredConverter), not just by setting Go
// struct fields: a quantity, a string list and a plain string each decode
// differently, and the reflection-based pinning test cannot see that.
//
// scrapeProtocols is the one with visible behaviour behind it: the agent
// negotiates its own Accept header, so an operator who pinned
// PrometheusText0.0.4 because a target's OpenMetrics is broken gets the
// opposite of what the CR says.
func TestNewlyParsedFieldsAreReportedThroughTheDecode(t *testing.T) {
	u := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": "ns", "name": "m"},
		"spec": map[string]any{
			"selector":                       map[string]any{},
			"scrapeProtocols":                []any{"PrometheusText0.0.4"},
			"fallbackScrapeProtocol":         "PrometheusText0.0.4",
			"selectorMechanism":              "RelabelConfig",
			"nativeHistogramBucketLimit":     int64(160),
			"nativeHistogramMinBucketFactor": "1.1",
			"convertClassicHistogramsToNHCB": true,
			"scrapeClassicHistograms":        true,
			"endpoints": []any{map[string]any{
				"port":                 "http",
				"noProxy":              "10.0.0.0/8",
				"proxyFromEnvironment": true,
				"proxyConnectHeader": map[string]any{
					"Proxy-Authorization": []any{map[string]any{"name": "s", "key": "k"}},
				},
			}},
		},
	}}
	m, err := Parse(u)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got := IgnoredFields(m.Endpoints)
	for _, want := range []string{
		"scrapeProtocols", "fallbackScrapeProtocol", "selectorMechanism",
		"nativeHistogramBucketLimit", "nativeHistogramMinBucketFactor",
		"convertClassicHistogramsToNHCB", "scrapeClassicHistograms",
		"noProxy", "proxyFromEnvironment", "proxyConnectHeader",
	} {
		if !slices.Contains(got, want) {
			t.Errorf("%s is set on the CR and not reported: %v", want, got)
		}
	}
	// proxyConnectHeader carries SecretKeySelectors. It is NOT honoured, so it
	// must not have quietly become a secret channel outside Endpoint.secretRefs.
	ix := NewIndex()
	if err := ix.Upsert(u); err != nil {
		t.Fatal(err)
	}
	if refs := ix.AuthSecretRefs(); len(refs) != 0 {
		t.Errorf("proxyConnectHeader's secret refs reached the scrape-auth allowlist: %v", refs)
	}
}

// An endpoint naming neither port nor targetPort resolves to NO targets — the
// phantom-target guard, which is deliberately narrower than
// prometheus-operator (which emits no port filter at all and scrapes every port
// of every matching EndpointSlice). Being narrower is a choice; being silent
// about it is the defect: the outcome is identical to portNumber's, which IS
// reported, and whose comment gives the reason.
func TestEndpointNamingNoPortIsReported(t *testing.T) {
	sm := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": "ns", "name": "m"},
		"spec": map[string]any{
			"selector":  map[string]any{},
			"endpoints": []any{map[string]any{"path": "/metrics"}},
		},
	}}
	m, err := Parse(sm)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := IgnoredFields(m.Endpoints); !slices.Contains(got, "port(unset)") {
		t.Errorf("an endpoint naming no port yields no targets and must say so; got %v", got)
	}
	// A named port is the ordinary case and must stay quiet.
	sm.Object["spec"].(map[string]any)["endpoints"] = []any{map[string]any{"port": "http"}}
	m, err = Parse(sm)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := IgnoredFields(m.Endpoints); slices.Contains(got, "port(unset)") {
		t.Errorf("an endpoint naming a port must not report port(unset); got %v", got)
	}
	// portNumber already names the cause; reporting an unset port beside it
	// would be false — the endpoint DOES name a port, we just do not honour it.
	pm := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": "ns", "name": "pm"},
		"spec": map[string]any{
			"selector":            map[string]any{},
			"podMetricsEndpoints": []any{map[string]any{"portNumber": int64(9090)}},
		},
	}}
	pmm, err := ParsePodMonitor(pm)
	if err != nil {
		t.Fatalf("ParsePodMonitor: %v", err)
	}
	got := IgnoredFields(pmm.Endpoints)
	if !slices.Contains(got, "portNumber") || slices.Contains(got, "port(unset)") {
		t.Errorf("portNumber alone must report portNumber and nothing about an unset port; got %v", got)
	}
}
