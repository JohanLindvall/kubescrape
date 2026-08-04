package servicemonitors

import (
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
}
