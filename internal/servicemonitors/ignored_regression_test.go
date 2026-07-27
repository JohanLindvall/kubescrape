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
