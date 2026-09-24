package servicemonitors_test

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// End to end through the real derivation: a PodMonitor endpoint declaring
// `scheme: HTTPS` beside a bearer credential must yield an https:// target —
// the credential rides on the target either way, so the scheme is the only
// thing standing between it and the pod network in cleartext.
func TestUpperCaseSchemeMonitorTargetIsHTTPS(t *testing.T) {
	pm, err := servicemonitors.ParsePodMonitor(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": "tenant", "name": "pm"},
		"spec": map[string]any{
			"selector": map[string]any{},
			"podMetricsEndpoints": []any{map[string]any{
				"port": "metrics", "scheme": "HTTPS",
				"bearerTokenSecret": map[string]any{"name": "tok", "key": "token"},
			}},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	pod := kubemeta.Pod{
		Namespace: "tenant", Name: "p", PodIP: "10.0.0.1", Phase: "Running",
		Containers: []kubemeta.Container{{Name: "app", Ports: []kubemeta.ContainerPort{{Name: "metrics", Port: 9090}}}},
	}
	targets := scrape.PodMonitorTargets(pod, "tenant/pm", pm.Endpoints[0])
	if len(targets) != 1 {
		t.Fatalf("got %d targets, want 1", len(targets))
	}
	if tg := targets[0]; !strings.HasPrefix(tg.URL, "https://") || tg.AuthSecret == "" {
		t.Errorf("target %q (credential %q): an HTTPS endpoint's credential must not be scraped over http", tg.URL, tg.AuthSecret)
	}
}
