package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

type dedupTarget struct {
	URL               string `json:"url"`
	Source            string `json:"source"`
	Monitor           string `json:"monitor"`
	AuthSecret        string `json:"authSecret"`
	MetricRelabelings []struct {
		Action string   `json:"action"`
		Regex  string   `json:"regex"`
		Source []string `json:"sourceLabels"`
	} `json:"metricRelabelings"`
}

// annotatedPodAndMonitor builds a pod carrying prometheus.io annotations that
// resolve to the SAME URL a PodMonitor endpoint does — the common case when an
// org-wide scrape annotation coexists with prometheus-operator CRDs.
func annotatedPodAndMonitor(t *testing.T, endpoint map[string]any) *httptest.Server {
	t.Helper()
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-1", Namespace: "default",
			UID: types.UID("pod-uid"), ResourceVersion: "1",
			Labels: map[string]string{"app": "web"},
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
				"prometheus.io/port":   "9090",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node1",
			Containers: []corev1.Container{{
				Name: "app", Image: "img",
				Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9090}},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.9.9.9"},
	})
	monitors := servicemonitors.NewIndex()
	if err := monitors.UpsertPodMonitor(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "pm1", "namespace": "default"},
		"spec": map[string]any{
			"selector":            map[string]any{"matchLabels": map[string]any{"app": "web"}},
			"podMetricsEndpoints": []any{endpoint},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(Config{
		Store: st, Services: services.NewIndex(), Monitors: monitors,
		Resolver: stubResolver{}, MaxWait: 500 * time.Millisecond, Ready: closedChan(),
	}).Handler())
	t.Cleanup(srv.Close)
	return srv
}

// When an annotation-derived target and a monitor-derived one resolve to the
// same URL, the dedup must keep the monitor's endpoint CONFIGURATION. Dropping
// the metricRelabelings exported the very series a drop rule asked to remove —
// silently, and contradicting the design rule that a relabel failure fails the
// scrape rather than exporting what the user asked to drop.
func TestDedupKeepsMonitorRelabelings(t *testing.T) {
	srv := annotatedPodAndMonitor(t, map[string]any{
		"port": "metrics",
		"metricRelabelings": []any{map[string]any{
			"action":       "drop",
			"sourceLabels": []any{"__name__"},
			"regex":        "secret_.*",
		}},
	})

	var out struct {
		Targets []dedupTarget `json:"targets"`
	}
	getJSON(t, srv.URL+"/v1/nodes/node1/targets", http.StatusOK, &out)
	if len(out.Targets) != 1 {
		t.Fatalf("want a single deduped target, got %+v", out.Targets)
	}
	if n := len(out.Targets[0].MetricRelabelings); n != 1 {
		t.Fatalf("target has %d metricRelabelings, want 1: the monitor's drop rule was lost to URL dedup (%+v)",
			n, out.Targets[0])
	}
}

// Same for the bearer token: losing it means scraping a token-protected
// endpoint unauthenticated — a 401 and total metric loss for that target,
// visible only as up=0.
func TestDedupKeepsMonitorAuthSecret(t *testing.T) {
	srv := annotatedPodAndMonitor(t, map[string]any{
		"port": "metrics",
		"bearerTokenSecret": map[string]any{
			"name": "tok",
			"key":  "token",
		},
	})

	var out struct {
		Targets []dedupTarget `json:"targets"`
	}
	getJSON(t, srv.URL+"/v1/nodes/node1/targets", http.StatusOK, &out)
	if len(out.Targets) != 1 {
		t.Fatalf("want a single deduped target, got %+v", out.Targets)
	}
	if out.Targets[0].AuthSecret == "" {
		t.Fatalf("target lost the monitor's bearerTokenSecret to URL dedup: %+v", out.Targets[0])
	}
}

// The dedup predicate must not enumerate config fields: every time an endpoint
// field became interpreted (basicAuth, authorization, tlsConfig, interval) the
// enumeration went stale and that config was silently dropped again.
func TestDedupKeepsEveryKindOfMonitorConfig(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endpoint map[string]any
	}{
		{"basicAuth", map[string]any{"port": "metrics", "basicAuth": map[string]any{
			"username": map[string]any{"name": "c", "key": "u"},
		}}},
		{"authorization", map[string]any{"port": "metrics", "authorization": map[string]any{
			"credentials": map[string]any{"name": "c", "key": "t"},
		}}},
		{"tlsConfig", map[string]any{"port": "metrics", "tlsConfig": map[string]any{
			"ca": map[string]any{"secret": map[string]any{"name": "ca", "key": "ca.crt"}},
		}}},
		{"interval", map[string]any{"port": "metrics", "interval": "10s"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := annotatedPodAndMonitor(t, tc.endpoint)
			var out struct {
				Targets []dedupTarget `json:"targets"`
			}
			getJSON(t, srv.URL+"/v1/nodes/node1/targets", http.StatusOK, &out)
			if len(out.Targets) != 1 {
				t.Fatalf("want one deduped target, got %+v", out.Targets)
			}
			if out.Targets[0].Monitor == "" {
				t.Fatalf("the monitor target lost to dedup, taking its %s with it: %+v", tc.name, out.Targets[0])
			}
		})
	}
}
