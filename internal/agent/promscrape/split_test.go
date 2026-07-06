package promscrape

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
)

const ksmBody = `# TYPE kube_pod_info gauge
kube_pod_info{namespace="ns1",pod="pod1",uid="0a1b2c3d-1111-2222-3333-444455556666",node="node9"} 1
kube_pod_info{namespace="ns2",pod="ghost",uid="9f8e7d6c-9999-8888-7777-666655554444",node="node9"} 1
# TYPE kube_pod_container_status_restarts_total counter
kube_pod_container_status_restarts_total{namespace="ns1",pod="pod1",container="app"} 2
# TYPE kube_namespace_labels gauge
kube_namespace_labels{namespace="ns1",label_team="core"} 1
# TYPE ksm_own_metric gauge
ksm_own_metric 42
`

func ksmSplitters(t *testing.T) []*Splitter {
	t.Helper()
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodLabels: map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}},
		Rules: []SplitRule{
			{
				Metrics: `kube_pod_.+`,
				GroupBy: map[string]string{
					"namespace": "k8s.namespace.name", "pod": "k8s.pod.name",
					"uid": "k8s.pod.uid", "container": "k8s.container.name", "node": "k8s.node.name",
				},
				Enrich: true,
			},
			{
				Metrics: `kube_namespace_.+`,
				GroupBy: map[string]string{"namespace": "k8s.namespace.name"},
			},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return sp
}

func TestSplitterScrape(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(ksmBody))
	}))
	defer srv.Close()

	target := testTarget(srv.URL)
	target.Pod.Name = "ksm-abc"
	target.Pod.Labels = map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}

	meta := &fakeMetaSource{} // resolves ns1/pod1 (uid1), errors otherwise
	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{target}, Exporter: exp, StartTime: time.Now(),
		Splitters: ksmSplitters(t),
		Kubelet:   KubeletConfig{Meta: meta},
	})
	if _, err := s.scrapeTarget(context.Background(), target); err != nil {
		t.Fatal(err)
	}

	// Expected resources: pod1 pod-level (kube_pod_info), pod1 container
	// "app" (restarts), ghost (label identity), ns1 (namespace only), and
	// the KSM pod itself for ksm_own_metric.
	rms := exp.batches[0].ResourceMetrics()
	byKey := map[string]pmetric.ResourceMetrics{}
	for i := 0; i < rms.Len(); i++ {
		res := rms.At(i).Resource()
		key := attrStr(res, "k8s.namespace.name") + "/" + attrStr(res, "k8s.pod.name") + "/" + attrStr(res, "k8s.container.name")
		byKey[key] = rms.At(i)
	}
	if len(byKey) != 5 {
		t.Fatalf("got %d resources: %v", len(byKey), keys(byKey))
	}

	// Enriched pod-level resource with grouped labels stripped from points.
	rm, ok := byKey["ns1/pod1/"]
	if !ok {
		t.Fatalf("missing pod1 resource: %v", keys(byKey))
	}
	if attrStr(rm.Resource(), "k8s.deployment.name") != "dep1" || attrStr(rm.Resource(), "k8s.pod.uid") != uid1 {
		t.Fatalf("pod1 not enriched: %v", rm.Resource().Attributes().AsRaw())
	}
	if names := metricNames(rm); len(names) != 1 || names[0] != "kube_pod_info" {
		t.Fatalf("pod1 metrics = %v", names)
	}
	dp := rm.ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0)
	if _, leaked := dp.Attributes().Get("pod"); leaked {
		t.Fatalf("grouped label leaked to data point: %v", dp.Attributes().AsRaw())
	}

	// Container-level resource: enrichment resolved the container too.
	rm, ok = byKey["ns1/pod1/app"]
	if !ok {
		t.Fatalf("missing pod1/app resource: %v", keys(byKey))
	}
	if attrStr(rm.Resource(), "container.id") != appCID {
		t.Fatalf("container resource not enriched: %v", rm.Resource().Attributes().AsRaw())
	}
	if names := metricNames(rm); len(names) != 1 || names[0] != "kube_pod_container_status_restarts_total" {
		t.Fatalf("container metrics = %v", names)
	}

	// Unresolvable pod: identity from the mapped labels.
	rm, ok = byKey["ns2/ghost/"]
	if !ok {
		t.Fatalf("missing ghost resource: %v", keys(byKey))
	}
	if attrStr(rm.Resource(), "k8s.pod.uid") != ghostUID || attrStr(rm.Resource(), "k8s.node.name") != "node9" {
		t.Fatalf("ghost resource = %v", rm.Resource().Attributes().AsRaw())
	}

	// Namespace-scoped rule: no pod attrs, ungrouped labels stay on points.
	rm, ok = byKey["ns1//"]
	if !ok {
		t.Fatalf("missing namespace resource: %v", keys(byKey))
	}
	dp = rm.ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0)
	if v, _ := dp.Attributes().Get("label_team"); v.Str() != "core" {
		t.Fatalf("namespace dp attrs = %v", dp.Attributes().AsRaw())
	}

	// Unmatched series stay on the target's own (enriched) resource.
	rm, ok = byKey["ns1/ksm-abc/"]
	if !ok {
		t.Fatalf("missing self resource: %v", keys(byKey))
	}
	if names := metricNames(rm); len(names) != 1 || names[0] != "ksm_own_metric" {
		t.Fatalf("self metrics = %v", names)
	}
	if v, _ := rm.Resource().Attributes().Get("url.full"); v.Str() == "" {
		t.Fatal("self resource missing url.full")
	}
}

func TestSplitterMatch(t *testing.T) {
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{Namespace: "monitoring", PodName: "ksm-.+"},
		Rules: []SplitRule{{GroupBy: map[string]string{"namespace": "k8s.namespace.name"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	s := New(Config{Node: "n", Interval: time.Hour, Timeout: time.Second,
		Targets: staticTargets{}, Exporter: &captureExporter{}, Splitters: sp})

	if s.splitterFor(kubemeta.Pod{Namespace: "monitoring", Name: "ksm-1"}) == nil {
		t.Fatal("matching pod must select the splitter")
	}
	if s.splitterFor(kubemeta.Pod{Namespace: "default", Name: "ksm-1"}) != nil {
		t.Fatal("namespace mismatch must not select the splitter")
	}
	if s.splitterFor(kubemeta.Pod{Namespace: "monitoring", Name: "other"}) != nil {
		t.Fatal("name mismatch must not select the splitter")
	}
}

func TestSplitterValidation(t *testing.T) {
	if _, err := NewSplitters([]SplitterConfig{{
		Rules: []SplitRule{{GroupBy: map[string]string{"a": "b"}}},
	}}); err == nil {
		t.Fatal("empty match must error")
	}
	if _, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodName: "x"},
	}}); err == nil {
		t.Fatal("no rules must error")
	}
	if _, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodName: "x"},
		Rules: []SplitRule{{Metrics: "("}},
	}}); err == nil {
		t.Fatal("bad regex must error")
	}
}
