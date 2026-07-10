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
	if attrStr(rm.Resource(), "k8s.pod.uid") != ghostUID {
		t.Fatalf("ghost resource = %v", rm.Resource().Attributes().AsRaw())
	}
	// k8s.node.name is a data-point attribute on a split resource, not a
	// resource attribute (cmb-alloy placement).
	if _, onRes := rm.Resource().Attributes().Get("k8s.node.name"); onRes {
		t.Fatalf("k8s.node.name leaked onto the split resource: %v", rm.Resource().Attributes().AsRaw())
	}
	gdp := rm.ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0)
	if v, _ := gdp.Attributes().Get("k8s.node.name"); v.Str() != "node9" {
		t.Fatalf("ghost data-point node = %v, want node9 (attrs %v)", v.AsRaw(), gdp.Attributes().AsRaw())
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

func TestSplitterDatapointAttributesOverride(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(ksmBody))
	}))
	defer srv.Close()

	target := testTarget(srv.URL)
	target.Pod.Name = "ksm-abc"
	target.Pod.Labels = map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}

	// Override: keep k8s.node.name on the resource (empty datapoint list).
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodLabels: map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}},
		Rules: []SplitRule{{
			Metrics: `kube_pod_.+`,
			GroupBy: map[string]string{
				"namespace": "k8s.namespace.name", "pod": "k8s.pod.name",
				"uid": "k8s.pod.uid", "node": "k8s.node.name",
			},
			DatapointAttributes: &[]string{},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{target}, Exporter: exp, StartTime: time.Now(),
		Splitters: sp, Kubelet: KubeletConfig{Meta: &fakeMetaSource{}},
	})
	if _, err := s.scrapeTarget(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	rms := exp.batches[0].ResourceMetrics()
	found := false
	for i := 0; i < rms.Len(); i++ {
		res := rms.At(i).Resource()
		if attrStr(res, "k8s.pod.name") == "ghost" {
			found = true
			if attrStr(res, "k8s.node.name") != "node9" {
				t.Fatalf("override should keep node on the resource: %v", res.Attributes().AsRaw())
			}
		}
	}
	if !found {
		t.Fatal("ghost resource not produced")
	}
}

func TestSplitterInstancePrefix(t *testing.T) {
	body := "# TYPE kube_pod_info gauge\n" +
		`kube_pod_info{namespace="ns1",pod="pod1",uid="` + uid1 + `",node="node9"} 1` + "\n"
	run := func(t *testing.T, sp []*Splitter) pmetric.ResourceMetrics {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(body))
		}))
		defer srv.Close()
		target := testTarget(srv.URL) // owner Deployment "dep1" -> service.name dep1
		target.Pod.Name = "ksm-abc"
		target.Pod.Labels = map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}
		exp := &captureExporter{}
		s := New(Config{
			Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
			Targets: staticTargets{target}, Exporter: exp, StartTime: time.Now(),
			Splitters: sp, Kubelet: KubeletConfig{Meta: &fakeMetaSource{}},
		})
		if _, err := s.scrapeTarget(context.Background(), target); err != nil {
			t.Fatal(err)
		}
		rms := exp.batches[0].ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			if attrStr(rms.At(i).Resource(), "k8s.pod.name") == "pod1" {
				return rms.At(i)
			}
		}
		t.Fatal("pod1 resource not produced")
		return pmetric.ResourceMetrics{}
	}

	splitter := func(prefix *string) []*Splitter {
		sp, err := NewSplitters([]SplitterConfig{{
			Match: SplitterMatch{PodLabels: map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}},
			Rules: []SplitRule{{
				Metrics:        `kube_pod_.+`,
				GroupBy:        map[string]string{"namespace": "k8s.namespace.name", "pod": "k8s.pod.name", "uid": "k8s.pod.uid"},
				InstancePrefix: prefix,
			}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		return sp
	}

	// Default: prefix is the describing target's service.name (dep1).
	rm := run(t, splitter(nil))
	if got := attrStr(rm.Resource(), "service.instance.id"); got != "dep1-"+uid1 {
		t.Errorf("default instance = %q, want dep1-%s", got, uid1)
	}
	// Explicit override.
	custom := "ksm"
	rm = run(t, splitter(&custom))
	if got := attrStr(rm.Resource(), "service.instance.id"); got != "ksm-"+uid1 {
		t.Errorf("override instance = %q, want ksm-%s", got, uid1)
	}
	// Empty string disables the prefix.
	empty := ""
	rm = run(t, splitter(&empty))
	if got := attrStr(rm.Resource(), "service.instance.id"); got != uid1 {
		t.Errorf("disabled instance = %q, want %s", got, uid1)
	}
}

func TestSplitterDropLabelsAndAttributes(t *testing.T) {
	body := `# TYPE kube_namespace_labels gauge
kube_namespace_labels{namespace="ns1",label_team="core",label_env="prod",owner="x"} 1
# TYPE kube_node_labels gauge
kube_node_labels{node="node9",label_zone="eu-1"} 1
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	target := testTarget(srv.URL)
	target.Pod.Name = "ksm-abc"
	target.Pod.Labels = map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}

	// kube_node_labels keeps its label_* points (ordered first); the generic
	// labels rule drops them and gets a fallback service.name.
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodLabels: map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}},
		Rules: []SplitRule{
			{
				Metrics: `kube_node_labels`,
				GroupBy: map[string]string{"node": "k8s.node.name"},
			},
			{
				Metrics:    `kube_.+_labels`,
				GroupBy:    map[string]string{"namespace": "k8s.namespace.name"},
				DropLabels: `label_.+`,
				Attributes: map[string]string{"service.name": "unknown"},
			},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{target}, Exporter: exp, StartTime: time.Now(),
		Splitters: sp, Kubelet: KubeletConfig{Meta: &fakeMetaSource{}},
	})
	if _, err := s.scrapeTarget(context.Background(), target); err != nil {
		t.Fatal(err)
	}

	rms := exp.batches[0].ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		switch metricNames(rm)[0] {
		case "kube_namespace_labels":
			dp := rm.ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0)
			for _, k := range []string{"label_team", "label_env"} {
				if _, ok := dp.Attributes().Get(k); ok {
					t.Errorf("%s not dropped: %v", k, dp.Attributes().AsRaw())
				}
			}
			if v, _ := dp.Attributes().Get("owner"); v.Str() != "x" {
				t.Errorf("non-matching label lost: %v", dp.Attributes().AsRaw())
			}
			if v := attrStr(rm.Resource(), "service.name"); v != "unknown" {
				t.Errorf("fallback service.name = %q, want unknown", v)
			}
		case "kube_node_labels":
			dp := rm.ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0)
			if v, _ := dp.Attributes().Get("label_zone"); v.Str() != "eu-1" {
				t.Errorf("kube_node_labels must keep label_*: %v", dp.Attributes().AsRaw())
			}
			if v := attrStr(rm.Resource(), "service.name"); v == "unknown" {
				t.Error("fallback attribute leaked onto the node-labels rule")
			}
		}
	}
}

func TestSplitterAttributesDontOverride(t *testing.T) {
	body := "# TYPE kube_pod_info gauge\n" +
		`kube_pod_info{namespace="ns1",pod="pod1",uid="` + uid1 + `"} 1` + "\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	target := testTarget(srv.URL)
	target.Pod.Name = "ksm-abc"
	target.Pod.Labels = map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodLabels: map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}},
		Rules: []SplitRule{{
			Metrics:    `kube_pod_.+`,
			GroupBy:    map[string]string{"namespace": "k8s.namespace.name", "pod": "k8s.pod.name", "uid": "k8s.pod.uid"},
			Enrich:     true,
			Attributes: map[string]string{"service.name": "unknown"},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{target}, Exporter: exp, StartTime: time.Now(),
		Splitters: sp, Kubelet: KubeletConfig{Meta: &fakeMetaSource{}},
	})
	if _, err := s.scrapeTarget(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	rms := exp.batches[0].ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		res := rms.At(i).Resource()
		if attrStr(res, "k8s.pod.name") == "pod1" {
			// Enrichment resolved the owner; the fallback must not override it.
			if got := attrStr(res, "service.name"); got != "dep1" {
				t.Fatalf("service.name = %q, want dep1 (enriched owner)", got)
			}
			return
		}
	}
	t.Fatal("pod1 resource not produced")
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
