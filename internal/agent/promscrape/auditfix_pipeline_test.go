package promscrape

import (
	"context"
	"strings"
	"testing"
	"time"
)

// newCID is a container incarnation the fake metadata source does not know —
// the just-restarted container whose id the kubelet has not posted yet.
const newCID = "abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd"

// A cadvisor row whose cgroup names a container id the store cannot resolve
// must keep its OWN identity. Resolving the POD and matching its current
// container by NAME stamps a DIFFERENT incarnation's container.id, image and
// restart_count (hence service.instance.id) onto the row: the dead
// incarnation's identity on the live one's cumulative counters, and — while
// cadvisor still exports the exited container's cgroup — two ResourceMetrics
// with byte-identical attributes in one payload.
func TestCadvisorUnknownContainerIDKeepsRowIdentity(t *testing.T) {
	body := "# TYPE container_cpu_usage_seconds_total counter\n" +
		`container_cpu_usage_seconds_total{namespace="ns1",pod="pod1",container="app",` +
		`id="/kubepods/burstable/pod` + uid1 + `/` + newCID + `",image="img:2",name="app"} 7` + "\n"
	srv := serveBody(t, body)

	exp := &captureExporter{}
	s := newKubeletScraper(t, srv.URL, &fakeMetaSource{}, exp, false)
	if _, err := s.scrapeCadvisor(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(exp.batches) != 1 {
		t.Fatalf("got %d batches", len(exp.batches))
	}
	rms := exp.batches[0].ResourceMetrics()
	if rms.Len() != 1 {
		t.Fatalf("got %d resources: %v", rms.Len(), exp.batches[0])
	}
	res := rms.At(0).Resource()
	if got := attrStr(res, "container.id"); got != newCID {
		t.Errorf("container.id = %q, want the row's own %s (attrs %v)", got, newCID, res.Attributes().AsRaw())
	}
	if got := attrStr(res, "service.instance.id"); got != newCID {
		t.Errorf("service.instance.id = %q, want %s", got, newCID)
	}
	if got := attrStr(res, "container.image.name"); got != "img:2" {
		t.Errorf("container.image.name = %q, want the row's own img:2", got)
	}
	// The pod half must still be enriched: only the container identity is
	// unknown.
	if got := attrStr(res, "k8s.deployment.name"); got != "dep1" {
		t.Errorf("k8s.deployment.name = %q, want dep1 (pod enrichment lost)", got)
	}
	if got := attrStr(res, "service.name"); got != "dep1" {
		t.Errorf("service.name = %q, want dep1 (owner-derived)", got)
	}
	if _, ok := res.Attributes().Get("k8s.container.restart_count"); ok {
		t.Errorf("restart_count of another incarnation stamped: %v", res.Attributes().AsRaw())
	}
}

// The same rule on the splitter path: an enrich rule grouping by container.id
// must keep the label's own container id when the store cannot resolve it.
func TestSplitterUnknownContainerIDKeepsRowIdentity(t *testing.T) {
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodLabels: map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}},
		Rules: []SplitRule{{
			Metrics: `kube_pod_.+`,
			GroupBy: map[string]string{
				"namespace": "k8s.namespace.name", "pod": "k8s.pod.name",
				"container": "k8s.container.name", "container_id": "container.id",
			},
			Enrich: true,
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	body := "# TYPE kube_pod_container_info gauge\n" +
		`kube_pod_container_info{namespace="ns1",pod="pod1",container="app",container_id="containerd://` + newCID + `"} 1` + "\n"
	srv := serveBody(t, body)
	target := testTarget(srv.URL)
	target.Pod.Name = "ksm-abc"
	target.Pod.Labels = map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{target}, Exporter: exp, StartTime: time.Now(),
		Splitters: sp, Kubelet: KubeletConfig{Meta: &fakeMetaSource{}},
	})
	if _, err := s.scrapeTarget(context.Background(), target, s.cfg.Timeout); err != nil {
		t.Fatal(err)
	}
	rms := exp.batches[0].ResourceMetrics()
	if rms.Len() != 1 {
		t.Fatalf("got %d resources", rms.Len())
	}
	res := rms.At(0).Resource()
	if got := attrStr(res, "container.id"); got != newCID {
		t.Errorf("container.id = %q, want the row's own %s (attrs %v)", got, newCID, res.Attributes().AsRaw())
	}
	if got := attrStr(res, "k8s.deployment.name"); got != "dep1" {
		t.Errorf("k8s.deployment.name = %q, want dep1 (pod enrichment lost)", got)
	}
}

// A split rule that asked for enrichment and did not get it must still carry a
// service.name: without one the series has no `job` label at all, so the SAME
// series becomes unselectable whenever the metadata lookup blips (and the miss
// is negative-cached for a minute).
func TestSplitterUnresolvedEnrichKeepsServiceName(t *testing.T) {
	srv := serveBody(t, ksmBody)
	target := testTarget(srv.URL)
	target.Pod.Name = "ksm-abc"
	target.Pod.Labels = map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{target}, Exporter: exp, StartTime: time.Now(),
		Splitters: ksmSplitters(t), Kubelet: KubeletConfig{Meta: &fakeMetaSource{}},
	})
	if _, err := s.scrapeTarget(context.Background(), target, s.cfg.Timeout); err != nil {
		t.Fatal(err)
	}
	rms := exp.batches[0].ResourceMetrics()
	found := false
	for i := 0; i < rms.Len(); i++ {
		res := rms.At(i).Resource()
		if attrStr(res, "k8s.pod.name") != "ghost" {
			continue
		}
		found = true
		if got := attrStr(res, "service.name"); got != "ghost" {
			t.Errorf("unresolved split resource service.name = %q, want the pod name: %v",
				got, res.Attributes().AsRaw())
		}
	}
	if !found {
		t.Fatal("ghost resource not produced")
	}
}

// A rule that never asked for enrichment keeps its label-only shape: minting a
// job label there would change series identity for a shape that never flapped,
// and an explicit `attributes` fallback must still win over the compensation.
func TestSplitterServiceNameFallbackScope(t *testing.T) {
	body := "# TYPE kube_pod_info gauge\n" +
		`kube_pod_info{namespace="ns2",pod="ghost"} 1` + "\n" +
		"# TYPE kube_pod_status_phase gauge\n" +
		`kube_pod_status_phase{namespace="ns2",pod="ghost"} 1` + "\n"
	run := func(t *testing.T, rule SplitRule) string {
		t.Helper()
		rule.Metrics = `kube_pod_info`
		rule.GroupBy = map[string]string{"namespace": "k8s.namespace.name", "pod": "k8s.pod.name"}
		sp, err := NewSplitters([]SplitterConfig{{
			Match: SplitterMatch{PodLabels: map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}},
			Rules: []SplitRule{rule},
		}})
		if err != nil {
			t.Fatal(err)
		}
		srv := serveBody(t, body)
		target := testTarget(srv.URL)
		target.Pod.Name = "ksm-abc"
		target.Pod.Labels = map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}
		exp := &captureExporter{}
		s := New(Config{
			Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
			Targets: staticTargets{target}, Exporter: exp, StartTime: time.Now(),
			Splitters: sp, Kubelet: KubeletConfig{Meta: &fakeMetaSource{}},
		})
		if _, err := s.scrapeTarget(context.Background(), target, s.cfg.Timeout); err != nil {
			t.Fatal(err)
		}
		rms := exp.batches[0].ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			if attrStr(rms.At(i).Resource(), "k8s.pod.name") == "ghost" {
				return attrStr(rms.At(i).Resource(), "service.name")
			}
		}
		t.Fatal("ghost resource not produced")
		return ""
	}
	if got := run(t, SplitRule{}); got != "" {
		t.Errorf("label-only rule gained service.name = %q", got)
	}
	if got := run(t, SplitRule{Enrich: true, Attributes: map[string]string{"service.name": "unknown"}}); got != "unknown" {
		t.Errorf("explicit fallback lost: service.name = %q, want unknown", got)
	}
}

// A groupBy entry mapping a label to an EMPTY attribute name is a silent
// dimension loss (the label leaves the data points, its value lands under an
// unqueryable empty resource key), so it must be refused where the sibling
// regexes are — inside -check-config.
func TestNewSplittersRejectsEmptyGroupByAttribute(t *testing.T) {
	_, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodLabels: map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}},
		Rules: []SplitRule{{
			Metrics: `kube_pod_.+`,
			GroupBy: map[string]string{"namespace": "k8s.namespace.name", "pod": ""},
		}},
	}})
	if err == nil {
		t.Fatal("empty groupBy attribute name accepted")
	}
	if !strings.Contains(err.Error(), `"pod"`) {
		t.Errorf("error does not name the offending label: %v", err)
	}
}
