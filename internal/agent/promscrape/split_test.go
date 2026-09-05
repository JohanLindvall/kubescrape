package promscrape

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// TestMemoSplitDropLabelsPerRule pins that the dropLabels memo is keyed by
// (rule, label) and not by label name alone: two rules disagreeing about the
// same label name must each get their own verdict.
func TestMemoSplitDropLabelsPerRule(t *testing.T) {
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodLabels: map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}},
		Rules: []SplitRule{
			{ // rule 0 drops "extra"
				Metrics:    `kube_a_.+`,
				GroupBy:    map[string]string{"namespace": "k8s.namespace.name"},
				DropLabels: `extra`,
			},
			{ // rule 1 drops "other" but KEEPS "extra"
				Metrics:    `kube_b_.+`,
				GroupBy:    map[string]string{"namespace": "k8s.namespace.name"},
				DropLabels: `other`,
			},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	body := `# TYPE kube_a_metric gauge
kube_a_metric{namespace="ns1",extra="e1",other="o1"} 1
# TYPE kube_b_metric gauge
kube_b_metric{namespace="ns1",extra="e2",other="o2"} 2
`
	srv := serveBody(t, body)
	target := testTarget(srv.URL)
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
	got := map[string]map[string]any{}
	rms := exp.batches[0].ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				got[m.Name()] = m.Gauge().DataPoints().At(0).Attributes().AsRaw()
			}
		}
	}
	if _, dropped := got["kube_a_metric"]["extra"]; dropped {
		t.Fatalf("rule 0 kept dropped label: %v", got["kube_a_metric"])
	}
	if _, ok := got["kube_a_metric"]["other"]; !ok {
		t.Fatalf("rule 0 dropped a label it should keep: %v", got["kube_a_metric"])
	}
	if _, ok := got["kube_b_metric"]["extra"]; !ok {
		t.Fatalf("rule 1 dropped 'extra' — dropLabels verdict leaked across rules: %v", got["kube_b_metric"])
	}
	if _, dropped := got["kube_b_metric"]["other"]; dropped {
		t.Fatalf("rule 1 kept dropped label: %v", got["kube_b_metric"])
	}
}

// TestMemoSplitRuleNilCached pins the map-miss vs stored-nil distinction in
// ruleMemo (families matching no rule stay on the target's own resource) and
// that the memo survives a chunk flush.
func TestMemoSplitRuleNilCached(t *testing.T) {
	body := `# TYPE ksm_own_metric gauge
ksm_own_metric{i="1"} 1
# TYPE kube_pod_info gauge
kube_pod_info{namespace="ns1",pod="pod1",uid="` + uid1 + `",node="node9"} 1
# TYPE ksm_own_metric2 gauge
ksm_own_metric2{i="2"} 2
# TYPE kube_namespace_labels gauge
kube_namespace_labels{namespace="ns1",label_team="core"} 1
`
	srv := serveBody(t, body)
	target := testTarget(srv.URL)
	target.Pod.Name = "ksm-abc"
	target.Pod.Labels = map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}

	run := func(batchPoints int) []string {
		exp := &captureExporter{}
		s := New(Config{
			Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
			Targets: staticTargets{target}, Exporter: exp, StartTime: time.Now(),
			Splitters: ksmSplitters(t), Kubelet: KubeletConfig{Meta: &fakeMetaSource{}},
			BatchPoints: batchPoints,
		})
		if _, err := s.scrapeTarget(context.Background(), target, s.cfg.Timeout); err != nil {
			t.Fatal(err)
		}
		return identitySeries(exp.batches)
	}
	whole := run(10000)
	chunked := run(1)
	if strings.Join(whole, "\n") != strings.Join(chunked, "\n") {
		t.Fatalf("chunked split scrape differs:\nwhole:\n%s\n\nchunked:\n%s",
			strings.Join(whole, "\n"), strings.Join(chunked, "\n"))
	}
	// The two non-matching families must land on the KSM pod's own resource,
	// i.e. share one resource carrying url.full.
	joined := strings.Join(whole, "\n")
	if !strings.Contains(joined, "ksm_own_metric|") || !strings.Contains(joined, "ksm_own_metric2|") {
		t.Fatalf("self-resource families missing: %s", joined)
	}
}

// richMeta resolves every pod with a realistic metadata set (owner chain, node,
// container), as the metadata service does in a real cluster: the split
// resources then carry ~12 attributes each.
type richMeta struct{}

func (richMeta) PodByName(_ context.Context, ns, name string) (*kubemeta.Pod, error) {
	return &kubemeta.Pod{
		Name: name, Namespace: ns, UID: fmt.Sprintf("%x-1111-2222-3333-444455556666", len(name)),
		NodeName: "ip-10-0-13-244.eu-west-1.compute.internal",
		Owners: []kubemeta.Owner{
			{Kind: "ReplicaSet", Name: name + "-7d8f9c", UID: "rs-uid-0000-1111"},
			{Kind: "Deployment", Name: "workload-deployment", UID: "dep-uid-0000-1111"},
		},
		Labels: map[string]string{"app.kubernetes.io/name": "workload"},
		Containers: []kubemeta.Container{{
			Name: "app", ID: "1111111111111111111111111111111111111111111111111111111111111111",
			Image: "registry.example.com/team/workload:1.2.3",
		}},
	}, nil
}

func (richMeta) Container(_ context.Context, _ string, _ time.Duration) (*kubemeta.ContainerMetadata, error) {
	return nil, errors.New("not found")
}

// TestSplitChunkBytesRespectGRPCLimit: the BatchBytes guard exists to stop a
// collector rejecting an oversized DECOMPRESSED message, which loses a
// target's metrics entirely — so the ESTIMATE it flushes on has to cover
// everything the chunk encodes, not just the data points.
//
// For the split (kube-state-metrics) and cadvisor batchers a chunk holds one
// ResourceMetrics PER DESCRIBED OBJECT, each with a full resource attribute
// set plus its own copy of every metric descriptor; a rule demoting resource
// attributes onto the points repeats those on every point. Each of those was
// once uncounted, and each on its own put the encoded message 1.7-2.5x over
// its estimate and past the 4 MiB gRPC default.
//
// Each case below isolates ONE of those omissions, with the guard that would
// otherwise mask it taken out of the way — a case where two estimate errors
// cancel proves nothing, which is what the last two cases are about.
func TestSplitChunkBytesRespectGRPCLimit(t *testing.T) {
	const objects = 12000
	body := func(fams ...string) string {
		var sb strings.Builder
		for _, f := range fams {
			fmt.Fprintf(&sb, "# TYPE %s gauge\n", f)
			for i := 0; i < objects; i++ {
				fmt.Fprintf(&sb, "%s{namespace=\"namespace-%d\",pod=\"workload-deployment-%d-abcde\",uid=\"%08x-1111-2222-3333-444455556666\",node=\"node9\",phase=\"Running\"} 1\n", f, i%50, i, i)
			}
		}
		return sb.String()
	}

	// leanBody carries ONE label per series, so the estimate's habitual
	// over-charging of stripped groupBy labels is nearly absent and cannot
	// silently make up for an uncharged data-point attribute.
	leanBody := func() string {
		var sb strings.Builder
		for _, f := range []string{"kube_pod_status_ready", "kube_pod_status_phase"} {
			fmt.Fprintf(&sb, "# TYPE %s gauge\n", f)
			for i := 0; i < objects; i++ {
				fmt.Fprintf(&sb, "%s{pod=\"workload-deployment-%d-abcde\"} 1\n", f, i)
			}
		}
		return sb.String()
	}()

	cases := []struct {
		name        string
		body        string
		meta        MetaSource
		splitters   []*Splitter
		batchPoints int // 0 = default 10k
	}{
		// The byte bound as the only guard (a deployment raising -metrics-batch-points):
		// chunks flush at the 3 MiB estimate and encode to > 5 MiB.
		{"byte-bound-only", body("kube_pod_info", "kube_pod_status_phase", "kube_pod_owner"),
			&fakeMetaSource{}, ksmSplitters(t), 1 << 20},
		// Stock defaults, with the split resources enriched as in a real
		// cluster (~12 attributes each). The BYTE bound is what fires here —
		// at ~4k points, well short of the 10k cap — precisely BECAUSE
		// resourceBytes is charged; while it was not, the point cap was the
		// only thing stopping the chunk and 10k enriched per-object resources
		// encoded past 4 MiB.
		{"defaults-enriched", body("kube_pod_info"), richMeta{}, enrichSplitters(t), 0},
		// Several attributes DEMOTED onto every data point. They are removed
		// from the resource before it is charged and the per-point estimators
		// only measure the sample's own labels, so #attrs x #points bytes were
		// encoded and counted nowhere — the one case where the estimate's
		// habitual over-charging of stripped labels does not cancel it out.
		{"datapoint-attributes", body("kube_pod_info"), richMeta{}, demotingSplitters(t), 0},
		// The case that actually CROSSES the limit when the demoted attributes
		// go uncharged, and the reason the one above does not: there, every
		// demoted attribute came from a groupBy LABEL, and the estimate
		// over-charges stripped labels by almost exactly what it was missing —
		// the two cancelled, leaving the chunk under the limit either way.
		//
		// Here the demoted attributes are rule `attributes` (properties of the
		// described object, the cmb-alloy placement) with no label behind them
		// to over-charge, the series carry one label instead of five, and the
		// point cap is out of the way — so the byte bound is the only thing
		// deciding chunk size and it is deciding it on numbers that are ~60%
		// wrong. The chunk grows past 4 MiB and every export of the target is
		// rejected.
		{"demoted-object-attributes", leanBody, &fakeMetaSource{}, objectAttrSplitters(t), 1 << 20},
	}
	var m pmetric.ProtoMarshaler
	const grpcLimit = 4 << 20
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := serveBody(t, tc.body)
			target := testTarget(srv.URL)
			target.Pod.Name = "ksm-abc"
			target.Pod.Labels = map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}
			exp := &captureExporter{}
			s := New(Config{
				Node: "node1", Interval: time.Hour, Timeout: time.Minute,
				Targets: staticTargets{target}, Exporter: exp, StartTime: time.Now(),
				Splitters: tc.splitters, Kubelet: KubeletConfig{Meta: tc.meta},
				BatchPoints: tc.batchPoints,
			})
			if _, err := s.scrapeTarget(context.Background(), target, s.cfg.Timeout); err != nil {
				t.Fatal(err)
			}
			for i, md := range exp.batches {
				buf, err := m.MarshalMetrics(md)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("chunk %d: %d points, %d resources, %d encoded bytes (BatchBytes=%d)",
					i, md.DataPointCount(), md.ResourceMetrics().Len(), len(buf), defaultBatchBytes)
				if len(buf) > grpcLimit {
					t.Errorf("chunk %d encodes to %d bytes — past the 4 MiB gRPC default the BatchBytes guard exists to respect; the size estimate counts data points only and ignores %d per-object resources",
						i, len(buf), md.ResourceMetrics().Len())
				}
			}
		})
	}
}

// enrichSplitters groups by namespace/pod/node (no uid, so enrichment is not
// uid-cross-checked) with enrichment on — the standard KSM setup.
func enrichSplitters(t *testing.T) []*Splitter {
	t.Helper()
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodLabels: map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}},
		Rules: []SplitRule{{
			Metrics: `kube_pod_.+`,
			GroupBy: map[string]string{"namespace": "k8s.namespace.name", "pod": "k8s.pod.name", "node": "k8s.node.name"},
			Enrich:  true,
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return sp
}

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

// demotingSplitters moves several attributes off the resource onto every data
// point, which is what datapointAttributes is for.
func demotingSplitters(t *testing.T) []*Splitter {
	t.Helper()
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodLabels: map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}},
		Rules: []SplitRule{{
			Metrics: `kube_pod_.+`,
			GroupBy: map[string]string{
				"namespace": "k8s.namespace.name", "pod": "k8s.pod.name",
				"uid": "k8s.pod.uid", "node": "k8s.node.name", "phase": "k8s.pod.phase",
			},
			DatapointAttributes: &[]string{"k8s.node.name", "k8s.pod.phase", "k8s.pod.uid", "k8s.namespace.name"},
			Enrich:              true,
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return sp
}

// objectAttrSplitters demotes attributes that came from the rule's own
// `attributes` fallbacks rather than from a groupBy label — the described
// object's cluster/zone/node, which is exactly what datapointAttributes is
// for. Nothing in the series over-charges for them, so if dpaBytes stops
// counting them the estimate is simply missing them.
func objectAttrSplitters(t *testing.T) []*Splitter {
	t.Helper()
	demoted := []string{
		"k8s.node.name", "k8s.cluster.name", "cloud.availability_zone",
		"cloud.region", "cloud.account.id", "k8s.node.instance_type",
	}
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodLabels: map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}},
		Rules: []SplitRule{{
			Metrics: `kube_pod_.+`,
			GroupBy: map[string]string{"pod": "k8s.pod.name"},
			Attributes: map[string]string{
				"k8s.node.name":           "ip-10-0-13-244.eu-west-1.compute.internal",
				"k8s.cluster.name":        "prod-eu-west-1-blue",
				"cloud.availability_zone": "eu-west-1a",
				"cloud.region":            "eu-west-1",
				"cloud.account.id":        "123456789012",
				"k8s.node.instance_type":  "m6i.4xlarge",
			},
			DatapointAttributes: &demoted,
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return sp
}

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
	srv := serveBody(t, ksmBody)

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
	if _, err := s.scrapeTarget(context.Background(), target, s.cfg.Timeout); err != nil {
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
	srv := serveBody(t, ksmBody)

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
	if _, err := s.scrapeTarget(context.Background(), target, s.cfg.Timeout); err != nil {
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
		t.Helper()
		srv := serveBody(t, body)
		target := testTarget(srv.URL) // owner Deployment "dep1" -> service.name dep1
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
	srv := serveBody(t, body)

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
	if _, err := s.scrapeTarget(context.Background(), target, s.cfg.Timeout); err != nil {
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

// A split rule's `attributes` fallbacks are applied AFTER Build, i.e. after the
// operator's global enable/disable filter has run — so they have to be filtered
// again, exactly as the tailer re-filters its post-Build source statics. The
// filter is global by design (a `pipelines:` section may not set enable/disable
// at all), so a rule fallback silently surviving a `disable` list is the filter
// not being global after all.
func TestSplitterAttributesRespectTheAttributeFilter(t *testing.T) {
	body := "# TYPE kube_pod_info gauge\n" +
		`kube_pod_info{namespace="ns1",pod="pod1"} 1` + "\n"
	srv := serveBody(t, body)

	target := testTarget(srv.URL)
	target.Pod.Name = "ksm-abc"
	target.Pod.Labels = map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}

	filter, err := attrs.NewFilterFromLists(nil, []string{`cost\.center`})
	if err != nil {
		t.Fatal(err)
	}
	builder, err := attrs.NewBuilder(&attrs.Config{}, filter)
	if err != nil {
		t.Fatal(err)
	}
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodLabels: map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}},
		Rules: []SplitRule{{
			Metrics: `kube_pod_.+`,
			GroupBy: map[string]string{"namespace": "k8s.namespace.name", "pod": "k8s.pod.name"},
			Attributes: map[string]string{
				"cost.center":  "cc-42",   // disabled: must not reach the resource
				"service.name": "unknown", // kept: the fallback still works
			},
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
		Attrs: &attrs.Builders{Targets: builder},
	})
	if _, err := s.scrapeTarget(context.Background(), target, s.cfg.Timeout); err != nil {
		t.Fatal(err)
	}
	rms := exp.batches[0].ResourceMetrics()
	found := false
	for i := 0; i < rms.Len(); i++ {
		res := rms.At(i).Resource()
		if attrStr(res, "k8s.pod.name") != "pod1" {
			continue
		}
		found = true
		if v, ok := res.Attributes().Get("cost.center"); ok {
			t.Errorf("disabled attribute survived as a rule fallback: cost.center=%q", v.Str())
		}
		if got := attrStr(res, "service.name"); got != "unknown" {
			t.Errorf("service.name = %q, want the rule's fallback: the re-filter must not eat allowed attributes", got)
		}
	}
	if !found {
		t.Fatal("pod1 resource not produced")
	}
}

func TestSplitterAttributesDontOverride(t *testing.T) {
	body := "# TYPE kube_pod_info gauge\n" +
		`kube_pod_info{namespace="ns1",pod="pod1",uid="` + uid1 + `"} 1` + "\n"
	srv := serveBody(t, body)

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
	if _, err := s.scrapeTarget(context.Background(), target, s.cfg.Timeout); err != nil {
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

// Two rules with equal-cardinality groupBy sets must not merge objects whose
// label VALUES collide: kube_pod_info{pod="worker-1"} and
// kube_node_info{node="worker-1"} describe different objects.
func TestSplitterRuleIdentityInResourceKey(t *testing.T) {
	body := `# TYPE kube_pod_info gauge
kube_pod_info{pod="worker-1"} 1
# TYPE kube_node_info gauge
kube_node_info{node="worker-1"} 1
`
	srv := serveBody(t, body)

	target := testTarget(srv.URL)
	target.Pod.Name = "ksm-abc"
	target.Pod.Labels = map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodLabels: map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}},
		Rules: []SplitRule{
			{
				Metrics:             `kube_pod_info`,
				GroupBy:             map[string]string{"pod": "k8s.pod.name"},
				DatapointAttributes: &[]string{},
			},
			{
				Metrics:             `kube_node_info`,
				GroupBy:             map[string]string{"node": "k8s.node.name"},
				DatapointAttributes: &[]string{},
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
	if _, err := s.scrapeTarget(context.Background(), target, s.cfg.Timeout); err != nil {
		t.Fatal(err)
	}

	rms := exp.batches[0].ResourceMetrics()
	if rms.Len() != 2 {
		t.Fatalf("got %d resources, want 2 (rules must not merge on value collision)", rms.Len())
	}
	var sawPod, sawNode bool
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		names := metricNames(rm)
		switch {
		case attrStr(rm.Resource(), "k8s.pod.name") == "worker-1":
			sawPod = true
			if len(names) != 1 || names[0] != "kube_pod_info" {
				t.Errorf("pod resource metrics = %v", names)
			}
		case attrStr(rm.Resource(), "k8s.node.name") == "worker-1":
			sawNode = true
			if len(names) != 1 || names[0] != "kube_node_info" {
				t.Errorf("node resource metrics = %v", names)
			}
		}
	}
	if !sawPod || !sawNode {
		t.Fatalf("missing split resources: pod=%v node=%v", sawPod, sawNode)
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

// Histograms and summaries route through the splitter with their shape intact.
func TestSplitterHistogramSummary(t *testing.T) {
	body := `# TYPE kube_pod_wait_seconds histogram
kube_pod_wait_seconds_bucket{namespace="ns1",pod="pod1",le="0.1"} 3
kube_pod_wait_seconds_bucket{namespace="ns1",pod="pod1",le="1"} 5
kube_pod_wait_seconds_bucket{namespace="ns1",pod="pod1",le="+Inf"} 6
kube_pod_wait_seconds_sum{namespace="ns1",pod="pod1"} 4.2
kube_pod_wait_seconds_count{namespace="ns1",pod="pod1"} 6
# TYPE kube_pod_size_bytes summary
kube_pod_size_bytes{namespace="ns1",pod="pod1",quantile="0.5"} 100
kube_pod_size_bytes{namespace="ns1",pod="pod1",quantile="0.99"} 250
kube_pod_size_bytes_sum{namespace="ns1",pod="pod1"} 1000
kube_pod_size_bytes_count{namespace="ns1",pod="pod1"} 8
`
	srv := serveBody(t, body)

	target := testTarget(srv.URL)
	target.Pod.Name = "ksm-abc"
	target.Pod.Labels = map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodLabels: map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}},
		Rules: []SplitRule{{
			Metrics: `kube_pod_.+`,
			GroupBy: map[string]string{"namespace": "k8s.namespace.name", "pod": "k8s.pod.name"},
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
	if _, err := s.scrapeTarget(context.Background(), target, s.cfg.Timeout); err != nil {
		t.Fatal(err)
	}

	rms := exp.batches[0].ResourceMetrics()
	var hist, summ pmetric.Metric
	for i := 0; i < rms.Len(); i++ {
		if attrStr(rms.At(i).Resource(), "k8s.pod.name") != "pod1" {
			continue
		}
		ms := rms.At(i).ScopeMetrics().At(0).Metrics()
		for j := 0; j < ms.Len(); j++ {
			switch ms.At(j).Name() {
			case "kube_pod_wait_seconds":
				hist = ms.At(j)
			case "kube_pod_size_bytes":
				summ = ms.At(j)
			}
		}
	}
	if hist.Type() != pmetric.MetricTypeHistogram {
		t.Fatalf("histogram not routed: %v", hist.Type())
	}
	hdp := hist.Histogram().DataPoints().At(0)
	if hdp.Count() != 6 || hdp.Sum() != 4.2 || hdp.ExplicitBounds().Len() != 2 {
		t.Fatalf("histogram dp = count %d sum %v bounds %d", hdp.Count(), hdp.Sum(), hdp.ExplicitBounds().Len())
	}
	if summ.Type() != pmetric.MetricTypeSummary {
		t.Fatalf("summary not routed: %v", summ.Type())
	}
	sdp := summ.Summary().DataPoints().At(0)
	if sdp.Count() != 8 || sdp.Sum() != 1000 || sdp.QuantileValues().Len() != 2 {
		t.Fatalf("summary dp = count %d sum %v quantiles %d", sdp.Count(), sdp.Sum(), sdp.QuantileValues().Len())
	}
}

// Every groupBy label must reach the resource as its mapped attribute, whether
// or not the pod lookup resolved. putSplitLabels strips them from the data
// points unconditionally, so writing them only on the enrichment-failed path
// destroyed any groupBy attribute the k8s attribute build does not itself
// produce: the rows then collapse onto one identical resource as
// indistinguishable duplicate points, and only one survives downstream.
func TestSplitterGroupByAttrsSurviveEnrichment(t *testing.T) {
	body := `# TYPE kube_pod_container_resource_requests gauge
kube_pod_container_resource_requests{namespace="ns1",pod="pod1",resource="cpu"} 0.5
kube_pod_container_resource_requests{namespace="ns1",pod="pod1",resource="memory"} 1024
`
	srv := serveBody(t, body)
	target := testTarget(srv.URL)
	target.Pod.Name = "ksm-abc"
	target.Pod.Labels = map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}

	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodLabels: map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}},
		Rules: []SplitRule{{
			Metrics: `kube_pod_container_resource_.+`,
			// namespace/pod resolve through the metadata service; `resource` is
			// a groupBy attribute nothing else can supply.
			GroupBy: map[string]string{"namespace": "k8s.namespace.name", "pod": "k8s.pod.name", "resource": "k8s.resource_name"},
			Enrich:  true,
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
	if _, err := s.scrapeTarget(context.Background(), target, s.cfg.Timeout); err != nil {
		t.Fatal(err)
	}

	seen := map[string]bool{}
	rms := exp.batches[0].ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		if v := attrStr(rm.Resource(), "k8s.resource_name"); v != "" {
			seen[v] = true
		}
	}
	for _, want := range []string{"cpu", "memory"} {
		if !seen[want] {
			t.Errorf("no resource carries k8s.resource_name=%s; the groupBy attribute was stripped from the points and never written (got %v)", want, seen)
		}
	}
}

// TestSplitterCoalescingGroupByMergesSpellings pins that the route key is the
// RENDERED identity — one effective value per distinct ATTRIBUTE — not the
// per-label value vector. Several groupBy labels may map to one attribute
// (namespace vs exported_namespace, the relabeled-exporter shape), and a
// per-label key minted one resource per SPELLING: two rows naming the same
// object through different labels rendered byte-identical attribute sets on
// two ResourceMetrics, whose points — every groupBy label stripped by
// putSplitLabels — became byte-identical duplicate series in one payload, the
// exact duplicate class the parser rejects per line.
func TestSplitterCoalescingGroupByMergesSpellings(t *testing.T) {
	body := `# TYPE kube_namespace_status_phase gauge
kube_namespace_status_phase{namespace="ns1",phase="Active"} 1
kube_namespace_status_phase{exported_namespace="ns1",phase="Terminating"} 0
`
	srv := serveBody(t, body)
	target := testTarget(srv.URL)
	target.Pod.Name = "ksm-abc"
	target.Pod.Labels = map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodLabels: map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}},
		Rules: []SplitRule{{
			Metrics: `kube_namespace_.+`,
			GroupBy: map[string]string{
				"namespace":          "k8s.namespace.name",
				"exported_namespace": "k8s.namespace.name",
			},
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
	if _, err := s.scrapeTarget(context.Background(), target, s.cfg.Timeout); err != nil {
		t.Fatal(err)
	}

	rms := exp.batches[0].ResourceMetrics()
	if rms.Len() != 1 {
		t.Fatalf("got %d resources, want 1: rows spelling one object through different groupBy labels must share a resource", rms.Len())
	}
	if got := attrStr(rms.At(0).Resource(), "k8s.namespace.name"); got != "ns1" {
		t.Fatalf("k8s.namespace.name = %q, want ns1", got)
	}
	ms := rms.At(0).ScopeMetrics().At(0).Metrics()
	if ms.Len() != 1 {
		t.Fatalf("got %d metrics, want 1: %v", ms.Len(), metricNames(rms.At(0)))
	}
	dps := ms.At(0).Gauge().DataPoints()
	if dps.Len() != 2 {
		t.Fatalf("got %d data points, want both rows on the one merged resource", dps.Len())
	}
	phases := map[string]bool{}
	for i := 0; i < dps.Len(); i++ {
		for _, l := range []string{"namespace", "exported_namespace"} {
			if _, leaked := dps.At(i).Attributes().Get(l); leaked {
				t.Fatalf("grouped label %s leaked to data point: %v", l, dps.At(i).Attributes().AsRaw())
			}
		}
		v, _ := dps.At(i).Attributes().Get("phase")
		phases[v.Str()] = true
	}
	if !phases["Active"] || !phases["Terminating"] {
		t.Fatalf("missing data points: %v", phases)
	}
}

// The other half of the coalescing contract: rows naming genuinely DIFFERENT
// objects keep distinct resources, and when both coalescing labels are set the
// LAST non-empty in sorted-label order wins — the same precedence the rendered
// attributes give it (non-empty overwrites), so the key can never disagree
// with the resource it selects.
func TestSplitterCoalescingGroupByKeepsDistinctObjects(t *testing.T) {
	// Sorted-label coalesce order is exported_namespace, then namespace: the
	// third row must key on "ns1", not "shadow".
	body := `# TYPE kube_namespace_status_phase gauge
kube_namespace_status_phase{namespace="ns1",phase="Active"} 1
kube_namespace_status_phase{exported_namespace="ns2",phase="Active"} 1
kube_namespace_status_phase{exported_namespace="shadow",namespace="ns1",phase="Terminating"} 0
`
	srv := serveBody(t, body)
	target := testTarget(srv.URL)
	target.Pod.Name = "ksm-abc"
	target.Pod.Labels = map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodLabels: map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}},
		Rules: []SplitRule{{
			Metrics: `kube_namespace_.+`,
			GroupBy: map[string]string{
				"namespace":          "k8s.namespace.name",
				"exported_namespace": "k8s.namespace.name",
			},
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
	if _, err := s.scrapeTarget(context.Background(), target, s.cfg.Timeout); err != nil {
		t.Fatal(err)
	}

	// Exactly TWO resources: counting points per namespace alone would also
	// pass on the old per-label keying, which minted a third resource for the
	// both-labels row and still rendered it as ns1.
	rms := exp.batches[0].ResourceMetrics()
	if rms.Len() != 2 {
		t.Fatalf("got %d resources, want 2 (ns1 and ns2)", rms.Len())
	}
	points := map[string]int{}
	for i := 0; i < rms.Len(); i++ {
		ns := attrStr(rms.At(i).Resource(), "k8s.namespace.name")
		points[ns] += rms.At(i).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().Len()
	}
	if points["ns1"] != 2 || points["ns2"] != 1 {
		t.Fatalf("points per namespace = %v, want ns1:2 ns2:1 (namespace beats the earlier-sorted exported_namespace)", points)
	}
}

// TestSplitterCoalescingExtractionKeepsNonEmptyIdentity pins the identity
// EXTRACTION feeding resolveContext against the same last-non-empty coalesce:
// it used to assign per groupBy entry unconditionally, so a coalescing rule
// whose later-sorted label was EMPTY blanked the pod name the row did carry —
// enrichment then failed (or resolved another object) while the rendered
// attributes, which skip empties, still named the right one.
func TestSplitterCoalescingExtractionKeepsNonEmptyIdentity(t *testing.T) {
	body := "# TYPE kube_pod_info gauge\n" +
		`kube_pod_info{namespace="ns1",pod="pod1"} 1` + "\n"
	srv := serveBody(t, body)
	target := testTarget(srv.URL)
	target.Pod.Name = "ksm-abc"
	target.Pod.Labels = map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodLabels: map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}},
		Rules: []SplitRule{{
			Metrics: `kube_pod_.+`,
			// "pod" and "pod_name" coalesce onto k8s.pod.name; "pod_name"
			// sorts LAST and is absent from the row.
			GroupBy: map[string]string{
				"namespace": "k8s.namespace.name",
				"pod":       "k8s.pod.name",
				"pod_name":  "k8s.pod.name",
			},
			Enrich: true,
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
	if _, err := s.scrapeTarget(context.Background(), target, s.cfg.Timeout); err != nil {
		t.Fatal(err)
	}

	rms := exp.batches[0].ResourceMetrics()
	if rms.Len() != 1 {
		t.Fatalf("got %d resources, want 1", rms.Len())
	}
	res := rms.At(0).Resource()
	// Only a successful pod resolution can supply the uid and the owner chain:
	// the row carries neither.
	if got := attrStr(res, "k8s.pod.uid"); got != uid1 {
		t.Fatalf("k8s.pod.uid = %q, want %s: the empty pod_name label blanked the extracted pod name and enrichment never resolved (attrs %v)",
			got, uid1, res.Attributes().AsRaw())
	}
	if got := attrStr(res, "k8s.deployment.name"); got != "dep1" {
		t.Fatalf("k8s.deployment.name = %q, want dep1 (attrs %v)", got, res.Attributes().AsRaw())
	}
}

// A kube-state-metrics exposition is FAMILY-major: consecutive rows describe
// DIFFERENT objects, so the batcher's last-seen memos miss on essentially every
// row and the key path underneath them is what runs. It built the resource key
// in a strings.Builder and the metric key by concatenation — two allocations per
// row, ~500k per cycle at 12k pods — where the sibling cadvisor batcher had
// already established the reused-scratch + map[string(buf)] discipline.
func TestSplitBatcherRoutingIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodName: "ksm-.+"},
		Rules: []SplitRule{{
			Metrics: `kube_pod_.+`,
			GroupBy: map[string]string{"namespace": "k8s.namespace.name", "pod": "k8s.pod.name", "uid": "k8s.pod.uid"},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: time.Second,
		Targets: staticTargets{}, Exporter: &captureExporter{},
		Splitters: sp, Kubelet: KubeletConfig{Meta: &fakeMetaSource{}},
		StartTime: time.Unix(1, 0),
	})
	target := testTarget("http://ksm:8080/metrics")
	target.Pod.Name = "ksm-abc"
	cb := newSplitBatcher(context.Background(), s, target, sp[0], time.Unix(2, 0))

	// Two described objects of one family, visited alternately: every call is a
	// last-seen memo MISS and a map HIT, which is the steady state of a
	// family-major exposition.
	rows := [2][]Label{}
	for i := range rows {
		rows[i] = []Label{
			{Name: "namespace", Value: "prod-payments"},
			{Name: "pod", Value: fmt.Sprintf("payments-6f7b9c%03d", i)},
			{Name: "uid", Value: fmt.Sprintf("0a1b2c3d-1111-2222-3333-4444555%05d", i)},
			{Name: "phase", Value: "Running"},
		}
	}
	shape := func(m pmetric.Metric) { shapeNumber(m, false) }
	for i := range rows {
		cb.metric("kube_pod_status_phase", metricMeta{}, rows[i], shape) // create both resources and metrics
	}
	i := 0
	allocs := testing.AllocsPerRun(200, func() {
		cb.metric("kube_pod_status_phase", metricMeta{}, rows[i%2], shape)
		i++
	})
	if allocs != 0 {
		t.Fatalf("routing an already-known object allocates %v times, want 0: the resource or metric key is being materialized per row", allocs)
	}
}
