package promscrape

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// serveBody returns a test server serving a fixed exposition body.
func serveBody(t *testing.T, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// identitySeries flattens every exported batch into "resourcekey|metric|dpattrs=value"
// strings, so two runs can be compared regardless of chunking.
func identitySeries(batches []pmetric.Metrics) []string {
	var out []string
	for _, md := range batches {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			rm := rms.At(i)
			res := fmt.Sprint(rm.Resource().Attributes().AsRaw())
			sms := rm.ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)
					var dps pmetric.NumberDataPointSlice
					switch m.Type() {
					case pmetric.MetricTypeSum:
						dps = m.Sum().DataPoints()
					case pmetric.MetricTypeGauge:
						dps = m.Gauge().DataPoints()
					default:
						continue
					}
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						out = append(out, fmt.Sprintf("%s|%s|%v|%v", res, m.Name(), dp.Attributes().AsRaw(), dp.DoubleValue()))
					}
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

// TestMemoCadvisorChunkingStable pins that the per-scrape cgroupMemo (which
// deliberately survives take()/reset()) produces identical series whether the
// scrape is exported as one chunk or as many.
func TestMemoCadvisorChunkingStable(t *testing.T) {
	srv := serveBody(t, string(cadvisorBody))

	run := func(batchPoints int) []string {
		exp := &captureExporter{}
		s := newKubeletScraper(t, srv.URL, &fakeMetaSource{}, exp, false)
		s.cfg.BatchPoints = batchPoints
		if _, err := s.scrapeCadvisor(context.Background()); err != nil {
			t.Fatal(err)
		}
		return identitySeries(exp.batches)
	}

	whole := run(10000)
	chunked := run(1) // export after every single point
	if len(whole) == 0 {
		t.Fatal("no series")
	}
	if strings.Join(whole, "\n") != strings.Join(chunked, "\n") {
		t.Fatalf("chunked scrape differs from whole scrape:\nwhole:\n%s\n\nchunked:\n%s",
			strings.Join(whole, "\n"), strings.Join(chunked, "\n"))
	}
}

// TestMemoCadvisorSandboxOrderIndependent feeds a sandbox (container="POD") row
// and a container row that share one cgroup id — the adversarial case for a memo
// keyed on the raw id value — in both orders. The memo must hold only the parse
// result, never the sandbox-adjusted identity, so the outcome must not depend on
// which row was seen first.
func TestMemoCadvisorSandboxOrderIndependent(t *testing.T) {
	id := "/kubepods/burstable/pod" + uid1 + "/" + appCID
	sandboxRow := fmt.Sprintf("container_cpu_usage_seconds_total{namespace=%q,pod=%q,container=\"POD\",id=%q} 0.1\n", "ns1", "pod1", id)
	containerRow := fmt.Sprintf("container_cpu_usage_seconds_total{namespace=%q,pod=%q,container=%q,id=%q,image=\"img:1\"} 12.5\n", "ns1", "pod1", "app", id)
	head := "# TYPE container_cpu_usage_seconds_total counter\n"

	run := func(body string) []string {
		srv := serveBody(t, body)
		exp := &captureExporter{}
		s := newKubeletScraper(t, srv.URL, &fakeMetaSource{}, exp, false)
		if _, err := s.scrapeCadvisor(context.Background()); err != nil {
			t.Fatal(err)
		}
		return identitySeries(exp.batches)
	}

	a := run(head + sandboxRow + containerRow)
	b := run(head + containerRow + sandboxRow)
	if strings.Join(a, "\n") != strings.Join(b, "\n") {
		t.Fatalf("identity depends on row order:\nsandbox-first:\n%s\n\ncontainer-first:\n%s",
			strings.Join(a, "\n"), strings.Join(b, "\n"))
	}
	// Sanity: the sandbox row must NOT carry container.id / image on its resource.
	if strings.Count(strings.Join(a, "\n"), appCID) == 0 {
		t.Fatalf("container row lost its container id: %s", strings.Join(a, "\n"))
	}
}

// TestMemoCadvisorLargeBodyNoAliasing is the memo-key-lifetime attack: the
// cgroupMemo retains the raw "id" label value across the whole scrape, so if the
// parser ever handed out a string aliasing its reused read buffer the memo would
// misattribute series. It forces every dangerous condition at once — a body far
// larger than the 64 KiB read buffer (ReadSlice recycles it), ids longer than
// the value-intern length cap (128) so they are never interned, and more
// distinct label values than MaxInternedValues (8192) so the intern table fills
// and later values are fresh allocations. Each sample's VALUE encodes the index
// of its own container, so any misattribution shows up as a resource whose
// container.id does not match its data point's value.
func TestMemoCadvisorLargeBodyNoAliasing(t *testing.T) {
	const n = 3000
	uid := func(i int) string { return fmt.Sprintf("%08x-1111-2222-3333-%012x", i, i) }
	cid := func(i int) string { return fmt.Sprintf("%064x", i) }
	// systemd layout: > 128 chars, so the value is never interned.
	id := func(i int) string {
		return "/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod" +
			strings.ReplaceAll(uid(i), "-", "_") + ".slice/cri-containerd-" + cid(i) + ".scope"
	}
	var sb strings.Builder
	for _, fam := range []string{"container_cpu_usage_seconds_total", "container_memory_usage_bytes"} {
		fmt.Fprintf(&sb, "# TYPE %s gauge\n", fam)
		for i := 0; i < n; i++ {
			fmt.Fprintf(&sb, "%s{namespace=\"ns%d\",pod=\"pod-%d\",container=\"app-%d\",id=%q} %d\n",
				fam, i, i, i, id(i), i)
		}
	}
	if sb.Len() < 64*1024 {
		t.Fatalf("body too small to exercise the read buffer: %d", sb.Len())
	}
	srv := serveBody(t, sb.String())
	exp := &captureExporter{}
	s := newKubeletScraper(t, srv.URL, &fakeMetaSource{}, exp, false)
	s.cfg.BatchPoints = 777 // force many chunk flushes mid-scrape
	if _, err := s.scrapeCadvisor(context.Background()); err != nil {
		t.Fatal(err)
	}
	points := 0
	for _, md := range exp.batches {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			rm := rms.At(i)
			gotCID := attrStr(rm.Resource(), "container.id")
			gotUID := attrStr(rm.Resource(), "k8s.pod.uid")
			gotName := attrStr(rm.Resource(), "k8s.container.name")
			sms := rm.ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					dps := ms.At(k).Gauge().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						idx := int(dps.At(d).DoubleValue())
						points++
						if gotCID != cid(idx) || gotUID != uid(idx) || gotName != fmt.Sprintf("app-%d", idx) {
							t.Fatalf("sample %d landed on resource uid=%s cid=%s name=%s (want uid=%s cid=%s)",
								idx, gotUID, gotCID, gotName, uid(idx), cid(idx))
						}
					}
				}
			}
		}
	}
	if points != 2*n {
		t.Fatalf("got %d points, want %d", points, 2*n)
	}
}

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
	if _, err := s.scrapeTarget(context.Background(), target); err != nil {
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
		if _, err := s.scrapeTarget(context.Background(), target); err != nil {
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

// TestSplitChunkBytesRespectGRPCLimit: the BatchBytes guard (added to stop a
// collector rejecting an oversized DECOMPRESSED message, which loses a target's
// metrics entirely) estimates a chunk's size from its DATA POINTS only —
// numberBytes/histBytes/summBytes in convert.go. For the split
// (kube-state-metrics) and cadvisor batchers a chunk holds one ResourceMetrics
// PER DESCRIBED OBJECT, each carrying a full resource attribute set plus a
// per-resource copy of every metric's name/descriptor. None of that is counted,
// so the encoded message runs ~1.7-2.5x the estimate and exceeds the 4 MiB gRPC
// default the guard exists to stay under.
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
		// Stock defaults, but the split resources enriched as in a real cluster:
		// the 10k-point bound flushes chunks that still encode past 4 MiB, and the
		// byte guard never fires because its estimate ignores the resources.
		{"defaults-enriched", body("kube_pod_info"), richMeta{}, enrichSplitters(t), 0},
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
			if _, err := s.scrapeTarget(context.Background(), target); err != nil {
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
