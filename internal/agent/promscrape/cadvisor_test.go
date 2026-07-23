package promscrape

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

const (
	uid1     = "0a1b2c3d-1111-2222-3333-444455556666"
	ghostUID = "9f8e7d6c-9999-8888-7777-666655554444"
	appCID   = "d4f00c1e8a2b4c5d6e7f80912a3b4c5d6e7f80912a3b4c5d6e7f80912a3b4c5d"
	pauseCID = "eeeeffff00001111222233334444555566667777888899990000aaaabbbbcccc"
	ghostCID = "1234123412341234123412341234123412341234123412341234123412341234"
)

var pod1Meta = kubemeta.Pod{
	Name: "pod1", Namespace: "ns1", UID: uid1, NodeName: "node1",
	Owners: []kubemeta.Owner{{Kind: "Deployment", Name: "dep1", UID: "d1"}},
	Containers: []kubemeta.Container{
		{Name: "app", ID: appCID, Image: "img:1"},
	},
}

type fakeMetaSource struct {
	podCalls       atomic.Int64
	containerCalls atomic.Int64
}

func (f *fakeMetaSource) PodByName(_ context.Context, namespace, name string) (*kubemeta.Pod, error) {
	f.podCalls.Add(1)
	if namespace == "ns1" && name == "pod1" {
		p := pod1Meta
		return &p, nil
	}
	return nil, errors.New("not found")
}

func (f *fakeMetaSource) Container(_ context.Context, id string, _ time.Duration) (*kubemeta.ContainerMetadata, error) {
	f.containerCalls.Add(1)
	if id == appCID {
		p := pod1Meta
		return &kubemeta.ContainerMetadata{ContainerID: id, Container: p.Containers[0], Pod: p}, nil
	}
	return nil, errors.New("not found")
}

var cadvisorBody = strings.NewReplacer(
	"UID1", uid1, "GHOSTUID", ghostUID,
	"APPCID", appCID, "PAUSECID", pauseCID, "GHOSTCID", ghostCID,
).Replace(`# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{namespace="ns1",pod="pod1",container="app",id="/kubepods/burstable/podUID1/APPCID",image="img:1"} 12.5
container_cpu_usage_seconds_total{namespace="ns1",pod="pod1",container="POD",id="/kubepods/burstable/podUID1/PAUSECID"} 0.1
container_cpu_usage_seconds_total{namespace="ns1",pod="ghost",container="app",id="/kubepods/besteffort/podGHOSTUID/GHOSTCID",image="ghostimg:2",name="ghost-app"} 1
container_cpu_usage_seconds_total{id="/kubepods"} 100
container_cpu_usage_seconds_total{id="/"} 200
# TYPE container_network_receive_bytes_total counter
container_network_receive_bytes_total{namespace="ns1",pod="pod1",id="/kubepods/burstable/podUID1",interface="eth0"} 5000
# TYPE machine_cpu_cores gauge
machine_cpu_cores 8
`)

func newKubeletScraper(t *testing.T, url string, meta MetaSource, exp MetricExporter, disableRollups bool) *Scraper {
	t.Helper()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("tok123\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{}, Exporter: exp, StartTime: time.Now(),
		Kubelet: KubeletConfig{
			Endpoint:       url,
			Cadvisor:       true,
			NodeMetrics:    true,
			DisableRollups: disableRollups,
			TokenFile:      tokenFile,
			Meta:           meta,
		},
	})
}

func attrStr(res pcommon.Resource, key string) string {
	if v, ok := res.Attributes().Get(key); ok {
		return v.Str()
	}
	return ""
}

// resourcesByIdentity keys each resource by pod uid + container id/name.
func resourcesByIdentity(md pmetric.Metrics) map[string]pmetric.ResourceMetrics {
	out := map[string]pmetric.ResourceMetrics{}
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		res := rms.At(i).Resource()
		key := attrStr(res, "k8s.pod.uid") + "/" + attrStr(res, "container.id") + "/" + attrStr(res, "k8s.container.name")
		out[key] = rms.At(i)
	}
	return out
}

func metricNames(rm pmetric.ResourceMetrics) []string {
	var names []string
	ms := rm.ScopeMetrics().At(0).Metrics()
	for i := 0; i < ms.Len(); i++ {
		names = append(names, ms.At(i).Name())
	}
	return names
}

func TestScrapeCadvisor(t *testing.T) {
	var gotAuth, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		_, _ = w.Write([]byte(cadvisorBody))
	}))
	defer srv.Close()

	meta := &fakeMetaSource{}
	exp := &captureExporter{}
	s := newKubeletScraper(t, srv.URL, meta, exp, false)
	if _, err := s.scrapeCadvisor(context.Background()); err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer tok123" || gotPath != "/metrics/cadvisor" {
		t.Fatalf("auth=%q path=%q", gotAuth, gotPath)
	}

	byID := resourcesByIdentity(exp.batches[0])
	if len(byID) != 4 {
		t.Fatalf("got %d resources: %v", len(byID), byID)
	}

	// Container-level: resolved via the cgroup container ID — exact
	// incarnation, full metadata.
	rm, ok := byID[uid1+"/"+appCID+"/app"]
	if !ok {
		t.Fatalf("missing container-level resource; have %v", keys(byID))
	}
	res := rm.Resource()
	if attrStr(res, "k8s.deployment.name") != "dep1" || attrStr(res, "k8s.node.name") != "node1" {
		t.Fatalf("container resource attrs = %v", res.Attributes().AsRaw())
	}
	// id/image/name duplicate the resolved resource identity — elided from
	// pod/container-row data points (cmb-alloy parity).
	cpuDP := rm.ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0)
	for _, k := range []string{"id", "image", "name"} {
		if _, ok := cpuDP.Attributes().Get(k); ok {
			t.Fatalf("redundant %q label on container data point: %v", k, cpuDP.Attributes().AsRaw())
		}
	}

	// Pod-level: the sandbox ("POD") row and the network series (no
	// container label, pod cgroup id) share one pod resource — the pause
	// container ID must not leak into it.
	rm, ok = byID[uid1+"//"]
	if !ok {
		t.Fatalf("missing pod-level resource; have %v", keys(byID))
	}
	res = rm.Resource()
	if attrStr(res, "k8s.pod.name") != "pod1" || attrStr(res, "k8s.deployment.name") != "dep1" {
		t.Fatalf("pod resource attrs = %v", res.Attributes().AsRaw())
	}
	names := metricNames(rm)
	if len(names) != 2 || names[0] != "container_cpu_usage_seconds_total" || names[1] != "container_network_receive_bytes_total" {
		t.Fatalf("pod-level metrics = %v", names)
	}
	// Network point keeps its non-identity labels.
	netDP := rm.ScopeMetrics().At(0).Metrics().At(1).Sum().DataPoints().At(0)
	if v, _ := netDP.Attributes().Get("interface"); v.Str() != "eth0" {
		t.Fatalf("network dp attrs = %v", netDP.Attributes().AsRaw())
	}

	// Unknown pod/container: identity from labels and cgroup path; the image
	// label (elided from the data points) becomes the resource's image.
	rm, ok = byID[ghostUID+"/"+ghostCID+"/app"]
	if !ok {
		t.Fatalf("missing ghost resource; have %v", keys(byID))
	}
	if attrStr(rm.Resource(), "k8s.pod.name") != "ghost" ||
		attrStr(rm.Resource(), "container.image.name") != "ghostimg:2" {
		t.Fatalf("ghost resource attrs = %v", rm.Resource().Attributes().AsRaw())
	}

	// Node-level: hierarchy rollups and machine_* under the node resource.
	rm, ok = byID["//"]
	if !ok {
		t.Fatalf("missing node-level resource; have %v", keys(byID))
	}
	names = metricNames(rm)
	if len(names) != 2 { // container_cpu (rollups) + machine_cpu_cores
		t.Fatalf("node-level metrics = %v", names)
	}
	// Rollup rows share the node resource; there the cgroup path in "id" is
	// the only distinguisher and must STAY on the data points.
	rollupDPs := rm.ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints()
	ids := map[string]bool{}
	for i := 0; i < rollupDPs.Len(); i++ {
		if v, ok := rollupDPs.At(i).Attributes().Get("id"); ok {
			ids[v.Str()] = true
		}
	}
	if !ids["/kubepods"] || !ids["/"] {
		t.Fatalf("rollup data points must keep their id label, got %v", ids)
	}

	// Second scrape: metadata comes from the cache.
	pc, cc := meta.podCalls.Load(), meta.containerCalls.Load()
	if _, err := s.scrapeCadvisor(context.Background()); err != nil {
		t.Fatal(err)
	}
	if meta.podCalls.Load() != pc || meta.containerCalls.Load() != cc {
		t.Fatal("metadata lookups not cached across scrapes")
	}
}

func TestScrapeCadvisorRollupsDisabled(t *testing.T) {
	srv := serveBody(t, cadvisorBody)

	exp := &captureExporter{}
	s := newKubeletScraper(t, srv.URL, &fakeMetaSource{}, exp, true)
	if _, err := s.scrapeCadvisor(context.Background()); err != nil {
		t.Fatal(err)
	}

	byID := resourcesByIdentity(exp.batches[0])
	// Container-level series and genuinely pod-scoped families (network)
	// survive; the /kubepods and / hierarchy rows AND the pod-level rows of
	// container-scoped families (the sandbox cpu row) are gone.
	if _, ok := byID[uid1+"/"+appCID+"/app"]; !ok {
		t.Fatalf("container-level series must survive the rollup filter: %v", keys(byID))
	}
	rm, ok := byID[uid1+"//"]
	if !ok {
		t.Fatalf("pod-scoped network series must survive the rollup filter: %v", keys(byID))
	}
	if names := metricNames(rm); len(names) != 1 || names[0] != "container_network_receive_bytes_total" {
		t.Fatalf("pod-level metrics with rollups disabled = %v (container-scoped rollups must be dropped)", names)
	}
	rm, ok = byID["//"]
	if !ok {
		t.Fatal("machine_* must survive the rollup filter")
	}
	if names := metricNames(rm); len(names) != 1 || names[0] != "machine_cpu_cores" {
		t.Fatalf("node-level metrics with rollups disabled = %v", names)
	}
}

func keys[V any](m map[string]V) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestScrapeNodeMetrics(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _ = w.Write([]byte("# TYPE kubelet_running_pods gauge\nkubelet_running_pods 7\n"))
	}))
	defer srv.Close()

	exp := &captureExporter{}
	s := newKubeletScraper(t, srv.URL, &fakeMetaSource{}, exp, false)
	if _, err := s.scrapeNodeMetrics(context.Background()); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/metrics" {
		t.Fatalf("path = %q", gotPath)
	}
	rm := exp.batches[0].ResourceMetrics().At(0)
	res := rm.Resource()
	if attrStr(res, "k8s.node.name") != "node1" || attrStr(res, "service.name") != "kubelet" {
		t.Fatalf("node resource attrs = %v", res.Attributes().AsRaw())
	}
	m := rm.ScopeMetrics().At(0).Metrics().At(0)
	if m.Name() != "kubelet_running_pods" || m.Gauge().DataPoints().At(0).DoubleValue() != 7 {
		t.Fatalf("metric = %s", m.Name())
	}
}

func TestScrapeCadvisorAuthError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer srv.Close()

	exp := &captureExporter{}
	s := newKubeletScraper(t, srv.URL, &fakeMetaSource{}, exp, false)
	if _, err := s.scrapeCadvisor(context.Background()); err == nil {
		t.Fatal("expected error for 403")
	}
	if len(exp.batches) != 0 {
		t.Fatalf("batches = %d", len(exp.batches))
	}
}

// Histogram and summary families route through the cadvisor batcher with the
// pod/container resource attribution and their shape intact.
func TestScrapeCadvisorHistogramSummary(t *testing.T) {
	body := strings.NewReplacer("UID1", uid1, "APPCID", appCID).Replace(`# TYPE container_lat_seconds histogram
container_lat_seconds_bucket{namespace="ns1",pod="pod1",container="app",id="/kubepods/burstable/podUID1/APPCID",le="0.1"} 2
container_lat_seconds_bucket{namespace="ns1",pod="pod1",container="app",id="/kubepods/burstable/podUID1/APPCID",le="+Inf"} 5
container_lat_seconds_sum{namespace="ns1",pod="pod1",container="app",id="/kubepods/burstable/podUID1/APPCID"} 1.5
container_lat_seconds_count{namespace="ns1",pod="pod1",container="app",id="/kubepods/burstable/podUID1/APPCID"} 5
# TYPE container_size_bytes summary
container_size_bytes{namespace="ns1",pod="pod1",container="app",id="/kubepods/burstable/podUID1/APPCID",quantile="0.9"} 42
container_size_bytes_sum{namespace="ns1",pod="pod1",container="app",id="/kubepods/burstable/podUID1/APPCID"} 100
container_size_bytes_count{namespace="ns1",pod="pod1",container="app",id="/kubepods/burstable/podUID1/APPCID"} 3
`)
	srv := serveBody(t, body)

	exp := &captureExporter{}
	s := newKubeletScraper(t, srv.URL, &fakeMetaSource{}, exp, false)
	if _, err := s.scrapeCadvisor(context.Background()); err != nil {
		t.Fatal(err)
	}

	rm, ok := resourcesByIdentity(exp.batches[0])[uid1+"/"+appCID+"/app"]
	if !ok {
		t.Fatal("container resource missing")
	}
	var hist, summ pmetric.Metric
	ms := rm.ScopeMetrics().At(0).Metrics()
	for i := 0; i < ms.Len(); i++ {
		switch ms.At(i).Name() {
		case "container_lat_seconds":
			hist = ms.At(i)
		case "container_size_bytes":
			summ = ms.At(i)
		}
	}
	if hist.Type() != pmetric.MetricTypeHistogram {
		t.Fatalf("histogram type = %v", hist.Type())
	}
	hdp := hist.Histogram().DataPoints().At(0)
	if hdp.Count() != 5 || hdp.Sum() != 1.5 || hdp.ExplicitBounds().Len() != 1 {
		t.Fatalf("histogram dp = count %d sum %v bounds %d", hdp.Count(), hdp.Sum(), hdp.ExplicitBounds().Len())
	}
	// Identity labels are elided from histogram data points too.
	if _, leaked := hdp.Attributes().Get("id"); leaked {
		t.Fatalf("id label leaked: %v", hdp.Attributes().AsRaw())
	}
	if summ.Type() != pmetric.MetricTypeSummary {
		t.Fatalf("summary type = %v", summ.Type())
	}
	sdp := summ.Summary().DataPoints().At(0)
	if sdp.Count() != 3 || sdp.Sum() != 100 || sdp.QuantileValues().Len() != 1 {
		t.Fatalf("summary dp = count %d sum %v quantiles %d", sdp.Count(), sdp.Sum(), sdp.QuantileValues().Len())
	}
}
