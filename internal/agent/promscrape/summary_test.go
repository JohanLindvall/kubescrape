package promscrape

// Tests for the kubelet /stats/summary scrape.
//
// FIXTURE PROVENANCE: testdata/summary-*.json are hand-written against
// k8s.io/kubelet/pkg/apis/stats/v1alpha1 (the Summary/NodeStats/PodStats/
// ContainerStats/VolumeStats/FsStats/RlimitStats/ProcessStats shapes), not
// captured from a live cluster — there is none in this repo's test
// environment. They carry the version-dependent shapes a mixed fleet really
// serves: 1.28 has no runtime.containerFs, 1.30 has it, 1.33 adds the
// feature-gated io/psi blocks. Every one of them also carries the cpu, memory,
// network and swap blocks this pipeline refuses to export, so the refusal is
// exercised rather than assumed.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// The scrape clock every fixture-driven test converts against: the fixtures'
// stat times sit at or just before it, as a real payload's do.
var summaryScrape = time.Date(2026, 8, 13, 10, 0, 5, 0, time.UTC)

func loadSummary(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// newSummaryScraper is newKubeletScraper with the summary scrape on and a real
// attrs.Builders, which production always has (cmd/kubescrape-agent calls
// buildAttrs unconditionally) and which is what carries the per-pipeline
// instance prefixes this pipeline's identity depends on.
func newSummaryScraper(t *testing.T, url string, meta MetaSource, exp MetricExporter) *Scraper {
	t.Helper()
	s := newKubeletScraper(t, url, meta, exp, false)
	s.cfg.Kubelet.Summary = true
	b, err := attrs.NewBuilders(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.cfg.Attrs = b
	return s
}

// convertFixture runs one fixture through the conversion and returns every
// exported batch.
func convertFixture(t *testing.T, s *Scraper, exp *captureExporter, body []byte) []pmetric.Metrics {
	t.Helper()
	if _, err := s.convertSummary(context.Background(), "https://node1:10250"+summaryPath, body, summaryScrape); err != nil {
		t.Fatalf("converting the summary: %v", err)
	}
	return exp.batches
}

// summaryPoint is one exported data point flattened for assertions.
type summaryPoint struct {
	res    map[string]any
	metric string
	unit   string
	desc   string
	attrs  map[string]any
	value  int64
	ts     time.Time
	// resIdx identifies the ResourceMetrics the point sits on, which is what
	// "these points share a resource" has to be asserted against — equal
	// attribute maps are not the same thing as one resource.
	resIdx string
}

func flattenSummary(batches []pmetric.Metrics) []summaryPoint {
	var out []summaryPoint
	for bi, md := range batches {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			rm := rms.At(i)
			res := rm.Resource().Attributes().AsRaw()
			sms := rm.ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)
					if m.Type() != pmetric.MetricTypeGauge {
						continue
					}
					dps := m.Gauge().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						out = append(out, summaryPoint{
							res:    res,
							metric: m.Name(),
							unit:   m.Unit(),
							desc:   m.Description(),
							attrs:  dp.Attributes().AsRaw(),
							value:  dp.IntValue(),
							ts:     dp.Timestamp().AsTime(),
							resIdx: fmt.Sprintf("%d/%d", bi, i),
						})
					}
				}
			}
		}
	}
	return out
}

func pointsNamed(pts []summaryPoint, name string) []summaryPoint {
	var out []summaryPoint
	for _, p := range pts {
		if p.metric == name {
			out = append(out, p)
		}
	}
	return out
}

func metricNameSet(pts []summaryPoint) []string {
	seen := map[string]bool{}
	for _, p := range pts {
		seen[p.metric] = true
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// THE test. The whole point of the feature is that a summary series JOINS the
// cadvisor series for the same container, and a join is decided entirely by the
// resource attributes: the OTLP→Prometheus translation derives `job` and
// `instance` from them and nothing else lines the two up.
//
// So: feed a cadvisor row for ns1/pod1's `app` container through the cadvisor
// batcher, feed a /stats/summary payload naming the same pod and container
// through the summary path, and require the two resources to be equal attribute
// for attribute. It is the direct sibling of
// TestFillContainerResourceMatchesTheCadvisorRow, and what it catches is
// somebody giving this pipeline an identity path of its own — which passes
// every other test in this file.
func TestSummaryContainerResourceMatchesTheCadvisorRow(t *testing.T) {
	body := loadSummary(t, "summary-1.30.json")

	cadvisorExp := &captureExporter{}
	cs := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, cadvisorExp)
	cb := newCadvisorBatcher(context.Background(), cs, summaryScrape)
	// container_fs_usage_bytes is the cadvisor row this pipeline's rootfs point
	// sits beside, and a gauge, so the two flatten the same way.
	row := fmt.Sprintf("# TYPE container_fs_usage_bytes gauge\n"+
		"container_fs_usage_bytes{namespace=\"ns1\",pod=\"pod1\",container=\"app\",id=%q,image=\"img:1\"} 24576\n",
		"/kubepods/burstable/pod"+uid1+"/"+appCID)
	if _, err := cs.parseAndExport(context.Background(), strings.NewReader(row), false, false, cb, pipelineCadvisor, "u"); err != nil {
		t.Fatal(err)
	}
	cadvisorRes := flattenSummary(cadvisorExp.batches)
	if len(cadvisorRes) != 1 {
		t.Fatalf("the cadvisor fixture produced %d points, want 1", len(cadvisorRes))
	}

	exp := &captureExporter{}
	s := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, exp)
	pts := flattenSummary(convertFixture(t, s, exp, body))

	var got map[string]any
	for _, p := range pts {
		if p.metric == metContainerEphemeralUsage.name && p.res["k8s.container.name"] == "app" && p.res["k8s.namespace.name"] == "ns1" {
			got = p.res
			break
		}
	}
	if got == nil {
		t.Fatalf("no container ephemeral-storage point for ns1/pod1/app; got %v", metricNameSet(pts))
	}
	if want := cadvisorRes[0].res; !reflect.DeepEqual(want, got) {
		t.Fatalf("the summary container resource differs from the cadvisor row's, so the series will not join:\ncadvisor: %v\nsummary:  %v", want, got)
	}
	// And it is not vacuously equal: the attributes that decide the join have to
	// be in there. container.id in particular is the one the summary payload does
	// NOT carry — it comes from the pod document, through the shared identity
	// path, which is the whole mechanism under test.
	for _, k := range []string{"k8s.namespace.name", "k8s.pod.name", "k8s.container.name", "container.id", "service.name", "service.instance.id"} {
		if _, ok := got[k]; !ok {
			t.Errorf("%s is missing from the summary container resource: %v", k, got)
		}
	}
	if got["service.instance.id"] != "cadvisor-"+appCID {
		t.Errorf("service.instance.id = %v, want the cadvisor-prefixed container id: the pod/container resources are the CADVISOR pipeline's, deliberately", got["service.instance.id"])
	}
}

// A batcher keyed on the pod rather than on the container passes every other
// test here and fails this one: two containers of one pod, each with its own
// numbers, must land on two resources carrying their own k8s.container.name.
func TestSummaryContainerStatsDoNotCrossContainers(t *testing.T) {
	body := []byte(`{"pods":[{"podRef":{"name":"pod1","namespace":"ns1","uid":"` + uid1 + `"},
      "containers":[
        {"name":"app","rootfs":{"usedBytes":100,"inodesUsed":1},"logs":{"usedBytes":10,"inodesUsed":2}},
        {"name":"sidecar","rootfs":{"usedBytes":200,"inodesUsed":3},"logs":{"usedBytes":20,"inodesUsed":4}}
      ]}]}`)
	exp := &captureExporter{}
	s := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, exp)
	pts := flattenSummary(convertFixture(t, s, exp, body))

	usage := pointsNamed(pts, metContainerEphemeralUsage.name)
	got := map[string]int64{}
	resources := map[string]string{}
	for _, p := range usage {
		name, _ := p.res["k8s.container.name"].(string)
		if name == "" {
			t.Fatalf("a container ephemeral-storage point landed on a resource with no k8s.container.name: %v", p.res)
		}
		got[name+"/"+fmt.Sprint(p.attrs["fs.type"])] = p.value
		resources[name] = p.resIdx
	}
	want := map[string]int64{"app/rootfs": 100, "app/logs": 10, "sidecar/rootfs": 200, "sidecar/logs": 20}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("per-container values crossed containers:\nwant %v\ngot  %v", want, got)
	}
	if len(resources) != 2 || resources["app"] == resources["sidecar"] {
		t.Fatalf("the two containers share a resource (%v), so one container's numbers are attributed to the other", resources)
	}
}

// Pod-level statistics belong to the POD: one resource, no container name on
// it, and exactly one point however many containers the pod has.
func TestSummaryPodStatsLandOnThePodResource(t *testing.T) {
	exp := &captureExporter{}
	s := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, exp)
	pts := flattenSummary(convertFixture(t, s, exp, loadSummary(t, "summary-1.30.json")))

	got := pointsNamed(pts, metPodFilesystemUsage.name)
	byPod := map[string]int{}
	for _, p := range got {
		if _, ok := p.res["k8s.container.name"]; ok {
			t.Errorf("a pod-level point landed on a CONTAINER resource: %v", p.res)
		}
		byPod[fmt.Sprint(p.res["k8s.namespace.name"], "/", p.res["k8s.pod.name"])]++
	}
	for pod, n := range byPod {
		if n != 1 {
			t.Errorf("%s has %d pod ephemeral-storage points, want exactly 1 (one per pod, not one per container)", pod, n)
		}
	}
	if byPod["ns1/pod1"] != 1 {
		t.Fatalf("no pod ephemeral-storage point for ns1/pod1: %v", byPod)
	}
	for _, p := range got {
		if p.res["k8s.namespace.name"] == "ns1" && p.res["k8s.pod.name"] == "pod1" && p.value != 38912 {
			t.Errorf("ns1/pod1 ephemeral-storage usage = %d, want 38912", p.value)
		}
	}
}

// Volume statistics ride the POD's resource — the same ResourceMetrics its
// ephemeral-storage points sit on, not a sibling — with the volume named on the
// data point.
//
// The resource IDENTITY is compared, not the attribute maps: two resources with
// equal attributes are exactly the failure this arrangement exists to prevent
// (attrs.Identity has no volume component, so per-volume resources would share
// one job and instance and flap target_info), so an assertion that could not
// tell them apart would pass on the broken shape.
func TestSummaryVolumeStatsRideThePodResource(t *testing.T) {
	exp := &captureExporter{}
	s := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, exp)
	pts := flattenSummary(convertFixture(t, s, exp, loadSummary(t, "summary-1.30.json")))

	var podRes string
	for _, p := range pointsNamed(pts, metPodFilesystemUsage.name) {
		if p.res["k8s.pod.name"] == "pod1" {
			podRes = p.resIdx
		}
	}
	if podRes == "" {
		t.Fatal("no pod-level point for ns1/pod1 to compare the volumes against")
	}

	usage := pointsNamed(pts, volumeMetrics.usage.name)
	byName := map[string]summaryPoint{}
	for _, p := range usage {
		if p.res["k8s.pod.name"] != "pod1" {
			continue
		}
		name, _ := p.attrs[attrVolumeName].(string)
		if name == "" {
			t.Fatalf("a volume point carries no %s on the data point: %v", attrVolumeName, p.attrs)
		}
		if _, ok := p.res[attrVolumeName]; ok {
			t.Errorf("%s is on the RESOURCE, so every volume of a pod mints its own resource: %v", attrVolumeName, p.res)
		}
		if p.resIdx != podRes {
			t.Errorf("volume %q sits on resource %s, not on the pod's %s", name, p.resIdx, podRes)
		}
		byName[name] = p
	}
	if len(byName) != 2 {
		t.Fatalf("got %d volumes for ns1/pod1, want 2 points on one resource", len(byName))
	}
	if got := byName["data"]; got.attrs[attrPVCName] != "data-pod1" || got.attrs[attrPVCNamespace] != "ns1" {
		t.Errorf("the PVC-backed volume lost its claim reference: %v", got.attrs)
	}
	if got := byName["kube-api-access-abcde"]; got.attrs[attrPVCName] != nil {
		t.Errorf("a projected volume with no pvcRef gained one: %v", got.attrs)
	}
	if byName["data"].value != 1000000000 || byName["kube-api-access-abcde"].value != 6144 {
		t.Errorf("volume usage values crossed volumes: %v", byName)
	}
}

// The negative of the test above, stated as the failure it prevents and run
// over the whole payload: no two resources in one export may share
// (service.name, service.namespace, service.instance.id). That triple is the
// Prometheus (job, instance), and two resources sharing it with different
// attribute sets is a target_info that flaps between them every scrape.
//
// It catches the per-volume-resource design, a lost instance prefix, and any
// future change to how attrs.Identity derives an instance.
func TestSummaryResourcesDoNotShareAJobAndInstance(t *testing.T) {
	for _, fixture := range []string{"summary-1.28.json", "summary-1.30.json", "summary-1.33.json"} {
		t.Run(fixture, func(t *testing.T) {
			exp := &captureExporter{}
			s := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, exp)
			pts := flattenSummary(convertFixture(t, s, exp, loadSummary(t, fixture)))

			seen := map[string]string{}
			for _, p := range pts {
				id := fmt.Sprint(p.res["service.name"], "\x00", p.res["service.namespace"], "\x00", p.res["service.instance.id"])
				if prev, ok := seen[id]; ok && prev != p.resIdx {
					t.Fatalf("two resources share (job, instance) %q: %s and %s", id, prev, p.resIdx)
				}
				seen[id] = p.resIdx
			}
			if len(seen) < 2 {
				t.Fatalf("only %d distinct identities in the payload; the assertion is vacuous", len(seen))
			}
		})
	}
}

// A pod the metadata service cannot place is still EXPORTED, with the identity
// the payload itself carries — cadvisor's policy, deliberately not
// cgroupstats'. The summary names the namespace, pod, uid and container, i.e.
// more than a cadvisor row, so the label fallback yields a resource with a
// service.name and therefore a Prometheus job; a cgroup path yields two ids and
// nothing to fall back to, which is why the sampler drops instead.
//
// What is lost meanwhile is the JOIN, and the test says so: the unresolved
// container resource has no container.id, so its instance differs from the one
// the cadvisor row for the same container would carry.
func TestSummaryUnresolvedPodFallsBackLikeCadvisor(t *testing.T) {
	exp := &captureExporter{}
	s := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, exp)
	pts := flattenSummary(convertFixture(t, s, exp, loadSummary(t, "summary-1.30.json")))

	var podRes, ctrRes map[string]any
	for _, p := range pts {
		if p.res["k8s.pod.name"] != "ghost" {
			continue
		}
		if p.res["k8s.container.name"] == "worker" {
			ctrRes = p.res
		} else if _, ok := p.res["k8s.container.name"]; !ok {
			podRes = p.res
		}
	}
	if podRes == nil || ctrRes == nil {
		t.Fatalf("the unplaceable pod was not exported at all (pod=%v container=%v)", podRes, ctrRes)
	}
	for k, want := range map[string]any{
		"k8s.namespace.name": "ns2",
		"k8s.pod.name":       "ghost",
		"k8s.pod.uid":        ghostUID,
		"service.name":       "ghost", // without it the series has no Prometheus job at all
	} {
		if podRes[k] != want {
			t.Errorf("unresolved pod resource %s = %v, want %v", k, podRes[k], want)
		}
	}
	if ctrRes["k8s.container.name"] != "worker" || ctrRes["service.name"] != "ghost" {
		t.Errorf("unresolved container resource lost its identity: %v", ctrRes)
	}
	// The bounded, self-healing cost: no container.id to key the instance on.
	if _, ok := ctrRes["container.id"]; ok {
		t.Errorf("an unresolved container gained a container.id from nowhere: %v", ctrRes)
	}
	if want := "cadvisor-" + ghostUID + "/worker"; ctrRes["service.instance.id"] != want {
		t.Errorf("unresolved container instance = %v, want %v", ctrRes["service.instance.id"], want)
	}

	// And it is built by the SAME function an unresolved cadvisor row goes
	// through, so the two agree in the unresolved state as well as the resolved
	// one. The cadvisor row additionally carries a container id and image from
	// its cgroup path and labels, which the summary has no source for.
	cadvisor := pcommon.NewResource()
	s.fillIdentityResource(context.Background(), cadvisor,
		cadvisorIdentity{namespace: "ns2", pod: "ghost", podUID: ghostUID, container: "worker"})
	if want, got := cadvisor.Attributes().AsRaw(), ctrRes; !reflect.DeepEqual(want, got) {
		t.Errorf("the unresolved summary resource diverges from the unresolved cadvisor resource for the same object:\ncadvisor: %v\nsummary:  %v", want, got)
	}
}

// The node-level resource must not collide with the /metrics scrape's, which
// carries the same service.name and the same node. Only the summary pipeline's
// own instance prefix keeps them apart — without it the two differ on url.full
// alone, which the OTLP→Prometheus translation makes no label of, so one series
// would be described by two attribute sets flapping every cycle.
func TestSummaryNodeInstanceDoesNotCollideWithNodeMetrics(t *testing.T) {
	exp := &captureExporter{}
	s := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, exp)

	summaryRes := pcommon.NewResource()
	newSummaryBatcher(context.Background(), s, "https://node1:10250"+summaryPath, summaryScrape).fillNodeResource(summaryRes)

	nodeRes := pcommon.NewResource()
	nodeRes.Attributes().PutStr("service.name", "kubelet")
	nodeRes.Attributes().PutStr("url.full", "https://node1:10250/metrics")
	s.attrsFor(pipelineNode).Build(nodeRes, attrs.Context{Node: s.nodeInfo()})

	cadvisorRes := pcommon.NewResource()
	s.attrsFor(pipelineCadvisor).Build(cadvisorRes, attrs.Context{Node: s.nodeInfo()})

	sum := summaryRes.Attributes().AsRaw()
	for name, other := range map[string]pcommon.Resource{"node": nodeRes, "cadvisor": cadvisorRes} {
		o := other.Attributes().AsRaw()
		if sum["service.instance.id"] == o["service.instance.id"] && sum["service.name"] == o["service.name"] {
			t.Errorf("the summary node resource shares (job, instance) with the %s pipeline's: %v vs %v", name, sum, o)
		}
	}
	if sum["service.instance.id"] != "summary-node1" {
		t.Errorf("summary node instance = %v, want summary-node1", sum["service.instance.id"])
	}
}

// exportHealth's kubelet branch resolves its builder through attrsFor, whose
// default arm returns the TARGETS builder. Without a summary case the summary
// scrape's up / scrape_duration_seconds / scrape_samples_scraped would be
// emitted on (job=kubelet, instance=<node>) — the same identity the /metrics
// scrape's health uses, with a different value, in one payload.
func TestSummaryHealthMetricsHaveTheirOwnInstance(t *testing.T) {
	exp := &captureExporter{}
	s := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, exp)
	s.cfg.HealthMetrics = true

	s.exportHealth(context.Background(), []scrapeOutcome{
		{pipeline: pipelineCadvisor, url: "https://node1:10250/metrics/cadvisor", ok: true},
		{pipeline: pipelineNode, url: "https://node1:10250/metrics", ok: true},
		{pipeline: pipelineSummary, url: "https://node1:10250" + summaryPath, ok: false},
	})

	seen := map[string]string{}
	for _, p := range flattenSummary(exp.batches) {
		if p.metric != "up" {
			continue
		}
		id := fmt.Sprint(p.res["service.name"], "\x00", p.res["service.instance.id"])
		if prev, ok := seen[id]; ok {
			t.Fatalf("two kubelet pipelines emit up{} under one identity %q (%s and %s): one series, two conflicting points, one payload", id, prev, p.resIdx)
		}
		seen[id] = p.resIdx
	}
	if len(seen) != 3 {
		t.Fatalf("got %d distinct up{} identities, want one per kubelet pipeline: %v", len(seen), seen)
	}
}

// An absent field is NOT a zero. The kubelet's schema is pointers throughout
// and a nil means "not reported"; a container exported as using 0 bytes of
// ephemeral storage looks healthy and is unmeasured, which is the worst thing
// this pipeline can do.
//
// The expected names are enumerated exactly — a len(metrics) > 0 assertion
// passes on every wrong answer here.
func TestSummaryNilFieldsAreNotZero(t *testing.T) {
	exp := &captureExporter{}
	s := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, exp)
	pts := flattenSummary(convertFixture(t, s, exp, loadSummary(t, "summary-sparse.json")))

	// The fixture: no ephemeral-storage on the pod, no logs on the container, no
	// inodesUsed on its rootfs, no runtime and no rlimit on the node, and only
	// three of the node filesystem's six leaves.
	want := []string{
		metContainerEphemeralUsage.name,
		metPodProcessCount.name,
		nodeFsMetrics.available.name,
		nodeFsMetrics.capacity.name,
		nodeFsMetrics.usage.name,
	}
	sort.Strings(want)
	if got := metricNameSet(pts); !reflect.DeepEqual(want, got) {
		t.Fatalf("a sparse payload exported the wrong metric set:\nwant %v\ngot  %v", want, got)
	}
	// The rootfs usage that IS present, and only for rootfs.
	usage := pointsNamed(pts, metContainerEphemeralUsage.name)
	if len(usage) != 1 || usage[0].attrs[attrFsType] != "rootfs" {
		t.Fatalf("container ephemeral-storage points = %v, want exactly one rootfs point (an absent logs block is not a zero)", usage)
	}
}

// The complement, and it matters as much: a field PRESENT and zero is a fact. A
// container that genuinely wrote nothing to its rootfs, and a pod running no
// processes, must both be exported as 0 — absent and zero have to be
// distinguishable in BOTH directions.
func TestSummaryZeroIsExported(t *testing.T) {
	exp := &captureExporter{}
	s := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, exp)
	pts := flattenSummary(convertFixture(t, s, exp, loadSummary(t, "summary-sparse.json")))

	usage := pointsNamed(pts, metContainerEphemeralUsage.name)
	if len(usage) != 1 || usage[0].value != 0 {
		t.Fatalf("rootfs usedBytes:0 exported as %v, want one point valued 0", usage)
	}
	procs := pointsNamed(pts, metPodProcessCount.name)
	if len(procs) != 1 || procs[0].value != 0 {
		t.Fatalf("process_count:0 exported as %v, want one point valued 0", procs)
	}
}

// Every point takes the STAT's own time, not the scrape's. The kubelet's
// volume-stats calculator refreshes on its own ~1 minute period and the summary
// serves that cache, so most scrapes carry a volume reading older than the
// scrape; stamping scrape time would fabricate freshness.
func TestSummaryTimestampIsTheStatsOwn(t *testing.T) {
	stat := summaryScrape.Add(-2 * time.Minute).UTC()
	body := []byte(`{"pods":[{"podRef":{"name":"pod1","namespace":"ns1","uid":"` + uid1 + `"},
      "volume":[{"time":"` + stat.Format(time.RFC3339) + `","usedBytes":5,"name":"stale"},
                {"usedBytes":7,"name":"undated"}]}]}`)
	exp := &captureExporter{}
	s := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, exp)
	pts := flattenSummary(convertFixture(t, s, exp, body))

	byName := map[string]summaryPoint{}
	for _, p := range pointsNamed(pts, volumeMetrics.usage.name) {
		byName[fmt.Sprint(p.attrs[attrVolumeName])] = p
	}
	if got := byName["stale"].ts; !got.Equal(stat) {
		t.Errorf("a two-minute-old volume stat was stamped %v, want its own time %v", got, stat)
	}
	if got := byName["undated"].ts; !got.Equal(summaryScrape) {
		t.Errorf("a volume stat with no time was stamped %v, want the scrape time %v", got, summaryScrape)
	}
}

// Per-element degradation, pkg/promparse' rule applied to JSON: one element
// that does not decode costs its own metrics and nothing else.
//
// "Does not decode" is the boundary the array walk can express — an element
// that WALKS but is not a pod/container/volume object. A byte-level syntax
// error inside the array is a document failure (nothing can say where the next
// element starts), which is the sibling test below.
func TestSummaryMalformedElementIsSkipped(t *testing.T) {
	cases := []struct {
		name string
		body string
		want float64
	}{
		{
			name: "pod",
			body: `{"pods":[
			  {"podRef":{"name":"a","namespace":"ns1"},"ephemeral-storage":{"usedBytes":1}},
			  {"podRef":"not-an-object"},
			  {"podRef":{"name":"b","namespace":"ns1"},"ephemeral-storage":{"usedBytes":2}}]}`,
			want: 1,
		},
		{
			name: "container",
			body: `{"pods":[{"podRef":{"name":"a","namespace":"ns1"},"containers":[
			  {"name":"good","rootfs":{"usedBytes":1}},
			  {"name":["not","a","name"],"rootfs":{"usedBytes":2}},
			  {"rootfs":{"usedBytes":3}}]}]}`,
			want: 2, // a non-scalar name reads as "", as does an absent one
		},
		{
			name: "volume",
			body: `{"pods":[{"podRef":{"name":"a","namespace":"ns1"},"volume":[
			  {"name":"good","usedBytes":1},
			  {"usedBytes":2}]}]}`,
			want: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := obs.ScrapeMalformed.WithLabelValues(pipelineSummary).Value()
			exp := &captureExporter{}
			s := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, exp)
			n, err := s.convertSummary(context.Background(), "u", []byte(tc.body), summaryScrape)
			if err != nil {
				t.Fatalf("one broken element failed the whole scrape: %v", err)
			}
			if n == 0 {
				t.Fatal("nothing was exported; a broken element must cost only its own metrics")
			}
			if got := obs.ScrapeMalformed.WithLabelValues(pipelineSummary).Value() - before; got != tc.want {
				t.Errorf("malformed elements counted %v, want %v", got, tc.want)
			}
		})
	}
	// The good elements really did survive.
	exp := &captureExporter{}
	s := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, exp)
	if _, err := s.convertSummary(context.Background(), "u", []byte(cases[0].body), summaryScrape); err != nil {
		t.Fatal(err)
	}
	pts := pointsNamed(flattenSummary(exp.batches), metPodFilesystemUsage.name)
	if len(pts) != 2 {
		t.Fatalf("got %d pod points around one broken element, want the two good pods", len(pts))
	}
}

// A DOCUMENT-level failure fails the scrape and exports nothing: a body whose
// shape is not what was assumed says nothing trustworthy about the part already
// walked, unlike a streamed exposition whose prefix was parsed from complete
// lines.
func TestSummaryMalformedDocumentFailsTheScrape(t *testing.T) {
	cases := map[string]string{
		"truncated":     `{"node":{"nodeName":"node1"},"pods":[{"podRef":{"name":"a",`,
		"no pods key":   `{"node":{"nodeName":"node1","fs":{"usedBytes":1}}}`,
		"array at root": `[{"podRef":{"name":"a","namespace":"ns1"}}]`,
		"broken token":  `{"pods":[{"podRef":{"name":"a","namespace":"ns1"}} {"podRef":{"name":"b","namespace":"ns1"}}]}`,
		"not json":      "# TYPE up gauge\nup 1\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			before := obs.ScrapeMalformed.WithLabelValues(pipelineSummary).Value()
			exp := &captureExporter{}
			s := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, exp)
			if _, err := s.convertSummary(context.Background(), "u", []byte(body), summaryScrape); err == nil {
				t.Fatal("a malformed document reported success")
			}
			if len(exp.batches) != 0 {
				t.Errorf("a malformed document exported %d batches", len(exp.batches))
			}
			// The per-element counter is for elements. A document that could not be
			// read is kubescrape_scrapes_total{outcome="error"}, and counting it
			// here too would make a broken kubelet look like a partially broken one.
			if got := obs.ScrapeMalformed.WithLabelValues(pipelineSummary).Value() - before; got != 0 {
				t.Errorf("a document-level failure moved the per-element malformed counter by %v", got)
			}
		})
	}
}

// The body cap is a bound on a response this process does not control, and
// hitting it exports nothing partial: a truncated JSON document has no meaning.
func TestSummaryBodyCapIsEnforced(t *testing.T) {
	// A syntactically valid document, far over the cap.
	var sb strings.Builder
	sb.WriteString(`{"pods":[`)
	for i := 0; sb.Len() < maxSummaryBytes+4096; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"podRef":{"name":"pod-%d","namespace":"ns1","uid":"u-%d"},"ephemeral-storage":{"usedBytes":%d}}`, i, i, i)
	}
	sb.WriteString(`]}`)
	body := sb.String()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	exp := &captureExporter{}
	s := newSummaryScraper(t, srv.URL, &fakeMetaSource{}, exp)
	n, err := s.scrapeSummary(context.Background())
	if err == nil {
		t.Fatal("a body over maxSummaryBytes was accepted")
	}
	if !strings.Contains(err.Error(), "maxSummaryBytes") {
		t.Errorf("error %q does not name the cap it tripped", err)
	}
	if n != 0 || len(exp.batches) != 0 {
		t.Errorf("an over-cap body exported %d points in %d batches, want nothing partial", n, len(exp.batches))
	}
}

// The kubelet versions a mixed fleet really runs. Nothing this pipeline exports
// depends on a field newer than 1.24; what genuinely differs is
// runtime.containerFs (absent before 1.30 and whenever the runtime does not
// split it), which must be absent rather than zero — and the 1.33 io/psi blocks,
// which must not appear at all.
func TestSummaryAcrossKubeletVersions(t *testing.T) {
	// The names every version must produce, containerfs aside.
	common := []string{
		metContainerEphemeralInodesUsed.name, metContainerEphemeralUsage.name,
		metPodFilesystemInodesUsed.name, metPodFilesystemUsage.name, metPodProcessCount.name,
		volumeMetrics.available.name, volumeMetrics.capacity.name, volumeMetrics.inodes.name,
		volumeMetrics.inodesFree.name, volumeMetrics.inodesUsed.name, volumeMetrics.usage.name,
		nodeFsMetrics.available.name, nodeFsMetrics.capacity.name, nodeFsMetrics.inodes.name,
		nodeFsMetrics.inodesFree.name, nodeFsMetrics.inodesUsed.name, nodeFsMetrics.usage.name,
		nodeImageFsMetrics.available.name, nodeImageFsMetrics.capacity.name, nodeImageFsMetrics.inodes.name,
		nodeImageFsMetrics.inodesFree.name, nodeImageFsMetrics.inodesUsed.name, nodeImageFsMetrics.usage.name,
		metNodeProcessCount.name, metNodeProcessMax.name,
	}
	sort.Strings(common)

	for _, tc := range []struct {
		fixture     string
		containerFs bool
	}{
		{"summary-1.28.json", false},
		{"summary-1.30.json", true},
		{"summary-1.33.json", true},
	} {
		t.Run(tc.fixture, func(t *testing.T) {
			exp := &captureExporter{}
			s := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, exp)
			pts := flattenSummary(convertFixture(t, s, exp, loadSummary(t, tc.fixture)))
			got := metricNameSet(pts)

			for _, name := range common {
				if !slices.Contains(got, name) {
					t.Errorf("%s is missing: %v", name, got)
				}
			}
			hasContainerFs := slices.Contains(got, nodeContainerFsMetrics.usage.name)
			if hasContainerFs != tc.containerFs {
				t.Errorf("containerfs metrics present = %v, want %v (an absent runtime.containerFs must be absent, never zero)", hasContainerFs, tc.containerFs)
			}
			// The refusal, exercised against payloads that carry every refused
			// block: cpu, memory, network, swap and (1.33) psi.
			for _, name := range got {
				if !slices.ContainsFunc(summaryMetrics, func(e struct {
					m     summaryMetric
					level string
				}) bool {
					return e.m.name == name
				}) {
					t.Errorf("%s is not in the pinned metric table", name)
				}
			}
		})
	}
}

// The metric table is wire-visible and the UNIT half rewrites the NAME: the
// OTLP→Prometheus translation appends the unit, so a bare "1" on a gauge turns
// k8s.volume.inodes into k8s_volume_inodes_ratio. Both halves are pinned by
// exact match, cgroupstats' metricNames one column wider.
func TestSummaryMetricNamesAndUnits(t *testing.T) {
	want := []struct{ name, unit, level string }{
		{"k8s.container.ephemeral_storage.usage", "By", "container"},
		{"k8s.container.ephemeral_storage.inodes.used", "{inode}", "container"},

		{"k8s.pod.filesystem.usage", "By", "pod"},
		{"k8s.pod.filesystem.inodes.used", "{inode}", "pod"},
		{"k8s.pod.process.count", "{process}", "pod"},

		{"k8s.volume.usage", "By", "volume"},
		{"k8s.volume.available", "By", "volume"},
		{"k8s.volume.capacity", "By", "volume"},
		{"k8s.volume.inodes", "{inode}", "volume"},
		{"k8s.volume.inodes.free", "{inode}", "volume"},
		{"k8s.volume.inodes.used", "{inode}", "volume"},

		{"k8s.node.filesystem.usage", "By", "node"},
		{"k8s.node.filesystem.available", "By", "node"},
		{"k8s.node.filesystem.capacity", "By", "node"},
		{"k8s.node.filesystem.inodes", "{inode}", "node"},
		{"k8s.node.filesystem.inodes.free", "{inode}", "node"},
		{"k8s.node.filesystem.inodes.used", "{inode}", "node"},
		{"k8s.node.imagefs.usage", "By", "node"},
		{"k8s.node.imagefs.available", "By", "node"},
		{"k8s.node.imagefs.capacity", "By", "node"},
		{"k8s.node.imagefs.inodes", "{inode}", "node"},
		{"k8s.node.imagefs.inodes.free", "{inode}", "node"},
		{"k8s.node.imagefs.inodes.used", "{inode}", "node"},
		{"k8s.node.containerfs.usage", "By", "node"},
		{"k8s.node.containerfs.available", "By", "node"},
		{"k8s.node.containerfs.capacity", "By", "node"},
		{"k8s.node.containerfs.inodes", "{inode}", "node"},
		{"k8s.node.containerfs.inodes.free", "{inode}", "node"},
		{"k8s.node.containerfs.inodes.used", "{inode}", "node"},
		{"k8s.node.process.count", "{process}", "node"},
		{"k8s.node.process.max", "{process}", "node"},
	}
	if len(summaryMetrics) != len(want) {
		t.Fatalf("summaryMetrics has %d entries, want %d", len(summaryMetrics), len(want))
	}
	for i, w := range want {
		got := summaryMetrics[i]
		if got.m.name != w.name || got.m.unit != w.unit || got.level != w.level {
			t.Errorf("summaryMetrics[%d] = (%q, %q, %q), want (%q, %q, %q)",
				i, got.m.name, got.m.unit, got.level, w.name, w.unit, w.level)
		}
		if got.m.desc == "" {
			t.Errorf("%s has no description", got.m.name)
		}
		// Descriptions repeat per RESOURCE in a one-resource-per-object payload,
		// which is where cgroupstats measured 480-byte descriptions at 81% of the
		// batch. One sentence is the budget.
		if len(got.m.desc) > 160 {
			t.Errorf("%s has a %d-byte description; a descriptor is repeated per described object", got.m.name, len(got.m.desc))
		}
	}
	// The unit field must never rewrite a name into something else: "1" is the
	// one that does (_ratio on a gauge), and a plain word would be appended
	// verbatim.
	for _, e := range summaryMetrics {
		switch e.m.unit {
		case "By":
		default:
			if !strings.HasPrefix(e.m.unit, "{") || !strings.HasSuffix(e.m.unit, "}") {
				t.Errorf("%s has unit %q: only By and brace-annotated units survive the OTLP→Prometheus name rewrite intact", e.m.name, e.m.unit)
			}
		}
	}
}

// The refusal to export cpu/memory/network/swap is what keeps this pipeline
// from shipping, under second names and on byte-identical resources, data
// /metrics/cadvisor already carries. A comment cannot enforce that; adding one
// metric is all it would take.
func TestSummaryDoesNotDuplicateCadvisorSeries(t *testing.T) {
	banned := []string{"cpu", "memory", "network", "swap", "psi", "pressure", "rss", "page_fault", "accelerator"}
	for _, e := range summaryMetrics {
		for _, b := range banned {
			if strings.Contains(e.m.name, b) {
				t.Errorf("%s names %q: cadvisor already reports it for the same objects, on the resources this pipeline is built to match", e.m.name, b)
			}
		}
	}
}

// One test instead of five separate failures, and the checklist a ninth
// pipeline copies: "summary" has to be registered in every list that decides
// what a pipeline name means.
func TestSummaryPipelineIsRegisteredEverywhere(t *testing.T) {
	if !slices.Contains(filterPipelineNames, pipelineSummary) {
		t.Errorf("%q is not in filterPipelineNames, so metrics.pipelines.summary is rejected as a typo", pipelineSummary)
	}
	f, err := NewMetricFilters(map[string][]FilterRule{
		"summary": {{Action: "drop", Metrics: `k8s\.volume\..+`, Labels: map[string]string{attrVolumeName: "kube-api-access-.*"}}},
	})
	if err != nil {
		t.Fatalf("compiling a summary filter: %v", err)
	}
	if f.filterFor(pipelineSummary) == nil {
		t.Fatal("filterFor(summary) returned nothing, so the compiled rules would never run")
	}

	b, err := attrs.NewBuilders(&attrs.Config{Pipelines: map[string]*attrs.Config{
		"summary": {Static: map[string]string{"probe": "1"}},
	}}, nil)
	if err != nil {
		t.Fatalf("resourceAttributes.pipelines.summary was rejected: %v", err)
	}
	if b.Summary == nil {
		t.Fatal("attrs.Builders.Summary is nil, so NewBuilders' assign map is missing the pipeline")
	}
	res := pcommon.NewResource()
	res.Attributes().PutStr("k8s.node.name", "node1")
	b.Summary.Build(res, attrs.Context{})
	if v, _ := res.Attributes().Get("service.instance.id"); v.Str() != "summary-node1" {
		t.Errorf("the summary builder derived instance %q, want the summary- prefix from defaultInstancePrefix", v.Str())
	}

	s := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, &captureExporter{})
	if s.attrsFor(pipelineSummary) != s.cfg.Attrs.Summary {
		t.Error("attrsFor has no summary case, so the pipeline falls through to the targets builder")
	}
	if !slices.Contains(kubeletDueKeys, dueKeySummary) {
		t.Errorf("the summary schedule key is not in kubeletDueKeys, so its due time is discarded whenever the target fetch fails: %q", kubeletDueKeys)
	}
	if len(nodePaths) != nNodePaths || len(volumePaths) != nVolumePaths {
		t.Errorf("the decode path tables and their slot constants disagree: nodePaths=%d/%d volumePaths=%d/%d",
			len(nodePaths), nNodePaths, len(volumePaths), nVolumePaths)
	}
}

// -cadvisor-rollups=false drops pod-level rows of container-scoped cadvisor
// families. Reusing cadvisorBatcher.addNumber here would put every pod-level
// summary series through that same drop() — silently, on a non-default flag.
func TestSummaryRollupFlagDoesNotDropPodStats(t *testing.T) {
	exp := &captureExporter{}
	s := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, exp)
	s.cfg.Kubelet.DisableRollups = true
	pts := flattenSummary(convertFixture(t, s, exp, loadSummary(t, "summary-1.30.json")))

	for _, name := range []string{metPodFilesystemUsage.name, metPodFilesystemInodesUsed.name, metPodProcessCount.name, volumeMetrics.usage.name} {
		if len(pointsNamed(pts, name)) == 0 {
			t.Errorf("%s disappeared under -cadvisor-rollups=false", name)
		}
	}
}

// A bare "status 403" against a kubelet is undiagnosable, and it is the most
// likely first-contact failure of this pipeline: /stats/* authorizes against
// nodes/stats while the agent's ClusterRole has historically held only
// nodes/metrics, so the other two kubelet scrapes keep working.
func TestSummaryForbiddenNamesTheRBACRule(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer srv.Close()

	var logged strings.Builder
	exp := &captureExporter{}
	s := newSummaryScraper(t, srv.URL, &fakeMetaSource{}, exp)
	s.log = slog.New(slog.NewTextHandler(&logged, nil))

	_, err := s.scrapeSummary(context.Background())
	if err == nil {
		t.Fatal("a 403 reported success")
	}
	var se *statusError
	if !errors.As(err, &se) || se.code != http.StatusForbidden {
		t.Fatalf("the 403 did not survive as a typed status: %v", err)
	}
	if !strings.Contains(logged.String(), "nodes/stats") {
		t.Fatalf("the 403 was not explained; an operator sees only %q. log: %s", err, logged.String())
	}
}

// The byte estimate is what keeps a chunk under the collector's 4 MiB receive
// limit, and the one-resource-per-object shape is exactly where a missing
// resourceBytes/chargeDescriptor call goes unnoticed: the split batcher's
// dpaBytes bug left ~1 MB uncounted the same way.
func TestSummaryByteEstimateTracksTheEncodedSize(t *testing.T) {
	exp := &captureExporter{}
	s := newSummaryScraper(t, "http://unused", &denseMetaSource{}, exp)
	// One chunk, so the estimate and the encoding describe the same payload.
	s.cfg.BatchPoints = 1 << 30
	s.cfg.BatchBytes = 1 << 30

	sb := newSummaryBatcher(context.Background(), s, "https://node1:10250"+summaryPath, summaryScrape)
	body := denseSummary(110, 2, 2)
	if err := sb.addNode(body); err != nil {
		t.Fatal(err)
	}
	if err := sb.addPods(body); err != nil {
		t.Fatal(err)
	}
	if sb.count() < 2000 {
		t.Fatalf("the dense fixture produced only %d points; the estimate would not be exercised", sb.count())
	}
	estimated := sb.size()
	encoded := (&pmetric.ProtoMarshaler{}).MetricsSize(sb.take())

	// Slightly low is by design (the constants are approximations with the
	// BatchBytes headroom absorbing them); badly low is the failure that gets a
	// whole payload rejected.
	if estimated > encoded*115/100 || estimated < encoded*85/100 {
		t.Errorf("estimated %d bytes for a %d-byte payload (%.0f%%), want within 15%%", estimated, encoded, 100*float64(estimated)/float64(encoded))
	}
}

// denseSummary renders a node's worth of summary JSON: pods x containers, with
// volumes each, one of them PVC-backed.
func denseSummary(pods, containers, volumes int) []byte {
	var sb strings.Builder
	sb.WriteString(`{"node":{"nodeName":"node1",` +
		`"fs":{"time":"2026-08-13T10:00:00Z","availableBytes":40000000000,"capacityBytes":50000000000,"usedBytes":10000000000,"inodesFree":2900000,"inodes":3000000,"inodesUsed":100000},` +
		`"runtime":{"imageFs":{"time":"2026-08-13T10:00:00Z","availableBytes":40000000000,"capacityBytes":50000000000,"usedBytes":8000000000,"inodesFree":2950000,"inodes":3000000,"inodesUsed":50000},` +
		`"containerFs":{"time":"2026-08-13T10:00:00Z","availableBytes":40000000000,"capacityBytes":50000000000,"usedBytes":2000000000,"inodesFree":2990000,"inodes":3000000,"inodesUsed":10000}},` +
		`"rlimit":{"time":"2026-08-13T10:00:00Z","maxpid":131072,"curproc":420}},"pods":[`)
	for p := range pods {
		if p > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"podRef":{"name":"workload-%d-5f7b9c8d6-abcde","namespace":"team-%d","uid":%q},`, p, p%12, denseUID(p))
		sb.WriteString(`"ephemeral-storage":{"time":"2026-08-13T10:00:00Z","usedBytes":38912,"inodesUsed":18},"process_stats":{"process_count":6},"containers":[`)
		for c := range containers {
			if c > 0 {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, `{"name":"container-%d","rootfs":{"time":"2026-08-13T10:00:00Z","usedBytes":%d,"inodesUsed":%d},"logs":{"time":"2026-08-13T10:00:00Z","usedBytes":%d,"inodesUsed":1}}`,
				c, 24576+c, 7+c, 8192+c)
		}
		sb.WriteString(`],"volume":[`)
		for v := range volumes {
			if v > 0 {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, `{"time":"2026-08-13T09:59:12Z","availableBytes":4188160,"capacityBytes":4194304,"usedBytes":%d,"inodesFree":511990,"inodes":512000,"inodesUsed":10,"name":"volume-%d"`, 6144+v, v)
			if v == 0 {
				fmt.Fprintf(&sb, `,"pvcRef":{"name":"data-workload-%d","namespace":"team-%d"}`, p, p%12)
			}
			sb.WriteByte('}')
		}
		sb.WriteString(`]}`)
	}
	sb.WriteString(`]}`)
	return []byte(sb.String())
}

// BenchmarkScrapeSummary reports the conversion cost of a dense node. It is
// REPORTING only, with no asserted ceiling: this is a once-per-interval walk
// rather than a per-line hot path, and the repo's rule is that a claimed
// allocation budget must also be a Test — claiming one the code has not been
// shaped for would be a budget nobody maintains.
func BenchmarkScrapeSummary(b *testing.B) {
	body := denseSummary(110, 2, 2)
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{}, Exporter: &discardExporter{}, StartTime: time.Now(),
		BatchPoints: 1 << 30, BatchBytes: 1 << 30,
		Kubelet: KubeletConfig{Endpoint: "http://unused", Summary: true, Meta: &denseMetaSource{}},
	})
	builders, err := attrs.NewBuilders(nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	s.cfg.Attrs = builders
	b.ReportAllocs()
	for b.Loop() {
		if _, err := s.convertSummary(context.Background(), "u", body, summaryScrape); err != nil {
			b.Fatal(err)
		}
	}
}

type discardExporter struct{}

func (discardExporter) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }

// denseMetaSource resolves every pod denseSummary names — with the UID the
// fixture gives it, which resolveContext cross-checks — so both the benchmark
// and the byte estimate see the RESOLVED path: a resolved resource carries the
// owners and labels that make up most of its attribute bytes, and an estimate
// measured against the unresolved shape would be measuring a payload production
// never sends.
type denseMetaSource struct{}

func (denseMetaSource) PodByName(_ context.Context, namespace, name string) (*kubemeta.Pod, error) {
	var idx int
	if _, err := fmt.Sscanf(name, "workload-%d-", &idx); err != nil {
		return nil, notFound()
	}
	return &kubemeta.Pod{
		Name: name, Namespace: namespace, NodeName: "node1", UID: denseUID(idx),
		Owners: []kubemeta.Owner{{Kind: "ReplicaSet", Name: name + "-rs"}, {Kind: "Deployment", Name: "workload"}},
		Labels: map[string]string{"app.kubernetes.io/name": "workload", "team": namespace},
		Containers: []kubemeta.Container{
			// Distinct per pod: a shared container id would give every pod's
			// container resource one service.instance.id, which is the collision
			// the fixtures are here to make visible rather than to manufacture.
			{Name: "container-0", ID: fmt.Sprintf("%064x", idx*2+1), Image: "registry.example/app:1.2.3"},
			{Name: "container-1", ID: fmt.Sprintf("%064x", idx*2+2), Image: "registry.example/sidecar:4.5.6"},
		},
	}, nil
}

func (denseMetaSource) Container(context.Context, string, time.Duration) (*kubemeta.ContainerMetadata, error) {
	return nil, notFound()
}

// denseUID is the pod uid denseSummary stamps for pod index i.
func denseUID(i int) string { return fmt.Sprintf("%08x-1111-2222-3333-444455556666", i) }

// The static-pod fixture's identities. staticPodUID is what the KUBELET
// reports in podRef.uid: it mints a static pod's UID from the manifest itself
// (an md5, hence the undashed form no API-server UID ever takes) and stamps
// that same value on the mirror pod as kubernetes.io/config.hash.
// mirrorPodUID is the UID the API server assigned the mirror pod, and the only
// one the metadata service can ever serve.
const (
	staticPodUID    = "1b4f0e9851971998e732078544c96b36"
	mirrorPodUID    = "5d41402a-abc4-1a2b-9c3d-4e5f60718293"
	apiserverCID    = "b7e23ec29af22b0b4e41da31e868d57226121c84e2a4b3f9c2d1a0e5f6b78901"
	successorCID    = "0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0"
	apiserverImage  = "registry.k8s.io/kube-apiserver:v1.30.4"
	staticPodCgroup = "/kubepods/besteffort/pod" + staticPodUID + "/" + apiserverCID
)

// apiserverMirrorPod is what the metadata service serves for the fixture's
// static pod: the MIRROR object, under the API server's own UID, carrying the
// two annotations the kubelet writes to say which static pod it mirrors.
var apiserverMirrorPod = kubemeta.Pod{
	Name: "kube-apiserver-node1", Namespace: "kube-system", UID: mirrorPodUID, NodeName: "node1",
	Annotations: map[string]string{
		"kubernetes.io/config.hash":   staticPodUID,
		"kubernetes.io/config.mirror": staticPodUID,
		"kubernetes.io/config.source": "file",
		"kubernetes.io/config.seen":   "2026-08-13T09:00:11.000000000Z",
	},
	Labels:     map[string]string{"component": "kube-apiserver", "tier": "control-plane"},
	Containers: []kubemeta.Container{{Name: "kube-apiserver", ID: apiserverCID, Image: apiserverImage}},
}

// mirrorMetaSource answers for one pod by namespace/name and by container id,
// delegating everything else to fakeMetaSource. The pod is a field so the
// same fixture can be run against the mirror pod and against an impostor that
// merely shares its name.
type mirrorMetaSource struct {
	fakeMetaSource
	pod kubemeta.Pod
}

func (m *mirrorMetaSource) PodByName(ctx context.Context, namespace, name string) (*kubemeta.Pod, error) {
	if namespace == m.pod.Namespace && name == m.pod.Name {
		p := m.pod
		return &p, nil
	}
	return m.fakeMetaSource.PodByName(ctx, namespace, name)
}

func (m *mirrorMetaSource) Container(ctx context.Context, id string, wait time.Duration) (*kubemeta.ContainerMetadata, error) {
	for i := range m.pod.Containers {
		if m.pod.Containers[i].ID == id {
			p := m.pod
			return &kubemeta.ContainerMetadata{ContainerID: id, Container: p.Containers[i], Pod: p}, nil
		}
	}
	return m.fakeMetaSource.Container(ctx, id, wait)
}

// A STATIC pod — kube-apiserver, etcd, kube-scheduler, kube-controller-manager
// — is reported by the kubelet under a UID no API server ever held: the kubelet
// mints it from the manifest, while the object the API server admits is a
// MIRROR pod under a UID of its own. So the by-name lookup's UID cross-check
// refuses the answer, and the whole control plane resolves to nothing on every
// scrape, forever — the pods whose ephemeral-storage numbers are worth the
// most, on the node whose disk fills up.
//
// Their CADVISOR rows resolve fine (a container id is a container id), which
// makes this a JOIN failure rather than a missing series, so the assertion is
// the join itself: the same resource as the cadvisor row for the same
// container, attribute for attribute.
func TestSummaryStaticPodJoinsTheCadvisorRow(t *testing.T) {
	cadvisorExp := &captureExporter{}
	cs := newSummaryScraper(t, "http://unused", &mirrorMetaSource{pod: apiserverMirrorPod}, cadvisorExp)
	cb := newCadvisorBatcher(context.Background(), cs, summaryScrape)
	// A static pod's cgroup carries that same kubelet-minted UID, in the undashed
	// form cgroupid reads as a pod uid alongside the canonical one. This row
	// resolves through its container id regardless — an id is an id — which is
	// why the cadvisor half of the join has always worked while the summary's
	// did not.
	row := fmt.Sprintf("# TYPE container_fs_usage_bytes gauge\n"+
		"container_fs_usage_bytes{namespace=\"kube-system\",pod=\"kube-apiserver-node1\",container=\"kube-apiserver\",id=%q,image=%q} 229376\n",
		staticPodCgroup, apiserverImage)
	if _, err := cs.parseAndExport(context.Background(), strings.NewReader(row), false, false, cb, pipelineCadvisor, "u"); err != nil {
		t.Fatal(err)
	}
	cadvisorPts := flattenSummary(cadvisorExp.batches)
	if len(cadvisorPts) != 1 {
		t.Fatalf("the cadvisor row produced %d points, want 1", len(cadvisorPts))
	}
	if cadvisorPts[0].res["container.id"] != apiserverCID {
		t.Fatalf("the cadvisor row did not resolve either, so comparing against it proves nothing: %v", cadvisorPts[0].res)
	}

	beforePods := obs.SummaryUnresolved.WithLabelValues(levelPod).Value()
	beforeCtrs := obs.SummaryUnresolved.WithLabelValues(levelContainer).Value()

	exp := &captureExporter{}
	s := newSummaryScraper(t, "http://unused", &mirrorMetaSource{pod: apiserverMirrorPod}, exp)
	pts := flattenSummary(convertFixture(t, s, exp, loadSummary(t, "summary-static-pod.json")))

	var ctrRes, podRes map[string]any
	for _, p := range pts {
		switch {
		case p.metric == metContainerEphemeralUsage.name && p.attrs[attrFsType] == "rootfs":
			ctrRes = p.res
		case p.metric == metPodFilesystemUsage.name:
			podRes = p.res
		}
	}
	if ctrRes == nil || podRes == nil {
		t.Fatalf("the static pod exported no container/pod point at all: %v", metricNameSet(pts))
	}
	if want := cadvisorPts[0].res; !reflect.DeepEqual(want, ctrRes) {
		t.Fatalf("the static pod's summary resource differs from its cadvisor row's, so the series will not join:\ncadvisor: %v\nsummary:  %v", want, ctrRes)
	}
	// And it RESOLVED, rather than the two sides agreeing on a label fallback.
	for k, want := range map[string]any{
		"k8s.pod.uid":         mirrorPodUID, // the API server's, not the one the kubelet reported
		"container.id":        apiserverCID,
		"service.instance.id": "cadvisor-" + apiserverCID,
	} {
		if ctrRes[k] != want {
			t.Errorf("the static pod's container resource has %s = %v, want %v", k, ctrRes[k], want)
		}
	}
	// The pod-level statistics — the ephemeral-storage number this pipeline
	// exists for — carry the mirror pod's metadata too, not just the container's.
	for k, want := range map[string]any{
		"k8s.pod.uid":             mirrorPodUID,
		"k8s.pod.label.component": "kube-apiserver",
		"k8s.pod.label.tier":      "control-plane",
		"service.instance.id":     "cadvisor-" + mirrorPodUID,
	} {
		if podRes[k] != want {
			t.Errorf("the static pod's pod-level resource has %s = %v, want %v", k, podRes[k], want)
		}
	}
	if d := obs.SummaryUnresolved.WithLabelValues(levelPod).Value() - beforePods; d != 0 {
		t.Errorf("the static pod counted %v unresolved pods", d)
	}
	if d := obs.SummaryUnresolved.WithLabelValues(levelContainer).Value() - beforeCtrs; d != 0 {
		t.Errorf("the static pod counted %v unresolved containers", d)
	}
}

// The other half of the same rule, and the reason the UID cross-check is
// REDIRECTED rather than dropped: a pod recreated under a name must not lend
// its identity to statistics about its predecessor. The metadata service
// answers for this namespace/name with a pod that claims to mirror nothing, so
// the payload is describing something else — and the resource must fall back to
// the label identity, exactly as for a pod the service has never heard of.
func TestSummaryRecreatedPodDoesNotLendItsIdentity(t *testing.T) {
	impostor := apiserverMirrorPod
	impostor.UID = "77777777-8888-9999-aaaa-bbbbccccdddd"
	// An ordinary API-created pod: no config.hash, no config.mirror, so nothing
	// ties it to the static pod whose UID the kubelet reported.
	impostor.Annotations = map[string]string{"kubernetes.io/config.source": "api"}
	impostor.Labels = map[string]string{"pod-template-hash": "successor"}
	impostor.Owners = []kubemeta.Owner{{Kind: "Deployment", Name: "successor"}}
	impostor.Containers = []kubemeta.Container{{Name: "kube-apiserver", ID: successorCID, Image: apiserverImage}}

	beforePods := obs.SummaryUnresolved.WithLabelValues(levelPod).Value()
	beforeCtrs := obs.SummaryUnresolved.WithLabelValues(levelContainer).Value()

	exp := &captureExporter{}
	s := newSummaryScraper(t, "http://unused", &mirrorMetaSource{pod: impostor}, exp)
	pts := flattenSummary(convertFixture(t, s, exp, loadSummary(t, "summary-static-pod.json")))

	var ctrRes, podRes map[string]any
	for _, p := range pts {
		switch {
		case p.metric == metContainerEphemeralUsage.name && p.attrs[attrFsType] == "rootfs":
			ctrRes = p.res
		case p.metric == metPodFilesystemUsage.name:
			podRes = p.res
		}
	}
	if ctrRes == nil || podRes == nil {
		t.Fatalf("the pod was not exported at all: %v", metricNameSet(pts))
	}
	for _, res := range []map[string]any{podRes, ctrRes} {
		if res["k8s.pod.uid"] != staticPodUID {
			t.Errorf("k8s.pod.uid = %v, want the uid the payload itself carried (%s): the successor's identity must not be stamped on its predecessor's statistics", res["k8s.pod.uid"], staticPodUID)
		}
		if _, ok := res["k8s.deployment.name"]; ok {
			t.Errorf("the successor's owner chain was stamped on the predecessor's statistics: %v", res)
		}
		if _, ok := res["k8s.pod.label.pod-template-hash"]; ok {
			t.Errorf("the successor's labels were stamped on the predecessor's statistics: %v", res)
		}
	}
	if id, ok := ctrRes["container.id"]; ok {
		t.Errorf("container.id = %v: the successor's container incarnation was stamped on statistics about another one", id)
	}
	// And the condition is counted, since this is exactly the unresolved state.
	if d := obs.SummaryUnresolved.WithLabelValues(levelPod).Value() - beforePods; d != 1 {
		t.Errorf("unresolved pods counted %v, want 1", d)
	}
	if d := obs.SummaryUnresolved.WithLabelValues(levelContainer).Value() - beforeCtrs; d != 1 {
		t.Errorf("unresolved containers counted %v, want 1", d)
	}
}

// The pipeline's headline risk is that its series join nothing, and an operator
// has to be able to ALERT on it: the log line is said once per process, so
// without a counter a fleet-wide metadata outage is invisible from the
// monitoring side. Counted per OBJECT — a pod with four statistics is one
// unplaceable pod, not four — and split by level: `pod` is a pod the service
// could not place, `container` a container whose resource carries no
// container.id, which an unplaceable pod always implies (so the two do move
// together here) and a placed pod can still cause on its own — see
// TestSummaryUnresolvedCountsAContainerWhosePodResolved.
func TestSummaryUnresolvedObjectsAreCounted(t *testing.T) {
	beforePods := obs.SummaryUnresolved.WithLabelValues(levelPod).Value()
	beforeCtrs := obs.SummaryUnresolved.WithLabelValues(levelContainer).Value()

	exp := &captureExporter{}
	s := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, exp)
	pts := flattenSummary(convertFixture(t, s, exp, loadSummary(t, "summary-1.30.json")))

	// The fixture: ns1/pod1 resolves, ns2/ghost and ns1/partial do not (one
	// container each), and ns1/starting reports no statistic at all, so it mints
	// no resource and cannot be unresolved.
	if got := obs.SummaryUnresolved.WithLabelValues(levelPod).Value() - beforePods; got != 2 {
		t.Errorf("unresolved pods counted %v, want 2 (ghost and partial, once each however many points they carry)", got)
	}
	if got := obs.SummaryUnresolved.WithLabelValues(levelContainer).Value() - beforeCtrs; got != 2 {
		t.Errorf("unresolved containers counted %v, want 2 (ghost/worker and partial/sparse)", got)
	}
	// Not vacuous: the resolved pod really is in this payload, and it did not
	// count.
	var resolved bool
	for _, p := range pts {
		if p.res["k8s.pod.name"] == "pod1" && p.res["container.id"] == appCID {
			resolved = true
		}
	}
	if !resolved {
		t.Fatal("no resolved object in the payload, so the counts above could be measuring anything")
	}
}

// kubescrape_scrape_samples_total documents itself as the count "before
// filtering", and kubescrape_scrape_samples_dropped_total is the term
// subtracted from it — that arithmetic is how an operator sees a keep/drop rule
// that emptied a pipeline. Counting a summary point only once it had survived
// the filter broke it for every consumer of the metric: scraped fell with
// dropped, and the difference stayed the same number it had always been.
func TestSummarySampleCountIsBeforeFiltering(t *testing.T) {
	body := loadSummary(t, "summary-1.30.json")

	exp := &captureExporter{}
	s := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, exp)
	unfiltered, err := s.convertSummary(context.Background(), "u", body, summaryScrape)
	if err != nil {
		t.Fatal(err)
	}
	if exported := len(flattenSummary(exp.batches)); unfiltered != exported {
		t.Fatalf("with no filter the scrape reported %d samples for %d exported points", unfiltered, exported)
	}

	filters, err := NewMetricFilters(map[string][]FilterRule{
		"summary": {{Action: "drop", Metrics: `k8s\.volume\..+`}},
	})
	if err != nil {
		t.Fatal(err)
	}
	fexp := &captureExporter{}
	fs := newSummaryScraper(t, "http://unused", &fakeMetaSource{}, fexp)
	fs.cfg.Filters = filters

	before := obs.ScrapeSamplesDropped.WithLabelValues(pipelineSummary, "filter").Value()
	got, err := fs.convertSummary(context.Background(), "u", body, summaryScrape)
	if err != nil {
		t.Fatal(err)
	}
	kept := len(flattenSummary(fexp.batches))
	dropped := obs.ScrapeSamplesDropped.WithLabelValues(pipelineSummary, "filter").Value() - before

	if kept >= unfiltered || dropped == 0 {
		t.Fatalf("the drop rule removed nothing (%d points kept of %d, %v dropped); the assertion below would be vacuous", kept, unfiltered, dropped)
	}
	if got != unfiltered {
		t.Errorf("the filtered scrape reported %d samples and the unfiltered one %d: kubescrape_scrape_samples_total{pipeline=%q} is the count BEFORE filtering, as every other pipeline counts it", got, unfiltered, pipelineSummary)
	}
	if want := float64(got) - dropped; want != float64(kept) {
		t.Errorf("scraped(%d) - dropped(%v) = %v, but %d points reached the collector", got, dropped, want, kept)
	}
}

// startingPodMetaSource answers for ns1/pod1 with a document that LISTS the
// container the summary reports and has no ID for it. That is not a contrived
// shape: kubemeta builds Pod.Containers from the pod SPEC with the status
// folded in, so between a container starting and its status reaching the API
// server (widened to podMetaCacheTTL by this scraper's own cache) the document
// names it with an empty ID — the common way a summary container fails to
// carry the container.id its join is made of.
type startingPodMetaSource struct{}

func (startingPodMetaSource) PodByName(_ context.Context, namespace, name string) (*kubemeta.Pod, error) {
	if namespace != "ns1" || name != "pod1" {
		return nil, notFound()
	}
	p := pod1Meta
	p.Containers = []kubemeta.Container{
		{Name: "app", ID: appCID, Image: "img:1"},
		{Name: "starting", Image: "img:2"}, // declared, not yet running: no ID
	}
	return &p, nil
}

func (startingPodMetaSource) Container(context.Context, string, time.Duration) (*kubemeta.ContainerMetadata, error) {
	return nil, notFound()
}

// TestSummaryUnresolvedCountsAContainerWhosePodResolved is the pin for what
// level="container" means.
//
// The counter used to be driven by fillIdentityResource's `resolved` alone,
// which answers about the POD: it is true whenever the pod was placed, however
// little of the container was. So the two labels always moved together and the
// container half could not report the case it exists for — a container of a pod
// the service HAS, which the service could not place. The join really is lost
// there (no container.id, so service.instance.id is cadvisor-<uid>/<name> while
// the cadvisor row for the same container carries cadvisor-<id>), nothing else
// reports it, and an operator alerting on the counter saw a flat zero.
//
// Both ways of getting there are pinned, because they are not the same input:
// a name the document does not carry at all, and a name it carries with no ID.
func TestSummaryUnresolvedCountsAContainerWhosePodResolved(t *testing.T) {
	body := func(container string) []byte {
		return []byte(`{"pods":[{"podRef":{"name":"pod1","namespace":"ns1","uid":"` + uid1 + `"},
		  "containers":[
		    {"name":"app","rootfs":{"usedBytes":100,"inodesUsed":1}},
		    {"name":"` + container + `","rootfs":{"usedBytes":200,"inodesUsed":3}}
		  ]}]}`)
	}
	cases := []struct {
		name      string
		meta      MetaSource
		container string
	}{
		{"name-the-pod-document-does-not-list", &fakeMetaSource{}, "sidecar"},
		{"listed-from-the-spec-with-no-status-yet", startingPodMetaSource{}, "starting"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			beforePods := obs.SummaryUnresolved.WithLabelValues(levelPod).Value()
			beforeCtrs := obs.SummaryUnresolved.WithLabelValues(levelContainer).Value()

			exp := &captureExporter{}
			s := newSummaryScraper(t, "http://unused", c.meta, exp)
			pts := flattenSummary(convertFixture(t, s, exp, body(c.container)))

			var missed, placed map[string]any
			for _, p := range pts {
				switch p.res["k8s.container.name"] {
				case c.container:
					missed = p.res
				case "app":
					placed = p.res
				}
			}
			if missed == nil || placed == nil {
				t.Fatalf("both containers must still be EXPORTED (placed=%v missed=%v)", placed, missed)
			}
			// Not vacuous: the pod resolved, and its resolvable container joins.
			if placed["container.id"] != appCID || placed["service.name"] != "dep1" {
				t.Fatalf("the resolvable container did not resolve, so the counts below measure nothing: %v", placed)
			}
			if d := obs.SummaryUnresolved.WithLabelValues(levelPod).Value() - beforePods; d != 0 {
				t.Errorf("unresolved pods counted %v, want 0: the pod itself resolved", d)
			}
			if d := obs.SummaryUnresolved.WithLabelValues(levelContainer).Value() - beforeCtrs; d != 1 {
				t.Errorf("unresolved containers counted %v, want 1 — the container the service could not place is the case this label documents", d)
			}
			// And the loss it stands for: no container.id, so the instance is not
			// the one the cadvisor row for this container carries.
			if id, ok := missed["container.id"]; ok {
				t.Fatalf("the unplaceable container gained a container.id (%v), so there would be nothing to count", id)
			}
			if want := "cadvisor-" + uid1 + "/" + c.container; missed["service.instance.id"] != want {
				t.Errorf("instance = %v, want %v (the shape that does not join the cadvisor row's cadvisor-<id>)",
					missed["service.instance.id"], want)
			}
		})
	}
}
