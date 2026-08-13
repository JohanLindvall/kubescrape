package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/promscrape"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// The /stats/summary cost figure is a MEASUREMENT quoted in three places an
// operator reads before turning the scrape on — the -kubelet-summary flag help,
// charts/kubescrape/values.yaml and deploy/agent.yaml — and three copies of a
// number drift. The first version of them said "two thirds of them volumes"
// where volumes are just over half, which is the sort of claim nothing but the
// code can settle.
//
// So the code settles it: this scrapes a synthetic node of the shape the three
// copies NAME, counts what comes out, and requires each of them to quote that
// count. The shape is part of the claim — a bare "2550 data points" means
// nothing without the pods, containers and volumes it assumes — so the pod
// count is checked in the text too.
const (
	costPods       = 110 // a node at the default max-pods
	costContainers = 2   // an app and a sidecar
	costVolumes    = 2   // the projected token every pod mounts, plus one PVC
)

// costCopies are the three places the figure is written down. Paths are
// relative to this package; the test fails loudly rather than skipping if one
// moves, since a silently-skipped guard is how the "two thirds" survived.
var costCopies = []string{
	"../../charts/kubescrape/values.yaml",
	"../../deploy/agent.yaml",
}

func TestKubeletSummaryCostFigureMatchesTheCode(t *testing.T) {
	total, volumes := measureSummaryPoints(t, nil)

	// Volumes are the pipeline's cardinality lever and every copy says so in
	// words. The words have to keep matching the arithmetic: at six points per
	// measured volume the share moves as soon as the metric set does.
	share := float64(volumes) / float64(total)
	if share < 0.45 || share > 0.55 {
		t.Errorf("volume points are %d of %d (%.0f%%); the flag help, the chart and the manifest all call that %q — rewrite the words as well as the numbers",
			volumes, total, 100*share, "just over half")
	}

	want := []string{strconv.Itoa(total), strconv.Itoa(volumes), strconv.Itoa(costPods)}
	sources := map[string]string{"the -kubelet-summary flag help": flagHelp(t, "kubelet-summary")}
	for _, path := range costCopies {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading a copy of the cost figure: %v", err)
		}
		sources[path] = string(b)
	}
	for name, text := range sources {
		for _, n := range want {
			if !strings.Contains(text, n) {
				t.Errorf("%s does not quote %s; the measured shape is %d pods x %d containers x %d volumes = %d data points, %d of them volume series",
					name, n, costPods, costContainers, costVolumes, total, volumes)
			}
		}
	}
}

// values.yaml prints a worked drop rule under it — the pipeline's only
// cardinality lever, since the toggle is all or nothing — and quotes what the
// rule saves. A config example is the kind of prose that is copied verbatim and
// never run: a label key spelled k8s_volume_name instead of k8s.volume.name
// matches nothing, drops nothing and looks exactly like a rule that works.
//
// So it is run. The rules here are the ones in the comment, and the comment has
// to keep containing them; the numbers are what the scrape actually loses.
func TestKubeletSummaryChartDropExampleSavesWhatItSays(t *testing.T) {
	rules := []promscrape.FilterRule{
		{Action: "drop", Metrics: `k8s\.volume\..+`, Labels: map[string]string{"k8s.volume.name": "kube-api-access-.*"}},
		{Action: "drop", Metrics: `k8s\.pod\.process\.count`},
	}
	filters, err := promscrape.NewMetricFilters(map[string][]promscrape.FilterRule{"summary": rules})
	if err != nil {
		t.Fatalf("the documented rules do not compile: %v", err)
	}

	// Half the volume points (one of the two volumes per pod is the token) and
	// every pod's process count.
	const (
		wantTotal   = 1780
		wantVolumes = 660
	)
	total, volumes := measureSummaryPoints(t, filters)
	if total != wantTotal || volumes != wantVolumes {
		t.Errorf("the documented drop rules left %d points (%d volume), want %d (%d volume) — either they no longer select what they name, or the saving quoted in values.yaml is wrong",
			total, volumes, wantTotal, wantVolumes)
	}

	values, err := os.ReadFile("../../charts/kubescrape/values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, frag := range []string{
		`k8s\.volume\..+`,
		"k8s.volume.name: 'kube-api-access-.*'",
		`k8s\.pod\.process\.count`,
		strconv.Itoa(wantTotal),
	} {
		if !strings.Contains(string(values), frag) {
			t.Errorf("values.yaml no longer contains %q, so what this test exercises is not what an operator copies", frag)
		}
	}
}

// flagHelp is the registered usage string, which is what -help prints and what
// docs/FLAGS.md is generated from.
func flagHelp(t *testing.T, name string) string {
	t.Helper()
	f := flag.Lookup(name)
	if f == nil {
		t.Fatalf("no -%s flag is registered", name)
	}
	return f.Usage
}

// measureSummaryPoints runs one real summary scrape of a synthetic node through
// filters (nil = keep everything) and returns the data points exported and how
// many of them are volume series. It goes through the exported Scraper rather
// than reimplementing the conversion: a count this package computed for itself
// would agree with the documentation and with nothing else.
func measureSummaryPoints(t *testing.T, filters *promscrape.MetricFilters) (total, volumes int) {
	t.Helper()

	body := summaryDocument(costPods, costContainers, costVolumes)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stats/summary" {
			t.Errorf("the scraper fetched %q; only /stats/summary is enabled here", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	exp := &countingExporter{done: make(chan struct{}, 1)}
	sc := promscrape.New(promscrape.Config{
		Node:     "node1",
		Interval: time.Hour, // one cycle, then park
		Timeout:  30 * time.Second,
		// Only the summary scrape: annotation targets and the other two kubelet
		// endpoints would add points the figure does not claim.
		DisableTargets: true,
		HealthMetrics:  false,
		Filters:        filters,
		Exporter:       exp,
		StartTime:      time.Now(),
		Logger:         slog.New(slog.NewTextHandler(&testWriter{t}, nil)),
		Kubelet: promscrape.KubeletConfig{
			Endpoint: srv.URL,
			Summary:  true,
			Meta:     costMetaSource{},
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sc.Run(ctx)
	select {
	case <-exp.done:
	case <-time.After(30 * time.Second):
		t.Fatal("the summary scrape exported nothing")
	}
	cancel()

	for _, md := range exp.batches() {
		for _, rm := range md.ResourceMetrics().All() {
			for _, sm := range rm.ScopeMetrics().All() {
				for _, m := range sm.Metrics().All() {
					n := m.Gauge().DataPoints().Len()
					total += n
					if strings.HasPrefix(m.Name(), "k8s.volume.") {
						volumes += n
					}
				}
			}
		}
	}
	t.Logf("%d pods x %d containers x %d volumes: %d data points, %d of them volumes (%.1f%%)",
		costPods, costContainers, costVolumes, total, volumes, 100*float64(volumes)/float64(total))
	return total, volumes
}

// summaryDocument renders a kubelet /stats/summary body. It carries the blocks
// a real kubelet sends and this pipeline deliberately ignores — cpu, memory,
// network, and the pod/container ephemeral available/capacity/inodes numbers
// the kubelet copies verbatim from the node's rootfs — so the count measured
// here is the count of what is EXPORTED, not of what was offered.
func summaryDocument(pods, containers, volumes int) []byte {
	const ts = `"time":"2026-08-13T10:00:00Z"`
	var b strings.Builder
	fs := func(used, inodesUsed int) string {
		return fmt.Sprintf(`{%s,"availableBytes":40000000000,"capacityBytes":50000000000,"usedBytes":%d,"inodesFree":2900000,"inodes":3000000,"inodesUsed":%d}`,
			ts, used, inodesUsed)
	}
	b.WriteString(`{"node":{"nodeName":"node1",` +
		`"cpu":{` + ts + `,"usageNanoCores":1200000000,"usageCoreNanoSeconds":98765432100},` +
		`"memory":{` + ts + `,"workingSetBytes":8000000000},` +
		`"network":{` + ts + `,"name":"eth0","rxBytes":123,"txBytes":456},` +
		`"fs":` + fs(10000000000, 100000) + `,` +
		`"runtime":{"imageFs":` + fs(8000000000, 50000) + `,"containerFs":` + fs(2000000000, 10000) + `},` +
		`"rlimit":{` + ts + `,"maxpid":131072,"curproc":420}},"pods":[`)
	for p := range pods {
		if p > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"podRef":{"name":"workload-%d-5f7b9c8d6-abcde","namespace":"team-%d","uid":"%08x-1111-2222-3333-444455556666"},`, p, p%12, p)
		b.WriteString(`"cpu":{` + ts + `,"usageNanoCores":2500000,"usageCoreNanoSeconds":812345678},` +
			`"memory":{` + ts + `,"workingSetBytes":52428800},` +
			`"network":{` + ts + `,"name":"eth0","rxBytes":9876,"txBytes":5432},` +
			// available/capacity/inodes/inodesFree here are the NODE's rootfs
			// numbers, copied onto every pod; only usedBytes and inodesUsed are
			// the pod's own and only those two are exported.
			`"ephemeral-storage":{` + ts + `,"availableBytes":40000000000,"capacityBytes":50000000000,"usedBytes":38912,"inodesFree":2900000,"inodes":3000000,"inodesUsed":18},` +
			`"process_stats":{"process_count":6},"containers":[`)
		for c := range containers {
			if c > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `{"name":"container-%d","startTime":"2026-08-13T09:00:00Z",`+
				`"cpu":{%s,"usageNanoCores":1200000,"usageCoreNanoSeconds":412345678},`+
				`"memory":{%s,"workingSetBytes":26214400},`+
				`"rootfs":{%s,"availableBytes":40000000000,"capacityBytes":50000000000,"usedBytes":%d,"inodesFree":2900000,"inodes":3000000,"inodesUsed":%d},`+
				`"logs":{%s,"availableBytes":40000000000,"capacityBytes":50000000000,"usedBytes":%d,"inodesFree":2900000,"inodes":3000000,"inodesUsed":1}}`,
				c, ts, ts, ts, 24576+c, 7+c, ts, 8192+c)
		}
		b.WriteString(`],"volume":[`)
		for v := range volumes {
			if v > 0 {
				b.WriteByte(',')
			}
			// v == 0 is the kube-api-access-* projected token volume every pod
			// mounts; the rest are PVC-backed.
			name := fmt.Sprintf("kube-api-access-%05d", p)
			pvc := ""
			if v > 0 {
				name = fmt.Sprintf("volume-%d", v)
				pvc = fmt.Sprintf(`,"pvcRef":{"name":"data-workload-%d","namespace":"team-%d"}`, p, p%12)
			}
			fmt.Fprintf(&b, `{"time":"2026-08-13T09:59:12Z","availableBytes":4188160,"capacityBytes":4194304,"usedBytes":%d,"inodesFree":511990,"inodes":512000,"inodesUsed":10,"name":%q%s}`,
				6144+v, name, pvc)
		}
		b.WriteString(`]}`)
	}
	b.WriteString(`]}`)
	return []byte(b.String())
}

// costMetaSource resolves every pod the document names, so the measurement runs
// the production (resolved) path.
type costMetaSource struct{}

func (costMetaSource) PodByName(_ context.Context, namespace, name string) (*kubemeta.Pod, error) {
	var idx int
	if _, err := fmt.Sscanf(name, "workload-%d-", &idx); err != nil {
		return nil, errors.New("not found")
	}
	return &kubemeta.Pod{
		Name: name, Namespace: namespace, NodeName: "node1",
		UID: fmt.Sprintf("%08x-1111-2222-3333-444455556666", idx),
		Containers: []kubemeta.Container{
			{Name: "container-0", ID: fmt.Sprintf("%064x", idx*2+1), Image: "registry.example/app:1"},
			{Name: "container-1", ID: fmt.Sprintf("%064x", idx*2+2), Image: "registry.example/sidecar:1"},
		},
	}, nil
}

func (costMetaSource) Container(context.Context, string, time.Duration) (*kubemeta.ContainerMetadata, error) {
	return nil, errors.New("not found")
}

// countingExporter keeps every exported payload and signals the first one.
type countingExporter struct {
	mu   sync.Mutex
	got  []pmetric.Metrics
	done chan struct{}
}

func (e *countingExporter) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	e.mu.Lock()
	e.got = append(e.got, md)
	e.mu.Unlock()
	select {
	case e.done <- struct{}{}:
	default:
	}
	return nil
}

func (e *countingExporter) batches() []pmetric.Metrics {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]pmetric.Metrics(nil), e.got...)
}

// testWriter routes the scraper's log through the test's output.
type testWriter struct{ t *testing.T }

func (w *testWriter) Write(p []byte) (int, error) {
	w.t.Logf("scraper: %s", strings.TrimRight(string(p), "\n"))
	return len(p), nil
}
