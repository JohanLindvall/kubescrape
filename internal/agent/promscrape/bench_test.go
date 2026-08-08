package promscrape

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"github.com/JohanLindvall/kubescrape/pkg/promparse"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// k8sScrapeBody synthesizes a typical Kubernetes workload exposition: repeated
// namespace/pod labels, counters, gauges and histograms with 12 buckets.
func k8sScrapeBody(series int) string {
	var sb strings.Builder
	sb.WriteString("# TYPE http_requests_total counter\n")
	for i := 0; i < series; i++ {
		fmt.Fprintf(&sb, "http_requests_total{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",container=\"app\",method=\"GET\",code=\"200\",path=\"/api/v1/orders\"} %d\n", i%40, i*7)
	}
	sb.WriteString("# TYPE process_resident_memory_bytes gauge\n")
	for i := 0; i < series/4; i++ {
		fmt.Fprintf(&sb, "process_resident_memory_bytes{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\"} %d\n", i%40, 100000000+i)
	}
	sb.WriteString("# TYPE http_request_duration_seconds histogram\n")
	bounds := []string{"0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "+Inf"}
	for i := 0; i < series/8; i++ {
		for bi, le := range bounds {
			fmt.Fprintf(&sb, "http_request_duration_seconds_bucket{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",handler=\"/api\",le=\"%s\"} %d\n", i%40, le, (bi+1)*10)
		}
		fmt.Fprintf(&sb, "http_request_duration_seconds_sum{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",handler=\"/api\"} 42.5\n", i%40)
		fmt.Fprintf(&sb, "http_request_duration_seconds_count{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",handler=\"/api\"} 120\n", i%40)
	}
	return sb.String()
}

// ksmSplitBody synthesizes a kube-state-metrics style exposition: family-major
// order, several rows per object for the phase-style families.
func ksmSplitBody(pods int) string {
	var sb strings.Builder
	sb.WriteString("# TYPE kube_pod_info gauge\n")
	for i := 0; i < pods; i++ {
		fmt.Fprintf(&sb, "kube_pod_info{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",uid=\"0a1b2c3d-1111-2222-3333-4444555%05d\",node=\"node9\"} 1\n", i, i)
	}
	sb.WriteString("# TYPE kube_pod_status_phase gauge\n")
	for i := 0; i < pods; i++ {
		for _, phase := range []string{"Pending", "Running", "Succeeded", "Failed", "Unknown"} {
			fmt.Fprintf(&sb, "kube_pod_status_phase{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",uid=\"0a1b2c3d-1111-2222-3333-4444555%05d\",phase=\"%s\"} 0\n", i, i, phase)
		}
	}
	return sb.String()
}

// BenchmarkSplitConvert measures the splitter routing path: parse -> convert
// -> per-object resources.
func BenchmarkSplitConvert(b *testing.B) {
	input := ksmSplitBody(200)
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodName: "ksm-.+"},
		Rules: []SplitRule{{
			Metrics: `kube_pod_.+`,
			GroupBy: map[string]string{
				"namespace": "k8s.namespace.name", "pod": "k8s.pod.name", "uid": "k8s.pod.uid",
			},
		}},
	}})
	if err != nil {
		b.Fatal(err)
	}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: time.Second,
		Targets: staticTargets{}, Exporter: &captureExporter{},
		Splitters: sp, Kubelet: KubeletConfig{Meta: &fakeMetaSource{}},
		StartTime: time.Unix(1, 0),
	})
	target := testTarget("http://ksm:8080/metrics")
	target.Pod.Name = "ksm-abc"
	var points int
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	for b.Loop() {
		cb := newSplitBatcher(s, context.Background(), target, sp[0], time.Unix(2, 0))
		conv := newConverter(cb, nil)
		p := promparse.Get(promparse.Options{MaxLineBytes: 1 << 20})
		_, err := p.Parse(strings.NewReader(input), func(smp Sample) error {
			_ = conv.add(smp)
			return nil
		})
		promparse.Put(p)
		if err != nil {
			b.Fatal(err)
		}
		_ = conv.finish()
		points = cb.count()
	}
	if points == 0 {
		b.Fatal("no points")
	}
}

// cadvisorBenchBody synthesizes a cadvisor exposition: family-major order,
// per-container rows with cgroup ids, plus a per-device family.
func cadvisorBenchBody(containers int) string {
	var sb strings.Builder
	cg := func(i int) string {
		return fmt.Sprintf("/kubepods/burstable/pod0a1b2c3d-1111-2222-3333-4444555%05d/d4f00c1e8a2b4c5d6e7f80912a3b4c5d6e7f80912a3b4c5d6e7f80912a3%05d", i, i)
	}
	sb.WriteString("# TYPE container_cpu_usage_seconds_total counter\n")
	for i := 0; i < containers; i++ {
		fmt.Fprintf(&sb, "container_cpu_usage_seconds_total{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",container=\"app\",id=\"%s\",image=\"img:1\"} 12.5\n", i, cg(i))
	}
	sb.WriteString("# TYPE container_fs_usage_bytes gauge\n")
	for i := 0; i < containers; i++ {
		for _, dev := range []string{"/dev/sda1", "/dev/sda2", "overlay"} {
			fmt.Fprintf(&sb, "container_fs_usage_bytes{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",container=\"app\",id=\"%s\",device=\"%s\"} 4096\n", i, cg(i), dev)
		}
	}
	return sb.String()
}

// BenchmarkCadvisorConvert measures the cadvisor routing path: parse ->
// identity from the cgroup id -> per-pod/container resources.
func BenchmarkCadvisorConvert(b *testing.B) {
	input := cadvisorBenchBody(100)
	for _, rollups := range []bool{true, false} {
		name := "rollups"
		if !rollups {
			name = "norollups"
		}
		b.Run(name, func(b *testing.B) {
			s := New(Config{
				Node: "node1", Interval: time.Hour, Timeout: time.Second,
				Targets: staticTargets{}, Exporter: &captureExporter{},
				Kubelet:   KubeletConfig{Meta: &fakeMetaSource{}, DisableRollups: !rollups},
				StartTime: time.Unix(1, 0),
			})
			var points int
			b.SetBytes(int64(len(input)))
			b.ReportAllocs()
			for b.Loop() {
				cb := newCadvisorBatcher(s, time.Unix(2, 0), context.Background())
				conv := newConverter(cb, nil)
				p := promparse.Get(promparse.Options{MaxLineBytes: 1 << 20})
				_, err := p.Parse(strings.NewReader(input), func(smp Sample) error {
					_ = conv.add(smp)
					return nil
				})
				promparse.Put(p)
				if err != nil {
					b.Fatal(err)
				}
				_ = conv.finish()
				points = cb.count()
			}
			if points == 0 {
				b.Fatal("no points")
			}
		})
	}
}

// histSummBody synthesizes a histogram/summary-only exposition: the component
// series of one point (12 _bucket rows plus _sum and _count) all carry the same
// labels bar le, which is what the converter's per-family grouping keys on.
func histSummBody(sets int) string {
	var sb strings.Builder
	bounds := []string{"0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "+Inf"}
	sb.WriteString("# TYPE http_request_duration_seconds histogram\n")
	for i := 0; i < sets; i++ {
		for bi, le := range bounds {
			fmt.Fprintf(&sb, "http_request_duration_seconds_bucket{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",container=\"app\",handler=\"/api/v1/orders\",method=\"GET\",le=\"%s\"} %d\n", i, le, (bi+1)*10)
		}
		fmt.Fprintf(&sb, "http_request_duration_seconds_sum{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",container=\"app\",handler=\"/api/v1/orders\",method=\"GET\"} 42.5\n", i)
		fmt.Fprintf(&sb, "http_request_duration_seconds_count{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",container=\"app\",handler=\"/api/v1/orders\",method=\"GET\"} 120\n", i)
	}
	sb.WriteString("# TYPE rpc_latency_seconds summary\n")
	for i := 0; i < sets; i++ {
		for _, q := range []string{"0.5", "0.9", "0.99"} {
			fmt.Fprintf(&sb, "rpc_latency_seconds{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",service=\"orders\",quantile=\"%s\"} 0.25\n", i, q)
		}
		fmt.Fprintf(&sb, "rpc_latency_seconds_sum{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",service=\"orders\"} 12.5\n", i)
		fmt.Fprintf(&sb, "rpc_latency_seconds_count{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",service=\"orders\"} 60\n", i)
	}
	return sb.String()
}

// BenchmarkHistogramConvert isolates the component-grouping path: every sample
// here routes through converter.hist/summ, i.e. through labelKey. The mixed
// BenchmarkConvertScrape dilutes it with counters and gauges, which never touch
// it.
func BenchmarkHistogramConvert(b *testing.B) {
	input := histSummBody(400)
	var points int
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	for b.Loop() {
		bt := newBatcher(func(pcommon.Resource) {}, time.Unix(1, 0), time.Unix(2, 0))
		conv := newConverter(bt, nil)
		p := promparse.Get(promparse.Options{MaxLineBytes: 1 << 20})
		_, err := p.Parse(strings.NewReader(input), func(s Sample) error {
			_ = conv.add(s)
			return nil
		})
		promparse.Put(p)
		if err != nil {
			b.Fatal(err)
		}
		_ = conv.finish()
		points = bt.count()
	}
	if points == 0 {
		b.Fatal("no points")
	}
}

// BenchmarkConvertScrape measures the full parse -> filter -> convert -> OTLP
// pipeline for a typical Kubernetes exposition.
func BenchmarkConvertScrape(b *testing.B) {
	input := k8sScrapeBody(4000)
	filter, err := newMetricFilter([]FilterRule{
		{Action: "keep", Metrics: "http_request_duration_seconds_bucket", Labels: map[string]string{"handler": "/api"}},
		{Action: "drop", Metrics: "(go_|promhttp_|process_start_).+"},
	})
	if err != nil {
		b.Fatal(err)
	}
	var points int
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	for b.Loop() {
		bt := newBatcher(func(pcommon.Resource) {}, time.Unix(1, 0), time.Unix(2, 0))
		conv := newConverter(bt, nil)
		fs := filter.session()
		p := promparse.Get(promparse.Options{MaxLineBytes: 1 << 20}) // the production path: pooled parser + reader
		_, err := p.Parse(strings.NewReader(input), func(s Sample) error {
			if !fs.Keep(s.Name, s.Labels) {
				return nil
			}
			_ = conv.add(s)
			return nil
		})
		promparse.Put(p)
		if err != nil {
			b.Fatal(err)
		}
		_ = conv.finish()
		points = bt.count()
	}
	if points == 0 {
		b.Fatal("no points")
	}
}

// The scrape loop calls fs.Keep once per SAMPLE — 100k times for the target
// size this package exists to survive. The per-scrape name->rule-bitmask memo
// and the per-(matcher,value) label-regex memo are what make that free after
// the first sample of a family; a regexp.MatchString on a converted string, or
// a map keyed by a struct built per call, would put an allocation back on every
// sample and only a benchmark would notice.
func TestFilterSessionAllocationBudget(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	filter, err := newMetricFilter([]FilterRule{
		{Action: "keep", Metrics: "http_request_duration_seconds_bucket", Labels: map[string]string{"handler": "/api"}},
		{Action: "drop", Metrics: "(go_|promhttp_|process_start_).+"},
	})
	if err != nil {
		t.Fatal(err)
	}
	labels := []Label{
		{Name: "namespace", Value: "prod-payments"},
		{Name: "pod", Value: "payments-6f7b9c001"},
		{Name: "handler", Value: "/api"},
		{Name: "le", Value: "0.05"},
	}
	fs := filter.session()
	fs.Keep("http_request_duration_seconds_bucket", labels) // warm both memos
	if allocs := testing.AllocsPerRun(200, func() {
		fs.Keep("http_request_duration_seconds_bucket", labels)
	}); allocs != 0 {
		t.Fatalf("the per-sample filter path allocates %v times, want 0", allocs)
	}
}

// protoHistBody synthesizes a delimited-protobuf exposition of one classic
// histogram family: `metrics` label sets of 12 buckets each, the shape
// -scrape-native-histograms puts EVERY classic family of every target on.
func protoHistBody(tb testing.TB, metrics int) []byte {
	bounds := []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, math.Inf(1)}
	fam := &dto.MetricFamily{
		Name: ptr("http_request_duration_seconds"), Help: ptr("Request latency."), Unit: ptr("seconds"),
		Type: dto.MetricType_HISTOGRAM.Enum(),
	}
	for i := 0; i < metrics; i++ {
		h := &dto.Histogram{SampleCount: ptr(uint64(120)), SampleSum: ptr(42.5)}
		for bi, ub := range bounds {
			h.Bucket = append(h.Bucket, &dto.Bucket{UpperBound: ptr(ub), CumulativeCount: ptr(uint64((bi + 1) * 10))})
		}
		fam.Metric = append(fam.Metric, &dto.Metric{
			Label: []*dto.LabelPair{
				{Name: ptr("namespace"), Value: ptr("prod-payments")},
				{Name: ptr("pod"), Value: ptr(fmt.Sprintf("payments-6f7b9c%03d", i))},
				{Name: ptr("handler"), Value: ptr("/api/v1/orders")},
			},
			Histogram: h,
		})
	}
	return protoBody(tb, fam)
}

const protoBenchMetrics = 500

// protoBenchScraper feeds one protobuf exposition through the protobuf front.
func protoBenchScrape(tb testing.TB, s *Scraper, body []byte) {
	cb := newBatcher(func(pcommon.Resource) {}, time.Unix(1, 0), time.Unix(2, 0))
	ss := s.newScrapeSession(context.Background(), cb, pipelineTargets, "t", "t", nil, true)
	if _, err := s.parseProtoAndExport(ss, bytes.NewReader(body)); err != nil {
		tb.Fatal(err)
	}
	if cb.count() == 0 {
		tb.Fatal("no points")
	}
}

// BenchmarkProtoClassicHistograms REPORTS the protobuf front's per-sample cost;
// TestProtoClassicHistogramAllocationBudget is what fails a build.
func BenchmarkProtoClassicHistograms(b *testing.B) {
	body := protoHistBody(b, protoBenchMetrics)
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: time.Second,
		NativeHistograms: true, Targets: staticTargets{}, Exporter: &captureExporter{},
		StartTime: time.Unix(1, 0),
	})
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	for b.Loop() {
		protoBenchScrape(b, s, body)
	}
}

// The protobuf front rebuilt three family-invariant names ("_bucket", "_sum",
// "_count") inside the per-Metric loop and rendered the same bucket bound with
// fmt.Sprintf — a string plus interface boxing — once per bucket of every
// metric. -scrape-native-histograms puts EVERY classic family of every target
// on this front, so at the package's 100k-series target that was ~1M avoidable
// allocations per scrape per target.
func TestProtoClassicHistogramAllocationBudget(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	body := protoHistBody(t, protoBenchMetrics)
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: time.Second,
		NativeHistograms: true, Targets: staticTargets{}, Exporter: &captureExporter{},
		StartTime: time.Unix(1, 0),
	})
	const samples = protoBenchMetrics * 14 // 12 buckets + _sum + _count
	allocs := testing.AllocsPerRun(20, func() { protoBenchScrape(t, s, body) })
	if perSample := allocs / samples; perSample > 8 {
		t.Fatalf("the protobuf front allocates %.2f times per sample (%v per scrape), want <= 8: "+
			"the family-invariant names or the bucket-bound strings are being rebuilt per metric", perSample, allocs)
	}
}
