package promscrape

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/promparse"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// k8sScrapeBody synthesizes a typical Kubernetes workload exposition: repeated
// namespace/pod labels, counters, gauges and histograms with 12 buckets.
func k8sScrapeBody(series int) string {
	var sb strings.Builder
	sb.WriteString("# TYPE http_requests_total counter\n")
	for i := range series {
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
	for i := range pods {
		fmt.Fprintf(&sb, "kube_pod_info{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",uid=\"0a1b2c3d-1111-2222-3333-4444555%05d\",node=\"node9\"} 1\n", i, i)
	}
	sb.WriteString("# TYPE kube_pod_status_phase gauge\n")
	for i := range pods {
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
		cb := newSplitBatcher(context.Background(), s, target, sp[0], time.Unix(2, 0))
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
	for i := range containers {
		fmt.Fprintf(&sb, "container_cpu_usage_seconds_total{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",container=\"app\",id=\"%s\",image=\"img:1\"} 12.5\n", i, cg(i))
	}
	sb.WriteString("# TYPE container_fs_usage_bytes gauge\n")
	for i := range containers {
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
				cb := newCadvisorBatcher(context.Background(), s, time.Unix(2, 0))
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
	for i := range sets {
		for bi, le := range bounds {
			fmt.Fprintf(&sb, "http_request_duration_seconds_bucket{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",container=\"app\",handler=\"/api/v1/orders\",method=\"GET\",le=\"%s\"} %d\n", i, le, (bi+1)*10)
		}
		fmt.Fprintf(&sb, "http_request_duration_seconds_sum{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",container=\"app\",handler=\"/api/v1/orders\",method=\"GET\"} 42.5\n", i)
		fmt.Fprintf(&sb, "http_request_duration_seconds_count{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",container=\"app\",handler=\"/api/v1/orders\",method=\"GET\"} 120\n", i)
	}
	sb.WriteString("# TYPE rpc_latency_seconds summary\n")
	for i := range sets {
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

// discardSink is a converter sink that emits nothing, so an allocation count
// taken through it is the CONVERTER's own and not pdata's.
type discardSink struct{}

func (discardSink) addNumber(Sample, bool)        {}
func (discardSink) addHistogram(string, *histAcc) {}
func (discardSink) addSummary(string, *summAcc)   {}

// A converter lives for one scrape, so its freelists start empty and the
// largest histogram/summary family of every scrape builds a FRESH accumulator
// per label set. Growing that accumulator's labels and buckets one append at a
// time cost ~9 allocations per series; presized from the previous label set
// (converter.bucketHint/quantHint) it is the struct, the key, the labels and
// the buckets — four. BenchmarkHistogramConvert reports it; this is what fails
// a build.
func TestHistogramConvertAllocationBudget(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	const sets = 400 // one histogram and one summary family of this many series
	var samples []Sample
	p := promparse.Get(promparse.Options{MaxLineBytes: 1 << 20})
	if _, err := p.Parse(strings.NewReader(histSummBody(sets)), func(s Sample) error {
		s.Labels = slices.Clone(s.Labels) // only valid during the callback
		samples = append(samples, s)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	promparse.Put(p)

	convert := func() {
		conv := newConverter(discardSink{}, nil)
		for _, s := range samples {
			if err := conv.add(s); err != nil {
				t.Fatal(err)
			}
		}
		if err := conv.finish(); err != nil {
			t.Fatal(err)
		}
	}
	// Four per fresh accumulator plus the per-converter maps and order slice
	// growing: measured 3,276 for the 800 series here, against 7,670 with the
	// accumulators grown one append at a time.
	const ceiling = 5 * 2 * sets
	if allocs := testing.AllocsPerRun(20, convert); allocs > ceiling {
		t.Fatalf("converting %d histogram and %d summary series allocates %v times, want <= %d "+
			"(an accumulator grown one append at a time costs ~9 per series)",
			sets, sets, allocs, ceiling)
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

	// Past maxMemoBytes the memo stops growing and mask() computes into the
	// session's scratch words instead — the path a target naming thousands of
	// long series lands on for the rest of its scrape, and one that must not
	// trade the memo's allocation for a per-sample one.
	spent := filter.session()
	spent.budget.bytes = maxMemoBytes
	if allocs := testing.AllocsPerRun(200, func() {
		spent.Keep("http_request_duration_seconds_bucket", labels)
	}); allocs != 0 {
		t.Fatalf("the over-budget filter path allocates %v times, want 0", allocs)
	}
	if len(spent.offsets) != 0 {
		t.Fatalf("the spent session still memoized %d names", len(spent.offsets))
	}

	// A filter past 128 rules takes a three-word mask; a memo hit is still a
	// reslice of the flat backing array.
	many := make([]FilterRule, 0, 130)
	for i := range 129 {
		many = append(many, FilterRule{Action: "drop", Metrics: fmt.Sprintf("never_%d_.+", i)})
	}
	many = append(many, FilterRule{Action: "keep", Metrics: "http_.+", Labels: map[string]string{"handler": "/api"}})
	wide, err := newMetricFilter(many)
	if err != nil {
		t.Fatal(err)
	}
	ws := wide.session()
	if ws.words != 3 {
		t.Fatalf("a %d-rule filter has a %d-word mask, want 3", len(many), ws.words)
	}
	ws.Keep("http_request_duration_seconds_bucket", labels) // warm both memos
	if allocs := testing.AllocsPerRun(200, func() {
		ws.Keep("http_request_duration_seconds_bucket", labels)
	}); allocs != 0 {
		t.Fatalf("the multi-word filter path allocates %v times, want 0", allocs)
	}
}

// relabelBenchChain is a kube-prometheus-shaped metricRelabelings chain: a
// name-only drop, a keep on a low-cardinality join, and a drop keyed on the
// high-cardinality pod label, which the per-rule last-seen memo cannot help.
func relabelBenchChain(tb testing.TB) *relabelFilter {
	tb.Helper()
	var c relabelCache
	f, _, err := c.session([]kubemeta.RelabelRule{
		{Action: "drop", SourceLabels: []string{"__name__"}, Regex: "(go_|promhttp_|process_start_).+"},
		{Action: "keep", SourceLabels: []string{"namespace", "container"}, Regex: "prod-.*;app"},
		{Action: "drop", SourceLabels: []string{"pod"}, Regex: "canary-.*"},
	})
	if err != nil {
		tb.Fatal(err)
	}
	return f
}

// The relabel chain runs once per SAMPLE for every target a monitor with
// metricRelabelings selects. Its per-rule last-seen memo must cost nothing on
// either path: a HIT is a memcmp against a buffer the rule already owns, and a
// MISS re-copies into that buffer's existing capacity. Two label sets
// alternate here so every call exercises the pod rule's miss.
func TestRelabelChainAllocationBudget(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	f := relabelBenchChain(t)
	a := []Label{{Name: "namespace", Value: "prod-payments"}, {Name: "pod", Value: "payments-6f7b9c001"}, {Name: "container", Value: "app"}}
	b := []Label{{Name: "namespace", Value: "prod-payments"}, {Name: "pod", Value: "payments-6f7b9c002"}, {Name: "container", Value: "app"}}
	f.Keep("http_requests_total", a) // warm every rule's memo buffer
	f.Keep("http_requests_total", b)
	if allocs := testing.AllocsPerRun(200, func() {
		f.Keep("http_requests_total", a)
		f.Keep("http_requests_total", b)
	}); allocs != 0 {
		t.Fatalf("the per-sample relabel path allocates %v times, want 0", allocs)
	}
}

// BenchmarkRelabelKeep REPORTS the relabel chain's per-sample cost over a
// family-ordered exposition's label sets; TestRelabelChainAllocationBudget is
// what fails a build.
func BenchmarkRelabelKeep(b *testing.B) {
	f := relabelBenchChain(b)
	sets := make([][]Label, 64)
	for i := range sets {
		sets[i] = []Label{
			{Name: "namespace", Value: "prod-payments"},
			{Name: "pod", Value: fmt.Sprintf("payments-6f7b9c%03d", i)},
			{Name: "container", Value: "app"},
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		for _, ls := range sets {
			f.Keep("http_requests_total", ls)
		}
	}
}

// protoHistBody synthesizes a delimited-protobuf exposition of one classic
// histogram family: `metrics` label sets of 12 buckets each, the shape
// -scrape-native-histograms puts EVERY classic family of every target on.
func protoHistBody(tb testing.TB, metrics int) []byte {
	tb.Helper()
	bounds := []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, math.Inf(1)}
	fam := &dto.MetricFamily{
		Name: new("http_request_duration_seconds"), Help: new("Request latency."), Unit: new("seconds"),
		Type: dto.MetricType_HISTOGRAM.Enum(),
	}
	for i := range metrics {
		h := &dto.Histogram{SampleCount: new(uint64(120)), SampleSum: new(42.5)}
		for bi, ub := range bounds {
			h.Bucket = append(h.Bucket, &dto.Bucket{UpperBound: new(ub), CumulativeCount: new(uint64((bi + 1) * 10))})
		}
		fam.Metric = append(fam.Metric, &dto.Metric{
			Label: []*dto.LabelPair{
				{Name: new("namespace"), Value: new("prod-payments")},
				{Name: new("pod"), Value: new(fmt.Sprintf("payments-6f7b9c%03d", i))},
				{Name: new("handler"), Value: new("/api/v1/orders")},
			},
			Histogram: h,
		})
	}
	return protoBody(tb, fam)
}

const protoBenchMetrics = 500

// protoBenchScraper feeds one protobuf exposition through the protobuf front.
func protoBenchScrape(tb testing.TB, s *Scraper, body []byte) {
	tb.Helper()
	cb := newBatcher(func(pcommon.Resource) {}, time.Unix(1, 0), time.Unix(2, 0))
	ss := s.newScrapeSession(context.Background(), cb, pipelineTargets, "t", "t", nil, true)
	if _, err := ss.parseProtoAndExport(bytes.NewReader(body)); err != nil {
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
// allocations per scrape per target. It also copied the metric's whole label
// slice into a fresh array for every bucket row (one component slice per
// Metric now, only its le slot rewritten): 50,617 -> 44,620 allocs per scrape
// here, 7.23 -> 6.37 per sample, which is what the ceiling of 7 pins.
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
	if perSample := allocs / samples; perSample > 7 {
		t.Fatalf("the protobuf front allocates %.2f times per sample (%v per scrape), want <= 7: "+
			"the family-invariant names, the bucket-bound strings or the component label sets are being rebuilt per row", perSample, allocs)
	}
}

// BenchmarkScrapeSession drives the WHOLE production text front, which no
// other benchmark in this package covers: parseAndExportFiltered ->
// scrapeSession.accept (the MaxSamples bound, the filter session, the relabel
// chain) -> converter.add -> converter.check -> exportIfFull, over a live
// batcher whose chunk bound actually flushes.
//
// BenchmarkConvertScrape deliberately measures the CONVERTER, so it calls
// fs.Keep and conv.add directly and builds its converter with a NIL emit hook.
// The consequence is that the per-sample fullness poll — two interface calls
// through the batch interface, once per data point — is invisible to it, as is
// everything accept does. Both are on the real path for every one of the 100k
// samples this package exists to survive, so a regression in either would have
// shown up in no benchmark at all.
//
// The exporter must DISCARD: captureExporter retains every chunk, which grows
// the live heap across iterations until the GC, not the parse path, is what is
// being measured.
func BenchmarkScrapeSession(b *testing.B) {
	input := k8sScrapeBody(4000)
	filters, err := NewMetricFilters(map[string][]FilterRule{
		"targets": {{Action: "drop", Metrics: "(go_|promhttp_|process_start_).+"}},
	})
	if err != nil {
		b.Fatal(err)
	}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: time.Second,
		Targets: staticTargets{}, Exporter: discardExporter{},
		StartTime: time.Unix(1, 0), Filters: filters,
	})
	ctx := context.Background()
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	for b.Loop() {
		cb := newBatcher(func(pcommon.Resource) {}, time.Unix(1, 0), time.Unix(2, 0))
		if _, err := s.parseAndExportFiltered(ctx, strings.NewReader(input), false, false, cb, "targets", "t", "t", nil); err != nil {
			b.Fatal(err)
		}
	}
}
