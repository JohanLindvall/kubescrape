// Tests for exposition-to-OTLP conversion (convert.go): family shape
// handling and the point/byte-bounded chunker.
package promscrape

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/promparse"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// A family name reused across incompatible metric shapes (a histogram family
// then a bare number sample of the same name) must skip the colliding sample,
// count it (obs.ScrapeCollisions), and leave the rest of the scrape intact —
// the numberDataPoint default branch.
func TestNumberSampleOnHistogramFamilySkipped(t *testing.T) {
	// The bare "lat 42" arrives AFTER the histogram family flushed (the family
	// switch at ok_total emits it), so the name is already claimed by a
	// Histogram-shaped metric when the number sample reaches the batcher.
	body := `# TYPE lat histogram
lat_bucket{le="1"} 5
lat_bucket{le="+Inf"} 7
lat_sum 3.5
lat_count 7
# TYPE ok counter
ok_total 1
lat 42
`
	before := obs.ScrapeCollisions.Value()
	bt := newBatcher(func(pcommon.Resource) {}, time.Unix(1, 0), time.Unix(2, 0))
	conv := newConverter(bt, nil)
	p := newParser(promparse.Options{MaxLineBytes: 1 << 20})
	if _, err := p.Parse(strings.NewReader(body), func(s Sample) error {
		return conv.add(s)
	}); err != nil {
		t.Fatal(err)
	}
	if err := conv.finish(); err != nil {
		t.Fatal(err)
	}

	if got := obs.ScrapeCollisions.Value() - before; got != 1 {
		t.Fatalf("collision delta = %v, want 1 (the bare lat sample)", got)
	}
	metrics := bt.take().ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	byName := map[string]pmetric.MetricType{}
	for i := 0; i < metrics.Len(); i++ {
		byName[metrics.At(i).Name()] = metrics.At(i).Type()
	}
	if byName["lat"] != pmetric.MetricTypeHistogram {
		t.Fatalf("lat = %v, want the Histogram (the number sample must not claim it)", byName["lat"])
	}
	if byName["ok_total"] != pmetric.MetricTypeSum {
		t.Fatalf("rest of the scrape lost: %v", byName)
	}
}

// Negative/NaN cumulative counts wrap uint64 into ~9.2e18 garbage; such
// exposition must be counted malformed, not exported.
func TestNegativeCountCountedMalformed(t *testing.T) {
	body := `# TYPE rpc summary
rpc_sum 8000
rpc_count -1
# TYPE lat histogram
lat_bucket{le="1"} -5
lat_bucket{le="+Inf"} 7
lat_count NaN
lat_sum 1
# TYPE inf summary
inf_sum 1
inf_count +Inf
# TYPE huge summary
huge_sum 1
huge_count 1e300
`
	bt := newBatcher(func(pcommon.Resource) {}, time.Unix(1, 0), time.Unix(2, 0))
	conv := newConverter(bt, nil)
	p := newParser(promparse.Options{MaxLineBytes: 1 << 20})
	if _, err := p.Parse(strings.NewReader(body), func(s Sample) error {
		return conv.add(s)
	}); err != nil {
		t.Fatal(err)
	}
	if err := conv.finish(); err != nil {
		t.Fatal(err)
	}
	if conv.malformed != 5 { // rpc_count, lat_bucket{le=1}, lat_count, inf_count, huge_count
		t.Fatalf("malformed = %d, want 5", conv.malformed)
	}
	// Nothing exported a wrapped ~9.2e18 count.
	md := bt.take()
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		ms := rms.At(i).ScopeMetrics().At(0).Metrics()
		for j := 0; j < ms.Len(); j++ {
			m := ms.At(j)
			switch m.Type() {
			case pmetric.MetricTypeSummary:
				for k := 0; k < m.Summary().DataPoints().Len(); k++ {
					if m.Summary().DataPoints().At(k).Count() > 1<<40 {
						t.Fatalf("summary count wrapped: %d", m.Summary().DataPoints().At(k).Count())
					}
				}
			case pmetric.MetricTypeHistogram:
				for k := 0; k < m.Histogram().DataPoints().Len(); k++ {
					if m.Histogram().DataPoints().At(k).Count() > 1<<40 {
						t.Fatalf("histogram count wrapped: %d", m.Histogram().DataPoints().At(k).Count())
					}
				}
			}
		}
	}
}

// helpUnitBody exercises every family shape with a "# HELP"/"# UNIT" pair.
const helpUnitBody = `# TYPE http_requests counter
# HELP http_requests Total requests handled.
# UNIT http_requests requests
http_requests_total{code="200"} 5
# TYPE rpc_latency_seconds histogram
# HELP rpc_latency_seconds RPC latency.
# UNIT rpc_latency_seconds seconds
rpc_latency_seconds_bucket{le="0.5"} 1
rpc_latency_seconds_bucket{le="+Inf"} 2
rpc_latency_seconds_sum 0.4
rpc_latency_seconds_count 2
# TYPE rpc_size_bytes summary
# HELP rpc_size_bytes Payload size.
# UNIT rpc_size_bytes bytes
rpc_size_bytes{quantile="0.5"} 3
rpc_size_bytes_sum 6
rpc_size_bytes_count 2
# TYPE mem_bytes gauge
# HELP mem_bytes Resident memory.
mem_bytes 42
# TYPE undocumented gauge
undocumented 1
# EOF
`

// descriptions collects name -> "description|unit" across every resource of a
// payload.
func descriptions(md pmetric.Metrics) map[string]string {
	out := map[string]string{}
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				out[m.Name()] = m.Description() + "|" + m.Unit()
			}
		}
	}
	return out
}

// convertBody runs an exposition through the converter into cb.
func convertBody(t *testing.T, cb chunker, body string, openMetrics bool) {
	t.Helper()
	conv := newConverter(cb, nil)
	p := newParser(promparse.Options{MaxLineBytes: 1 << 20, OpenMetrics: openMetrics})
	if _, err := p.Parse(strings.NewReader(body), func(s Sample) error { return conv.add(s) }); err != nil {
		t.Fatal(err)
	}
	if err := conv.finish(); err != nil {
		t.Fatal(err)
	}
}

// HELP and UNIT are the standard Prometheus -> OTLP mapping for a metric's
// Description and Unit — what Grafana and every other OTLP consumer displays.
// Every family shape must carry them, on every batcher.
func TestHelpAndUnitOnExportedMetrics(t *testing.T) {
	bt := newBatcher(func(pcommon.Resource) {}, time.Unix(1, 0), time.Unix(2, 0))
	convertBody(t, bt, helpUnitBody, true)
	got := descriptions(bt.take())
	want := map[string]string{
		"http_requests_total": "Total requests handled.|requests",
		"rpc_latency_seconds": "RPC latency.|seconds",
		"rpc_size_bytes":      "Payload size.|bytes",
		"mem_bytes":           "Resident memory.|",
		"undocumented":        "|",
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s = %q, want %q", name, got[name], w)
		}
	}
}

// The split and cadvisor batchers emit one resource per DESCRIBED object, each
// with its own copy of the descriptor — the description must be on all of them,
// not only the first.
func TestHelpAndUnitOnPerObjectResources(t *testing.T) {
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: time.Second,
		Targets: staticTargets{}, Exporter: &captureExporter{},
		Kubelet: KubeletConfig{Meta: &fakeMetaSource{}}, StartTime: time.Unix(1, 0),
	})

	t.Run("split", func(t *testing.T) {
		sp, err := NewSplitters([]SplitterConfig{{
			Match: SplitterMatch{PodName: "ksm-.+"},
			Rules: []SplitRule{{Metrics: `kube_pod_.+`, GroupBy: map[string]string{
				"namespace": "k8s.namespace.name", "pod": "k8s.pod.name",
			}}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		target := testTarget("http://ksm:8080/metrics")
		target.Pod.Name = "ksm-abc"
		cb := newSplitBatcher(context.Background(), s, target, sp[0], time.Unix(2, 0))
		convertBody(t, cb, `# HELP kube_pod_info Information about pod.
# TYPE kube_pod_info gauge
kube_pod_info{namespace="ns1",pod="p1"} 1
kube_pod_info{namespace="ns1",pod="p2"} 1
`, false)
		md := cb.take()
		if n := md.ResourceMetrics().Len(); n != 2 {
			t.Fatalf("resources = %d, want one per described pod", n)
		}
		for i := 0; i < md.ResourceMetrics().Len(); i++ {
			m := md.ResourceMetrics().At(i).ScopeMetrics().At(0).Metrics().At(0)
			if m.Description() != "Information about pod." {
				t.Errorf("resource %d description = %q", i, m.Description())
			}
		}
	})

	t.Run("cadvisor", func(t *testing.T) {
		cb := newCadvisorBatcher(context.Background(), s, time.Unix(2, 0))
		convertBody(t, cb, `# HELP container_cpu_usage_seconds_total Cumulative cpu time consumed.
# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{namespace="ns1",pod="pod1",container="app",id="/kubepods/burstable/pod`+uid1+`/`+appCID+`"} 12.5
container_cpu_usage_seconds_total{id="/kubepods"} 100
`, false)
		md := cb.take()
		if n := md.ResourceMetrics().Len(); n != 2 {
			t.Fatalf("resources = %d, want the container and the rollup", n)
		}
		for i := 0; i < md.ResourceMetrics().Len(); i++ {
			m := md.ResourceMetrics().At(i).ScopeMetrics().At(0).Metrics().At(0)
			if m.Description() != "Cumulative cpu time consumed." {
				t.Errorf("resource %d description = %q", i, m.Description())
			}
		}
	})
}

// The chunk-size estimate gates every flush; a description it does not charge
// for is a chunk that encodes past the collector's receive limit. The estimate
// must grow by at least what the descriptions actually add to the payload.
func TestDescriptionsChargedToSizeEstimate(t *testing.T) {
	help := strings.Repeat("x", 300)
	build := func(withHelp bool) (est, encoded int) {
		var body strings.Builder
		for i := 0; i < 20; i++ {
			if withHelp {
				fmt.Fprintf(&body, "# HELP fam%02d %s\n# UNIT fam%02d seconds\n", i, help, i)
			}
			fmt.Fprintf(&body, "# TYPE fam%02d counter\nfam%02d_total 1\n", i, i)
		}
		body.WriteString("# EOF\n")
		bt := newBatcher(func(pcommon.Resource) {}, time.Unix(1, 0), time.Unix(2, 0))
		convertBody(t, bt, body.String(), true)
		est = bt.size()
		var m pmetric.ProtoMarshaler
		return est, m.MetricsSize(bt.take())
	}
	estWith, encWith := build(true)
	estWithout, encWithout := build(false)
	if estWith-estWithout < encWith-encWithout {
		t.Fatalf("estimate grew by %d bytes but the payload grew by %d: an under-charged chunk flushes past the collector limit",
			estWith-estWithout, encWith-encWithout)
	}
}

// grpcDefaultLimit is the collector's default max_recv_msg_size. A payload past
// it is rejected wholesale, so every export of that target would fail.
const grpcDefaultLimit = 4 << 20

func serve(t *testing.T, body string) string {
	t.Helper()
	return serveBody(t, body).URL
}

// TestChunksStayUnderCollectorLimit: a label-rich family of 10k series marshals
// to well over 4 MiB at the default 10k-point BatchPoints. The byte bound must
// split it so no single payload can be rejected.
func TestChunksStayUnderCollectorLimit(t *testing.T) {
	var body strings.Builder
	body.WriteString("# TYPE http_requests counter\n")
	for i := 0; i < 20000; i++ {
		_, _ = fmt.Fprintf(&body, `http_requests_total{namespace="some-namespace-name",pod="workload-abcdef1234-xyz%05d",container="application-container",method="GET",path="/api/v1/resource/subresource/%05d",status="200",instance="10.244.13.%d:8080",job="some-long-job-name"} %d`+"\n", i, i, i%255, i)
	}
	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 30 * time.Second,
		Targets:   staticTargets{testTarget(serve(t, body.String()))},
		Exporter:  exp,
		StartTime: time.Now(),
	}) // BatchPoints and BatchBytes both defaulted
	s.cycle(context.Background())

	if exp.points() != 20000 {
		t.Fatalf("got %d points, want 20000 (nothing may be lost to chunking)", exp.points())
	}
	var m pmetric.ProtoMarshaler
	for i, b := range exp.batches {
		if sz := m.MetricsSize(b); sz > grpcDefaultLimit {
			t.Errorf("batch %d is %d bytes, over the collector's %d-byte limit", i, sz, grpcDefaultLimit)
		}
	}
	if len(exp.batches) < 2 {
		t.Fatalf("got %d batches: the byte bound never split the scrape", len(exp.batches))
	}
}

// TestHistogramFamilyDoesNotOvershoot: a single histogram family emits all its
// points at once when it ends. The chunk check must run BETWEEN those points,
// not only after the next parsed sample, or one family blows the limit.
func TestHistogramFamilyDoesNotOvershoot(t *testing.T) {
	var body strings.Builder
	body.WriteString("# TYPE latency histogram\n")
	bounds := []string{"0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "+Inf"}
	for i := 0; i < 12000; i++ {
		lbl := fmt.Sprintf(`handler="/api/v1/some/reasonably/long/path/%05d",method="GET",namespace="some-namespace-name",pod="workload-abcdef1234-xyz%05d"`, i, i)
		for j, b := range bounds {
			fmt.Fprintf(&body, "latency_bucket{%s,le=\"%s\"} %d\n", lbl, b, j+1)
		}
		_, _ = fmt.Fprintf(&body, "latency_sum{%s} 1.5\nlatency_count{%s} %d\n", lbl, lbl, len(bounds))
	}
	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 30 * time.Second,
		Targets:   staticTargets{testTarget(serve(t, body.String()))},
		Exporter:  exp,
		StartTime: time.Now(),
	})
	s.cycle(context.Background())

	if exp.points() != 12000 {
		t.Fatalf("got %d histogram points, want 12000", exp.points())
	}
	var m pmetric.ProtoMarshaler
	for i, b := range exp.batches {
		if sz := m.MetricsSize(b); sz > grpcDefaultLimit {
			t.Errorf("batch %d is %d bytes, over the collector's %d-byte limit", i, sz, grpcDefaultLimit)
		}
	}
	if len(exp.batches) < 2 {
		t.Fatalf("got %d batches: the family's emission was never split", len(exp.batches))
	}
}

// TestPartialScrapeExportedOnSampleLimit: hitting MaxSamples aborts the scrape,
// but everything converted up to that point must still be exported — dropping
// it would lose a whole scrape's worth of samples silently.
func TestPartialScrapeExportedOnSampleLimit(t *testing.T) {
	var body strings.Builder
	body.WriteString("# TYPE things counter\n")
	for i := 0; i < 500; i++ {
		_, _ = fmt.Fprintf(&body, "things_total{i=\"%d\"} %d\n", i, i)
	}
	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		BatchPoints: 10_000, MaxSamples: 100,
		Targets:   staticTargets{testTarget(serve(t, body.String()))},
		Exporter:  exp,
		StartTime: time.Now(),
	})
	s.cycle(context.Background())

	// 100 samples pass the limit check; the 101st aborts.
	if got := exp.points(); got != 100 {
		t.Fatalf("got %d exported points, want the 100 parsed before the abort", got)
	}
}

// TestPartialScrapeExportedOnTruncatedBody: a target that dies mid-body (or a
// scrape that times out reading it) must still yield what was already parsed.
func TestPartialScrapeExportedOnTruncatedBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100000") // promise more than we send
		for i := 0; i < 50; i++ {
			_, _ = fmt.Fprintf(w, "things_total{i=\"%d\"} %d\n", i, i)
		}
		w.(http.Flusher).Flush()
		// Hijack and close the connection: the client sees an unexpected EOF.
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	defer srv.Close()

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets:   staticTargets{testTarget(srv.URL)},
		Exporter:  exp,
		StartTime: time.Now(),
	})
	s.cycle(context.Background())

	if exp.points() == 0 {
		t.Fatal("truncated scrape exported nothing; the parsed prefix must survive")
	}
}

// Exemplar-rich scrapes must respect the byte bound too: exemplar labels land
// in FilteredAttributes and are unbounded by the parser, so charging a flat
// 48 bytes per exemplar let a conforming OpenMetrics endpoint (two ~50-char
// exemplar labels per bucket) build 8.6 MiB chunks — over the collector's
// 4 MiB receive limit, i.e. wholesale rejection of every export.
func TestExemplarChunksStayUnderCollectorLimit(t *testing.T) {
	var body strings.Builder
	body.WriteString("# TYPE lat histogram\n")
	ex := `# {zvalue="eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",wvalue="ffffffffffffffffffffffffffffffffffffffffffffffff"} 0.5`
	for i := 0; i < 6000; i++ {
		for b, le := range []string{"0.001", "0.01", "0.1", "1", "10", "+Inf"} {
			_, _ = fmt.Fprintf(&body, `lat_bucket{i="%06d",le=%q} %d %s`+"\n", i, le, b+1, ex)
		}
		_, _ = fmt.Fprintf(&body, "lat_sum{i=\"%06d\"} 1\nlat_count{i=\"%06d\"} 6\n", i, i)
	}
	body.WriteString("# EOF\n")
	// Exemplars parse only in OpenMetrics mode, detected from Content-Type.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/openmetrics-text; version=1.0.0")
		_, _ = w.Write([]byte(body.String()))
	}))
	t.Cleanup(srv.Close)
	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 30 * time.Second,
		Exemplars: true,
		Targets:   staticTargets{testTarget(srv.URL)},
		Exporter:  exp,
		StartTime: time.Now(),
	})
	s.cycle(context.Background())

	var m pmetric.ProtoMarshaler
	total := 0
	for i, b := range exp.batches {
		total += b.DataPointCount()
		if sz := m.MetricsSize(b); sz > grpcDefaultLimit {
			t.Errorf("batch %d is %d bytes, over the collector's %d-byte limit", i, sz, grpcDefaultLimit)
		}
	}
	if total != 6000 {
		t.Fatalf("got %d histogram points, want 6000", total)
	}
}

// Duplicate quantiles in malformed exposition ("0.5" twice, "0.50") must
// dedup keep-last like the bucket path — not emit multiple entries for one
// quantile on a single Summary point.
func TestDuplicateQuantilesDedupKeepLast(t *testing.T) {
	body := `# TYPE rpc summary
rpc{quantile="0.5"} 1
rpc{quantile="0.5"} 2
rpc{quantile="0.50"} 3
rpc{quantile="0.9"} 9
rpc_sum 1
rpc_count 4
`
	bt := newBatcher(func(pcommon.Resource) {}, time.Unix(1, 0), time.Unix(2, 0))
	conv := newConverter(bt, nil)
	p := newParser(promparse.Options{MaxLineBytes: 1 << 20})
	if _, err := p.Parse(strings.NewReader(body), func(s Sample) error { return conv.add(s) }); err != nil {
		t.Fatal(err)
	}
	if err := conv.finish(); err != nil {
		t.Fatal(err)
	}
	md := bt.take()
	dp := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Summary().DataPoints().At(0)
	if dp.QuantileValues().Len() != 2 {
		t.Fatalf("quantiles = %d, want 2 (0.5 deduped keep-last, 0.9)", dp.QuantileValues().Len())
	}
	if q := dp.QuantileValues().At(0); q.Quantile() != 0.5 || q.Value() != 3 {
		t.Fatalf("q0 = %v/%v, want 0.5/3 (last occurrence wins)", q.Quantile(), q.Value())
	}
}

// An exemplar timestamp can be parseable yet beyond the ms→ns int64 range (the
// parser bounds timestamps to int64 MILLISECONDS): the bare multiplication
// wrapped and stamped garbage on the exemplar, while the identical sample-path
// conversion (pointTS) fell back to the scrape time. The two paths must agree.
func TestExemplarTimestampOverflowFallsBack(t *testing.T) {
	fallback := pcommon.NewTimestampFromTime(time.Unix(1700000000, 0))
	dp := pmetric.NewMetrics().ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty().SetEmptySum().DataPoints().AppendEmpty()

	overflowMs := math.MaxInt64/int64(time.Millisecond) + 1
	for _, tsMs := range []int64{overflowMs, -overflowMs} {
		ex := dp.Exemplars().AppendEmpty()
		setExemplar(ex, Exemplar{Value: 1, TimestampMs: tsMs}, fallback)
		if ex.Timestamp() != fallback {
			t.Fatalf("timestamp %d ms: got %d, want the fallback %d", tsMs, ex.Timestamp(), fallback)
		}
	}

	// An in-range timestamp still converts exactly; an absent one falls back.
	ex := dp.Exemplars().AppendEmpty()
	setExemplar(ex, Exemplar{Value: 1, TimestampMs: 1_700_000_000_123}, fallback)
	if want := pcommon.Timestamp(1_700_000_000_123 * int64(time.Millisecond)); ex.Timestamp() != want {
		t.Fatalf("in-range: got %d, want %d", ex.Timestamp(), want)
	}
	ex = dp.Exemplars().AppendEmpty()
	setExemplar(ex, Exemplar{Value: 1}, fallback)
	if ex.Timestamp() != fallback {
		t.Fatalf("absent: got %d, want the fallback %d", ex.Timestamp(), fallback)
	}
}
