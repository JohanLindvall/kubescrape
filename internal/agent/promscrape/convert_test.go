// Tests for exposition-to-OTLP conversion (convert.go): family shape
// handling and the point/byte-bounded chunker.
package promscrape

import (
	"context"
	"fmt"
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
	bt := newBatcher(func(pcommon.Resource) {}, 1<<30, time.Unix(1, 0), time.Unix(2, 0))
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
`
	bt := newBatcher(func(pcommon.Resource) {}, 1<<30, time.Unix(1, 0), time.Unix(2, 0))
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
	if conv.malformed != 3 { // rpc_count, lat_bucket{le=1}, lat_count
		t.Fatalf("malformed = %d, want 3", conv.malformed)
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
