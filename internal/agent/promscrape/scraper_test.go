package promscrape

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
)

type captureExporter struct {
	mu      sync.Mutex
	batches []pmetric.Metrics
}

func (c *captureExporter) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batches = append(c.batches, md)
	return nil
}

func (c *captureExporter) points() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, b := range c.batches {
		n += b.DataPointCount()
	}
	return n
}

type staticTargets []kubemeta.ScrapeTarget

func (s staticTargets) NodeTargets(context.Context, string) ([]kubemeta.ScrapeTarget, error) {
	return s, nil
}

func testTarget(url string) kubemeta.ScrapeTarget {
	return kubemeta.ScrapeTarget{
		URL: url,
		Pod: kubemeta.Pod{
			Name: "pod1", Namespace: "ns1", UID: "uid1", NodeName: "node1",
			Owners: []kubemeta.Owner{{Kind: "Deployment", Name: "dep1"}},
		},
		Service: &kubemeta.Service{Name: "svc1", UID: "svc-uid"},
	}
}

func TestScrapeChunking(t *testing.T) {
	// 25 samples with a 10-point batch limit -> 3 exports.
	var body strings.Builder
	body.WriteString("# TYPE things counter\n")
	for i := 0; i < 25; i++ {
		fmt.Fprintf(&body, "things_total{i=\"%d\"} %d\n", i, i)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body.String()))
	}))
	defer srv.Close()

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		BatchPoints: 10, Targets: staticTargets{testTarget(srv.URL)}, Exporter: exp,
		StartTime: time.Now(),
	})
	s.cycle(context.Background())

	if len(exp.batches) != 3 {
		t.Fatalf("got %d batches, want 3", len(exp.batches))
	}
	if exp.points() != 25 {
		t.Fatalf("got %d points, want 25", exp.points())
	}

	// Resource attributes present on every batch.
	for i, b := range exp.batches {
		rm := b.ResourceMetrics().At(0)
		a := rm.Resource().Attributes()
		if v, _ := a.Get("k8s.pod.name"); v.Str() != "pod1" {
			t.Errorf("batch %d: k8s.pod.name = %q", i, v.Str())
		}
		if v, _ := a.Get("k8s.deployment.name"); v.Str() != "dep1" {
			t.Errorf("batch %d: k8s.deployment.name = %q", i, v.Str())
		}
		if v, _ := a.Get("k8s.service.name"); v.Str() != "svc1" {
			t.Errorf("batch %d: k8s.service.name = %q", i, v.Str())
		}
	}
	// Counters became monotonic cumulative sums.
	m := exp.batches[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	if m.Type() != pmetric.MetricTypeSum || !m.Sum().IsMonotonic() {
		t.Fatalf("metric type = %v", m.Type())
	}
}

func TestScrapeSampleLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for i := 0; i < 100; i++ {
			fmt.Fprintf(w, "m%d 1\n", i)
		}
	}))
	defer srv.Close()

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		MaxSamples: 10, BatchPoints: 1000,
		Targets: staticTargets{testTarget(srv.URL)}, Exporter: exp,
	})
	if err := s.scrapeTarget(context.Background(), testTarget(srv.URL)); err != ErrTooManySamples {
		t.Fatalf("err = %v, want ErrTooManySamples", err)
	}
}

func TestScrapeHistogramAndSummaryConversion(t *testing.T) {
	body := `# TYPE http_duration histogram
http_duration_bucket{path="/a",le="0.1"} 100
http_duration_bucket{path="/a",le="0.5"} 140
http_duration_bucket{path="/a",le="+Inf"} 150
http_duration_sum{path="/a"} 53.4
http_duration_count{path="/a"} 150
http_duration_bucket{path="/b",le="0.1"} 1
http_duration_bucket{path="/b",le="+Inf"} 3
http_duration_sum{path="/b"} 2
http_duration_count{path="/b"} 3
# TYPE rpc summary
rpc{quantile="0.5"} 1.1
rpc{quantile="0.99"} 3.2
rpc_sum 8000
rpc_count 2000
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{testTarget(srv.URL)}, Exporter: exp, StartTime: time.Now(),
	})
	if err := s.scrapeTarget(context.Background(), testTarget(srv.URL)); err != nil {
		t.Fatal(err)
	}
	if len(exp.batches) != 1 {
		t.Fatalf("got %d batches", len(exp.batches))
	}
	metrics := exp.batches[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	if metrics.Len() != 2 {
		t.Fatalf("got %d metrics, want histogram+summary", metrics.Len())
	}

	hist := metrics.At(0)
	if hist.Name() != "http_duration" || hist.Type() != pmetric.MetricTypeHistogram {
		t.Fatalf("metric 0 = %s %v", hist.Name(), hist.Type())
	}
	if hist.Histogram().AggregationTemporality() != pmetric.AggregationTemporalityCumulative {
		t.Error("histogram not cumulative")
	}
	dps := hist.Histogram().DataPoints()
	if dps.Len() != 2 {
		t.Fatalf("got %d histogram points, want 2 (one per label set)", dps.Len())
	}
	dp := dps.At(0)
	if v, _ := dp.Attributes().Get("path"); v.Str() != "/a" {
		t.Fatalf("dp0 path = %q", v.Str())
	}
	if dp.Count() != 150 || dp.Sum() != 53.4 {
		t.Errorf("dp0 count=%d sum=%v", dp.Count(), dp.Sum())
	}
	if b := dp.ExplicitBounds().AsRaw(); len(b) != 2 || b[0] != 0.1 || b[1] != 0.5 {
		t.Errorf("bounds = %v", b)
	}
	// De-cumulated: 100, 140-100, 150-140.
	if c := dp.BucketCounts().AsRaw(); len(c) != 3 || c[0] != 100 || c[1] != 40 || c[2] != 10 {
		t.Errorf("bucket counts = %v", c)
	}

	summ := metrics.At(1)
	if summ.Name() != "rpc" || summ.Type() != pmetric.MetricTypeSummary {
		t.Fatalf("metric 1 = %s %v", summ.Name(), summ.Type())
	}
	sdp := summ.Summary().DataPoints().At(0)
	if sdp.Count() != 2000 || sdp.Sum() != 8000 {
		t.Errorf("summary count=%d sum=%v", sdp.Count(), sdp.Sum())
	}
	if sdp.QuantileValues().Len() != 2 || sdp.QuantileValues().At(1).Quantile() != 0.99 ||
		sdp.QuantileValues().At(1).Value() != 3.2 {
		t.Errorf("quantiles = %+v", sdp.QuantileValues())
	}
}

func TestScrapeExemplars(t *testing.T) {
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	body := "# TYPE requests counter\n" +
		"requests_total 10 # {trace_id=\"" + traceID + "\",user=\"x\"} 1.5\n" +
		"# TYPE lat histogram\n" +
		"lat_bucket{le=\"1\"} 5 # {trace_id=\"" + traceID + "\"} 0.7\n" +
		"lat_bucket{le=\"+Inf\"} 6\n" +
		"lat_count 6\n" +
		"lat_sum 4.2\n" +
		"# EOF\n"
	var gotAccept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAccept = r.Header.Get("Accept")
		w.Header().Set("Content-Type", "application/openmetrics-text;version=1.0.0")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second, Exemplars: true,
		Targets: staticTargets{testTarget(srv.URL)}, Exporter: exp, StartTime: time.Now(),
	})
	if err := s.scrapeTarget(context.Background(), testTarget(srv.URL)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(gotAccept, "openmetrics") {
		t.Fatalf("Accept = %q, want OpenMetrics negotiation", gotAccept)
	}

	metrics := exp.batches[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	counter := metrics.At(0)
	if counter.Type() != pmetric.MetricTypeSum {
		t.Fatalf("metric 0 type = %v", counter.Type())
	}
	exs := counter.Sum().DataPoints().At(0).Exemplars()
	if exs.Len() != 1 {
		t.Fatalf("counter exemplars = %d", exs.Len())
	}
	ex := exs.At(0)
	if ex.DoubleValue() != 1.5 || ex.TraceID().String() != traceID {
		t.Errorf("exemplar value=%v traceID=%s", ex.DoubleValue(), ex.TraceID())
	}
	if v, ok := ex.FilteredAttributes().Get("user"); !ok || v.Str() != "x" {
		t.Errorf("filtered attributes = %+v", ex.FilteredAttributes().AsRaw())
	}

	hist := metrics.At(1)
	if hist.Type() != pmetric.MetricTypeHistogram {
		t.Fatalf("metric 1 type = %v", hist.Type())
	}
	hexs := hist.Histogram().DataPoints().At(0).Exemplars()
	if hexs.Len() != 1 || hexs.At(0).DoubleValue() != 0.7 {
		t.Fatalf("histogram exemplars = %d", hexs.Len())
	}
}

func TestScrapeExemplarsDisabled(t *testing.T) {
	body := "# TYPE r counter\nr_total 1 # {trace_id=\"abc\"} 0.5\n# EOF\n"
	var gotAccept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAccept = r.Header.Get("Accept")
		w.Header().Set("Content-Type", "application/openmetrics-text;version=1.0.0")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{testTarget(srv.URL)}, Exporter: exp, StartTime: time.Now(),
	})
	if err := s.scrapeTarget(context.Background(), testTarget(srv.URL)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(gotAccept, "openmetrics") {
		t.Fatalf("Accept = %q; should not negotiate OpenMetrics when exemplars are off", gotAccept)
	}
	m := exp.batches[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	if m.Sum().DataPoints().At(0).Exemplars().Len() != 0 {
		t.Fatal("exemplars attached despite being disabled")
	}
}

func TestScrapeAttrFilter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("m 1\n"))
	}))
	defer srv.Close()

	filter, err := attrs.NewFilter("", `k8s\.pod\.label\..*,url\.full,k8s\.service\..*`)
	if err != nil {
		t.Fatal(err)
	}
	builder, err := attrs.NewBuilder(&attrs.Config{
		Static:     map[string]string{"cluster": "test"},
		Attributes: map[string]string{"app": `{{ index .Pod.Labels "app" }}`},
	}, filter)
	if err != nil {
		t.Fatal(err)
	}
	target := testTarget(srv.URL)
	target.Pod.Labels = map[string]string{"app": "x"}

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{target}, Exporter: exp, StartTime: time.Now(),
		Attrs: &attrs.Builders{Targets: builder},
	})
	if err := s.scrapeTarget(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	got := exp.batches[0].ResourceMetrics().At(0).Resource().Attributes()
	for _, banned := range []string{"k8s.pod.label.app", "url.full", "k8s.service.name"} {
		if _, ok := got.Get(banned); ok {
			t.Errorf("filtered attribute %q still present: %v", banned, got.AsRaw())
		}
	}
	if v, _ := got.Get("k8s.pod.name"); v.Str() != "pod1" {
		t.Fatalf("kept attributes damaged: %v", got.AsRaw())
	}
	// Static and template attributes are injected before the filter runs.
	if v, _ := got.Get("cluster"); v.Str() != "test" {
		t.Errorf("static attribute missing: %v", got.AsRaw())
	}
	if v, _ := got.Get("app"); v.Str() != "x" {
		t.Errorf("template attribute missing: %v", got.AsRaw())
	}
}

func TestScrapeHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{testTarget(srv.URL)}, Exporter: exp,
	})
	if err := s.scrapeTarget(context.Background(), testTarget(srv.URL)); err == nil {
		t.Fatal("expected error for 503 response")
	}
	if len(exp.batches) != 0 {
		t.Fatalf("no batches expected, got %d", len(exp.batches))
	}
}
