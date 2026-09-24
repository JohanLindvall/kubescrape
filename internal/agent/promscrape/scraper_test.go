package promscrape

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/promparse"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
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
	for i := range 25 {
		fmt.Fprintf(&body, "things_total{i=\"%d\"} %d\n", i, i)
	}
	srv := serveBody(t, body.String())

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

func TestScrapeHealthMetrics(t *testing.T) {
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "m 1\nn 2\n")
	}))
	defer okSrv.Close()
	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer badSrv.Close()

	good := testTarget(okSrv.URL)
	bad := testTarget(badSrv.URL)
	bad.Pod.Name = "pod2"

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{good, bad}, Exporter: exp, StartTime: time.Now(),
		HealthMetrics: true,
	})
	s.cycle(context.Background())

	// The last batch is the health payload: one resource per target with
	// up / scrape_duration_seconds / scrape_samples_scraped.
	health := exp.batches[len(exp.batches)-1]
	if health.ResourceMetrics().Len() != 2 {
		t.Fatalf("health resources = %d", health.ResourceMetrics().Len())
	}
	ups := map[string]float64{}
	samples := map[string]float64{}
	for i := 0; i < health.ResourceMetrics().Len(); i++ {
		rm := health.ResourceMetrics().At(i)
		pod := attrStr(rm.Resource(), "k8s.pod.name")
		ms := rm.ScopeMetrics().At(0).Metrics()
		for j := 0; j < ms.Len(); j++ {
			switch ms.At(j).Name() {
			case "up":
				ups[pod] = ms.At(j).Gauge().DataPoints().At(0).DoubleValue()
			case "scrape_samples_scraped":
				samples[pod] = ms.At(j).Gauge().DataPoints().At(0).DoubleValue()
			}
		}
	}
	if ups["pod1"] != 1 || ups["pod2"] != 0 {
		t.Fatalf("up = %v", ups)
	}
	if samples["pod1"] != 2 || samples["pod2"] != 0 {
		t.Fatalf("scrape_samples_scraped = %v", samples)
	}
}

// hangingExporter blocks every export until its context is done — a
// blackholed collector behind an unbuffered chain — and records whether the
// context it was handed carried a deadline at all.
type hangingExporter struct{ unbounded atomic.Int64 }

func (h *hangingExporter) ExportMetrics(ctx context.Context, _ pmetric.Metrics) error {
	if _, ok := ctx.Deadline(); !ok {
		h.unbounded.Add(1)
	}
	<-ctx.Done()
	return ctx.Err()
}

// The health export runs after wg.Wait on Run's own context, so it was the one
// export of the cycle no scrape budget bounded: against a destination that
// hangs, cycle() — and with it the node's whole scrape loop, since Run cannot
// tick while it runs — waited out the exporter's retry budget (~48s at the
// defaults) every cycle. It is bounded by the cycle's clamp now, and the
// expiry is still warned about.
func TestHealthExportIsBoundedByTheCycleBudget(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError) // fails before any export
	}))
	t.Cleanup(bad.Close)

	const budget = 200 * time.Millisecond
	exp := &hangingExporter{}
	var buf strings.Builder // slog's handler serialises its own writes
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: budget,
		Targets: staticTargets{testTarget(bad.URL)}, Exporter: exp, StartTime: time.Now(),
		HealthMetrics: true,
	})
	s.log = slog.New(slog.NewTextHandler(&buf, nil))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	s.cycle(ctx)
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("cycle took %v against a %v budget: the health export waited on the hung destination for as long as the caller's context allowed", took, budget)
	}
	if n := exp.unbounded.Load(); n != 0 {
		t.Errorf("%d exports ran on a context with no deadline", n)
	}
	// cycle() joined every goroutine that logs before it returned.
	if out := buf.String(); !strings.Contains(out, "exporting scrape health metrics") {
		t.Errorf("the health export's own deadline expiring must still be warned about (only a shutdown is silent):\n%s", out)
	}
}

// A failing health export is a persisting condition — the collector is down,
// and otlpexport already narrates that with a transition, a re-warn and a
// recovery — while exportHealth runs once per cycle on every node. Its own line
// is throttled, or the outage is repeated at scrape cadence across the fleet.
func TestHealthExportFailureIsThrottled(t *testing.T) {
	srv := serveBody(t, "m 1\n")
	var buf strings.Builder // slog's handler serialises its own writes
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{testTarget(srv.URL)}, Exporter: rejectingExporter{}, StartTime: time.Now(),
		HealthMetrics: true,
	})
	s.log = slog.New(slog.NewTextHandler(&buf, nil))
	for range 3 {
		expireSchedule(s)
		s.cycle(context.Background())
	}
	if n := strings.Count(buf.String(), "exporting scrape health metrics"); n != 1 {
		t.Errorf("%d health-export warnings over 3 failing cycles, want 1:\n%s", n, buf.String())
	}
}

// The partial-scrape salvage's export failure is throttled per target the same
// way: an aborting target in front of a failing collector otherwise logged one
// Warn per target per cycle on every node.
func TestPartialScrapeExportFailureIsThrottled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for i := range 100 {
			_, _ = fmt.Fprintf(w, "m%d 1\n", i)
		}
	}))
	t.Cleanup(srv.Close)
	var buf strings.Builder
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		MaxSamples: 10, BatchPoints: 1000,
		Targets: staticTargets{testTarget(srv.URL)}, Exporter: rejectingExporter{},
	})
	s.log = slog.New(slog.NewTextHandler(&buf, nil))
	for range 3 {
		if _, err := s.scrapeTarget(context.Background(), testTarget(srv.URL), s.cfg.Timeout); !errors.Is(err, ErrTooManySamples) {
			t.Fatalf("err = %v, want ErrTooManySamples (the abort salvage runs on)", err)
		}
	}
	if n := strings.Count(buf.String(), "exporting partial scrape"); n != 1 {
		t.Errorf("%d partial-scrape export warnings over 3 aborted scrapes, want 1:\n%s", n, buf.String())
	}
}

func TestScrapeSampleLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for i := range 100 {
			_, _ = fmt.Fprintf(w, "m%d 1\n", i)
		}
	}))
	defer srv.Close()

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		MaxSamples: 10, BatchPoints: 1000,
		Targets: staticTargets{testTarget(srv.URL)}, Exporter: exp,
	})
	if _, err := s.scrapeTarget(context.Background(), testTarget(srv.URL), s.cfg.Timeout); err != ErrTooManySamples {
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
	srv := serveBody(t, body)

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{testTarget(srv.URL)}, Exporter: exp, StartTime: time.Now(),
	})
	if _, err := s.scrapeTarget(context.Background(), testTarget(srv.URL), s.cfg.Timeout); err != nil {
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

// A summary-typed sample without a quantile label must be counted as
// malformed, not emitted as a gauge under the family name — the gauge would
// claim the name and silently block the family's real Summary metric.
func TestSummaryWithoutQuantileCountedMalformed(t *testing.T) {
	body := `# TYPE rpc summary
rpc 5
rpc{quantile="0.5"} 1.1
rpc_sum 8000
rpc_count 2000
`
	bt := newBatcher(func(pcommon.Resource) {}, time.Unix(1, 0), time.Unix(2, 0))
	conv := newConverter(bt, nil)
	p := promparse.New(promparse.Options{MaxLineBytes: 1 << 20})
	malformed, err := p.Parse(strings.NewReader(body), func(s Sample) error {
		_ = conv.add(s)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = conv.finish()
	if malformed != 0 || conv.malformed != 1 {
		t.Fatalf("parser malformed = %d, converter malformed = %d, want 0 and 1", malformed, conv.malformed)
	}

	metrics := bt.take().ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	if metrics.Len() != 1 {
		t.Fatalf("got %d metrics, want only the summary", metrics.Len())
	}
	m := metrics.At(0)
	if m.Name() != "rpc" || m.Type() != pmetric.MetricTypeSummary {
		t.Fatalf("metric = %s %v, want rpc Summary", m.Name(), m.Type())
	}
	dp := m.Summary().DataPoints().At(0)
	if dp.Count() != 2000 || dp.Sum() != 8000 || dp.QuantileValues().Len() != 1 {
		t.Fatalf("summary dp = count %d sum %v quantiles %d", dp.Count(), dp.Sum(), dp.QuantileValues().Len())
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
	if _, err := s.scrapeTarget(context.Background(), testTarget(srv.URL), s.cfg.Timeout); err != nil {
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
	if _, err := s.scrapeTarget(context.Background(), testTarget(srv.URL), s.cfg.Timeout); err != nil {
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

// The Accept header is the one part of a scrape a TARGET negotiates on, so the
// exact bytes are wire-visible: the offers are composed from one const block
// (a fallback spelled once, protoContentType shared with the response check),
// and this pins that the composition renders what each mode always sent.
func TestScrapeTargetOffersTheAcceptHeaderOfItsMode(t *testing.T) {
	var gotAccept atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAccept.Store(r.Header.Get("Accept"))
		_, _ = w.Write([]byte("m 1\n"))
	}))
	defer srv.Close()

	for _, c := range []struct {
		name              string
		exemplars, native bool
		want              string
	}{
		{"text", false, false, "text/plain;version=0.0.4"},
		{"exemplars", true, false, "application/openmetrics-text;version=1.0.0;q=1,text/plain;version=0.0.4;q=0.5"},
		{"native histograms", true, true, "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited;q=1," +
			"application/openmetrics-text;version=1.0.0;q=0.8,text/plain;version=0.0.4;q=0.5"},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := New(Config{
				Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
				Exemplars: c.exemplars, NativeHistograms: c.native,
				Exporter: &captureExporter{}, StartTime: time.Now(),
			})
			if _, err := s.scrapeTarget(context.Background(), testTarget(srv.URL), s.cfg.Timeout); err != nil {
				t.Fatal(err)
			}
			if got, _ := gotAccept.Load().(string); got != c.want {
				t.Errorf("Accept = %q, want %q", got, c.want)
			}
		})
	}
	if acceptJSON != "application/json" {
		t.Errorf("acceptJSON = %q", acceptJSON)
	}
}

func TestScrapeAttrFilter(t *testing.T) {
	srv := serveBody(t, "m 1\n")

	filter, err := attrs.NewFilterFromLists(nil, []string{`k8s\.pod\.label\..*`, `url\.full`, `k8s\.service\..*`})
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
	if _, err := s.scrapeTarget(context.Background(), target, s.cfg.Timeout); err != nil {
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
	if _, err := s.scrapeTarget(context.Background(), testTarget(srv.URL), s.cfg.Timeout); err == nil {
		t.Fatal("expected error for 503 response")
	}
	if len(exp.batches) != 0 {
		t.Fatalf("no batches expected, got %d", len(exp.batches))
	}
}

// TestTypeRedeclarationDoesNotCorruptLaterFamilies pins the flushFamily
// delete-as-emitted fix: a family TYPE-redeclared histogram->summary put its
// key into order twice, double-freeing an accumulator so two later label sets
// shared one — a valid family's series were silently destroyed.
func TestTypeRedeclarationDoesNotCorruptLaterFamilies(t *testing.T) {
	exposition := `# TYPE weird histogram
weird_bucket{le="+Inf"} 1
weird_sum 1
weird_count 1
# TYPE weird summary
weird_sum 1
weird_count 1
# TYPE a histogram
a_bucket{s="1",le="+Inf"} 5
a_sum{s="1"} 5
a_count{s="1"} 5
a_bucket{s="2",le="+Inf"} 7
a_sum{s="2"} 7
a_count{s="2"} 7
`
	bt := newBatcher(func(pcommon.Resource) {}, time.Unix(1, 0), time.Unix(2, 0))
	conv := newConverter(bt, nil)
	p := promparse.New(promparse.Options{MaxLineBytes: 1 << 20})
	if _, err := p.Parse(strings.NewReader(exposition), func(s Sample) error {
		_ = conv.add(s)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	_ = conv.finish()

	counts := map[string]uint64{}
	ms := bt.take().ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	for k := 0; k < ms.Len(); k++ {
		m := ms.At(k)
		if m.Name() != "a" || m.Type() != pmetric.MetricTypeHistogram {
			continue
		}
		dps := m.Histogram().DataPoints()
		for d := 0; d < dps.Len(); d++ {
			dp := dps.At(d)
			if v, ok := dp.Attributes().Get("s"); ok {
				counts[v.Str()] = dp.Count()
			}
		}
	}
	if counts["1"] != 5 || counts["2"] != 7 {
		t.Fatalf("family a corrupted: got counts %v, want s=1:5 s=2:7", counts)
	}
}

// The /debug/targets snapshot: every scrape of the last cycle appears with
// its outcome, failures sorted first, error text included.
func TestScrapeStatusSnapshot(t *testing.T) {
	good := serveBody(t, "m 1\n")
	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 2 * time.Second,
		Targets: staticTargets{
			testTarget(good.URL),
			testTarget("http://127.0.0.1:1"), // refused: a failing target
		},
		Exporter:  exp,
		StartTime: time.Now(),
	})
	if s.Status() != nil {
		t.Fatal("status before the first cycle must be nil")
	}
	s.cycle(context.Background())

	st := s.Status()
	if st == nil || len(st.Targets) != 2 {
		t.Fatalf("status = %+v, want 2 targets", st)
	}
	if st.Targets[0].Up || st.Targets[0].Error == "" {
		t.Fatalf("failures must sort first with an error: %+v", st.Targets[0])
	}
	if !st.Targets[1].Up || st.Targets[1].Samples != 1 {
		t.Fatalf("good target: %+v", st.Targets[1])
	}
	if st.Targets[1].Pod == "" || st.Targets[1].Pipeline != "targets" {
		t.Fatalf("target identity missing: %+v", st.Targets[1])
	}
}

// The kubelet due times must survive a cycle whose target list could not be
// fetched. They are not derived from that list — they ride in the same map for
// convenience — and discarding them left every kubelet scrape permanently past
// due while targetIntervals stayed frozen at whatever fine cadence a monitor
// had requested. A metadata-service rollout then re-clocked /metrics/cadvisor
// and /metrics to that cadence on every node in the cluster.
//
// All THREE kubelet scrapes are enabled here, /stats/summary included: it is
// the most expensive of them on a dense node, and it is a per-pipeline flag
// away from being the only one an agent runs.
func TestKubeletScheduleSurvivesTargetFetchFailure(t *testing.T) {
	kubelet := serveBody(t, "# TYPE up gauge\nup 1\n")
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: time.Second,
		Targets:   failingTargets{},
		Exporter:  &captureExporter{},
		StartTime: time.Now(),
		Kubelet: KubeletConfig{
			Endpoint: kubelet.URL, Cadvisor: true, NodeMetrics: true, Summary: true,
			Meta: &fakeMetaSource{},
		},
	})
	s.cycle(context.Background())
	for _, k := range kubeletDueKeys {
		if _, ok := s.dueAt(k); !ok {
			t.Errorf("kubelet due time %q discarded because the target fetch failed", k)
		}
	}
}

// The kubelet scrapes do not depend on the target list, so a slow or
// blackholed metadata service must not hold them back. The fetch is bounded
// only by the metadata client's own timeout (15s at the defaults), and spawning
// the kubelet scrapes after it delayed every kubelet pipeline by that much each
// cycle — /metrics included, which resolves nothing. The fetch here answers
// only once the kubelet has been asked, so the test waits on no clock unless
// the order is wrong.
func TestKubeletScrapesDoNotWaitForTheTargetFetch(t *testing.T) {
	asked := make(chan struct{})
	var once sync.Once
	kubelet := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		once.Do(func() { close(asked) })
		_, _ = w.Write([]byte("# TYPE up gauge\nup 1\n"))
	}))
	t.Cleanup(kubelet.Close)
	src := &gatedTargets{gate: asked, giveUp: 3 * time.Second}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 10 * time.Second,
		Targets: src, Exporter: &captureExporter{}, StartTime: time.Now(),
		Kubelet: KubeletConfig{Endpoint: kubelet.URL, NodeMetrics: true},
	})
	s.cycle(context.Background())
	if !src.opened.Load() {
		t.Fatal("the kubelet was not asked until the target fetch had given up: a hung metadata service delays a pipeline that never uses it")
	}
	if _, ok := s.dueAt(dueKeyNode); !ok {
		t.Error("the /metrics scrape was not scheduled")
	}
}

// gatedTargets answers once gate closes, or fails after giveUp, as a metadata
// service that is not answering does.
type gatedTargets struct {
	gate   <-chan struct{}
	giveUp time.Duration
	opened atomic.Bool
}

func (g *gatedTargets) NodeTargets(ctx context.Context, _ string) ([]kubemeta.ScrapeTarget, error) {
	select {
	case <-g.gate:
		g.opened.Store(true)
		return nil, nil
	case <-time.After(g.giveUp):
		return nil, errors.New("metadata service did not answer")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// failingTargets always fails, as a metadata service being rolled does.
type failingTargets struct{}

func (failingTargets) NodeTargets(context.Context, string) ([]kubemeta.ScrapeTarget, error) {
	return nil, errors.New("metadata service unavailable")
}

// flakyTargets serves its list until fail is set, then fails as a metadata
// service being rolled does.
type flakyTargets struct {
	list []kubemeta.ScrapeTarget
	fail atomic.Bool
}

func (f *flakyTargets) NodeTargets(context.Context, string) ([]kubemeta.ScrapeTarget, error) {
	if f.fail.Load() {
		return nil, errors.New("metadata service unavailable")
	}
	return f.list, nil
}

// expireSchedule makes every scheduled key due at the next cycle: the passage
// of the interval, without sleeping through it.
func expireSchedule(s *Scraper) {
	s.dueMu.Lock()
	defer s.dueMu.Unlock()
	for k := range s.due {
		s.due[k] = time.Time{}
	}
}

// countingTarget serves one sample and counts the scrapes it received.
func countingTarget(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("m 1\n"))
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

// The metadata service is a singleton, so a node drain or a rollout fails
// every agent's target fetch for as long as its replacement takes to become
// ready. Scraping NOTHING for that long was a hole in every discovered
// target's data while the targets themselves were healthy — and
// /debug/targets kept showing them up, from the previous snapshot.
func TestFailedTargetFetchScrapesTheLastKnownTargets(t *testing.T) {
	srv, hits := countingTarget(t)
	src := &flakyTargets{list: []kubemeta.ScrapeTarget{testTarget(srv.URL)}}
	var buf strings.Builder
	exp := &captureExporter{}
	s := debugScraper(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: src, Exporter: exp, StartTime: time.Now(),
	}, &buf)

	s.cycle(context.Background())
	if hits.Load() != 1 {
		t.Fatalf("hits = %d after the first cycle, want 1", hits.Load())
	}
	src.fail.Store(true)
	for range 3 {
		expireSchedule(s)
		s.cycle(context.Background())
	}
	if got := hits.Load(); got != 4 {
		t.Fatalf("hits = %d after three cycles whose fetch failed, want 4: the last known list must keep being scraped", got)
	}
	st := s.Status()
	if len(st.Targets) != 1 || !st.Targets[0].Up || st.Targets[0].Pending {
		t.Errorf("status = %+v, want the one target, scraped and up", st.Targets)
	}
	out := buf.String()
	if !strings.Contains(out, "scraping the last known target list") {
		t.Errorf("the reuse was not announced:\n%s", out)
	}

	src.fail.Store(false)
	s.cycle(context.Background())
	if !strings.Contains(buf.String(), "fetching scrape targets recovered") {
		t.Errorf("the recovery was not reported:\n%s", buf.String())
	}
}

// The reuse is BOUNDED: a list is a snapshot of pod IPs, pod CIDRs are per
// node, and an address freed by a pod deleted during the outage is likely to
// be recycled on this very node — scraped under the dead pod's identity for as
// long as the stale list is honoured.
func TestStaleTargetListExpires(t *testing.T) {
	srv, hits := countingTarget(t)
	src := &flakyTargets{list: []kubemeta.ScrapeTarget{testTarget(srv.URL)}}
	var buf strings.Builder
	s := debugScraper(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: src, Exporter: &captureExporter{}, StartTime: time.Now(),
	}, &buf)
	s.cycle(context.Background())
	src.fail.Store(true)
	expireSchedule(s)
	s.cycle(context.Background()) // inside the bound: reused
	if hits.Load() != 2 {
		t.Fatalf("hits = %d, want 2 (the reuse inside the bound)", hits.Load())
	}

	s.lastGoodAt = time.Now().Add(-maxStaleTargetList - time.Second)
	for range 2 {
		expireSchedule(s)
		s.cycle(context.Background())
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("hits = %d, want 2: a list older than %v must not be scraped", got, maxStaleTargetList)
	}
	if n := strings.Count(buf.String(), "too old to reuse"); n != 1 {
		t.Errorf("the expiry was reported %d times, want once (it is a transition):\n%s", n, buf.String())
	}
}

// A failing fetch is a persisting condition, noticed every cycle on every node
// at a tick that can be as short as 1s. It says so on the transition and then
// at most once per window — the tailer's and the export reporter's shape.
func TestTargetFetchFailureIsNarratedNotRepeated(t *testing.T) {
	var buf strings.Builder
	s := debugScraper(Config{
		Node: "node1", Interval: time.Hour, Exporter: &captureExporter{},
		Targets: failingTargets{},
	}, &buf)
	for range 5 {
		s.cycle(context.Background())
	}
	out := buf.String()
	if n := strings.Count(out, "fetching scrape targets"); n != 1 {
		t.Fatalf("logged %d lines for one ongoing failure, want 1 (the transition):\n%s", n, out)
	}
	if !strings.Contains(out, "no discovered target is scraped") {
		t.Errorf("with no last known list the line must say nothing is scraped:\n%s", out)
	}
	if s.fetchFail.failures != 5 {
		t.Errorf("failures = %d, want 5 (the repeat line carries the count)", s.fetchFail.failures)
	}
}

// salvage cannot send on an expired context, so a body that stalls past the
// scrape budget exports only the chunks already flushed — never the converted
// remainder. Pinned so the documentation cannot claim otherwise again.
func TestReadTimeoutMidBodyExportsOnlyFlushedChunks(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("a 1\nb 2\nc 3\n"))
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 300 * time.Millisecond, BatchPoints: 2,
		Targets: staticTargets{}, Exporter: exp, StartTime: time.Now(),
	})
	samples, err := s.scrapeTarget(context.Background(), testTarget(srv.URL), 300*time.Millisecond)
	if err == nil {
		t.Fatal("a body stalling past the budget reported success")
	}
	if got := failureReason(err); got != reasonTimeout {
		t.Errorf("reason = %q (%v), want %q", got, err, reasonTimeout)
	}
	if samples != 3 {
		t.Errorf("samples = %d, want 3 (all three lines arrived before the stall)", samples)
	}
	if got := exp.points(); got != 2 {
		t.Errorf("exported %d points, want 2: exactly the chunk flushed before the stall", got)
	}
}
