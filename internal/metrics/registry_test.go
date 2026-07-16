package metrics

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestRegistryExport(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("test_reg_total", "a counter")
	cv := r.CounterVec("test_reg_vec_total", "a labeled counter", "outcome")
	g := r.Gauge("test_reg_gauge", "a gauge")
	h := r.HistogramVec("test_reg_seconds", "a histogram", nil, "pipeline")
	r.GaugeFunc("test_reg_func", "an export-time gauge", func() float64 { return 7 })

	c.Inc()
	c.Add(2)
	cv.WithLabelValues("ok").Inc()
	cv.WithLabelValues("ok").Inc()
	cv.WithLabelValues("error").Inc()
	g.Set(5)
	g.Set(9) // set semantics: last value wins
	h.WithLabelValues("targets").Observe(0.03)
	h.WithLabelValues("targets").Observe(2)

	res := pcommon.NewResource()
	res.Attributes().PutStr("service.name", "test-agent")

	exp := &capExporter{}
	if err := r.Export(context.Background(), exp, res); err != nil {
		t.Fatal(err)
	}
	if len(exp.md) != 1 {
		t.Fatalf("payloads = %d", len(exp.md))
	}
	rm := exp.md[0].ResourceMetrics().At(0)
	if v, _ := rm.Resource().Attributes().Get("service.name"); v.Str() != "test-agent" {
		t.Fatalf("resource = %v", rm.Resource().Attributes().AsRaw())
	}

	// Counter: cumulative sum 3 (plus zero-baseline points on first export).
	m, ok := exp.find("test_reg_total")
	if !ok || m.Type() != pmetric.MetricTypeSum || !m.Sum().IsMonotonic() {
		t.Fatalf("counter metric shape wrong: %v", ok)
	}
	var total float64
	for i := 0; i < m.Sum().DataPoints().Len(); i++ {
		total += m.Sum().DataPoints().At(i).DoubleValue()
	}
	if total != 3 {
		t.Fatalf("counter total = %v", total)
	}
	if c.Value() != 3 {
		t.Fatalf("counter Value() = %v", c.Value())
	}

	// CounterVec: per-label values.
	if got := cv.WithLabelValues("ok").Value(); got != 2 {
		t.Fatalf("vec ok = %v", got)
	}
	if got := cv.WithLabelValues("error").Value(); got != 1 {
		t.Fatalf("vec error = %v", got)
	}

	// Gauge: last set wins.
	m, ok = exp.find("test_reg_gauge")
	if !ok || m.Type() != pmetric.MetricTypeGauge {
		t.Fatal("gauge missing")
	}
	if v := m.Gauge().DataPoints().At(0).DoubleValue(); v != 9 {
		t.Fatalf("gauge = %v", v)
	}

	// GaugeFunc evaluated at export.
	m, ok = exp.find("test_reg_func")
	if !ok || m.Gauge().DataPoints().At(0).DoubleValue() != 7 {
		t.Fatal("gauge func missing/wrong")
	}

	// Histogram: count/sum and label.
	m, ok = exp.find("test_reg_seconds")
	if !ok || m.Type() != pmetric.MetricTypeHistogram {
		t.Fatal("histogram missing")
	}
	dp := m.Histogram().DataPoints().At(0)
	if dp.Count() != 2 || dp.Sum() != 2.03 {
		t.Fatalf("histogram count/sum = %d/%v", dp.Count(), dp.Sum())
	}
	if v, _ := dp.Attributes().Get("pipeline"); v.Str() != "targets" {
		t.Fatalf("histogram labels = %v", dp.Attributes().AsRaw())
	}

	// A second export still carries the cumulative counter (no idle reset).
	exp2 := &capExporter{}
	if err := r.Export(context.Background(), exp2, res); err != nil {
		t.Fatal(err)
	}
	m, ok = exp2.find("test_reg_total")
	if !ok {
		t.Fatal("counter missing on re-export")
	}
	total = 0
	for i := 0; i < m.Sum().DataPoints().Len(); i++ {
		total += m.Sum().DataPoints().At(i).DoubleValue()
	}
	if total != 3 {
		t.Fatalf("re-export counter total = %v (must not reset)", total)
	}
}

// Run exports periodically and once more on shutdown; GaugeVec labels land on
// the data points.
func TestRegistryRun(t *testing.T) {
	r := NewRegistry()
	gv := r.GaugeVec("test_run_gauge", "labeled gauge", "shard")
	gv.WithLabelValues("a").Set(1)
	gv.WithLabelValues("b").Set(2)

	res := pcommon.NewResource()
	res.Attributes().PutStr("service.name", "run-test")
	exp := &lockedCapExporter{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx, exp, 20*time.Millisecond, res, nil); close(done) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if exp.count() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	m, ok := exp.snapshot().find("test_run_gauge")
	if !ok {
		t.Fatal("gauge never exported")
	}
	vals := map[string]float64{}
	dps := m.Gauge().DataPoints()
	for i := 0; i < dps.Len(); i++ {
		if v, ok := dps.At(i).Attributes().Get("shard"); ok {
			vals[v.Str()] = dps.At(i).DoubleValue()
		}
	}
	if vals["a"] != 1 || vals["b"] != 2 {
		t.Fatalf("gauge vec values = %v", vals)
	}
}

// lockedCapExporter is a capExporter safe for polling from another goroutine
// (Registry.Run exports concurrently with the test's checks).
type lockedCapExporter struct {
	mu    sync.Mutex
	inner capExporter
}

func (c *lockedCapExporter) ExportMetrics(ctx context.Context, md pmetric.Metrics) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inner.ExportMetrics(ctx, md)
}

func (c *lockedCapExporter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.inner.md)
}

func (c *lockedCapExporter) snapshot() *capExporter {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := &capExporter{md: append([]pmetric.Metrics(nil), c.inner.md...)}
	return cp
}
