package metrics

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// TestVecConcurrentFirstUse hammers the wrapper cache's first use of ONE tuple
// from many goroutines plus concurrent Value() reads (run under -race).
func TestVecConcurrentFirstUse(t *testing.T) {
	r := NewRegistry()
	cv := r.CounterVec("audit_conc_total", "d", "k")

	const goroutines, n = 8, 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				cv.WithLabelValues("same").Inc()
				_ = cv.WithLabelValues("same").Value()
			}
		}()
	}
	wg.Wait()
	if got := cv.WithLabelValues("same").Value(); got != goroutines*n {
		t.Fatalf("Value = %v, want %d", got, goroutines*n)
	}
}

// TestRegistryConcurrentExportAndObserve runs two exporters against the same
// registry while counters/gauges/histograms are hammered (run under -race):
// GaugeFunc evaluation happens inside Export, concurrently with observes.
func TestRegistryConcurrentExportAndObserve(t *testing.T) {
	r := NewRegistry()
	c := r.CounterVec("audit_race_total", "d", "k")
	g := r.Gauge("audit_race_gauge", "d")
	h := r.HistogramVec("audit_race_hist", "d", []float64{1, 5}, "k")
	var n atomic.Int64
	r.GaugeFunc("audit_race_func", "d", func() float64 { return float64(n.Load()) })
	// A CounterFunc too: its delta state (gaugeFunc.last) is a read-modify-
	// write that concurrent Exports race on without the per-func mutex.
	r.CounterFunc("audit_race_cfunc_total", "d", func() float64 { return float64(n.Load()) })

	resAttrs := pcommon.NewResource()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				c.WithLabelValues("a").Inc()
				g.Set(float64(i))
				h.WithLabelValues("a").Observe(2)
				n.Add(1)
			}
		}(i)
	}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if err := r.Export(context.Background(), &capExporter{}, resAttrs); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
	if c.WithLabelValues("a").Value() == 0 {
		t.Fatal("no counter value")
	}
}

// TestGaugeFuncReentrant: a GaugeFunc that itself drives registry metrics (and
// registers nothing new) must not deadlock Export; its value is exported.
func TestGaugeFuncReentrant(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("audit_reentrant_total", "d")
	r.GaugeFunc("audit_reentrant_gauge", "d", func() float64 {
		c.Inc() // re-enters the series lock of ANOTHER series during Export
		return 42
	})

	resAttrs := pcommon.NewResource()
	exp := &capExporter{}
	done := make(chan error, 1)
	go func() { done <- r.Export(context.Background(), exp, resAttrs) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Export deadlocked with a re-entrant GaugeFunc")
	}
	m, ok := exp.find("audit_reentrant_gauge")
	if !ok || m.Gauge().DataPoints().At(0).DoubleValue() != 42 {
		t.Fatal("re-entrant gauge func value missing/wrong")
	}
	if c.Value() != 1 {
		t.Fatalf("counter bumped from GaugeFunc = %v, want 1", c.Value())
	}
}

// vecKey's netstring encoding must keep aliasing tuples distinct: with a plain
// separator, ("x\x00y","z") and ("x","y\x00z") would collide. Only the
// single-label fast path may return the raw value.
func TestVecKeyMultiLabelCollisionProof(t *testing.T) {
	r := NewRegistry()
	v := r.CounterVec("test_veckey_total", "t", "a", "b")
	v.WithLabelValues("x\x00y", "z").Add(1)
	v.WithLabelValues("x", "y\x00z").Add(2)
	v.WithLabelValues("1:x", "").Add(4)
	v.WithLabelValues("", "1:x").Add(8)

	// Four distinct tuples → four independent counters.
	for _, tc := range []struct {
		vals []string
		want float64
	}{
		{[]string{"x\x00y", "z"}, 1},
		{[]string{"x", "y\x00z"}, 2},
		{[]string{"1:x", ""}, 4},
		{[]string{"", "1:x"}, 8},
	} {
		if got := v.WithLabelValues(tc.vals...).Value(); got != tc.want {
			t.Fatalf("tuple %q = %v, want %v (tuples aliased)", tc.vals, got, tc.want)
		}
	}
}

// Prometheus semantics: an empty label value is equivalent to the label being
// absent (labels.set drops empty values), so a short call and a padded call
// deliberately share one series — while values that merely CONTAIN the
// netstring syntax stay distinct.
func TestVecKeyEmptyValueEquivalence(t *testing.T) {
	r := NewRegistry()
	v := r.CounterVec("test_veckey_short_total", "t", "a", "b")
	v.WithLabelValues("1:x").Add(1)       // {a="1:x"} — 1 value, 2-label vec
	v.WithLabelValues("1:x", "").Add(100) // {a="1:x", b=""} ≡ {a="1:x"}
	v.WithLabelValues("x", "").Add(10)    // {a="x"} — distinct

	if got := v.WithLabelValues("1:x").Value(); got != 101 {
		t.Fatalf("{a=1:x} = %v, want 101 (empty-b call must merge, not fork)", got)
	}
	if got := v.WithLabelValues("x", "").Value(); got != 10 {
		t.Fatalf("{a=x} = %v, want 10 (must stay distinct from {a=1:x})", got)
	}
}

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

// Run exports periodically and once more on shutdown; vec labels land on
// the data points.
func TestRegistryRun(t *testing.T) {
	r := NewRegistry()
	cv := r.CounterVec("test_run_counter", "labeled counter", "shard")
	cv.WithLabelValues("a").Add(1)
	cv.WithLabelValues("b").Add(2)

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

	m, ok := exp.snapshot().find("test_run_counter")
	if !ok {
		t.Fatal("counter never exported")
	}
	vals := map[string]float64{}
	dps := m.Sum().DataPoints()
	for i := 0; i < dps.Len(); i++ {
		if v, ok := dps.At(i).Attributes().Get("shard"); ok {
			vals[v.Str()] = dps.At(i).DoubleValue()
		}
	}
	if vals["a"] != 1 || vals["b"] != 2 {
		t.Fatalf("counter vec values = %v", vals)
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

// CounterFunc values are pulled at export time and render as a cumulative
// monotonic sum (counter semantics for counts owned by foreign atomics).
func TestRegistryCounterFunc(t *testing.T) {
	r := NewRegistry()
	n := 0.0
	r.CounterFunc("test_func_total", "pulled counter", func() float64 { return n })
	n = 7

	exp := &capExporter{}
	if err := r.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	m, ok := exp.find("test_func_total")
	if !ok {
		t.Fatal("counter-func never exported")
	}
	if m.Type() != pmetric.MetricTypeSum || !m.Sum().IsMonotonic() {
		t.Fatalf("counter-func rendered as %v (monotonic=%v), want monotonic Sum", m.Type(), m.Sum().IsMonotonic())
	}
	// A newly admitted counter zero-backfills two earlier points (counter
	// birth for Prometheus); the LIVE value is the last data point.
	last := func(e *capExporter) float64 {
		m, ok := e.find("test_func_total")
		if !ok {
			t.Fatal("counter-func not exported")
		}
		dps := m.Sum().DataPoints()
		return dps.At(dps.Len() - 1).DoubleValue()
	}
	if got := last(exp); got != 7 {
		t.Fatalf("value = %v, want 7", got)
	}

	// fn returns a CUMULATIVE total: a second export with an unchanged total
	// must still report 7, not 14 — pushing the total into an accumulating
	// counter series every export inflated a one-time burst into a permanent
	// per-interval rate.
	exp2 := &capExporter{}
	if err := r.Export(context.Background(), exp2, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	if got := last(exp2); got != 7 {
		t.Fatalf("second export = %v, want 7 (cumulative fn must not re-add)", got)
	}

	// Growth appears as growth; a foreign counter reset re-counts from zero.
	n = 9
	exp3 := &capExporter{}
	if err := r.Export(context.Background(), exp3, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	if got := last(exp3); got != 9 {
		t.Fatalf("third export = %v, want 9", got)
	}
	n = 3 // reset (atomic zeroed): the new total counts as fresh growth
	exp4 := &capExporter{}
	if err := r.Export(context.Background(), exp4, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	if got := last(exp4); got != 12 {
		t.Fatalf("post-reset export = %v, want 12 (9 + fresh 3)", got)
	}
}
