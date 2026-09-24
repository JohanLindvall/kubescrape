package metrics

import (
	"context"
	"errors"
	"maps"
	"strings"
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
	for range goroutines {
		wg.Go(func() {
			for range n {
				cv.WithLabelValues("same").Inc()
				_ = cv.WithLabelValues("same").Value()
			}
		})
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
	for i := range 4 {
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
	for range 2 {
		wg.Go(func() {
			for range 50 {
				if err := r.Export(context.Background(), &capExporter{}, resAttrs); err != nil {
					t.Error(err)
				}
			}
		})
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

// appendVecKey's netstring encoding must keep aliasing tuples distinct: with a plain
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

// A tuple whose key outgrows the stack buffer (vecKeyBuf) takes append's heap
// growth instead. It must still resolve to ONE cached wrapper per tuple — the
// miss arm materialises the grown key, and the next hit must find it under the
// same bytes.
func TestLongMultiLabelTupleResolvesToOneSeries(t *testing.T) {
	r := NewRegistry()
	v := r.CounterVec("test_veckey_long_total", "t", "a", "b")
	long := strings.Repeat("x", 3*vecKeyBuf)
	first := v.WithLabelValues(long, "y")
	first.Add(1)
	if again := v.WithLabelValues(long, "y"); again != first {
		t.Fatal("a long tuple resolved to a second wrapper: the grown key was cached under different bytes")
	}
	v.WithLabelValues(long, "y").Add(2)
	if got := first.Value(); got != 3 {
		t.Fatalf("long tuple value = %v, want 3", got)
	}
	if got := v.WithLabelValues(long, "z").Value(); got != 0 {
		t.Fatalf("a different long tuple aliased the first: value = %v, want 0", got)
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
// the data points. The shutdown export is asserted, not assumed: a bump made
// after the periodic exports must be in the LAST payload, and that payload
// must have gone out under the final export's own deadline (the periodic ones
// run on the caller's deadline-less ctx), so it cannot be a stray tick.
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
	if exp.count() == 0 {
		t.Fatal("Run never exported on its interval")
	}
	cv.WithLabelValues("a").Add(10) // after the periodic exports, before shutdown
	cancel()
	<-done

	if !exp.lastHadDeadline() {
		t.Fatal("the last payload was not the shutdown export (it carried no deadline of its own)")
	}
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
	if vals["a"] != 11 || vals["b"] != 2 {
		t.Fatalf("counter vec values in the shutdown export = %v, want a=11 (the bump before cancel) b=2", vals)
	}
}

// failNExporter fails its first `fail` calls, then succeeds. Safe to poll
// from the test goroutine while Run exports.
type failNExporter struct {
	mu    sync.Mutex
	fail  int
	calls int
}

func (f *failNExporter) ExportMetrics(context.Context, pmetric.Metrics) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.fail {
		return errors.New("collector unavailable")
	}
	return nil
}

func (f *failNExporter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// A failed self-metrics export cannot be COUNTED — the counter that would
// record it rides in the payload that is not arriving — so Run's narration is
// the only signal there is: one WARN when the outage starts (claiming the
// throttle slot, so the next failures inside the window are Debug rather than
// a second WARN), and one Info when it ends that says how many attempts it
// spanned.
func TestRegistryRunNarratesFailureAndRecovery(t *testing.T) {
	r := NewRegistry()
	r.Counter("test_run_narrate_total", "c").Inc()
	exp := &failNExporter{fail: 3}
	log, buf := capture()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx, exp, 5*time.Millisecond, pcommon.NewResource(), log); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for exp.count() < 5 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	if exp.count() < 5 {
		t.Fatalf("Run exported %d times in 5s, want at least 5", exp.count())
	}

	out := buf.String()
	if n := strings.Count(out, "level=WARN"); n != 1 {
		t.Errorf("WARN lines = %d, want exactly 1 (the outage's first failure; repeats inside the window are Debug):\n%s", n, out)
	}
	if !strings.Contains(out, "exporting self-metrics failed; this process's own telemetry is not reaching the collector") {
		t.Errorf("the WARN must say what is failing:\n%s", out)
	}
	if n := strings.Count(out, `level=DEBUG msg="exporting self-metrics failed"`); n != 2 {
		t.Errorf("Debug repeats = %d, want 2 (failures 2 and 3):\n%s", n, out)
	}
	var recovered []string
	for ln := range strings.SplitSeq(out, "\n") {
		if strings.Contains(ln, "succeeded again") {
			recovered = append(recovered, ln)
		}
	}
	if len(recovered) != 1 || !strings.Contains(recovered[0], "level=INFO") || !strings.Contains(recovered[0], "failures=3") {
		t.Errorf("want exactly one INFO recovery line carrying failures=3, got %q\n%s", recovered, out)
	}
}

// lockedCapExporter is a capExporter safe for polling from another goroutine
// (Registry.Run exports concurrently with the test's checks).
type lockedCapExporter struct {
	mu       sync.Mutex
	inner    capExporter
	deadline bool // the last export's ctx carried a deadline
}

func (c *lockedCapExporter) ExportMetrics(ctx context.Context, md pmetric.Metrics) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, c.deadline = ctx.Deadline()
	return c.inner.ExportMetrics(ctx, md)
}

// lastHadDeadline reports whether the most recent export's context carried a
// deadline — Run's shutdown export does, its periodic ones do not.
func (c *lockedCapExporter) lastHadDeadline() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deadline
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

// CounterFuncVec is GaugeFuncVec's shape with a counter's semantics: a running
// total per label value, rendered as a cumulative monotonic Sum with per-label
// delta bookkeeping.
//
// It exists because kubescrape_log_metrics_dropped_capped_by_metric was a
// GAUGE carrying a monotonic since-start total (and spelled its label name
// inside the metric name). A gauge does not mark the reset at a restart, so
// rate()/increase() over it silently swallowed one — the counter it should
// always have been could not be registered because this constructor was the
// one shape the Registry lacked.
func TestRegistryCounterFuncVec(t *testing.T) {
	r := NewRegistry()
	totals := map[string]float64{}
	r.CounterFuncVec("test_capped_total", "per-metric drops", "metric",
		func() map[string]float64 { return totals })

	// Data-driven label set: nothing to report yet, nothing exported.
	exp0 := &capExporter{}
	if err := r.Export(context.Background(), exp0, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	if _, ok := exp0.find("test_capped_total"); ok {
		t.Fatal("exported a point before any label value had a value")
	}

	// Sum over the label, per label value: the LAST point of each stream is
	// the live cumulative value (a fresh counter backfills two zeros first).
	live := func(e *capExporter, label string) (float64, bool) {
		m, ok := e.find("test_capped_total")
		if !ok {
			return 0, false
		}
		if m.Type() != pmetric.MetricTypeSum || !m.Sum().IsMonotonic() {
			t.Fatalf("rendered as %v (monotonic=%v), want monotonic Sum", m.Type(), m.Sum().IsMonotonic())
		}
		var v float64
		found := false
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			lv, ok := dp.Attributes().Get("metric")
			if !ok || lv.Str() != label {
				continue
			}
			v, found = dp.DoubleValue(), true
		}
		return v, found
	}

	totals["requests"] = 4
	exp1 := &capExporter{}
	if err := r.Export(context.Background(), exp1, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	if v, ok := live(exp1, "requests"); !ok || v != 4 {
		t.Fatalf("requests = %v (found %v), want 4", v, ok)
	}

	// The fn returns a CUMULATIVE total per label; an unchanged one must not
	// re-add into the accumulating series.
	exp2 := &capExporter{}
	if err := r.Export(context.Background(), exp2, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	if v, ok := live(exp2, "requests"); !ok || v != 4 {
		t.Fatalf("unchanged total re-added: %v, want 4", v)
	}

	// A second label value keeps its OWN delta state.
	totals["requests"] = 6
	totals["latency"] = 10
	exp3 := &capExporter{}
	if err := r.Export(context.Background(), exp3, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	if v, _ := live(exp3, "requests"); v != 6 {
		t.Fatalf("requests = %v, want 6", v)
	}
	if v, _ := live(exp3, "latency"); v != 10 {
		t.Fatalf("latency = %v, want 10 — per-label delta state leaked between label values", v)
	}

	// A value fn stops reporting stays on the push at its last total, and its
	// delta state survives: when it reappears unchanged it continues rather
	// than re-adding its whole total (the CounterFuncVec doc's claim).
	delete(totals, "latency")
	exp4 := &capExporter{}
	if err := r.Export(context.Background(), exp4, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	if v, ok := live(exp4, "latency"); !ok || v != 10 {
		t.Fatalf("vanished latency on the push = %v (found %v), want frozen at 10", v, ok)
	}
	totals["latency"] = 10
	exp5 := &capExporter{}
	if err := r.Export(context.Background(), exp5, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	if v, _ := live(exp5, "latency"); v != 10 {
		t.Fatalf("reappearing latency = %v, want 10 — the total was re-added", v)
	}
}

// GaugeFuncVec's contract is a key set that never shrinks, because the two
// delivery modalities disagree about a label value fn stops reporting: the
// push keeps exporting its last reading (Registry series never expire and
// nothing unlinks the absent value), while Dump, built from fn alone, drops it.
// This pins that answer so a data-driven caller is a deliberate choice rather
// than a discovery — and so the doc on GaugeFuncVec cannot drift from it.
func TestGaugeFuncVecVanishedLabelValueFreezesOnThePush(t *testing.T) {
	r := NewRegistry()
	vals := map[string]float64{"a": 1, "b": 2}
	r.GaugeFuncVec("test_backlog", "d", "signal", func() map[string]float64 {
		out := make(map[string]float64, len(vals))
		maps.Copy(out, vals)
		return out
	})
	pushed := func() map[string]float64 {
		t.Helper()
		exp := &capExporter{}
		if err := r.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
			t.Fatal(err)
		}
		m, ok := exp.find("test_backlog")
		if !ok {
			t.Fatal("test_backlog not exported")
		}
		got := map[string]float64{}
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			lv, _ := dps.At(i).Attributes().Get("signal")
			got[lv.Str()] = dps.At(i).DoubleValue()
		}
		return got
	}
	dumped := func() map[string]float64 {
		got := map[string]float64{}
		for _, s := range r.Dump() {
			if s.Name != "test_backlog" {
				continue
			}
			for _, p := range s.Points {
				got[p.Labels[0][1]] = p.Value
			}
		}
		return got
	}

	if got := pushed(); len(got) != 2 || got["a"] != 1 || got["b"] != 2 {
		t.Fatalf("first push = %v, want a=1 b=2", got)
	}

	delete(vals, "b")
	vals["a"] = 5
	if got := pushed(); len(got) != 2 || got["a"] != 5 || got["b"] != 2 {
		t.Fatalf("push after b vanished = %v, want a=5 and b FROZEN at 2", got)
	}
	if got := dumped(); len(got) != 1 || got["a"] != 5 {
		t.Fatalf("dump after b vanished = %v, want only a=5", got)
	}
}

// Two registrations of one NAME must render ONE metric. The per-instance
// Register* hooks are documented as supporting several instances (two
// DynamicMetricSets in one process each publish the log-metrics drop family),
// and a second series under the same name is not merely untidy: the Prometheus
// arm renders both as const metrics with an identical name and label set,
// Gather's duplicate check fails, and promhttp answers 500 to EVERY scrape —
// the whole self-metrics scrape lost, in the one mode that carries them.
func TestRegistryDeduplicatesMetricNames(t *testing.T) {
	r := NewRegistry()
	var a, b float64
	r.CounterFunc("dup_total", "d", func() float64 { return a })
	r.CounterFunc("dup_total", "d", func() float64 { return b })

	countNamed := func(exp *capExporter, name string) int {
		n := 0
		for _, md := range exp.md {
			rms := md.ResourceMetrics()
			for i := 0; i < rms.Len(); i++ {
				sms := rms.At(i).ScopeMetrics()
				for j := 0; j < sms.Len(); j++ {
					ms := sms.At(j).Metrics()
					for k := 0; k < ms.Len(); k++ {
						if ms.At(k).Name() == name {
							n++
						}
					}
				}
			}
		}
		return n
	}

	a, b = 3, 4
	exp := &capExporter{}
	if err := r.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	if got := countNamed(exp, "dup_total"); got != 1 {
		t.Fatalf("dup_total rendered %d times in one payload, want 1", got)
	}
	// Reuse AGGREGATES: the series accumulates what is observed into it, and
	// each func keeps its own delta bookkeeping.
	m, ok := exp.find("dup_total")
	if !ok {
		t.Fatal("dup_total not exported")
	}
	// The LAST point of the stream is the live cumulative value (a fresh
	// counter backfills zeros ahead of it).
	dps := m.Sum().DataPoints()
	if v := dps.At(dps.Len() - 1).DoubleValue(); v != 7 {
		t.Fatalf("dup_total = %v, want 7 (3 + 4)", v)
	}

	// Two handles on one gauge name are two writers of one series, not two
	// series — and a labeled counter's second registration shares it too.
	r.Gauge("dup_gauge", "d").Set(1)
	r.Gauge("dup_gauge", "d").Set(2)
	exp2 := &capExporter{}
	if err := r.Export(context.Background(), exp2, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	if got := countNamed(exp2, "dup_gauge"); got != 1 {
		t.Fatalf("dup_gauge rendered %d times, want 1", got)
	}
}

// A name re-registered with a DIFFERENT shape cannot be served: the two cannot
// both render, and handing one call site the other's series is exactly the
// silent mismatch the dedupe exists to prevent. Registrations are code-driven
// and run at startup, so this is a bug to crash on.
func TestRegistryRefusesConflictingRedeclaration(t *testing.T) {
	r := NewRegistry()
	r.Counter("conflict_total", "d")
	defer func() {
		if recover() == nil {
			t.Fatal("re-registering conflict_total as a gauge was accepted")
		}
	}()
	r.Gauge("conflict_total", "d")
}

// A name registered BOTH directly and as a func is a shape conflict even when
// kind and action agree: the two would share one series, and Dump reports a
// func-backed series from its live fns alone, SKIPPING the db the direct
// handle observes into — so the direct increments would ship on the OTLP push
// (Export folds both) and silently vanish from the Prometheus scrape, the one
// delivery-modality knob (-self-metrics-interval) silently changing the
// values. Same-shape repeats stay legal in both forms: func+func and a direct
// gauge pair are pinned by TestRegistryDeduplicatesMetricNames (the advertised
// per-instance hook pattern), a direct counter pair by the control below.
func TestRegistryRefusesDirectFuncMix(t *testing.T) {
	mustPanic := func(what, name string, f func()) {
		t.Helper()
		defer func() {
			p := recover()
			if p == nil {
				t.Fatalf("%s was accepted", what)
			}
			if msg, ok := p.(string); !ok || !strings.Contains(msg, name) {
				t.Fatalf("%s: panic %q does not name the metric", what, p)
			}
		}()
		f()
	}

	r := NewRegistry()
	r.Counter("mix_total", "d").Inc()
	mustPanic("re-registering direct mix_total as a CounterFunc", "mix_total", func() {
		r.CounterFunc("mix_total", "d", func() float64 { return 0 })
	})

	r = NewRegistry()
	r.GaugeFunc("mix_gauge", "d", func() float64 { return 0 })
	mustPanic("re-registering func-backed mix_gauge as a direct gauge", "mix_gauge", func() {
		r.Gauge("mix_gauge", "d")
	})

	// The Vec forms register through the same add path and must refuse the
	// same mix.
	r = NewRegistry()
	r.CounterVec("mix_vec_total", "d", "l").WithLabelValues("v").Inc()
	mustPanic("re-registering direct mix_vec_total as a CounterFuncVec", "mix_vec_total", func() {
		r.CounterFuncVec("mix_vec_total", "d", "l", func() map[string]float64 { return nil })
	})

	r = NewRegistry()
	r.GaugeFuncVec("mix_vec_gauge", "d", "l", func() map[string]float64 { return nil })
	mustPanic("re-registering func-backed mix_vec_gauge as a direct gauge", "mix_vec_gauge", func() {
		r.Gauge("mix_vec_gauge", "d")
	})

	// Control: a same-shape DIRECT repeat still shares the series without
	// panicking.
	r = NewRegistry()
	r.Counter("repeat_total", "d").Inc()
	c := r.Counter("repeat_total", "d")
	c.Inc()
	if v := c.Value(); v != 2 {
		t.Fatalf("repeat_total = %v, want 2 (two handles on one series)", v)
	}
}
