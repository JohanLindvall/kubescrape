package spanmetrics

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/cumagg"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// capExporter captures exported metric payloads (mutexed: Run tests read it
// from the test goroutine while the Run goroutine exports).
type capExporter struct {
	mu sync.Mutex
	md []pmetric.Metrics
}

func (c *capExporter) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	cp := pmetric.NewMetrics()
	md.CopyTo(cp)
	c.mu.Lock()
	c.md = append(c.md, cp)
	c.mu.Unlock()
	return nil
}

func (c *capExporter) exports() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.md)
}

// find returns the metric with the given name from any export.
func (c *capExporter) find(name string) (pmetric.Metric, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, md := range c.md {
		if m, ok := findIn(md, name); ok {
			return m, true
		}
	}
	return pmetric.Metric{}, false
}

// traces builds a single-resource trace batch. Each span is (name, kind,
// status, durationSeconds, extra attrs).
func traces(service string, spans ...spanSpec) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	if service != "" {
		rs.Resource().Attributes().PutStr("service.name", service)
	}
	ss := rs.ScopeSpans().AppendEmpty()
	base := pcommon.Timestamp(1_700_000_000 * 1e9)
	for _, sp := range spans {
		s := ss.Spans().AppendEmpty()
		s.SetName(sp.name)
		s.SetKind(sp.kind)
		s.Status().SetCode(sp.status)
		s.SetStartTimestamp(base)
		s.SetEndTimestamp(base + pcommon.Timestamp(sp.dur*float64(time.Second)))
		if sp.traceID != (pcommon.TraceID{}) {
			s.SetTraceID(sp.traceID)
		}
		if sp.spanID != (pcommon.SpanID{}) {
			s.SetSpanID(sp.spanID)
		}
		if sp.parentID != (pcommon.SpanID{}) {
			s.SetParentSpanID(sp.parentID)
		}
		for k, v := range sp.attrs {
			s.Attributes().PutStr(k, v)
		}
	}
	return td
}

type spanSpec struct {
	name     string
	kind     ptrace.SpanKind
	status   ptrace.StatusCode
	dur      float64
	attrs    map[string]string
	traceID  pcommon.TraceID
	spanID   pcommon.SpanID
	parentID pcommon.SpanID
}

func dp(m pmetric.Metric) pmetric.NumberDataPointSlice { return m.Sum().DataPoints() }

func attr(a pcommon.Map, k string) string {
	if v, ok := a.Get(k); ok {
		return v.AsString()
	}
	return ""
}

func TestGeneratorCallsAndDuration(t *testing.T) {
	g := New(Config{})
	g.Consume(traces("checkout",
		spanSpec{name: "GET /", kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk, dur: 0.005},
		spanSpec{name: "GET /", kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk, dur: 0.007},
		spanSpec{name: "GET /", kind: ptrace.SpanKindServer, status: ptrace.StatusCodeError, dur: 0.02},
	))

	exp := &capExporter{}
	if err := g.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}

	calls, ok := exp.find("traces.span.metrics.calls")
	if !ok {
		t.Fatal("calls metric not exported")
	}
	if calls.Type() != pmetric.MetricTypeSum || !calls.Sum().IsMonotonic() {
		t.Fatalf("calls is not a monotonic sum: %v", calls.Type())
	}
	// Two series: (Ok) with 2 calls, (Error) with 1.
	byStatus := map[string]float64{}
	dps := dp(calls)
	for i := 0; i < dps.Len(); i++ {
		d := dps.At(i)
		if attr(d.Attributes(), "service.name") != "checkout" || attr(d.Attributes(), "span.name") != "GET /" ||
			attr(d.Attributes(), "span.kind") != "Server" {
			t.Fatalf("unexpected dimensions: %v", d.Attributes().AsRaw())
		}
		byStatus[attr(d.Attributes(), "status.code")] += numberVal(d)
	}
	if byStatus["Ok"] != 2 || byStatus["Error"] != 1 {
		t.Fatalf("calls by status = %v, want Ok:2 Error:1", byStatus)
	}

	dur, ok := exp.find("traces.span.metrics.duration")
	if !ok {
		t.Fatal("duration metric not exported")
	}
	if dur.Type() != pmetric.MetricTypeHistogram {
		t.Fatalf("duration is not a histogram: %v", dur.Type())
	}
	var total uint64
	var sum float64
	hps := dur.Histogram().DataPoints()
	for i := 0; i < hps.Len(); i++ {
		total += hps.At(i).Count()
		sum += hps.At(i).Sum()
	}
	if total != 3 {
		t.Fatalf("duration total count = %d, want 3", total)
	}
	if sum < 0.031 || sum > 0.033 { // 0.005+0.007+0.02
		t.Fatalf("duration sum = %v, want ~0.032", sum)
	}
}

func numberVal(d pmetric.NumberDataPoint) float64 {
	if d.ValueType() == pmetric.NumberDataPointValueTypeInt {
		return float64(d.IntValue())
	}
	return d.DoubleValue()
}

func TestExtraDimensions(t *testing.T) {
	g := New(Config{Dimensions: []string{"http.request.method"}})
	g.Consume(traces("api",
		spanSpec{name: "handle", kind: ptrace.SpanKindServer, status: ptrace.StatusCodeUnset, dur: 0.01,
			attrs: map[string]string{"http.request.method": "POST"}},
	))
	exp := &capExporter{}
	if err := g.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	calls, _ := exp.find("traces.span.metrics.calls")
	d := dp(calls).At(0)
	if got := attr(d.Attributes(), "http.request.method"); got != "POST" {
		t.Fatalf("extra dimension http.request.method = %q, want POST", got)
	}
}

func TestCardinalityCap(t *testing.T) {
	g := New(Config{MaxCardinality: 2})
	before := obs.SpanMetricsDropped.Value()
	// 3 distinct span names → the 3rd tuple exceeds the cap and is dropped.
	for _, n := range []string{"a", "b", "c", "c"} {
		g.Consume(traces("svc", spanSpec{name: n, kind: ptrace.SpanKindInternal, status: ptrace.StatusCodeUnset, dur: 0.001}))
	}
	if got := obs.SpanMetricsDropped.Value() - before; got != 2 { // "c" dropped twice
		t.Fatalf("dropped delta = %v, want 2", got)
	}
	if g.store.Len() != 2 {
		t.Fatalf("admitted tuples = %d, want 2 (cap held)", g.store.Len())
	}
	exp := &capExporter{}
	_ = g.Export(context.Background(), exp, pcommon.NewResource())
	calls, _ := exp.find("traces.span.metrics.calls")
	names := map[string]bool{}
	dps := dp(calls)
	for i := 0; i < dps.Len(); i++ {
		names[attr(dps.At(i).Attributes(), "span.name")] = true
	}
	if dps.Len() != 2 || len(names) != 2 || names["c"] {
		t.Fatalf("exported span.name series = %v, want exactly {a,b}", names)
	}
}

func TestCumulativeAcrossExports(t *testing.T) {
	g := New(Config{})
	sp := spanSpec{name: "x", kind: ptrace.SpanKindClient, status: ptrace.StatusCodeOk, dur: 0.003}
	g.Consume(traces("s", sp))
	exp := &capExporter{}
	_ = g.Export(context.Background(), exp, pcommon.NewResource())
	g.Consume(traces("s", sp)) // second observation
	exp2 := &capExporter{}
	_ = g.Export(context.Background(), exp2, pcommon.NewResource())
	calls, _ := exp2.find("traces.span.metrics.calls")
	if v := numberVal(dp(calls).At(0)); v != 2 {
		t.Fatalf("cumulative calls = %v, want 2", v)
	}
}

type recExporter struct{ got []ptrace.Traces }

func (r *recExporter) ExportTraces(_ context.Context, td ptrace.Traces) error {
	r.got = append(r.got, td)
	return nil
}

func TestTapConsumesAndForwards(t *testing.T) {
	g := New(Config{})
	rec := &recExporter{}
	tp := g.Tap(rec)
	td := traces("svc", spanSpec{name: "op", kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk, dur: 0.01})
	if err := tp.ExportTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	if len(rec.got) != 1 || rec.got[0].SpanCount() != 1 {
		t.Fatalf("tap did not forward the batch: %+v", rec.got)
	}
	exp := &capExporter{}
	_ = g.Export(context.Background(), exp, pcommon.NewResource())
	if _, ok := exp.find("traces.span.metrics.calls"); !ok {
		t.Fatal("tap did not aggregate the span")
	}
}

func TestConcurrentConsume(t *testing.T) {
	g := New(Config{})
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				g.Consume(traces("svc", spanSpec{name: "op", kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk, dur: 0.002}))
			}
		}()
	}
	wg.Wait()
	exp := &capExporter{}
	if err := g.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	calls, _ := exp.find("traces.span.metrics.calls")
	var total float64
	dps := dp(calls)
	for i := 0; i < dps.Len(); i++ {
		total += numberVal(dps.At(i))
	}
	if total != 8*500 {
		t.Fatalf("concurrent calls total = %v, want %d", total, 8*500)
	}
}

func TestDurationSkewClamped(t *testing.T) {
	td := ptrace.NewTraces()
	s := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	s.SetStartTimestamp(pcommon.Timestamp(100))
	s.SetEndTimestamp(pcommon.Timestamp(50)) // end before start
	if d := cumagg.SpanSeconds(s); d != 0 {
		t.Fatalf("skewed duration = %v, want 0", d)
	}
}

var (
	tid1 = pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	sid1 = pcommon.SpanID([8]byte{1, 1, 1, 1, 1, 1, 1, 1})
)

func TestSizeCounter(t *testing.T) {
	g := New(Config{})
	g.Consume(traces("svc", spanSpec{
		name: "op", kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk, dur: 0.01,
		attrs: map[string]string{"http.route": "/api/v1/users"},
	}))
	exp := &capExporter{}
	if err := g.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	size, ok := exp.find("traces.span.metrics.size")
	if !ok {
		t.Fatal("size metric not exported")
	}
	if size.Type() != pmetric.MetricTypeSum || !size.Sum().IsMonotonic() || size.Unit() != "By" {
		t.Fatalf("size is not a monotonic byte sum: type=%v unit=%q", size.Type(), size.Unit())
	}
	// name(2) + ids(24) + attr key+value.
	want := int64(len("op") + 24 + len("http.route") + len("/api/v1/users"))
	if got := size.Sum().DataPoints().At(0).IntValue(); got != want {
		t.Fatalf("size = %d, want %d", got, want)
	}
}

func TestExemplarsOnDuration(t *testing.T) {
	g := New(Config{})
	g.Consume(traces("svc", spanSpec{
		name: "op", kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk, dur: 0.03,
		traceID: tid1, spanID: sid1,
	}))
	exp := &capExporter{}
	if err := g.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	dur, _ := exp.find("traces.span.metrics.duration")
	hp := dur.Histogram().DataPoints().At(0)
	if hp.Exemplars().Len() != 1 {
		t.Fatalf("exemplars = %d, want 1", hp.Exemplars().Len())
	}
	ex := hp.Exemplars().At(0)
	if ex.TraceID() != tid1 || ex.SpanID() != sid1 {
		t.Fatalf("exemplar ids = %v/%v, want %v/%v", ex.TraceID(), ex.SpanID(), tid1, sid1)
	}
	if ex.DoubleValue() < 0.029 || ex.DoubleValue() > 0.031 {
		t.Fatalf("exemplar value = %v, want ~0.03", ex.DoubleValue())
	}
	// Exemplars are reset after a DELIVERED export: a second export with no new
	// spans carries none (the cumulative histogram still exports).
	exp2 := &capExporter{}
	_ = g.Export(context.Background(), exp2, pcommon.NewResource())
	dur2, _ := exp2.find("traces.span.metrics.duration")
	if n := dur2.Histogram().DataPoints().At(0).Exemplars().Len(); n != 0 {
		t.Fatalf("exemplars after reset = %d, want 0", n)
	}
}

type failExporter struct{}

func (failExporter) ExportMetrics(context.Context, pmetric.Metrics) error {
	return context.DeadlineExceeded
}

// A FAILED export must not wipe the exemplars unseen: the next successful
// export still carries them.
func TestExemplarsSurviveFailedExport(t *testing.T) {
	g := New(Config{})
	g.Consume(traces("svc", spanSpec{
		name: "op", kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk, dur: 0.03,
		traceID: tid1, spanID: sid1,
	}))
	if err := g.Export(context.Background(), failExporter{}, pcommon.NewResource()); err == nil {
		t.Fatal("export should have failed")
	}
	exp := &capExporter{}
	if err := g.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	dur, _ := exp.find("traces.span.metrics.duration")
	if n := dur.Histogram().DataPoints().At(0).Exemplars().Len(); n != 1 {
		t.Fatalf("exemplars after failed-then-successful export = %d, want 1 (wiped unseen)", n)
	}
}

func TestExemplarsDisabled(t *testing.T) {
	no := false
	g := New(Config{Exemplars: &no})
	g.Consume(traces("svc", spanSpec{name: "op", kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk, dur: 0.03, traceID: tid1, spanID: sid1}))
	exp := &capExporter{}
	_ = g.Export(context.Background(), exp, pcommon.NewResource())
	dur, _ := exp.find("traces.span.metrics.duration")
	if n := dur.Histogram().DataPoints().At(0).Exemplars().Len(); n != 0 {
		t.Fatalf("exemplars with Exemplars=false = %d, want 0", n)
	}
}

// span builds a one-span batch with the given span name.
func span(name string) ptrace.Traces {
	return traces("svc", spanSpec{name: name, kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk, dur: 0.004})
}

// seriesNames returns the span.name dimension of every exported calls point.
func seriesNames(t *testing.T, exp *capExporter) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	calls, ok := exp.find("traces.span.metrics.calls")
	if !ok {
		return out
	}
	dps := dp(calls)
	for i := 0; i < dps.Len(); i++ {
		out[attr(dps.At(i).Attributes(), "span.name")] = true
	}
	return out
}

// A cardinality cap without eviction is a ONE-WAY LATCH: once a burst of
// short-lived span names fills it, no new service on the node is ever measured
// again. Stale series must be evicted at export and their slots reused.
func TestStaleSeriesEvictedFreesCardinalitySlot(t *testing.T) {
	ctx := context.Background()
	res := pcommon.NewResource()
	g := New(Config{MaxCardinality: 2, StaleAfter: "15m"})
	now := time.Unix(1_700_000_000, 0)
	g.now = func() time.Time { return now }

	g.Consume(span("burst-a"))
	g.Consume(span("burst-b"))
	// The cap is full: a third tuple is refused.
	before := obs.SpanMetricsDropped.Value()
	g.Consume(span("newcomer"))
	if got := obs.SpanMetricsDropped.Value() - before; got != 1 {
		t.Fatalf("dropped delta = %v, want 1 (cap should hold before eviction)", got)
	}

	exp1 := &capExporter{}
	if err := g.Export(ctx, exp1, res); err != nil {
		t.Fatal(err)
	}
	if names := seriesNames(t, exp1); !names["burst-a"] || !names["burst-b"] || len(names) != 2 {
		t.Fatalf("first export series = %v, want {burst-a,burst-b}", names)
	}

	// Nothing observed for longer than staleAfter: the next export evicts.
	evictedBefore := obs.SpanMetricsEvicted.Value()
	now = now.Add(16 * time.Minute)
	exp2 := &capExporter{}
	if err := g.Export(ctx, exp2, res); err != nil {
		t.Fatal(err)
	}
	if exp2.exports() != 0 {
		t.Fatalf("exported %d payloads after everything went stale, want 0", exp2.exports())
	}
	if n := g.store.Len(); n != 0 {
		t.Fatalf("series after eviction = %d, want 0", n)
	}
	if got := obs.SpanMetricsEvicted.Value() - evictedBefore; got != 2 {
		t.Fatalf("evicted counter delta = %v, want 2", got)
	}

	// The freed slots admit new tuples.
	dropBefore := obs.SpanMetricsDropped.Value()
	g.Consume(span("newcomer"))
	if got := obs.SpanMetricsDropped.Value() - dropBefore; got != 0 {
		t.Fatalf("dropped delta = %v after eviction, want 0 (slot must be reusable)", got)
	}
	exp3 := &capExporter{}
	if err := g.Export(ctx, exp3, res); err != nil {
		t.Fatal(err)
	}
	names := seriesNames(t, exp3)
	if len(names) != 1 || !names["newcomer"] {
		t.Fatalf("post-eviction export series = %v, want exactly {newcomer}", names)
	}
}

// Eviction must never destroy observations no export has carried: an export
// interval may legally exceed staleAfter.
func TestStaleSeriesSurviveUntilExported(t *testing.T) {
	ctx := context.Background()
	res := pcommon.NewResource()
	g := New(Config{StaleAfter: "1m"})
	now := time.Unix(1_700_000_000, 0)
	g.now = func() time.Time { return now }

	g.Consume(span("rare"))
	now = now.Add(10 * time.Minute) // stale, but never exported
	exp := &capExporter{}
	if err := g.Export(ctx, exp, res); err != nil {
		t.Fatal(err)
	}
	if names := seriesNames(t, exp); !names["rare"] {
		t.Fatalf("series evicted before any export carried it: %v", names)
	}
	// Reported now: the next stale export drops it.
	now = now.Add(10 * time.Minute)
	exp2 := &capExporter{}
	if err := g.Export(ctx, exp2, res); err != nil {
		t.Fatal(err)
	}
	if exp2.exports() != 0 || g.store.Len() != 0 {
		t.Fatalf("exports=%d series=%d after the reported series went stale, want 0/0", exp2.exports(), g.store.Len())
	}
}

// A re-created series restarts its cumulative counters, so it must carry a
// FRESH start timestamp — otherwise the value looks like a counter jumping
// backwards under an unchanged start.
func TestReCreatedSeriesGetsFreshStartTimestamp(t *testing.T) {
	ctx := context.Background()
	res := pcommon.NewResource()
	g := New(Config{StaleAfter: "5m"})
	now := time.Unix(1_700_000_000, 0)
	g.now = func() time.Time { return now }

	g.Consume(span("op"))
	exp1 := &capExporter{}
	_ = g.Export(ctx, exp1, res)
	calls1, _ := exp1.find("traces.span.metrics.calls")
	first := dp(calls1).At(0).StartTimestamp()

	now = now.Add(6 * time.Minute)
	_ = g.Export(ctx, &capExporter{}, res) // evicts
	g.Consume(span("op"))                  // same dimensions, new series
	exp2 := &capExporter{}
	_ = g.Export(ctx, exp2, res)
	calls2, ok := exp2.find("traces.span.metrics.calls")
	if !ok {
		t.Fatal("re-created series not exported")
	}
	d := dp(calls2).At(0)
	if v := numberVal(d); v != 1 {
		t.Fatalf("re-created series calls = %v, want 1 (cumulative restarted)", v)
	}
	if d.StartTimestamp() <= first {
		t.Fatalf("start timestamp %v not advanced past %v after re-creation", d.StartTimestamp(), first)
	}
}

// A series ADMITTED between Export's clock read and renderRED's lock hold
// carries a Meta.Start LATER than the payload's point timestamp; unclamped,
// its first export rendered StartTimestamp > Timestamp — an inverted
// cumulative interval (cumagg.ClampStart has the full race). The true start
// renders from the next export on.
func TestStartClampedWhenSeriesAdmittedDuringExport(t *testing.T) {
	g := New(Config{})
	exportTime := time.Unix(1_700_000_000, 0) // Export's one clock read
	admitted := exportTime.Add(time.Second)   // the receive goroutine's later one
	g.now = func() time.Time { return admitted }
	g.Consume(span("op"))

	wantTS := pcommon.NewTimestampFromTime(exportTime)
	checkStamps(t, g.store.Render(pcommon.NewResource(), exportTime), wantTS, wantTS)

	// The next export's ts lies past the true start, which renders unclamped.
	next := exportTime.Add(2 * time.Second)
	checkStamps(t, g.store.Render(pcommon.NewResource(), next),
		pcommon.NewTimestampFromTime(admitted), pcommon.NewTimestampFromTime(next))
}

// checkStamps checks every data point of every metric in md against one wanted
// (start, ts) pair.
func checkStamps(t *testing.T, md pmetric.Metrics, wantStart, wantTS pcommon.Timestamp) {
	t.Helper()
	points := 0
	check := func(name string, start, ts pcommon.Timestamp) {
		points++
		if start != wantStart || ts != wantTS {
			t.Errorf("%s point start/ts = %v/%v, want %v/%v", name, start, ts, wantStart, wantTS)
		}
	}
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				switch m.Type() {
				case pmetric.MetricTypeSum:
					dps := m.Sum().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						check(m.Name(), dps.At(l).StartTimestamp(), dps.At(l).Timestamp())
					}
				case pmetric.MetricTypeHistogram:
					dps := m.Histogram().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						check(m.Name(), dps.At(l).StartTimestamp(), dps.At(l).Timestamp())
					}
				}
			}
		}
	}
	if points == 0 {
		t.Fatal("no data points rendered")
	}
}

func TestStaleAfterConfig(t *testing.T) {
	if got := New(Config{}).store.StaleAfter(); got != defaultStaleAfter {
		t.Fatalf("default staleAfter = %v, want %v", got, defaultStaleAfter)
	}
	if got := New(Config{StaleAfter: "0"}).store.StaleAfter(); got != 0 {
		t.Fatalf("explicit 0 staleAfter = %v, want 0 (eviction disabled)", got)
	}
	if err := (Config{StaleAfter: "fifteen minutes"}).Validate(); err == nil {
		t.Fatal("Validate accepted an unparseable staleAfter")
	}
	// An unparseable value still aggregates, on the default.
	if got := New(Config{StaleAfter: "fifteen minutes"}).store.StaleAfter(); got != defaultStaleAfter {
		t.Fatalf("bad staleAfter fell back to %v, want %v", got, defaultStaleAfter)
	}
	// A NEGATIVE value is REFUSED, not clamped. 0 means "eviction disabled"
	// here, so clamping a negative to it silently turned the cardinality cap
	// into the one-way latch this field exists to prevent — and the sibling
	// serviceGraph.staleAfter had always refused the same input (see
	// cumagg.ParseStaleAfter and the cross-package test beside it).
	if err := (Config{StaleAfter: "-15m"}).Validate(); err == nil {
		t.Fatal("Validate accepted a negative staleAfter")
	}
	if got := New(Config{StaleAfter: "-15m"}).store.StaleAfter(); got != defaultStaleAfter {
		t.Fatalf("negative staleAfter resolved to %v, want the default %v (0 would DISABLE eviction)", got, defaultStaleAfter)
	}
	// Eviction disabled: a long-idle series keeps reporting.
	g := New(Config{StaleAfter: "0"})
	now := time.Unix(1_700_000_000, 0)
	g.now = func() time.Time { return now }
	g.Consume(span("op"))
	_ = g.Export(context.Background(), &capExporter{}, pcommon.NewResource())
	now = now.Add(24 * time.Hour)
	exp := &capExporter{}
	_ = g.Export(context.Background(), exp, pcommon.NewResource())
	if names := seriesNames(t, exp); !names["op"] {
		t.Fatalf("series dropped with eviction disabled: %v", names)
	}
}

// A rendered-but-NOT-delivered series must survive: eviction may only drop
// values the collector actually acked.
func TestStaleSeriesSurviveFailedExport(t *testing.T) {
	ctx := context.Background()
	res := pcommon.NewResource()
	g := New(Config{StaleAfter: "1m"})
	now := time.Unix(1_700_000_000, 0)
	g.now = func() time.Time { return now }

	g.Consume(span("rare"))
	if err := g.Export(ctx, failExporter{}, res); err == nil {
		t.Fatal("export should have failed")
	}
	now = now.Add(10 * time.Minute) // stale, but nothing was ever delivered
	exp := &capExporter{}
	if err := g.Export(ctx, exp, res); err != nil {
		t.Fatal(err)
	}
	if names := seriesNames(t, exp); !names["rare"] {
		t.Fatalf("series evicted although no export ever delivered it: %v", names)
	}
	// Delivered now: it becomes evictable.
	now = now.Add(10 * time.Minute)
	exp2 := &capExporter{}
	if err := g.Export(ctx, exp2, res); err != nil {
		t.Fatal(err)
	}
	if exp2.exports() != 0 || g.store.Len() != 0 {
		t.Fatalf("exports=%d series=%d after a delivered series went stale, want 0/0", exp2.exports(), g.store.Len())
	}
}

// The series key must be built from the SAME truncated values the data points
// render. Keyed on the raw ones, two spans differing only past maxDimBytes
// occupied two series that rendered byte-identical attribute sets — duplicate
// points in one export, which downstream reads as a conflict rather than as
// extra detail — and the key retained in the map was as long as the sender
// cared to make span.name, leaking past the bound truncation exists to impose.
func TestSeriesKeyTruncatesLikeTheRenderedDimensions(t *testing.T) {
	prefix := strings.Repeat("a", maxDimBytes)
	exp := &capExporter{}
	g := New(Config{Dimensions: []string{"db.statement"}})

	// Same span name and same extra-dimension value up to the cut; different
	// only in the bytes no data point will ever carry.
	g.Consume(traces("svc",
		spanSpec{name: prefix + "-one", dur: 0.01, attrs: map[string]string{"db.statement": prefix + "-x"}},
		spanSpec{name: prefix + "-two", dur: 0.02, attrs: map[string]string{"db.statement": prefix + "-y"}},
	))

	if n := g.store.Len(); n != 1 {
		t.Fatalf("series = %d, want 1: spans that render identically must share one series", n)
	}
	g.store.Range(func(k string, _ *spanSeries) bool {
		// 6 parts at most maxDimBytes each, plus the length prefixes; the
		// untruncated key grew with whatever the sender sent.
		if max := len(g.names) * (maxDimBytes + 8); len(k) > max {
			t.Errorf("series key is %d bytes, want <= %d: untruncated values are retained in the map", len(k), max)
		}
		return true
	})

	if err := g.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	m, ok := exp.find("traces.span.metrics.calls")
	if !ok {
		t.Fatal("calls metric missing")
	}
	dps := dp(m)
	if dps.Len() != 1 {
		t.Fatalf("exported %d data points, want 1 (a duplicate series renders the identical attribute set twice)", dps.Len())
	}
	if got := dps.At(0).IntValue(); got != 2 {
		t.Errorf("calls = %d, want 2: both spans belong to the one rendered series", got)
	}
	if got := attr(dps.At(0).Attributes(), "span.name"); got != prefix {
		t.Errorf("span.name = %q (%d bytes), want the %d-byte truncation", got, len(got), maxDimBytes)
	}
	if got := attr(dps.At(0).Attributes(), "db.statement"); got != prefix {
		t.Errorf("db.statement = %q (%d bytes), want the %d-byte truncation", got, len(got), maxDimBytes)
	}
}

// Truncating a Go string is a RESLICE: it keeps the whole original alive. The
// dimension values a series retains therefore have to be cut with a copy, or
// maxDimBytes bounds nothing — a sender controlling span.name pins its full
// length per series for staleAfter, which is the exact scenario the constant's
// comment says it prevents. (The key observe builds may still reslice: the map
// copies it on insert.)
func TestTruncatedDimensionDoesNotRetainTheSenderString(t *testing.T) {
	const huge = 4 << 20
	name := strings.Repeat("x", huge)
	g := New(Config{})
	g.Consume(traces("checkout", spanSpec{
		name: name, kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk,
		dur: 0.01, traceID: tid1, spanID: sid1,
	}))

	if g.store.Len() != 1 {
		t.Fatalf("series = %d, want 1", g.store.Len())
	}
	g.store.Range(func(_ string, s *spanSeries) bool {
		got := s.dims[1] // span.name
		if len(got) != maxDimBytes {
			t.Fatalf("retained span.name is %d bytes, want %d", len(got), maxDimBytes)
		}
		if unsafe.StringData(got) == unsafe.StringData(name) {
			t.Fatalf("the retained %d-byte label still points into the %d-byte span name: "+
				"the whole string stays alive for staleAfter", len(got), huge)
		}
		return true
	})
}

// The allocator's view of the same thing: a burst of over-long span names must
// not leave their bytes resident once the payloads are gone.
func TestTruncatedDimensionsDoNotAccumulateHeap(t *testing.T) {
	const (
		huge   = 4 << 20
		series = 8
	)
	g := New(Config{})
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := 0; i < series; i++ {
		name := strings.Repeat(string(rune('a'+i)), huge)
		g.Consume(traces("checkout", spanSpec{
			name: name, kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk,
			dur: 0.01, traceID: tid1, spanID: sid1,
		}))
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("heap retained by %d series cut from %d MiB span names: %d bytes",
		series, huge>>20, retained)
	if g.store.Len() != series {
		t.Fatalf("series = %d, want %d", g.store.Len(), series)
	}
	if max := int64(huge); retained > max {
		t.Errorf("retained %d bytes, want well under one span name (%d): the truncation pins its source",
			retained, max)
	}
}
