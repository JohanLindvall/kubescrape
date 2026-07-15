package spanmetrics

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

type capExporter struct{ md []pmetric.Metrics }

func (c *capExporter) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	cp := pmetric.NewMetrics()
	md.CopyTo(cp)
	c.md = append(c.md, cp)
	return nil
}

// find returns the metric with the given name from the last export.
func (c *capExporter) find(name string) (pmetric.Metric, bool) {
	for _, md := range c.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					if ms.At(k).Name() == name {
						return ms.At(k), true
					}
				}
			}
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
	if len(g.series) != 2 {
		t.Fatalf("admitted tuples = %d, want 2 (cap held)", len(g.series))
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
	if d := durationSeconds(s); d != 0 {
		t.Fatalf("skewed duration = %v, want 0", d)
	}
}

var (
	tid1 = pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	sid1 = pcommon.SpanID([8]byte{1, 1, 1, 1, 1, 1, 1, 1})
	sid2 = pcommon.SpanID([8]byte{2, 2, 2, 2, 2, 2, 2, 2})
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
	// Exemplars are reset each export: a second export with no new spans carries
	// none (the cumulative histogram still exports).
	exp2 := &capExporter{}
	_ = g.Export(context.Background(), exp2, pcommon.NewResource())
	dur2, _ := exp2.find("traces.span.metrics.duration")
	if n := dur2.Histogram().DataPoints().At(0).Exemplars().Len(); n != 0 {
		t.Fatalf("exemplars after reset = %d, want 0", n)
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

func TestServiceGraph(t *testing.T) {
	g := New(Config{ServiceGraphs: true})
	// A client span in service "frontend" calling a server span in "checkout":
	// the server span's parent is the client span. The two arrive as separate
	// batches (order independent) — here server first, then client.
	g.Consume(traces("checkout", spanSpec{
		name: "POST /pay", kind: ptrace.SpanKindServer, status: ptrace.StatusCodeError, dur: 0.05,
		traceID: tid1, spanID: sid2, parentID: sid1,
	}))
	g.Consume(traces("frontend", spanSpec{
		name: "call checkout", kind: ptrace.SpanKindClient, status: ptrace.StatusCodeOk, dur: 0.06,
		traceID: tid1, spanID: sid1,
	}))

	exp := &capExporter{}
	if err := g.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	total, ok := exp.find("traces.service_graph.request.total")
	if !ok {
		t.Fatal("service graph request.total not exported")
	}
	tp := total.Sum().DataPoints().At(0)
	if attr(tp.Attributes(), "client") != "frontend" || attr(tp.Attributes(), "server") != "checkout" {
		t.Fatalf("edge = %v, want frontend->checkout", tp.Attributes().AsRaw())
	}
	if tp.IntValue() != 1 {
		t.Fatalf("request.total = %d, want 1", tp.IntValue())
	}
	failed, _ := exp.find("traces.service_graph.request.failed")
	if v := failed.Sum().DataPoints().At(0).IntValue(); v != 1 {
		t.Fatalf("request.failed = %d, want 1 (server errored)", v)
	}
	if _, ok := exp.find("traces.service_graph.request.server"); !ok {
		t.Fatal("server latency histogram not exported")
	}
	if _, ok := exp.find("traces.service_graph.request.client"); !ok {
		t.Fatal("client latency histogram not exported")
	}
	// The edge is complete, so no half-edge is left pending.
	if n := len(g.sg.pending); n != 0 {
		t.Fatalf("pending half-edges = %d, want 0", n)
	}
}

func TestServiceGraphDisabledByDefault(t *testing.T) {
	g := New(Config{})
	if g.sg != nil {
		t.Fatal("service graph should be nil unless ServiceGraphs is set")
	}
	g.Consume(traces("a", spanSpec{name: "x", kind: ptrace.SpanKindClient, status: ptrace.StatusCodeOk, dur: 0.01, traceID: tid1, spanID: sid1}))
	exp := &capExporter{}
	_ = g.Export(context.Background(), exp, pcommon.NewResource())
	if _, ok := exp.find("traces.service_graph.request.total"); ok {
		t.Fatal("service graph metrics exported when disabled")
	}
}
