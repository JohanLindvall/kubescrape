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
		for k, v := range sp.attrs {
			s.Attributes().PutStr(k, v)
		}
	}
	return td
}

type spanSpec struct {
	name   string
	kind   ptrace.SpanKind
	status ptrace.StatusCode
	dur    float64
	attrs  map[string]string
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
	if len(g.seen) != 2 {
		t.Fatalf("admitted tuples = %d, want 2 (cap held)", len(g.seen))
	}
	exp := &capExporter{}
	_ = g.Export(context.Background(), exp, pcommon.NewResource())
	calls, _ := exp.find("traces.span.metrics.calls")
	// Count distinct series by span.name (counters also emit synthetic-zero
	// points): the cap admitted a and b, never c.
	names := map[string]bool{}
	dps := dp(calls)
	for i := 0; i < dps.Len(); i++ {
		names[attr(dps.At(i).Attributes(), "span.name")] = true
	}
	if len(names) != 2 || names["c"] {
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
