package otlpsplit

// The splitter's contract was pinned by fixed shape tables, and a shape table
// is only as good as its author's imagination: the alternating-leaf shape that
// broke the documented amplification bound sat outside every table while a test
// asserted the (wrong) bound. FuzzSplitInvariants decodes arbitrary bytes into
// a bounded payload SHAPE — resources x scopes x leaves, with every size drawn
// from tiny through cap-sized, and a cap from 1 KiB to 64 KiB — builds it for
// all three signals, and asserts the properties every caller relies on:
//
//   - every leaf ships exactly once, in order, under its own resource and scope;
//   - every resource and every scope (empty ones included) ships at least once;
//   - a non-empty input never yields zero parts;
//   - the report counts EXACTLY the parts over the cap;
//   - an over-cap part is a single leaf unless a split was abandoned;
//   - the parts cost at most the documented constant multiple of the input.
//
// `go test` runs the seed corpus; `go test -run '^$' -fuzz FuzzSplitInvariants`
// explores.

import (
	"strconv"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// fuzzBytes hands out the fuzz input one byte at a time, zero once exhausted,
// so every input — however short — decodes to a well-formed shape.
type fuzzBytes struct {
	b []byte
	i int
}

func (f *fuzzBytes) next() int {
	if f.i >= len(f.b) {
		return 0
	}
	v := int(f.b[f.i])
	f.i++
	return v
}

// size draws a byte count relative to the cap: tiny, small, a fraction of the
// cap, or near (possibly over) it — the last two are where the edges are.
func (f *fuzzBytes) size(capBytes int) int {
	switch f.next() % 4 {
	case 0:
		return f.next() % 16
	case 1:
		return f.next() * 4
	case 2:
		return capBytes * f.next() / 256
	default:
		return capBytes - 96 + f.next()%160
	}
}

// fuzzLeaf is one leaf in input order, with the resource and scope it belongs to.
type fuzzLeaf struct{ rid, sid int }

// fuzzShape is the decoded shape: per resource, per scope, the leaf sizes. For
// metrics a "leaf" is a metric, and its data points are points[].
type fuzzShape struct {
	capBytes int
	res      []fuzzResource
}

type fuzzResource struct {
	attr   int
	scopes []fuzzScope
}

type fuzzScope struct {
	attr   int
	leaves []fuzzLeafSpec
}

type fuzzLeafSpec struct {
	size   int   // record body / span name / metric description
	kind   int   // metrics only: 0 empty, 1 gauge, 2 sum, 3 histogram, 4 exponential, 5 summary
	points []int // metrics only: per-point pad size
}

func decodeShape(data []byte) fuzzShape {
	f := &fuzzBytes{b: data}
	sh := fuzzShape{capBytes: 1<<10 + (f.next()<<8|f.next())%(63<<10+1)}
	for r, nr := 0, 1+f.next()%3; r < nr; r++ {
		res := fuzzResource{attr: f.size(sh.capBytes)}
		for s, ns := 0, f.next()%3; s < ns; s++ {
			sc := fuzzScope{}
			if f.next()%4 == 0 {
				sc.attr = f.size(sh.capBytes)
			}
			for l, nl := 0, f.next()%9; l < nl; l++ {
				leaf := fuzzLeafSpec{size: f.size(sh.capBytes), kind: f.next() % 6}
				if leaf.kind != 0 {
					np := f.next() % 6
					if f.next()%8 == 0 {
						np = f.next() // occasionally many points: the data-point split
					}
					for p := 0; p < np; p++ {
						leaf.points = append(leaf.points, f.size(sh.capBytes)/4)
					}
				}
				sc.leaves = append(sc.leaves, leaf)
			}
			res.scopes = append(res.scopes, sc)
		}
		sh.res = append(sh.res, res)
	}
	return sh
}

func fuzzPad(n int) string { return strings.Repeat("p", n) }

func FuzzSplitInvariants(f *testing.F) {
	for _, seed := range [][]byte{
		{},
		{0, 0, 0},
		{4, 0, 1, 3, 200, 2, 0, 8, 3, 150, 1, 3, 3, 3, 3, 3, 3, 3},
		{255, 255, 2, 3, 255, 2, 1, 3, 255, 8, 3, 90, 2, 1, 3, 90, 1, 3, 200},
		{8, 0, 0, 3, 250, 1, 0, 8, 1, 3, 5, 1, 2, 7, 5, 0, 3, 0, 3, 0},
		{16, 0, 2, 2, 200, 2, 0, 4, 1, 3, 3, 1, 0, 200, 0, 1, 0, 1, 0, 1, 0, 1, 0},
		{2, 0, 0, 3, 240, 1, 0, 6, 0, 3, 1, 0, 1, 2, 0, 1, 0, 1, 0, 3, 80, 0},
		[]byte(strings.Repeat("\x03\xc8\x02\x07", 16)),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		sh := decodeShape(data)
		checkLogsShape(t, sh)
		checkTracesShape(t, sh)
		checkMetricsShape(t, sh)
	})
}

// checkReport asserts the two properties every signal shares: exactly the
// over-cap parts are reported, and one is a single leaf unless a split was
// abandoned.
func checkReport(t *testing.T, signal string, sizes, leaves []int, capBytes int, rep Report) {
	t.Helper()
	over := 0
	for i, sz := range sizes {
		if sz <= capBytes {
			continue
		}
		over++
		if rep.Abandoned == 0 && leaves[i] > 1 {
			t.Errorf("%s: part %d is %d bytes over the %d cap and carries %d leaves; only a single leaf or an abandoned split may exceed the cap",
				signal, i, sz, capBytes, leaves[i])
		}
	}
	if got := rep.Oversize + rep.Abandoned; got != over {
		t.Errorf("%s: %d parts over the cap, the report counts %d (%+v)", signal, over, got, rep)
	}
}

func checkAmplification(t *testing.T, signal string, sizes []int, in, bound int) {
	t.Helper()
	total := 0
	for _, sz := range sizes {
		total += sz
	}
	if total > bound*in {
		t.Errorf("%s: parts total %d bytes for a %d-byte input (%.2fx), over the %dx bound", signal, total, in, float64(total)/float64(in), bound)
	}
}

// ids reads the leaf ids and resource/scope identity back out of a part.
func idOf(m pcommon.Map, key string) int {
	v, ok := m.Get(key)
	if !ok {
		return -1
	}
	return int(v.Int())
}

func checkLogsShape(t *testing.T, sh fuzzShape) {
	ld := plog.NewLogs()
	var want []fuzzLeaf
	scopes := 0
	for r, res := range sh.res {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutInt("rid", int64(r))
		rl.Resource().Attributes().PutStr("pad", fuzzPad(res.attr))
		for s, sc := range res.scopes {
			sl := rl.ScopeLogs().AppendEmpty()
			sl.Scope().SetName(strconv.Itoa(s))
			sl.Scope().Attributes().PutStr("pad", fuzzPad(sc.attr))
			scopes++
			for _, leaf := range sc.leaves {
				lr := sl.LogRecords().AppendEmpty()
				lr.Attributes().PutInt("id", int64(len(want)))
				lr.Body().SetStr(fuzzPad(leaf.size))
				want = append(want, fuzzLeaf{r, s})
			}
		}
	}
	parts, rep := LogsWithReport(ld, sh.capBytes)
	if len(sh.res) > 0 && len(parts) == 0 {
		t.Fatal("logs: a non-empty input yielded zero parts")
	}
	var sizes, leaves []int
	next := 0
	seenRes, seenScope := map[int]bool{}, map[[2]int]bool{}
	for _, p := range parts {
		sizes = append(sizes, logMarshaler.LogsSize(p))
		leaves = append(leaves, p.LogRecordCount())
		for i := 0; i < p.ResourceLogs().Len(); i++ {
			rl := p.ResourceLogs().At(i)
			rid := idOf(rl.Resource().Attributes(), "rid")
			seenRes[rid] = true
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				sl := rl.ScopeLogs().At(j)
				sid, _ := strconv.Atoi(sl.Scope().Name())
				seenScope[[2]int{rid, sid}] = true
				for k := 0; k < sl.LogRecords().Len(); k++ {
					id := idOf(sl.LogRecords().At(k).Attributes(), "id")
					if id != next {
						t.Fatalf("logs: record %d arrived where record %d was due — lost, duplicated or reordered", id, next)
					}
					if w := want[id]; w.rid != rid || w.sid != sid {
						t.Fatalf("logs: record %d shipped under resource %d scope %d, want %d/%d", id, rid, sid, w.rid, w.sid)
					}
					next++
				}
			}
		}
	}
	if next != len(want) {
		t.Fatalf("logs: %d of %d records shipped", next, len(want))
	}
	if len(seenRes) != len(sh.res) || len(seenScope) != scopes {
		t.Errorf("logs: %d/%d resources and %d/%d scopes shipped; an empty one's identity was dropped",
			len(seenRes), len(sh.res), len(seenScope), scopes)
	}
	checkReport(t, "logs", sizes, leaves, sh.capBytes, rep)
	checkAmplification(t, "logs", sizes, logMarshaler.LogsSize(ld), logsAmplificationBound)
}

func checkTracesShape(t *testing.T, sh fuzzShape) {
	td := ptrace.NewTraces()
	var want []fuzzLeaf
	for r, res := range sh.res {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutInt("rid", int64(r))
		rs.Resource().Attributes().PutStr("pad", fuzzPad(res.attr))
		for s, sc := range res.scopes {
			ss := rs.ScopeSpans().AppendEmpty()
			ss.Scope().SetName(strconv.Itoa(s))
			ss.Scope().Attributes().PutStr("pad", fuzzPad(sc.attr))
			for _, leaf := range sc.leaves {
				sp := ss.Spans().AppendEmpty()
				sp.Attributes().PutInt("id", int64(len(want)))
				sp.SetName(fuzzPad(leaf.size))
				want = append(want, fuzzLeaf{r, s})
			}
		}
	}
	parts, rep := TracesWithReport(td, sh.capBytes)
	if len(sh.res) > 0 && len(parts) == 0 {
		t.Fatal("traces: a non-empty input yielded zero parts")
	}
	var sizes, leaves []int
	next := 0
	for _, p := range parts {
		sizes = append(sizes, traceMarshaler.TracesSize(p))
		leaves = append(leaves, p.SpanCount())
		for i := 0; i < p.ResourceSpans().Len(); i++ {
			rs := p.ResourceSpans().At(i)
			rid := idOf(rs.Resource().Attributes(), "rid")
			for j := 0; j < rs.ScopeSpans().Len(); j++ {
				ss := rs.ScopeSpans().At(j)
				sid, _ := strconv.Atoi(ss.Scope().Name())
				for k := 0; k < ss.Spans().Len(); k++ {
					id := idOf(ss.Spans().At(k).Attributes(), "id")
					if id != next {
						t.Fatalf("traces: span %d arrived where span %d was due", id, next)
					}
					if w := want[id]; w.rid != rid || w.sid != sid {
						t.Fatalf("traces: span %d shipped under resource %d scope %d, want %d/%d", id, rid, sid, w.rid, w.sid)
					}
					next++
				}
			}
		}
	}
	if next != len(want) {
		t.Fatalf("traces: %d of %d spans shipped", next, len(want))
	}
	checkReport(t, "traces", sizes, leaves, sh.capBytes, rep)
	checkAmplification(t, "traces", sizes, traceMarshaler.TracesSize(td), logsAmplificationBound)
}

func checkMetricsShape(t *testing.T, sh fuzzShape) {
	md := pmetric.NewMetrics()
	type point struct{ metric int }
	var want []point
	metrics := 0
	for r, res := range sh.res {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutInt("rid", int64(r))
		rm.Resource().Attributes().PutStr("pad", fuzzPad(res.attr))
		for s, sc := range res.scopes {
			sm := rm.ScopeMetrics().AppendEmpty()
			sm.Scope().SetName(strconv.Itoa(s))
			sm.Scope().Attributes().PutStr("pad", fuzzPad(sc.attr))
			for _, leaf := range sc.leaves {
				m := sm.Metrics().AppendEmpty()
				m.SetName(strconv.Itoa(metrics))
				m.SetDescription(fuzzPad(leaf.size))
				for _, pad := range leaf.points {
					a := appendFuzzPoint(m, leaf.kind)
					a.PutInt("id", int64(len(want)))
					a.PutStr("pad", fuzzPad(pad))
					want = append(want, point{metrics})
				}
				metrics++
			}
		}
	}
	parts, rep := MetricsWithReport(md, sh.capBytes)
	if len(sh.res) > 0 && len(parts) == 0 {
		t.Fatal("metrics: a non-empty input yielded zero parts")
	}
	var sizes, leaves []int
	next := 0
	seenMetric := map[int]bool{}
	for _, p := range parts {
		sizes = append(sizes, metricMarshaler.MetricsSize(p))
		// A leaf here is a metric AND its points: an over-cap part may carry
		// one metric with at most one point.
		leaf := max(p.MetricCount(), p.DataPointCount())
		leaves = append(leaves, leaf)
		for i := 0; i < p.ResourceMetrics().Len(); i++ {
			sms := p.ResourceMetrics().At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)
					mid, _ := strconv.Atoi(m.Name())
					seenMetric[mid] = true
					eachFuzzPoint(m, func(a pcommon.Map) {
						id := idOf(a, "id")
						if id != next {
							t.Fatalf("metrics: point %d arrived where point %d was due", id, next)
						}
						if want[id].metric != mid {
							t.Fatalf("metrics: point %d shipped under metric %d, want %d", id, mid, want[id].metric)
						}
						next++
					})
				}
			}
		}
	}
	if next != len(want) {
		t.Fatalf("metrics: %d of %d data points shipped", next, len(want))
	}
	if len(seenMetric) != metrics {
		t.Errorf("metrics: %d of %d metrics shipped", len(seenMetric), metrics)
	}
	checkReport(t, "metrics", sizes, leaves, sh.capBytes, rep)
	checkAmplification(t, "metrics", sizes, metricMarshaler.MetricsSize(md), metricsAmplificationBound)
}

// appendFuzzPoint adds one data point of the metric's kind (setting the kind on
// first use) and returns its attributes.
func appendFuzzPoint(m pmetric.Metric, kind int) pcommon.Map {
	switch kind {
	case 1:
		if m.Type() != pmetric.MetricTypeGauge {
			m.SetEmptyGauge()
		}
		dp := m.Gauge().DataPoints().AppendEmpty()
		dp.SetIntValue(1)
		return dp.Attributes()
	case 2:
		if m.Type() != pmetric.MetricTypeSum {
			m.SetEmptySum().SetIsMonotonic(true)
		}
		dp := m.Sum().DataPoints().AppendEmpty()
		dp.SetDoubleValue(2)
		return dp.Attributes()
	case 3:
		if m.Type() != pmetric.MetricTypeHistogram {
			m.SetEmptyHistogram()
		}
		dp := m.Histogram().DataPoints().AppendEmpty()
		dp.BucketCounts().FromRaw([]uint64{1, 2})
		dp.ExplicitBounds().FromRaw([]float64{1})
		return dp.Attributes()
	case 4:
		if m.Type() != pmetric.MetricTypeExponentialHistogram {
			m.SetEmptyExponentialHistogram()
		}
		dp := m.ExponentialHistogram().DataPoints().AppendEmpty()
		dp.Positive().BucketCounts().FromRaw([]uint64{3})
		return dp.Attributes()
	default:
		if m.Type() != pmetric.MetricTypeSummary {
			m.SetEmptySummary()
		}
		dp := m.Summary().DataPoints().AppendEmpty()
		dp.QuantileValues().AppendEmpty().SetQuantile(0.5)
		return dp.Attributes()
	}
}

// eachFuzzPoint visits a metric's data points' attributes, whatever its kind.
func eachFuzzPoint(m pmetric.Metric, fn func(pcommon.Map)) {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		for i := 0; i < m.Gauge().DataPoints().Len(); i++ {
			fn(m.Gauge().DataPoints().At(i).Attributes())
		}
	case pmetric.MetricTypeSum:
		for i := 0; i < m.Sum().DataPoints().Len(); i++ {
			fn(m.Sum().DataPoints().At(i).Attributes())
		}
	case pmetric.MetricTypeHistogram:
		for i := 0; i < m.Histogram().DataPoints().Len(); i++ {
			fn(m.Histogram().DataPoints().At(i).Attributes())
		}
	case pmetric.MetricTypeExponentialHistogram:
		for i := 0; i < m.ExponentialHistogram().DataPoints().Len(); i++ {
			fn(m.ExponentialHistogram().DataPoints().At(i).Attributes())
		}
	case pmetric.MetricTypeSummary:
		for i := 0; i < m.Summary().DataPoints().Len(); i++ {
			fn(m.Summary().DataPoints().At(i).Attributes())
		}
	}
}
