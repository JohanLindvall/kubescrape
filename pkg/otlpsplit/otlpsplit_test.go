package otlpsplit

import (
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// buildLogs makes a Logs with resources resources, each with recordsPer records
// whose body is bodyLen bytes and which carry a fat attribute set.
func buildLogs(resources, recordsPer, bodyLen int) plog.Logs {
	ld := plog.NewLogs()
	for r := 0; r < resources; r++ {
		rl := ld.ResourceLogs().AppendEmpty()
		res := rl.Resource().Attributes()
		res.PutStr("service.name", fmt.Sprintf("svc-%d", r))
		res.PutStr("k8s.pod.name", fmt.Sprintf("pod-%d-abcdef", r))
		res.PutStr("k8s.namespace.name", "production")
		res.PutStr("k8s.node.name", "node-01.internal.example.com")
		sl := rl.ScopeLogs().AppendEmpty()
		sl.Scope().SetName("test")
		for i := 0; i < recordsPer; i++ {
			lr := sl.LogRecords().AppendEmpty()
			lr.Body().SetStr(strings.Repeat("x", bodyLen))
			lr.Attributes().PutStr("log.iostream", "stdout")
			lr.Attributes().PutInt("id", int64(r*recordsPer+i))
		}
	}
	return ld
}

// collectBodies returns every record body across a slice of Logs, in order.
func collectBodies(parts []plog.Logs) []string {
	var out []string
	for _, ld := range parts {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			rl := ld.ResourceLogs().At(i)
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				lrs := rl.ScopeLogs().At(j).LogRecords()
				for k := 0; k < lrs.Len(); k++ {
					out = append(out, lrs.At(k).Body().Str())
				}
			}
		}
	}
	return out
}

func TestSplitLogsBounds(t *testing.T) {
	var m plog.ProtoMarshaler
	cases := []struct {
		name                        string
		resources, recordsPer, body int
		max                         int
	}{
		{"many small resources", 200, 1, 40, 8 << 10},       // split at resource level
		{"one huge resource", 1, 5000, 40, 16 << 10},        // split within a resource
		{"few fat resources", 10, 500, 200, 32 << 10},       // mixed
		{"single record over cap", 1, 1, 50 << 10, 8 << 10}, // unsplittable → alone
		{"already fits", 3, 3, 10, 1 << 20},                 // returns unchanged
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ld := buildLogs(tc.resources, tc.recordsPer, tc.body)
			want := collectBodies([]plog.Logs{ld})
			parts := Logs(ld, tc.max)

			// Every part is within the cap, except a part holding a single
			// record that alone exceeds it (nothing can shrink that).
			for i, p := range parts {
				sz := m.LogsSize(p)
				single := p.ResourceLogs().Len() == 1 &&
					p.ResourceLogs().At(0).ScopeLogs().Len() == 1 &&
					p.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().Len() == 1
				if sz > tc.max && !single {
					t.Errorf("part %d is %d bytes, over the %d cap", i, sz, tc.max)
				}
			}
			// No record lost or duplicated, order preserved.
			got := collectBodies(parts)
			if len(got) != len(want) {
				t.Fatalf("record count changed: got %d, want %d", len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("record %d changed after split", i)
				}
			}
		})
	}
}

func TestSplitMetricsBounds(t *testing.T) {
	var m pmetric.ProtoMarshaler
	md := pmetric.NewMetrics()
	total := 0
	for r := 0; r < 5; r++ {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service.name", fmt.Sprintf("svc-%d", r))
		sm := rm.ScopeMetrics().AppendEmpty()
		for i := 0; i < 400; i++ {
			mm := sm.Metrics().AppendEmpty()
			mm.SetName(fmt.Sprintf("metric_%d_%d_with_a_longish_name", r, i))
			dp := mm.SetEmptyGauge().DataPoints().AppendEmpty()
			dp.SetDoubleValue(float64(i))
			dp.Attributes().PutStr("label", strings.Repeat("v", 32))
			total++
		}
	}
	parts := Metrics(md, 16<<10)
	got := 0
	for _, p := range parts {
		if sz := m.MetricsSize(p); sz > 16<<10 {
			// only acceptable if the part is a single metric
			if p.ResourceMetrics().Len() != 1 || p.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().Len() != 1 {
				t.Errorf("metrics part is %d bytes, over cap", sz)
			}
		}
		for i := 0; i < p.ResourceMetrics().Len(); i++ {
			for j := 0; j < p.ResourceMetrics().At(i).ScopeMetrics().Len(); j++ {
				got += p.ResourceMetrics().At(i).ScopeMetrics().At(j).Metrics().Len()
			}
		}
	}
	if got != total {
		t.Fatalf("metric count changed: got %d, want %d", got, total)
	}
}

func TestSplitTracesBounds(t *testing.T) {
	var m ptrace.ProtoMarshaler
	td := ptrace.NewTraces()
	total := 0
	for r := 0; r < 4; r++ {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", fmt.Sprintf("svc-%d", r))
		ss := rs.ScopeSpans().AppendEmpty()
		for i := 0; i < 300; i++ {
			sp := ss.Spans().AppendEmpty()
			sp.SetName(fmt.Sprintf("operation-%d-%d", r, i))
			sp.SetTraceID(pcommon.TraceID([16]byte{byte(i), byte(r), 3}))
			sp.SetSpanID(pcommon.SpanID([8]byte{byte(i), 2}))
			sp.Attributes().PutStr("http.route", strings.Repeat("/seg", 8))
			total++
		}
	}
	parts := Traces(td, 12<<10)
	got := 0
	for _, p := range parts {
		if sz := m.TracesSize(p); sz > 12<<10 {
			if p.ResourceSpans().Len() != 1 || p.ResourceSpans().At(0).ScopeSpans().At(0).Spans().Len() != 1 {
				t.Errorf("traces part is %d bytes, over cap", sz)
			}
		}
		for i := 0; i < p.ResourceSpans().Len(); i++ {
			for j := 0; j < p.ResourceSpans().At(i).ScopeSpans().Len(); j++ {
				got += p.ResourceSpans().At(i).ScopeSpans().At(j).Spans().Len()
			}
		}
	}
	if got != total {
		t.Fatalf("span count changed: got %d, want %d", got, total)
	}
}

// A payload already within the cap is returned as the same single value.
func TestSplitNoOpWithinCap(t *testing.T) {
	ld := buildLogs(2, 2, 10)
	parts := Logs(ld, 1<<20)
	if len(parts) != 1 {
		t.Fatalf("in-cap payload split into %d parts", len(parts))
	}
	// Disabled (<=0) also returns unchanged.
	if got := Logs(buildLogs(500, 10, 100), -1); len(got) != 1 {
		t.Fatalf("negative cap should disable splitting, got %d parts", len(got))
	}
}

// buildOneBigMetric returns a payload holding ONE resource/scope/metric of the
// given type with points data points, each carrying a unique "idx" attribute
// and enough padding that the family alone dwarfs a small cap. Type-level and
// point-level detail (temporality, monotonicity, exemplars, exponential
// scale/zero-count/offsets, quantiles, timestamps) is populated so the split
// can be checked for preserving it.
func buildOneBigMetric(typ pmetric.MetricType, points int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.SetSchemaUrl("https://schema/res")
	rm.Resource().Attributes().PutStr("service.name", "kube-state-metrics")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.SetSchemaUrl("https://schema/scope")
	sm.Scope().SetName("scope-x")
	m := sm.Metrics().AppendEmpty()
	m.SetName("kube_pod_container_status_ready")
	m.SetDescription("Describes whether the containers readiness check succeeded.")
	m.SetUnit("1")
	m.Metadata().PutStr("meta.key", "meta.value")

	start := pcommon.Timestamp(1_700_000_000 * 1e9)
	ts := start + pcommon.Timestamp(30*1e9)
	pad := strings.Repeat("p", 40)
	fill := func(a pcommon.Map, i int) {
		a.PutInt("idx", int64(i))
		a.PutStr("pod", fmt.Sprintf("pod-%d-%s", i, pad))
	}
	switch typ {
	case pmetric.MetricTypeGauge:
		dps := m.SetEmptyGauge().DataPoints()
		for i := 0; i < points; i++ {
			d := dps.AppendEmpty()
			fill(d.Attributes(), i)
			d.SetStartTimestamp(start)
			d.SetTimestamp(ts)
			d.SetDoubleValue(float64(i))
		}
	case pmetric.MetricTypeSum:
		s := m.SetEmptySum()
		s.SetIsMonotonic(true)
		s.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		for i := 0; i < points; i++ {
			d := s.DataPoints().AppendEmpty()
			fill(d.Attributes(), i)
			d.SetStartTimestamp(start)
			d.SetTimestamp(ts)
			d.SetIntValue(int64(i))
		}
	case pmetric.MetricTypeHistogram:
		h := m.SetEmptyHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		for i := 0; i < points; i++ {
			d := h.DataPoints().AppendEmpty()
			fill(d.Attributes(), i)
			d.SetStartTimestamp(start)
			d.SetTimestamp(ts)
			d.SetCount(uint64(i))
			d.SetSum(float64(i))
			d.ExplicitBounds().FromRaw([]float64{1, 2, 3})
			d.BucketCounts().FromRaw([]uint64{1, 2, 3, 4})
			ex := d.Exemplars().AppendEmpty()
			ex.SetDoubleValue(float64(i))
			ex.SetTraceID(pcommon.TraceID([16]byte{byte(i), 9}))
		}
	case pmetric.MetricTypeExponentialHistogram:
		e := m.SetEmptyExponentialHistogram()
		e.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		for i := 0; i < points; i++ {
			d := e.DataPoints().AppendEmpty()
			fill(d.Attributes(), i)
			d.SetStartTimestamp(start)
			d.SetTimestamp(ts)
			d.SetScale(3)
			d.SetZeroCount(uint64(i))
			d.Positive().SetOffset(int32(7))
			d.Positive().BucketCounts().FromRaw([]uint64{1, 2, 3, 4})
			d.Negative().SetOffset(int32(-2))
			d.Negative().BucketCounts().FromRaw([]uint64{5, 6})
		}
	case pmetric.MetricTypeSummary:
		su := m.SetEmptySummary()
		for i := 0; i < points; i++ {
			d := su.DataPoints().AppendEmpty()
			fill(d.Attributes(), i)
			d.SetStartTimestamp(start)
			d.SetTimestamp(ts)
			d.SetCount(uint64(i))
			d.SetSum(float64(i))
			q := d.QuantileValues().AppendEmpty()
			q.SetQuantile(0.99)
			q.SetValue(float64(i))
		}
	}
	return md
}

// eachPoint calls fn with every data point's attributes and a per-type detail
// string that must survive the split verbatim.
func eachPoint(md pmetric.Metrics, fn func(a pcommon.Map, detail string)) {
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ms := rm.ScopeMetrics().At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					dps := m.Gauge().DataPoints()
					for n := 0; n < dps.Len(); n++ {
						d := dps.At(n)
						fn(d.Attributes(), fmt.Sprintf("%v/%d/%d", d.DoubleValue(), d.StartTimestamp(), d.Timestamp()))
					}
				case pmetric.MetricTypeSum:
					dps := m.Sum().DataPoints()
					for n := 0; n < dps.Len(); n++ {
						d := dps.At(n)
						fn(d.Attributes(), fmt.Sprintf("%d/%d/%d", d.IntValue(), d.StartTimestamp(), d.Timestamp()))
					}
				case pmetric.MetricTypeHistogram:
					dps := m.Histogram().DataPoints()
					for n := 0; n < dps.Len(); n++ {
						d := dps.At(n)
						exTrace := pcommon.TraceID{}
						if d.Exemplars().Len() > 0 {
							exTrace = d.Exemplars().At(0).TraceID()
						}
						fn(d.Attributes(), fmt.Sprintf("%d/%v/%v/%v/%x/%d", d.Count(), d.Sum(),
							d.BucketCounts().AsRaw(), d.ExplicitBounds().AsRaw(), exTrace, d.Timestamp()))
					}
				case pmetric.MetricTypeExponentialHistogram:
					dps := m.ExponentialHistogram().DataPoints()
					for n := 0; n < dps.Len(); n++ {
						d := dps.At(n)
						fn(d.Attributes(), fmt.Sprintf("%d/%d/%d/%v/%d/%v/%d", d.Scale(), d.ZeroCount(),
							d.Positive().Offset(), d.Positive().BucketCounts().AsRaw(),
							d.Negative().Offset(), d.Negative().BucketCounts().AsRaw(), d.Timestamp()))
					}
				case pmetric.MetricTypeSummary:
					dps := m.Summary().DataPoints()
					for n := 0; n < dps.Len(); n++ {
						d := dps.At(n)
						qs := ""
						for q := 0; q < d.QuantileValues().Len(); q++ {
							qv := d.QuantileValues().At(q)
							qs += fmt.Sprintf("[%v=%v]", qv.Quantile(), qv.Value())
						}
						fn(d.Attributes(), fmt.Sprintf("%d/%v/%s/%d", d.Count(), d.Sum(), qs, d.Timestamp()))
					}
				}
			}
		}
	}
}

// singleLeaf reports whether p holds exactly one data point — the only part
// allowed to exceed the cap (nothing can shrink it).
func singleLeaf(p pmetric.Metrics) bool {
	return p.DataPointCount() == 1
}

// A single metric FAMILY larger than the cap must split by data point: stopping
// at the family hands the collector a part it rejects wholesale.
func TestSplitMetricsByDataPoint(t *testing.T) {
	var mar pmetric.ProtoMarshaler
	const maxBytes = 16 << 10
	types := []struct {
		name string
		typ  pmetric.MetricType
	}{
		{"gauge", pmetric.MetricTypeGauge},
		{"sum", pmetric.MetricTypeSum},
		{"histogram", pmetric.MetricTypeHistogram},
		{"exponential histogram", pmetric.MetricTypeExponentialHistogram},
		{"summary", pmetric.MetricTypeSummary},
	}
	for _, tc := range types {
		t.Run(tc.name, func(t *testing.T) {
			const points = 4000
			md := buildOneBigMetric(tc.typ, points)
			want := map[int64]string{}
			eachPoint(md, func(a pcommon.Map, detail string) {
				v, _ := a.Get("idx")
				want[v.Int()] = detail
			})
			if len(want) != points {
				t.Fatalf("fixture built %d distinct points, want %d", len(want), points)
			}
			srcSize := mar.MetricsSize(md)

			parts := Metrics(md, maxBytes)
			if len(parts) < 2 {
				t.Fatalf("an over-cap %s family produced %d part(s); it was not split", tc.name, len(parts))
			}
			seen := map[int64]int{}
			for i, p := range parts {
				if sz := mar.MetricsSize(p); sz > maxBytes && !singleLeaf(p) {
					t.Errorf("part %d is %d bytes, over the %d cap with %d points", i, sz, maxBytes, p.DataPointCount())
				}
				// Every part carries the full metric identity and type.
				if p.ResourceMetrics().Len() != 1 || p.ResourceMetrics().At(0).ScopeMetrics().Len() != 1 {
					t.Fatalf("part %d has unexpected shape", i)
				}
				rm := p.ResourceMetrics().At(0)
				if rm.SchemaUrl() != "https://schema/res" {
					t.Errorf("part %d lost the resource schema URL", i)
				}
				if _, ok := rm.Resource().Attributes().Get("service.name"); !ok {
					t.Errorf("part %d lost the resource attributes", i)
				}
				sm := rm.ScopeMetrics().At(0)
				if sm.SchemaUrl() != "https://schema/scope" || sm.Scope().Name() != "scope-x" {
					t.Errorf("part %d lost the scope identity", i)
				}
				if sm.Metrics().Len() != 1 {
					t.Fatalf("part %d holds %d metrics, want 1", i, sm.Metrics().Len())
				}
				m := sm.Metrics().At(0)
				if m.Name() != "kube_pod_container_status_ready" || m.Unit() != "1" ||
					m.Description() != "Describes whether the containers readiness check succeeded." {
					t.Errorf("part %d lost the metric identity: %q/%q/%q", i, m.Name(), m.Unit(), m.Description())
				}
				if v, ok := m.Metadata().Get("meta.key"); !ok || v.Str() != "meta.value" {
					t.Errorf("part %d lost the metric metadata", i)
				}
				if m.Type() != tc.typ {
					t.Fatalf("part %d changed the metric type: %v want %v", i, m.Type(), tc.typ)
				}
				switch tc.typ {
				case pmetric.MetricTypeSum:
					if !m.Sum().IsMonotonic() || m.Sum().AggregationTemporality() != pmetric.AggregationTemporalityDelta {
						t.Errorf("part %d lost sum monotonic/temporality", i)
					}
				case pmetric.MetricTypeHistogram:
					if m.Histogram().AggregationTemporality() != pmetric.AggregationTemporalityCumulative {
						t.Errorf("part %d lost histogram temporality", i)
					}
				case pmetric.MetricTypeExponentialHistogram:
					if m.ExponentialHistogram().AggregationTemporality() != pmetric.AggregationTemporalityDelta {
						t.Errorf("part %d lost exponential-histogram temporality", i)
					}
				}
				eachPoint(p, func(a pcommon.Map, detail string) {
					v, ok := a.Get("idx")
					if !ok {
						t.Fatalf("part %d has a point without its attributes", i)
					}
					seen[v.Int()]++
					if got := want[v.Int()]; got != detail {
						t.Fatalf("point %d changed across the split: got %q want %q", v.Int(), detail, got)
					}
				})
			}
			if len(seen) != points {
				t.Fatalf("points across the parts: %d distinct, want %d", len(seen), points)
			}
			for idx, n := range seen {
				if n != 1 {
					t.Fatalf("point %d appears %d times across the parts, want exactly 1", idx, n)
				}
			}
			// The input is never mutated.
			if got := md.DataPointCount(); got != points {
				t.Fatalf("input mutated: %d points left, want %d", got, points)
			}
			if got := mar.MetricsSize(md); got != srcSize {
				t.Fatalf("input mutated: size %d, want %d", got, srcSize)
			}
		})
	}
}

// A data point that alone exceeds the cap is emitted alone — never dropped, and
// never bundled with another point.
func TestSplitMetricsSinglePointOverCapGoesAlone(t *testing.T) {
	var mar pmetric.ProtoMarshaler
	const maxBytes = 8 << 10
	for _, points := range []int{1, 3} {
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service.name", "svc")
		sm := rm.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName("huge_gauge")
		dps := m.SetEmptyGauge().DataPoints()
		for i := 0; i < points; i++ {
			d := dps.AppendEmpty()
			d.Attributes().PutInt("idx", int64(i))
			d.Attributes().PutStr("blob", strings.Repeat("z", 40<<10)) // one point > cap
			d.SetDoubleValue(float64(i))
		}
		parts := Metrics(md, maxBytes)
		if len(parts) != points {
			t.Fatalf("%d over-cap points produced %d parts, want %d", points, len(parts), points)
		}
		seen := map[int64]int{}
		for i, p := range parts {
			if p.DataPointCount() != 1 {
				t.Fatalf("part %d holds %d points; an over-cap point must go alone", i, p.DataPointCount())
			}
			if sz := mar.MetricsSize(p); sz <= maxBytes {
				t.Fatalf("part %d is %d bytes; the fixture must exceed the %d cap", i, sz, maxBytes)
			}
			eachPoint(p, func(a pcommon.Map, _ string) {
				v, _ := a.Get("idx")
				seen[v.Int()]++
			})
		}
		if len(seen) != points {
			t.Fatalf("lost points: %d of %d survived", len(seen), points)
		}
	}
}
