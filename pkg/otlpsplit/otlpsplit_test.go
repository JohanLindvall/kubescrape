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
