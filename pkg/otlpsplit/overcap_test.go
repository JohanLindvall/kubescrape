package otlpsplit

// An identity-bearing EMPTY scope must not blind the leaf-level cap check.
//
// With `held > 0` as the only emit-and-reopen guard, a chunk that had
// accumulated a record-less scope's identity bytes (curBytes past the
// resource+current-scope base with held still 0) took the next scope's first
// leaf unchecked: a 16 KiB empty scope followed by a 60 KiB record emitted a
// ~78 KiB part under a 64 KiB cap. An over-cap part is rejected wholesale by
// the collector — the exact loss this package exists to prevent — so every
// part must stay within the cap with no leaf lost. Same shape in all three
// splitters.

import (
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const (
	overcapMax = 64 << 10
	idPad      = 16 << 10
	leafPad    = 60 << 10
)

func TestEmptyScopeIdentityDoesNotBlindTheLogRecordCap(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "svc")
	empty := rl.ScopeLogs().AppendEmpty()
	empty.Scope().SetName("identity-heavy")
	empty.Scope().Attributes().PutStr("pad", strings.Repeat("a", idPad))
	full := rl.ScopeLogs().AppendEmpty()
	full.Scope().SetName("lib")
	full.LogRecords().AppendEmpty().Body().SetStr(strings.Repeat("x", leafPad))
	if size := logMarshaler.LogsSize(ld); size <= overcapMax {
		t.Fatalf("test payload (%d bytes) does not exceed the cap", size)
	}

	records := 0
	for i, p := range Logs(ld, overcapMax) {
		if got := logMarshaler.LogsSize(p); got > overcapMax {
			t.Errorf("part %d is %d bytes, over the %d cap", i, got, overcapMax)
		}
		records += p.LogRecordCount()
	}
	if records != 1 {
		t.Errorf("parts carry %d records, input had 1", records)
	}
}

func TestEmptyScopeIdentityDoesNotBlindTheMetricCap(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	empty := rm.ScopeMetrics().AppendEmpty()
	empty.Scope().SetName("identity-heavy")
	empty.Scope().Attributes().PutStr("pad", strings.Repeat("a", idPad))
	full := rm.ScopeMetrics().AppendEmpty()
	full.Scope().SetName("lib")
	m := full.Metrics().AppendEmpty()
	m.SetName("fat")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(1)
	dp.Attributes().PutStr("pad", strings.Repeat("x", leafPad))
	if size := metricMarshaler.MetricsSize(md); size <= overcapMax {
		t.Fatalf("test payload (%d bytes) does not exceed the cap", size)
	}

	points := 0
	for i, p := range Metrics(md, overcapMax) {
		if got := metricMarshaler.MetricsSize(p); got > overcapMax {
			t.Errorf("part %d is %d bytes, over the %d cap", i, got, overcapMax)
		}
		points += p.DataPointCount()
	}
	if points != 1 {
		t.Errorf("parts carry %d data points, input had 1", points)
	}
}

func TestEmptyScopeIdentityDoesNotBlindTheSpanCap(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "svc")
	empty := rs.ScopeSpans().AppendEmpty()
	empty.Scope().SetName("identity-heavy")
	empty.Scope().Attributes().PutStr("pad", strings.Repeat("a", idPad))
	full := rs.ScopeSpans().AppendEmpty()
	full.Scope().SetName("lib")
	sp := full.Spans().AppendEmpty()
	sp.SetName("span")
	sp.Attributes().PutStr("pad", strings.Repeat("x", leafPad))
	if size := traceMarshaler.TracesSize(td); size <= overcapMax {
		t.Fatalf("test payload (%d bytes) does not exceed the cap", size)
	}

	spans := 0
	for i, p := range Traces(td, overcapMax) {
		if got := traceMarshaler.TracesSize(p); got > overcapMax {
			t.Errorf("part %d is %d bytes, over the %d cap", i, got, overcapMax)
		}
		spans += p.SpanCount()
	}
	if spans != 1 {
		t.Errorf("parts carry %d spans, input had 1", spans)
	}
}
