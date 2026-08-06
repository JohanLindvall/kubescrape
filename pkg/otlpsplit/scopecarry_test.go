package otlpsplit

// Part-count regressions for the big-resource split.
//
// Both properties are about the SCOPE loop, and both were measured as
// amplification against the ideal ceil(bytes/cap):
//
//  1. a chunk carries across scope boundaries, so the part count is not
//     max(ceil(bytes/cap), scopes) — the OTel-SDK shape (one ScopeLogs per
//     instrumentation library) hit exactly that;
//  2. the per-chunk fixed cost counts the CURRENT scope only, not every scope
//     of the resource — once that over-count exceeded the cap the
//     always-take-one guard made every single leaf its own part.
//
// Both are performance, not correctness: the assertions here bound the part
// count while the shared invariants (every part within the cap, no leaf lost)
// are re-checked alongside so a "fix" cannot buy parts with over-cap chunks.

import (
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// idealParts is the floor any splitter can reach: the payload's bytes over the
// cap. The tests allow generous slack over it — the point is the ORDER, not an
// exact packing.
func idealParts(size, maxBytes int) int { return (size + maxBytes - 1) / maxBytes }

// --- 1. chunks carry across scope boundaries ---

func TestSplitLogsCarriesChunksAcrossScopes(t *testing.T) {
	const (
		scopes     = 64
		perScope   = 8
		bodyBytes  = 4000
		maxBytes   = 512 << 10
		slackParts = 2 // packing slack over the byte-ideal
	)
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "svc")
	for s := 0; s < scopes; s++ {
		sl := rl.ScopeLogs().AppendEmpty()
		sl.Scope().SetName(fmt.Sprintf("io.opentelemetry.instrumentation.lib-%d", s))
		for i := 0; i < perScope; i++ {
			sl.LogRecords().AppendEmpty().Body().SetStr(strings.Repeat("x", bodyBytes))
		}
	}
	size := logMarshaler.LogsSize(ld)
	if size <= maxBytes {
		t.Fatalf("test payload (%d bytes) does not exceed the cap", size)
	}
	parts := Logs(ld, maxBytes)
	want := idealParts(size, maxBytes) + slackParts
	if len(parts) > want {
		t.Errorf("%d parts for %d bytes at a %d cap (%d scopes); want <= %d — a scope boundary must not be a part boundary",
			len(parts), size, maxBytes, scopes, want)
	}
	records := 0
	for i, p := range parts {
		if got := logMarshaler.LogsSize(p); got > maxBytes {
			t.Errorf("part %d is %d bytes, over the %d cap", i, got, maxBytes)
		}
		records += p.LogRecordCount()
	}
	if want := scopes * perScope; records != want {
		t.Errorf("parts carry %d records, input had %d", records, want)
	}
}

func TestSplitTracesCarriesChunksAcrossScopes(t *testing.T) {
	const (
		scopes    = 48
		perScope  = 8
		padBytes  = 4000
		maxBytes  = 512 << 10
		slackPart = 2
	)
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "svc")
	for s := 0; s < scopes; s++ {
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName(fmt.Sprintf("lib-%d", s))
		for i := 0; i < perScope; i++ {
			sp := ss.Spans().AppendEmpty()
			sp.SetName("span")
			sp.Attributes().PutStr("pad", strings.Repeat("x", padBytes))
		}
	}
	size := traceMarshaler.TracesSize(td)
	if size <= maxBytes {
		t.Fatalf("test payload (%d bytes) does not exceed the cap", size)
	}
	parts := Traces(td, maxBytes)
	if want := idealParts(size, maxBytes) + slackPart; len(parts) > want {
		t.Errorf("%d parts for %d bytes at a %d cap (%d scopes); want <= %d",
			len(parts), size, maxBytes, scopes, want)
	}
	spans := 0
	for i, p := range parts {
		if got := traceMarshaler.TracesSize(p); got > maxBytes {
			t.Errorf("part %d is %d bytes, over the %d cap", i, got, maxBytes)
		}
		spans += p.SpanCount()
	}
	if want := scopes * perScope; spans != want {
		t.Errorf("parts carry %d spans, input had %d", spans, want)
	}
}

func TestSplitMetricsCarriesChunksAcrossScopes(t *testing.T) {
	const (
		scopes    = 48
		perScope  = 8
		padBytes  = 4000
		maxBytes  = 512 << 10
		slackPart = 2
	)
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	for s := 0; s < scopes; s++ {
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName(fmt.Sprintf("lib-%d", s))
		for i := 0; i < perScope; i++ {
			m := sm.Metrics().AppendEmpty()
			m.SetName("m")
			dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
			dp.SetDoubleValue(1)
			dp.Attributes().PutStr("pad", strings.Repeat("x", padBytes))
		}
	}
	size := metricMarshaler.MetricsSize(md)
	if size <= maxBytes {
		t.Fatalf("test payload (%d bytes) does not exceed the cap", size)
	}
	parts := Metrics(md, maxBytes)
	if want := idealParts(size, maxBytes) + slackPart; len(parts) > want {
		t.Errorf("%d parts for %d bytes at a %d cap (%d scopes); want <= %d",
			len(parts), size, maxBytes, scopes, want)
	}
	points := 0
	for i, p := range parts {
		if got := metricMarshaler.MetricsSize(p); got > maxBytes {
			t.Errorf("part %d is %d bytes, over the %d cap", i, got, maxBytes)
		}
		points += p.DataPointCount()
	}
	if want := scopes * perScope; points != want {
		t.Errorf("parts carry %d data points, input had %d", points, want)
	}
}

// --- 2. the per-chunk base counts the current scope only ---

// buildFatScopeLogs is the shape a `logAttributes` rule with `target: scope`
// on a long field produces: many scopes, each carrying a large scope
// ATTRIBUTE, with ordinary records. Measured from a copy holding every scope's
// framing, the per-chunk base exceeded the cap outright and every record
// became its own part.
func buildFatScopeLogs(scopes, perScope, scopeAttrBytes, bodyBytes int) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "svc")
	for s := 0; s < scopes; s++ {
		sl := rl.ScopeLogs().AppendEmpty()
		sl.Scope().SetName(fmt.Sprintf("scope-%d", s))
		sl.Scope().Attributes().PutStr("scope.pad", strings.Repeat("s", scopeAttrBytes))
		for i := 0; i < perScope; i++ {
			sl.LogRecords().AppendEmpty().Body().SetStr(strings.Repeat("x", bodyBytes))
		}
	}
	return ld
}

func TestSplitLogsBaseCountsOnlyTheCurrentScope(t *testing.T) {
	const (
		scopes   = 400
		perScope = 5
		maxBytes = 64 << 10
	)
	ld := buildFatScopeLogs(scopes, perScope, 1000, 64)
	size := logMarshaler.LogsSize(ld)
	if size <= maxBytes {
		t.Fatalf("test payload (%d bytes) does not exceed the cap", size)
	}
	// The old base — every scope's framing — alone exceeded the cap, which is
	// what turned the always-take-one guard into one part per record.
	parts := Logs(ld, maxBytes)
	if want := idealParts(size, maxBytes) + 2; len(parts) > want {
		t.Errorf("%d parts for %d bytes at a %d cap; want <= %d (a chunk must be charged for the scope it holds, not for every scope of the resource)",
			len(parts), size, maxBytes, want)
	}
	records := 0
	for i, p := range parts {
		if got := logMarshaler.LogsSize(p); got > maxBytes {
			t.Errorf("part %d is %d bytes, over the %d cap", i, got, maxBytes)
		}
		records += p.LogRecordCount()
	}
	if want := scopes * perScope; records != want {
		t.Errorf("parts carry %d records, input had %d", records, want)
	}
}
