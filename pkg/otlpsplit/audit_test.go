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

// ---------------------------------------------------------------------------
// Angle 1: byte-accounting upper bound under adversarial framing.
// Long keys, deeply nested map/slice attribute values so length-prefix varints
// are multi-byte. Every emitted part (except a lone over-cap leaf) must marshal
// to <= maxBytes.
// ---------------------------------------------------------------------------

func fatAttrs(m pcommon.Map, seed, depth int) {
	m.PutStr(fmt.Sprintf("very.long.attribute.key.number.%d.with.dots.and.more", seed), strings.Repeat("V", 137))
	sl := m.PutEmptySlice(fmt.Sprintf("slice.key.%d", seed))
	for i := 0; i < 12; i++ {
		sl.AppendEmpty().SetStr(strings.Repeat("s", 40+i))
	}
	if depth > 0 {
		fatAttrs(m.PutEmptyMap(fmt.Sprintf("nested.map.%d", seed)), seed+1, depth-1)
	}
}

func buildAdversarialLogs(resources, scopesPer, recordsPer, bodyLen int) plog.Logs {
	ld := plog.NewLogs()
	for r := 0; r < resources; r++ {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.SetSchemaUrl(fmt.Sprintf("https://schemas.example.com/resource/v%d", r))
		fatAttrs(rl.Resource().Attributes(), r, 4)
		for sc := 0; sc < scopesPer; sc++ {
			sl := rl.ScopeLogs().AppendEmpty()
			sl.SetSchemaUrl(fmt.Sprintf("https://schemas.example.com/scope/v%d", sc))
			sl.Scope().SetName(fmt.Sprintf("scope-with-a-longish-instrumentation-name-%d", sc))
			sl.Scope().SetVersion("v1.2.3-adversarial")
			fatAttrs(sl.Scope().Attributes(), sc*7, 3)
			for i := 0; i < recordsPer; i++ {
				lr := sl.LogRecords().AppendEmpty()
				lr.Body().SetStr(strings.Repeat("x", bodyLen))
				fatAttrs(lr.Attributes(), i, 3)
			}
		}
	}
	return ld
}

func TestAuditLogsUpperBoundStress(t *testing.T) {
	var m plog.ProtoMarshaler
	caps := []int{2 << 10, 4 << 10, 8 << 10, 16 << 10, 37 << 10, 100 << 10}
	shapes := []struct{ res, sc, rec, body int }{
		{50, 1, 1, 30},
		{1, 1, 4000, 25},
		{1, 8, 300, 60},
		{20, 3, 40, 300},
		{5, 2, 10, 1200},
	}
	for _, cp := range caps {
		for _, sh := range shapes {
			ld := buildAdversarialLogs(sh.res, sh.sc, sh.rec, sh.body)
			parts := Logs(ld, cp)
			for i, p := range parts {
				sz := m.LogsSize(p)
				single := p.ResourceLogs().Len() == 1 &&
					p.ResourceLogs().At(0).ScopeLogs().Len() == 1 &&
					p.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().Len() == 1
				if sz > cp && !single {
					t.Errorf("cap=%d shape=%v part %d = %d bytes, OVER cap (framing undercounted)", cp, sh, i, sz)
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Angle 2: data preservation — metrics (all 5 datapoint types) and traces
// (events/links). No point lost/duplicated; resource+scope attrs and SchemaUrl
// copied onto every chunk.
// ---------------------------------------------------------------------------

func buildAllTypeMetrics(resources, perType int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	for r := 0; r < resources; r++ {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.SetSchemaUrl("https://schema/res")
		rm.Resource().Attributes().PutStr("service.name", fmt.Sprintf("svc-%d", r))
		rm.Resource().Attributes().PutStr("k8s.node.name", "node-01")
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.SetSchemaUrl("https://schema/scope")
		sm.Scope().SetName("scope-x")
		sm.Scope().Attributes().PutStr("scope.attr", "keepme")
		for i := 0; i < perType; i++ {
			g := sm.Metrics().AppendEmpty()
			g.SetName(fmt.Sprintf("gauge_%d_%d", r, i))
			gp := g.SetEmptyGauge().DataPoints().AppendEmpty()
			gp.SetDoubleValue(float64(i))
			gp.Attributes().PutStr("l", strings.Repeat("g", 30))

			s := sm.Metrics().AppendEmpty()
			s.SetName(fmt.Sprintf("sum_%d_%d", r, i))
			sp := s.SetEmptySum().DataPoints().AppendEmpty()
			sp.SetIntValue(int64(i))
			sp.Attributes().PutStr("l", strings.Repeat("s", 30))

			h := sm.Metrics().AppendEmpty()
			h.SetName(fmt.Sprintf("hist_%d_%d", r, i))
			hp := h.SetEmptyHistogram().DataPoints().AppendEmpty()
			hp.BucketCounts().FromRaw([]uint64{1, 2, 3, 4})
			hp.ExplicitBounds().FromRaw([]float64{1, 2, 3})
			hp.Attributes().PutStr("l", strings.Repeat("h", 30))

			e := sm.Metrics().AppendEmpty()
			e.SetName(fmt.Sprintf("exp_%d_%d", r, i))
			ep := e.SetEmptyExponentialHistogram().DataPoints().AppendEmpty()
			ep.Positive().BucketCounts().FromRaw([]uint64{1, 2, 3})
			ep.Attributes().PutStr("l", strings.Repeat("e", 30))

			su := sm.Metrics().AppendEmpty()
			su.SetName(fmt.Sprintf("summ_%d_%d", r, i))
			qp := su.SetEmptySummary().DataPoints().AppendEmpty()
			qv := qp.QuantileValues().AppendEmpty()
			qv.SetQuantile(0.99)
			qv.SetValue(float64(i))
			qp.Attributes().PutStr("l", strings.Repeat("q", 30))
		}
	}
	return md
}

func TestAuditMetricsPreservationAllTypes(t *testing.T) {
	md := buildAllTypeMetrics(4, 120)
	wantPoints := md.DataPointCount()
	wantMetrics := 0
	names := map[string]int{}
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ms := rm.ScopeMetrics().At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				names[ms.At(k).Name()]++
				wantMetrics++
			}
		}
	}
	parts := Metrics(md, 16<<10)
	if len(parts) < 2 {
		t.Fatalf("expected a split, got %d parts", len(parts))
	}
	gotPoints, gotMetrics := 0, 0
	gotNames := map[string]int{}
	for _, p := range parts {
		for i := 0; i < p.ResourceMetrics().Len(); i++ {
			rm := p.ResourceMetrics().At(i)
			if rm.SchemaUrl() != "https://schema/res" {
				t.Errorf("resource SchemaUrl lost: %q", rm.SchemaUrl())
			}
			if _, ok := rm.Resource().Attributes().Get("service.name"); !ok {
				t.Errorf("resource attr service.name lost")
			}
			for j := 0; j < rm.ScopeMetrics().Len(); j++ {
				sm := rm.ScopeMetrics().At(j)
				if sm.SchemaUrl() != "https://schema/scope" {
					t.Errorf("scope SchemaUrl lost: %q", sm.SchemaUrl())
				}
				if v, ok := sm.Scope().Attributes().Get("scope.attr"); !ok || v.Str() != "keepme" {
					t.Errorf("scope attr lost")
				}
				ms := sm.Metrics()
				for k := 0; k < ms.Len(); k++ {
					gotNames[ms.At(k).Name()]++
					gotMetrics++
				}
			}
		}
		gotPoints += p.DataPointCount()
	}
	if gotPoints != wantPoints {
		t.Fatalf("datapoints: got %d want %d", gotPoints, wantPoints)
	}
	if gotMetrics != wantMetrics {
		t.Fatalf("metrics: got %d want %d", gotMetrics, wantMetrics)
	}
	for n, c := range names {
		if gotNames[n] != c {
			t.Fatalf("metric %q count changed: got %d want %d (lost/dup)", n, gotNames[n], c)
		}
	}
}

func TestAuditTracesPreservationEventsLinks(t *testing.T) {
	td := ptrace.NewTraces()
	total := 0
	names := map[string]int{}
	for r := 0; r < 3; r++ {
		rs := td.ResourceSpans().AppendEmpty()
		rs.SetSchemaUrl("https://schema/rs")
		rs.Resource().Attributes().PutStr("service.name", fmt.Sprintf("svc-%d", r))
		ss := rs.ScopeSpans().AppendEmpty()
		ss.SetSchemaUrl("https://schema/ss")
		ss.Scope().Attributes().PutStr("scope.attr", "keepme")
		for i := 0; i < 200; i++ {
			sp := ss.Spans().AppendEmpty()
			nm := fmt.Sprintf("op-%d-%d", r, i)
			sp.SetName(nm)
			names[nm]++
			total++
			sp.SetTraceID(pcommon.TraceID([16]byte{byte(i), byte(r), 9}))
			sp.SetSpanID(pcommon.SpanID([8]byte{byte(i), 7}))
			ev := sp.Events().AppendEmpty()
			ev.SetName("evt")
			ev.Attributes().PutStr("k", strings.Repeat("z", 40))
			lk := sp.Links().AppendEmpty()
			lk.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3}))
			lk.Attributes().PutStr("lk", strings.Repeat("w", 40))
		}
	}
	parts := Traces(td, 12<<10)
	if len(parts) < 2 {
		t.Fatalf("expected split, got %d parts", len(parts))
	}
	got := 0
	gotNames := map[string]int{}
	for _, p := range parts {
		for i := 0; i < p.ResourceSpans().Len(); i++ {
			rs := p.ResourceSpans().At(i)
			if rs.SchemaUrl() != "https://schema/rs" {
				t.Errorf("rs schema lost")
			}
			for j := 0; j < rs.ScopeSpans().Len(); j++ {
				ss := rs.ScopeSpans().At(j)
				if ss.SchemaUrl() != "https://schema/ss" {
					t.Errorf("ss schema lost")
				}
				if _, ok := ss.Scope().Attributes().Get("scope.attr"); !ok {
					t.Errorf("scope attr lost")
				}
				for k := 0; k < ss.Spans().Len(); k++ {
					sp := ss.Spans().At(k)
					if sp.Events().Len() != 1 || sp.Links().Len() != 1 {
						t.Errorf("span %q lost events/links: ev=%d lk=%d", sp.Name(), sp.Events().Len(), sp.Links().Len())
					}
					gotNames[sp.Name()]++
					got++
				}
			}
		}
	}
	if got != total {
		t.Fatalf("spans: got %d want %d", got, total)
	}
	for n, c := range names {
		if gotNames[n] != c {
			t.Fatalf("span %q count changed", n)
		}
	}
}

// ---------------------------------------------------------------------------
// Angle 2/4: scope attributes preserved on every chunk of a within-resource
// split (a dropped scope attr changes stream identity).
// ---------------------------------------------------------------------------

func TestAuditScopeAttrsPreservedOnBigSplit(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.SetSchemaUrl("https://res/schema")
	rl.Resource().Attributes().PutStr("service.name", "svc")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.SetSchemaUrl("https://scope/schema")
	sl.Scope().SetName("logger")
	sl.Scope().Attributes().PutStr("scope.identity", "critical")
	for i := 0; i < 3000; i++ {
		sl.LogRecords().AppendEmpty().Body().SetStr(strings.Repeat("x", 40))
	}
	parts := Logs(ld, 8<<10)
	if len(parts) < 2 {
		t.Fatalf("expected split, got %d", len(parts))
	}
	for i, p := range parts {
		s := p.ResourceLogs().At(0).ScopeLogs().At(0)
		if p.ResourceLogs().At(0).SchemaUrl() != "https://res/schema" {
			t.Errorf("part %d: resource schema lost", i)
		}
		if s.SchemaUrl() != "https://scope/schema" {
			t.Errorf("part %d: scope schema lost", i)
		}
		if v, ok := s.Scope().Attributes().Get("scope.identity"); !ok || v.Str() != "critical" {
			t.Errorf("part %d: scope attr lost", i)
		}
	}
}

// ---------------------------------------------------------------------------
// Angle 4: degenerate inputs.
// ---------------------------------------------------------------------------

func TestAuditDegenerateInputs(t *testing.T) {
	// Empty logs, over-cap disabled? within cap → single value.
	if got := Logs(plog.NewLogs(), 4096); len(got) != 1 || got[0].ResourceLogs().Len() != 0 {
		t.Fatalf("empty logs: got %d parts", len(got))
	}
	// ResourceLogs with zero ScopeLogs (small) survives as one part.
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().Resource().Attributes().PutStr("k", "v")
	if got := Logs(ld, 4096); len(got) != 1 || got[0].ResourceLogs().Len() != 1 {
		t.Fatalf("zero-scope resource dropped: %d parts", len(got))
	}
	// maxBytes=1: an over-cap payload, every record alone (one per part).
	big := buildLogs(2, 20, 40)
	parts := Logs(big, 1)
	for i, p := range parts {
		if n := p.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().Len(); n != 1 {
			t.Fatalf("maxBytes=1: part %d holds %d records, want 1", i, n)
		}
	}
	if got := len(collectBodies(parts)); got != 40 {
		t.Fatalf("maxBytes=1 lost/dup records: got %d want 40", got)
	}
}

// ---------------------------------------------------------------------------
// Angle 6: fast path returns the SAME value (no copy) when within cap.
// ---------------------------------------------------------------------------

func TestAuditFastPathNoCopy(t *testing.T) {
	ld := buildLogs(3, 3, 10)
	parts := Logs(ld, 1<<20)
	if len(parts) != 1 {
		t.Fatalf("within-cap split into %d parts", len(parts))
	}
	// Same underlying value: mutating the returned part is visible on the input.
	parts[0].ResourceLogs().At(0).Resource().Attributes().PutStr("mutated", "yes")
	if _, ok := ld.ResourceLogs().At(0).Resource().Attributes().Get("mutated"); !ok {
		t.Fatalf("fast path copied instead of returning the same value")
	}
}

// ---------------------------------------------------------------------------
// BUG angle 4: a byte-oversized Logs whose only content is record-less scopes
// splits to ZERO parts, so exportLogsOnce reports success without sending
// anything (the value is silently "accepted"). Contrast with the under-cap
// path, which copies the whole ResourceLogs (empty scopes included).
// ---------------------------------------------------------------------------

func TestOverCapRecordlessResourceSentWhole(t *testing.T) {
	const maxBytes = 4 << 10
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	// One giant resource attribute pushes the resource over the cap.
	rl.Resource().Attributes().PutStr("huge", strings.Repeat("H", 8<<10))
	rl.ScopeLogs().AppendEmpty().Scope().SetName("record-less-scope")

	if (&plog.ProtoMarshaler{}).LogsSize(ld) <= maxBytes {
		t.Fatalf("test precondition: payload must exceed cap")
	}
	// A non-empty input must never yield zero parts (that would report the
	// export delivered while sending nothing): the over-cap record-less resource
	// is sent whole — rejected and counted at the collector, never dropped.
	parts := Logs(ld, maxBytes)
	if len(parts) != 1 {
		t.Fatalf("over-cap record-less resource produced %d parts, want 1 (sent whole, never silently dropped)", len(parts))
	}
}

// ---------------------------------------------------------------------------
// BUG angle 2/4: within the big-resource split, a record-less ScopeLogs that
// sits among non-empty scopes is dropped entirely (scope + its attrs), whereas
// the under-cap path preserves it. Identity-bearing empty scopes vanish only
// when their resource happens to exceed the cap.
// ---------------------------------------------------------------------------

func TestBigSplitPreservesEmptyScope(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "svc")

	// scope A: record-less but identity-bearing.
	sa := rl.ScopeLogs().AppendEmpty()
	sa.Scope().SetName("scope-A-empty-but-meaningful")
	sa.Scope().Attributes().PutStr("marker", "A")

	// scope B: enough records to push the resource over the cap.
	sb := rl.ScopeLogs().AppendEmpty()
	sb.Scope().SetName("scope-B")
	for i := 0; i < 3000; i++ {
		sb.LogRecords().AppendEmpty().Body().SetStr(strings.Repeat("x", 40))
	}

	countScopeA := func(parts []plog.Logs) int {
		n := 0
		for _, p := range parts {
			for i := 0; i < p.ResourceLogs().Len(); i++ {
				r := p.ResourceLogs().At(i)
				for j := 0; j < r.ScopeLogs().Len(); j++ {
					if r.ScopeLogs().At(j).Scope().Name() == "scope-A-empty-but-meaningful" {
						n++
					}
				}
			}
		}
		return n
	}

	// Under cap: scope A survives.
	if got := countScopeA(Logs(ld, 1<<20)); got != 1 {
		t.Fatalf("under-cap path unexpectedly dropped empty scope A (%d)", got)
	}
	// Over cap: scope A is dropped by splitBigResourceLogs.
	if got := countScopeA(Logs(ld, 8<<10)); got == 0 {
		t.Fatalf("BUG: over-cap split dropped record-less scope A (its scope attrs/identity); under-cap path keeps it")
	}
}

// A scope-less resource whose attributes alone exceed the cap must still ship
// (as its own part) when OTHER resources share the payload — the len(out)==0
// guard only covers the single-resource case.
func TestSplitScopelessOverCapResourceInMixedPayloadShips(t *testing.T) {
	ld := plog.NewLogs()
	// Normal resource with a record.
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "ok")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hello")
	// Scope-less resource with attributes far past the cap.
	big := ld.ResourceLogs().AppendEmpty()
	big.Resource().Attributes().PutStr("huge", strings.Repeat("x", 4096))

	parts := Logs(ld, 1024)
	total := 0
	sawBig := false
	for _, p := range parts {
		rls := p.ResourceLogs()
		total += rls.Len()
		for i := 0; i < rls.Len(); i++ {
			if _, ok := rls.At(i).Resource().Attributes().Get("huge"); ok {
				sawBig = true
			}
		}
	}
	if total != 2 || !sawBig {
		t.Fatalf("scope-less over-cap resource dropped from mixed payload: %d resources across %d parts, big=%v", total, len(parts), sawBig)
	}
}
