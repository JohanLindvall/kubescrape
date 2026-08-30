package otlpsplit

import (
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// The attack these tests keep refuted, from the third adversarial review.
//
// Any pod in the cluster reaches the agent's unauthenticated OTLP listeners
// (-ingest) or the trace tier's application ports. One push whose SINGLE
// resource carries one attribute a few bytes under -otlp-max-send-bytes, plus
// half a million empty records, passes every admission bound (bytes, decoded
// estimate, nesting depth) and is forwarded verbatim to the exporter. The
// splitter used to re-copy that near-cap resource into a chunk PER RECORD:
// measured at 100,000 parts and 366 GiB of marshal+gzip+send from a 4.5 MiB
// push, all on the goroutine holding the sender's in-flight slot, with the
// parts slice materialised in full before the first send.
//
// The bound is amplification, not part count: parts may total at most
// minChunkRoomDiv times the input, whatever shape the sender chose.

// nearCapAttr is a value that leaves less than one chunk's worth of room under
// the cap once it is framed — the attacker's choice.
func nearCapAttr(maxBytes int) string { return strings.Repeat("a", maxBytes-2048) }

func totalLogBytes(parts []plog.Logs) int {
	var m plog.ProtoMarshaler
	total := 0
	for _, p := range parts {
		total += m.LogsSize(p)
	}
	return total
}

func TestNearCapResourceFramingDoesNotAmplifyLogs(t *testing.T) {
	const maxBytes = 1 << 20
	const records = 20000

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("sender.chosen", nearCapAttr(maxBytes))
	sl := rl.ScopeLogs().AppendEmpty()
	for i := 0; i < records; i++ {
		sl.LogRecords().AppendEmpty()
	}

	var m plog.ProtoMarshaler
	in := m.LogsSize(ld)
	parts, rep := LogsWithReport(ld, maxBytes)

	// Before the splitPaysOff guard this was `records` parts and ~20 GiB.
	if total := totalLogBytes(parts); total > minChunkRoomDiv*in {
		t.Fatalf("%d parts totalling %d bytes for a %d-byte input (%.0fx, bound %dx)",
			len(parts), total, in, float64(total)/float64(in), minChunkRoomDiv)
	}
	if rep.Abandoned != 1 {
		t.Fatalf("abandoned split not reported: %+v (parts=%d)", rep, len(parts))
	}
	// Nothing is lost: the remainder ships whole and is rejected at the
	// collector, which is what the counter and its warning are for.
	got := 0
	for _, p := range parts {
		for i := 0; i < p.ResourceLogs().Len(); i++ {
			for j := 0; j < p.ResourceLogs().At(i).ScopeLogs().Len(); j++ {
				got += p.ResourceLogs().At(i).ScopeLogs().At(j).LogRecords().Len()
			}
		}
	}
	if got != records {
		t.Fatalf("records lost or duplicated: got %d want %d", got, records)
	}
}

// The scope identity is re-copied per chunk exactly like the resource, so a
// modest resource plus one near-cap SCOPE name degenerates the same way.
func TestNearCapScopeFramingDoesNotAmplifyLogs(t *testing.T) {
	const maxBytes = 1 << 20
	const records = 20000

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("k8s.namespace.name", "team-a")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().Attributes().PutStr("sender.chosen", nearCapAttr(maxBytes))
	for i := 0; i < records; i++ {
		sl.LogRecords().AppendEmpty()
	}

	var m plog.ProtoMarshaler
	in := m.LogsSize(ld)
	parts, rep := LogsWithReport(ld, maxBytes)
	if total := totalLogBytes(parts); total > minChunkRoomDiv*in {
		t.Fatalf("%d parts totalling %d bytes for a %d-byte input (%.0fx, bound %dx)",
			len(parts), total, in, float64(total)/float64(in), minChunkRoomDiv)
	}
	if rep.Abandoned != 1 {
		t.Fatalf("abandoned split not reported: %+v (parts=%d)", rep, len(parts))
	}
}

func TestNearCapResourceFramingDoesNotAmplifyTraces(t *testing.T) {
	const maxBytes = 1 << 20
	const spans = 20000

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("sender.chosen", nearCapAttr(maxBytes))
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < spans; i++ {
		ss.Spans().AppendEmpty()
	}

	var m ptrace.ProtoMarshaler
	in := m.TracesSize(td)
	parts, rep := TracesWithReport(td, maxBytes)
	total := 0
	for _, p := range parts {
		total += m.TracesSize(p)
	}
	if total > minChunkRoomDiv*in {
		t.Fatalf("%d parts totalling %d bytes for a %d-byte input (%.0fx, bound %dx)",
			len(parts), total, in, float64(total)/float64(in), minChunkRoomDiv)
	}
	if rep.Abandoned != 1 {
		t.Fatalf("abandoned split not reported: %+v (parts=%d)", rep, len(parts))
	}
}

func TestNearCapResourceFramingDoesNotAmplifyMetrics(t *testing.T) {
	const maxBytes = 1 << 20
	const points = 20000

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("sender.chosen", nearCapAttr(maxBytes))
	sm := rm.ScopeMetrics().AppendEmpty()
	g := sm.Metrics().AppendEmpty()
	g.SetName("q")
	dps := g.SetEmptyGauge().DataPoints()
	for i := 0; i < points; i++ {
		dps.AppendEmpty().SetIntValue(int64(i))
	}

	var m pmetric.ProtoMarshaler
	in := m.MetricsSize(md)
	parts, rep := MetricsWithReport(md, maxBytes)
	total := 0
	for _, p := range parts {
		total += m.MetricsSize(p)
	}
	if total > minChunkRoomDiv*in {
		t.Fatalf("%d parts totalling %d bytes for a %d-byte input (%.0fx, bound %dx)",
			len(parts), total, in, float64(total)/float64(in), minChunkRoomDiv)
	}
	if rep.Abandoned == 0 {
		t.Fatalf("abandoned split not reported: %+v (parts=%d)", rep, len(parts))
	}
}

// The data-point splitter re-copies the metric SHELL per chunk, and on a
// pushed payload the description is the sender's string — so the same
// degeneration is reachable with a small resource and a fat descriptor.
func TestNearCapMetricShellDoesNotAmplify(t *testing.T) {
	const maxBytes = 1 << 20
	const points = 20000

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("k8s.namespace.name", "team-a")
	sm := rm.ScopeMetrics().AppendEmpty()
	g := sm.Metrics().AppendEmpty()
	g.SetName("q")
	g.SetDescription(nearCapAttr(maxBytes))
	dps := g.SetEmptyGauge().DataPoints()
	for i := 0; i < points; i++ {
		dps.AppendEmpty().SetIntValue(int64(i))
	}

	var m pmetric.ProtoMarshaler
	in := m.MetricsSize(md)
	parts, rep := MetricsWithReport(md, maxBytes)
	total := 0
	for _, p := range parts {
		total += m.MetricsSize(p)
	}
	if total > minChunkRoomDiv*in {
		t.Fatalf("%d parts totalling %d bytes for a %d-byte input (%.0fx, bound %dx)",
			len(parts), total, in, float64(total)/float64(in), minChunkRoomDiv)
	}
	if rep.Abandoned == 0 {
		t.Fatalf("abandoned split not reported: %+v (parts=%d)", rep, len(parts))
	}
	// Every data point still ships.
	got := 0
	for _, p := range parts {
		got += p.DataPointCount()
	}
	if got != points {
		t.Fatalf("data points lost or duplicated: got %d want %d", got, points)
	}
}

// A payload whose framing is comfortably under the cap must still split
// normally — the guard must not be reachable by an honest producer, whose
// resource attributes are Kubernetes metadata and whose parts are the delivery
// this package exists for.
func TestOrdinaryFramingStillSplitsCleanly(t *testing.T) {
	const maxBytes = 1 << 20

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	// A generous but realistic resource: ~16 KiB of annotations against a
	// 1 MiB cap (in production the cap is 3.75 MiB).
	rl.Resource().Attributes().PutStr("k8s.pod.annotation.config", strings.Repeat("y", 16<<10))
	sl := rl.ScopeLogs().AppendEmpty()
	for i := 0; i < 4000; i++ {
		sl.LogRecords().AppendEmpty().Body().SetStr(strings.Repeat("z", 2000))
	}

	var m plog.ProtoMarshaler
	parts, rep := LogsWithReport(ld, maxBytes)
	if rep.Abandoned != 0 || rep.Oversize != 0 {
		t.Fatalf("guard fired on an ordinary payload: %+v", rep)
	}
	if len(parts) < 2 {
		t.Fatalf("expected a real split, got %d parts", len(parts))
	}
	for i, p := range parts {
		if sz := m.LogsSize(p); sz > maxBytes {
			t.Fatalf("part %d = %d bytes, over the %d cap", i, sz, maxBytes)
		}
	}
}
