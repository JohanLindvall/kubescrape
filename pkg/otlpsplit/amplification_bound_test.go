package otlpsplit

import (
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// The amplification bound for splits that are NOT abandoned and NOT reported:
// the silent path, where nothing counts the parts and the bound is the whole
// defence. minChunkRoomDiv derives it as 1 + k(d-1), k being how many chunks
// one room's worth of real content can be spread over; these are the shapes
// that set k, each asserted under its own bound. Every shape here keeps its
// framing just INSIDE splitPaysOff's threshold — the worst place for the
// bound, since one step further the split is abandoned and costs d at most.
const (
	// nextFitBound is k = 2: next-fit only guarantees two consecutive chunks
	// more than a room between them. The worst shape for logs and spans.
	nextFitBound = 1 + 2*(minChunkRoomDiv-1)
	// logsAmplificationBound is the whole bound for logs (and spans).
	logsAmplificationBound = nextFitBound
	// zeroSizeLeafBound is k = 4: elemOverhead charges an empty data point 8
	// bytes for its 2 real ones, so a "full" chunk of them is a quarter full.
	zeroSizeLeafBound = 1 + 4*(minChunkRoomDiv-1)
	// metricsAmplificationBound is k = 6, the whole bound for metrics: four
	// over-charged chunks per room of empty data points, plus the chunk a
	// data-point split closes before it and its own part-filled last chunk.
	metricsAmplificationBound = 1 + 6*(minChunkRoomDiv-1)
)

// boundCap is small so each shape needs only thousands of leaves; the bound
// is a ratio, so it scales.
const boundCap = 64 << 10

// insideThreshold is a resource attribute value that, with the attribute and
// scope framing around it, leaves a room just above maxBytes/minChunkRoomDiv:
// splitPaysOff still says yes, by a few hundred bytes at most.
func insideThreshold(maxBytes int) string {
	return strings.Repeat("a", maxBytes-maxBytes/minChunkRoomDiv-256)
}

// amplification checks one shape's parts against bound, and that the shape
// really ran on the silent path and really reached past d — a shape that stays
// under d is not exercising what the bound had to be corrected for.
func amplification(t *testing.T, name string, in, total int, rep Report, bound int) {
	t.Helper()
	amp := float64(total) / float64(in)
	t.Logf("%s: in=%d total=%d amplification=%.2fx (bound %dx)", name, in, total, amp, bound)
	if rep != (Report{}) {
		t.Fatalf("%s: the shape took a REPORTED path (%+v); it must pin the silent one", name, rep)
	}
	if total > bound*in {
		t.Errorf("%s: parts total %d bytes for a %d-byte input (%.2fx), over the %dx bound", name, total, in, amp, bound)
	}
	if amp <= minChunkRoomDiv {
		t.Errorf("%s: %.2fx does not reach past minChunkRoomDiv (%d); the shape no longer exercises the bound", name, amp, minChunkRoomDiv)
	}
}

func TestAlternatingLeafSizesStayWithinBound(t *testing.T) {
	const pairs = 60

	t.Run("logs", func(t *testing.T) {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("sender.chosen", insideThreshold(boundCap))
		sl := rl.ScopeLogs().AppendEmpty()
		base := logMarshaler.ResourceLogsSize(emptyScopesRL(rl)) + elemOverhead +
			logMarshaler.ScopeLogsSize(emptyRecordsSL(sl)) + elemOverhead
		if !splitPaysOff(base, boundCap) {
			t.Fatal("the framing must sit inside the threshold")
		}
		room := boundCap - base
		for range pairs {
			sl.LogRecords().AppendEmpty()
			// Just over what the empty record leaves: it cannot join it, and
			// the next empty record cannot join IT.
			sl.LogRecords().AppendEmpty().Body().SetStr(strings.Repeat("b", room-24))
		}
		parts, rep := LogsWithReport(ld, boundCap)
		amplification(t, "logs", logMarshaler.LogsSize(ld), totalLogBytes(parts), rep, logsAmplificationBound)
	})

	t.Run("spans", func(t *testing.T) {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("sender.chosen", insideThreshold(boundCap))
		ss := rs.ScopeSpans().AppendEmpty()
		base := traceMarshaler.ResourceSpansSize(emptyScopesRS(rs)) + elemOverhead +
			traceMarshaler.ScopeSpansSize(emptySpansSS(ss)) + elemOverhead
		if !splitPaysOff(base, boundCap) {
			t.Fatal("the framing must sit inside the threshold")
		}
		room := boundCap - base
		for range pairs {
			ss.Spans().AppendEmpty()
			ss.Spans().AppendEmpty().SetName(strings.Repeat("b", room-28))
		}
		parts, rep := TracesWithReport(td, boundCap)
		var total int
		for _, p := range parts {
			total += traceMarshaler.TracesSize(p)
		}
		amplification(t, "spans", traceMarshaler.TracesSize(td), total, rep, logsAmplificationBound)
	})

	t.Run("datapoints", func(t *testing.T) {
		md, ms, room := metricsInsideThreshold(t)
		g := ms.AppendEmpty()
		g.SetName("g")
		pts := g.SetEmptyGauge().DataPoints()
		// The data-point split's room is what the resource, the scope AND the
		// metric shell leave; the big point is sized to fill it alone.
		shell := pmetric.NewMetric()
		copyMetricShell(g, shell)
		dpRoom := room - metricMarshaler.MetricSize(shell) - elemOverhead
		big := pmetric.NewNumberDataPoint()
		for n := dpRoom; n > 0; n-- {
			big.Attributes().PutStr("k", strings.Repeat("v", n))
			if metricMarshaler.NumberDataPointSize(big)+elemOverhead <= dpRoom-4 {
				break
			}
		}
		for range pairs {
			pts.AppendEmpty()
			big.CopyTo(pts.AppendEmpty())
		}
		parts, rep := MetricsWithReport(md, boundCap)
		amplification(t, "datapoints", metricMarshaler.MetricsSize(md), totalMetricBytes(parts), rep, nextFitBound)
	})
}

// The plainest k = 2 shape, and the one that disproved the old "at most d
// times the input" claim: every leaf is just over HALF the room, so no two fit
// one chunk and each part re-carries the whole framing for half a room of
// content. Each part then costs B + R/2 against B <= (d-1)R, which tends to
// 2d-1 as the leaf count grows — past d, inside nextFitBound.
func TestSplitOutputIsBoundedForHalfRoomLeaves(t *testing.T) {
	const leaves = 60

	t.Run("logs", func(t *testing.T) {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("sender.chosen", insideThreshold(boundCap))
		sl := rl.ScopeLogs().AppendEmpty()
		base := logMarshaler.ResourceLogsSize(emptyScopesRL(rl)) + elemOverhead +
			logMarshaler.ScopeLogsSize(emptyRecordsSL(sl)) + elemOverhead
		if !splitPaysOff(base, boundCap) {
			t.Fatal("the framing must sit inside the threshold")
		}
		room := boundCap - base
		for range leaves {
			sl.LogRecords().AppendEmpty().Body().SetStr(strings.Repeat("b", room/2+64))
		}
		parts, rep := LogsWithReport(ld, boundCap)
		if len(parts) != leaves {
			t.Fatalf("%d parts for %d half-room records; the shape must put exactly one per part", len(parts), leaves)
		}
		amplification(t, "half-room records", logMarshaler.LogsSize(ld), totalLogBytes(parts), rep, nextFitBound)
	})

	t.Run("spans", func(t *testing.T) {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("sender.chosen", insideThreshold(boundCap))
		ss := rs.ScopeSpans().AppendEmpty()
		base := traceMarshaler.ResourceSpansSize(emptyScopesRS(rs)) + elemOverhead +
			traceMarshaler.ScopeSpansSize(emptySpansSS(ss)) + elemOverhead
		if !splitPaysOff(base, boundCap) {
			t.Fatal("the framing must sit inside the threshold")
		}
		room := boundCap - base
		for range leaves {
			ss.Spans().AppendEmpty().SetName(strings.Repeat("b", room/2+64))
		}
		parts, rep := TracesWithReport(td, boundCap)
		if len(parts) != leaves {
			t.Fatalf("%d parts for %d half-room spans; the shape must put exactly one per part", len(parts), leaves)
		}
		var total int
		for _, p := range parts {
			total += traceMarshaler.TracesSize(p)
		}
		amplification(t, "half-room spans", traceMarshaler.TracesSize(td), total, rep, nextFitBound)
	})

	t.Run("datapoints", func(t *testing.T) {
		md, ms, room := metricsInsideThreshold(t)
		g := ms.AppendEmpty()
		g.SetName("g")
		pts := g.SetEmptyGauge().DataPoints()
		// The data-point split's room is what the resource, the scope AND the
		// metric shell leave.
		shell := pmetric.NewMetric()
		copyMetricShell(g, shell)
		dpRoom := room - metricMarshaler.MetricSize(shell) - elemOverhead
		for range leaves {
			pts.AppendEmpty().Attributes().PutStr("k", strings.Repeat("v", dpRoom/2+64))
		}
		parts, rep := MetricsWithReport(md, boundCap)
		if got := totalPoints(parts); got != leaves {
			t.Fatalf("%d data points shipped, want %d", got, leaves)
		}
		for i, p := range parts {
			if n := p.DataPointCount(); n > 1 {
				t.Fatalf("part %d holds %d half-room data points; the shape must put at most one per part", i, n)
			}
		}
		amplification(t, "half-room data points", metricMarshaler.MetricsSize(md), totalMetricBytes(parts), rep, nextFitBound)
	})
}

// An empty data point is charged elemOverhead (8) for 2 real bytes, so a chunk
// the split considers full carries a quarter of its room.
func TestZeroSizeDataPointsStayWithinBound(t *testing.T) {
	md, ms, room := metricsInsideThreshold(t)
	g := ms.AppendEmpty()
	g.SetName("g")
	pts := g.SetEmptyGauge().DataPoints()
	for i := 0; i < 40*room/elemOverhead; i++ { // forty over-charged chunks' worth
		pts.AppendEmpty()
	}
	parts, rep := MetricsWithReport(md, boundCap)
	amplification(t, "zero-size data points", metricMarshaler.MetricsSize(md), totalMetricBytes(parts), rep, zeroSizeLeafBound)
}

// The metrics worst case: a tiny metric before each metric just over the room.
// The data-point split closes the chunk holding the tiny metric, spreads the
// over-room metric's empty points over four over-charged chunks, and leaves its
// own last chunk holding a handful of points.
func TestMetricSplitsStayWithinTheMetricsBound(t *testing.T) {
	md, ms, room := metricsInsideThreshold(t)
	for range 8 {
		ms.AppendEmpty().SetName("t")
		h := ms.AppendEmpty()
		h.SetName("h")
		pts := h.SetEmptyGauge().DataPoints()
		// 2 real bytes each: just over the room in real size, which is what
		// sends the metric to the data-point split.
		for i := 0; i < room/2+8; i++ {
			pts.AppendEmpty()
		}
	}
	parts, rep := MetricsWithReport(md, boundCap)
	amplification(t, "tiny + over-room metrics", metricMarshaler.MetricsSize(md), totalMetricBytes(parts), rep, metricsAmplificationBound)
}

// metricsInsideThreshold is a metrics payload with one resource and one scope
// whose framing sits just inside the threshold, returning the scope's metric
// slice and the room it leaves.
func metricsInsideThreshold(t *testing.T) (pmetric.Metrics, pmetric.MetricSlice, int) {
	t.Helper()
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("sender.chosen", insideThreshold(boundCap))
	sm := rm.ScopeMetrics().AppendEmpty()
	base := metricMarshaler.ResourceMetricsSize(emptyScopesRM(rm)) + elemOverhead +
		metricMarshaler.ScopeMetricsSize(emptyMetricsSM(sm)) + elemOverhead
	if !splitPaysOff(base, boundCap) {
		t.Fatal("the framing must sit inside the threshold")
	}
	return md, sm.Metrics(), boundCap - base
}

func totalMetricBytes(parts []pmetric.Metrics) int {
	total := 0
	for _, p := range parts {
		total += metricMarshaler.MetricsSize(p)
	}
	return total
}

func totalPoints(parts []pmetric.Metrics) int {
	n := 0
	for _, p := range parts {
		n += p.DataPointCount()
	}
	return n
}
