package otlpsplit

// Report is what kubescrape_export_oversize_parts_total{reason="item"|"framing"}
// and its "the collector will reject that part" warning are made of, and it was
// asserted nowhere but as == 0. It was also WRONG in one direction: the packing
// estimate charges elemOverhead (8 bytes) per element against a real 2-5, so in
// a window a few bytes under the cap a part that FITS was counted — and warned
// about — as one the collector would reject (measured: 15 of 61 sizes around a
// 4 KiB cap). The report must count exactly the parts whose REAL encoded size
// is over the cap: no fewer (a loss nobody hears about), no more (a false
// alarm on a part that was delivered).

import (
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const reportCap = 4 << 10

// reportShape builds a payload whose leaf (or framing) size is n and reports
// how many of its parts are really over the cap, and what the split reported.
type reportShape struct {
	name string
	run  func(n int) (over int, rep Report)
}

func overLogs(parts []plog.Logs) (over int) {
	for _, p := range parts {
		if logMarshaler.LogsSize(p) > reportCap {
			over++
		}
	}
	return over
}

func overMetrics(parts []pmetric.Metrics) (over int) {
	for _, p := range parts {
		if metricMarshaler.MetricsSize(p) > reportCap {
			over++
		}
	}
	return over
}

func overTraces(parts []ptrace.Traces) (over int) {
	for _, p := range parts {
		if traceMarshaler.TracesSize(p) > reportCap {
			over++
		}
	}
	return over
}

func reportShapes() []reportShape {
	pad := func(n int) string { return strings.Repeat("x", n) }
	return []reportShape{
		{"logs: two records near the cap", func(n int) (int, Report) {
			ld := plog.NewLogs()
			sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
			sl.LogRecords().AppendEmpty().Body().SetStr(pad(n))
			sl.LogRecords().AppendEmpty().Body().SetStr(pad(n))
			parts, rep := LogsWithReport(ld, reportCap)
			return overLogs(parts), rep
		}},
		{"logs: scope-less resources near the cap", func(n int) (int, Report) {
			ld := plog.NewLogs()
			ld.ResourceLogs().AppendEmpty().Resource().Attributes().PutStr("a", pad(n))
			ld.ResourceLogs().AppendEmpty().Resource().Attributes().PutStr("b", pad(n))
			parts, rep := LogsWithReport(ld, reportCap)
			return overLogs(parts), rep
		}},
		{"logs: abandoned remainder near the cap", func(n int) (int, Report) {
			// Framing past splitPaysOff's threshold, so the split is given up
			// and the remainder ships whole — over the cap or, near it, not.
			// A small resource ahead of it keeps the whole payload over the
			// cap, so the split runs even where the big resource alone fits.
			ld := plog.NewLogs()
			ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr(pad(1000))
			rl := ld.ResourceLogs().AppendEmpty()
			rl.Resource().Attributes().PutStr("sender.chosen", pad(n-600))
			sl := rl.ScopeLogs().AppendEmpty()
			for range 3 {
				sl.LogRecords().AppendEmpty().Body().SetStr(pad(150))
			}
			parts, rep := LogsWithReport(ld, reportCap)
			return overLogs(parts), rep
		}},
		{"metrics: two one-point metrics near the cap", func(n int) (int, Report) {
			md := pmetric.NewMetrics()
			ms := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
			for range 2 {
				m := ms.AppendEmpty()
				m.SetName("m")
				m.SetEmptyGauge().DataPoints().AppendEmpty().Attributes().PutStr("k", pad(n))
			}
			parts, rep := MetricsWithReport(md, reportCap)
			return overMetrics(parts), rep
		}},
		{"metrics: data-point split, points near the cap", func(n int) (int, Report) {
			md := pmetric.NewMetrics()
			m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			m.SetName("m")
			dps := m.SetEmptySum().DataPoints()
			for range 3 {
				dps.AppendEmpty().Attributes().PutStr("k", pad(n))
			}
			parts, rep := MetricsWithReport(md, reportCap)
			return overMetrics(parts), rep
		}},
		{"metrics: abandoned data-point split near the cap", func(n int) (int, Report) {
			// The metric SHELL (a sender's description) past the threshold,
			// behind a small metric so the payload is over the cap throughout.
			md := pmetric.NewMetrics()
			ms := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
			small := ms.AppendEmpty()
			small.SetName("small")
			small.SetEmptyGauge().DataPoints().AppendEmpty().Attributes().PutStr("k", pad(1000))
			m := ms.AppendEmpty()
			m.SetName("m")
			m.SetDescription(pad(n - 600))
			dps := m.SetEmptyGauge().DataPoints()
			for range 3 {
				dps.AppendEmpty().Attributes().PutStr("k", pad(150))
			}
			parts, rep := MetricsWithReport(md, reportCap)
			return overMetrics(parts), rep
		}},
		{"traces: two spans near the cap", func(n int) (int, Report) {
			td := ptrace.NewTraces()
			ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
			ss.Spans().AppendEmpty().SetName(pad(n))
			ss.Spans().AppendEmpty().SetName(pad(n))
			parts, rep := TracesWithReport(td, reportCap)
			return overTraces(parts), rep
		}},
		{"traces: scope-less resources near the cap", func(n int) (int, Report) {
			td := ptrace.NewTraces()
			td.ResourceSpans().AppendEmpty().Resource().Attributes().PutStr("a", pad(n))
			td.ResourceSpans().AppendEmpty().Resource().Attributes().PutStr("b", pad(n))
			parts, rep := TracesWithReport(td, reportCap)
			return overTraces(parts), rep
		}},
	}
}

func TestReportCountsExactlyThePartsOverTheCap(t *testing.T) {
	for _, sh := range reportShapes() {
		t.Run(sh.name, func(t *testing.T) {
			sawOver, sawFit := false, false
			for n := reportCap - 200; n <= reportCap+100; n++ {
				over, rep := sh.run(n)
				if got := rep.Oversize + rep.Abandoned; got != over {
					t.Errorf("leaf size %d: %d parts over the cap, the report counts %d (%+v)", n, over, got, rep)
				}
				if over > 0 {
					sawOver = true
				} else {
					sawFit = true
				}
			}
			// The sweep must straddle the cap, or it proves one side only.
			if !sawOver || !sawFit {
				t.Fatalf("the sweep never produced both a fitting and an over-cap part (over=%v fit=%v); resize the shape", sawOver, sawFit)
			}
		})
	}
}
