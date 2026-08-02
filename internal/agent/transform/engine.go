package transform

// The Starlark engine. Each signal's script defines transform(batch); batch
// iterates lazy host objects (record/span/metric views over pdata), so a
// script pays only for the fields it touches. Starlark is hermetic by
// construction — no I/O, no imports, no clock — and each run gets a fresh
// Thread with a step limit, so a pathological script terminates with an
// error instead of wedging an export goroutine.

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.starlark.net/starlark"
	"go.starlark.net/syntax"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// maxSteps bounds one transform invocation (~a few ms of work). A batch is
// at most a few thousand records; a well-behaved script uses a tiny fraction
// of this.
const maxSteps = 10_000_000

func contentHash(raw []byte) string {
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:8])
}

type starlarkProgram struct {
	signal string
	fn     starlark.Callable
}

// compileStarlark compiles src and resolves its transform() function. The
// compile includes a smoke evaluation of the module (top-level statements
// run), so syntax and load-time errors are caught at config time.
func compileStarlark(signal, src string) (*starlarkProgram, error) {
	opts := &syntax.FileOptions{Set: true, While: true, GlobalReassign: true}
	thread := &starlark.Thread{Name: "compile:" + signal}
	// Bound the smoke evaluation like the run path: a top-level comprehension
	// (e.g. `_ = [x for x in range(1<<40)]`) is a plain expression Starlark
	// runs at module load, with no step or ctx cap otherwise — an operator
	// typo would hang the agent at startup (CompileFile is synchronous in
	// run()) or wedge the reload goroutine forever, breaking the
	// keep-last-good-program guarantee. maxSteps caps it to a config error.
	thread.SetMaxExecutionSteps(maxSteps)
	globals, err := starlark.ExecFileOptions(opts, thread, signal+".star", src, nil)
	if err != nil {
		return nil, fmt.Errorf("transforms %s: %w", signal, err)
	}
	fn, ok := globals["transform"].(starlark.Callable)
	if !ok {
		return nil, fmt.Errorf("transforms %s: script must define transform(batch)", signal)
	}
	globals.Freeze() // shared across export goroutines: must be immutable
	return &starlarkProgram{signal: signal, fn: fn}, nil
}

// run invokes transform(batch) on a fresh bounded thread.
func (p *starlarkProgram) run(batch starlark.Value) error {
	thread := &starlark.Thread{Name: "transform:" + p.signal}
	thread.SetMaxExecutionSteps(maxSteps)
	if _, err := starlark.Call(thread, p.fn, starlark.Tuple{batch}, nil); err != nil {
		obs.TransformErrors.WithLabelValues(p.signal).Inc()
		return fmt.Errorf("transform %s: %w", p.signal, err)
	}
	return nil
}

func (p *starlarkProgram) runLogs(ld plog.Logs) error {
	if err := p.run(&logBatch{ld: ld}); err != nil {
		return err
	}
	p.countDropped(pruneLogs(ld))
	return nil
}

func (p *starlarkProgram) runMetrics(md pmetric.Metrics) error {
	if err := p.run(&metricBatch{md: md}); err != nil {
		return err
	}
	p.countDropped(pruneMetrics(md))
	return nil
}

func (p *starlarkProgram) runTraces(td ptrace.Traces) error {
	if err := p.run(&traceBatch{td: td}); err != nil {
		return err
	}
	p.countDropped(pruneTraces(td))
	return nil
}

// countDropped records what this invocation's drop() calls discarded.
//
// A transform drop is INTENDED loss, which is exactly why it has to be
// counted: the intent lives in an operator-edited file that HOT-RELOADS, so
// nothing about the deploy marks the moment a predicate started matching
// everything. Until now the prune below was silent — a node's logs could go
// with no error logged and no counter moved (see hostobj.go), leaving the
// export path indistinguishable from an idle one.
func (p *starlarkProgram) countDropped(n int) {
	if n > 0 {
		obs.TransformDropped.WithLabelValues(p.signal).Add(float64(n))
	}
}

// prune* remove records marked dropped and any groups left empty, returning
// how many records/points/spans went.
func pruneLogs(ld plog.Logs) int {
	dropped := 0
	rls := ld.ResourceLogs()
	rls.RemoveIf(func(rl plog.ResourceLogs) bool {
		sls := rl.ScopeLogs()
		sls.RemoveIf(func(sl plog.ScopeLogs) bool {
			sl.LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
				_, drop := lr.Attributes().Get(dropMarker)
				if drop {
					lr.Attributes().Remove(dropMarker)
					dropped++
				}
				return drop
			})
			return sl.LogRecords().Len() == 0
		})
		return sls.Len() == 0
	})
	return dropped
}

func pruneMetrics(md pmetric.Metrics) int {
	dropped := 0
	rms := md.ResourceMetrics()
	rms.RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		sms := rm.ScopeMetrics()
		sms.RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			sm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				if _, drop := m.Metadata().Get(dropMarker); drop {
					// A whole metric costs every point it carried: the unit
					// this counter reports is data points, so that a metrics
					// drop is comparable with a logs one.
					dropped += dataPointCount(m)
					return true
				}
				// Points dropped individually. A metric emptied by them goes
				// too: a point-less metric carries no data and would ship as
				// pure descriptor overhead.
				n, empty := pruneDataPoints(m)
				dropped += n
				return empty
			})
			return sm.Metrics().Len() == 0
		})
		return sms.Len() == 0
	})
	return dropped
}

// dataPointCount is one metric's point count across every kind.
func dataPointCount(m pmetric.Metric) int {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		return m.Gauge().DataPoints().Len()
	case pmetric.MetricTypeSum:
		return m.Sum().DataPoints().Len()
	case pmetric.MetricTypeHistogram:
		return m.Histogram().DataPoints().Len()
	case pmetric.MetricTypeExponentialHistogram:
		return m.ExponentialHistogram().DataPoints().Len()
	case pmetric.MetricTypeSummary:
		return m.Summary().DataPoints().Len()
	}
	return 0
}

// pruneDataPoints removes points a script called drop() on and reports how
// many went and whether the metric is left empty. The marker lives in the
// point's own attributes (as for logs and spans); only dropped points ever
// carry it, and they are removed here, so no survivor can ship it. A metric
// where only SOME points were dropped keeps the rest.
func pruneDataPoints(m pmetric.Metric) (dropped int, empty bool) {
	drop := func(attrs pcommon.Map) bool {
		if _, ok := attrs.Get(dropMarker); ok {
			dropped++
			return true
		}
		return false
	}
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		pts := m.Gauge().DataPoints()
		pts.RemoveIf(func(p pmetric.NumberDataPoint) bool { return drop(p.Attributes()) })
		return dropped, pts.Len() == 0
	case pmetric.MetricTypeSum:
		pts := m.Sum().DataPoints()
		pts.RemoveIf(func(p pmetric.NumberDataPoint) bool { return drop(p.Attributes()) })
		return dropped, pts.Len() == 0
	case pmetric.MetricTypeHistogram:
		pts := m.Histogram().DataPoints()
		pts.RemoveIf(func(p pmetric.HistogramDataPoint) bool { return drop(p.Attributes()) })
		return dropped, pts.Len() == 0
	case pmetric.MetricTypeExponentialHistogram:
		pts := m.ExponentialHistogram().DataPoints()
		pts.RemoveIf(func(p pmetric.ExponentialHistogramDataPoint) bool { return drop(p.Attributes()) })
		return dropped, pts.Len() == 0
	case pmetric.MetricTypeSummary:
		pts := m.Summary().DataPoints()
		pts.RemoveIf(func(p pmetric.SummaryDataPoint) bool { return drop(p.Attributes()) })
		return dropped, pts.Len() == 0
	}
	return 0, false
}

func pruneTraces(td ptrace.Traces) int {
	dropped := 0
	rss := td.ResourceSpans()
	rss.RemoveIf(func(rs ptrace.ResourceSpans) bool {
		sss := rs.ScopeSpans()
		sss.RemoveIf(func(ss ptrace.ScopeSpans) bool {
			ss.Spans().RemoveIf(func(sp ptrace.Span) bool {
				_, drop := sp.Attributes().Get(dropMarker)
				if drop {
					sp.Attributes().Remove(dropMarker)
					dropped++
				}
				return drop
			})
			return ss.Spans().Len() == 0
		})
		return sss.Len() == 0
	})
	return dropped
}
