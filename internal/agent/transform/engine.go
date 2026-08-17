package transform

// The Starlark engine. Each signal's script defines transform(batch); batch
// iterates lazy host objects (record/span/metric views over pdata), so a
// script pays only for the fields it touches. Starlark is hermetic by
// construction — no I/O, no imports, no clock — and every invocation, the
// compile-time module evaluation included, runs under the budget in limits.go
// (steps, wall clock, and the per-value/per-invocation size caps the operator
// amplifiers are rewritten into), so a pathological script terminates with an
// error instead of wedging an export goroutine or OOM-killing the process.

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.starlark.net/starlark"
	"go.starlark.net/syntax"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

func contentHash(raw []byte) string {
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:8])
}

type starlarkProgram struct {
	signal string
	fn     starlark.Callable
	// print routes the universe's print() into the throttled script log; built
	// once per program so no invocation allocates a closure for it.
	print func(*starlark.Thread, string)
	// threads pools armed threads. The parse hook runs ONE invocation PER
	// LINE, where a fresh Thread plus the thread-local map the guards read
	// their budget from is pure per-line overhead; a Thread also carries its
	// frame freelist, so reuse is cheaper than what this replaced.
	threads sync.Pool
}

// newThread builds a thread with the whole budget wired: Print (never
// starlark-go's fmt.Fprintln(os.Stderr) fallback), the thread-local the guards
// charge against, and the periodic OnMaxSteps checkpoint.
func (p *starlarkProgram) newThread(name string) *starlark.Thread {
	th := &starlark.Thread{Name: name, Print: p.print}
	th.SetLocal(budgetKey, &budget{})
	th.OnMaxSteps = budgetOf(th).onMaxSteps
	return th
}

// arm resets everything an invocation could carry into the next one: the step
// counter, the cancellation latch (a cancelled thread stays cancelled) and the
// budget's clock and spend.
func arm(th *starlark.Thread) *starlark.Thread {
	th.Steps = 0
	th.Uncancel()
	th.SetMaxExecutionSteps(stepsPerCheck)
	budgetOf(th).reset()
	return th
}

func (p *starlarkProgram) thread() *starlark.Thread {
	if th, ok := p.threads.Get().(*starlark.Thread); ok {
		return arm(th)
	}
	return arm(p.newThread("transform:" + p.signal))
}

// release returns a thread to the pool. Called only on a normal return: a
// panic unwinds past it, leaving the dirty thread to the garbage collector
// (this repo has no recover(), so the process is going down anyway).
func (p *starlarkProgram) release(th *starlark.Thread) { p.threads.Put(th) }

// compileStarlark compiles src and resolves its transform() function. The
// compile includes a smoke evaluation of the module (top-level statements
// run), so syntax and load-time errors are caught at config time.
func compileStarlark(signal, src string) (*starlarkProgram, error) {
	return compileStarlarkFn(signal, src, "transform")
}

// compileStarlarkFn compiles src and resolves fnName — the batch transforms
// all define transform(batch); the hook sections each define their own
// (admit/target/decide/parse).
//
// The parse/rewrite/compile steps are spelled out rather than left to
// ExecFileOptions because the amplifier rewrite (rewrite.go) has to happen
// between the parse and the resolve.
func compileStarlarkFn(signal, src, fnName string) (*starlarkProgram, error) {
	opts := &syntax.FileOptions{Set: true, While: true, GlobalReassign: true}
	f, err := opts.Parse(signal+".star", src, 0)
	if err != nil {
		return nil, fmt.Errorf("transforms %s: %w", signal, err)
	}
	if err := rewriteAmplifiers(f); err != nil {
		return nil, fmt.Errorf("transforms %s: %w", signal, err)
	}
	pre := predeclared(signal)
	prog, err := starlark.FileProgram(f, pre.Has)
	if err != nil {
		return nil, fmt.Errorf("transforms %s: %w", signal, err)
	}
	p := &starlarkProgram{signal: signal}
	p.print = func(_ *starlark.Thread, msg string) { scriptLog(signal, msg) }
	// The module evaluation gets the SAME budget as a run: its top-level
	// statements are ordinary code (`_hog = list(range(1<<26))` is eleven steps
	// and a gigabyte), it runs at every startup and every reload, and a process
	// that dies here dies before there is a last-good program to fall back to.
	globals, err := prog.Init(arm(p.newThread("compile:"+signal)), pre)
	if err != nil {
		return nil, fmt.Errorf("transforms %s: %w", signal, err)
	}
	fn, ok := globals[fnName].(starlark.Callable)
	if !ok {
		return nil, fmt.Errorf("transforms %s: script must define %s(...)", signal, fnName)
	}
	globals.Freeze() // shared across export goroutines: must be immutable
	p.fn = fn
	return p, nil
}

// call invokes the program's function with args on a bounded thread, returning
// its value (the hook accessors interpret it; run below discards it). Errors
// count into obs.TransformErrors under the program's signal.
func (p *starlarkProgram) call(args ...starlark.Value) (starlark.Value, error) {
	th := p.thread()
	v, err := starlark.Call(th, p.fn, starlark.Tuple(args), nil)
	p.release(th)
	if err != nil {
		return nil, fmt.Errorf("transform %s: %w", p.signal, err)
	}
	return v, nil
}

// run invokes transform(batch) on a bounded thread.
func (p *starlarkProgram) run(batch starlark.Value) error {
	th := p.thread()
	_, err := starlark.Call(th, p.fn, starlark.Tuple{batch}, nil)
	p.release(th)
	if err != nil {
		obs.TransformErrors.WithLabelValues(p.signal).Inc()
		return fmt.Errorf("transform %s: %w", p.signal, err)
	}
	return nil
}

// run* return the pruned-record count WITHOUT counting it: the caller counts
// via countDropped only once the batch's journey ends in an ack (a delivered
// forward, or an emptied payload acked without a send). Counting at prune
// time inflated kubescrape_transform_dropped_total by one full batch per
// transient retry for the copy-path producers, which re-offer the same object
// and re-run the script on a fresh copy every attempt — an operator alerting
// on the rate saw drop volume proportional to outage length, not intent.
func (p *starlarkProgram) runLogs(ld plog.Logs, em MetricEmitter) (int, error) {
	if err := p.run(&logBatch{ld: ld, em: em}); err != nil {
		return 0, err
	}
	return pruneLogs(ld), nil
}

func (p *starlarkProgram) runMetrics(md pmetric.Metrics, em MetricEmitter) (int, error) {
	if err := p.run(&metricBatch{md: md, em: em}); err != nil {
		return 0, err
	}
	return pruneMetrics(md), nil
}

func (p *starlarkProgram) runTraces(td ptrace.Traces, em MetricEmitter) (int, error) {
	if err := p.run(&traceBatch{td: td, em: em}); err != nil {
		return 0, err
	}
	return pruneTraces(td), nil
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
				_, drop := lr.Attributes().Get(DropMarker)
				if drop {
					lr.Attributes().Remove(DropMarker)
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
				if _, drop := m.Metadata().Get(DropMarker); drop {
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
		if _, ok := attrs.Get(DropMarker); ok {
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
	// An UNTYPED metric (pmetric.MetricTypeEmpty) has no data points to drop
	// and no data points to keep, so it is empty by definition and reported as
	// such — the prune's caller removes it. Reporting `false` here shipped it
	// instead: a name, a description and a unit, on every export, carrying no
	// measurement, legal enough that nothing downstream would ever reject it.
	// Nothing in this repo builds one (pushed ones die at the ingest receipt
	// seam, otlpingest/emptymetrics.go), which is exactly why this arm needs to
	// be right rather than exercised.
	return 0, true
}

func pruneTraces(td ptrace.Traces) int {
	dropped := 0
	rss := td.ResourceSpans()
	rss.RemoveIf(func(rs ptrace.ResourceSpans) bool {
		sss := rs.ScopeSpans()
		sss.RemoveIf(func(ss ptrace.ScopeSpans) bool {
			ss.Spans().RemoveIf(func(sp ptrace.Span) bool {
				_, drop := sp.Attributes().Get(DropMarker)
				if drop {
					sp.Attributes().Remove(DropMarker)
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
