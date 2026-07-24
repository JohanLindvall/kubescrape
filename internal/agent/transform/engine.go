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
	pruneLogs(ld)
	return nil
}

func (p *starlarkProgram) runMetrics(md pmetric.Metrics) error {
	if err := p.run(&metricBatch{md: md}); err != nil {
		return err
	}
	pruneMetrics(md)
	return nil
}

func (p *starlarkProgram) runTraces(td ptrace.Traces) error {
	if err := p.run(&traceBatch{td: td}); err != nil {
		return err
	}
	pruneTraces(td)
	return nil
}

// prune* remove records marked dropped and any groups left empty.
func pruneLogs(ld plog.Logs) {
	rls := ld.ResourceLogs()
	rls.RemoveIf(func(rl plog.ResourceLogs) bool {
		sls := rl.ScopeLogs()
		sls.RemoveIf(func(sl plog.ScopeLogs) bool {
			sl.LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
				_, drop := lr.Attributes().Get(dropMarker)
				if drop {
					lr.Attributes().Remove(dropMarker)
				}
				return drop
			})
			return sl.LogRecords().Len() == 0
		})
		return sls.Len() == 0
	})
}

func pruneMetrics(md pmetric.Metrics) {
	rms := md.ResourceMetrics()
	rms.RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		sms := rm.ScopeMetrics()
		sms.RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			sm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				return m.Name() == dropMarker
			})
			return sm.Metrics().Len() == 0
		})
		return sms.Len() == 0
	})
}

func pruneTraces(td ptrace.Traces) {
	rss := td.ResourceSpans()
	rss.RemoveIf(func(rs ptrace.ResourceSpans) bool {
		sss := rs.ScopeSpans()
		sss.RemoveIf(func(ss ptrace.ScopeSpans) bool {
			ss.Spans().RemoveIf(func(sp ptrace.Span) bool {
				_, drop := sp.Attributes().Get(dropMarker)
				if drop {
					sp.Attributes().Remove(dropMarker)
				}
				return drop
			})
			return ss.Spans().Len() == 0
		})
		return sss.Len() == 0
	})
}
