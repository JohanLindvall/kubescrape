package transform

// The batch iterators are LAZY PER RECORD, not just per field.
//
// They used to materialize every record (or span, or metric, or data point)
// into a slice of host objects before the script's first statement, so the
// package's "scripts pay only for the fields they touch" claim stopped at the
// record boundary and a script that broke out early paid for the whole batch.
// A promscrape metrics chunk is 10,000 data points.
//
// The views are still freshly allocated per step rather than reused, which
// TestIteratedRecordsMayBeRetained pins: a script may keep them.

import (
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func benchTraces(n int) ptrace.Traces {
	td := ptrace.NewTraces()
	sps := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	for i := 0; i < n; i++ {
		sps.AppendEmpty().SetName("op")
	}
	return td
}

func benchMetrics(points int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	ms := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
	m := ms.AppendEmpty()
	m.SetName("m")
	dps := m.SetEmptyGauge().DataPoints()
	for i := 0; i < points; i++ {
		dps.AppendEmpty().SetDoubleValue(float64(i))
	}
	return md
}

// A script that stops at the first element must not pay for the rest of the
// batch. The ceiling is a small constant plus Starlark's own per-call frames —
// nothing proportional to n.
func TestIterationIsLazyPerRecord(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector changes escape analysis and adds bookkeeping allocations")
	}
	const n = 4096
	const ceiling = 128 // Starlark call overhead; the point is that it is not O(n)

	t.Run("logs", func(t *testing.T) {
		prog := mustCompile(t, "logs: |\n  def transform(batch):\n      for r in batch:\n          return\n")
		ld := benchLogs(n)
		if got := testing.AllocsPerRun(5, func() {
			if _, err := prog.logs.runLogs(ld, nil); err != nil {
				t.Fatal(err)
			}
		}); got > ceiling {
			t.Errorf("%.0f allocs to visit ONE of %d records; want <= %d", got, n, ceiling)
		}
	})

	t.Run("traces", func(t *testing.T) {
		prog := mustCompile(t, "traces: |\n  def transform(batch):\n      for s in batch:\n          return\n")
		td := benchTraces(n)
		if got := testing.AllocsPerRun(5, func() {
			if _, err := prog.traces.runTraces(td, nil); err != nil {
				t.Fatal(err)
			}
		}); got > ceiling {
			t.Errorf("%.0f allocs to visit ONE of %d spans; want <= %d", got, n, ceiling)
		}
	})

	t.Run("metrics-datapoints", func(t *testing.T) {
		prog := mustCompile(t, "metrics: |\n  def transform(batch):\n      for m in batch:\n          for p in m.datapoints:\n              return\n")
		md := benchMetrics(n)
		if got := testing.AllocsPerRun(5, func() {
			if _, err := prog.metrics.runMetrics(md, nil); err != nil {
				t.Fatal(err)
			}
		}); got > ceiling {
			t.Errorf("%.0f allocs to visit ONE of %d data points; want <= %d", got, n, ceiling)
		}
	})
}

// The iterator hands out a FRESH view per step, so a script may keep the
// values past the loop — the natural two-pass shape. Reusing one view would
// make every element of this list the last record, silently.
func TestIteratedRecordsMayBeRetained(t *testing.T) {
	prog := mustCompile(t, strings.Join([]string{
		"logs: |",
		"  def transform(batch):",
		"      kept = [r for r in batch if r.attributes[\"keep\"] == \"yes\"]",
		"      for r in kept:",
		"          r.attributes[\"marked\"] = \"1\"",
		"",
	}, "\n"))
	ld := benchLogs(4)
	lrs := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	for i := 0; i < lrs.Len(); i++ {
		if i%2 == 0 {
			lrs.At(i).Attributes().PutStr("keep", "yes")
		}
	}
	if _, err := prog.logs.runLogs(ld, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < lrs.Len(); i++ {
		_, marked := lrs.At(i).Attributes().Get("marked")
		if want := i%2 == 0; marked != want {
			t.Errorf("record %d: marked=%v, want %v — a retained record view must still name its own record", i, marked, want)
		}
	}
}

func mustCompile(t *testing.T, script string) *Program {
	t.Helper()
	p, err := Compile([]byte(script))
	if err != nil {
		t.Fatal(err)
	}
	return p
}
