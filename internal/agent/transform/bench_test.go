package transform

// Reporting benchmarks for the package's cost-model claim (~1µs per TOUCHED
// record; see the package doc). Nothing here ENFORCES a budget — transforms
// are cold relative to the tailer's line path and run once per exported
// BATCH — but the claim lived in prose with no measurement behind it, so
// future editors of the host objects had no way to see a 10x regression.
// Honest reporting only: if the numbers drift, update the doc or the code.

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// benchLogs is a realistic small batch: 256 records under one resource.
func benchLogs(n int) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("k8s.namespace.name", "ns1")
	lrs := rl.ScopeLogs().AppendEmpty().LogRecords()
	for i := 0; i < n; i++ {
		lr := lrs.AppendEmpty()
		lr.Body().SetStr("GET /api/v1/things 200 12ms client=10.0.0.1")
		lr.Attributes().PutStr("level", "info")
	}
	return ld
}

// BenchmarkTransformLogRecord runs transform(batch) over 256 records and
// reports ns/record: a no-op pass (the iteration floor) and a body-touching
// pass (the documented ~1µs/touched-record shape).
func BenchmarkTransformLogRecord(b *testing.B) {
	const n = 256
	run := func(b *testing.B, script string) {
		b.Helper()
		prog, err := Compile([]byte(script))
		if err != nil {
			b.Fatal(err)
		}
		ld := benchLogs(n)
		if _, err := prog.logs.runLogs(ld); err != nil { // warm + sanity
			b.Fatal(err)
		}
		if ld.LogRecordCount() != n {
			b.Fatalf("records = %d, want %d (the benchmark script must not drop)", ld.LogRecordCount(), n)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			if _, err := prog.logs.runLogs(ld); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/n, "ns/record")
	}
	b.Run("noop", func(b *testing.B) {
		run(b, "logs: |\n  def transform(batch):\n      for r in batch:\n          pass\n")
	})
	b.Run("touch-body", func(b *testing.B) {
		run(b, "logs: |\n  def transform(batch):\n      for r in batch:\n          if \"teapot\" in r.body:\n              pass\n")
	})
}

// discardExp forwards nothing: the wrapper benchmarks below measure the seam,
// not a downstream.
type discardExp struct{}

func (discardExp) ExportLogs(context.Context, plog.Logs) error          { return nil }
func (discardExp) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }
func (discardExp) ExportTraces(context.Context, ptrace.Traces) error    { return nil }

// BenchmarkWrapperNoopScript is the whole-seam cost of a `def
// transform(batch): return` script: "copy" is the unmarked path (deep copy +
// invocation — the package doc's headline number), "handoff" the marked
// in-place path (invocation only; a no-op script leaves the reused payload
// untouched, so iterating over one object is sound).
func BenchmarkWrapperNoopScript(b *testing.B) {
	noop := "  def transform(batch):\n      return\n"
	prog, err := Compile([]byte("logs: |\n" + noop + "metrics: |\n" + noop))
	if err != nil {
		b.Fatal(err)
	}
	w := Wrap(discardExp{}, nil, prog)
	plain, marked := context.Background(), Handoff(context.Background())

	ld := benchLogs(1024)
	b.Run("logs-1024/copy", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := w.ExportLogs(plain, ld); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("logs-1024/handoff", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := w.ExportLogs(marked, ld); err != nil {
				b.Fatal(err)
			}
		}
	})

	md := benchMetrics(10_000) // iterate_test.go's promscrape-chunk shape
	b.Run("metrics-10k/copy", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := w.ExportMetrics(plain, md); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("metrics-10k/handoff", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := w.ExportMetrics(marked, md); err != nil {
				b.Fatal(err)
			}
		}
	})
}
