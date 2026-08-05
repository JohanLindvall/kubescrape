package transform

// Reporting benchmarks for the package's cost-model claim (~1µs per TOUCHED
// record; see the package doc). Nothing here ENFORCES a budget — transforms
// are cold relative to the tailer's line path and run once per exported
// BATCH — but the claim lived in prose with no measurement behind it, so
// future editors of the host objects had no way to see a 10x regression.
// Honest reporting only: if the numbers drift, update the doc or the code.

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
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
		if err := prog.logs.runLogs(ld); err != nil { // warm + sanity
			b.Fatal(err)
		}
		if ld.LogRecordCount() != n {
			b.Fatalf("records = %d, want %d (the benchmark script must not drop)", ld.LogRecordCount(), n)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			if err := prog.logs.runLogs(ld); err != nil {
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
