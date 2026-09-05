package logenrich

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// logenrich.Apply runs once per exported log record on every producer that has
// -enrich on (the default): the tailer, journald, the events reader and the
// Azure reader. A live agent profile put it at 7.2-7.8% of the process's CPU
// at 4,000 log entries/s/node, and the package had no benchmark at all — so
// there was nothing to notice a regression against, and nothing pinning the
// per-record allocation count that the tailer's own flush budget depends on.
//
// The three lines below are the three enrich FORMATS (json, logfmt, pattern)
// plus the no-format case, which is the one a plain application line takes.

const (
	jsonLine = `{"level":"info","ts":"2026-07-24T10:00:00.123456Z","msg":"handled request",` +
		`"trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"b7ad6b7169203331",` +
		`"path":"/api/v1/orders","status":200,"duration_ms":42.5}`
	logfmtLine = `level=info ts=2026-07-24T10:00:00.123456Z msg="handled request" ` +
		`trace_id=0af7651916cd43dd8448eb211c80319c span_id=b7ad6b7169203331 ` +
		`path=/api/v1/orders status=200 duration_ms=42.5`
	patternLine = `2026-07-24T10:00:00.123Z ERROR [My.App.Worker] handled request failed: ` +
		`System.InvalidOperationException: the operation is not valid`
	plainLine = `handled request path=/api/v1/orders in 42.5ms and returned the expected body`
)

func benchApply(b *testing.B, line string) {
	b.Helper()
	// One record, re-cleared per iteration: Apply's cost is the parse plus the
	// attribute puts, and a fresh pdata record per iteration would measure
	// pdata's allocator instead.
	lr := plog.NewLogs().ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	b.ReportAllocs()
	for b.Loop() {
		lr.Attributes().Clear()
		Apply(lr, line)
	}
}

func BenchmarkApplyJSON(b *testing.B)    { benchApply(b, jsonLine) }
func BenchmarkApplyLogfmt(b *testing.B)  { benchApply(b, logfmtLine) }
func BenchmarkApplyPattern(b *testing.B) { benchApply(b, patternLine) }
func BenchmarkApplyPlain(b *testing.B)   { benchApply(b, plainLine) }

// TestApplyAllocationBudget is the enforcement the benchmarks above only
// report: a benchmark cannot fail a build (go test runs each for zero
// iterations), so a per-record allocation ceiling that lives in a bench
// comment is documentation.
//
// The record's attribute map is REUSED here, as it is in the benchmarks: that
// isolates what Apply itself costs from what building a fresh pdata record
// costs, which the tailer's own TestIngestFlushAllocationBudget already bounds
// end to end.
func TestApplyAllocationBudget(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector changes escape analysis and adds bookkeeping allocations")
	}
	for _, tc := range []struct {
		name string
		line string
		want float64
	}{
		// ZERO, for every format. The parse is ParseInto over a stack-held
		// Result (no per-line heap Result), the promoted attributes go into an
		// attribute map that already has capacity, and putStr does not escape.
		// A budget of 0 is worth having precisely because it cannot drift
		// quietly: any future field that needs a heap value shows up here as
		// the first allocation rather than as a percent nobody notices.
		{"json", jsonLine, 0},
		{"logfmt", logfmtLine, 0},
		{"pattern", patternLine, 0},
		{"plain", plainLine, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lr := plog.NewLogs().ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
			got := testing.AllocsPerRun(200, func() {
				lr.Attributes().Clear()
				Apply(lr, tc.line)
			})
			if got > tc.want {
				t.Fatalf("Apply allocated %.0f per record, budget %.0f", got, tc.want)
			}
		})
	}
}
