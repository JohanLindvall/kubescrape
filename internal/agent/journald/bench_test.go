package journald

// Benchmarks for the journal's per-entry convert cost (batch -> OTLP grouping
// through the shared log chain), for parity with events' BenchmarkConvert and
// azurediag's BenchmarkConvertLogs/Metrics.
//
// Context for the absence of a budget: journald is a LOW-RATE producer — tens
// to hundreds of entries a second per node — so, like events, it does not carry
// the tailer's 0-alloc line budget. The shared chain's per-record cost is
// already pinned where it is hot (the tailer's BenchmarkIngestFlush and its
// allocation-budget tests). This one REPORTS, so a regression in the journal's
// own half — the grouping key, the unit resource, the record stamp — shows up
// as a number rather than not at all.

import (
	"strconv"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
	"github.com/JohanLindvall/kubescrape/internal/logline"
)

// benchLogs defeats dead-code elimination of convert's result.
var benchLogs plog.Logs

func BenchmarkConvert(b *testing.B) {
	const nEntries, nUnits = 256, 8

	run := func(b *testing.B, cfg Config, unitless bool) {
		b.Helper()
		cfg.Exporter = &captureExporter{}
		cfg.BatchSize = nEntries + 1
		r := New(cfg)
		for i := range nEntries {
			u := strconv.Itoa(i % nUnits)
			e := mkEntry("c"+strconv.Itoa(i), "unit-"+u+".service",
				"I0924 11:16:05.123456    1234 kubelet.go:2462] SyncLoop (PLEG): event for pod "+strconv.Itoa(i), "6")
			e.ident = "ident-" + u
			if unitless {
				// Kernel and syslog entries carry no _SYSTEMD_UNIT: grouped by
				// SYSLOG_IDENTIFIER through the tagged ident key.
				e.unit = ""
			}
			r.ingest(e, e.message, 0)
		}
		if ld := r.convert(); ld.LogRecordCount() != nEntries || ld.ResourceLogs().Len() != nUnits {
			b.Fatalf("records = %d in %d resources, want %d in %d",
				ld.LogRecordCount(), ld.ResourceLogs().Len(), nEntries, nUnits)
		}
		b.ReportAllocs()
		for b.Loop() {
			benchLogs = r.convert()
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/nEntries, "ns/entry")
	}

	b.Run("unit", func(b *testing.B) { run(b, Config{}, false) })
	b.Run("ident-only", func(b *testing.B) { run(b, Config{}, true) })
	// The full record chain: enrichment plus a rules pass (a keep rule on the
	// synthetic __severity__ that every entry passes, so the batch composition
	// matches the bare runs).
	b.Run("rules+enrich", func(b *testing.B) {
		rules, err := logline.NewLineFilter([]logline.LineRule{
			{Action: "keep", Match: []string{"__severity__=info"}},
		})
		if err != nil {
			b.Fatal(err)
		}
		run(b, Config{Chain: logchain.Config{Rules: rules, Enrich: true}}, false)
	})
}
