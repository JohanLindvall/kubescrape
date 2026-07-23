package tailer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
)

// nullExporter accepts everything (measures the pipeline, not the network).
type nullExporter struct{}

func (nullExporter) ExportLogs(context.Context, plog.Logs) error { return nil }

// benchTailer builds a tailer + one resolved containerd file whose pipeline
// can be fed directly, bypassing the filesystem (shared with fuzz_test.go).
func benchTailer(b testing.TB, cfg Config) (*Tailer, *file) {
	b.Helper()
	cfg.Exporter = nullExporter{}
	cfg.Metadata = fakeMeta{}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 1 << 30 // flush manually
	}
	tl := New(cfg)
	f := &file{
		path:        "/var/log/containers/" + logName,
		source:      &compiledSource{name: "containers", containerd: true, multiline: cfg.Multiline},
		containerID: "0123456789abcdef",
		resolved:    true,
		resource:    pcommon.NewResource(),
	}
	a := f.resource.Attributes()
	a.PutStr("k8s.namespace.name", "prod-payments")
	a.PutStr("k8s.pod.name", "payments-6f7b9c001")
	a.PutStr("k8s.container.name", "app")
	a.PutStr("k8s.node.name", "node-7")
	a.PutStr("service.name", "payments")
	a.PutStr("service.namespace", "prod-payments")
	a.PutStr("service.instance.id", "0123456789abcdef")
	tl.newPipeline(f)
	tl.files[f.path] = f
	return tl, f
}

// benchLines is a typical mix: JSON app lines and a plain-text line.
func benchLines(n int) []string {
	base := time.Date(2026, 7, 11, 10, 0, 0, 0, time.UTC)
	out := make([]string, n)
	for i := range out {
		ts := base.Add(time.Duration(i) * time.Millisecond).Format(time.RFC3339Nano)
		switch i % 3 {
		case 0:
			out[i] = ts + ` stdout F {"level":"info","msg":"handled request","http_status":200,"method":"GET","path":"/api/v1/orders","latency_ms":42.5}`
		case 1:
			out[i] = ts + ` stdout F {"level":"debug","msg":"cache lookup","key":"user:1234","hit":true}`
		default:
			out[i] = ts + ` stderr F 2026/07/11 10:00:00 error contacting upstream: connection refused`
		}
	}
	return out
}

// feedAll pushes lines through feedLine, as consume does.
func feedAll(tl *Tailer, f *file, lines []string) {
	ctx := context.Background()
	off := f.lineStart
	for _, l := range lines {
		start := off
		off += int64(len(l)) + 1
		tl.feedLine(ctx, f, l, start, off)
	}
	f.lineStart = off
	f.readPos = off
}

// BenchmarkIngestLine measures the per-line pipeline cost (CRI parse, offset
// ledger, multiline stages, batch append) without flushing.
func BenchmarkIngestLine(b *testing.B) {
	tl, f := benchTailer(b, Config{Multiline: true})
	lines := benchLines(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i += len(lines) {
		feedAll(tl, f, lines)
		tl.batch = tl.batch[:0] // discard without flushing
	}
}

// BenchmarkIngestFlush measures the full ingestion path per line: pipeline +
// record building + export (null), with enrichment on — the production shape.
func BenchmarkIngestFlush(b *testing.B) {
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{"plain", Config{Multiline: true}},
		{"enrich", Config{Multiline: true, Enrich: true}},
		{"enrich+metrics+rules", Config{Multiline: true, Enrich: true}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			cfg := tc.cfg
			if tc.name == "enrich+metrics+rules" {
				set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
					Name: "http_requests_total", Type: metrics.CounterType, Value: "1",
					Match:  []string{"level=info"},
					Labels: []string{"status=$http_status(_xx)"},
				}})
				if err != nil {
					b.Fatal(err)
				}
				cfg.LogMetrics = set
				rules, err := logline.NewLineFilter([]logline.LineRule{
					{Action: "drop", Match: []string{"__severity__=debug"}},
				})
				if err != nil {
					b.Fatal(err)
				}
				cfg.Rules = rules
			}
			tl, f := benchTailer(b, cfg)
			lines := benchLines(1024)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i += len(lines) {
				feedAll(tl, f, lines)
				tl.flush(ctx)
			}
			b.StopTimer()
			if got := fmt.Sprint(len(tl.batch)); got != "0" {
				b.Fatalf("batch not flushed: %s", got)
			}
		})
	}
}
