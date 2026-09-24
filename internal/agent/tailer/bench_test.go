package tailer

import (
	"context"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
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
	for _, l := range lines {
		feedOne(tl, f, l)
	}
}

// feedOne pushes a single line through feedLine, advancing the file's offsets
// exactly as consume does.
func feedOne(tl *Tailer, f *file, l string) {
	start := f.lineStart
	end := start + int64(len(l)) + 1
	tl.feedLine(context.Background(), f, l, start, end, time.Now())
	f.lineStart = end
	f.readPos = end
}

// BenchmarkIngestLine measures the per-line pipeline cost (CRI parse, offset
// ledger, multiline stages, batch append) without flushing.
func BenchmarkIngestLine(b *testing.B) {
	tl, f := benchTailer(b, Config{Multiline: true})
	lines := benchLines(1024)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		feedOne(tl, f, lines[i])
		if i++; i == len(lines) {
			i = 0
			tl.batch = tl.batch[:0] // discard without flushing
		}
	}
}

// benchChunk builds one read-sized (>= 64 KiB, the tailer's scratch buffer)
// chunk of whole CRI lines, and reports how many lines it holds.
func benchChunk() ([]byte, int) {
	var buf []byte
	n := 0
	for len(buf) < 64*1024 {
		for _, l := range benchLines(64) {
			buf = append(buf, l...)
			buf = append(buf, '\n')
			n++
		}
	}
	return buf, n
}

// BenchmarkIngestChunk measures the per-CHUNK cost: the carry buffer plus the
// per-line pipeline. The only per-line allocation is consume's string(line);
// anything above that is the pending buffer being reallocated per read. One
// iteration is one 64 KiB read, so ns/op and B/op are per CHUNK — divide by the
// reported ns/line's line count for the per-line figure.
func BenchmarkIngestChunk(b *testing.B) {
	tl, f := benchTailer(b, Config{Multiline: true})
	chunk, lines := benchChunk()
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		tl.ingestChunk(ctx, f, chunk, false)
		tl.batch = tl.batch[:0] // discard without flushing
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(lines), "ns/line")
}

// flushShape is one production flush configuration and the per-line
// allocation ceiling it is held to. BenchmarkIngestFlush REPORTS every shape and
// TestIngestFlushAllocationBudget ENFORCES every shape: a benchmark cannot fail
// a build, so a shape carrying a figure but no assertion is documentation, not
// a budget — which is what enrich+metrics+rules (the one shape through
// logchain's rules scratch record and MoveAndAppendTo) was until both read
// their shapes from here.
type flushShape struct {
	name    string
	cfg     Config
	lines   []string
	ceiling float64 // allocations per line
}

// flushShapes builds the shapes afresh (a log-metrics set and a line filter are
// per-tailer state, so two runs must not share them).
func flushShapes(tb testing.TB) []flushShape {
	tb.Helper()
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "http_requests_total", Type: metrics.CounterType, Value: "1",
		Match:  []string{"level=info"},
		Labels: []string{"status=$http_status(_xx)"},
	}})
	if err != nil {
		tb.Fatal(err)
	}
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", Match: []string{"__severity__=debug"}},
	})
	if err != nil {
		tb.Fatal(err)
	}
	return []flushShape{
		{"plain", Config{Multiline: true}, benchLines(1024), 4},
		{"enrich", Config{Multiline: true, Chain: logchain.Config{Enrich: true}}, benchLines(1024), 4},
		{"enrich+metrics+rules", Config{Multiline: true, Chain: logchain.Config{Enrich: true, LogMetrics: set, Rules: rules}}, benchLines(1024), 5},
		// Every line lifts a resource and a scope attribute. A record lifting a
		// RESOURCE attribute used to build logattrs.Key up to five times —
		// twice in scope, once more for the log-metrics bind key, and twice
		// again when Dest re-ran scope to find the group buildRecord already
		// had — six allocations a record that no budget covered, since no
		// other shape configures lifting. Measured 18 allocs/line before the
		// resource key was built once and the group reused by Dest, 12 after.
		{"enrich+lift", Config{Multiline: true, Chain: logchain.Config{Enrich: true, LogAttrs: liftExtractor(tb)}}, liftLines(1024), 12},
	}
}

// BenchmarkIngestFlush measures the full ingestion path per line: pipeline +
// record building + export (null), per flushShape — enrich is the production
// shape.
func BenchmarkIngestFlush(b *testing.B) {
	for _, tc := range flushShapes(b) {
		b.Run(tc.name, func(b *testing.B) {
			tl, f := benchTailer(b, tc.cfg)
			ctx := context.Background()
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				feedOne(tl, f, tc.lines[i])
				if i++; i == len(tc.lines) {
					i = 0
					tl.flush(ctx)
				}
			}
			// The tail batch's lines are already counted in N, so its flush is
			// charged too: left outside the timer (b.Loop stops it on exit),
			// allocs/op read low by however much of the last batch was
			// unflushed — 0 at -benchtime=1000x, the true 4/4/5 only at 1s.
			b.StartTimer()
			tl.flush(ctx)
			b.StopTimer()
			if got := strconv.Itoa(len(tl.batch)); got != "0" {
				b.Fatalf("batch not flushed: %s", got)
			}
		})
	}
}
