package events

// Benchmarks for the events pipeline's per-event cost: ingest (eventAttrs +
// resource resolution + append), convert (batch -> OTLP grouping, bare and
// with the rules+enrich chain), and eventAttrs alone.
//
// Context for the budgets: this pipeline is LOW-RATE — tens of events/second
// cluster-wide, bursting on incidents — and a cluster-singleton, so it does
// not carry the tailer's 0-alloc line budget or promscrape's per-sample
// discipline. What matters is that one event's cost is microseconds, not
// milliseconds, and that a batch flush does not balloon against the
// singleton's 256Mi limit.

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// benchSink defeats dead-code elimination of pure-function results.
var benchSink map[string]any

// benchPod is the involved pod every benchmark event resolves to, shaped like
// a real resolved pod (owner chain, node, IP, labels).
func benchPod() *kubemeta.Pod {
	return &kubemeta.Pod{
		Name: "web-abc", Namespace: "default", UID: "pod-uid", NodeName: "node1",
		PodIP:  "10.0.1.23",
		Labels: map[string]string{"app": "web", "pod-template-hash": "6f7b9c"},
		Owners: []kubemeta.Owner{{Kind: "Deployment", Name: "web"}},
	}
}

// benchEvent is one realistic Warning occurrence; the involved object's UID
// matches benchPod, so ingest takes the resolved-identity path.
func benchEvent() *corev1.Event {
	return event("web-abc.17f3a2b9c1d0e4f5", "BackOff",
		"Back-off restarting failed container app in pod web-abc_default(11111111-2222-3333-4444-555555555555)",
		"Warning", "12345", 7, time.Now())
}

// benchFill ingests n events spread across pods involved objects. The
// involved UID stays "pod-uid" so every event resolves through fakeMeta and
// carries a full resolved resource; the involved NAME varies, so the batch
// still groups into `pods` distinct ResourceLogs — realistic group count and
// resource size, identical identity content.
func benchFill(b *testing.B, r *Reader, n, pods int) {
	b.Helper()
	ctx := context.Background()
	now := time.Now()
	kinds := []struct{ reason, msg, typ string }{
		{"BackOff", "Back-off restarting failed container app in pod web_default(11111111-2222-3333-4444-555555555555)", "Warning"},
		{"Pulled", `Successfully pulled image "ghcr.io/acme/web:1.2.3" in 388ms (388ms including waiting)`, "Normal"},
		{"Unhealthy", "Readiness probe failed: HTTP probe failed with statuscode: 503", "Warning"},
		{"Started", "Started container app", "Normal"},
	}
	for i := 0; i < n; i++ {
		k := kinds[i%len(kinds)]
		e := event(fmt.Sprintf("ev-%03d", i), k.reason, k.msg, k.typ, strconv.Itoa(10000+i), int32(i%5+1), now)
		e.InvolvedObject.Name = fmt.Sprintf("web-%02d", i%pods)
		r.ingest(ctx, e)
	}
	if len(r.batch) != n {
		b.Fatalf("batch = %d, want %d", len(r.batch), n)
	}
}

// BenchmarkIngest is the per-event ingest cost: eventAttrs, the metadata
// resolution (fakeMeta, so the metaclient's own cost is excluded) and the
// resource build, plus the batch append.
func BenchmarkIngest(b *testing.B) {
	r := New(Config{Meta: fakeMeta{pod: benchPod()}, Exporter: &captureExporter{}})
	ctx := context.Background()
	e := benchEvent()
	r.ingest(ctx, e) // warm the batch's capacity
	b.ReportAllocs()
	for b.Loop() {
		r.batch = r.batch[:0]
		r.ingest(ctx, e)
	}
}

// BenchmarkConvert renders a realistic flush: 256 events across 32 involved
// pods, grouped into per-object ResourceLogs. convert only reads the batch,
// so one ingested batch serves every iteration.
func BenchmarkConvert(b *testing.B) {
	const nEvents, nPods = 256, 32

	run := func(b *testing.B, cfg Config) {
		b.Helper()
		cfg.Meta = fakeMeta{pod: benchPod()}
		cfg.Exporter = &captureExporter{}
		cfg.BatchSize = nEvents + 1 // ingest must never auto-flush
		r := New(cfg)
		benchFill(b, r, nEvents, nPods)
		if ld := r.convert(); ld.LogRecordCount() != nEvents { // warm + sanity: nothing dropped
			b.Fatalf("records = %d, want %d", ld.LogRecordCount(), nEvents)
		}
		b.ReportAllocs()
		for b.Loop() {
			r.convert()
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/nEvents, "ns/event")
	}

	b.Run("bare", func(b *testing.B) {
		run(b, Config{})
	})
	// The full record chain: enrichment plus a rules pass (a keep rule on the
	// synthetic __severity__, so every record takes the scratch-slice path and
	// the batch composition matches bare).
	b.Run("rules+enrich", func(b *testing.B) {
		rules, err := logline.NewLineFilter([]logline.LineRule{
			{Action: "keep", Match: []string{"__severity__=warn"}},
		})
		if err != nil {
			b.Fatal(err)
		}
		run(b, Config{Rules: rules, Enrich: true})
	})
}

// BenchmarkEventAttrs isolates the record-attribute map build.
func BenchmarkEventAttrs(b *testing.B) {
	e := benchEvent()
	b.ReportAllocs()
	for b.Loop() {
		benchSink = eventAttrs(e)
	}
}
