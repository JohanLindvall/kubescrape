package spanmetrics

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/cumagg"
	"github.com/JohanLindvall/kubescrape/internal/agent/tracehash"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

func benchTraces(svc string, attrs map[string]string) ptrace.Traces {
	return traces(svc, spanSpec{
		name: "GET /api/v1/orders", kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk,
		dur: 0.012, traceID: tid1, spanID: sid1, attrs: attrs,
	})
}

func BenchmarkConsume(b *testing.B) {
	g := New(Config{})
	td := benchTraces("checkout", map[string]string{"http.route": "/api/v1/orders", "http.method": "GET"})
	g.Consume(td) // warm the series
	b.ReportAllocs()
	for b.Loop() {
		g.Consume(td)
	}
}

// BenchmarkConsumeBatch pins the per-batch cost model: the staleness clock is
// read once per Consume (per exported payload), not per span, so a realistic
// multi-span batch pays it only once. Per-span work must stay 0 allocs.
func BenchmarkConsumeBatch(b *testing.B) {
	g := New(Config{})
	specs := make([]spanSpec, 100)
	for i := range specs {
		specs[i] = spanSpec{
			name: "GET /api/v1/orders", kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk,
			dur: 0.012, traceID: tid1, spanID: sid1,
		}
	}
	td := traces("checkout", specs...)
	g.Consume(td) // warm the series
	b.ReportAllocs()
	for b.Loop() {
		g.Consume(td)
	}
}

// The benchmarks above REPORT the budget; a benchmark cannot fail a build, so
// this is what holds it. Consume runs on the trace tier's receiver goroutines
// for every span the cluster emits: observe builds its series key on a stack
// buffer and looks it up with map[string(key)], which the compiler only elides
// while nothing forces the key onto the heap. A fmt call, a []string built per
// span, or reading the clock per span instead of per batch each cost one
// allocation and nothing else would notice.
func TestConsumeAllocationBudget(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	for _, tc := range []struct {
		name string
		cfg  Config
		td   ptrace.Traces
	}{
		// The warm path: one span against an admitted series.
		{"single-span", Config{}, benchTraces("checkout", map[string]string{
			"http.route": "/api/v1/orders", "http.method": "GET",
		})},
		// Configured extra dimensions read span-then-resource attributes; the
		// key grows but must still be built on the stack.
		{"dimensions", Config{Dimensions: []string{"http.route", "http.method"}}, benchTraces("checkout", map[string]string{
			"http.route": "/api/v1/orders", "http.method": "GET",
		})},
		// An INT dimension — the status code every HTTP span carries. Rendering
		// it to a string first (pdata's AsString, strconv.FormatInt) allocated
		// for every value outside 0-99; the key now appends the value itself,
		// so neither the common code nor a port-sized one may cost anything.
		{"int-dimension", Config{Dimensions: []string{"http.response.status_code", "server.port"}}, func() ptrace.Traces {
			td := benchTraces("checkout", nil)
			a := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes()
			a.PutInt("http.response.status_code", 200)
			a.PutInt("server.port", 8080)
			return td
		}()},
		// The tier's exemplar predicate (the head sampler's per-span decision)
		// runs once per span: a bound method value, called, allocates nothing.
		{"exemplar-keep", Config{ExemplarKeep: func(sp ptrace.Span) bool {
			return tracehash.Keep(sp.TraceID(), tracehash.Threshold(0.5))
		}}, benchTraces("checkout", nil)},
		// Names AT the truncation limit, and past it: the series key is built
		// on a per-call stack buffer, so a key that outgrew it allocated on
		// EVERY span — one 256-byte span name was enough while the buffer was
		// 256 bytes, and some database instrumentations name a span after its
		// statement text. Both built-in strings at the cut are the largest key
		// the built-ins can make (keyScratchBytes).
		{"names-at-the-cut", Config{}, traces(strings.Repeat("s", cumagg.MaxLabelBytes), spanSpec{
			name: strings.Repeat("n", cumagg.MaxLabelBytes), kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk,
			dur: 0.012, traceID: tid1, spanID: sid1,
		})},
		{"names-past-the-cut", Config{}, traces(strings.Repeat("s", 4096), spanSpec{
			name: strings.Repeat("n", 4096), kind: ptrace.SpanKindUnspecified, status: ptrace.StatusCodeUnset,
			dur: 0.012, traceID: tid1, spanID: sid1,
		})},
		{"batch-of-names-at-the-cut", Config{}, func() ptrace.Traces {
			specs := make([]spanSpec, 100)
			for i := range specs {
				specs[i] = spanSpec{
					name: strings.Repeat("n", cumagg.MaxLabelBytes), kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk,
					dur: 0.012, traceID: tid1, spanID: sid1,
				}
			}
			return traces("checkout", specs...)
		}()},
		// A realistic batch: the staleness clock is read once per Consume, not
		// per span, so 100 spans must still cost nothing.
		{"batch", Config{}, func() ptrace.Traces {
			specs := make([]spanSpec, 100)
			for i := range specs {
				specs[i] = spanSpec{
					name: "GET /api/v1/orders", kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk,
					dur: 0.012, traceID: tid1, spanID: sid1,
				}
			}
			return traces("checkout", specs...)
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := New(tc.cfg)
			td := tc.td
			g.Consume(td) // admit the series
			if allocs := testing.AllocsPerRun(200, func() { g.Consume(td) }); allocs != 0 {
				t.Fatalf("Consume allocates %v times per batch, want 0", allocs)
			}
		})
	}
}

// --- at the cardinality cap, and under the concurrency the tier receives with ---
//
// The benchmarks above measure ONE warm series in an otherwise empty map, which
// is the shape of a test and not of a tier: the cap is 20000, the map lookup is
// a hash plus a compare against a key that is longer than a cache line, and the
// series a batch touches are scattered across it. And Consume runs on the
// receiver goroutines — up to -ingest-max-in-flight of them — every one of which
// takes the SAME series mutex once per span, so a serial measurement cannot see
// what the lock costs.

// cardinalityTraces builds a batch of `n` spans whose series are spread over
// `card` distinct span names, i.e. `card` distinct series in the map.
func cardinalityTraces(card, n int) ptrace.Traces {
	specs := make([]spanSpec, n)
	for i := range specs {
		specs[i] = spanSpec{
			// The stride keeps consecutive spans of one batch from landing in
			// one map bucket, which is what a real batch does and what makes
			// the lookup pay for the map's size.
			name:   "GET /api/v1/orders/" + strconv.Itoa((i*7919)%card),
			kind:   ptrace.SpanKindServer,
			status: ptrace.StatusCodeOk,
			dur:    0.012,
			// Distinct ids, so the exemplar path is exercised as it is in
			// production rather than short-circuited by an empty trace id.
			traceID: pcommon.TraceID{byte(i), byte(i >> 8), 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 1},
			spanID:  pcommon.SpanID{byte(i), 2, 3, 4, 5, 6, 7, 8},
		}
	}
	return traces("checkout", specs...)
}

// warmAtCardinality admits `card` series and returns a generator holding them.
func warmAtCardinality(card int) *Generator {
	g := New(Config{MaxCardinality: card * 2})
	for i := 0; i < card; i += 500 {
		g.Consume(cardinalityTraces(card, min(500, card-i)))
	}
	// One pass over the whole key space, so every series exists.
	for i := 0; i < card; i += 1000 {
		specs := make([]spanSpec, 0, 1000)
		for j := i; j < min(i+1000, card); j++ {
			specs = append(specs, spanSpec{name: "GET /api/v1/orders/" + strconv.Itoa(j),
				kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk, dur: 0.012})
		}
		g.Consume(traces("checkout", specs...))
	}
	return g
}

func BenchmarkConsumeAtCardinality(b *testing.B) {
	for _, card := range []int{100, 20000} {
		g := warmAtCardinality(card)
		td := cardinalityTraces(card, 200)
		b.Run(strconv.Itoa(card)+"-series", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				g.Consume(td)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/200, "ns/span")
		})
	}
}

// BenchmarkConsumeParallel is the shape that matters: several receiver
// goroutines folding spans into one series map. Every span takes the store
// mutex, so this is where lock hand-off shows up and a serial benchmark cannot.
func BenchmarkConsumeParallel(b *testing.B) {
	g := warmAtCardinality(20000)
	td := cardinalityTraces(20000, 200)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			g.Consume(td)
		}
	})
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/200, "ns/span")
}

// BenchmarkExportAtCardinality prices one export interval's whole render at the
// cap. It runs once a minute, so its wall clock is not the point — the point is
// what it allocates, because that garbage is charged to the tier's heap between
// two collections.
func BenchmarkExportAtCardinality(b *testing.B) {
	g := warmAtCardinality(20000)
	exp := &nopMetricsExporter{}
	res := pcommon.NewResource()
	res.Attributes().PutStr("service.name", "kubescrape-agent")
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if err := g.Export(ctx, exp, res); err != nil {
			b.Fatal(err)
		}
	}
}

type nopMetricsExporter struct{}

func (nopMetricsExporter) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }
