package spanmetrics

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"
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
