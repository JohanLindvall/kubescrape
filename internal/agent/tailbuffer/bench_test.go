package tailbuffer

import (
	"context"
	"encoding/binary"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// The receive path runs on the tier's handler goroutines, once per pushed span,
// under one mutex — so its allocation budget is the thing to watch. The floor is
// not zero: a buffered span is COPIED out of the request (the request's memory
// goes back to the transport when the handler returns, and the trace has to
// outlive it by the whole decision window), and pdata's CopyTo is where those
// allocations are. What must stay flat is everything else — no per-call maps, no
// per-span slices, no label lookups, no payload allocated for a push that is
// purely buffered.
//
// Numbers from the machine this was written on (Go 1.25, amd64, Ryzen 7 8840HS),
// per SPAN — the receive benchmarks push 20 spans each, so divide their op
// figures by 20:
//
//	BenchmarkReceiveNewTraces      666 ns   14 allocs   841 B   (a trace per span: every span pays for its own entry, ResourceSpans and resource copy — the worst case)
//	BenchmarkReceiveExistingTrace  266 ns    4 allocs   365 B   (the steady state: a span joining a group that exists)
//	BenchmarkReceiveLateDrop        58 ns    0 allocs     0 B   (decided drop: a cache probe and a counter)
//	BenchmarkReceiveLateKeep       385 ns   10 allocs   568 B   (decided keep: copied into an outgoing payload and forwarded)
//
// The 365 B is the memory bound's unit of account: a minimal two-attribute span.
// Budget ~1 KiB for a realistic one (memory.go sizes maxSpans on that figure),
// plus one ResourceSpans/ScopeSpans wrapper and its copy of the resource
// attributes per (pushed payload, trace) group — ~320 B with a bare
// service.name, ~1040 B with the attribute set the tier's enricher stamps.
// resource_bench_test.go measures the group cost across push sizes and records
// why those groups are not interned.
//
// The zero on the late-drop line is the one that is asserted below, because it
// is the only one this package fully controls: it is the path a heavily-sampling
// shard spends most of its time on, and it must not allocate.

// discard is a no-op exporter: the benchmarks measure this package, not OTLP.
type discard struct{}

func (discard) ExportTraces(context.Context, ptrace.Traces) error { return nil }

// reset empties the buffer without deciding anything, so a benchmark can run for
// millions of iterations without its memory growing into the measurement. Only
// the benchmarks use it — production frees a trace by deciding it.
func (b *Buffer) reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	// clear() rather than fresh maps: a benchmark that measures per-trace memory
	// must not have two 1024-bucket map allocations per iteration in its number.
	clear(b.trace)
	b.order = b.order[:0]
	b.head = 0
	b.spans = 0
	clear(b.cache.m)
	b.cache.fifo = b.cache.fifo[:0]
	b.cache.head = 0
}

func benchBuffer(b *testing.B, spansPerTrace int) *Buffer {
	b.Helper()
	buf, err := New(Config{
		Config:           alwaysCfg(),
		DecisionWait:     "1m",
		MaxTraces:        1 << 20,
		MaxSpans:         1 << 22,
		MaxSpansPerTrace: spansPerTrace,
	}, discard{}, nil)
	if err != nil {
		b.Fatal(err)
	}
	return buf
}

// A push of 20 spans belonging to 20 different traces, none of them seen before:
// the shape a shard sees when applications push batches of unrelated requests.
func BenchmarkReceiveNewTraces(b *testing.B) {
	const batches, spans = 64, 20
	payloads := make([]ptrace.Traces, batches)
	for p := range payloads {
		specs := make([]spanSpec, spans)
		for i := range specs {
			id := uint64(p*spans + i)
			specs[i] = spanSpec{trace: id, span: id, end: 10, attrs: map[string]any{
				"http.route": "/api/v1/orders", "http.status_code": 200,
			}}
		}
		payloads[p] = payload("checkout", specs...)
	}
	buf := benchBuffer(b, 1<<20)
	ctx := context.Background()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if i%batches == 0 {
			buf.reset() // amortized over batches*spans spans: below the reporting floor
		}
		if err := buf.ExportTraces(ctx, payloads[i%batches]); err != nil {
			b.Fatal(err)
		}
		i++
	}
}

// The steady state: spans joining a trace that is already assembling.
func BenchmarkReceiveExistingTrace(b *testing.B) {
	const spans = 20
	specs := make([]spanSpec, spans)
	for i := range specs {
		specs[i] = spanSpec{trace: 1, span: uint64(i), end: 10, attrs: map[string]any{
			"http.route": "/api/v1/orders", "http.status_code": 200,
		}}
	}
	td := payload("checkout", specs...)
	buf := benchBuffer(b, 1<<20)
	ctx := context.Background()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if i%512 == 0 {
			buf.reset()
		}
		if err := buf.ExportTraces(ctx, td); err != nil {
			b.Fatal(err)
		}
		i++
	}
}

// decidedDropBuffer returns a buffer that has already decided trace 5 DROP, so
// a further push of td takes the late path: a cache probe and a counter.
func decidedDropBuffer(t testing.TB) (*Buffer, ptrace.Traces, context.Context) {
	t.Helper()
	td := payload("checkout", spanSpec{trace: 5, span: 1, end: 10})
	buf, err := New(Config{Config: errorsCfg(), DecisionWait: "1ms"}, discard{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	clk := newClock()
	buf.now = clk.now
	ctx := context.Background()
	if err := buf.ExportTraces(ctx, td); err != nil {
		t.Fatal(err)
	}
	clk.advance(time.Second)
	buf.Sweep(ctx) // decides DROP (no error span) and caches the verdict
	clk.advance(-time.Second)
	return buf, td, ctx
}

// A span whose trace was decided DROP: a cache probe and a counter, nothing
// else. This is where a sampling shard spends its time, and it is asserted to
// allocate nothing.
func BenchmarkReceiveLateDrop(b *testing.B) {
	buf, td, ctx := decidedDropBuffer(b)
	b.ReportAllocs()
	for b.Loop() {
		if err := buf.ExportTraces(ctx, td); err != nil {
			b.Fatal(err)
		}
	}
}

// The benchmark REPORTS the budget; a benchmark cannot fail a build, so this is
// what holds it. This assertion used to live inside BenchmarkReceiveLateDrop,
// where `go test` never reached it. A sampling shard spends most of its receive
// path here — every span of every already-dropped trace — so a per-push map, a
// slice or a closure is a per-span cost on the busiest goroutines the tier has.
func TestReceiveLateDropAllocationBudget(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	buf, td, ctx := decidedDropBuffer(t)
	if err := buf.ExportTraces(ctx, td); err != nil { // warm the grouping scratch
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if err := buf.ExportTraces(ctx, td); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("the decided-drop receive path allocates %v times per push, want 0", allocs)
	}
}

// A span whose trace was decided KEEP: copied into an outgoing payload and
// forwarded on this goroutine.
func BenchmarkReceiveLateKeep(b *testing.B) {
	td := payload("checkout", spanSpec{trace: 5, span: 1, end: 10})
	buf, err := New(Config{Config: alwaysCfg(), DecisionWait: "1ms"}, discard{}, nil)
	if err != nil {
		b.Fatal(err)
	}
	clk := newClock()
	buf.now = clk.now
	ctx := context.Background()
	if err := buf.ExportTraces(ctx, td); err != nil {
		b.Fatal(err)
	}
	clk.advance(time.Second)
	buf.Sweep(ctx)
	clk.advance(-time.Second)

	b.ReportAllocs()
	for b.Loop() {
		if err := buf.ExportTraces(ctx, td); err != nil {
			b.Fatal(err)
		}
	}
}

// The whole cycle: 1000 spans pushed (100 traces of 10), then judged and
// exported in one pass. It is dominated by the receive path above — the decision
// itself is a policy evaluation per trace (agent/tailsample's benchmarks: tens
// of nanoseconds, 0 allocs) plus one payload move.
func BenchmarkDecide(b *testing.B) {
	const traces, spans = 100, 10
	specs := make([]spanSpec, 0, traces*spans)
	for t := 0; t < traces; t++ {
		for s := 0; s < spans; s++ {
			specs = append(specs, spanSpec{trace: uint64(t), span: uint64(t*spans + s), end: 10,
				attrs: map[string]any{"http.route": "/api/v1/orders"}})
		}
	}
	td := payload("checkout", specs...)
	buf := benchBuffer(b, 1<<20)
	clk := newClock()
	buf.now = clk.now
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if err := buf.ExportTraces(ctx, td); err != nil {
			b.Fatal(err)
		}
		clk.advance(2 * time.Minute)
		buf.Sweep(ctx)
	}
}

// --- at the occupancy a shard actually runs at ---
//
// Everything above measures a buffer holding one trace or twenty and a decision
// cache holding one verdict, which is the shape of a test. A shard in steady
// state holds `push rate x decisionWait` traces — thousands — and its decision
// cache fills to decisionCacheSize (100000 by default). Both are maps keyed by
// a 16-byte trace id, which Go cannot specialise the way it specialises a
// string or a uint64 key, so their cost is a function of how full they are and
// a one-entry measurement cannot see it.

// fillBuffer parks `traces` assembling traces in the buffer (one span each) and
// leaves the clock stopped so nothing decides under the measurement.
func fillBuffer(b *testing.B, buf *Buffer, traces int) {
	b.Helper()
	ctx := context.Background()
	const chunk = 200
	for base := 0; base < traces; base += chunk {
		specs := make([]spanSpec, 0, chunk)
		for i := base; i < min(base+chunk, traces); i++ {
			specs = append(specs, spanSpec{trace: uint64(i), span: uint64(i), end: 10})
		}
		if err := buf.ExportTraces(ctx, payload("checkout", specs...)); err != nil {
			b.Fatal(err)
		}
	}
	if got := buf.Stats().Traces; got != traces {
		b.Fatalf("setup: %d traces buffered, want %d", got, traces)
	}
}

// BenchmarkReceiveAtOccupancy: a push of 20 spans of new traces into a buffer
// already holding a realistic in-flight population.
func BenchmarkReceiveAtOccupancy(b *testing.B) {
	for _, live := range []int{20, 20000} {
		const batches, spans = 64, 20
		payloads := make([]ptrace.Traces, batches)
		for p := range payloads {
			specs := make([]spanSpec, spans)
			for i := range specs {
				id := uint64(1<<32 + p*spans + i)
				specs[i] = spanSpec{trace: id, span: id, end: 10, attrs: map[string]any{
					"http.route": "/api/v1/orders", "http.status_code": 200,
				}}
			}
			payloads[p] = payload("checkout", specs...)
		}
		buf := benchBuffer(b, 1<<20)
		clk := newClock()
		buf.now = clk.now
		fillBuffer(b, buf, live)
		ctx := context.Background()
		b.Run(strconv.Itoa(live)+"-live", func(b *testing.B) {
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				if err := buf.ExportTraces(ctx, payloads[i%batches]); err != nil {
					b.Fatal(err)
				}
				i++
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/spans, "ns/span")
		})
	}
}

// BenchmarkReceiveLateDropAtCacheSize: the path a heavily-sampling shard spends
// most of its time on, measured against a decision cache at its configured
// capacity rather than holding the single verdict the benchmark just made.
func BenchmarkReceiveLateDropAtCacheSize(b *testing.B) {
	for _, cached := range []int{1, 100000} {
		buf, td, ctx := decidedDropBuffer(b)
		// Fill the cache around the one verdict the probe will hit, so the
		// lookup pays for a full map.
		for i := range cached {
			var id pcommon.TraceID
			binary.BigEndian.PutUint64(id[8:], uint64(i)+1000)
			buf.cache.put(id, false, 0, buf.now())
		}
		b.Run(strconv.Itoa(cached)+"-cached", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := buf.ExportTraces(ctx, td); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
