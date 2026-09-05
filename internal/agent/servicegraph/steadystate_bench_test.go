package servicegraph

import (
	"encoding/binary"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// The benchmarks in processor_test.go measure a store holding one request or a
// hundred; a shard in steady state holds `request rate x wait` half-edges,
// which at the defaults (wait 10s, maxItems 10000) is the whole cap. That is a
// different measurement: the map is large, the entries a batch pairs with were
// inserted thousands of spans ago and are cold in cache, and the incremental
// expiry pass actually has work to do.
//
// steadyStore fills a processor with `live` unpaired client halves whose server
// halves have not arrived, i.e. exactly what the store holds between two
// requests, and returns it with the clock parked so nothing expires under the
// measurement.
func steadyStore(b *testing.B, live int, dims []string) *Processor {
	b.Helper()
	p := NewProcessor(Config{MaxItems: live * 4, Wait: "10s", Dimensions: dims}, discardLog())
	p.SetSink(&countSink{})
	now := t0
	p.now = func() time.Time { return now }
	const chunk = 500
	for i := 0; i < live; i += chunk {
		p.Consume(unpairableClientSpans(i, min(chunk, live-i)))
	}
	if got := p.Stats().Items; got != live {
		b.Fatalf("setup: store holds %d half-edges, want %d", got, live)
	}
	return p
}

// pairingBatch is one push as the tier receives it: `traces` requests, each a
// client span and the server span that pairs with it, so half the spans insert
// and half complete an edge — the steady state of a shard that owns both halves.
func pairingBatch(base, traces int) ptrace.Traces {
	return pairingBatchAttrs(base, traces, map[string]string{"http.request.method": "GET", "peer.service": "orders"})
}

// realisticSpanAttrs is what otelhttp actually puts on a span. It matters here
// because every attribute lookup this package makes — the peer-attribute
// precedence scan, the database classification, each configured dimension — is
// a LINEAR scan of that list (pcommon.Map has no index), so a two-attribute
// fixture prices the scans at a seventh of what a real span costs.
var realisticSpanAttrs = map[string]string{
	"http.request.method":       "GET",
	"url.full":                  "https://orders.shop.svc.cluster.local/api/v2/orders/8f3a/shipments?expand=carrier",
	"url.path":                  "/api/v2/orders/8f3a/shipments",
	"url.scheme":                "https",
	"server.address":            "orders.shop.svc.cluster.local",
	"server.port":               "8443",
	"http.response.status_code": "200",
	"user_agent.original":       "checkout/2.14.3 (linux; go1.24.2) otelhttp/0.58.0",
	"network.protocol.version":  "1.1",
	"peer.service":              "orders",
	"enduser.id":                "usr_01HQ8ZK3M4N5P6Q7R8S9T0V1W2",
	"shop.tenant":               "acme-nordics",
	"http.request.resend":       "true",
	"http.request.body.size":    "0",
}

func pairingBatchAttrs(base, traces int, attrs map[string]string) ptrace.Traces {
	spans := make([]sgSpan, 0, traces*2)
	for i := base; i < base+traces; i++ {
		var tid pcommon.TraceID
		var sid pcommon.SpanID
		binary.BigEndian.PutUint64(tid[8:], uint64(i+1))
		binary.BigEndian.PutUint64(sid[:], uint64(i+1))
		spans = append(spans,
			sgSpan{name: "GET /orders", kind: ptrace.SpanKindClient, dur: 0.10, traceID: tid, spanID: sid, attrs: attrs},
			sgSpan{name: "GET /orders", kind: ptrace.SpanKindServer, dur: 0.09, traceID: tid, spanID: spanID(0x7f), parentID: sid, attrs: attrs})
	}
	return sgTraces("checkout", spans...)
}

// BenchmarkConsumeSteadyState is the shard's real per-span cost: pairing into a
// store that already holds a full working set of half-edges. The
// realistic-attrs arm carries the attribute set otelhttp emits, which is what
// the peer and database scans actually walk.
func BenchmarkConsumeSteadyState(b *testing.B) {
	for _, live := range []int{100, 10000} {
		p := steadyStore(b, live, nil)
		td := pairingBatch(live+1, 100) // 200 spans: 100 inserts, 100 completions
		b.Run(strconv.Itoa(live)+"-live", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				p.Consume(td)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/200, "ns/span")
		})
	}
}

// The same store, with the attribute set a real instrumented span carries.
func BenchmarkConsumeSteadyStateRealisticAttrs(b *testing.B) {
	p := steadyStore(b, 10000, nil)
	td := pairingBatchAttrs(10001, 100, realisticSpanAttrs)
	b.ReportAllocs()
	for b.Loop() {
		p.Consume(td)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/200, "ns/span")
}

// The same, with the metric Registry as the sink rather than a counter, so the
// completed edge pays for its series key and fold as it does in production.
func BenchmarkConsumeSteadyStateIntoRegistry(b *testing.B) {
	p := steadyStore(b, 10000, []string{"http.request.method", "peer.service"})
	p.SetSink(NewRegistry(Config{}))
	td := pairingBatch(10001, 100)
	b.ReportAllocs()
	for b.Loop() {
		p.Consume(td)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/200, "ns/span")
}

// A full store must stay allocation-free: the free list and the array key exist
// for exactly this, and a map that has grown to the cap must not start costing
// an allocation per lookup.
func TestConsumeSteadyStateIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	p := NewProcessor(Config{MaxItems: 40000, Wait: "10s"}, discardLog())
	p.SetSink(&countSink{})
	now := t0
	p.now = func() time.Time { return now }
	for i := 0; i < 10000; i += 500 {
		p.Consume(unpairableClientSpans(i, 500))
	}
	td := pairingBatch(10001, 100)
	p.Consume(td) // warm the free list and the join scratch
	if n := testing.AllocsPerRun(50, func() { p.Consume(td) }); n != 0 {
		t.Errorf("Consume allocates %v times per 200-span batch against a full store, want 0", n)
	}
}
