package tracesample

import (
	"context"
	"log/slog"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// The sampler runs on the trace tier's per-push path, so its cost is per SPAN
// of everything the cluster sends. The fixtures below are what an instrumented
// service actually pushes — a resource carrying the tier's enrichment and spans
// with the attribute and event count otelhttp emits — because the whole cost of
// the sampling path is COPYING spans, and a span with no attributes costs
// almost nothing to copy. Benchmarking the bare shape would have said the path
// was free.

// benchResource is the resource shape the tier's enricher leaves on a pushed
// payload: the sender's own service identity plus the k8s attributes.
func benchResource(res pcommon.Resource) {
	a := res.Attributes()
	a.PutStr("service.name", "checkout")
	a.PutStr("service.namespace", "shop")
	a.PutStr("service.instance.id", "shop/checkout-7d9f8c6b5d-x2k9p")
	a.PutStr("service.version", "2.14.3")
	a.PutStr("k8s.namespace.name", "shop")
	a.PutStr("k8s.pod.name", "checkout-7d9f8c6b5d-x2k9p")
	a.PutStr("k8s.pod.uid", "6f1a2b3c-4d5e-6f70-8192-a3b4c5d6e7f8")
	a.PutStr("k8s.container.name", "checkout")
	a.PutStr("k8s.node.name", "ip-10-42-3-17.eu-north-1.compute.internal")
	a.PutStr("k8s.deployment.name", "checkout")
	a.PutStr("cloud.region", "eu-north-1")
	a.PutStr("telemetry.sdk.language", "go")
	a.PutStr("telemetry.sdk.name", "opentelemetry")
	a.PutStr("telemetry.sdk.version", "1.32.0")
}

// benchSpan is a typical instrumented HTTP span: 14 attributes, 2 events.
func benchSpan(s ptrace.Span, tid pcommon.TraceID, i int) {
	s.SetTraceID(tid)
	var sid pcommon.SpanID
	sid[0], sid[1] = byte(i>>8), byte(i)
	s.SetSpanID(sid)
	s.SetKind(ptrace.SpanKindClient)
	s.SetName("GET /api/v2/orders/{orderId}/shipments")
	s.SetStartTimestamp(pcommon.Timestamp(1_750_000_000_000_000_000))
	s.SetEndTimestamp(pcommon.Timestamp(1_750_000_000_042_000_000))
	a := s.Attributes()
	a.PutStr("http.request.method", "GET")
	a.PutStr("url.full", "https://orders.shop.svc.cluster.local/api/v2/orders/8f3a/shipments?expand=carrier")
	a.PutStr("url.path", "/api/v2/orders/8f3a/shipments")
	a.PutStr("url.scheme", "https")
	a.PutStr("server.address", "orders.shop.svc.cluster.local")
	a.PutInt("server.port", 8443)
	a.PutInt("http.response.status_code", 200)
	a.PutStr("user_agent.original", "checkout/2.14.3 (linux; go1.24.2) otelhttp/0.58.0")
	a.PutStr("network.protocol.version", "1.1")
	a.PutStr("peer.service", "orders")
	a.PutStr("enduser.id", "usr_01HQ8ZK3M4N5P6Q7R8S9T0V1W2")
	a.PutStr("shop.tenant", "acme-nordics")
	a.PutBool("http.request.resend", false)
	a.PutDouble("http.request.body.size", 0)
	e := s.Events().AppendEmpty()
	e.SetName("exception")
	e.Attributes().PutStr("exception.type", "net/http: request canceled (Client.Timeout exceeded)")
	e.Attributes().PutStr("exception.stacktrace", "goroutine 421 [running]:\nnet/http.(*Client).do(...)\n\t/usr/local/go/src/net/http/client.go:724")
	e2 := s.Events().AppendEmpty()
	e2.SetName("retry")
	e2.Attributes().PutInt("retry.attempt", 3)
}

// benchPayload is an SDK batch: `traces` traces of `perTrace` spans each, one
// resource and one scope, which is what a single service's BatchSpanProcessor
// emits.
func benchPayload(traces, perTrace int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	benchResource(rs.Resource())
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp")
	ss.Scope().SetVersion("0.58.0")
	n := 0
	for t := 0; t < traces; t++ {
		var tid pcommon.TraceID
		tid[0], tid[1] = byte(t>>8), byte(t)
		tid[15] = 0xaa
		for p := 0; p < perTrace; p++ {
			benchSpan(ss.Spans().AppendEmpty(), tid, n)
			n++
		}
	}
	return td
}

type nopExporter struct{}

func (nopExporter) ExportTraces(context.Context, ptrace.Traces) error { return nil }

// BenchmarkExportTraces prices the sampler at the probabilities an operator
// actually sets. The 1.0 arm is the all-kept fast path (no copy at all); the
// others take the copying path, which is where every span of the cluster's
// traffic is paid for.
func BenchmarkExportTraces(b *testing.B) {
	ctx := context.Background()
	for _, p := range []struct {
		name string
		prob float64
	}{
		{"keep-all", 1},
		{"prob-0.5", 0.5},
		{"prob-0.1", 0.1},
		{"prob-0.01", 0.01},
	} {
		b.Run(p.name, func(b *testing.B) {
			s := New(Config{Probability: p.prob, Logger: slog.New(slog.DiscardHandler)}, nopExporter{})
			td := benchPayload(40, 5) // 200 spans, the shape an SDK batches
			b.ReportAllocs()
			for b.Loop() {
				if err := s.ExportTraces(ctx, td); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(td.SpanCount()), "ns/span")
		})
	}
}

// The all-kept fast path forwards the caller's own payload: it must allocate
// nothing at all, or the commonest configuration (sampling off, the rate cap as
// a pure safety valve) pays a payload copy for nothing.
func TestExportAllKeptIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	s := New(Config{Probability: 1}, nopExporter{})
	td := benchPayload(40, 5)
	ctx := context.Background()
	if n := testing.AllocsPerRun(50, func() {
		if err := s.ExportTraces(ctx, td); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Errorf("all-kept ExportTraces allocates %v times per payload, want 0", n)
	}
}

// The sampling path's cost must follow the spans that SHIP, not the spans that
// arrive. It used to deep-copy the whole payload and then delete the drops out
// of it, so `probability: 0.01` paid the identical 4822 allocations per 200-span
// batch that `probability: 1` would have — on the trace tier's per-push path,
// for every span the cluster emits.
//
// The bound is expressed against the cost of copying the WHOLE payload,
// measured here rather than written down, so it pins the PROPERTY (the copy is
// proportional to what survives) instead of one machine's number and cannot be
// invalidated by a pdata release changing what a span copy costs. At
// probability 0.1 roughly a tenth of the spans survive and the measured figure
// is 266 allocations against 4822 for the whole-payload copy, so an EIGHTH is
// the ceiling — the shape this replaced exceeded the whole-payload copy
// outright, so the bound has two and a half times the headroom it needs and
// still fails that shape by a factor of eight.
func TestSampledCopyIsProportionalToKeptSpans(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	td := benchPayload(40, 5)
	whole := testing.AllocsPerRun(20, func() {
		out := ptrace.NewTraces()
		td.CopyTo(out)
	})
	s := New(Config{Probability: 0.1}, nopExporter{})
	ctx := context.Background()
	got := testing.AllocsPerRun(20, func() {
		if err := s.ExportTraces(ctx, td); err != nil {
			t.Fatal(err)
		}
	})
	if limit := whole / 8; got > limit {
		t.Errorf("sampling a 200-span payload at probability 0.1 allocates %v times, want at most %v (an eighth of the %v a whole-payload copy costs)", got, limit, whole)
	}
}
