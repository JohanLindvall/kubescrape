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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.Consume(td)
	}
}
