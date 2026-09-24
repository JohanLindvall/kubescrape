package tracesample

import (
	"encoding/binary"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/tracehash"
)

// SpanKept and TraceKept answer "will this ship?" for the exemplar-bearing
// aggregators above the sampler, so they must agree with what ExportTraces
// actually forwards: SpanKept exactly (guard rails included), TraceKept as the
// probability alone (never true for a trace the probability drops).
func TestKeptPredicatesAgreeWithWhatShips(t *testing.T) {
	keepErrors := true
	next := &sink{}
	s := New(Config{Probability: 0.1, KeepErrors: &keepErrors}, next)
	threshold := tracehash.Threshold(0.1)

	for i := range 2000 {
		var id [16]byte
		binary.BigEndian.PutUint64(id[:8], uint64(i)*0x9E3779B97F4A7C15)
		binary.BigEndian.PutUint64(id[8:], uint64(i))
		td := ptrace.NewTraces()
		sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		sp.SetTraceID(pcommon.TraceID(id))
		if i%7 == 0 {
			sp.Status().SetCode(ptrace.StatusCodeError) // a guard-rail keep
		}
		next.spans = 0
		if err := s.ExportTraces(t.Context(), td); err != nil {
			t.Fatal(err)
		}
		shipped := next.spans == 1
		if got := s.SpanKept(sp); got != shipped {
			t.Fatalf("span %d: SpanKept = %v, but it shipped = %v", i, got, shipped)
		}
		if got, want := s.TraceKept(sp.TraceID()), tracehash.Keep(sp.TraceID(), threshold); got != want {
			t.Fatalf("trace %d: TraceKept = %v, want the probability decision %v", i, got, want)
		}
		if s.TraceKept(sp.TraceID()) && !shipped {
			t.Fatalf("trace %d: TraceKept is true for a span that did not ship", i)
		}
	}
}
