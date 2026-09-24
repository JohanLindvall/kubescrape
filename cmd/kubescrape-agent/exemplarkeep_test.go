package main

import (
	"context"
	"encoding/binary"
	"log/slog"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/servicegraph"
	"github.com/JohanLindvall/kubescrape/internal/agent/tracehash"
	"github.com/JohanLindvall/kubescrape/internal/agent/tracesample"
)

// The span-metrics generator and the service-graph Registry sit ABOVE the head
// sampler in the owner chain so they count every request, and both attach
// exemplars — links to traces. buildOwnerChain is what hands each of them the
// sampler's decision; without it, at probability 0.1, nine exemplars in ten
// named a trace the sampler dropped. This drives the REAL chain, so a lost
// wiring line fails here even though both packages' own tests still pass.
func TestOwnerChainGivesBothAggregatorsTheSamplersExemplarDecision(t *testing.T) {
	oldSM := *spanMetrics
	*spanMetrics = true
	t.Cleanup(func() { *spanMetrics = oldSM })

	reg := servicegraph.NewRegistry(servicegraph.Config{}, slog.New(slog.DiscardHandler))
	proc := servicegraph.NewProcessor(servicegraph.Config{}, reg, slog.New(slog.DiscardHandler))

	keepErrors := false
	out := &chainOut{}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	p := &pipelines{
		wg: &wg, log: slog.New(slog.DiscardHandler), out: out, selfOut: out,
		fileCfg:         agentConfig{TraceSampling: &tracesample.Config{Probability: 0.1, KeepErrors: &keepErrors}},
		serviceGraphReg: reg,
	}
	chain, err := p.buildOwnerChain(ctx, proc)
	if err != nil {
		t.Fatalf("buildOwnerChain: %v", err)
	}
	if p.spanMetricsGen == nil {
		t.Fatal("the span-metrics generator was not built")
	}

	threshold := tracehash.Threshold(0.1)
	var sgExemplars, smExemplars, sampledAway int
	for i := range 500 {
		var tid pcommon.TraceID
		binary.BigEndian.PutUint64(tid[:8], uint64(i)*0x9E3779B97F4A7C15)
		binary.BigEndian.PutUint64(tid[8:], uint64(i)+1)
		if !tracehash.Keep(tid, threshold) {
			sampledAway++
		}
		if err := chain.ExportTraces(ctx, clientServerPair(tid)); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}

		sg := &metricsCapture{}
		if err := reg.Export(ctx, sg, pcommon.NewResource()); err != nil {
			t.Fatal(err)
		}
		sm := &metricsCapture{}
		if err := p.spanMetricsGen.Export(ctx, sm, pcommon.NewResource()); err != nil {
			t.Fatal(err)
		}
		for _, c := range []struct {
			what string
			md   pmetric.Metrics
			n    *int
		}{{"service graph", sg.md, &sgExemplars}, {"span metrics", sm.md, &smExemplars}} {
			for _, id := range exemplarTraceIDs(c.md) {
				*c.n++
				if !tracehash.Keep(id, threshold) {
					t.Fatalf("push %d: a %s exemplar names trace %v, which the head sampler dropped", i, c.what, id)
				}
			}
		}
	}
	if sgExemplars == 0 || smExemplars == 0 || sampledAway == 0 {
		t.Fatalf("service-graph exemplars %d, span-metric exemplars %d, sampled-away traces %d: the fixture exercises neither side",
			sgExemplars, smExemplars, sampledAway)
	}
}

// clientServerPair is one request of trace tid: a CLIENT span in checkout and
// the SERVER span it called in orders, which the pairing store completes into
// an edge.
func clientServerPair(tid pcommon.TraceID) ptrace.Traces {
	td := ptrace.NewTraces()
	start := pcommon.NewTimestampFromTime(time.Unix(1, 0))
	end := pcommon.NewTimestampFromTime(time.Unix(1, int64(50*time.Millisecond)))
	for i, svc := range []string{"checkout", "orders"} {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", svc)
		sp := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		sp.SetTraceID(tid)
		sp.SetStartTimestamp(start)
		sp.SetEndTimestamp(end)
		if i == 0 {
			sp.SetKind(ptrace.SpanKindClient)
			sp.SetSpanID(pcommon.SpanID{1})
		} else {
			sp.SetKind(ptrace.SpanKindServer)
			sp.SetSpanID(pcommon.SpanID{2})
			sp.SetParentSpanID(pcommon.SpanID{1})
		}
	}
	return td
}

// exemplarTraceIDs is every exemplar's trace id on every histogram point in md.
func exemplarTraceIDs(md pmetric.Metrics) []pcommon.TraceID {
	var out []pcommon.TraceID
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				if ms.At(k).Type() != pmetric.MetricTypeHistogram {
					continue
				}
				dps := ms.At(k).Histogram().DataPoints()
				for d := 0; d < dps.Len(); d++ {
					exs := dps.At(d).Exemplars()
					for e := 0; e < exs.Len(); e++ {
						out = append(out, exs.At(e).TraceID())
					}
				}
			}
		}
	}
	return out
}
