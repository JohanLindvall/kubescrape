package main

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/servicegraph"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailbuffer"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailsample"
)

// chainOut is p.out for the chain tests: the collector end of the owner chain.
type chainOut struct {
	captureTraces
}

func (*chainOut) ExportLogs(context.Context, plog.Logs) error          { return nil }
func (*chainOut) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }

// tailCfg is a policy list keeping only traces with an ERROR span, plus a window
// long enough that nothing decides itself during the test.
func tailCfg() *tailbuffer.Config {
	return &tailbuffer.Config{
		Config: tailsample.Config{Policies: []tailsample.PolicyConfig{{
			Name: "errors", Type: tailsample.TypeStatusCode,
			StatusCode: &tailsample.StatusCodeConfig{StatusCodes: []string{"ERROR"}},
		}}},
		DecisionWait: "1h",
	}
}

func tailChain(t *testing.T, proc *servicegraph.Processor) (*pipelines, servicegraph.TracesExporter, *chainOut, func()) {
	t.Helper()
	out := &chainOut{}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	p := &pipelines{
		wg: &wg, log: slog.New(slog.DiscardHandler), out: out,
		fileCfg: agentConfig{TailSampling: tailCfg()},
	}
	chain, err := p.buildOwnerChain(ctx, proc)
	if err != nil {
		cancel()
		t.Fatalf("buildOwnerChain: %v", err)
	}
	return p, chain, out, func() { cancel(); wg.Wait() }
}

// The composition rule the whole ordering exists for: edge pairing (and, when it
// is on, the span-metrics tap) counts REQUESTS, so it must see every span —
// including the ones tail sampling is about to throw away. Tail sampling is
// therefore the LAST thing above the exporter, and a span it drops has already
// been counted by everything above it.
func TestTailSamplingSitsBelowThePairTap(t *testing.T) {
	proc := servicegraph.NewProcessor(servicegraph.Config{}, slog.New(slog.DiscardHandler))
	p, chain, out, stop := tailChain(t, proc)
	defer stop()

	// A CLIENT span with no error: the policy list will drop this trace.
	if err := chain.ExportTraces(context.Background(), oneClientSpan()); err != nil {
		t.Fatalf("ExportTraces: %v", err)
	}
	if got := proc.Stats().Items; got != 1 {
		t.Fatalf("the pairing store took %d half-edges, want 1 — the graph must see spans tail sampling drops", got)
	}
	if got := out.spans(); got != 0 {
		t.Fatalf("%d spans reached the collector before the decision window closed", got)
	}
	if got := p.tailBuffer.Stats().Spans; got != 1 {
		t.Fatalf("the tail buffer holds %d spans, want 1", got)
	}

	// The window never elapses here; the shutdown flush decides it, and the
	// verdict is drop.
	p.tailBuffer.Flush(context.Background())
	if got := out.spans(); got != 0 {
		t.Fatalf("%d spans reached the collector for a trace no policy matched", got)
	}
}

// And the keep half, through the same chain: the flush is what makes a graceful
// stop lose nothing (see agent/tailbuffer's package doc on why a hard kill
// does).
func TestShutdownFlushExportsTheKeeps(t *testing.T) {
	proc := servicegraph.NewProcessor(servicegraph.Config{}, slog.New(slog.DiscardHandler))
	p, chain, out, stop := tailChain(t, proc)
	defer stop()

	td := oneClientSpan()
	td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Status().SetCode(ptrace.StatusCodeError)
	if err := chain.ExportTraces(context.Background(), td); err != nil {
		t.Fatalf("ExportTraces: %v", err)
	}
	if got := out.spans(); got != 0 {
		t.Fatalf("%d spans reached the collector before the decision window closed", got)
	}
	p.tailBuffer.Flush(context.Background())
	if got := out.spans(); got != 1 {
		t.Fatalf("the shutdown flush exported %d spans, want 1 (buffered spans are acked to their senders — a graceful stop must not drop them)", got)
	}
}

// Off unless configured: the tier must not start buffering traces because the
// binary can.
func TestTailSamplingIsOffWithoutPolicies(t *testing.T) {
	proc := servicegraph.NewProcessor(servicegraph.Config{}, slog.New(slog.DiscardHandler))
	out := &chainOut{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	p := &pipelines{wg: &wg, log: slog.New(slog.DiscardHandler), out: out}
	chain, err := p.buildOwnerChain(ctx, proc)
	if err != nil {
		t.Fatalf("buildOwnerChain: %v", err)
	}
	if p.tailBuffer != nil {
		t.Fatal("a tail-sampling buffer was built for a config with no tailSampling section")
	}
	if err := chain.ExportTraces(ctx, oneClientSpan()); err != nil {
		t.Fatalf("ExportTraces: %v", err)
	}
	if got := out.spans(); got != 1 {
		t.Fatalf("%d spans reached the collector, want 1 (nothing should be buffered)", got)
	}
	cancel()
	wg.Wait()
}
