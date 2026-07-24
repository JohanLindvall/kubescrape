package tracesample

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type capExporter struct {
	batches []ptrace.Traces
	err     error
}

func (c *capExporter) ExportTraces(_ context.Context, td ptrace.Traces) error {
	if c.err != nil {
		return c.err
	}
	c.batches = append(c.batches, td)
	return nil
}

func (c *capExporter) spans() int {
	n := 0
	for _, td := range c.batches {
		n += td.SpanCount()
	}
	return n
}

// payload builds n spans with distinct trace IDs; err/slow flags mark span 0.
func payload(n int, withErr bool, slow time.Duration) ptrace.Traces {
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	base := time.Unix(1000, 0)
	for i := 0; i < n; i++ {
		sp := ss.Spans().AppendEmpty()
		var id pcommon.TraceID
		id[0] = byte(i >> 8)
		id[1] = byte(i)
		id[15] = 0xaa
		sp.SetTraceID(id)
		sp.SetStartTimestamp(pcommon.NewTimestampFromTime(base))
		end := base.Add(time.Millisecond)
		if i == 0 && slow > 0 {
			end = base.Add(slow)
		}
		sp.SetEndTimestamp(pcommon.NewTimestampFromTime(end))
		if i == 0 && withErr {
			sp.Status().SetCode(ptrace.StatusCodeError)
		}
	}
	return td
}

func TestProbabilisticConsistentAndProportional(t *testing.T) {
	next := &capExporter{}
	s := New(Config{Probability: 0.25}, next)

	if err := s.ExportTraces(context.Background(), payload(4000, false, 0)); err != nil {
		t.Fatal(err)
	}
	kept := next.spans()
	if kept < 800 || kept > 1200 {
		t.Fatalf("kept %d of 4000 at p=0.25, want ~1000", kept)
	}

	// Deterministic: the identical payload samples identically (a sender
	// retry must not re-roll the dice).
	next2 := &capExporter{}
	s2 := New(Config{Probability: 0.25}, next2)
	if err := s2.ExportTraces(context.Background(), payload(4000, false, 0)); err != nil {
		t.Fatal(err)
	}
	if next2.spans() != kept {
		t.Fatalf("resample differs: %d vs %d", next2.spans(), kept)
	}
}

func TestGuardRailsKeepErrorsAndSlowSpans(t *testing.T) {
	next := &capExporter{}
	s := New(Config{Probability: 0.0000001, KeepSlowerThan: time.Second}, next)

	td := payload(3, true, 2*time.Second) // span 0 is BOTH error and slow
	if err := s.ExportTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	if next.spans() != 1 {
		t.Fatalf("kept %d, want exactly the error/slow span", next.spans())
	}
}

func TestAllSampledAwayAcksWithoutSend(t *testing.T) {
	next := &capExporter{}
	falseV := false
	s := New(Config{Probability: 0.0000001, KeepErrors: &falseV}, next)
	if err := s.ExportTraces(context.Background(), payload(10, true, 0)); err != nil {
		t.Fatal(err)
	}
	if len(next.batches) != 0 {
		t.Fatalf("empty payload forwarded: %d batches", len(next.batches))
	}
}

func TestRateCapBoundsSpansAndRefills(t *testing.T) {
	next := &capExporter{}
	s := New(Config{MaxSpansPerSecond: 10}, next)
	now := time.Unix(0, 0)
	s.now = func() time.Time { return now }

	if err := s.ExportTraces(context.Background(), payload(100, false, 0)); err != nil {
		t.Fatal(err)
	}
	if got := next.spans(); got != 10 {
		t.Fatalf("kept %d, want the 10-span burst", got)
	}
	now = now.Add(500 * time.Millisecond) // refills 5 tokens
	if err := s.ExportTraces(context.Background(), payload(100, false, 0)); err != nil {
		t.Fatal(err)
	}
	if got := next.spans(); got != 15 {
		t.Fatalf("kept %d total, want 15 after a half-second refill", got)
	}
}

func TestForwardErrorPropagates(t *testing.T) {
	next := &capExporter{err: context.DeadlineExceeded}
	s := New(Config{Probability: 0.5}, next)
	if err := s.ExportTraces(context.Background(), payload(100, false, 0)); err == nil {
		t.Fatal("forward failure must propagate (the sender owns the retry)")
	}
}

// The sampler must NEVER mutate its input: the spanmetrics tap sits above it
// and aggregates from the payload after a successful forward — an in-place
// prune would derive RED metrics from the sampled subset only.
func TestInputPayloadNotMutated(t *testing.T) {
	next := &capExporter{}
	s := New(Config{Probability: 0.25}, next)
	td := payload(1000, false, 0)
	if err := s.ExportTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	if td.SpanCount() != 1000 {
		t.Fatalf("input mutated: %d spans left of 1000", td.SpanCount())
	}
	if next.spans() >= 1000 || next.spans() == 0 {
		t.Fatalf("forwarded %d, want a sampled subset", next.spans())
	}
	// All-kept fast path forwards without copying and without mutation.
	next2 := &capExporter{}
	s2 := New(Config{Probability: 1}, next2)
	td2 := payload(10, false, 0)
	if err := s2.ExportTraces(context.Background(), td2); err != nil {
		t.Fatal(err)
	}
	if td2.SpanCount() != 10 || next2.spans() != 10 {
		t.Fatalf("all-kept path: input=%d forwarded=%d", td2.SpanCount(), next2.spans())
	}
}
