package tracesample

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// failThenAccept fails the first send and accepts every later one — the shape a
// collector back-pressure window has from the sampler's side: the push fails,
// the sender re-pushes the identical payload, the second attempt lands.
type failThenAccept struct {
	sends int
	kept  int
}

func (f *failThenAccept) ExportTraces(_ context.Context, td ptrace.Traces) error {
	f.sends++
	if f.sends == 1 {
		return errors.New("collector unavailable")
	}
	f.kept += td.SpanCount()
	return nil
}

// The drops of a payload whose forward FAILED must not be counted: the sender
// re-pushes the identical payload, the probability decision is deterministic, so
// the same spans are dropped again and the counter reports one full sampling
// loss per retry. Forward first, count on success — the rule the spanmetrics tap
// above this sampler already follows.
func TestDropsAreCountedOnlyAfterASuccessfulForward(t *testing.T) {
	next := &failThenAccept{}
	s := New(Config{Probability: 0.5}, next)
	td := payload(200, false, 0)

	before := obs.TraceSpansDropped.WithLabelValues("probability").Value()
	if err := s.ExportTraces(context.Background(), td); err == nil {
		t.Fatal("the failing forward must surface to the sender")
	}
	if err := s.ExportTraces(context.Background(), td); err != nil {
		t.Fatalf("the retry: %v", err)
	}
	got := obs.TraceSpansDropped.WithLabelValues("probability").Value()

	want := float64(200 - next.kept)
	if want == 0 {
		t.Fatal("the payload lost nothing to probability; the test proves nothing")
	}
	if got-before != want {
		t.Fatalf("counted %v dropped spans over a failed push and its retry, want %v (the failed pass counted its drops before the forward's outcome was known)", got-before, want)
	}
}

// A payload sampled away ENTIRELY is acked without a send, so there is no
// forward outcome to wait on and its drops are final — they must still be
// counted.
func TestDropsAreCountedWhenNothingIsLeftToSend(t *testing.T) {
	next := &capExporter{}
	s := New(Config{Probability: 0.5}, next)
	// Probability 0 keeps nothing; the guard rails are off (no error status, no
	// slow span), so every span of the payload goes.
	s.threshold = 0

	before := obs.TraceSpansDropped.WithLabelValues("probability").Value()
	if err := s.ExportTraces(context.Background(), payload(50, false, 0)); err != nil {
		t.Fatal(err)
	}
	if next.spans() != 0 {
		t.Fatalf("%d spans were forwarded by a sampler keeping nothing", next.spans())
	}
	if got := obs.TraceSpansDropped.WithLabelValues("probability").Value() - before; got != 50 {
		t.Fatalf("counted %v dropped spans, want 50", got)
	}
}
