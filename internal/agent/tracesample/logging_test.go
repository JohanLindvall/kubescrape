package tracesample

// The spans/second cap, seen from the operator's side. The probability decision
// is the feature working and is deliberately never logged; the rate cap is a
// safety valve, and when it bites the sampling is no longer what was configured
// AND traces ship in halves (the cap is per span, not per trace).

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

func capturedLog() (*slog.Logger, func() string) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h), buf.String
}

type sink struct{ spans int }

func (s *sink) ExportTraces(_ context.Context, td ptrace.Traces) error {
	s.spans += td.SpanCount()
	return nil
}

// batch builds n spans of distinct traces.
func batch(n int) ptrace.Traces {
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	for i := 0; i < n; i++ {
		sp := ss.Spans().AppendEmpty()
		var id [16]byte
		id[15] = byte(i + 1)
		sp.SetTraceID(id)
		sp.SetName("op")
	}
	return td
}

func TestRateCapIsWarnedOnceWithTheKnob(t *testing.T) {
	log, dump := capturedLog()
	s := New(Config{MaxSpansPerSecond: 2, Logger: log}, &sink{})
	s.now = func() time.Time { return time.Unix(1000, 0) } // no refill

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := s.ExportTraces(ctx, batch(8)); err != nil {
			t.Fatal(err)
		}
	}
	out := dump()
	if n := strings.Count(out, "maxSpansPerSecond cap is dropping spans"); n != 1 {
		t.Errorf("want one throttled warning, got %d:\n%s", n, out)
	}
	if !strings.Contains(out, "maxSpansPerSecond=2") {
		t.Errorf("the warning does not name the knob to raise:\n%s", out)
	}
}

// Probability drops are the configured behaviour and must never produce a line:
// at probability 0.1 that would be nine lines in ten, forever.
func TestProbabilityDropsAreSilent(t *testing.T) {
	log, dump := capturedLog()
	s := New(Config{Probability: 0.0001, Logger: log}, &sink{})
	if err := s.ExportTraces(context.Background(), batch(64)); err != nil {
		t.Fatal(err)
	}
	if out := dump(); out != "" {
		t.Errorf("the probability sampler logged:\n%s", out)
	}
}

// Config.Validate refuses a percent-vs-fraction probability, so a start that
// reaches New with one has got past validation — and what New then does is ship
// 100% of spans. That is the expensive direction to be silent about.
func TestOutOfRangeProbabilityIsWarned(t *testing.T) {
	log, dump := capturedLog()
	New(Config{Probability: 50, Logger: log}, &sink{})
	if !strings.Contains(dump(), "not a fraction") {
		t.Errorf("an out-of-range probability was accepted silently:\n%s", dump())
	}
}

// The zero value means "unset", not "out of range": it must not warn.
func TestUnsetProbabilityIsSilent(t *testing.T) {
	log, dump := capturedLog()
	New(Config{Logger: log}, &sink{})
	if out := dump(); out != "" {
		t.Errorf("an unset probability logged:\n%s", out)
	}
}
