package tracesample

import (
	"context"
	"math"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"sigs.k8s.io/yaml"
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
	for i := range n {
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

// keepErrors, isolated from keepSlowerThan: an ERROR span is kept at a
// probability that keeps nothing else, and only while the guard rail is on —
// which it is by DEFAULT. The test this replaced used one span that was both
// an error and slow, so either arm of keep() could be deleted with every test
// in this package (and tailbuffer's and tailsample's) still passing.
func TestKeepErrorsGuardRail(t *testing.T) {
	off := false
	for _, tc := range []struct {
		name       string
		keepErrors *bool
		withErr    bool
		want       int
	}{
		{"default keeps the error span", nil, true, 1},
		{"keepErrors false samples it like any other", &off, true, 0},
		{"no error span, nothing kept", nil, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := &capExporter{}
			s := New(Config{Probability: 0.0000001, KeepErrors: tc.keepErrors}, next)
			if err := s.ExportTraces(context.Background(), payload(3, tc.withErr, 0)); err != nil {
				t.Fatal(err)
			}
			if got := next.spans(); got != tc.want {
				t.Fatalf("kept %d, want %d", got, tc.want)
			}
		})
	}
}

// KeepsErrors is the answer configWarnings reads, so it must be the one the
// sampler arms — including the unset-means-on default, which is what a
// re-derived copy of it would drift on.
func TestKeepsErrorsIsWhatTheSamplerArms(t *testing.T) {
	on, off := true, false
	for _, tc := range []struct {
		name       string
		keepErrors *bool
		want       bool
	}{
		{"unset defaults on", nil, true},
		{"explicit true", &on, true},
		{"explicit false", &off, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Probability: 0.5, KeepErrors: tc.keepErrors}
			if got := cfg.KeepsErrors(); got != tc.want {
				t.Fatalf("KeepsErrors() = %v, want %v", got, tc.want)
			}
			if got := New(cfg, &capExporter{}).keepErr; got != tc.want {
				t.Fatalf("the sampler armed keepErrors = %v, KeepsErrors() says %v", got, tc.want)
			}
		})
	}
}

// keepSlowerThan, isolated from keepErrors (off here): a span at or above the
// threshold is kept, one below it is sampled like any other.
func TestKeepSlowerThanGuardRail(t *testing.T) {
	off := false
	for _, tc := range []struct {
		name string
		slow time.Duration // span 0's duration; 0 = 1ms like the rest
		want int
	}{
		{"a slower span is kept", 2 * time.Second, 1},
		{"the threshold itself is kept (>=)", time.Second, 1},
		{"a faster span is sampled like any other", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := &capExporter{}
			s := New(Config{Probability: 0.0000001, KeepErrors: &off, KeepSlowerThan: "1s"}, next)
			if err := s.ExportTraces(context.Background(), payload(3, false, tc.slow)); err != nil {
				t.Fatal(err)
			}
			if got := next.spans(); got != tc.want {
				t.Fatalf("kept %d, want %d", got, tc.want)
			}
		})
	}
}

// A span that ENDS before it starts (unfinished, or a clock that stepped) is
// not slow either, however the timestamps sit. A modest inversion converts to a
// negative duration on its own; an EXTREME one — a garbage start near the top
// of the uint64 range — wraps end-start around to a small POSITIVE number, and
// only the end > start guard keeps that from reading as a slow span that must
// be kept at any probability.
func TestKeepSlowerThanIgnoresASpanThatEndsBeforeItStarts(t *testing.T) {
	off := false
	for _, tc := range []struct {
		name       string
		start, end pcommon.Timestamp
	}{
		{"five seconds inverted", pcommon.Timestamp(20 * time.Second), pcommon.Timestamp(15 * time.Second)},
		// end-start wraps to 20s + 11ns: past the 10s threshold if measured.
		{"a start near the top of the range", pcommon.Timestamp(math.MaxUint64 - 10), pcommon.Timestamp(20 * time.Second)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := &capExporter{}
			s := New(Config{Probability: 0.0000001, KeepErrors: &off, KeepSlowerThan: "10s"}, next)
			td := payload(1000, false, 0)
			sps := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
			for i := 0; i < sps.Len(); i++ {
				sps.At(i).SetStartTimestamp(tc.start)
				sps.At(i).SetEndTimestamp(tc.end)
			}
			if err := s.ExportTraces(context.Background(), td); err != nil {
				t.Fatal(err)
			}
			if got := next.spans(); got != 0 {
				t.Fatalf("kept %d of 1000 spans that end before they start, at probability 1e-7", got)
			}
		})
	}
}

// A span with no start timestamp (0 = unknown in OTLP) is not "slow": measured
// anyway it spans from the Unix epoch to its end, ~56 years, and every such
// span was kept whatever the probability. tailsample's traceDuration skips
// start-less spans for the same reason.
func TestKeepSlowerThanIgnoresASpanWithNoStart(t *testing.T) {
	off := false
	next := &capExporter{}
	s := New(Config{Probability: 0.0000001, KeepErrors: &off, KeepSlowerThan: "10s"}, next)
	td := payload(1000, false, 0)
	sps := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	for i := 0; i < sps.Len(); i++ {
		sps.At(i).SetStartTimestamp(0)
	}
	if err := s.ExportTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	if got := next.spans(); got != 0 {
		t.Fatalf("kept %d of 1000 start-less spans at probability 1e-7: a missing start read as ~56 years", got)
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

// keepSlowerThan is documented as a Go duration string ("2s"). The config is
// decoded through sigs.k8s.io/yaml -> encoding/json, which cannot unmarshal a
// string into time.Duration — so a time.Duration-typed field made the
// DOCUMENTED spelling a fatal startup error (the loader is strict).
func TestKeepSlowerThanAcceptsDurationString(t *testing.T) {
	var c Config
	if err := yaml.UnmarshalStrict([]byte("keepSlowerThan: 2s\n"), &c); err != nil {
		t.Fatalf("the documented spelling failed to decode: %v", err)
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	got, err := c.SlowerThan()
	if err != nil || got != 2*time.Second {
		t.Fatalf("SlowerThan = %v, %v; want 2s", got, err)
	}
	// A malformed value must fail startup, not silently disable the guard rail.
	var bad Config
	if err := yaml.UnmarshalStrict([]byte("keepSlowerThan: 2quarters\n"), &bad); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := bad.Validate(); err == nil {
		t.Fatal("a malformed keepSlowerThan must be rejected at startup")
	}
}

// The all-kept fast path must PAY for what it forwards. It used to peek the
// bucket without consuming, so every payload smaller than the current fill
// bypassed the cap and the bucket refilled to full before the next one — a
// 10/s cap forwarding 100 spans/s with reason="rate" at 0. The cap only bound
// payloads that were ALSO losing spans to probability.
func TestRateCapBindsWhenNothingIsSampledAway(t *testing.T) {
	next := &capExporter{}
	s := New(Config{MaxSpansPerSecond: 10}, next) // probability defaults to keep-all
	now := time.Unix(0, 0)
	s.now = func() time.Time { return now }

	// Twenty payloads of five spans each, all within one second: 100 spans
	// offered against a 10/s cap with a 10-span burst.
	for range 20 {
		if err := s.ExportTraces(context.Background(), payload(5, false, 0)); err != nil {
			t.Fatal(err)
		}
	}
	if got := next.spans(); got > 10 {
		t.Errorf("forwarded %d spans under a 10/s cap with a 10-span burst; the fast path never debited the bucket", got)
	}

	// And the cap still refills: a second later the next payload goes through.
	now = now.Add(time.Second)
	before := next.spans()
	if err := s.ExportTraces(context.Background(), payload(5, false, 0)); err != nil {
		t.Fatal(err)
	}
	if next.spans() == before {
		t.Error("nothing forwarded after a full second of refill; the cap is now stuck shut")
	}
}
