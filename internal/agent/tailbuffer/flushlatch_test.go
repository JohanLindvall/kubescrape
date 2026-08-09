package tailbuffer

// The flushed latch: after the shutdown Flush has drained the buffer, nothing
// will ever flush it again — main runs Flush exactly once, after ASKING the
// receivers to stop. Asking is weaker than stopped: http.Server.Shutdown never
// interrupts an active handler (a trickled body has the whole 60s ReadTimeout),
// and the producer-join budget can expire while gRPC's GracefulStop still waits
// on a slow RPC. So a straggler push can complete its take() AFTER the flush.
// Before the latch those spans were buffered, acked, and lost in silence — no
// counter moved, no warn fired, and the buffered-spans gauges reported the loss
// only to a process about to exit. These tests pin the replacement contract:
// a post-Flush push is decided on the spot, on the spans present, and its ack
// is honest — its keeps went out on that same push.

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// A straggler push after Flush is decided immediately: its keeps ride out on
// the push itself, the decision is counted early{shutdown}, the verdict is
// cached (a second straggler follows it as a plain late span), and the buffer
// — and so the gauges a hard kill would be measured by — stays empty.
func TestPostFlushPushIsDecidedImmediatelyAndItsAckIsHonest(t *testing.T) {
	cap := &capture{}
	b, _ := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1m"}, cap)
	ctx := context.Background()

	// The flush that latches. An EMPTY buffer still latches: main's Flush runs
	// unconditionally, and the straggler window opens after it either way.
	b.Flush(ctx)

	early0 := counter(obs.TailSampleEarly.WithLabelValues(reasonShutdown))
	kept0 := counter(obs.TailSampleSpans.WithLabelValues("kept"))

	// The straggler: two spans of one new trace, in one push.
	if err := b.ExportTraces(ctx, payload("checkout",
		spanSpec{trace: 901, span: 1, end: 5}, spanSpec{trace: 901, span: 2, end: 5})); err != nil {
		t.Fatal(err)
	}
	if got := cap.sends(); got != 1 {
		t.Fatalf("sends = %d, want 1 (the keeps must leave on the straggler's own push)", got)
	}
	if got := cap.count(); got != 2 {
		t.Fatalf("exported %d spans, want 2", got)
	}
	if got := b.Stats(); got != (Stats{}) {
		t.Fatalf("the buffer holds %+v after a post-Flush push; nothing will ever flush it again", got)
	}
	if got := counter(obs.TailSampleEarly.WithLabelValues(reasonShutdown)) - early0; got != 1 {
		t.Fatalf("early{shutdown} counted %v, want 1 (one trace, decided once on both spans present)", got)
	}
	if got := counter(obs.TailSampleSpans.WithLabelValues("kept")) - kept0; got != 2 {
		t.Fatalf("spans{kept} counted %v, want 2", got)
	}

	// The verdict was cached: a second straggler for the same trace is an
	// ordinary late span, not a second decision.
	late0 := counter(obs.TailSampleLate.WithLabelValues("kept"))
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 901, span: 3, end: 5})); err != nil {
		t.Fatal(err)
	}
	if got := counter(obs.TailSampleLate.WithLabelValues("kept")) - late0; got != 1 {
		t.Fatalf("late{kept} counted %v, want 1 (the second straggler must follow the cached verdict)", got)
	}
	if got := counter(obs.TailSampleEarly.WithLabelValues(reasonShutdown)) - early0; got != 1 {
		t.Fatalf("early{shutdown} counted %v after the second straggler, want still 1", got)
	}
	if got := b.Stats(); got != (Stats{}) {
		t.Fatalf("the buffer holds %+v after a late span", got)
	}
}

// The straggler's spans are the SENDER's, not ours: the forwarded payload must
// not carry the otlpexport.Own marker. Owning it would spool it — acking the
// sender's push against a durability it does not need, since a NACK makes it
// retransmit — which is exactly the pass-through rule late spans already
// follow (TestLateSpansAreNotOwned).
func TestPostFlushDecidedSpansAreNotOwned(t *testing.T) {
	cap := &ownCapture{}
	b, _ := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1m"}, cap)
	ctx := context.Background()
	b.Flush(ctx)

	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 911, span: 1, end: 5})); err != nil {
		t.Fatal(err)
	}
	got := cap.marks()
	if len(got) != 1 || got[0] {
		t.Fatalf("marks = %v, want exactly one UNMARKED export: the sender still holds these spans, so a failed send is its retry's to recover, not the spool's to ack", got)
	}
}

// A post-Flush push whose forward fails is NACKed with nothing tallied — not
// kept (it did not land) and not lost (the sender still holds it) — and the
// sender's retry then follows the verdict the failed attempt already cached,
// tallying exactly once as late spans. This is the late-span discipline
// applied to the straggler: across a NACK plus its retry, one payload's worth.
func TestPostFlushPushNACKsWithoutTallyingAndTheRetryTalliesOnce(t *testing.T) {
	cap := &capture{}
	b, _ := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1m"}, cap)
	ctx := context.Background()
	b.Flush(ctx)

	early0 := counter(obs.TailSampleEarly.WithLabelValues(reasonShutdown))
	kept0 := counter(obs.TailSampleSpans.WithLabelValues("kept"))
	lost0 := counter(obs.TailSampleSpans.WithLabelValues("lost"))
	late0 := counter(obs.TailSampleLate.WithLabelValues("kept"))

	push := payload("checkout", spanSpec{trace: 921, span: 1, end: 5})
	cap.fail(errors.New("collector down"))
	if err := b.ExportTraces(ctx, push); err == nil {
		t.Fatal("a failed forward of a post-Flush straggler was acked; the sender's retry is the only recovery left")
	}
	if got := counter(obs.TailSampleSpans.WithLabelValues("kept")) - kept0; got != 0 {
		t.Fatalf("spans{kept} counted %v on a NACKed push, want 0", got)
	}
	if got := counter(obs.TailSampleSpans.WithLabelValues("lost")) - lost0; got != 0 {
		t.Fatalf("spans{lost} counted %v, want 0: the sender still holds the payload, so a NACK is not loss", got)
	}
	if got := counter(obs.TailSampleEarly.WithLabelValues(reasonShutdown)) - early0; got != 1 {
		t.Fatalf("early{shutdown} counted %v, want 1 (the decision happened and its verdict was cached even though the forward failed)", got)
	}
	if got := b.Stats(); got != (Stats{}) {
		t.Fatalf("the buffer holds %+v after a NACKed post-Flush push", got)
	}

	// The sender's retry: the same spans now follow the cached keep as late
	// spans, and land exactly once.
	cap.fail(nil)
	if err := b.ExportTraces(ctx, push); err != nil {
		t.Fatal(err)
	}
	if got := counter(obs.TailSampleLate.WithLabelValues("kept")) - late0; got != 1 {
		t.Fatalf("late{kept} = %v across the NACK plus its retry, want exactly one payload's worth (1)", got)
	}
	if got := counter(obs.TailSampleSpans.WithLabelValues("kept")) - kept0; got != 0 {
		t.Fatalf("spans{kept} = %v, want 0 (the retry rides the late path; nothing double-tallies)", got)
	}
	if got := cap.count(); got != 1 {
		t.Fatalf("exported %d distinct spans, want 1", got)
	}
}

// A post-Flush push whose trace decides DROP still ACKS, without a send: the
// decision is final (cached), the drop is counted, and NACKing would make the
// sender retry spans nothing will ever keep.
func TestPostFlushDecidedDropAcksWithoutASend(t *testing.T) {
	cap := &capture{}
	b, _ := newTestBuffer(t, Config{Config: errorsCfg(), DecisionWait: "1m"}, cap)
	ctx := context.Background()
	b.Flush(ctx)

	dropped0 := counter(obs.TailSampleSpans.WithLabelValues("dropped"))
	early0 := counter(obs.TailSampleEarly.WithLabelValues(reasonShutdown))
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 931, span: 1, end: 5})); err != nil {
		t.Fatalf("a post-Flush push decided DROP must still be acked: %v", err)
	}
	if got := cap.sends(); got != 0 {
		t.Fatalf("sends = %d, want 0 (a decided drop has nothing to forward)", got)
	}
	if got := counter(obs.TailSampleSpans.WithLabelValues("dropped")) - dropped0; got != 1 {
		t.Fatalf("spans{dropped} counted %v, want 1", got)
	}
	if got := counter(obs.TailSampleEarly.WithLabelValues(reasonShutdown)) - early0; got != 1 {
		t.Fatalf("early{shutdown} counted %v, want 1", got)
	}
	if got := b.Stats(); got != (Stats{}) {
		t.Fatalf("the buffer holds %+v", got)
	}

	// A follow-up straggler follows the cached DROP as a late drop — even one
	// that would have been kept on its own merits, which is what makes this a
	// test of the cache rather than of the policy.
	lateDrop0 := counter(obs.TailSampleLate.WithLabelValues("dropped"))
	if err := b.ExportTraces(ctx, payload("checkout",
		spanSpec{trace: 931, span: 2, end: 5, status: ptrace.StatusCodeError})); err != nil {
		t.Fatal(err)
	}
	if got := cap.sends(); got != 0 {
		t.Fatalf("sends = %d, want 0", got)
	}
	if got := counter(obs.TailSampleLate.WithLabelValues("dropped")) - lateDrop0; got != 1 {
		t.Fatalf("late{dropped} counted %v, want 1", got)
	}
}

// The latch composes with what Flush itself decided: a push carrying BOTH a
// straggler for a flush-decided trace (the late path) AND a brand-new trace
// (the post-Flush decision) forwards both in one send and tallies each on its
// own counter.
func TestPostFlushPushMixesLateSpansAndNewTraces(t *testing.T) {
	cap := &capture{}
	b, _ := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1m"}, cap)
	ctx := context.Background()

	// One trace buffered before the flush; Flush decides it (keep) and caches.
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 941, span: 1, end: 5})); err != nil {
		t.Fatal(err)
	}
	b.Flush(ctx)

	late0 := counter(obs.TailSampleLate.WithLabelValues("kept"))
	kept0 := counter(obs.TailSampleSpans.WithLabelValues("kept"))
	sends0 := cap.sends()
	if err := b.ExportTraces(ctx, payload("checkout",
		spanSpec{trace: 941, span: 2, end: 5},                // late: follows the flush's cached keep
		spanSpec{trace: 942, span: 1, end: 5})); err != nil { // new: decided by the latch
		t.Fatal(err)
	}
	if got := cap.sends() - sends0; got != 1 {
		t.Fatalf("sends = %d, want 1 (both classes leave in the push's one payload)", got)
	}
	if got := counter(obs.TailSampleLate.WithLabelValues("kept")) - late0; got != 1 {
		t.Fatalf("late{kept} counted %v, want 1", got)
	}
	if got := counter(obs.TailSampleSpans.WithLabelValues("kept")) - kept0; got != 1 {
		t.Fatalf("spans{kept} counted %v, want 1 (the new trace's span)", got)
	}
	if got := b.Stats(); got != (Stats{}) {
		t.Fatalf("the buffer holds %+v", got)
	}
}
