package tailbuffer

// The memory bounds, seen from the operator's side.
//
// Every bound here trades trace COMPLETENESS for a smaller buffer, and an early
// decision is judged on the spans present — so a sustained rate means the
// sampling an operator configured is not the sampling they are getting. The
// decision path is allocation-budgeted (TestDropDecisionAllocationBudget,
// TestKeepDecisionAllocationBudget) and runs under the buffer mutex, so it may
// only bump a counter; the line is the sweep's.

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

func capturedLog() (*slog.Logger, func() string) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h), buf.String
}

func newLoggingBuffer(t *testing.T, cfg Config) (*Buffer, *clock, func() string) {
	t.Helper()
	log, dump := capturedLog()
	b, err := New(cfg, discard{}, log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c := newClock()
	b.now = c.now
	return b, c, dump
}

// maxTraces binding: the oldest trace is decided before its window closes.
func TestEarlyDecisionsAreReportedBySweep(t *testing.T) {
	b, _, dump := newLoggingBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1m", MaxTraces: 1})
	ctx := context.Background()

	for i := 1; i <= 4; i++ {
		if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: uint64(i), span: 1, end: 5})); err != nil {
			t.Fatal(err)
		}
	}
	// Nothing is due yet — the report rides the sweep, so until one runs the
	// operator sees only the counter.
	if out := dump(); strings.Contains(out, "decided before their decisionWait") {
		t.Errorf("the early-decision line was emitted from the receive path:\n%s", out)
	}
	b.Sweep(ctx)

	out := dump()
	if n := strings.Count(out, "decided before their decisionWait"); n != 1 {
		t.Errorf("want one aggregate line, got %d:\n%s", n, out)
	}
	if !strings.Contains(out, "byMaxTraces=3") {
		t.Errorf("the line does not say how many were forced, or by which bound:\n%s", out)
	}
	if !strings.Contains(out, "maxTraces=1") {
		t.Errorf("the line does not name the configured bound:\n%s", out)
	}

	// A second sweep with nothing new to report stays silent.
	b.Sweep(ctx)
	if n := strings.Count(dump(), "decided before their decisionWait"); n != 1 {
		t.Errorf("the line repeated with nothing new to say:\n%s", dump())
	}
}

// A graceful stop decides every buffered trace early BY DESIGN, so Flush must
// not print a scary line in every rolling update.
func TestShutdownFlushDoesNotWarnAboutEarlyDecisions(t *testing.T) {
	b, _, dump := newLoggingBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1m"})
	ctx := context.Background()
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 1, span: 1, end: 5})); err != nil {
		t.Fatal(err)
	}
	b.Flush(ctx)
	if out := dump(); strings.Contains(out, "decided before their decisionWait") {
		t.Errorf("a graceful flush warned about its own early decisions:\n%s", out)
	}
}

// Steady state: traces decided because their window closed say nothing.
func TestOrdinaryDecisionsAreSilent(t *testing.T) {
	b, clk, dump := newLoggingBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "10ms"})
	ctx := context.Background()
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 1, span: 1, end: 5})); err != nil {
		t.Fatal(err)
	}
	base := len(dump()) // New itself reports the buffer sizing; ignore that
	clk.advance(time.Second)
	b.Sweep(ctx)
	if out := dump()[base:]; out != "" {
		t.Errorf("a healthy sweep logged:\n%s", out)
	}
}

// The counts on the aggregate line must describe the window the line NAMES.
//
// The throttle is a minute; the drain that reports runs on the sweep ticker
// (100ms-1s). Draining the tallies on every sweep and throttling the line
// afterwards therefore throws away every suppressed sweep's counts, so the line
// that does escape claims a minute and carries one tick — understating by the
// tick ratio, which is exactly the direction that makes an operator size a
// bound too generously. Suppressed drains must leave the tallies standing.
func TestSuppressedSweepsStillCountTowardsTheReportedWindow(t *testing.T) {
	b, _, dump := newLoggingBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1m", MaxTraces: 1})
	ctx := context.Background()

	push := func(ids ...uint64) {
		t.Helper()
		for _, id := range ids {
			if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: id, span: 1, end: 5})); err != nil {
				t.Fatal(err)
			}
		}
	}

	// First window: 3 forced out by maxTraces (the 4th stays buffered). The
	// throttle's zero value fires immediately, so this one is reported.
	push(1, 2, 3, 4)
	b.Sweep(ctx)
	if n := strings.Count(dump(), "byMaxTraces=3"); n != 1 {
		t.Fatalf("the first window was not reported as 3:\n%s", dump())
	}

	// Inside the throttle window: these sweeps must stay silent AND must not
	// spend the counts they saw.
	b.earlyEvery = time.Hour
	push(5, 6, 7)
	b.Sweep(ctx)
	push(8, 9)
	b.Sweep(ctx)
	if n := strings.Count(dump(), "decided before their decisionWait"); n != 1 {
		t.Fatalf("a throttled drain emitted a line:\n%s", dump())
	}

	// The next line the throttle admits must carry all five, not the two the
	// final sweep happened to see.
	b.earlyEvery = 0
	b.Sweep(ctx)
	out := dump()
	if n := strings.Count(out, "decided before their decisionWait"); n != 2 {
		t.Fatalf("want a second aggregate line, got %d:\n%s", n, out)
	}
	if !strings.Contains(out, "byMaxTraces=5") {
		t.Errorf("the line describes one sweep, not the window it names (want byMaxTraces=5):\n%s", out)
	}
}

// The counterpart: a quiet window must not spend the throttle slot, or the
// first drain that actually binds is suppressed for a minute.
func TestQuietSweepsDoNotSpendTheThrottleSlot(t *testing.T) {
	b, _, dump := newLoggingBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1m", MaxTraces: 1})
	ctx := context.Background()

	for range 5 {
		b.Sweep(ctx) // nothing buffered, nothing forced
	}
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 1, span: 1, end: 5})); err != nil {
		t.Fatal(err)
	}
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 2, span: 1, end: 5})); err != nil {
		t.Fatal(err)
	}
	b.Sweep(ctx)
	if !strings.Contains(dump(), "byMaxTraces=1") {
		t.Errorf("the first binding drain was suppressed by an earlier quiet one:\n%s", dump())
	}
}

// A graceful stop's early decisions RIDE the line once a real bound has forced
// one out.
//
// any() deliberately leaves reasonShutdown out of the decision to WRITE the
// line (every rolling update decides its whole buffer early, by design, and a
// scary line about it would be noise). It says, and the operator needs, that
// the count still appears when a real bound also bound — which is exactly the
// window in which "how much of this burst was the stop?" is the question being
// asked. The field was populated and zeroed and never rendered.
func TestEarlyReportNamesShutdownDecisions(t *testing.T) {
	b, _, dump := newLoggingBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1m", MaxTraces: 1})
	ctx := context.Background()

	// maxTraces forces trace 1 out early; trace 2 stays buffered.
	for i := uint64(1); i <= 2; i++ {
		if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: i, span: 1, end: 5})); err != nil {
			t.Fatal(err)
		}
	}
	// The graceful stop then decides trace 2 early too, in the same window.
	b.Flush(ctx)

	out := dump()
	if n := strings.Count(out, "decided before their decisionWait"); n != 1 {
		t.Fatalf("want one aggregate line, got %d:\n%s", n, out)
	}
	if !strings.Contains(out, "byMaxTraces=1") {
		t.Errorf("the bound that forced the line is not on it:\n%s", out)
	}
	if !strings.Contains(out, "byShutdown=1") {
		t.Errorf("the shutdown tally is collected and then never rendered; the line cannot separate the stop from the bound:\n%s", out)
	}
}

// Every early-decision reason's tally must reach the line.
//
// The per-reason tables are arrays, but reportEarly names its reasons one by
// one, so a reason added to the const block (and given a label and a counter)
// but left off the line would be counted, would make any() write the line, and
// would then be missing from it: the operator reading the line for "which
// bound?" would find every by* key it does carry at zero. Distinct values per
// reason, so no reason's tally can stand in for another's.
func TestEveryEarlyReasonIsReportedOnTheLine(t *testing.T) {
	b, _, dump := newLoggingBuffer(t, Config{Config: alwaysCfg()})
	tally := func(r earlyReason) int { return 1000 + int(r) }

	b.mu.Lock()
	for r := reasonNone + 1; r < numEarlyReasons; r++ {
		b.earlyPending[r] = tally(r)
	}
	early, ok := b.takeEarlyLocked()
	b.mu.Unlock()
	if !ok {
		t.Fatal("no line to write with every early reason pending")
	}
	b.reportEarly(early)

	out := dump()
	for r := reasonNone + 1; r < numEarlyReasons; r++ {
		if !regexp.MustCompile(fmt.Sprintf(`\bby[A-Za-z]+=%d\b`, tally(r))).MatchString(out) {
			t.Errorf("early reason %q is counted but its tally (%d) is not on the line:\n%s", r, tally(r), out)
		}
	}
}

// The two failed-export lines hold SEPARATE throttles.
//
// They describe one downstream condition and therefore co-occur, but only one
// of them reports LOSS: ExportTraces' line says a push was NACKed, which costs
// nothing (the sender still holds every span and retransmits), while the
// drain's says spans whose senders were acked at buffering time have been
// destroyed. The NACK line is emitted from every concurrent receive goroutine
// and is thus far the more frequent, so with one shared gate it claimed the
// slot and suppressed the only line that names spans actually lost — leaving a
// log that reads as if the senders had it covered. Same rule as the tailer's
// unresolved-file / metadata-budget pair.
func TestLossReportIsNotStarvedByTheNackReport(t *testing.T) {
	cap := &capture{}
	log, dump := capturedLog()
	b, err := New(Config{Config: alwaysCfg(), DecisionWait: "1s"}, cap, log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	clk := newClock()
	b.now = clk.now
	ctx := context.Background()

	// Decide trace 1 while the collector is healthy, so its keep is cached and
	// later spans for it are LATE spans: those ride out on the receive path.
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 1, span: 1, end: 5})); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Second)
	b.Sweep(ctx)

	// A permanent rejection: sendRetry does not sleep on one, so the drain
	// below fails on its first attempt.
	cap.fail(&otlpexport.HTTPStatusError{Code: 400, Body: "bad batch"})

	// Three NACKed late-span pushes. Nothing is lost — the senders keep them —
	// and the line is throttled to one, which is the whole point: it holds the
	// window.
	for i := uint64(2); i <= 4; i++ {
		if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 1, span: i, end: 5})); err == nil {
			t.Fatal("a failing collector must fail the push")
		}
	}
	if n := strings.Count(dump(), "the push is NACKed"); n != 1 {
		t.Fatalf("the NACK line is not throttled (%d lines):\n%s", n, dump())
	}

	// Now spans that really are lost: buffered (their senders acked), decided
	// by the sweep, and refused by the collector on the final attempt.
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 2, span: 1, end: 5})); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Second)
	b.Sweep(ctx)

	if !strings.Contains(dump(), "their senders were acked at buffering time") {
		t.Errorf("the data-loss line was starved by the harmless NACK line — an operator reads a log saying the senders have it covered while buffered spans are destroyed:\n%s", dump())
	}
}

// The receive path has BOTH events too, and they must not share a gate.
//
// A push that carries only the sender's own spans (late spans) and fails is a
// NACK — nothing lost. A push that also carries keeps a BOUND forced out of the
// buffer early and fails destroys those keeps: their senders were acked at
// buffering time and the retransmission re-presents only the pusher's own
// spans. The second used to be reported through the NACK line's gate, so the
// frequent harmless line suppressed the only report of the loss — the exact
// starvation the gate split exists to prevent — and the harmless line in turn
// claimed "counted lost" for a push that lost nothing.
func TestReceivePathLossIsNotStarvedByTheNackReport(t *testing.T) {
	cap := &capture{}
	log, dump := capturedLog()
	b, err := New(Config{Config: alwaysCfg(), DecisionWait: "1s", MaxTraces: 1}, cap, log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	clk := newClock()
	b.now = clk.now
	ctx := context.Background()

	// Trace 1's keep is cached while the collector is healthy.
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 1, span: 1, end: 5})); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Second)
	b.Sweep(ctx)

	// A permanent rejection: no retry sleeps, every send fails on attempt one.
	cap.fail(&otlpexport.HTTPStatusError{Code: 400, Body: "bad batch"})

	// The harmless one first: a late span of trace 1, NACKed back to its
	// sender. It takes its gate's slot, and it must not claim a loss.
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 1, span: 2, end: 5})); err == nil {
		t.Fatal("a failing collector must fail the push")
	}
	if out := dump(); strings.Count(out, "the push is NACKed") != 1 {
		t.Fatalf("want exactly one NACK line:\n%s", out)
	} else if strings.Contains(out, "counted lost") {
		t.Errorf("a push that carried only its sender's spans claimed a loss:\n%s", out)
	}

	// Now a real loss on the receive path: trace 2 is buffered (acked), and
	// trace 3's push makes maxTraces bind, forcing trace 2 out inside it.
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 2, span: 1, end: 5})); err != nil {
		t.Fatal(err)
	}
	lost0 := counter(obs.TailSampleSpans.WithLabelValues("lost"))
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 3, span: 1, end: 5})); err == nil {
		t.Fatal("the failed forward of an early-decided keep must still NACK the push")
	}
	if lost := counter(obs.TailSampleSpans.WithLabelValues("lost")) - lost0; lost != 1 {
		t.Fatalf("lost spans counted %v, want 1", lost)
	}
	out := dump()
	if !strings.Contains(out, "their senders were acked at buffering time") || !strings.Contains(out, "buffered=1") {
		t.Errorf("the receive-path loss was not reported — the NACK line starved it:\n%s", out)
	}
}

// slowExport is a downstream whose every export takes d on the buffer's clock.
type slowExport struct {
	capture
	clk *clock
	d   time.Duration
}

func (s *slowExport) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	s.clk.advance(s.d)
	return s.capture.ExportTraces(ctx, td)
}

// The decision loop decides nothing while its own send is in flight, so a slow
// collector fills the buffer and makes maxSpans (or maxTraces) bind — early
// decisions caused by EXPORT LATENCY, which a line naming only the bounds reads
// as a bound sized below the shard's span rate. The line therefore carries the
// slowest decision-loop export of its window.
func TestEarlyReportNamesTheSlowestDecisionExport(t *testing.T) {
	log, dump := capturedLog()
	next := &slowExport{d: 3 * time.Second}
	b, err := New(Config{Config: alwaysCfg(), DecisionWait: "1s", MaxTraces: 1}, next, log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	clk := newClock()
	b.now = clk.now
	next.clk = clk
	ctx := context.Background()

	// A sweep whose export takes 3s — three windows during which nothing is
	// decided on time.
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 1, span: 1, end: 5})); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Second)
	b.Sweep(ctx)

	// A bound binds; the next sweep reports it.
	for i := uint64(2); i <= 3; i++ {
		if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: i, span: 1, end: 5})); err != nil {
			t.Fatal(err)
		}
	}
	b.Sweep(ctx)

	out := dump()
	if !strings.Contains(out, "byMaxTraces=1") {
		t.Fatalf("the bound did not bind as the fixture intends:\n%s", out)
	}
	if !strings.Contains(out, "slowestDecisionExport=3s") {
		t.Errorf("the early-decision line does not name the slow export that stalled the decision loop:\n%s", out)
	}
}

// ...and ONLY a slow export of the window it reports. A drain that emits
// nothing must not leave the slowest export standing: kept until the next line,
// a stall that ended long before an unrelated bind — hours of healthy sweeps
// earlier — rides that line and points the operator at export latency for a
// bound that is simply sized below the shard's span rate.
func TestEarlyReportForgetsASlowExportFromBeforeTheBind(t *testing.T) {
	log, dump := capturedLog()
	next := &slowExport{d: 30 * time.Second}
	b, err := New(Config{Config: alwaysCfg(), DecisionWait: "1s", MaxTraces: 1}, next, log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	clk := newClock()
	b.now = clk.now
	next.clk = clk
	ctx := context.Background()

	push := func(id uint64) {
		t.Helper()
		if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: id, span: 1, end: 5})); err != nil {
			t.Fatal(err)
		}
	}
	decideOnTime := func(id uint64) {
		t.Helper()
		push(id)
		clk.advance(2 * time.Second)
		b.Sweep(ctx)
	}

	// One stall that no bound bound during...
	decideOnTime(1)
	// ...then healthy sweeps whose exports are fast.
	next.d = 10 * time.Millisecond
	for id := uint64(2); id <= 4; id++ {
		decideOnTime(id)
	}

	// Now a bound binds, for reasons of its own, and the next sweep reports it.
	push(5)
	push(6)
	b.Sweep(ctx)

	out := dump()
	if !strings.Contains(out, "byMaxTraces=1") {
		t.Fatalf("the bound did not bind as the fixture intends:\n%s", out)
	}
	if strings.Contains(out, "slowestDecisionExport=30s") {
		t.Errorf("the line blames a stall that ended three healthy sweeps before the bind:\n%s", out)
	}
	if !strings.Contains(out, "slowestDecisionExport=10ms") {
		t.Errorf("the line does not carry the slowest export of its own window (10ms):\n%s", out)
	}
}
