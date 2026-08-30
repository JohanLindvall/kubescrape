package tailbuffer

// The memory bounds, seen from the operator's side.
//
// Every bound here trades trace COMPLETENESS for a smaller buffer, and an early
// decision is judged on the spans present — so a sustained rate means the
// sampling an operator configured is not the sampling they are getting. The
// decision path is allocation-budgeted and runs under the buffer mutex, so it
// may only bump a counter; the line is the sweep's.

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
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
