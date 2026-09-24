package cumagg

// The cardinality cap, seen from the operator's side.
//
// A refusal happens on the caller's per-span / per-edge path, which both
// callers assert allocation-free, so the refusal itself may only bump a
// counter. The LINE is the export loop's — the repo's standard shape for
// anything a hot path notices.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func capturedLog() (*slog.Logger, func() string) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h), buf.String
}

// A full aggregate reports NOTHING about anything new until eviction frees a
// slot, so a service that starts after the burst is simply absent from RED
// metrics or from the graph. The counter cannot say which aggregate, what the
// cap is, or whether eviction is even enabled.
func TestCapPressureIsReportedOncePerWindow(t *testing.T) {
	log, dump := capturedLog()
	h := newHarness(1, 0) // cap 1, eviction disabled

	h.observe("a")
	for range 4 {
		if h.observe("b") {
			t.Fatal("the cap admitted a second series")
		}
	}
	h.store.reportCapPressure(log)
	h.store.reportCapPressure(log) // throttled: the cap is still full

	out := dump()
	if n := strings.Count(out, "cardinality cap is refusing"); n != 1 {
		t.Errorf("want one throttled line, got %d:\n%s", n, out)
	}
	for _, want := range []string{
		`aggregate="test metrics"`, // which of the two aggregators
		"dropped=4",                // how many since the last report
		"maxCardinality=1",         // the knob to raise
		"staleAfter=0s",            // eviction disabled is why the cap latched
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

// Nothing refused, nothing said: this runs once per export interval forever.
func TestCapPressureIsSilentWhenNothingIsRefused(t *testing.T) {
	log, dump := capturedLog()
	h := newHarness(10, 0)
	h.observe("a")
	h.store.reportCapPressure(log)
	if out := dump(); out != "" {
		t.Errorf("a healthy aggregate logged:\n%s", out)
	}
}

// The tally is per REPORT, not cumulative: the counter carries the running
// total, and a line saying "dropped=4" must mean four since the last line.
func TestCapPressureTallyResetsPerReport(t *testing.T) {
	log, _ := capturedLog()
	h := newHarness(1, 0)
	h.observe("a")
	h.observe("b")
	h.store.reportCapPressure(log)
	if n := h.store.capRefused.Load(); n != 0 {
		t.Errorf("the pending tally was not drained: %d", n)
	}
}

// The number on the line has to describe the window the line NAMES. The export
// interval is the report CADENCE (Run calls this once per tick) and
// capWarnEvery is the window, so most calls are suppressed — and a suppressed
// call that drained the tally would throw its refusals away, leaving the line
// that does print carrying one interval's worth while claiming five minutes.
//
// Reverse-patch check: restore the `n := st.capRefused.Swap(0)` ahead of the
// throttle in reportCapPressure and this fails with dropped=1 (the last cycle
// only) instead of dropped=5.
func TestSuppressedCyclesAccumulateIntoTheLineTheyAreCountedFor(t *testing.T) {
	log, dump := capturedLog()
	h := newHarness(1, 0) // cap 1, eviction disabled
	h.observe("a")

	// Cycle 1 spends the throttle slot and reports its one refusal.
	h.observe("b")
	h.store.reportCapPressure(log)

	// Cycles 2..6 are inside the same capWarnEvery window and emit nothing.
	// Their refusals must survive to the next line that does.
	for range 5 {
		h.observe("b")
		h.store.reportCapPressure(log)
	}
	if n := h.store.capRefused.Load(); n != 5 {
		t.Fatalf("suppressed cycles dropped their tally: pending=%d, want 5", n)
	}

	// The window elapses (a fresh throttle is how a test spells that without
	// waiting capWarnEvery); the next cycle reports every refusal since the
	// last line, not just its own.
	h.store.capWarn = logdedupe.Throttle{}
	h.store.reportCapPressure(log)

	out := dump()
	if n := strings.Count(out, "cardinality cap is refusing"); n != 2 {
		t.Fatalf("want two lines (one per window), got %d:\n%s", n, out)
	}
	if !strings.Contains(out, "dropped=5") {
		t.Errorf("the second line understates its window; want dropped=5 in:\n%s", out)
	}
}

// A quiet cycle must not SPEND the throttle slot: reportCapPressure runs once
// per export interval forever, so asking Allow when there is nothing to say
// would silence the first cycle that really binds for a whole window.
//
// Reverse-patch check: drop the `capRefused.Load() == 0` guard and this fails —
// the quiet cycle takes the slot and the refusal that follows is suppressed.
func TestAQuietCycleDoesNotSpendTheThrottleSlot(t *testing.T) {
	log, dump := capturedLog()
	h := newHarness(1, 0)
	h.observe("a")

	h.store.reportCapPressure(log) // nothing refused yet

	h.observe("b") // refused
	h.store.reportCapPressure(log)

	out := dump()
	if n := strings.Count(out, "cardinality cap is refusing"); n != 1 {
		t.Fatalf("want exactly the refusal line, got %d:\n%s", n, out)
	}
	// dropped=1, not dropped=0: the line the operator gets is the one about the
	// refusal, not a vacuous line the quiet cycle spent the slot on.
	if !strings.Contains(out, "dropped=1") {
		t.Errorf("the quiet cycle logged in place of the refusal:\n%s", out)
	}
}

// A non-positive export interval cannot reach time.NewTicker (it panics), so
// Run substitutes a minute — and the substitution must be said: every startup
// line prints the flag's value, so a silent fallback leaves the process
// describing an interval it does not use.
func TestNonPositiveRunIntervalFallbackIsWarned(t *testing.T) {
	for _, iv := range []time.Duration{0, -time.Second} {
		log, dump := capturedLog()
		h := newHarness(10, 0)
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Run's final export, then return
		h.store.Run(ctx, h, iv, pcommon.NewResource(), log)
		out := dump()
		if !strings.Contains(out, "exporting every minute instead") || !strings.Contains(out, `aggregate="test metrics"`) {
			t.Errorf("interval %v: the fallback was silent:\n%s", iv, out)
		}
	}

	log, dump := capturedLog()
	h := newHarness(10, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h.store.Run(ctx, h, time.Minute, pcommon.NewResource(), log)
	if out := dump(); strings.Contains(out, "exporting every minute instead") {
		t.Errorf("a positive interval warned:\n%s", out)
	}
}

// A constructor's staleAfter fallback is one arm for both aggregators, and its
// line must not contradict its own error: a NEGATIVE value parses fine and is
// refused all the same, so the line says "invalid" — "unparseable" sent the
// reader looking for a typo. A valid value, "0" (disable) included, is silent.
func TestResolveStaleAfterWarnsOnEveryRefusalAndOnlyThen(t *testing.T) {
	for _, bad := range []string{"-15m", "quarter hour"} {
		log, dump := capturedLog()
		if got := ResolveStaleAfter("traceMetrics.staleAfter", bad, 15*time.Minute, log); got != 15*time.Minute {
			t.Errorf("%q resolved to %v, want the default", bad, got)
		}
		out := dump()
		if !strings.Contains(out, "traceMetrics.staleAfter is invalid; using the default eviction age") {
			t.Errorf("%q: the fallback line is missing or names no field:\n%s", bad, out)
		}
		if !strings.Contains(out, "staleAfter=15m0s") {
			t.Errorf("%q: the line does not say what is used instead:\n%s", bad, out)
		}
	}
	for _, good := range []string{"", "0", "5m"} {
		log, dump := capturedLog()
		ResolveStaleAfter("serviceGraph.staleAfter", good, time.Minute, log)
		if out := dump(); out != "" {
			t.Errorf("a valid staleAfter %q logged:\n%s", good, out)
		}
	}
}

// flakyExporter fails its first n exports, then accepts.
type flakyExporter struct {
	n        int32
	attempts atomic.Int32
	accepted atomic.Int32
}

func (f *flakyExporter) ExportMetrics(context.Context, pmetric.Metrics) error {
	if f.attempts.Add(1) <= f.n {
		return errors.New("collector unavailable")
	}
	f.accepted.Add(1)
	return nil
}

// A collector outage is narrated as a RUN: one Warn when it starts, the
// attempts inside the restatement interval at Debug, and one Info when it
// ends carrying what it cost. It used to be a Warn on every tick for the whole
// outage and a recovery line that could not say how long it had lasted.
func TestRunNarratesAnExportOutageAsARun(t *testing.T) {
	log, dump := capturedLog()
	h := newHarness(10, 0)
	h.observe("a")
	exp := &flakyExporter{n: 3}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.store.Run(ctx, exp, time.Millisecond, pcommon.NewResource(), log)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for exp.accepted.Load() == 0 {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("no export was accepted within the deadline (%d attempts)", exp.attempts.Load())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done

	out := dump()
	if n := strings.Count(out, `level=WARN msg="exporting cumulative metrics failed`); n != 1 {
		t.Errorf("an outage of 3 failed exports warned %d times, want 1 (the repeats are Debug):\n%s", n, out)
	}
	if n := strings.Count(out, `level=DEBUG msg="exporting cumulative metrics failed"`); n != 2 {
		t.Errorf("the repeats inside the restatement interval logged %d Debug lines, want 2:\n%s", n, out)
	}
	if n := strings.Count(out, "cumulative-metrics export recovered"); n != 1 || !strings.Contains(out, "failures=3") {
		t.Errorf("want one recovery line carrying failures=3, got %d:\n%s", n, out)
	}
}
