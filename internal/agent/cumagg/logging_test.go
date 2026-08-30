package cumagg

// The cardinality cap, seen from the operator's side.
//
// A refusal happens on the caller's per-span / per-edge path, which both
// callers assert allocation-free, so the refusal itself may only bump a
// counter. The LINE is the export loop's — the repo's standard shape for
// anything a hot path notices.

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
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
	for i := 0; i < 4; i++ {
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
	for i := 0; i < 5; i++ {
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
