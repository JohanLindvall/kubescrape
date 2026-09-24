package cumagg

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type counter struct{ n int }

func (c *counter) Inc() { c.n++ }

// series is a minimal aggregate: the shared bookkeeping plus one value.
type series struct {
	Meta
	calls uint64
	ex    []Exemplar
}

func (s *series) reset() { ClearExemplars(s.ex) }

// harness is a whole aggregator in twenty lines: it renders one counter per
// series so the store's steps can be driven and observed on their own.
type harness struct {
	store    *Store[*series]
	now      time.Time
	dropped  counter
	evicted  counter
	exported []pmetric.Metrics
	failWith error
}

func newHarness(maxCard int, stale time.Duration) *harness {
	h := &harness{now: time.Unix(1_700_000_000, 0)}
	h.store = NewStore(Options[*series]{
		Scope:          "test",
		Name:           "test metrics",
		MaxCardinality: maxCard,
		StaleAfter:     stale,
		Dropped:        &h.dropped,
		Evicted:        &h.evicted,
		Now:            func() time.Time { return h.now },
		NewSeries:      func() *series { return &series{} },
		Render:         h.render,
		ResetExemplars: (*series).reset,
	})
	return h
}

func (h *harness) observe(key string) bool {
	h.store.Lock()
	defer h.store.Unlock()
	s, _, ok := h.store.AdmitLocked([]byte(key), h.now)
	if !ok {
		return false
	}
	s.calls++
	s.ex = RecordExemplar(s.ex, 2, 0, 1, 0, pcommon.TraceID([16]byte{1}), pcommon.SpanID([8]byte{1}))
	h.store.ObservedLocked(s, h.now)
	return true
}

func (h *harness) render(sm pmetric.ScopeMetrics, now time.Time) {
	h.store.Lock()
	defer h.store.Unlock()
	live := h.store.LivePointersLocked(nil, now)
	if len(live) == 0 {
		return
	}
	dps := SumMetric(sm, "calls", "", "")
	for _, s := range live {
		h.store.MarkRenderedLocked(s)
		p := dps.AppendEmpty()
		p.SetStartTimestamp(pcommon.NewTimestampFromTime(s.Start))
		p.SetIntValue(int64(s.calls))
		for i := range s.ex {
			if s.ex[i].Set {
				PutExemplar(p.Exemplars(), s.ex[i])
			}
		}
	}
}

func (h *harness) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	if h.failWith != nil {
		return h.failWith
	}
	cp := pmetric.NewMetrics()
	md.CopyTo(cp)
	h.exported = append(h.exported, cp)
	return nil
}

func (h *harness) export(t *testing.T) {
	t.Helper()
	err := h.store.Export(context.Background(), h, pcommon.NewResource())
	if (err != nil) != (h.failWith != nil) {
		t.Fatalf("Export error = %v, want failure=%v", err, h.failWith != nil)
	}
}

// The gate the whole package exists for: a series may be evicted only once a
// DELIVERED export carried its current values. An export interval longer than
// staleAfter, or an export that failed, must not destroy observations unseen.
func TestEvictionWaitsForDelivery(t *testing.T) {
	h := newHarness(10, time.Minute)
	h.observe("a")

	// Rendered but NOT delivered: still stale, still exported.
	h.failWith = errFail{}
	h.export(t)
	h.failWith = nil
	h.now = h.now.Add(10 * time.Minute)
	h.export(t)
	if h.evicted.n != 0 {
		t.Fatalf("evicted = %d, want 0: nothing had been delivered", h.evicted.n)
	}
	if n := h.store.Len(); n != 1 {
		t.Fatalf("series = %d, want 1", n)
	}

	// That export WAS delivered, so the same series may now age out — and an
	// all-evicted cycle sends nothing at all.
	before := len(h.exported)
	h.now = h.now.Add(10 * time.Minute)
	h.export(t)
	if h.evicted.n != 1 || h.store.Len() != 0 {
		t.Fatalf("evicted = %d, series = %d; want 1 and 0", h.evicted.n, h.store.Len())
	}
	if len(h.exported) != before {
		t.Errorf("an empty cycle sent %d payloads, want none", len(h.exported)-before)
	}
}

type errFail struct{}

func (errFail) Error() string { return "collector down" }

// observeDuringSend is the harness' exporter with one observation landing
// DURING the send: after Render has marked the series Rendered and before the
// delivery mark runs — the window an ingest goroutine hits on every export.
type observeDuringSend struct {
	h   *harness
	key string
}

func (o *observeDuringSend) ExportMetrics(ctx context.Context, md pmetric.Metrics) error {
	o.h.observe(o.key)
	return o.h.ExportMetrics(ctx, md)
}

// The delivery mark promotes only what the delivered payload CARRIED. A series
// observed while that payload was in flight is back in Observed, holds a value
// no collector has seen, and must stay ineligible for eviction until a later
// export delivers it — or an export interval longer than staleAfter, which is
// legal, evicts the one increment nobody exported. Nothing else pins it: the
// overlapping-exports test ends Rendered either way, and the eviction-gate test
// covers only a FAILED send.
func TestDeliveryMarkLeavesSeriesObservedDuringTheSendUndelivered(t *testing.T) {
	h := newHarness(10, time.Minute)
	h.observe("a")
	if err := h.store.Export(context.Background(), &observeDuringSend{h: h, key: "a"}, pcommon.NewResource()); err != nil {
		t.Fatalf("Export: %v", err)
	}

	h.now = h.now.Add(2 * time.Minute) // past staleAfter, before any export carried the second call
	h.export(t)
	if h.evicted.n != 0 {
		t.Fatalf("evicted = %d, want 0: the series held an observation no delivered export carried", h.evicted.n)
	}
	if len(h.exported) != 2 {
		t.Fatalf("payloads = %d, want 2", len(h.exported))
	}
	got := h.exported[1].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0).IntValue()
	if got != 2 {
		t.Errorf("second payload calls = %d, want 2 (the call observed during the first send)", got)
	}
}

// The delivery mark acts on what the delivered payload CARRIED and on nothing
// else. A series ADMITTED while the payload was in flight was in no payload at
// all, so its exemplar is evidence nobody has seen: clearing it with the rest
// dropped it unseen. The mark used to take every live series' pointer in a
// whole-map pass of its own, which is what reached it; it walks the render's own
// list now.
func TestDeliveryMarkLeavesSeriesAdmittedDuringTheSendAlone(t *testing.T) {
	h := newHarness(10, time.Minute)
	h.observe("a")
	if err := h.store.Export(context.Background(), &observeDuringSend{h: h, key: "b"}, pcommon.NewResource()); err != nil {
		t.Fatalf("Export: %v", err)
	}
	h.export(t)
	if len(h.exported) != 2 {
		t.Fatalf("payloads = %d, want 2", len(h.exported))
	}
	dps := h.exported[1].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints()
	if dps.Len() != 2 {
		t.Fatalf("second payload points = %d, want 2", dps.Len())
	}
	var exemplars []int
	for i := 0; i < dps.Len(); i++ {
		exemplars = append(exemplars, dps.At(i).Exemplars().Len())
	}
	// "a"'s exemplar was delivered by the first payload and cleared; "b"'s was
	// recorded during that send and delivered by none, so it must still be here.
	if exemplars[0]+exemplars[1] != 1 {
		t.Fatalf("exemplars per point = %v, want exactly one (the series admitted during the first send kept its evidence)", exemplars)
	}
}

// The list the delivery mark walks belongs to ONE export. Kept past it, it would
// grow by a whole render per interval and pin series eviction had already
// dropped — after a FAILED send too, which never reaches the mark at all.
func TestRenderedListDoesNotOutliveItsExport(t *testing.T) {
	h := newHarness(10, time.Minute)
	listed := func() int {
		h.store.Lock()
		defer h.store.Unlock()
		return len(h.store.rendered)
	}
	h.observe("a")
	h.observe("b")
	h.export(t)
	if n := listed(); n != 0 {
		t.Fatalf("after a delivered export the mark's list holds %d series, want 0", n)
	}
	h.failWith = errFail{}
	h.export(t)
	h.failWith = nil
	if n := listed(); n != 0 {
		t.Fatalf("after a failed export the mark's list holds %d series, want 0", n)
	}
	// A render outside Export (tests do it) must not leak into the next
	// export's mark either.
	h.store.Render(pcommon.NewResource(), h.now)
	h.export(t)
	if n := listed(); n != 0 {
		t.Fatalf("after an out-of-band render and an export the list holds %d series, want 0", n)
	}
}

// Zero staleAfter disables eviction outright: the branch a negative value used
// to reach by being clamped (see ParseStaleAfter).
func TestZeroStaleAfterDisablesEviction(t *testing.T) {
	h := newHarness(10, 0)
	h.observe("a")
	h.export(t)
	h.now = h.now.Add(24 * time.Hour)
	h.export(t)
	if h.evicted.n != 0 || h.store.Len() != 1 {
		t.Fatalf("evicted = %d, series = %d; want 0 and 1", h.evicted.n, h.store.Len())
	}
}

// The cap refuses NEW series, counts every refusal, and never touches the ones
// already admitted; a freed slot is reusable.
func TestCardinalityCapAndReuse(t *testing.T) {
	h := newHarness(2, time.Minute)
	for _, k := range []string{"a", "b"} {
		if !h.observe(k) {
			t.Fatalf("%q refused under the cap", k)
		}
	}
	if h.observe("c") || h.dropped.n != 1 {
		t.Fatalf("third series admitted, or dropped = %d, want 1", h.dropped.n)
	}
	if !h.observe("a") { // an admitted series keeps reporting
		t.Fatal("an existing series was refused by the cap")
	}

	h.export(t)
	h.now = h.now.Add(2 * time.Minute)
	h.export(t) // evicts both
	if !h.observe("c") {
		t.Fatalf("the freed slots did not admit a new series (dropped = %d)", h.dropped.n)
	}
}

// A re-created series restarts its counters, so it must carry a FRESH start
// timestamp: an unchanged one reads downstream as a counter jumping backwards.
func TestReCreatedSeriesGetsFreshStart(t *testing.T) {
	h := newHarness(10, time.Minute)
	h.observe("a")
	h.export(t)
	first := h.exported[0].ResourceMetrics().At(0).ScopeMetrics().At(0).
		Metrics().At(0).Sum().DataPoints().At(0).StartTimestamp()

	h.now = h.now.Add(2 * time.Minute)
	h.export(t) // evicts
	h.now = h.now.Add(time.Minute)
	h.observe("a")
	h.export(t)
	last := h.exported[len(h.exported)-1].ResourceMetrics().At(0).ScopeMetrics().At(0).
		Metrics().At(0).Sum().DataPoints()
	if v := last.At(0).IntValue(); v != 1 {
		t.Errorf("re-created series calls = %d, want 1 (cumulative restarted)", v)
	}
	if got := last.At(0).StartTimestamp(); got <= first {
		t.Errorf("start timestamp %v not advanced past %v after re-creation", got, first)
	}
}

// Exemplars are cleared on DELIVERY, not on rendering: a failed send keeps its
// evidence for the retry.
func TestExemplarsClearedOnlyAfterDelivery(t *testing.T) {
	h := newHarness(10, 0)
	h.observe("a")
	h.failWith = errFail{}
	h.export(t)
	h.failWith = nil
	h.export(t)
	if n := exemplarCount(h.exported[len(h.exported)-1]); n != 1 {
		t.Fatalf("exemplars after a failed-then-successful export = %d, want 1", n)
	}
	h.export(t)
	if n := exemplarCount(h.exported[len(h.exported)-1]); n != 0 {
		t.Fatalf("exemplars after delivery = %d, want 0", n)
	}
}

func exemplarCount(md pmetric.Metrics) int {
	return md.ResourceMetrics().At(0).ScopeMetrics().At(0).
		Metrics().At(0).Sum().DataPoints().At(0).Exemplars().Len()
}

// Run makes a final export after its context is cancelled, on a detached
// context: the last window's observations are as real as any other.
func TestRunFinalExportIsDetached(t *testing.T) {
	h := newHarness(10, 0)
	h.observe("a")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.store.Run(ctx, h, time.Hour, pcommon.NewResource(), nil) // the ticker never fires
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	if len(h.exported) != 1 {
		t.Fatalf("payloads = %d, want 1 from the final export", len(h.exported))
	}
}

// ClampStart bounds the STAMP, never Meta.Start: a series admitted between an
// export's clock read and the render's lock hold renders start == ts on that
// first export (the OTLP "cumulative began now" spelling) and its true start
// from the next export on, whose ts lies past it.
func TestClampStart(t *testing.T) {
	ts := pcommon.Timestamp(1_000)
	for _, tc := range []struct{ start, want pcommon.Timestamp }{
		{500, 500},     // the ordinary series: the true start renders
		{1_000, 1_000}, // exactly the export instant: a legal interval, kept
		{1_500, 1_000}, // admitted after the export's clock read: clamped to ts
	} {
		if got := ClampStart(tc.start, ts); got != tc.want {
			t.Errorf("ClampStart(%v, %v) = %v, want %v", tc.start, ts, got, tc.want)
		}
	}
}

func TestParseStaleAfter(t *testing.T) {
	const def = 15 * time.Minute
	for _, tc := range []struct {
		value   string
		want    time.Duration
		wantErr []string // substrings the error must name
	}{
		{value: "", want: def},
		{value: "5m", want: 5 * time.Minute},
		{value: "0", want: 0},  // eviction disabled, explicitly
		{value: "0s", want: 0}, // the same thing, spelled the other way
		{value: "quarter hour", want: def, wantErr: []string{"traceMetrics.staleAfter", "quarter hour"}},
		// The drift: this used to be clamped to 0 in one of the two callers,
		// which is the DISABLE branch — the cardinality cap back as a one-way
		// latch, reached through the field that exists to prevent it.
		{value: "-15m", want: def, wantErr: []string{"traceMetrics.staleAfter", "-15m", "negative", `"0"`}},
	} {
		t.Run(tc.value, func(t *testing.T) {
			got, err := ParseStaleAfter("traceMetrics.staleAfter", tc.value, def)
			if len(tc.wantErr) == 0 {
				if err != nil {
					t.Fatalf("ParseStaleAfter(%q) = %v", tc.value, err)
				}
			} else if err == nil {
				t.Fatalf("ParseStaleAfter(%q) accepted it", tc.value)
			} else {
				for _, w := range tc.wantErr {
					if !strings.Contains(err.Error(), w) {
						t.Errorf("error %q does not name %q", err, w)
					}
				}
			}
			// Even on an error the DEFAULT comes back, never 0: a constructor
			// falls back to it and keeps aggregating, and falling back to
			// "eviction disabled" is exactly the bug.
			if got != tc.want {
				t.Errorf("ParseStaleAfter(%q) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}

func TestBuiltinsConfigure(t *testing.T) {
	b := NewBuiltins("service.name", "span.name")
	got := b.Configure("x", []string{"http.route", "span.name", "", "http.route", "db.system"}, nil)
	want := []string{"http.route", "db.system"}
	if len(got) != len(want) {
		t.Fatalf("Configure = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Configure = %v, want %v (order is the label order)", got, want)
		}
	}
	if !b.Has("span.name") || b.Has("http.route") {
		t.Error("Has does not report the built-in set")
	}
	if b.Configure("x", nil, nil) != nil {
		t.Error("Configure(nil) should stay nil")
	}
	// A nil set applies the empty and repeat rules alone (servicegraph's
	// configured names, which are prefixed before they can meet a built-in).
	var none Builtins
	if got := none.Configure("x", []string{"a", "", "a", "b"}, nil); strings.Join(got, ",") != "a,b" {
		t.Errorf("nil Builtins Configure = %q, want [a b]", got)
	}
}

// A constructor's trace of what it dropped is exactly DimensionWarnings' list,
// at Debug and never at Warn (configWarnings owns the Warn, or a start prints
// each line twice) — and nothing at all when the logger is not at Debug.
func TestConfigureTracesExactlyTheDimensionWarningsAtDebug(t *testing.T) {
	b := NewBuiltins("span.name")
	names := []string{"http.route", "", "span.name", "http.route"}
	rec := &recordingHandler{level: slog.LevelDebug}
	b.Configure("traceMetrics.dimensions", names, slog.New(rec))
	want := b.DimensionWarnings("traceMetrics.dimensions", names)
	if len(want) != 3 {
		t.Fatalf("setup: DimensionWarnings = %q, want three drops", want)
	}
	if len(rec.msgs) != len(want) {
		t.Fatalf("Configure traced %d lines, want one per drop (%d): %+v", len(rec.msgs), len(want), rec.msgs)
	}
	for i, r := range rec.msgs {
		if r.level != slog.LevelDebug {
			t.Errorf("line %d logged at %v; the Warn is configWarnings', a start would print it twice", i, r.level)
		}
		if r.msg != want[i] {
			t.Errorf("line %d = %q, want DimensionWarnings' %q", i, r.msg, want[i])
		}
	}
	quiet := &recordingHandler{level: slog.LevelInfo}
	b.Configure("traceMetrics.dimensions", names, slog.New(quiet))
	if len(quiet.msgs) != 0 {
		t.Errorf("an Info-level logger received the Debug trace: %+v", quiet.msgs)
	}
}

// recordingHandler keeps each record's level and message verbatim.
type recordingHandler struct {
	level slog.Level
	msgs  []struct {
		level slog.Level
		msg   string
	}
}

func (h *recordingHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.level }
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.msgs = append(h.msgs, struct {
		level slog.Level
		msg   string
	}{r.Level, r.Message})
	return nil
}
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

// An empty configured name is a stray `- ` in a YAML list, and kept it renders
// an attribute with an EMPTY KEY on every data point. servicegraph refused it
// in a loop of its own while the shared rule kept it — the drift this package
// exists to end — so the rule and its report are one walk, and they must agree.
func TestDimensionWarningsReportExactlyWhatConfigureDrops(t *testing.T) {
	b := NewBuiltins("service.name", "span.name")
	names := []string{"http.route", "", "span.name", "http.route", "db.system", ""}
	kept := b.Configure("traceMetrics.dimensions", names, nil)
	warns := b.DimensionWarnings("traceMetrics.dimensions", names)
	if len(kept)+len(warns) != len(names) {
		t.Fatalf("kept %q and warned %q: every name is either kept or reported, exactly once", kept, warns)
	}
	for _, want := range []string{
		`traceMetrics.dimensions[1] "" is ignored: it is empty`,
		`traceMetrics.dimensions[2] "span.name" is ignored: it collides with a label every data point already carries (service.name,span.name)`,
		`traceMetrics.dimensions[3] "http.route" is ignored: it repeats an earlier entry`,
		`traceMetrics.dimensions[5] "" is ignored: it is empty`,
	} {
		if !containsPrefix(warns, want) {
			t.Errorf("missing %q in %q", want, warns)
		}
	}
	if w := b.DimensionWarnings("x", []string{"http.route", "db.system"}); w != nil {
		t.Errorf("a clean list warned: %q", w)
	}
}

func containsPrefix(ss []string, prefix string) bool {
	for _, s := range ss {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}
	return false
}
