package cumagg

import (
	"context"
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
	h.store.EvictLocked(now)
	if h.store.CountLocked() == 0 {
		return
	}
	dps := SumMetric(sm, "calls", "", "")
	h.store.EachLocked(func(s *series) {
		h.store.MarkRenderedLocked(s)
		p := dps.AppendEmpty()
		p.SetStartTimestamp(pcommon.NewTimestampFromTime(s.Start))
		p.SetIntValue(int64(s.calls))
		for i := range s.ex {
			if s.ex[i].Set {
				PutExemplar(p.Exemplars(), s.ex[i])
			}
		}
	})
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

func TestBuiltinsFilter(t *testing.T) {
	b := NewBuiltins("service.name", "span.name")
	got := b.Filter([]string{"http.route", "span.name", "http.route", "db.system"})
	want := []string{"http.route", "db.system"}
	if len(got) != len(want) {
		t.Fatalf("Filter = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Filter = %v, want %v (order is the label order)", got, want)
		}
	}
	if !b.Has("span.name") || b.Has("http.route") {
		t.Error("Has does not report the built-in set")
	}
	if b.Filter(nil) != nil {
		t.Error("Filter(nil) should stay nil")
	}
}
