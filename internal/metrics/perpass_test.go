package metrics

import (
	"context"
	"sync"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// fourFieldSource is a stats source published as four func metrics, the shape
// obs's Register*Stats hooks give every multi-field snapshot. Each call bumps
// every field, so a pass that sampled it more than once reports fields that
// disagree.
type fourFieldSource struct {
	calls int
	n     float64
}

type fourFields struct{ a, b, c, d float64 }

func (s *fourFieldSource) read() fourFields {
	s.calls++
	s.n++
	return fourFields{s.n, s.n, s.n, s.n}
}

func registerFourFields(r *Registry, snap func() fourFields) {
	r.GaugeFunc("perpass_a", "d", func() float64 { return snap().a })
	r.GaugeFunc("perpass_b", "d", func() float64 { return snap().b })
	r.GaugeFunc("perpass_c", "d", func() float64 { return snap().c })
	r.GaugeFunc("perpass_d", "d", func() float64 { return snap().d })
}

// One Export evaluates the four funcs back to back; unmemoised, that took the
// source's lock four times per export and published four separately-sampled
// readings of a struct whose whole point is that it is one instant.
func TestPerPassSamplesTheSourceOncePerExport(t *testing.T) {
	r := NewRegistry()
	src := &fourFieldSource{}
	registerFourFields(r, PerPass(r, src.read))

	exp := &capExporter{}
	if err := r.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	if src.calls != 1 {
		t.Fatalf("the source was sampled %d times for one export, want 1", src.calls)
	}
	for _, name := range []string{"perpass_a", "perpass_b", "perpass_c", "perpass_d"} {
		m, ok := exp.find(name)
		if !ok {
			t.Fatalf("%s not exported", name)
		}
		if got := m.Gauge().DataPoints().At(0).DoubleValue(); got != 1 {
			t.Errorf("%s = %v, want 1: the four fields of one export are not one snapshot", name, got)
		}
	}
}

// The scrape path is a pass too: one Dump samples the source once.
func TestPerPassSamplesTheSourceOncePerDump(t *testing.T) {
	r := NewRegistry()
	src := &fourFieldSource{}
	registerFourFields(r, PerPass(r, src.read))

	for _, d := range r.Dump() {
		if len(d.Points) != 1 || d.Points[0].Value != 1 {
			t.Errorf("%s = %+v, want one point at 1", d.Name, d.Points)
		}
	}
	if src.calls != 1 {
		t.Fatalf("the source was sampled %d times for one dump, want 1", src.calls)
	}
}

// The memo is a pass, not a cache: the next pass sees the source as it is
// then — including one that runs immediately after, which is the shutdown
// path's shape (Registry.Run's cancel-time export, then the mains' FinalExport
// after a final sweep moved the source). A clock-window memo republished the
// pre-sweep reading there unless its caller remembered to invalidate it.
func TestPerPassBackToBackPassesEachReadAfresh(t *testing.T) {
	r := NewRegistry()
	src := &fourFieldSource{}
	registerFourFields(r, PerPass(r, src.read))

	exp := &capExporter{}
	for i := 1; i <= 3; i++ {
		if err := r.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
			t.Fatal(err)
		}
		m, _ := exp.find("perpass_d")
		if got := m.Gauge().DataPoints().At(0).DoubleValue(); got != float64(i) {
			t.Fatalf("export %d read %v, want %d: a back-to-back pass reused the previous reading", i, got, i)
		}
	}
	r.Dump()
	if src.calls != 4 {
		t.Fatalf("the source was sampled %d times over three exports and a dump, want 4", src.calls)
	}
}

// Before the registry's first pass there is no pass to memoise over: a direct
// call reads the source every time.
func TestPerPassBeforeAnyPassIsNotMemoised(t *testing.T) {
	r := NewRegistry()
	src := &fourFieldSource{}
	snap := PerPass(r, src.read)
	snap()
	snap()
	if src.calls != 2 {
		t.Fatalf("the source was sampled %d times for two direct calls, want 2", src.calls)
	}
}

// After a pass has started, a direct call is attributed to it: the registry
// numbers a pass when it BEGINS and records no end, so a call between passes
// cannot tell it is outside one. It returns the latest pass's reading rather
// than re-sampling — the doc used to promise a fresh read "outside a pass",
// which held only before the first one — and the next pass reads afresh.
func TestPerPassDirectCallAfterAPassReturnsThatPassesReading(t *testing.T) {
	r := NewRegistry()
	src := &fourFieldSource{}
	snap := PerPass(r, src.read)
	registerFourFields(r, snap)

	if err := r.Export(context.Background(), &capExporter{}, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if got := snap().d; got != 1 {
			t.Fatalf("direct call %d after the export read %v, want the export's reading 1", i+1, got)
		}
	}
	if src.calls != 1 {
		t.Fatalf("the source was sampled %d times for one export and two direct calls, want 1", src.calls)
	}

	// An unregistered wrapper (one no pass evaluates) is still attributed to
	// the latest pass: the first direct call claims that pass, and a call after
	// the next pass reads afresh.
	other := &fourFieldSource{}
	unregistered := PerPass(r, other.read)
	unregistered()
	unregistered()
	if other.calls != 1 {
		t.Fatalf("an unregistered wrapper read its source %d times within one pass, want 1", other.calls)
	}
	r.Dump()
	if got := snap().d; got != 2 {
		t.Fatalf("direct call after a second pass read %v, want that pass's reading 2", got)
	}
	if src.calls != 2 {
		t.Fatalf("the source was sampled %d times over two passes, want 2", src.calls)
	}
	unregistered()
	if other.calls != 2 {
		t.Fatalf("an unregistered wrapper read its source %d times over two passes, want 2", other.calls)
	}
}

// Export and Dump may overlap (the Registry supports concurrent exporters), so
// the memo is shared between passes running at once: it must stay race-free
// and can at most re-sample, never sample more than once per pass started.
func TestPerPassUnderConcurrentPasses(t *testing.T) {
	r := NewRegistry()
	var mu sync.Mutex
	calls := 0
	snap := PerPass(r, func() fourFields {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return fourFields{1, 1, 1, 1}
	})
	registerFourFields(r, snap)

	const passes = 50
	var wg sync.WaitGroup
	for i := range passes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				_ = r.Export(context.Background(), &lockedCapExporter{}, pcommon.NewResource())
				return
			}
			r.Dump()
		}(i)
	}
	wg.Wait()
	if calls > passes {
		t.Fatalf("the source was sampled %d times over %d passes", calls, passes)
	}
}
