package cumagg

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// blockingHarness is a store whose sends can be held open, so two exports can be
// made to overlap on purpose.
type blockingHarness struct {
	store *Store[*series]
	now   time.Time

	mu       sync.Mutex
	renders  int
	rendered chan struct{} // one signal per render

	entered chan struct{} // a send has begun
	release chan struct{} // the held send may finish
	sends   int
	failNth int // this send (1-based) fails
}

func newBlockingHarness() *blockingHarness {
	h := &blockingHarness{
		now:      time.Unix(1_700_000_000, 0),
		rendered: make(chan struct{}, 8),
		entered:  make(chan struct{}, 8),
		release:  make(chan struct{}),
	}
	h.store = NewStore(Options[*series]{
		Scope:          "test",
		Name:           "test metrics",
		MaxCardinality: 10,
		StaleAfter:     time.Minute,
		Now:            func() time.Time { return h.now },
		NewSeries:      func() *series { return &series{} },
		Render:         h.render,
		ResetExemplars: (*series).reset,
	})
	return h
}

func (h *blockingHarness) observe(key string) {
	h.store.Lock()
	defer h.store.Unlock()
	s, _, ok := h.store.AdmitLocked([]byte(key), h.now)
	if !ok {
		return
	}
	s.calls++
	h.store.ObservedLocked(s, h.now)
}

func (h *blockingHarness) render(sm pmetric.ScopeMetrics, _ time.Time) {
	h.store.Lock()
	defer h.store.Unlock()
	if h.store.CountLocked() == 0 {
		return
	}
	dps := SumMetric(sm, "calls", "", "")
	h.store.EachLocked(func(s *series) {
		h.store.MarkRenderedLocked(s)
		p := dps.AppendEmpty()
		p.SetIntValue(int64(s.calls))
	})
	h.mu.Lock()
	h.renders++
	h.mu.Unlock()
	h.rendered <- struct{}{}
}

func (h *blockingHarness) ExportMetrics(_ context.Context, _ pmetric.Metrics) error {
	h.mu.Lock()
	h.sends++
	n := h.sends
	fail := n == h.failNth
	h.mu.Unlock()
	h.entered <- struct{}{}
	if n == 1 {
		<-h.release // the first send is the slow one
	}
	if fail {
		return errFail{}
	}
	return nil
}

// state reads the one series' delivery state.
func (h *blockingHarness) state() State {
	var st State
	h.store.Range(func(_ string, s *series) bool { st = s.meta().State; return false })
	return st
}

// Export is one transaction over the delivery gate. Overlapping exports would
// otherwise let a SUCCESSFUL send mark values delivered that only a FAILED
// payload ever carried: a slow export renders v1, an observation folds v2, a
// second export renders v2 and fails, and the first one's success then promotes
// the v2-carrying series — after which stale eviction may destroy the v1 -> v2
// delta (and the window's exemplars) with no collector having seen it.
func TestOverlappingExportsCannotCertifyEachOthersValues(t *testing.T) {
	h := newBlockingHarness()
	h.failWith(2) // the SECOND send fails
	h.observe("a")

	ctx := context.Background()
	res := pcommon.NewResource()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := h.store.Export(ctx, h, res); err != nil {
			t.Errorf("the first export: %v", err)
		}
	}()
	<-h.entered // the first export has rendered v1 and is inside its send
	<-h.rendered

	// A new observation the first payload does not carry.
	h.observe("a")

	wg.Add(1)
	go func() {
		defer wg.Done()
		// Fails; whether it even renders depends on the serialization.
		_ = h.store.Export(ctx, h, res)
	}()
	// The second export must NOT render while the first holds the gate. There is
	// no event to wait for when it is correctly blocked, so give it a window it
	// would comfortably beat if it were not.
	select {
	case <-h.rendered:
		t.Fatal("a second export rendered while the first was still in flight: its render is now the one the first export's success will mark delivered")
	case <-time.After(200 * time.Millisecond):
	}

	close(h.release)
	wg.Wait()

	// The second export rendered the new value and FAILED, so nothing may claim
	// it was delivered.
	if got := h.state(); got == Delivered {
		t.Fatal("values only a failed payload carried are marked Delivered; stale eviction may now destroy them unseen")
	}
}

func (h *blockingHarness) failWith(nth int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.failNth = nth
}
