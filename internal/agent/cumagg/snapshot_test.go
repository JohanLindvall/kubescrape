package cumagg

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// histSeries and histSlot are a minimal aggregator in the shape both real ones
// have: a label set aliased into the snapshot, and a latency histogram copied
// out of it.
type histSeries struct {
	Meta
	labels []string
	lat    Hist
}

type histSlot struct {
	labels []string
	start  time.Time
	lat    HistSnap
}

// snapHarness drives a Store through Snapshotter.Take exactly as spanmetrics'
// and servicegraph's renders do, so the one implementation is tested once for
// both.
type snapHarness struct {
	store  *Store[*histSeries]
	snaps  Snapshotter[*histSeries, histSlot]
	bounds []float64
	now    time.Time
	// copyHook, when set, runs inside CopyLocked (under the series lock).
	copyHook func(i int64)
	copies   atomic.Int64
	// unmarked counts series CopyLocked received in a state other than
	// Rendered: Take must mark each one before handing it over.
	unmarked atomic.Int64
}

func newSnapHarness(maxCard int, stale time.Duration) *snapHarness {
	h := &snapHarness{bounds: []float64{0.1, 1, 10}, now: time.Unix(1_700_000_000, 0)}
	nb := len(h.bounds) + 1
	h.snaps = Snapshotter[*histSeries, histSlot]{
		CopyLocked: func(e *histSlot, s *histSeries) {
			n := h.copies.Add(1)
			if s.State != Rendered {
				h.unmarked.Add(1)
			}
			e.labels, e.start = s.labels, s.Start
			e.lat.CopyFrom(&s.lat)
			if h.copyHook != nil {
				h.copyHook(n)
			}
		},
		Fit:     func(e *histSlot) { e.lat.Fit(nb) },
		Release: func(e *histSlot) { e.labels = nil },
	}
	h.store = NewStore(Options[*histSeries]{
		Scope:          "test",
		Name:           "test metrics",
		MaxCardinality: maxCard,
		StaleAfter:     stale,
		Now:            func() time.Time { return h.now },
		NewSeries:      func() *histSeries { return &histSeries{} },
		Render:         h.render,
		ResetExemplars: func(s *histSeries) { s.lat.ClearExemplars() },
	})
	return h
}

func (h *snapHarness) observe(key string, v float64) {
	nb := len(h.bounds) + 1
	idx := BucketIndex(h.bounds, v)
	h.store.Lock()
	defer h.store.Unlock()
	s, fresh, ok := h.store.AdmitLocked([]byte(key), h.now)
	if !ok {
		return
	}
	if fresh {
		s.labels = []string{key}
	}
	s.lat.Observe(idx, nb, v)
	s.lat.SetExemplar(idx, nb, v, pcommon.NewTimestampFromTime(h.now), pcommon.TraceID([16]byte{1}), pcommon.SpanID([8]byte{1}))
	h.store.ObservedLocked(s, h.now)
}

func (h *snapHarness) render(sm pmetric.ScopeMetrics, now time.Time) {
	snap := h.snaps.Take(h.store, now)
	if len(snap) == 0 {
		return
	}
	ts := pcommon.NewTimestampFromTime(now)
	dps := HistMetric(sm, "latency", "")
	for i := range snap {
		PutHistPoint(dps.AppendEmpty(), &snap[i].lat, h.bounds, ClampStart(pcommon.NewTimestampFromTime(snap[i].start), ts), ts)
	}
}

func (h *snapHarness) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }

func (h *snapHarness) export(t *testing.T) {
	t.Helper()
	if err := h.store.Export(context.Background(), h, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
}

// arrayCap totals the bucket and exemplar slots the whole scratch holds — its
// capacity, not its current length, which is the memory that stays reachable
// between renders.
func (h *snapHarness) arrayCap() int {
	whole := h.snaps.snap[:cap(h.snaps.snap)]
	n := 0
	for i := range whole {
		n += cap(whole[i].lat.Buckets) + cap(whole[i].lat.Ex)
	}
	return n
}

// The render scratch is REUSED across renders, and a later, smaller render
// reslices it. The tail past the new length keeps its bucket and exemplar
// capacity on purpose — that reuse is what the scratch is for — but it must not
// keep the LABELS of series that are gone: those alias the label sets of
// evicted series, so a burst up to the cardinality cap followed by mass stale
// eviction would pin every one of them (a slice each, plus their retained
// strings) for the process' life. Both aggregators' scratches are this one; the
// Release callback each one passes is pinned in its own package, since this
// harness supplies its own.
func TestSnapshotDoesNotPinTheLabelsOfEvictedSeries(t *testing.T) {
	const peak = 64
	h := newSnapHarness(peak+8, time.Minute)
	for i := range peak {
		h.observe(fmt.Sprintf("op-%03d", i), 0.5)
	}
	h.export(t) // renders every series and marks them delivered

	// The whole burst goes stale; the next render covers one series.
	h.now = h.now.Add(10 * time.Minute)
	h.observe("op-new", 0.5)
	h.export(t)

	if got := h.store.Len(); got != 1 {
		t.Fatalf("the store holds %d series after the eviction, want 1", got)
	}
	snap := h.snaps.snap
	if cap(snap) < peak {
		t.Fatalf("the scratch shrank to %d: the test no longer exercises its tail", cap(snap))
	}
	whole := snap[:cap(snap)]
	for i := len(snap); i < len(whole); i++ {
		if whole[i].labels != nil {
			t.Fatalf("scratch slot %d (past the %d live series) still holds an evicted series' labels %q", i, len(snap), whole[i].labels)
		}
	}
}

// Clearing the tail's labels bounds the STRINGS but not the ARRAYS: every
// unused slot keeps its bucket and exemplar slices, and an Exemplar is 48 B —
// 24.3 MB still reachable in servicegraph after one burst to the 20000-series
// cap and a mass stale eviction that left ONE series. The reuse is worth having
// for the steady state, not for a peak the process saw once, so a scratch that
// stays far under its capacity for snapShrinkRuns renders is rebuilt at the
// live size — and only then, because an oscillating workload must keep its
// capacity.
func TestSnapshotShrinksBackAfterACardinalityBurst(t *testing.T) {
	const peak = 2000
	h := newSnapHarness(peak+8, time.Minute)
	for i := range peak {
		h.observe(fmt.Sprintf("op-%04d", i), 0.5)
	}
	h.export(t)
	if cap(h.snaps.snap) < peak {
		t.Fatalf("the scratch holds %d slots after rendering %d series", cap(h.snaps.snap), peak)
	}
	burstArrays := h.arrayCap()

	h.now = h.now.Add(10 * time.Minute)
	for i := range snapShrinkRuns + 1 {
		h.observe("op-new", 0.5)
		h.export(t)
		h.now = h.now.Add(time.Second)
		// The hysteresis: not before snapShrinkRuns oversized renders.
		if i < snapShrinkRuns-1 && cap(h.snaps.snap) < peak {
			t.Fatalf("the scratch shrank to %d slots after %d oversized renders, want it kept until %d",
				cap(h.snaps.snap), i+1, snapShrinkRuns)
		}
	}

	if got := h.store.Len(); got != 1 {
		t.Fatalf("the store holds %d series after the eviction, want 1", got)
	}
	if got := cap(h.snaps.snap); got > peak/snapShrinkFactor {
		t.Errorf("the scratch still holds %d slots for 1 live series", got)
	}
	if got := h.arrayCap(); got > burstArrays/10 {
		t.Errorf("the scratch retains %d bucket+exemplar slots (peak render: %d): the tail's arrays are still reachable", got, burstArrays)
	}
}

// The snapshot's value copy is the render's chunked pass, and the one both
// aggregators' render-stall tests could not see: they gate on the pdata BUILD
// moving under the lock, and passed unchanged with the copy taken in one hold
// across the whole cardinality cap. Same shape as the delivery-mark test: park
// the copy inside its first chunk, queue a waiter, and require that it is served
// while series are still uncopied.
func TestSnapshotReleasesTheLockBetweenChunks(t *testing.T) {
	const series = 8 * snapChunk
	h := newSnapHarness(series+1, 0)
	for i := range series {
		h.observe(fmt.Sprintf("s-%05d", i), 0.5)
	}

	parked, release := make(chan struct{}), make(chan struct{})
	h.copyHook = func(n int64) {
		if n == 1 {
			close(parked)
			<-release
			return
		}
		if (n-1)%snapChunk == 0 {
			time.Sleep(chunkDwell) // see markHarness: give the waiter time to queue
		}
	}
	done := make(chan []histSlot, 1)
	go func() { done <- h.snaps.Take(h.store, h.now) }()
	<-parked

	acquired := make(chan struct{})
	var copiedWhenServed atomic.Int64
	go lockWaiter(h.store, func() { copiedWhenServed.Store(h.copies.Load()) }, acquired)
	waitUntilParkedOnMutex(t)
	close(release)

	<-acquired
	if got := len(<-done); got != series {
		t.Fatalf("Take returned %d slots, want %d", got, series)
	}
	if got := h.copies.Load(); got != series {
		t.Fatalf("CopyLocked ran %d times, want %d", got, series)
	}
	if n := h.unmarked.Load(); n != 0 {
		t.Errorf("%d series reached CopyLocked without being marked Rendered", n)
	}
	if served := copiedWhenServed.Load(); served >= series {
		t.Errorf("a queued Lock() was served only after all %d series had been copied: the snapshot is holding the series mutex for the whole slice, a receive-path stall linear in MaxCardinality", series)
	} else {
		t.Logf("the waiter was served after %d of %d series were copied", served, series)
	}
}

// The snapshot carries only the SET exemplars, in bucket order — the render
// writes nothing else, and copying every per-bucket slot put ~34 MB of memcpy
// under the series mutex at the cardinality cap — and an unobserved histogram
// (a virtual-node edge's missing side) is not Present, so it renders no
// zero-count point for a measurement nobody took.
func TestHistSnapCopiesOnlySetExemplarsAndPresence(t *testing.T) {
	bounds := []float64{0.1, 1, 10}
	nb := len(bounds) + 1
	var unobserved Hist
	var snap HistSnap
	snap.Fit(nb)
	snap.CopyFrom(&unobserved)
	if snap.Present {
		t.Fatal("an unobserved histogram is Present")
	}

	var h Hist
	tid, sid := pcommon.TraceID([16]byte{7}), pcommon.SpanID([8]byte{7})
	for _, v := range []float64{0.05, 5, 50} {
		i := BucketIndex(bounds, v)
		h.Observe(i, nb, v)
		if v != 5 {
			h.SetExemplar(i, nb, v, 1, tid, sid)
		}
	}
	snap.CopyFrom(&h)
	if !snap.Present || snap.Count != 3 || snap.Sum != h.Sum {
		t.Fatalf("snapshot = present %v count %d sum %v, want true 3 55.05", snap.Present, snap.Count, snap.Sum)
	}
	if len(snap.Ex) != 2 || snap.Ex[0].Value != 0.05 || snap.Ex[1].Value != 50 {
		t.Fatalf("snapshot exemplars = %+v, want the two SET ones in bucket order", snap.Ex)
	}

	p := pmetric.NewHistogramDataPoint()
	PutHistPoint(p, &snap, bounds, 1, 2)
	if p.Count() != 3 || p.BucketCounts().Len() != nb || p.ExplicitBounds().Len() != len(bounds) || p.Exemplars().Len() != 2 {
		t.Fatalf("point = count %d, %d buckets, %d bounds, %d exemplars", p.Count(), p.BucketCounts().Len(), p.ExplicitBounds().Len(), p.Exemplars().Len())
	}
	if p.StartTimestamp() != 1 || p.Timestamp() != 2 {
		t.Fatalf("point stamps = %d, %d, want 1, 2", p.StartTimestamp(), p.Timestamp())
	}

	h.ClearExemplars()
	snap.CopyFrom(&h)
	if len(snap.Ex) != 0 {
		t.Fatalf("a cleared histogram still snapshots %d exemplars", len(snap.Ex))
	}
}
