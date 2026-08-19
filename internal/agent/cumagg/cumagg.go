// Package cumagg is the cumulative-series state machine behind the agent's two
// self-contained OTLP aggregators: agent/spanmetrics (per-span RED metrics) and
// agent/servicegraph (paired edge metrics).
//
// Neither can use the shared metrics.Registry — exemplars are a histogram
// data-point feature it cannot express, and it has no way to give a series its
// OWN start timestamp, which is how a cumulative reset after an eviction is
// spelled. So each owns its aggregation, and each needs the same non-obvious
// decisions:
//
//   - a series' values move observed -> rendered -> delivered, and only a
//     DELIVERED series may be evicted: an export interval may legally exceed
//     staleAfter and an export can fail, so evicting on staleness alone destroys
//     observations no collector ever saw;
//   - an export is ONE transaction over that gate — render, send, mark — and
//     two of them must not interleave: the mark promotes whatever is currently
//     Rendered and cannot tell whose render it was, so one export's success
//     would certify values only the other's FAILED payload carried;
//   - eviction is what keeps the cardinality cap from being a ONE-WAY LATCH, so
//     it is not optional and disabling it has to be spelled out;
//   - a re-created series gets a FRESH start timestamp, or the restarted
//     counters read downstream as a counter jumping backwards;
//   - exemplars are cleared on DELIVERY, not on rendering, so a failed send
//     keeps its evidence for the retry;
//   - a cycle with nothing to report sends no payload at all;
//   - the export loop makes one final export on a DETACHED context, because the
//     last window's observations are as real as any other.
//
// Written twice, they drifted twice. A negative staleAfter was a config error in
// one and a silent clamp to "eviction disabled" in the other — which is the cap
// back as a latch, arrived at through the field whose whole purpose is to
// prevent it. And the built-in-label collision guard existed in two shapes and
// two places. One implementation is the fix; ParseStaleAfter and Builtins are
// the two rules that drifted.
//
// What deliberately stays with the callers, because it answers to a different
// contract in each: the metric NAMES, shells and label sets (servicegraph's are
// Grafana-Tempo-verbatim, spanmetrics' are OTel-dotted); the cardinality and
// eviction COUNTERS, one pair per aggregator, so an operator can see WHICH cap
// bound; the per-series aggregate and its histograms (one there, two here); and
// the render's locking strategy, which is a performance decision about that
// aggregator's receive path rather than part of the state machine.
package cumagg

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/transform"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// finalExportTimeout bounds the detached export Run makes after its context is
// cancelled: long enough for one round trip to a collector that is up, short
// enough that a shutting-down process is not held by one that is not.
const finalExportTimeout = 10 * time.Second

// Counter is the one thing an aggregator's obs counters are used for here.
// Taking it as an interface keeps this package free of internal/obs — and makes
// "which cap bound" a per-caller answer rather than a shared series.
type Counter interface{ Inc() }

// Exporter sends one OTLP metrics payload; satisfied by otlpexport.Client.
type Exporter interface {
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

// State tracks a series' values from observation to delivery, so eviction only
// ever drops values the collector has acked.
type State uint8

const (
	// Observed means there are new values since the last render.
	Observed State = iota
	// Rendered means the values went into a payload; delivery is unknown.
	Rendered
	// Delivered means a payload carrying them was acked.
	Delivered
)

// Meta is the bookkeeping every cumulative series carries. Embed it in the
// aggregate; the Store reads and writes it through Series.
type Meta struct {
	// Start is when this series was created. A series re-created after an
	// eviction restarts its cumulative counters from zero, and a fresh start
	// timestamp is how OTLP spells that reset.
	Start time.Time
	// LastSeen is the last observation. It is the staleness input; State is the
	// safety one, and eviction needs both.
	LastSeen time.Time
	// State is where the CURRENT values have got to (see State).
	State State
}

func (m *Meta) meta() *Meta { return m }

// Series is the constraint on a Store's value type: any pointer to a struct
// embedding Meta. The method is unexported, so embedding Meta is the only way
// to satisfy it — an aggregate cannot accidentally opt out of the bookkeeping
// the Store is about to do on its behalf.
type Series interface{ meta() *Meta }

// Options configures a Store. Everything here is either a bound the operator
// set or a callback for the part that differs between aggregators.
type Options[S Series] struct {
	// Scope is the OTLP instrumentation scope name stamped on every payload.
	Scope string
	// Name is what the export loop's log lines call this aggregate ("span
	// metrics", "service-graph metrics").
	Name string
	// MaxCardinality bounds the number of distinct series. A new tuple over the
	// cap is refused and counted; the series already admitted keep reporting,
	// because these are cumulative.
	MaxCardinality int
	// StaleAfter evicts a series whose values a delivered export carried and
	// that has not been observed since. Zero DISABLES eviction — parse the
	// operator's spelling with ParseStaleAfter, which is where that reading
	// (and the refusal of a negative) lives.
	StaleAfter time.Duration
	// Dropped counts cardinality-cap refusals, Evicted counts stale evictions.
	// Both are per-aggregator: the two caps bind for different reasons and an
	// operator has to be able to tell which one did.
	Dropped, Evicted Counter
	// Now is the clock. It is a func rather than a captured time source so a
	// caller can keep an injectable field of its own and have the Store read
	// the SAME one (pass a method value, not the field).
	Now func() time.Time
	// NewSeries makes an empty series. The Store stamps Meta.Start on it; the
	// caller fills the parts only it knows about (its label set, its bucket
	// slices) when AdmitLocked reports the series is fresh.
	NewSeries func() S
	// Render writes the caller's metric shells and data points into sm. It must
	// leave sm EMPTY when there is nothing to report: that is what makes an idle
	// cycle send no payload at all. It is called with the Store unlocked — how
	// (and for how long) it takes the lock is the caller's decision.
	Render func(sm pmetric.ScopeMetrics, now time.Time)
	// ResetExemplars clears one series' exemplars after a DELIVERED export. nil
	// for an aggregator without exemplars.
	ResetExemplars func(S)
}

// Store holds the series map and runs the state machine over it.
//
// Its mutex is EXPORTED through Lock/Unlock rather than hidden behind
// call-me-with-a-closure methods, for one measured reason: both callers fold a
// span's or an edge's values into the series on their per-request path, that
// fold has to happen under the same hold as the lookup, and a closure passed
// per call is an allocation per request (both packages assert 0 allocs/op
// there). The *Locked methods are the state machine's steps; the caller writes
// the straight-line sequence between them.
type Store[S Series] struct {
	opt Options[S]

	mu     sync.Mutex
	series map[string]S
	// exportGate holds ONE token, taken for the whole of an Export — render,
	// send and delivery mark together. Export is otherwise NOT safe against
	// itself: afterDelivered promotes every series whose CURRENT values are
	// Rendered and cannot tell WHOSE render they were, so two exports in flight
	// at once let one's successful send mark values delivered that only the
	// other's FAILED payload carried — and eviction may then destroy them
	// unseen, exemplars included. Both callers have a path that overlaps
	// (an explicit final Export beside the one Run makes on its detached
	// context, which is what a spent producer-join budget leaves), and neither
	// caller's own render lock can see the other export at all.
	//
	// A channel rather than a Mutex because the acquire must be CANCELLABLE:
	// every final export in this repo runs on a shutdown budget, and waiting out
	// a send wedged against a dead collector would spend it in a lock nobody
	// can see (Buffered.drainGate is the same shape for the same reason).
	exportGate chan struct{}
}

// NewStore builds a Store from opt.
func NewStore[S Series](opt Options[S]) *Store[S] {
	if opt.Now == nil {
		opt.Now = time.Now
	}
	st := &Store[S]{opt: opt, series: make(map[string]S), exportGate: make(chan struct{}, 1)}
	st.exportGate <- struct{}{} // the token starts free
	return st
}

// Lock and Unlock guard the series map. Everything named *Locked below runs
// under them; nothing here ever takes another lock while holding this one
// except the Counter it was given.
func (st *Store[S]) Lock() { st.mu.Lock() }

// Unlock releases the series map.
func (st *Store[S]) Unlock() { st.mu.Unlock() }

// AdmitLocked returns the series stored under key, creating it when absent.
// fresh says the caller still has to materialize the parts only it knows about;
// ok is false when the cardinality cap refused a NEW series, which is counted
// here and never silent.
//
// key is a []byte and not a string on purpose: both callers build it on a stack
// buffer, and map[string(key)] does not copy for the lookup, so a warm series
// costs no allocation.
func (st *Store[S]) AdmitLocked(key []byte, now time.Time) (s S, fresh, ok bool) {
	if got, have := st.series[string(key)]; have {
		return got, false, true
	}
	if len(st.series) >= st.opt.MaxCardinality {
		if st.opt.Dropped != nil {
			st.opt.Dropped.Inc()
		}
		return s, false, false // s is still the zero value
	}
	s = st.opt.NewSeries()
	s.meta().Start = now
	st.series[string(key)] = s
	return s, true, true
}

// ObservedLocked records that s was just observed. It is the last step of a
// fold: LastSeen feeds staleness, and the state going back to Observed is what
// stops eviction from dropping values a render has not carried yet.
func (st *Store[S]) ObservedLocked(s S, now time.Time) {
	m := s.meta()
	m.LastSeen = now
	m.State = Observed
}

// MarkRenderedLocked records that s' current values went into a payload. Only a
// series that reaches Delivered from here may ever be evicted.
func (st *Store[S]) MarkRenderedLocked(s S) { s.meta().State = Rendered }

// EvictLocked drops series that a delivered export carried and that have not
// been observed within StaleAfter. Without it the map only ever grows: dead
// series render into every export forever and — worse — the cardinality cap
// becomes a ONE-WAY LATCH, so one burst of short-lived label values permanently
// blinds the aggregate for everything that starts afterwards. A cumulative
// series that stops being reported is the standard staleness signal downstream.
func (st *Store[S]) EvictLocked(now time.Time) {
	if st.opt.StaleAfter <= 0 { // eviction disabled
		return
	}
	for k, s := range st.series {
		// Only a series whose CURRENT values a delivered export carried may go:
		// an export interval longer than StaleAfter — or a failed export — must
		// not destroy observations unseen.
		if m := s.meta(); m.State == Delivered && now.Sub(m.LastSeen) > st.opt.StaleAfter {
			delete(st.series, k)
			if st.opt.Evicted != nil {
				st.opt.Evicted.Inc()
			}
		}
	}
}

// CountLocked is the number of live series.
func (st *Store[S]) CountLocked() int { return len(st.series) }

// EachLocked calls f for every live series, in map order. It does NOT mark
// anything rendered: a caller that is building a payload calls
// MarkRenderedLocked as it goes, and one that is only looking must not.
func (st *Store[S]) EachLocked(f func(S)) {
	for _, s := range st.series {
		f(s)
	}
}

// PointersLocked appends every live series to dst and returns it. It is the
// cheapest pass that can exist over the map — one pointer write each — and it
// is what lets an expensive render walk the series across lock RELEASES: a
// slice can be walked in chunks, a map cannot.
func (st *Store[S]) PointersLocked(dst []S) []S {
	for _, s := range st.series {
		dst = append(dst, s)
	}
	return dst
}

// Len is CountLocked for a caller that holds nothing.
func (st *Store[S]) Len() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.series)
}

// Range calls f for every live series under the lock, stopping early on false.
// Diagnostics and tests; a render uses the *Locked steps directly.
func (st *Store[S]) Range(f func(key string, s S) bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	for k, s := range st.series {
		if !f(k, s) {
			return
		}
	}
}

// StaleAfter is the resolved eviction age (0 = eviction disabled).
func (st *Store[S]) StaleAfter() time.Duration { return st.opt.StaleAfter }

// Render builds one payload under res: the resource, the scope, and whatever
// Options.Render writes into it. A render that produced no metric at all yields
// an EMPTY pmetric.Metrics, which Export sends nothing for.
//
// A PARTIALLY empty render is dropped down to its populated metrics here, and
// that is a guarantee this seam owes rather than a repair of its callers.
// SumMetric and HistMetric append a metric's shell and hand back a data-point
// slice the caller may or may not write into, so the "leave the scope empty
// when there is nothing to report" contract in Options.Render's doc is kept by
// each caller precomputing whether it has anything — spanmetrics returns before
// creating its three shells on an empty snapshot, servicegraph computes
// anyClient/anyServer across the whole snapshot before creating either
// histogram. Both are correct today, and neither is enforced by anything but
// its own arithmetic. A third aggregator built on this store would create its
// shells optimistically, and the metric with no data points that follows is
// legal OTLP: nothing downstream rejects it, so it would ship its name,
// description, unit and framing on every export forever with no counter
// anywhere moving. Two hundred nanoseconds once per export interval is a cheap
// price for making that unrepresentable.
func (st *Store[S]) Render(res pcommon.Resource, now time.Time) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	res.CopyTo(rm.Resource())
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName(st.opt.Scope)
	// The version is the build's, exactly as every other producer stamps it —
	// there is no per-aggregator knob because there is nothing per-aggregator
	// about it. Withholding it here made the two trace-tier families
	// (traces.span.metrics.*, traces_service_graph_*) the only kubescrape
	// payloads carrying otel_scope_version="", so a query grouping by that
	// label silently excluded them from every release.
	sm.Scope().SetVersion(obs.ScopeVersion)

	st.opt.Render(sm, now)
	sm.Metrics().RemoveIf(noDataPoints)
	if sm.Metrics().Len() == 0 {
		return pmetric.NewMetrics() // nothing to send this cycle
	}
	return md
}

// noDataPoints reports whether m carries no data points, across every metric
// type and including an untyped one.
func noDataPoints(m pmetric.Metric) bool {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		return m.Gauge().DataPoints().Len() == 0
	case pmetric.MetricTypeSum:
		return m.Sum().DataPoints().Len() == 0
	case pmetric.MetricTypeHistogram:
		return m.Histogram().DataPoints().Len() == 0
	case pmetric.MetricTypeExponentialHistogram:
		return m.ExponentialHistogram().DataPoints().Len() == 0
	case pmetric.MetricTypeSummary:
		return m.Summary().DataPoints().Len() == 0
	default:
		return true
	}
}

// Export renders the current cumulative aggregate under res and sends it once.
// The order is the contract: render, send, and only THEN mark delivered — the
// delivered mark is what unlocks eviction and clears the exemplars, so a failed
// send must leave both alone.
//
// Exports are SERIALIZED (see Store.exportGate): the three steps are one
// transaction over the delivery gate, and a caller whose ctx ends while another
// export holds the gate gets that ctx's error having sent nothing — the values
// are cumulative, so the next export carries them.
func (st *Store[S]) Export(ctx context.Context, exp Exporter, res pcommon.Resource) error {
	select {
	case <-st.exportGate:
		defer func() { st.exportGate <- struct{}{} }()
	case <-ctx.Done():
		return ctx.Err()
	}
	md := st.Render(res, st.opt.Now())
	if md.ResourceMetrics().Len() == 0 {
		return nil
	}
	// Handoff to the transform seam: md is fresh pdata this Render just built
	// and is never re-offered — a failed send leaves the series Rendered and
	// the next export renders again — so the transform wrapper may run its
	// script in place instead of deep-copying the payload.
	if err := exp.ExportMetrics(transform.Handoff(ctx), md); err != nil {
		return err
	}
	st.afterDelivered()
	return nil
}

// afterDelivered records that the rendered values reached the collector (only
// those may later be evicted) and resets every recorded exemplar. A series
// OBSERVED between the render and this call is back in Observed and is
// deliberately not marked: its new values must still be exported before
// eviction may touch them. An exemplar recorded in that same window is dropped
// unseen — the one-interval recency window an exemplar has by nature.
func (st *Store[S]) afterDelivered() {
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, s := range st.series {
		if m := s.meta(); m.State == Rendered {
			m.State = Delivered
		}
		if st.opt.ResetExemplars != nil {
			st.opt.ResetExemplars(s)
		}
	}
}

// Run exports every interval until ctx is done, then once more. A non-positive
// interval falls back to one minute (NewTicker would panic).
func (st *Store[S]) Run(ctx context.Context, exp Exporter, interval time.Duration, res pcommon.Resource, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// The final export runs on a DETACHED context: ctx is already done,
			// and the last window's observations are as real as any other.
			// WithoutCancel, not Background: any values the caller put on ctx
			// (the otlpexport ownership marker rides there) must survive the
			// detach — the repo's shutdown-context invariant.
			fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalExportTimeout)
			if err := st.Export(fctx, exp, res); err != nil {
				log.Warn("final "+st.opt.Name+" export failed", "error", err)
			}
			cancel()
			return
		case <-ticker.C:
			if err := st.Export(ctx, exp, res); err != nil {
				log.Warn("exporting "+st.opt.Name+" failed", "error", err)
			}
		}
	}
}
