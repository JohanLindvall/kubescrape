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
// bound; and the per-series aggregate (one latency histogram there, two here)
// with its snapshot element. The latency histogram itself (Hist, HistSnap,
// PutHistPoint) and the render's chunked snapshot (Snapshotter) are shared:
// both aggregators had written them out line for line, made the same decisions
// for the same reasons with the same constants, and only one copy was tested.
package cumagg

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/transform"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/otlpsplit"
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
	// rendered is every series the current render marked (MarkRenderedLocked
	// appends; guarded by mu), and it is the whole of what the delivery mark
	// walks — see afterDelivered. Export empties it on the way in and on every
	// way out, so it never outlives the export whose render filled it: a failed
	// send's list would otherwise pin series that eviction has since dropped.
	// The backing array is reused across exports.
	rendered []S
	// capRefused counts cardinality-cap refusals since the last time the export
	// loop reported them. The COUNTER (Options.Dropped) carries the rate; this
	// is what lets the loop emit ONE throttled line per interval instead of one
	// per refused series on the caller's per-span/per-edge path, which is
	// allocation-budgeted in both callers. Atomic rather than mutex-guarded
	// because Run reads it without the series lock.
	capRefused atomic.Uint64
	// capWarn throttles that line: the cap, once reached, is reached on every
	// admission until eviction frees a slot.
	capWarn logdedupe.Throttle

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
		// One atomic add, on the refusal path only: the caller's warm path is
		// asserted allocation-free and runs per span / per edge, so the LINE is
		// the export loop's job (reportCapPressure).
		st.capRefused.Add(1)
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

// MarkRenderedLocked records that s' current values went into a payload, and
// lists s for the delivery mark that follows a successful send. Only a series
// that reaches Delivered from here may ever be evicted.
func (st *Store[S]) MarkRenderedLocked(s S) {
	s.meta().State = Rendered
	st.rendered = append(st.rendered, s)
}

// LivePointersLocked evicts the stale series and appends every survivor to dst,
// in ONE walk of the map.
//
// Eviction drops series that a delivered export carried and that have not been
// observed within StaleAfter. Without it the map only ever grows: dead series
// render into every export forever and — worse — the cardinality cap becomes a
// ONE-WAY LATCH, so one burst of short-lived label values permanently blinds the
// aggregate for everything that starts afterwards. A cumulative series that
// stops being reported is the standard staleness signal downstream.
//
// The two steps are ONE pass because the WALK is the cost, and this walk is the
// only whole-map pass a render cannot get rid of: a map cannot be iterated
// across lock releases, so it is the one hold that scales with MaxCardinality
// while everything downstream of it is chunked. Evicting in a pass of its own
// doubled it for nothing — both callers ran EvictLocked and PointersLocked
// back to back inside one hold, so the series were visited twice and the stall
// on the receive path (Record/observe take this same mutex, the pairing store
// from inside its own) was twice what it had to be.
//
// dst is the caller's reused scratch; the survivors come back in map order.
func (st *Store[S]) LivePointersLocked(dst []S, now time.Time) []S {
	evict := st.opt.StaleAfter > 0
	for k, s := range st.series {
		// Only a series whose CURRENT values a delivered export carried may go:
		// an export interval longer than StaleAfter — or a failed export — must
		// not destroy observations unseen.
		if m := s.meta(); evict && m.State == Delivered && now.Sub(m.LastSeen) > st.opt.StaleAfter {
			delete(st.series, k)
			if st.opt.Evicted != nil {
				st.opt.Evicted.Inc()
			}
			continue
		}
		dst = append(dst, s)
	}
	return dst
}

// Len is the number of live series, taken under the lock.
func (st *Store[S]) Len() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.series)
}

// Range calls f for every live series under the lock, stopping early on false.
// Only tests call it (the aggregators' own, from their packages); a render uses
// the *Locked steps directly, and f runs under the lock, so it must not take it.
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
// type and including an untyped one: the predicate Render's prune turns on.
func noDataPoints(m pmetric.Metric) bool { return otlpsplit.DataPointCount(m) == 0 }

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
	// The delivery mark walks exactly what THIS render marked, so the list
	// starts empty and is dropped on every way out — a failed send included,
	// whose series stay Rendered and are re-marked (and re-listed) by the next
	// render.
	st.resetRendered()
	defer st.resetRendered()
	md := st.Render(res, st.opt.Now())
	if md.ResourceMetrics().Len() == 0 {
		return nil
	}
	// Handoff to the transform seam: md is fresh pdata this Render just built
	// and is never re-offered — a failed send leaves the series Rendered and
	// the next export renders again — so the transform wrapper may run its
	// script in place instead of deep-copying the payload. Consumed: the next
	// render is a new point, never these again, so a failed send's script
	// drops are counted now or never.
	if err := exp.ExportMetrics(transform.Consumed(ctx), md); err != nil {
		return err
	}
	st.afterDelivered()
	return nil
}

// afterDelivered records that the rendered values reached the collector (only
// those may later be evicted) and resets the exemplars the payload carried. A
// series OBSERVED between the render and this call is back in Observed and is
// deliberately not marked: its new values must still be exported before
// eviction may touch them. An exemplar recorded on a rendered series in that
// same window is dropped unseen — the one-interval recency window an exemplar
// has by nature. A series ADMITTED in that window was in no payload, so it is
// not in the list and its exemplars are left alone.
//
// It walks the list the render built (st.rendered) rather than the map, so it
// makes no whole-map pass of its own. It used to take every live series' pointer
// in a second unchunked hold, although the only series it can promote are the
// ones this export's render just marked — only MarkRenderedLocked sets Rendered,
// the exportGate keeps any other render out, and eviction happens only inside
// the render's own LivePointersLocked walk. That walk is the one whole-map hold
// an export makes, and the one a map forces (it cannot be iterated across lock
// releases); the second one was 25-40% more of the same stall for nothing.
//
// CHUNKED (eachChunked, the snapshot's own walker), for the reason the snapshot
// is: this mutex is the one Record/observe take per edge and per span — the
// pairing store takes it from inside its OWN mutex — so every millisecond held
// here is a millisecond in which no shard goroutine can pair or aggregate. Held
// whole it was linear in MaxCardinality with nothing bounding it; a slice can be
// walked across lock releases where a map cannot.
func (st *Store[S]) afterDelivered() {
	st.mu.Lock()
	// Appended to only under mu, and nothing rewrites the entries below this
	// length before Export's deferred reset.
	ptrs := st.rendered
	st.mu.Unlock()

	st.eachChunked(ptrs, st.markDeliveredLocked)
}

// markDeliveredLocked is afterDelivered's step for one series. A series
// observed between the render and its chunk is back in Observed and is left
// alone, exactly as one observed before the first chunk always was.
func (st *Store[S]) markDeliveredLocked(_ int, s S) {
	if m := s.meta(); m.State == Rendered {
		m.State = Delivered
	}
	if st.opt.ResetExemplars != nil {
		st.opt.ResetExemplars(s)
	}
}

// resetRendered empties the delivery mark's list, dropping its references so it
// pins nothing between exports.
func (st *Store[S]) resetRendered() {
	st.mu.Lock()
	clear(st.rendered)
	st.rendered = st.rendered[:0]
	st.mu.Unlock()
}

// Run exports every interval until ctx is done, then once more. A non-positive
// interval falls back to one minute — NewTicker would panic — and SAYS so: the
// operator's flag reads one value while the loop runs on another, and the
// startup lines print the flag. cmd/kubescrape-agent's validateConfig refuses a
// typed non-positive interval, so this is the guard for a value arriving any
// other way, not the operator-facing answer.
func (st *Store[S]) Run(ctx context.Context, exp Exporter, interval time.Duration, res pcommon.Resource, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	if interval <= 0 {
		log.Warn("a non-positive cumulative-metrics export interval has no meaning (it is not 'off' and not 'as fast as possible'); exporting every minute instead",
			"aggregate", st.opt.Name, "interval", interval)
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// The run of failed exports, LOCAL because Run is one goroutine per store:
	// the first failure of a run warns, the repeats are restated at most every
	// exportWarnEvery (Debug in between) carrying the run's cost, and the
	// recovery is one Info. It used to be a Warn per interval for the whole
	// outage, with nothing saying how long it had lasted.
	var outage logdedupe.Outage
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
				// The aggregate NAME is an attribute rather than part of the
				// message: one message text is one grep, and an operator
				// filtering "which aggregate" wants a field to select on.
				log.Warn("the final cumulative-metrics export failed, so this process' last aggregation window is lost",
					"error", err, "aggregate", st.opt.Name)
			}
			cancel()
			return
		case <-ticker.C:
			err := st.Export(ctx, exp, res)
			now := time.Now()
			switch {
			case err != nil:
				if _, loud := outage.Fail(now, exportWarnEvery); loud {
					log.Warn("exporting cumulative metrics failed; the series are cumulative, so the next export carries them",
						"error", err, "aggregate", st.opt.Name, "failures", outage.Failures(), "outage", outage.Lasted(now))
				} else {
					log.Debug("exporting cumulative metrics failed", "error", err, "aggregate", st.opt.Name,
						"failures", outage.Failures())
				}
			case outage.Failing():
				failures, lasted, _ := outage.Recover(now)
				log.Info("cumulative-metrics export recovered", "aggregate", st.opt.Name,
					"failures", failures, "outage", lasted)
			}
			st.reportCapPressure(log)
		}
	}
}

// reportCapPressure emits the throttled aggregate line for cardinality-cap
// refusals since the last report. It is the sweep-side half of the pattern the
// repo uses wherever the refusal itself is on an allocation-budgeted path: a
// counter per item, a line per interval.
//
// A refusal is not a slow leak — a cumulative aggregate at its cap reports
// nothing about anything NEW until eviction frees a slot, so a service that
// starts after the burst is invisible in RED metrics or in the graph. That is
// what makes it a Warn rather than an Info.
//
// The ORDER is the whole point, and it is the same rule tailbuffer's
// takeEarlyLocked spells out for the same shape. Draining the tally first and
// consulting the throttle afterwards zeroes it on every suppressed cycle, so
// the line that eventually escapes carries ONE export interval's refusals while
// claiming to describe capWarnEvery — at the default 1m interval against a 5m
// window that is an operator sizing a cardinality burst ~5x low. Ask the
// throttle FIRST and drain only on the cycle that actually emits; the
// suppressed cycles keep accumulating into the number the line will print, so
// it describes the window it names.
//
// Allow is asked only when there is something to say (capRefused.Load() > 0),
// or a quiet cycle would spend the slot and suppress the first cycle that
// really binds. The Load/Swap pair is not atomic together, and does not need to
// be: this runs on Run's single goroutine, and a refusal landing between them
// is simply carried by the Swap into the line about to be printed.
func (st *Store[S]) reportCapPressure(log *slog.Logger) {
	if st.capRefused.Load() == 0 || !st.capWarn.Allow(capWarnEvery) {
		return
	}
	n := st.capRefused.Swap(0)
	log.Warn("the cumulative-metrics cardinality cap is refusing new series, so anything that starts now is missing from these metrics until eviction frees a slot",
		"aggregate", st.opt.Name, "dropped", n, "maxCardinality", st.opt.MaxCardinality,
		"series", st.Len(), "staleAfter", st.opt.StaleAfter)
}

// capWarnEvery re-warns while the cardinality cap is binding.
const capWarnEvery = 5 * time.Minute

// exportWarnEvery restates a persisting export failure; the attempts between
// restatements are Debug.
const exportWarnEvery = 5 * time.Minute
