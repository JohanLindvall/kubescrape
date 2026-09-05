package metrics

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JohanLindvall/haste/xxh3"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
)

// Registry is a set of directly-driven series for a process's OWN
// observability metrics (counters, gauges, histograms), exported over OTLP
// like every other signal — there is no Prometheus exposition. The API
// mirrors the prometheus client (Inc/Add/Set/Observe, WithLabelValues) so
// call sites read the same; the storage is this package's series type.
//
// Registry series never expire and have no cardinality cap: label sets come
// from code, not data.
type Registry struct {
	mu     sync.Mutex
	series []*series
	// byName is the one series per metric NAME. A registration that repeats a
	// name reuses it rather than appending a second series, because a second
	// series renders a second Metric under the same name in one ScopeMetrics —
	// which the Prometheus arm cannot express at all: registryCollector emits
	// both as const metrics with an identical name and label set, Gather's
	// duplicate check fails, and promhttp answers 500 to EVERY scrape, i.e. the
	// whole self-metrics scrape is lost. Repeat registrations are the advertised
	// shape for the per-instance Register* hooks (two DynamicMetricSets in one
	// process each publish the log-metrics drop family).
	byName map[string]*series
	// funcNames marks the names registered through addFunc. Func-ness is part
	// of a name's shape: Dump reports a func-backed series from its live fns
	// and SKIPS its db, so a direct handle observing into the shared series
	// would ship on the OTLP push (Export folds both) and silently vanish from
	// the Prometheus scrape — the one -self-metrics-interval knob choosing the
	// delivery modality must not also choose the values.
	funcNames map[string]bool
	funcs     []*gaugeFunc
	// drops are this registry's own refusal counters. They are not published:
	// a registry has no cardinality cap and its label sets come from code, so
	// only a 64-bit hash collision between two code-defined label sets can move
	// them. They used to land on kubescrape_log_metrics_dropped_*, a metric
	// documented as the LOG-derived store's.
	drops drops
	// dumpLabelErrors counts data points Dump could not render because their
	// stored label string did not parse back (dump.go). It is PUBLISHED, unlike
	// drops, and the reason is the path: Dump serves the Prometheus /metrics
	// exposition, which is how this process's own telemetry is delivered when
	// -self-metrics-interval=0. A silent skip there shrinks the one signal an
	// operator uses to diagnose everything else, and it shrinks it invisibly —
	// the series simply is not in the response.
	dumpLabelErrors atomic.Uint64
	// dumpWarn throttles the line beside that counter. Dump runs once per
	// scrape and the condition is a STATE (the stored string does not become
	// well-formed on its own), so an unthrottled line is one per series per
	// scrape, forever.
	dumpWarn logdedupe.Throttle
}

// DumpLabelErrors reports how many data points Dump has skipped because their
// stored label string did not parse. Published through obs as
// kubescrape_self_metrics_points_skipped_total.
func (r *Registry) DumpLabelErrors() uint64 { return r.dumpLabelErrors.Load() }

// registryExpiration keeps snapshot's idle handling permanently inactive —
// a self-metric is cumulative for the process lifetime.
const registryExpiration = 200 * 365 * 24 * time.Hour

type gaugeFunc struct {
	s  *series
	fn func() float64
	// labelName/fnVec hold the LABELED form (GaugeFuncVec): fnVec returns one
	// value per label value, and each becomes its own data point. Set together;
	// when fnVec is non-nil, fn is unused.
	labelName string
	fnVec     func() map[string]float64
	// mu serializes the fn()+delta+observe read-modify-write of `last`
	// against concurrent Exports. The in-repo wiring is sequential (Run is
	// one goroutine; FinalExport runs after Run has returned), but the
	// package's own tests exercise concurrent exporters as a supported
	// pattern, and unguarded, two overlapping Exports would double-count or
	// lose counter deltas (and race on `last`).
	mu sync.Mutex
	// last is the previously pushed cumulative value of a COUNTER func: fn()
	// returns a running total, but the counter series accumulates observations
	// (samp.value += v), so each export must push only the delta — pushing the
	// total re-added the whole count every export, inflating a one-time burst
	// into a permanent per-interval rate.
	last float64
	// lastVec is last, per label value, for a LABELED counter func
	// (CounterFuncVec). The label set is data-driven — a value appears only
	// once it has something to report — so entries are created on demand and
	// an absent one means the stream starts at zero, which is exactly the
	// delta to push.
	lastVec map[string]float64
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry { return &Registry{} }

// add returns the series for name, creating it on first registration and
// REUSING it afterwards (see Registry.byName). Reuse is what makes a repeated
// registration aggregate rather than duplicate: the series ACCUMULATES what is
// observed into it, so two CounterFuncs of one name sum (each keeps its own
// delta bookkeeping in its gaugeFunc), and two handles on a gauge are two
// writers of one value, exactly as two handles on the same registered metric
// always were.
//
// Every Registry series folds with actionSet (a counter and a histogram
// accumulate; a gauge sets) — the windowed aggregations are the log-derived
// store's, so the action is not a parameter here.
//
// A repeat under a name already registered with a DIFFERENT shape panics: the
// two cannot both be rendered, registrations are code-driven and run at
// startup, and silently serving one shape to a call site that asked for the
// other is the failure this dedupe exists to prevent. Func-ness is part of
// that shape (Registry.funcNames): a mixed name would render on the push and
// not on the scrape, so it is a conflict even when kind and action agree.
func (r *Registry) add(name, desc string, kind seriesKind, buckets []float64, funcBacked bool) *series {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.byName[name]; ok {
		if s.kind != kind || s.action != actionSet || !s.sameBuckets(kind, buckets) {
			panic("metrics: " + name + " re-registered with a different type, action or buckets")
		}
		if r.funcNames[name] != funcBacked {
			panic("metrics: " + name + " re-registered as both a direct metric and a func-backed one")
		}
		return s
	}
	s := newSeries(seriesSpec{
		name: name, desc: desc, kind: kind, action: actionSet,
		expiration: registryExpiration, buckets: buckets, drops: &r.drops,
	})
	if r.byName == nil {
		r.byName = make(map[string]*series)
	}
	r.byName[name] = s
	if funcBacked {
		if r.funcNames == nil {
			r.funcNames = make(map[string]bool)
		}
		r.funcNames[name] = true
	}
	r.series = append(r.series, s)
	return s
}

// Counter registers a monotonic counter.
func (r *Registry) Counter(name, desc string) *RegCounter {
	return &RegCounter{newBound(r.add(name, desc, kindCounter, nil, false), nil)}
}

// CounterVec registers a labeled monotonic counter.
func (r *Registry) CounterVec(name, desc string, labelNames ...string) *RegCounterVec {
	return &RegCounterVec{vec[RegCounter]{
		s: r.add(name, desc, kindCounter, nil, false), keys: labelNames,
		wrap: func(b bound) *RegCounter { return &RegCounter{b} },
	}}
}

// Gauge registers a set-latest gauge.
func (r *Registry) Gauge(name, desc string) *RegGauge {
	return &RegGauge{newBound(r.add(name, desc, kindGauge, nil, false), nil)}
}

// addFunc is the one constructor behind the four func-metric registrations:
// a series of the given kind plus a gaugeFunc entry evaluated at export time.
// labelName/fnVec carry the labeled (Vec) form; fn the scalar one.
func (r *Registry) addFunc(name, desc string, kind seriesKind, labelName string, fn func() float64, fnVec func() map[string]float64) {
	s := r.add(name, desc, kind, nil, true)
	r.mu.Lock()
	r.funcs = append(r.funcs, &gaugeFunc{s: s, fn: fn, labelName: labelName, fnVec: fnVec})
	r.mu.Unlock()
}

// GaugeFunc registers a gauge evaluated at export time.
func (r *Registry) GaugeFunc(name, desc string, fn func() float64) {
	r.addFunc(name, desc, kindGauge, "", fn, nil)
}

// CounterFunc registers a monotonic counter whose value is read at export
// time (for counts owned by another package's atomics). The function must be
// non-decreasing; it renders as a cumulative monotonic sum.
func (r *Registry) CounterFunc(name, desc string, fn func() float64) {
	r.addFunc(name, desc, kindCounter, "", fn, nil)
}

// GaugeFuncVec registers a gauge with ONE label whose values are read at
// export time: fn returns a value per label value, and each becomes a data
// point. For quantities owned by another package that are naturally per-signal
// or per-instance (the disk buffer's per-signal backlog, say) and would
// otherwise need one differently-NAMED metric each.
func (r *Registry) GaugeFuncVec(name, desc, labelName string, fn func() map[string]float64) {
	r.addFunc(name, desc, kindGauge, labelName, nil, fn)
}

// CounterFuncVec registers a MONOTONIC counter with ONE label whose values are
// read at export time: fn returns a running total per label value, and each
// becomes its own cumulative data point.
//
// GaugeFuncVec's shape with a counter's semantics. A since-start total carried
// by a gauge does not mark process restarts, so rate()/increase() over it
// silently swallow a restart's step down to zero — and a quantity that only
// ever grows is a counter whatever it is registered as. Each label value keeps
// its own delta bookkeeping (gaugeFunc.lastVec), for the reason CounterFunc
// keeps one: the series ACCUMULATES what is observed into it.
func (r *Registry) CounterFuncVec(name, desc, labelName string, fn func() map[string]float64) {
	r.addFunc(name, desc, kindCounter, labelName, nil, fn)
}

// HistogramVec registers a labeled histogram (nil buckets = the default
// latency buckets, matching prometheus.DefBuckets).
func (r *Registry) HistogramVec(name, desc string, buckets []float64, labelNames ...string) *RegHistogramVec {
	// The third and last door an "le" can reach a histogram's identity through
	// (the others are a logMetrics rule, refused by rejectHistogramLe, and a
	// script's label map, refused by EmitDirect). A histogram keyed on "le"
	// splits its distribution into one sample per value, each rendering a full
	// bucket set, and downstream it collides with the bucket label a Prometheus
	// consumer generates from the histogram's own buckets.
	//
	// A panic rather than an error because this door is CODE: label names here
	// are compile-time literals, every binary builds its metrics during startup,
	// and obs.go's registrations run in every test binary — so this cannot reach
	// production without failing the build first. That is the loud, immediate
	// report a programmer error deserves, and it is a construction-time
	// assertion, not a runtime one (nothing recovers it; see CLAUDE.md).
	for _, k := range labelNames {
		if k == leLabel {
			panic("metrics: histogram " + name + " declares a label named " + leLabel +
				": it is the bucket-bound label generated from the histogram's own buckets")
		}
	}
	return &RegHistogramVec{vec[RegHistogram]{
		s: r.add(name, desc, kindHistogram, buckets, false), keys: labelNames,
		wrap: func(b bound) *RegHistogram { return &RegHistogram{b} },
	}}
}

// bound is a series observed with a fixed (possibly empty) label set. The
// label accumulators are precomputed at construction so a hot-path Inc does
// not rehash the label set on every call.
type bound struct {
	s    *series
	lbls labels
	base xxh3.Uint128
	hash xxh3.Uint128 // mixHash(base), precomputed — bumps skip the avalanche
}

func newBound(s *series, lbls labels) bound {
	base := lbls.hashAccum()
	return bound{s: s, lbls: lbls, base: base, hash: mixHash(base)}
}

var emptyResource = pcommon.NewMap()

func (b bound) observe(v float64) {
	b.s.observePreHashed(b.lbls, b.hash, v, emptyResource)
}

// materialize creates this label set's series at zero (see series.materialize).
func (b bound) materialize() {
	b.s.materialize(b.lbls, b.hash)
}

// Value returns the current sum across the bound label set's samples (for
// tests and debugging).
func (b bound) Value() float64 {
	want := b.lbls.hash()
	b.s.mu.Lock()
	defer b.s.mu.Unlock()
	var total float64
	for samp := range b.s.all() {
		if lb, err := parseLabels(samp.labels); err == nil && lb.hash() == want {
			total += samp.value
		}
	}
	return total
}

// RegCounter is a monotonic counter.
type RegCounter struct{ bound }

// Inc adds one.
func (c *RegCounter) Inc() { c.observe(1) }

// Add adds v (must be >= 0).
func (c *RegCounter) Add(v float64) { c.observe(v) }

// RegGauge is a set-latest gauge.
type RegGauge struct{ bound }

// Set records the current value.
func (g *RegGauge) Set(v float64) { g.observe(v) }

// vec resolves label values to bound wrappers, caching the WRAPPER per value
// tuple so a repeated WithLabelValues on the hot path (e.g. a per-log-record
// counter) neither rebuilds the label set nor allocates the returned pointer.
//
// BINDING A LABEL SET CREATES ITS SERIES, at zero (series.materialize). Callers
// bind up front precisely to say "these outcomes exist in this process" —
// tailbuffer resolves one pair per configured tail-sampling policy from
// Evaluator.Names(), logenrich one per enrich format — and until this, that
// statement had no effect on the wire: the series still appeared only when the
// outcome first occurred, so "policy X kept nothing" read exactly like "policy
// X is not configured", which is the ambiguity those call sites (and the metric
// help texts) claim to remove. The materialization is per value tuple and
// happens on the cold cache-miss path, so a hot-path bump is unchanged.
//
// BINDING is the right moment for it and REGISTRATION is not, which is why
// Registry.Counter/Gauge keep publishing nothing until they are used: obs
// registers every metric of both binaries unconditionally at package init, so
// materializing there would have the metadata service publish a zero for every
// ingest, tailer and tail-sampling counter it does not have a code path for —
// "this never happened" asserted about a feature that is not in this process.
// A vec's label values are bound by the feature's own constructor, so they are
// a statement about what THIS process is configured to do.
type vec[W any] struct {
	s    *series
	keys []string
	wrap func(bound) *W

	mu    sync.Mutex
	cache map[string]*W
}

// vecKey builds a collision-proof cache key: values are length-prefixed, so a
// value containing the old separator byte cannot alias another tuple (e.g.
// ("x\x00y","z") vs ("x","y\x00z")). The single-value case stays alloc-free.
func vecKey(keys, vals []string) string {
	if len(vals) == 1 && len(keys) == 1 {
		// Only when the vec itself is single-label: a 1-value call against a
		// multi-label vec must not alias a netstring-encoded tuple.
		return vals[0]
	}
	n := 0
	for _, v := range vals {
		n += len(v) + 4 // value + ':' + up to a few length digits
	}
	var sb strings.Builder
	sb.Grow(n) // one allocation instead of the builder's growth reallocs
	for _, v := range vals {
		sb.WriteString(strconv.Itoa(len(v)))
		sb.WriteByte(':')
		sb.WriteString(v)
	}
	return sb.String()
}

func (v *vec[W]) with(vals []string) *W {
	key := vecKey(v.keys, vals)
	v.mu.Lock()
	w, ok := v.cache[key]
	if !ok {
		lbls := make(labels, 0, len(v.keys))
		for i, k := range v.keys {
			if i < len(vals) {
				lbls = lbls.set(k, vals[i])
			}
		}
		b := newBound(v.s, lbls)
		b.materialize()
		w = v.wrap(b)
		if v.cache == nil {
			v.cache = make(map[string]*W)
		}
		v.cache[key] = w
	}
	v.mu.Unlock()
	return w
}

// RegCounterVec is a labeled monotonic counter.
type RegCounterVec struct{ vec[RegCounter] }

// WithLabelValues binds label values (order matches the registered names).
func (v *RegCounterVec) WithLabelValues(vals ...string) *RegCounter {
	return v.with(vals)
}

// RegHistogramVec is a labeled histogram.
type RegHistogramVec struct{ vec[RegHistogram] }

// WithLabelValues binds label values.
func (v *RegHistogramVec) WithLabelValues(vals ...string) *RegHistogram {
	return v.with(vals)
}

// RegHistogram observes into fixed buckets.
type RegHistogram struct{ bound }

// Observe records one value.
func (h *RegHistogram) Observe(v float64) { h.observe(v) }

// counterDelta is the value to PUSH for a counter func whose fn returns a
// running total: the series ACCUMULATES observations (samp.value += v), so
// each export must push only the growth since the last one — pushing the
// total re-added the whole count every export, inflating a one-time burst
// into a permanent per-interval rate. A value that SHRANK means the
// underlying counter reset (a foreign atomic was zeroed): the new total is
// all fresh growth. One rule for the scalar and the per-label-value forms,
// which used to spell it twice.
func counterDelta(prev, v float64) float64 {
	if d := v - prev; d >= 0 {
		return d
	}
	return v
}

// seriesSnap pairs a series with its rendered snapshot so a failed export can
// give consumed zero-baseline flags back (rearmInitial).
type seriesSnap struct {
	s       *series
	samples []sample
}

// Export renders every registered series into one ResourceMetrics carrying
// the given resource attributes and sends it. The payload is fresh pdata per
// call and is never retained or re-sent on failure (the next interval renders
// again), which is what licenses the agent's transform.Handoff mark at the
// Run/FinalExport call sites in cmd/kubescrape-agent.
func (r *Registry) Export(ctx context.Context, exp Exporter, res pcommon.Resource) error {
	r.mu.Lock()
	series := append([]*series(nil), r.series...)
	funcs := append([]*gaugeFunc(nil), r.funcs...)
	r.mu.Unlock()

	for _, gf := range funcs {
		gf.mu.Lock()
		if gf.fnVec != nil {
			counter := gf.s.kind == kindCounter
			for lv, v := range gf.fnVec() {
				obsV := v
				if counter {
					// Per label value, the same delta bookkeeping CounterFunc
					// does (see gaugeFunc.last and counterDelta).
					obsV = counterDelta(gf.lastVec[lv], v)
					if gf.lastVec == nil {
						gf.lastVec = map[string]float64{}
					}
					gf.lastVec[lv] = v
				}
				gf.s.observe(labels{}.set(gf.labelName, lv), obsV, xxh3.Uint128{}, emptyResource, nil)
			}
			gf.mu.Unlock()
			continue
		}
		v := gf.fn()
		if gf.s.kind == kindCounter {
			// Push the delta since the last export (see gaugeFunc.last and
			// counterDelta).
			d := counterDelta(gf.last, v)
			gf.last = v
			gf.s.observe(nil, d, xxh3.Uint128{}, emptyResource, nil)
			gf.mu.Unlock()
			continue
		}
		gf.s.observe(nil, v, xxh3.Uint128{}, emptyResource, nil)
		gf.mu.Unlock()
	}

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	res.CopyTo(rm.Resource())
	scope := rm.ScopeMetrics().AppendEmpty()
	setScope(scope, RegistryScopeName)
	ts := time.Now()
	var snaps []seriesSnap
	for _, s := range series {
		samples := s.snapshot()
		if len(samples) == 0 {
			continue
		}
		renderSeries(scope, s, samples, ts)
		snaps = append(snaps, seriesSnap{s, samples})
	}
	if rm.ScopeMetrics().At(0).Metrics().Len() == 0 {
		return nil
	}
	err := exp.ExportMetrics(ctx, md)
	if err != nil {
		// The Registry has no retention, so a failed export must give each
		// counter's never-delivered zero baseline back — see rearmInitial.
		for _, sn := range snaps {
			sn.s.rearmInitial(sn.samples)
		}
	}
	return err
}

// Run exports the registry every interval until ctx is done, then once more.
func (r *Registry) Run(ctx context.Context, exp Exporter, interval time.Duration, res pcommon.Resource, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// Failure state is LOCAL because Run is one goroutine per process: the
	// transition shape (warn once, restate on a schedule, say when it
	// recovered) needs no field on the Registry, which is shared by every
	// package that declares a metric.
	var failures int
	var failedAt time.Time
	var reWarn logdedupe.Throttle
	for {
		select {
		case <-ctx.Done():
			// WithoutCancel, not Background: the shutdown export needs a live
			// deadline of its own, but it must keep whatever the caller put on
			// the context (otlpexport's ownership marker rides there, and a
			// bare Background would silently strip it).
			fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), FinalExportTimeout)
			r.FinalExport(fctx, exp, res, log)
			cancel()
			return
		case <-ticker.C:
			err := r.Export(ctx, exp, res)
			switch {
			case err != nil:
				failures++
				if failures == 1 {
					failedAt = time.Now()
					reWarn.Allow(reWarnInterval) // claim the slot this Warn occupies
				}
				// This is the process's OWN telemetry, so the failure cannot be
				// counted: the counter that would record it is in the payload
				// that is not arriving. The line is the only signal there is,
				// which is why it restates itself rather than going quiet after
				// the first — and why the repeats between restatements are Debug
				// rather than nothing.
				if failures == 1 || reWarn.Allow(reWarnInterval) {
					log.Warn("exporting self-metrics failed; this process's own telemetry is not reaching the collector",
						"error", err, "attempts", failures, "since", time.Since(failedAt).Round(time.Second))
				} else {
					log.Debug("exporting self-metrics failed", "error", err, "attempts", failures)
				}
			case failures > 0:
				log.Info("exporting self-metrics succeeded again",
					"attempts", failures, "since", time.Since(failedAt).Round(time.Second))
				failures = 0
			}
		}
	}
}

// FinalExportTimeout is the budget Run's own shutdown branch gives the last
// export. Callers that pass their own context choose their own; this is only
// for the branch that has nothing but an already-cancelled ctx to work from.
const FinalExportTimeout = 10 * time.Second

// FinalExport pushes one last snapshot — used by Run's shutdown branch and by
// both mains AFTER wg.Wait, so counters bumped by the final flushes (last
// batches, shutdown drops) that raced Run's own shutdown export are not lost.
//
// ctx is the CALLER's shutdown budget. It used to manufacture its own
// context.Background() with a hard-coded 10s, which meant neither main could
// fit this export inside the termination grace it was already budgeting every
// other final export against — and a Background context also drops whatever the
// caller put on it (otlpexport's ownership marker rides on the context).
func (r *Registry) FinalExport(ctx context.Context, exp Exporter, res pcommon.Resource, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	if err := r.Export(ctx, exp, res); err != nil {
		log.Warn("final self-metrics export failed", "error", err)
	}
}
