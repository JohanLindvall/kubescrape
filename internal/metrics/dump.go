package metrics

// A non-mutating, point-in-time read of a Registry, for serving the same
// metrics over a Prometheus scrape (obs's /metrics bridge) that the OTLP push
// path exports. Dump deliberately touches NONE of the interval state the push
// path depends on — no sealing, no initial-flag clearing, no counter-func
// delta bookkeeping — so a scrape can never steal samples from the exporter
// and the two readers may coexist.

import (
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/clip"
)

// RegistrySeries is one registered metric's current state.
type RegistrySeries struct {
	Name string
	Desc string
	// Kind is CounterType, GaugeType or HistogramType (the only kinds the
	// Registry constructors mint).
	Kind string
	// Bounds are a histogram's bucket bounds without the +Inf bucket; nil for
	// other kinds.
	Bounds []float64
	Points []RegistryPoint
}

// RegistryPoint is one label combination's current value.
type RegistryPoint struct {
	// Labels are the (key, value) pairs, key-sorted.
	Labels [][2]string
	// Value is the counter total or gauge value.
	Value float64
	// Count/Sum/Buckets are histogram-only: total observations, their sum, and
	// the CUMULATIVE count per Bounds entry (the +Inf bucket is Count).
	Count   uint64
	Sum     float64
	Buckets []uint64
}

// Dump reads every registered series and evaluated func metric. It is safe to
// call concurrently with observations and with Export, and mutates nothing:
// calling it any number of times leaves the next OTLP export byte-identical.
func (r *Registry) Dump() []RegistrySeries {
	r.mu.Lock()
	sers := append([]*series(nil), r.series...)
	funcs := append([]*gaugeFunc(nil), r.funcs...)
	r.mu.Unlock()

	// Func-backed series report the LIVE fn() value below and skip their db:
	// a counter func's db accumulates Export's deltas, so reading both would
	// double-report, and the db lags the atomic it mirrors anyway.
	funcBacked := make(map[*series]bool, len(funcs))
	for _, gf := range funcs {
		funcBacked[gf.s] = true
	}

	out := make([]RegistrySeries, 0, len(sers))
	for _, s := range sers {
		if funcBacked[s] {
			continue
		}
		if d, ok := r.dumpSeries(s); ok {
			out = append(out, d)
		}
	}
	// Several registrations of ONE name share one series (Registry.byName) and
	// each keeps its own gaugeFunc, so funcs may name the same series more than
	// once — the advertised shape for the per-instance Register* hooks (two
	// DynamicMetricSets in one process each publishing the drop family). One
	// entry per FUNC would render that name twice, which the Prometheus bridge
	// cannot express at all: registryCollector emits two const metrics with an
	// identical name and label set, Gather's duplicate check fails, and promhttp
	// answers 500 to EVERY scrape — losing the go_*/process_* collectors that
	// share the handler. The dedupe therefore has to hold on the READ side too,
	// not only in add().
	at := make(map[*series]int, len(funcs))
	for _, gf := range funcs {
		d := dumpFunc(gf)
		if i, ok := at[gf.s]; ok {
			mergeFuncPoints(&out[i], d.Points, gf.s.kind == kindCounter)
			continue
		}
		at[gf.s] = len(out)
		out = append(out, d)
	}
	return out
}

// mergeFuncPoints folds one more func's points into the entry already emitted
// for its series, the way Export folds them into the series itself: every func
// observes into the shared series, so a COUNTER accumulates each one's
// contribution (each keeps its own delta bookkeeping, and each fn returns its
// own running total) while a GAUGE is set, i.e. the last registration wins.
// Label sets no earlier func reported become points of their own.
func mergeFuncPoints(d *RegistrySeries, pts []RegistryPoint, counter bool) {
	for _, p := range pts {
		i := indexPoint(d.Points, p.Labels)
		if i < 0 {
			d.Points = append(d.Points, p)
			continue
		}
		if counter {
			d.Points[i].Value += p.Value
		} else {
			d.Points[i].Value = p.Value
		}
	}
	sortPoints(d.Points)
}

// indexPoint returns the position of the point carrying exactly these labels,
// or -1.
func indexPoint(ps []RegistryPoint, lbls [][2]string) int {
	for i := range ps {
		if slices.Equal(ps[i].Labels, lbls) {
			return i
		}
	}
	return -1
}

// kindType maps a series kind to its exported Dump type name; ok is false for
// kinds this reader cannot render (the Registry constructors never mint them).
// The one mapping, shared by dumpSeries and dumpFunc — twin switch statements
// are how one gains a kind the other does not.
func kindType(k seriesKind) (string, bool) {
	switch k {
	case kindCounter:
		return CounterType, true
	case kindGauge:
		return GaugeType, true
	case kindHistogram:
		return HistogramType, true
	default:
		return "", false
	}
}

// dumpFunc evaluates one func metric live. gf.mu serializes fn() with
// Export's own evaluation (fns need not be concurrent-safe); gf.last is
// deliberately NOT read or written — the delta bookkeeping belongs to the
// push path alone.
func dumpFunc(gf *gaugeFunc) RegistrySeries {
	kind, _ := kindType(gf.s.kind) // funcs are only ever counters or gauges
	d := RegistrySeries{Name: gf.s.name, Desc: gf.s.desc, Kind: kind}
	gf.mu.Lock()
	defer gf.mu.Unlock()
	if gf.fnVec != nil {
		for lv, v := range gf.fnVec() {
			d.Points = append(d.Points, RegistryPoint{
				Labels: [][2]string{{gf.labelName, lv}}, Value: v,
			})
		}
		sortPoints(d.Points)
		return d
	}
	d.Points = []RegistryPoint{{Value: gf.fn()}}
	return d
}

// dumpSeries reads one series' samples under its lock. ok is false for kinds
// the Registry cannot mint (defensive; nothing to render them as).
//
// It is a METHOD so an unparseable label string has somewhere to be counted:
// this is the /metrics serving path, and a skipped point vanishes from the
// operator's own telemetry with nothing left behind to say a point was skipped
// at all (see noteLabelParseError).
func (r *Registry) dumpSeries(s *series) (RegistrySeries, bool) {
	kind, ok := kindType(s.kind)
	if !ok {
		return RegistrySeries{}, false
	}
	d := RegistrySeries{Name: s.name, Desc: s.desc, Kind: kind}
	if s.kind == kindHistogram {
		d.Bounds = append([]float64(nil), s.buckets[:len(s.buckets)-1]...)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.kind != kindHistogram {
		for samp := range s.all() {
			lbls, err := parseLabels(samp.labels)
			if err != nil {
				r.noteLabelParseError(s.name, samp.labels, err)
				continue
			}
			d.Points = append(d.Points, RegistryPoint{Labels: pairs(lbls), Value: samp.value})
		}
		sortPoints(d.Points)
		return d, true
	}

	// Histogram: one sample IS one label set's whole distribution. The buckets
	// stay CUMULATIVE here, which is the Dump contract; the OTLP render
	// converts its copy to absolute counts. counts is CLONED — the live array
	// keeps counting after the lock is released — and nothing is mutated (Dump
	// serves a live Registry beside the push path, TestDumpNonMutating).
	for samp := range s.all() {
		lbls, err := parseLabels(samp.labels)
		if err != nil {
			r.noteLabelParseError(s.name, samp.labels, err)
			continue
		}
		d.Points = append(d.Points, RegistryPoint{
			Labels:  pairs(lbls),
			Count:   samp.count,
			Sum:     samp.value,
			Buckets: slices.Clone(samp.counts),
		})
	}
	sortPoints(d.Points)
	return d, true
}

// dumpLabelWarnEvery bounds the label-parse warning. The condition is a STATE —
// a stored string does not become well-formed on its own — and Dump runs once
// per scrape, so an unthrottled line would be one per affected series per
// scrape interval for the life of the process.
const dumpLabelWarnEvery = 10 * time.Minute

// noteLabelParseError counts a point Dump could not render and names it, at
// most once per window.
//
// This is called from INSIDE the series lock, and from the path that serves
// GET /metrics, so it must stay cheap: one atomic add, one atomic CAS, and a
// formatted line only on the rare window where the throttle opens. The
// KEYLESS throttle is deliberate — the counter carries the rate and one
// example is enough to reach the corrupt series, while a keyed table on a
// serving path would be a map write per skipped point.
//
// It cannot be a metric registered here: obs (where every kubescrape_* metric
// is declared) imports this package, so the counter is exported through
// DumpLabelErrors and published there as a CounterFunc.
func (r *Registry) noteLabelParseError(metric, labels string, err error) {
	r.dumpLabelErrors.Add(1)
	if !r.dumpWarn.Allow(dumpLabelWarnEvery) {
		return
	}
	// The label STRING is this process's own — every Registry label set comes
	// from code — so logging it leaks nothing a caller supplied, and it is the
	// only handle on what went wrong. It is cut, because a corrupt string has
	// no length anyone reasoned about.
	slog.Default().Warn("a self-metric data point was skipped: its stored label set could not be parsed back, "+
		"so it is missing from the Prometheus /metrics response this process serves for itself",
		"metric", metric, "labels", truncLabelString(labels), "error", err,
		"skipped", r.dumpLabelErrors.Load(),
		"note", "the OTLP push path reads the same stored string and degrades differently — it emits the point "+
			"with whatever labels parsed rather than dropping it — so the pushed copy of this series is "+
			"mislabelled, not missing. Further reports are suppressed for "+dumpLabelWarnEvery.String())
}

// truncLabelString bounds what a log line carries from a corrupt label string.
func truncLabelString(s string) string {
	const maxLoggedLabelBytes = 256
	return clip.Ellipsis(s, maxLoggedLabelBytes)
}

// pairs converts a key-sorted label set into the exported pair form.
func pairs(l labels) [][2]string {
	if len(l) == 0 {
		return nil
	}
	out := make([][2]string, 0, len(l))
	for _, e := range l {
		out = append(out, [2]string{e.key, e.value})
	}
	return out
}

// sortPoints orders points by their label pairs so a dump (and the scrape
// built from it) is deterministic across calls.
func sortPoints(ps []RegistryPoint) {
	slices.SortFunc(ps, func(a, b RegistryPoint) int {
		n := min(len(a.Labels), len(b.Labels))
		for i := 0; i < n; i++ {
			if c := strings.Compare(a.Labels[i][0], b.Labels[i][0]); c != 0 {
				return c
			}
			if c := strings.Compare(a.Labels[i][1], b.Labels[i][1]); c != 0 {
				return c
			}
		}
		return len(a.Labels) - len(b.Labels)
	})
}
