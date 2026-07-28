package metrics

// A non-mutating, point-in-time read of a Registry, for serving the same
// metrics over a Prometheus scrape (obs's /metrics bridge) that the OTLP push
// path exports. Dump deliberately touches NONE of the interval state the push
// path depends on — no sealing, no initial-flag clearing, no counter-func
// delta bookkeeping — so a scrape can never steal samples from the exporter
// and the two readers may coexist.

import (
	"slices"
	"strings"
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
		if d, ok := dumpSeries(s); ok {
			out = append(out, d)
		}
	}
	for _, gf := range funcs {
		out = append(out, dumpFunc(gf))
	}
	return out
}

// dumpFunc evaluates one func metric live. gf.mu serializes fn() with
// Export's own evaluation (fns need not be concurrent-safe); gf.last is
// deliberately NOT read or written — the delta bookkeeping belongs to the
// push path alone.
func dumpFunc(gf *gaugeFunc) RegistrySeries {
	d := RegistrySeries{Name: gf.s.name, Desc: gf.s.desc, Kind: GaugeType}
	if gf.s.kind == kindCounter {
		d.Kind = CounterType
	}
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
func dumpSeries(s *series) (RegistrySeries, bool) {
	d := RegistrySeries{Name: s.name, Desc: s.desc}
	switch s.kind {
	case kindCounter:
		d.Kind = CounterType
	case kindGauge:
		d.Kind = GaugeType
	case kindHistogram:
		d.Kind = HistogramType
		d.Bounds = append([]float64(nil), s.buckets[:len(s.buckets)-1]...)
	default:
		return RegistrySeries{}, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.kind != kindHistogram {
		for _, samp := range s.db {
			lbls, err := parseLabels(samp.labels)
			if err != nil {
				continue
			}
			d.Points = append(d.Points, RegistryPoint{Labels: pairs(lbls), Value: samp.value})
		}
		sortPoints(d.Points)
		return d, true
	}

	// Histogram: regroup the per-bucket streams by their labels without "le".
	// Each stream's count is already the CUMULATIVE count for its bound (observe
	// records into every bucket the value fits), and the +Inf stream carries the
	// total count and sum — exactly the Prometheus histogram shape.
	type hp struct {
		lbls  labels
		point RegistryPoint
	}
	groups := map[string]*hp{}
	for _, samp := range s.db {
		lbls, err := parseLabels(samp.labels)
		if err != nil {
			continue
		}
		lbls = lbls.without(leLabel)
		key := lbls.String()
		g := groups[key]
		if g == nil {
			g = &hp{lbls: lbls, point: RegistryPoint{Buckets: make([]uint64, len(d.Bounds))}}
			groups[key] = g
		}
		if samp.bucket == len(d.Bounds) { // +Inf: totals
			g.point.Count = samp.count
			g.point.Sum = samp.value
		} else if samp.bucket < len(d.Bounds) {
			g.point.Buckets[samp.bucket] = samp.count
		}
	}
	for _, g := range groups {
		g.point.Labels = pairs(g.lbls)
		d.Points = append(d.Points, g.point)
	}
	sortPoints(d.Points)
	return d, true
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
