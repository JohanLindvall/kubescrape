package metrics

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
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
	funcs  []gaugeFunc
}

// registryExpiration keeps snapshot's idle handling permanently inactive —
// a self-metric is cumulative for the process lifetime.
const registryExpiration = 200 * 365 * 24 * time.Hour

type gaugeFunc struct {
	s  *series
	fn func() float64
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) add(name, desc string, kind seriesKind, action gaugeAction, buckets []float64) *series {
	s := newSeries(seriesSpec{
		name: name, desc: desc, kind: kind, action: action,
		expiration: registryExpiration, buckets: buckets,
	})
	r.mu.Lock()
	r.series = append(r.series, s)
	r.mu.Unlock()
	return s
}

// Counter registers a monotonic counter.
func (r *Registry) Counter(name, desc string) *RegCounter {
	return &RegCounter{newBound(r.add(name, desc, kindCounter, actionSet, nil), nil)}
}

// CounterVec registers a labeled monotonic counter.
func (r *Registry) CounterVec(name, desc string, labelNames ...string) *RegCounterVec {
	return &RegCounterVec{vec[RegCounter]{
		s: r.add(name, desc, kindCounter, actionSet, nil), keys: labelNames,
		wrap: func(b bound) *RegCounter { return &RegCounter{b} },
	}}
}

// Gauge registers a set-latest gauge.
func (r *Registry) Gauge(name, desc string) *RegGauge {
	return &RegGauge{newBound(r.add(name, desc, kindGauge, actionSet, nil), nil)}
}

// GaugeVec registers a labeled gauge.
func (r *Registry) GaugeVec(name, desc string, labelNames ...string) *RegGaugeVec {
	return &RegGaugeVec{vec[RegGauge]{
		s: r.add(name, desc, kindGauge, actionSet, nil), keys: labelNames,
		wrap: func(b bound) *RegGauge { return &RegGauge{b} },
	}}
}

// GaugeFunc registers a gauge evaluated at export time.
func (r *Registry) GaugeFunc(name, desc string, fn func() float64) {
	s := r.add(name, desc, kindGauge, actionSet, nil)
	r.mu.Lock()
	r.funcs = append(r.funcs, gaugeFunc{s: s, fn: fn})
	r.mu.Unlock()
}

// HistogramVec registers a labeled histogram (nil buckets = the default
// latency buckets, matching prometheus.DefBuckets).
func (r *Registry) HistogramVec(name, desc string, buckets []float64, labelNames ...string) *RegHistogramVec {
	return &RegHistogramVec{vec[RegHistogram]{
		s: r.add(name, desc, kindHistogram, actionSet, buckets), keys: labelNames,
		wrap: func(b bound) *RegHistogram { return &RegHistogram{b} },
	}}
}

// bound is a series observed with a fixed (possibly empty) label set. The
// label accumulators are precomputed at construction so a hot-path Inc does
// not rehash the label set on every call.
type bound struct {
	s           *series
	lbls        labels
	base, check uint64
	hash        uint64 // mixHash(base), precomputed — bumps skip the avalanche
}

func newBound(s *series, lbls labels) bound {
	base, check := lbls.accums()
	return bound{s: s, lbls: lbls, base: base, check: check, hash: mixHash(base)}
}

var emptyResource = pcommon.NewMap()

func (b bound) observe(v float64) {
	b.s.observePreHashed(b.lbls, b.hash, b.check, v, emptyResource)
}

// Value returns the current sum across the bound label set's samples (for
// tests and debugging).
func (b bound) Value() float64 {
	want := b.lbls.hash()
	b.s.mu.Lock()
	defer b.s.mu.Unlock()
	var total float64
	for _, samp := range b.s.db {
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
	var sb strings.Builder
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
		w = v.wrap(newBound(v.s, lbls))
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

// RegGaugeVec is a labeled gauge.
type RegGaugeVec struct{ vec[RegGauge] }

// WithLabelValues binds label values.
func (v *RegGaugeVec) WithLabelValues(vals ...string) *RegGauge {
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

// Export renders every registered series into one ResourceMetrics carrying
// the given resource attributes and sends it.
func (r *Registry) Export(ctx context.Context, exp Exporter, res pcommon.Resource) error {
	r.mu.Lock()
	series := append([]*series(nil), r.series...)
	funcs := append([]gaugeFunc(nil), r.funcs...)
	r.mu.Unlock()

	for _, gf := range funcs {
		gf.s.observe(nil, gf.fn(), resKey{}, emptyResource, nil)
	}

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	res.CopyTo(rm.Resource())
	scope := rm.ScopeMetrics().AppendEmpty()
	scope.Scope().SetName("github.com/JohanLindvall/kubescrape/internal/obs")
	ts := time.Now()
	for _, s := range series {
		samples := s.snapshot()
		if len(samples) == 0 {
			continue
		}
		renderSeries(scope, s, samples, ts)
	}
	if rm.ScopeMetrics().At(0).Metrics().Len() == 0 {
		return nil
	}
	return exp.ExportMetrics(ctx, md)
}

// Run exports the registry every interval until ctx is done, then once more.
func (r *Registry) Run(ctx context.Context, exp Exporter, interval time.Duration, res pcommon.Resource, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := r.Export(fctx, exp, res); err != nil {
				log.Warn("final self-metrics export failed", "error", err)
			}
			cancel()
			return
		case <-ticker.C:
			if err := r.Export(ctx, exp, res); err != nil {
				log.Warn("exporting self-metrics failed", "error", err)
			}
		}
	}
}
