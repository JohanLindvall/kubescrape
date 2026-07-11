package otlpingest

import (
	"context"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// splitAndEnrich regroups every data point by the object ID on its own
// attributes, producing one ResourceMetrics per (input resource, object).
// Points without a resolvable ID stay under a copy of their original
// resource, unenriched. Metric identity (name/type/unit) and scope are
// preserved.
func (e *Enricher) splitAndEnrich(ctx context.Context, md pmetric.Metrics) pmetric.Metrics {
	out := pmetric.NewMetrics()
	// Enrichment is looked up once per distinct ID across the whole batch.
	enrichCache := map[string]pcommon.Map{}

	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		g := &metricGrouper{
			enricher:    e,
			ctx:         ctx,
			srcResource: rm.Resource(),
			srcSchema:   rm.SchemaUrl(),
			enrichCache: enrichCache,
			out:         out,
			rmByID:      map[string]pmetric.ResourceMetrics{},
			smByID:      map[idScope]pmetric.ScopeMetrics{},
			metByID:     map[idMetric]pmetric.Metric{},
		}
		// Points without their own ID fall back to the resource-level one, so
		// a mixed batch (auto mode) does not lose enrichment for resources
		// that carried the ID where it belongs.
		g.resToken, _ = e.findID(rm.Resource().Attributes())
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			ms := sm.Metrics()
			for k := 0; k < ms.Len(); k++ {
				g.route(sm, j, ms.At(k), k)
			}
		}
	}
	return out
}

type idScope struct {
	id    string
	scope int
}

type idMetric struct {
	id     string
	scope  int
	metric int
}

// metricGrouper accumulates one input ResourceMetrics' points into per-ID
// output resources.
type metricGrouper struct {
	enricher    *Enricher
	ctx         context.Context
	srcResource pcommon.Resource
	srcSchema   string
	resToken    string // resource-level ID, the fallback for ID-less points
	enrichCache map[string]pcommon.Map
	out         pmetric.Metrics
	rmByID      map[string]pmetric.ResourceMetrics
	smByID      map[idScope]pmetric.ScopeMetrics
	metByID     map[idMetric]pmetric.Metric
}

// route moves every data point of m into the output metric for its ID.
func (g *metricGrouper) route(sm pmetric.ScopeMetrics, scopeIdx int, m pmetric.Metric, metricIdx int) {
	move := func(dpAttrs pcommon.Map, copyTo func(dst pmetric.Metric)) {
		token, _ := g.enricher.findID(dpAttrs)
		if token == "" {
			token = g.resToken
		}
		dst := g.metric(sm, scopeIdx, m, metricIdx, token)
		copyTo(dst)
	}
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			move(dp.Attributes(), func(dst pmetric.Metric) { dp.CopyTo(dst.Gauge().DataPoints().AppendEmpty()) })
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			move(dp.Attributes(), func(dst pmetric.Metric) { dp.CopyTo(dst.Sum().DataPoints().AppendEmpty()) })
		}
	case pmetric.MetricTypeHistogram:
		dps := m.Histogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			move(dp.Attributes(), func(dst pmetric.Metric) { dp.CopyTo(dst.Histogram().DataPoints().AppendEmpty()) })
		}
	case pmetric.MetricTypeExponentialHistogram:
		dps := m.ExponentialHistogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			move(dp.Attributes(), func(dst pmetric.Metric) { dp.CopyTo(dst.ExponentialHistogram().DataPoints().AppendEmpty()) })
		}
	case pmetric.MetricTypeSummary:
		dps := m.Summary().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			move(dp.Attributes(), func(dst pmetric.Metric) { dp.CopyTo(dst.Summary().DataPoints().AppendEmpty()) })
		}
	}
}

// metric returns the output metric for the given ID, creating the resource,
// scope and metric shells (and enriching the resource) on first use.
func (g *metricGrouper) metric(sm pmetric.ScopeMetrics, scopeIdx int, m pmetric.Metric, metricIdx int, id string) pmetric.Metric {
	mk := idMetric{id: id, scope: scopeIdx, metric: metricIdx}
	if dst, ok := g.metByID[mk]; ok {
		return dst
	}
	scope := g.scope(sm, scopeIdx, id)
	dst := scope.Metrics().AppendEmpty()
	dst.SetName(m.Name())
	dst.SetDescription(m.Description())
	dst.SetUnit(m.Unit())
	m.Metadata().CopyTo(dst.Metadata())
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dst.SetEmptyGauge()
	case pmetric.MetricTypeSum:
		s := dst.SetEmptySum()
		s.SetAggregationTemporality(m.Sum().AggregationTemporality())
		s.SetIsMonotonic(m.Sum().IsMonotonic())
	case pmetric.MetricTypeHistogram:
		dst.SetEmptyHistogram().SetAggregationTemporality(m.Histogram().AggregationTemporality())
	case pmetric.MetricTypeExponentialHistogram:
		dst.SetEmptyExponentialHistogram().SetAggregationTemporality(m.ExponentialHistogram().AggregationTemporality())
	case pmetric.MetricTypeSummary:
		dst.SetEmptySummary()
	}
	g.metByID[mk] = dst
	return dst
}

func (g *metricGrouper) scope(sm pmetric.ScopeMetrics, scopeIdx int, id string) pmetric.ScopeMetrics {
	sk := idScope{id: id, scope: scopeIdx}
	if dst, ok := g.smByID[sk]; ok {
		return dst
	}
	rm := g.resource(id)
	dst := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().CopyTo(dst.Scope())
	dst.SetSchemaUrl(sm.SchemaUrl())
	g.smByID[sk] = dst
	return dst
}

func (g *metricGrouper) resource(id string) pmetric.ResourceMetrics {
	if rm, ok := g.rmByID[id]; ok {
		return rm
	}
	rm := g.out.ResourceMetrics().AppendEmpty()
	g.srcResource.CopyTo(rm.Resource())
	rm.SetSchemaUrl(g.srcSchema)
	if id != "" {
		g.mergeEnrichment(id, rm.Resource().Attributes())
	}
	g.rmByID[id] = rm
	return rm
}

// mergeEnrichment adds the k8s attributes for id to dst, never overwriting.
func (g *metricGrouper) mergeEnrichment(id string, dst pcommon.Map) {
	built, ok := g.enrichCache[id]
	if !ok {
		built = pcommon.NewMap()
		if pod, container := g.enricher.lookupByID(g.ctx, id); pod != nil {
			r := pcommon.NewResource()
			actx := attrs.Context{Pod: pod, Container: container}
			if g.enricher.cfg.NodeInfo != nil {
				actx.Node = g.enricher.cfg.NodeInfo()
			}
			g.enricher.cfg.Attrs.Build(r, actx)
			r.Attributes().CopyTo(built)
			obs.Ingested.WithLabelValues("enriched").Inc()
		} else {
			obs.Ingested.WithLabelValues("unresolved").Inc()
		}
		g.enrichCache[id] = built
	}
	built.Range(func(k string, v pcommon.Value) bool {
		if _, exists := dst.Get(k); !exists {
			v.CopyTo(dst.PutEmpty(k))
		}
		return true
	})
}
