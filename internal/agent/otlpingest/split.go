package otlpingest

import (
	"context"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

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

// route moves every data point of m into the output metric for its ID. The
// per-type loops copy directly (no per-point closures — this is the ingest
// hot path).
func (g *metricGrouper) route(sm pmetric.ScopeMetrics, scopeIdx int, m pmetric.Metric, metricIdx int) {
	if metricPointCount(m) == 0 {
		// No data points to route (an empty metric, or MetricTypeEmpty): the
		// per-type loops below would create no shell and the descriptor would be
		// dropped. Resource mode returns the metric in place, so preserve it here
		// too under the resource-level ID.
		g.metric(sm, scopeIdx, m, metricIdx, g.resToken)
		return
	}
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			dst := g.metricFor(sm, scopeIdx, m, metricIdx, dp.Attributes())
			dp.CopyTo(dst.Gauge().DataPoints().AppendEmpty())
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			dst := g.metricFor(sm, scopeIdx, m, metricIdx, dp.Attributes())
			dp.CopyTo(dst.Sum().DataPoints().AppendEmpty())
		}
	case pmetric.MetricTypeHistogram:
		dps := m.Histogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			dst := g.metricFor(sm, scopeIdx, m, metricIdx, dp.Attributes())
			dp.CopyTo(dst.Histogram().DataPoints().AppendEmpty())
		}
	case pmetric.MetricTypeExponentialHistogram:
		dps := m.ExponentialHistogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			dst := g.metricFor(sm, scopeIdx, m, metricIdx, dp.Attributes())
			dp.CopyTo(dst.ExponentialHistogram().DataPoints().AppendEmpty())
		}
	case pmetric.MetricTypeSummary:
		dps := m.Summary().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			dst := g.metricFor(sm, scopeIdx, m, metricIdx, dp.Attributes())
			dp.CopyTo(dst.Summary().DataPoints().AppendEmpty())
		}
	}
}

// metricPointCount is the number of data points on m, across its type (0 for
// MetricTypeEmpty).
func metricPointCount(m pmetric.Metric) int {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		return m.Gauge().DataPoints().Len()
	case pmetric.MetricTypeSum:
		return m.Sum().DataPoints().Len()
	case pmetric.MetricTypeHistogram:
		return m.Histogram().DataPoints().Len()
	case pmetric.MetricTypeExponentialHistogram:
		return m.ExponentialHistogram().DataPoints().Len()
	case pmetric.MetricTypeSummary:
		return m.Summary().DataPoints().Len()
	}
	return 0
}

// metricFor resolves one data point's ID (falling back to the resource-level
// one) and returns its output metric.
func (g *metricGrouper) metricFor(sm pmetric.ScopeMetrics, scopeIdx int, m pmetric.Metric, metricIdx int, dpAttrs pcommon.Map) pmetric.Metric {
	token, _ := g.enricher.findID(dpAttrs)
	if token == "" {
		token = g.resToken
	}
	return g.metric(sm, scopeIdx, m, metricIdx, token)
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
		if id != g.resToken {
			// This group is keyed by a point-level ID that differs from the
			// resource's own: the copied resource describes a DIFFERENT object.
			// Its ID attributes would mislabel (and mis-enrich downstream) every
			// point in the group — and so would the rest of the sender's identity
			// (k8s.pod.name, service.name, …), which names the EXPORTER, not the
			// object. The sender is authoritative about itself, not about others,
			// so the resolved identity OVERWRITES rather than merges here. Sender
			// attributes the builder does not supply (cluster name, SDK attrs,
			// custom) are untouched.
			g.stripIDAttrs(rm.Resource().Attributes())
			built := g.enricher.builtAttrs(g.ctx, g.enrichCache, id)
			overwriteAttrs(built, rm.Resource().Attributes())
			g.rmByID[id] = rm
			return rm
		}
		mergeAttrs(g.enricher.builtAttrs(g.ctx, g.enrichCache, id), rm.Resource().Attributes())
	} else if pod := g.enricher.peerPod(g.ctx); pod != nil {
		// No ID anywhere for these points: the opt-in peer-IP fallback still
		// attributes them to the pushing pod.
		obs.Ingested.WithLabelValues("peer_ip").Inc()
		g.enricher.build(pod, nil, rm.Resource().Attributes())
	} else {
		// Nothing identified these points. They are still forwarded (under the
		// unenriched source resource), but the outcome must show up in the
		// counters exactly as the resource-mode path's does.
		obs.Ingested.WithLabelValues("unresolved").Inc()
	}
	g.rmByID[id] = rm
	return rm
}

// stripIDAttrs removes the configured container-ID/pod-UID attribute keys.
func (g *metricGrouper) stripIDAttrs(a pcommon.Map) {
	for _, k := range g.enricher.containerIDKeys {
		a.Remove(k)
	}
	for _, k := range g.enricher.podUIDKeys {
		a.Remove(k)
	}
}
