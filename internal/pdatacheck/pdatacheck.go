// Package pdatacheck holds shape assertions about OTLP payloads that more
// than one package's tests need.
//
// It exists for ONE invariant so far: a metric with no data points must never
// be exported. Such a metric is not illegal OTLP, which is precisely the
// problem — nothing downstream rejects it, so it costs its name, description,
// unit and framing on every export forever while carrying no measurement, and
// no counter anywhere moves. The producers in this repo hold the invariant by
// CONSTRUCTION (every one of them creates a metric's shell and appends its
// first data point in the same function, so a shell cannot outlive its
// points), and sender-supplied payloads are normalised at first receipt by
// otlpingest. This package is what keeps the construction half honest as
// producers are added.
//
// It is a plain package rather than a _test file so every producer's tests can
// import it; nothing in production calls it.
package pdatacheck

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// EmptyMetrics returns a path-qualified name for every metric in md carrying
// zero data points, in payload order. An untyped metric (pmetric.
// MetricTypeEmpty) counts: it has no data points by definition.
//
// The returned slice is empty when the payload is clean, so a test reads
// `if names := pdatacheck.EmptyMetrics(md); len(names) > 0 { t.Fatalf(...) }`.
func EmptyMetrics(md pmetric.Metrics) []string {
	var out []string
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if MetricPointCount(m) > 0 {
					continue
				}
				out = append(out, fmt.Sprintf("rm[%d]/sm[%d]/metric[%d] %q (type %s)",
					i, j, k, m.Name(), m.Type()))
			}
		}
	}
	return out
}

// MetricPointCount is the number of data points on m across every metric
// type, and 0 for an untyped one.
func MetricPointCount(m pmetric.Metric) int {
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
	default:
		return 0
	}
}
