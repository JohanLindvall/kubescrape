package otlpingest

// Empty-metric pruning at first receipt.
//
// A metric carrying zero data points is legal OTLP, and that is exactly what
// makes it worth refusing here: nothing downstream rejects it. It rides every
// hop of the chain — enrichment, the split regrouping, the transform scripts,
// the router, the disk buffer, the wire — costing its name, description, unit
// and framing bytes against the send cap that exists to keep real data under a
// collector's receive limit, and delivering no measurement at the end of it.
// A backend stores nothing. No counter anywhere moves. It is pure, invisible
// waste, repeated on every push for as long as the sender keeps making it.
//
// kubescrape's OWN producers cannot emit one: every metric-building path in
// this repo creates a metric's shell and appends its first data point in the
// same function, so a shell cannot outlive its points (internal/pdatacheck is
// the assertion that keeps that true as producers are added). The only door an
// empty metric can come through is a sender's, which is why the prune lives
// here — at the application-facing receipt seam, beside reserved-key stripping
// and peer attribution, on the same both-transports path. The trace tier's
// INTERNAL receiver is deliberately not on it: what arrives there is
// kubescrape-to-kubescrape and was pruned when an application pushed it.
//
// It runs AFTER the ingest: admit hook (an operator's policy decides on the
// payload the sender actually sent) and BEFORE the all-rejected check, so a
// push consisting only of empty metrics is acked without a send rather than
// forwarded as an empty envelope.
//
// The removal is COUNTED and warned rather than silent: a sender emitting
// empty metrics has a bug in its instrumentation, and the whole reason this
// prune is needed is that nothing else would ever tell anyone.

import (
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// emptyMetricWarnEvery is the re-warn cadence. The condition is a STATE — an
// instrumentation bug ships the same empty metric on every push — and the
// useful information is one line.
const emptyMetricWarnEvery = time.Minute

// dropEmptyMetrics removes every metric in md that carries no data points,
// then the scopes and resources left with nothing in them, and returns the
// number of METRICS removed. Scope and resource pruning is a consequence
// rather than a second policy: an envelope with no metrics under it is the
// same zero-information framing, including one that arrived that way.
func dropEmptyMetrics(md pmetric.Metrics) int {
	dropped := 0
	// The three predicates are built ONCE rather than per scope: this runs on
	// every metrics push, and a closure minted per ScopeMetrics would make the
	// prune's own cost scale with the payload's structure — which is the
	// sender's choice, not ours.
	dropMetric := func(m pmetric.Metric) bool {
		if metricPointCount(m) > 0 {
			return false
		}
		dropped++
		return true
	}
	dropScope := func(sm pmetric.ScopeMetrics) bool { return sm.Metrics().Len() == 0 }
	dropResource := func(rm pmetric.ResourceMetrics) bool { return rm.ScopeMetrics().Len() == 0 }

	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sms.At(j).Metrics().RemoveIf(dropMetric)
		}
		sms.RemoveIf(dropScope)
	}
	rms.RemoveIf(dropResource)
	return dropped
}

// pruneEmptyMetrics is dropEmptyMetrics plus its accounting: the receipt-seam
// entry point both transports call.
func (s *Server) pruneEmptyMetrics(md pmetric.Metrics) {
	n := dropEmptyMetrics(md)
	if n == 0 {
		return
	}
	obs.IngestEmptyMetricsDropped.Add(float64(n))
	if s.emptyMetricWarns.Allow(emptyMetricWarnEvery) {
		s.log.Warn("ingest: a sender pushed metrics carrying no data points; dropped at receipt so they cost nothing downstream. Nothing rejects an empty metric, so this is the only report of it — the sender's instrumentation is creating metric descriptors it never records into",
			"dropped", n)
	}
}
