package promscrape

// The plain batcher — its lifecycle, and its emission half: how a converted
// point lands in the one resource of a target's payload — and the four
// interfaces the batchers are driven through: sink, which the converter and the
// protobuf front write every point to; expSink, which only the plain and split
// batchers implement; batch, the chunk lifecycle the shared size bound and the
// one export site (session.go) read, which the /stats/summary batcher
// implements too; and chunker, the two together. The point writers and the size
// model all the batchers share are in otlppoint.go.

import (
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// sink receives converted points; implemented by batcher (one resource per
// target), splitBatcher (one resource per described object) and
// cadvisorBatcher (one resource per pod/container).
type sink interface {
	addNumber(s Sample, monotonic bool)
	addHistogram(family string, acc *histAcc)
	addSummary(family string, acc *summAcc)
}

// expSink is a chunker that can take exponential histogram points (the plain
// and split batchers; the cadvisor batcher does not — the kubelet scrape
// stays on the text exposition).
type expSink interface {
	addExponential(family string, p expPoint)
}

// batch is the chunk-lifecycle half: what a batcher has to offer for the shared
// BatchPoints/BatchBytes bound (Scraper.chunkFull) to apply to it. It is split
// out of chunker because the /stats/summary batcher builds its points from JSON
// and never receives an exposition Sample — implementing sink there would mean
// three methods that can never be called, while the bound is the half that MUST
// be shared: it is what keeps a chunk under the collector's receive limit, and
// summary.go's one-resource-per-object shape is exactly where an unbounded
// batch goes past it.
type batch interface {
	take() pmetric.Metrics
	count() int
	size() int // estimated encoded size of the accumulated batch
}

// chunker is a sink that also manages batch lifecycles.
type chunker interface {
	sink
	batch
}

// batcher accumulates samples of one source into a pmetric.Metrics payload
// with a single resource, grouping data points by metric name.
type batcher struct {
	fillResource func(pcommon.Resource)
	startTS      pcommon.Timestamp
	scrapeTS     pcommon.Timestamp
	md           pmetric.Metrics
	sm           pmetric.ScopeMetrics
	byName       map[string]pmetric.Metric
	// lastName/lastMetric short-circuit the byName probe: consecutive samples
	// almost always belong to the same family, and names are interned so the
	// comparison is usually pointer-equal.
	lastName   string
	lastMetric pmetric.Metric
	lastOK     bool
	points     int
	bytes      int
}

func newBatcher(fillResource func(pcommon.Resource), start, scrape time.Time) *batcher {
	b := &batcher{
		fillResource: fillResource,
		startTS:      pcommon.NewTimestampFromTime(start),
		scrapeTS:     pcommon.NewTimestampFromTime(scrape),
	}
	b.reset()
	return b
}

func (b *batcher) reset() {
	b.md = pmetric.NewMetrics()
	rm := b.md.ResourceMetrics().AppendEmpty()
	b.fillResource(rm.Resource())
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName(scopeName)
	sm.Scope().SetVersion(obs.ScopeVersion)
	b.sm = sm
	if b.byName == nil {
		b.byName = make(map[string]pmetric.Metric)
	} else {
		clear(b.byName)
	}
	b.lastOK = false
	b.points = 0
	b.bytes = resourceBytes(rm.Resource(), scopeName) // this chunk's single resource
}

// take returns the accumulated payload and starts a fresh batch.
func (b *batcher) take() pmetric.Metrics {
	md := b.md
	b.reset()
	return md
}

func (b *batcher) count() int { return b.points }
func (b *batcher) size() int  { return b.bytes }

// metricByName resolves the batch's metric for a family name, with a
// last-seen fast path (samples arrive family-ordered).
func (b *batcher) metricByName(name string) (pmetric.Metric, bool) {
	if b.lastOK && name == b.lastName {
		return b.lastMetric, true
	}
	m, ok := b.byName[name]
	if ok {
		b.lastName, b.lastMetric, b.lastOK = name, m, true
	}
	return m, ok
}

// createMetric appends the metric for a family name the batch does not hold
// yet: shape sets its kind, its descriptor — name, framing and HELP/UNIT — is
// charged to the chunk-size estimate through chargeDescriptor, and it is indexed
// as the last-seen metric. shape runs synchronously and is never retained (see
// the shape funcs).
func (b *batcher) createMetric(name string, meta metricMeta, shape func(pmetric.Metric)) pmetric.Metric {
	m := b.sm.Metrics().AppendEmpty()
	m.SetName(name)
	shape(m)
	b.bytes += chargeDescriptor(m, name, meta)
	b.byName[name] = m
	b.lastName, b.lastMetric, b.lastOK = name, m, true
	return m
}

// addNumber emits a gauge or (monotonic cumulative) sum data point.
func (b *batcher) addNumber(s Sample, monotonic bool) {
	m, ok := b.metricByName(s.Name)
	if !ok {
		m = b.createMetric(s.Name, sampleMeta(s), func(m pmetric.Metric) { shapeNumber(m, monotonic) })
	}

	dp, ok := numberDataPoint(m, b.startTS)
	if !ok {
		return
	}
	dp.SetDoubleValue(s.Value)
	dp.SetTimestamp(pointTS(s.TimestampMs, b.scrapeTS))
	putLabels(dp.Attributes(), s.Labels)
	if s.Exemplar != nil {
		setExemplar(dp.Exemplars().AppendEmpty(), *s.Exemplar, b.scrapeTS)
	}
	b.points++
	b.bytes += numberBytes(s)
}

// addHistogram emits one Histogram data point from accumulated cumulative
// buckets: bounds exclude +Inf, bucket counts are de-cumulated, the overflow
// bucket is derived from the total count.
func (b *batcher) addHistogram(family string, acc *histAcc) {
	m, ok := b.metricByName(family)
	if !ok {
		m = b.createMetric(family, acc.meta, shapeHistogram)
	}
	dp, ok := histogramDataPoint(m, b.startTS)
	if !ok {
		return
	}
	dp.SetTimestamp(pointTS(acc.ts, b.scrapeTS))
	fillHistogramPoint(dp, acc)
	putLabels(dp.Attributes(), acc.labels)
	for _, e := range acc.exemplars {
		setExemplar(dp.Exemplars().AppendEmpty(), e, b.scrapeTS)
	}
	b.points++
	b.bytes += histBytes(acc)
}

// addExponential appends one native-histogram point as an OTLP exponential
// histogram (cumulative; the schema IS the OTLP scale — both are base-2).
func (b *batcher) addExponential(family string, p expPoint) {
	m, ok := b.metricByName(family)
	if !ok {
		m = b.createMetric(family, p.meta, shapeExponentialHistogram)
	}
	dp, ok := exponentialDataPoint(m, b.startTS)
	if !ok {
		return
	}
	dp.SetTimestamp(pointTS(p.ts, b.scrapeTS))
	fillExponentialPoint(dp, p)
	putLabels(dp.Attributes(), p.labels)
	for _, e := range p.exemplars {
		setExemplar(dp.Exemplars().AppendEmpty(), e, b.scrapeTS)
	}
	b.points++
	b.bytes += expHistBytes(&p)
}

// addSummary emits one Summary data point from accumulated quantiles.
func (b *batcher) addSummary(family string, acc *summAcc) {
	m, ok := b.metricByName(family)
	if !ok {
		m = b.createMetric(family, acc.meta, shapeSummary)
	}
	dp, ok := summaryDataPoint(m, b.startTS)
	if !ok {
		return
	}
	dp.SetTimestamp(pointTS(acc.ts, b.scrapeTS))
	fillSummaryPoint(dp, acc)
	putLabels(dp.Attributes(), acc.labels)
	b.points++
	b.bytes += summBytes(acc)
}
