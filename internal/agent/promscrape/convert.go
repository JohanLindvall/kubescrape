package promscrape

import (
	"encoding/hex"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// maxExemplarsPerPoint bounds the exemplars attached to one histogram data
// point (one per bucket line otherwise).
const maxExemplarsPerPoint = 16

// sink receives converted points; implemented by batcher (one resource per
// target) and cadvisorBatcher (one resource per pod/container).
type sink interface {
	addNumber(s Sample, monotonic bool)
	addHistogram(family string, acc *histAcc)
	addSummary(family string, acc *summAcc)
}

// Size estimation for byte-bounded chunking. A collector's default gRPC
// receive limit is 4 MiB and applies to the DECOMPRESSED message, so a batch
// bounded only by a data-point count can be rejected wholesale (10k points of
// a label-rich family marshal to >5 MiB) — every export of that target would
// then fail, losing all of its metrics. These constants approximate the OTLP
// protobuf encoding (measured within a few percent, always slightly low, which
// the default BatchBytes headroom absorbs).
//
// The non-point bytes are NOT rounding error: the split and cadvisor batchers
// emit one ResourceMetrics per DESCRIBED OBJECT (pod, container), each carrying
// a full enriched attribute set plus its own copy of every metric descriptor.
// Counting only data points underestimated a kube-state-metrics split by ~2x —
// 10k points flushed at an estimated 3 MiB and encoded to 6.7 MiB, past the
// very limit the estimate exists to respect. Every resource and metric is
// therefore charged where it is created.
const (
	pointOverheadBytes  = 32 // timestamps, value, framing of one data point
	attrOverheadBytes   = 8  // per-attribute protobuf framing
	bucketBytes         = 12 // one explicit bound + its count
	histFixedBytes      = 16 // count + sum
	quantileBytes       = 18 // quantile + value
	exemplarBytes       = 48 // value, timestamp, trace/span ids
	resOverheadBytes    = 24 // ResourceMetrics + Resource + ScopeMetrics framing
	metricOverheadBytes = 16 // one Metric: descriptor framing, type wrapper, temporality
)

// The instrumentation scope names stamped on every emitted ScopeMetrics.
const (
	scopeName         = "github.com/JohanLindvall/kubescrape/agent/promscrape"
	scopeNameCadvisor = "github.com/JohanLindvall/kubescrape/agent/promscrape/cadvisor"
)

// resourceBytes estimates the encoded size of one ResourceMetrics' non-point
// content: the resource attributes plus the framing of the resource, its scope
// and the scope name.
func resourceBytes(res pcommon.Resource, scopeName string) int {
	n := resOverheadBytes + len(scopeName)
	res.Attributes().Range(func(k string, v pcommon.Value) bool {
		n += len(k) + len(v.AsString()) + attrOverheadBytes
		return true
	})
	return n
}

// labelBytes estimates the encoded size of a label set.
func labelBytes(labels []Label) int {
	n := 0
	for _, l := range labels {
		n += len(l.Name) + len(l.Value) + attrOverheadBytes
	}
	return n
}

// numberBytes estimates the encoded size of one number data point.
func numberBytes(s Sample) int {
	n := pointOverheadBytes + labelBytes(s.Labels)
	if s.Exemplar != nil {
		n += exemplarBytes
	}
	return n
}

// histBytes estimates the encoded size of one histogram data point.
func histBytes(acc *histAcc) int {
	return pointOverheadBytes + histFixedBytes + labelBytes(acc.labels) +
		len(acc.buckets)*bucketBytes + len(acc.exemplars)*exemplarBytes
}

// summBytes estimates the encoded size of one summary data point.
func summBytes(acc *summAcc) int {
	return pointOverheadBytes + histFixedBytes + labelBytes(acc.labels) +
		len(acc.quantiles)*quantileBytes
}

// converter turns the sample stream into OTLP points. Gauges and counters
// pass straight through to the sink; histogram and summary component
// series (_bucket/_sum/_count, quantiles) are accumulated per family and per
// label set and emitted as proper Histogram/Summary data points when the
// family ends. Memory is bounded by the largest single family, not the
// scrape.
type converter struct {
	b sink
	// emit is called after every data point reaches the sink, giving the
	// caller the chance to flush a full chunk. It is called between the points
	// of a flushing family too — a single family can hold thousands of label
	// sets, so checking only per parsed sample would let one family's emission
	// overshoot the batch limits without bound.
	emit   func() error
	family string
	hists  map[string]*histAcc
	summs  map[string]*summAcc
	order  []string // first-seen emit order of label sets in the family
	keyBuf []byte   // reused fingerprint buffer (labelKey)
	keyLbl []Label  // reused sort scratch (labelKey)
	// Freed accumulators are recycled across families (their slices keep
	// their capacity), so histogram-heavy scrapes stop generating one
	// accumulator + bucket slice per label set per family.
	histFree []*histAcc
	summFree []*summAcc
	// malformed counts component samples that cannot participate in their
	// family (a bucket without le, a summary row without quantile); the
	// caller folds it into the parser's malformed count.
	malformed int
}

type histAcc struct {
	labels    []Label // without le
	ts        int64
	buckets   []cumBucket
	sum       float64
	hasSum    bool
	count     uint64
	hasCount  bool
	exemplars []Exemplar // deep-copied
}

type cumBucket struct {
	le  float64
	cum uint64
}

type summAcc struct {
	labels    []Label // without quantile
	ts        int64
	quantiles []quantileValue
	sum       float64
	hasSum    bool
	count     uint64
	hasCount  bool
}

type quantileValue struct {
	q, v float64
}

// newConverter creates a converter feeding b. emit (may be nil) is called
// after every point reaches the sink.
func newConverter(b sink, emit func() error) *converter {
	return &converter{
		b:     b,
		emit:  emit,
		hists: make(map[string]*histAcc),
		summs: make(map[string]*summAcc),
	}
}

// check gives the caller a chance to flush after a point was emitted.
func (c *converter) check() error {
	if c.emit == nil {
		return nil
	}
	return c.emit()
}

// add consumes one sample. Labels/exemplar are only valid during the call.
func (c *converter) add(s Sample) error {
	if s.Family != c.family {
		if err := c.flushFamily(); err != nil {
			return err
		}
		c.family = s.Family
	}
	switch s.Role {
	case RoleHistogramBucket:
		le, ok := labelFloat(s.Labels, "le")
		if !ok {
			c.malformed++ // bucket without le
			return nil
		}
		acc := c.hist(s)
		acc.buckets = append(acc.buckets, cumBucket{le: le, cum: uint64(s.Value)})
		if s.Exemplar != nil && len(acc.exemplars) < maxExemplarsPerPoint {
			acc.exemplars = append(acc.exemplars, copyExemplar(*s.Exemplar))
		}
	case RoleHistogramSum:
		acc := c.hist(s)
		acc.sum, acc.hasSum = s.Value, true
	case RoleHistogramCount:
		acc := c.hist(s)
		acc.count, acc.hasCount = uint64(s.Value), true
	case RoleSummaryQuantile:
		q, ok := labelFloat(s.Labels, "quantile")
		if !ok {
			// A summary-typed sample without a quantile label is malformed;
			// emitting it as a gauge would claim the family name and block
			// the family's real Summary metric (same name, other shape).
			c.malformed++
			return nil
		}
		acc := c.summ(s)
		acc.quantiles = append(acc.quantiles, quantileValue{q: q, v: s.Value})
	case RoleSummarySum:
		acc := c.summ(s)
		acc.sum, acc.hasSum = s.Value, true
	case RoleSummaryCount:
		acc := c.summ(s)
		acc.count, acc.hasCount = uint64(s.Value), true
	case RoleCounter:
		c.b.addNumber(s, true)
		return c.check()
	default:
		c.b.addNumber(s, false)
		return c.check()
	}
	return nil
}

// finish emits any accumulated family state; call after the parse.
func (c *converter) finish() error {
	return c.flushFamily()
}

func (c *converter) flushFamily() error {
	var err error
	for _, key := range c.order {
		// Delete as we emit: order can hold a key twice when a family is
		// TYPE-redeclared mid-exposition (hist() and summ() each append on
		// first sight in their own map), and re-processing it would emit a
		// zeroed phantom point AND push the accumulator into the freelist
		// twice — two later label sets then share one accumulator, silently
		// destroying a valid family's data.
		if acc, ok := c.hists[key]; ok {
			delete(c.hists, key)
			c.b.addHistogram(c.family, acc)
			*acc = histAcc{labels: acc.labels[:0], buckets: acc.buckets[:0], exemplars: acc.exemplars[:0]}
			c.histFree = append(c.histFree, acc)
			// A family can hold thousands of label sets: check for a full chunk
			// between points, not only once the family is done. On error keep
			// draining (the accumulators must still be recycled and the maps
			// cleared) but stop flushing; the caller aborts the scrape.
			if err == nil {
				err = c.check()
			}
		}
		if acc, ok := c.summs[key]; ok {
			delete(c.summs, key)
			c.b.addSummary(c.family, acc)
			*acc = summAcc{labels: acc.labels[:0], quantiles: acc.quantiles[:0]}
			c.summFree = append(c.summFree, acc)
			if err == nil {
				err = c.check()
			}
		}
	}
	c.order = c.order[:0]
	clear(c.hists)
	clear(c.summs)
	return err
}

func (c *converter) hist(s Sample) *histAcc {
	c.labelKey(s.Labels, "le")
	acc, ok := c.hists[string(c.keyBuf)] // keyed lookup: no allocation
	if !ok {
		key := string(c.keyBuf)
		if n := len(c.histFree); n > 0 {
			acc = c.histFree[n-1]
			c.histFree = c.histFree[:n-1]
		} else {
			acc = &histAcc{}
		}
		acc.labels = appendLabelsExcept(acc.labels[:0], s.Labels, "le")
		c.hists[key] = acc
		c.order = append(c.order, key)
	}
	if s.TimestampMs > acc.ts {
		acc.ts = s.TimestampMs
	}
	return acc
}

func (c *converter) summ(s Sample) *summAcc {
	c.labelKey(s.Labels, "quantile")
	acc, ok := c.summs[string(c.keyBuf)] // keyed lookup: no allocation
	if !ok {
		key := string(c.keyBuf)
		if n := len(c.summFree); n > 0 {
			acc = c.summFree[n-1]
			c.summFree = c.summFree[:n-1]
		} else {
			acc = &summAcc{}
		}
		acc.labels = appendLabelsExcept(acc.labels[:0], s.Labels, "quantile")
		c.summs[key] = acc
		c.order = append(c.order, key)
	}
	if s.TimestampMs > acc.ts {
		acc.ts = s.TimestampMs
	}
	return acc
}

// labelKey builds a canonical fingerprint of the labels (excluding one name)
// into c.keyBuf, reusing c.keyLbl as sort scratch so the hot path does not
// allocate. Exposition label order is stable within a family in practice; the
// key is order-insensitive anyway to be safe.
func (c *converter) labelKey(labels []Label, except string) {
	c.keyLbl = c.keyLbl[:0]
	for _, l := range labels {
		if l.Name != except {
			c.keyLbl = append(c.keyLbl, l)
		}
	}
	// Sorting by (name, value) matches sorting the joined "name\x00value"
	// strings byte-wise, so the fingerprint is order-insensitive.
	slices.SortFunc(c.keyLbl, func(a, b Label) int {
		if r := strings.Compare(a.Name, b.Name); r != 0 {
			return r
		}
		return strings.Compare(a.Value, b.Value)
	})
	c.keyBuf = c.keyBuf[:0]
	for _, l := range c.keyLbl {
		c.keyBuf = append(c.keyBuf, l.Name...)
		c.keyBuf = append(c.keyBuf, 0)
		c.keyBuf = append(c.keyBuf, l.Value...)
		c.keyBuf = append(c.keyBuf, 1)
	}
}

func appendLabelsExcept(dst []Label, labels []Label, except string) []Label {
	for _, l := range labels {
		if l.Name != except {
			dst = append(dst, l)
		}
	}
	return dst
}

func labelFloat(labels []Label, name string) (float64, bool) {
	v, err := strconv.ParseFloat(labelValue(labels, name), 64)
	if err != nil {
		return 0, false // missing label or unparseable value
	}
	return v, true
}

// --- batcher emission ---

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

// remember indexes a newly created metric (the sole creation choke point for
// the plain batcher, so the descriptor's bytes are charged here).
func (b *batcher) remember(name string, m pmetric.Metric) {
	b.byName[name] = m
	b.lastName, b.lastMetric, b.lastOK = name, m, true
	b.bytes += len(name) + metricOverheadBytes
}

// addNumber emits a gauge or (monotonic cumulative) sum data point.
func (b *batcher) addNumber(s Sample, monotonic bool) {
	m, ok := b.metricByName(s.Name)
	if !ok {
		m = b.sm.Metrics().AppendEmpty()
		m.SetName(s.Name)
		if monotonic {
			sum := m.SetEmptySum()
			sum.SetIsMonotonic(true)
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		} else {
			m.SetEmptyGauge()
		}
		b.remember(s.Name, m)
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
		m = b.sm.Metrics().AppendEmpty()
		m.SetName(family)
		h := m.SetEmptyHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		b.remember(family, m)
	}
	if m.Type() != pmetric.MetricTypeHistogram {
		obs.ScrapeCollisions.Inc()
		return
	}

	dp := m.Histogram().DataPoints().AppendEmpty()
	dp.SetStartTimestamp(b.startTS)
	dp.SetTimestamp(pointTS(acc.ts, b.scrapeTS))
	fillHistogramPoint(dp, acc)
	putLabels(dp.Attributes(), acc.labels)
	for _, e := range acc.exemplars {
		setExemplar(dp.Exemplars().AppendEmpty(), e, b.scrapeTS)
	}
	b.points++
	b.bytes += histBytes(acc)
}

// fillHistogramPoint converts accumulated cumulative buckets into the OTLP
// shape: bounds exclude +Inf, bucket counts are de-cumulated, the overflow
// bucket is derived from the total count.
// cmpFloat orders two floats for slices.SortFunc; a non-capturing comparator
// keeps the sort closure off the heap (unlike sort.Slice, which also boxes the
// slice and swaps via reflection).
func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func fillHistogramPoint(dp pmetric.HistogramDataPoint, acc *histAcc) {
	slices.SortFunc(acc.buckets, func(a, b cumBucket) int { return cmpFloat(a.le, b.le) })
	// Deduplicate repeated le values (keep the last occurrence).
	buckets := acc.buckets[:0]
	for i, bk := range acc.buckets {
		if i+1 < len(acc.buckets) && acc.buckets[i+1].le == bk.le {
			continue
		}
		buckets = append(buckets, bk)
	}

	total := acc.count
	if !acc.hasCount {
		if n := len(buckets); n > 0 {
			total = buckets[n-1].cum
		}
	}
	dp.SetCount(total)
	if acc.hasSum {
		dp.SetSum(acc.sum)
	}

	bounds := dp.ExplicitBounds()
	counts := dp.BucketCounts()
	var prev uint64
	for _, bk := range buckets {
		if math.IsInf(bk.le, 1) {
			continue
		}
		bounds.Append(bk.le)
		counts.Append(monotonicDiff(bk.cum, prev))
		prev = bk.cum
	}
	// Overflow bucket: everything above the last finite bound.
	counts.Append(monotonicDiff(total, prev))
}

// addSummary emits one Summary data point from accumulated quantiles.
func (b *batcher) addSummary(family string, acc *summAcc) {
	m, ok := b.metricByName(family)
	if !ok {
		m = b.sm.Metrics().AppendEmpty()
		m.SetName(family)
		m.SetEmptySummary()
		b.remember(family, m)
	}
	if m.Type() != pmetric.MetricTypeSummary {
		obs.ScrapeCollisions.Inc()
		return
	}

	dp := m.Summary().DataPoints().AppendEmpty()
	dp.SetStartTimestamp(b.startTS)
	dp.SetTimestamp(pointTS(acc.ts, b.scrapeTS))
	fillSummaryPoint(dp, acc)
	putLabels(dp.Attributes(), acc.labels)
	b.points++
	b.bytes += summBytes(acc)
}

// fillSummaryPoint sets count, sum and sorted quantile values.
func fillSummaryPoint(dp pmetric.SummaryDataPoint, acc *summAcc) {
	if acc.hasCount {
		dp.SetCount(acc.count)
	}
	if acc.hasSum {
		dp.SetSum(acc.sum)
	}
	slices.SortFunc(acc.quantiles, func(a, b quantileValue) int { return cmpFloat(a.q, b.q) })
	for _, qv := range acc.quantiles {
		q := dp.QuantileValues().AppendEmpty()
		q.SetQuantile(qv.q)
		q.SetValue(qv.v)
	}
}

// pointTS is the sample's own timestamp (ms) or the scrape time when it carried
// none. Shared by all three batchers.
func pointTS(tsMs int64, scrapeTS pcommon.Timestamp) pcommon.Timestamp {
	if tsMs != 0 {
		return pcommon.Timestamp(tsMs * int64(time.Millisecond))
	}
	return scrapeTS
}

// numberDataPoint appends a data point of m's kind — Sum stamps the cumulative
// start time. ok is false, counted as a name collision, when m is neither a Sum
// nor a Gauge (a family name reused across incompatible metric shapes).
func numberDataPoint(m pmetric.Metric, startTS pcommon.Timestamp) (pmetric.NumberDataPoint, bool) {
	switch m.Type() {
	case pmetric.MetricTypeSum:
		dp := m.Sum().DataPoints().AppendEmpty()
		dp.SetStartTimestamp(startTS)
		return dp, true
	case pmetric.MetricTypeGauge:
		return m.Gauge().DataPoints().AppendEmpty(), true
	default:
		obs.ScrapeCollisions.Inc()
		return pmetric.NumberDataPoint{}, false
	}
}

func putLabels(attrs pcommon.Map, labels []Label) {
	attrs.EnsureCapacity(len(labels))
	for _, l := range labels {
		attrs.PutStr(l.Name, l.Value)
	}
}

// setExemplar maps an exposition exemplar onto an OTLP exemplar: trace_id
// and span_id labels become the trace/span fields, everything else becomes
// filtered attributes.
func setExemplar(ex pmetric.Exemplar, e Exemplar, fallbackTS pcommon.Timestamp) {
	ex.SetDoubleValue(e.Value)
	if e.TimestampMs != 0 {
		ex.SetTimestamp(pcommon.Timestamp(e.TimestampMs * int64(time.Millisecond)))
	} else {
		ex.SetTimestamp(fallbackTS)
	}
	for _, l := range e.Labels {
		switch l.Name {
		case "trace_id":
			var id pcommon.TraceID
			if b, err := hex.DecodeString(l.Value); err == nil && len(b) == len(id) {
				copy(id[:], b)
				ex.SetTraceID(id)
				continue
			}
		case "span_id":
			var id pcommon.SpanID
			if b, err := hex.DecodeString(l.Value); err == nil && len(b) == len(id) {
				copy(id[:], b)
				ex.SetSpanID(id)
				continue
			}
		}
		ex.FilteredAttributes().PutStr(l.Name, l.Value)
	}
}

// monotonicDiff clamps decreasing cumulative counts (which would indicate a
// malformed exposition) to zero-width buckets.
func monotonicDiff(cur, prev uint64) uint64 {
	if cur < prev {
		return 0
	}
	return cur - prev
}
