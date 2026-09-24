package promscrape

// The OTLP side every batcher shares: a family's descriptor (metricMeta), the
// size model that keeps a chunk under the collector's receive limit, and the
// point writers — the data-point constructors with their collision guard, the
// shape funcs, the fill* functions that turn an accumulated or decoded point
// into its OTLP shape, and the label and exemplar writers. The plain
// (batch.go), split (splitbatch.go), cadvisor (cadvisorbatch.go) and summary
// (summarybatch.go) batchers differ in WHICH resource a point lands on, never in
// how the point is written or charged.

import (
	"cmp"
	"encoding/hex"
	"math"
	"slices"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// metricMeta is a family's exposition "# HELP"/"# UNIT", carried to whichever
// batcher creates the OTLP metric. It is a per-FAMILY fact: the parser resolves
// it once per family and the batchers stamp it once per metric, never per
// sample.
type metricMeta struct{ help, unit string }

func sampleMeta(s Sample) metricMeta { return metricMeta{help: s.Help, unit: s.Unit} }

// apply stamps the description and unit on a newly created metric and returns
// the bytes they add to the chunk-size estimate. The charge is not optional:
// the estimate is what keeps a chunk under the collector's 4 MiB receive limit,
// and a resource-per-object batcher (split, cadvisor) carries its own copy of
// every descriptor — uncharged HELP text would flush past the limit.
func (mm metricMeta) apply(m pmetric.Metric) int {
	n := 0
	if mm.help != "" {
		m.SetDescription(mm.help)
		n += len(mm.help) + metaFieldBytes
	}
	if mm.unit != "" {
		m.SetUnit(mm.unit)
		n += len(mm.unit) + metaFieldBytes
	}
	return n
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
	pointOverheadBytes = 32 // timestamps, value, framing of one data point
	attrOverheadBytes  = 8  // per-attribute protobuf framing
	// One explicit bound + its count. BOTH are packed fixed64 in OTLP
	// (HistogramDataPoint.explicit_bounds is a double, bucket_counts is
	// fixed64 — NOT varint; only the EXPONENTIAL histogram's counts are
	// varint), so a bucket costs a flat 8+8. Charging 12 made a bucket-heavy
	// family encode ~33% over its estimate and flush past the 4 MiB gRPC
	// limit the estimate exists to respect.
	bucketBytes         = 16
	histFixedBytes      = 16 // count + sum
	quantileBytes       = 18 // quantile + value
	exemplarBytes       = 48 // value, timestamp, trace/span ids (labels charged separately)
	resOverheadBytes    = 24 // ResourceMetrics + Resource + ScopeMetrics framing
	metricOverheadBytes = 16 // one Metric: descriptor framing, type wrapper, temporality
	// metaFieldBytes is one description/unit string field's protobuf framing
	// (tag + length varint), charged on top of the text so the estimate cannot
	// come in UNDER the encoded size of a description-heavy batch.
	metaFieldBytes = 3
)

// The instrumentation scope names stamped on every emitted ScopeMetrics.
const (
	scopeName         = "github.com/JohanLindvall/kubescrape/agent/promscrape"
	scopeNameCadvisor = "github.com/JohanLindvall/kubescrape/agent/promscrape/cadvisor"
	scopeNameSummary  = "github.com/JohanLindvall/kubescrape/agent/promscrape/summary"
)

// resourceBytes estimates the encoded size of one ResourceMetrics' non-point
// content: the resource attributes plus the framing of the resource, its scope,
// the scope name and the scope VERSION.
//
// The version is charged for the same reason the name is: every creation site
// stamps obs.ScopeVersion, which is a 40-char VCS revision in a shipped build,
// and the split and cadvisor batchers create one ScopeMetrics per DESCRIBED
// OBJECT — thousands per scrape on a KSM target, i.e. hundreds of kilobytes
// encoded and counted nowhere. A `go test` binary carries no VCS stamp, so the
// chunk-size guard tests see the 7-char fallback and cannot notice the
// omission; the shipped estimate was the one running short.
func resourceBytes(res pcommon.Resource, scopeName string) int {
	n := resOverheadBytes + len(scopeName) + len(obs.ScopeVersion) + metaFieldBytes
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

// exemplarSize estimates one exemplar's encoded size INCLUDING its labels:
// only trace_id/span_id become fixed-size fields — every other label lands in
// FilteredAttributes. Both fronts bound the set at OpenMetrics' 128 code points
// of names plus values (promparse.MaxExemplarLabelSetRunes), which is still up
// to 512 bytes of UTF-8 per exemplar. The old flat 48-byte charge let a
// 6k-series histogram whose exemplars carried two ~50-char labels flush at an
// estimated 3 MiB and encode to 8.6 MiB — past the 4 MiB collector receive
// limit the estimate exists to respect.
func exemplarSize(e *Exemplar) int {
	return exemplarBytes + labelBytes(e.Labels)
}

// numberBytes estimates the encoded size of one number data point.
func numberBytes(s Sample) int {
	n := pointOverheadBytes + labelBytes(s.Labels)
	if s.Exemplar != nil {
		n += exemplarSize(s.Exemplar)
	}
	return n
}

// histBytes estimates the encoded size of one histogram data point.
func histBytes(acc *histAcc) int {
	// +8 for the overflow count: OTLP always carries one more bucket_count
	// than explicit_bound, and a family whose exposition omitted +Inf has no
	// entry in acc.buckets to charge it against. Over-charging by one slot is
	// safe; under-charging flushes past the collector's receive limit.
	n := pointOverheadBytes + histFixedBytes + labelBytes(acc.labels) +
		len(acc.buckets)*bucketBytes + 8
	for i := range acc.exemplars {
		n += exemplarSize(&acc.exemplars[i])
	}
	return n
}

// summBytes estimates the encoded size of one summary data point.
func summBytes(acc *summAcc) int {
	return pointOverheadBytes + histFixedBytes + labelBytes(acc.labels) +
		len(acc.quantiles)*quantileBytes
}

// expHistBytes estimates one exponential histogram point's encoded size.
func expHistBytes(p *expPoint) int {
	// 9 bytes/bucket (max sint64 varint) not 3: a busy cumulative counter's
	// bucket count needs 4-5 bytes and can reach 9 near 2^63, so a
	// dense-bucket point must not be under-charged into an over-cap batch —
	// the byte bound is the ONE guard against wholesale collector rejection.
	n := pointOverheadBytes + histFixedBytes + labelBytes(p.labels) +
		16 + // zero threshold + zero count
		(len(p.pos)+len(p.neg))*9 + 16 // varint bucket counts + span framing
	for i := range p.exemplars {
		n += exemplarSize(&p.exemplars[i])
	}
	return n
}

// expPoint is one decoded native histogram.
type expPoint struct {
	labels    []Label
	meta      metricMeta
	exemplars []Exemplar
	ts        int64
	schema    int32
	zeroCount uint64
	zeroTh    float64
	count     uint64
	sum       float64
	hasSum    bool
	pos, neg  []uint64
	posOffset int32
	negOffset int32
}

// pointTS is the sample's own timestamp (ms) or the scrape time when it carried
// none. Shared by all three batchers and setExemplar.
func pointTS(tsMs int64, scrapeTS pcommon.Timestamp) pcommon.Timestamp {
	if tsMs > 0 {
		// A ms value beyond this wraps the int64 nanosecond product and would
		// stamp the point with a wildly wrong time (a far-future timestamp
		// silently became a 1970s one). Fall back to the scrape time, which is
		// the same thing an absent timestamp gets.
		if tsMs > math.MaxInt64/int64(time.Millisecond) {
			return scrapeTS
		}
		return pcommon.Timestamp(tsMs * int64(time.Millisecond))
	}
	// Zero means "no timestamp"; a NEGATIVE one (the classic format's
	// timestamp is a signed int64, so `foo 1 -1` parses cleanly) is pre-epoch,
	// which pcommon.Timestamp's unsigned model cannot represent — the uint64
	// cast turned -1 ms into a year-2554 stamp. Same fallback as absent.
	return scrapeTS
}

// numberDataPoint appends a data point of m's kind — Sum stamps the cumulative
// start time. ok is false, counted as a name collision, when m is neither a Sum
// nor a Gauge (a family name reused across incompatible metric shapes).
//
// histogramDataPoint/exponentialDataPoint/summaryDataPoint below are its
// bucketed-kind siblings: one type-mismatch → obs.ScrapeCollisions → bail
// decision for all three batchers (it used to be open-coded eight times), with
// the cumulative start time stamped on the appended point. The caller sets the
// point's own timestamp — which batcher clock applies is the caller's business.
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

func histogramDataPoint(m pmetric.Metric, startTS pcommon.Timestamp) (pmetric.HistogramDataPoint, bool) {
	if m.Type() != pmetric.MetricTypeHistogram {
		obs.ScrapeCollisions.Inc()
		return pmetric.HistogramDataPoint{}, false
	}
	dp := m.Histogram().DataPoints().AppendEmpty()
	dp.SetStartTimestamp(startTS)
	return dp, true
}

func exponentialDataPoint(m pmetric.Metric, startTS pcommon.Timestamp) (pmetric.ExponentialHistogramDataPoint, bool) {
	if m.Type() != pmetric.MetricTypeExponentialHistogram {
		obs.ScrapeCollisions.Inc()
		return pmetric.ExponentialHistogramDataPoint{}, false
	}
	dp := m.ExponentialHistogram().DataPoints().AppendEmpty()
	dp.SetStartTimestamp(startTS)
	return dp, true
}

func summaryDataPoint(m pmetric.Metric, startTS pcommon.Timestamp) (pmetric.SummaryDataPoint, bool) {
	if m.Type() != pmetric.MetricTypeSummary {
		obs.ScrapeCollisions.Inc()
		return pmetric.SummaryDataPoint{}, false
	}
	dp := m.Summary().DataPoints().AppendEmpty()
	dp.SetStartTimestamp(startTS)
	return dp, true
}

// The shape funcs initialize a just-created metric's kind, shared by the
// exposition batchers. The plain batcher takes one in createMetric, which runs
// only when a metric is CREATED; the split and cadvisor batchers take one as the
// `shape` argument of their metric() on the per-point path, and their addNumber
// passes a closure capturing `monotonic` there — one per number point. That costs
// nothing ONLY because metric() calls shape synchronously and never retains
// it, so the closure does not escape and lives on the caller's stack. Keep it
// that way: a metric() that stored shape would move every point's closure to
// the heap. TestSplitBatcherRoutingIsAllocationFree builds the closure per
// call, as addNumber does, so a splitBatcher.metric() whose shape escapes
// fails it; cadvisorBatcher.metric() has no such pin.
func shapeNumber(m pmetric.Metric, monotonic bool) {
	if monotonic {
		sum := m.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	} else {
		m.SetEmptyGauge()
	}
}

func shapeHistogram(m pmetric.Metric) {
	m.SetEmptyHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
}

func shapeExponentialHistogram(m pmetric.Metric) {
	m.SetEmptyExponentialHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
}

func shapeSummary(m pmetric.Metric) { m.SetEmptySummary() }

// chargeDescriptor stamps a newly created metric's HELP/UNIT and returns the
// descriptor's full contribution to the chunk-size estimate: name, framing and
// the description/unit text. It is the ONE spelling of that charge (there were
// six): a resource-per-object batcher (split, cadvisor) repeats every
// descriptor per described object, so an uncharged or half-charged descriptor
// flushes past the collector's 4 MiB receive limit the estimate exists to
// respect. TestBucketHeavyHistogramStaysUnderCollectorLimit guards the sum.
func chargeDescriptor(m pmetric.Metric, name string, meta metricMeta) int {
	return len(name) + metricOverheadBytes + meta.apply(m)
}

// fillHistogramPoint converts accumulated cumulative buckets into the OTLP
// shape: bounds exclude +Inf, bucket counts are de-cumulated, the overflow
// bucket is derived from the total count.
func fillHistogramPoint(dp pmetric.HistogramDataPoint, acc *histAcc) {
	// A non-capturing comparator keeps the sort closure off the heap (unlike
	// sort.Slice, which also boxes the slice and swaps via reflection).
	// cmp.Compare is a strict weak order even over NaN, which labelFloat
	// refuses before a bound can get here anyway.
	slices.SortFunc(acc.buckets, func(a, b cumBucket) int { return cmp.Compare(a.le, b.le) })
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
	// run is the cumulative count this point has EMITTED so far, which is what
	// the next difference must be taken against — not the exposition's own
	// previous cum. OTLP requires sum(bucket_counts) == count, and deriving each
	// bucket from a number that was never emitted breaks that on any exposition
	// whose cumulative counts are not monotonic or disagree with its _count:
	// `h_bucket{le="1"} 9` beside `h_count 3` shipped counts=[9 0] against
	// count=3, and a decreasing pair (le=1 -> 10, le=2 -> 5, _count 10) shipped
	// [10 0 5] = 15 against 10. Such a point is not merely wrong, it is invalid
	// OTLP: a validating collector may reject the whole chunk, which would cost
	// every OTHER target in it. Clamping into [run, total] makes the identity
	// hold BY CONSTRUCTION for every input, and leaves a well-formed histogram
	// (cums non-decreasing, total >= the last one) byte-identical.
	//
	// The two clamps say different things and both are the conservative reading:
	// a cumulative count cannot decrease (keep the frontier — what the retired
	// monotonicDiff did, except that it compared against the un-emitted cum and
	// so let the error back in one bucket later), and no bound may claim more
	// observations than the population the exposition declares in _count.
	var run uint64
	for _, bk := range buckets {
		if math.IsInf(bk.le, 1) {
			continue
		}
		bounds.Append(bk.le)
		cum := max(bk.cum, run)
		cum = min(cum, total)
		counts.Append(cum - run)
		run = cum
	}
	// Overflow bucket: everything above the last finite bound. run <= total
	// holds above, so this closes the point to exactly count.
	counts.Append(total - run)
}

// fillSummaryPoint sets count, sum and sorted quantile values.
func fillSummaryPoint(dp pmetric.SummaryDataPoint, acc *summAcc) {
	if acc.hasCount {
		dp.SetCount(acc.count)
	}
	if acc.hasSum {
		dp.SetSum(acc.sum)
	}
	slices.SortFunc(acc.quantiles, func(a, b quantileValue) int { return cmp.Compare(a.q, b.q) }) // see fillHistogramPoint
	// Deduplicate repeated quantiles (keep the last occurrence), mirroring the
	// bucket path: duplicate series lines ("0.5" and "0.50") otherwise emit
	// two entries for one quantile, which a downstream OTLP→Prometheus
	// translation renders as duplicate samples of the same series.
	for i, qv := range acc.quantiles {
		if i+1 < len(acc.quantiles) && acc.quantiles[i+1].q == qv.q {
			continue
		}
		q := dp.QuantileValues().AppendEmpty()
		q.SetQuantile(qv.q)
		q.SetValue(qv.v)
	}
}

// fillExponentialPoint copies one decoded native histogram onto an OTLP
// exponential-histogram point (the schema IS the OTLP scale — both are
// base-2). The sibling of fillHistogramPoint/fillSummaryPoint; timestamps and
// attributes stay with the batcher, which owns them.
func fillExponentialPoint(dp pmetric.ExponentialHistogramDataPoint, p expPoint) {
	dp.SetScale(p.schema)
	dp.SetZeroCount(p.zeroCount)
	dp.SetZeroThreshold(p.zeroTh)
	dp.SetCount(p.count)
	if p.hasSum {
		dp.SetSum(p.sum)
	}
	dp.Positive().SetOffset(p.posOffset)
	dp.Positive().BucketCounts().FromRaw(p.pos)
	dp.Negative().SetOffset(p.negOffset)
	dp.Negative().BucketCounts().FromRaw(p.neg)
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
//
// The ids are decoded straight into their fixed-size arrays, and only once the
// LENGTH is right: hex.DecodeString allocated the decoded bytes (and converted
// the value) for every exemplar, and for an over-long value — the target's
// choice — allocated half its length before the length check refused it. A
// value that is not exactly a valid id falls through to an attribute, as it
// always did.
func setExemplar(ex pmetric.Exemplar, e Exemplar, fallbackTS pcommon.Timestamp) {
	ex.SetDoubleValue(e.Value)
	// Through pointTS, never a bare ms→ns multiplication: the parser bounds
	// timestamps to int64 MILLISECONDS, so a far-future exemplar timestamp
	// would wrap the nanosecond product exactly as a sample's would — same
	// guard, same scrape-time fallback.
	ex.SetTimestamp(pointTS(e.TimestampMs, fallbackTS))
	for _, l := range e.Labels {
		switch l.Name {
		case "trace_id":
			var id pcommon.TraceID
			if decodeHexID(id[:], l.Value) {
				ex.SetTraceID(id)
				continue
			}
		case "span_id":
			var id pcommon.SpanID
			if decodeHexID(id[:], l.Value) {
				ex.SetSpanID(id)
				continue
			}
		}
		ex.FilteredAttributes().PutStr(l.Name, l.Value)
	}
}

// decodeHexID decodes s into dst when s is exactly dst's hex encoding. The
// length is checked FIRST, so a value of any other size costs nothing; the
// []byte conversion of an id-sized string does not escape hex.Decode and stays
// on the stack.
func decodeHexID(dst []byte, s string) bool {
	if len(s) != hex.EncodedLen(len(dst)) {
		return false
	}
	_, err := hex.Decode(dst, []byte(s))
	return err == nil
}
