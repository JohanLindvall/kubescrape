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
// FilteredAttributes, unbounded by the parser (OpenMetrics allows 128 chars of
// label runes). The old flat 48-byte charge let a 6k-series histogram whose
// exemplars carried two ~50-char labels flush at an estimated 3 MiB and
// encode to 8.6 MiB — past the 4 MiB collector receive limit the estimate
// exists to respect.
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

// maxFamilyAccBytes bounds the heap the converter RETAINS while accumulating
// one histogram/summary family. Holding accumulators for the current family
// only keeps memory off the whole-scrape scale, but it is not a bound: a family
// is as large as the target says it is, and a target is input this process does
// not control. A 524 KiB gzipped exposition — one histogram family of ~900k
// one-line label sets, which compresses to almost nothing because the lines are
// nearly identical — drove 447 MiB of peak heap and OOMKilled the node's agent
// while the scrape recorded outcome "ok". This is the sibling of the parser's
// own maxTypeBytes/maxMetaBytes: a bound on ENTRIES is not a bound on BYTES
// when the target chooses the keys.
//
// The value is per CONVERTER, and one lives per in-flight scrape, so the
// process-wide worst case is Config.Concurrency times this (plus the two
// kubelet scrapes): 16 MiB against the chart's default 512Mi agent limit and
// the default concurrency of 4 is ~64 MiB charged even if every target on the
// node is hostile at once. The floor under it is a family this repo already
// treats as legitimate — TestExemplarChunksStayUnderCollectorLimit's 6000
// series x 6 exemplar-bearing buckets charges ~11 MB — so the budget must stay
// comfortably above that, and a plain family of tens of thousands of label sets
// fits easily.
const maxFamilyAccBytes = 16 << 20

// The charge constants approximate RETAINED HEAP, and are deliberately NOT the
// pointOverheadBytes/attrOverheadBytes family above, which estimate the OTLP
// ENCODING of an emitted point. Approximate is enough: this bound exists to
// keep a hostile family inside an order of magnitude, not to account bytes.
// Every one of them errs high (a label's text is charged once through the key
// fingerprint even though the accumulator may retain its own copy of an
// un-interned value), because under-charging is what the budget is for.
const (
	// accOverheadBytes is one accumulator's fixed cost: the histAcc/summAcc
	// struct, its map entry, its c.order entry and the headers of its slices.
	accOverheadBytes = 192
	// labelRetainBytes is one []Label element (two string headers); the label
	// TEXT is charged once, as part of the key fingerprint.
	labelRetainBytes = 32
	// One cumBucket / one quantileValue.
	bucketRetainBytes   = 16
	quantileRetainBytes = 16
	// One retained Exemplar beside its (separately charged) labels.
	exemplarRetainBytes = 64
)

// exemplarRetained is one deep-copied exemplar's charge. Exemplar labels are
// bounded only by the line bound, so 16 of them per point (maxExemplarsPerPoint)
// is megabytes per accumulator if nothing charges them.
func exemplarRetained(e *Exemplar) int {
	n := exemplarRetainBytes
	for _, l := range e.Labels {
		n += labelRetainBytes + len(l.Name) + len(l.Value)
	}
	return n
}

// converter turns the sample stream into OTLP points. Gauges and counters
// pass straight through to the sink; histogram and summary component
// series (_bucket/_sum/_count, quantiles) are accumulated per family and per
// label set and emitted as proper Histogram/Summary data points when the
// family ends. Memory is bounded by the largest single family, not the
// scrape — and, within one family, by maxFamilyAccBytes.
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
	// Last-seen memo for labelKey, mirroring the parser's lastMetric/lastKV.
	// Every component series of a histogram/summary point carries the SAME
	// labels bar le/quantile — a 12-bucket family is 13 consecutive samples
	// resolving to one accumulator — yet each re-sorted and re-fingerprinted
	// the set and re-probed the map. lastLbl holds the previous call's filtered
	// labels AS FED (pre-sort, so the comparison matches the next call's own
	// pre-sort state) and lastAcc* the accumulator it resolved to; lastExcept
	// discriminates the histogram calls from the summary ones. Invalidated
	// whenever the accumulators go away (flushFamily).
	lastLbl     []Label
	lastExcept  string
	lastHistAcc *histAcc
	lastSummAcc *summAcc
	// Freed accumulators are recycled across families (their slices keep
	// their capacity), so histogram-heavy scrapes stop generating one
	// accumulator + bucket slice per label set per family.
	histFree []*histAcc
	summFree []*summAcc
	// malformed counts component samples that cannot participate in their
	// family (a bucket without le, a summary row without quantile); the
	// caller folds it into the parser's malformed count.
	malformed int
	// accBytes is what the current family's accumulators are charged (see
	// maxFamilyAccBytes); it is released with them in flushFamily. dropped
	// counts the component samples refused because the budget was spent —
	// perfectly well-formed exposition this process declined to hold, which is
	// why it is NOT folded into malformed: the operator is being told about a
	// refusal of ours, not a defect of theirs. The caller reports it as
	// obs.ScrapeSamplesDropped{reason="accumulator"}.
	accBytes int
	dropped  int
}

type histAcc struct {
	labels    []Label // without le
	meta      metricMeta
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
	meta      metricMeta
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

// charge reserves n bytes of the current family's accumulator budget, reporting
// whether it fit. A refusal spends nothing, so a large exemplar that will not
// fit cannot starve the buckets that would have.
func (c *converter) charge(n int) bool {
	if c.accBytes+n > maxFamilyAccBytes {
		return false
	}
	c.accBytes += n
	return true
}

// check gives the caller a chance to flush after a point was emitted.
func (c *converter) check() error {
	if c.emit == nil {
		return nil
	}
	return c.emit()
}

// add consumes one sample. Labels/exemplar are only valid during the call.
//
// The family switch below flushes on EVERY change, so a target that interleaves
// its families — against the format's "one single group" MUST — has each run of
// a histogram/summary family emitted as its own point, and a series repeated in
// the exposition becomes two points. Both are accepted costs of holding
// accumulators for the current family only (the constant-memory property this
// parser exists for); TestKnownDuplicateDataPointDivergences pins the shapes and
// states what fixing either would cost.
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
		if !validCount(s.Value) {
			c.malformed++ // uint64(negative/NaN) wraps to ~9.2e18 garbage
			return nil
		}
		acc := c.hist(s)
		if acc == nil || !c.charge(bucketRetainBytes) {
			// Over the family's byte budget: the sample is dropped and counted
			// HERE, once, for every refusal shape (hist/summ return nil without
			// counting, so a refused label set is one drop and not two). A
			// histogram that loses buckets but keeps its _count still emits a
			// valid point — the overflow bucket absorbs the difference by
			// construction, see fillHistogramPoint — so this degrades resolution
			// rather than shipping a broken distribution.
			c.dropped++
			return nil
		}
		acc.buckets = append(acc.buckets, cumBucket{le: le, cum: uint64(s.Value)})
		// A refused exemplar is not a refused sample: the point ships without
		// it, exactly as it does past maxExemplarsPerPoint.
		if s.Exemplar != nil && len(acc.exemplars) < maxExemplarsPerPoint && c.charge(exemplarRetained(s.Exemplar)) {
			acc.exemplars = append(acc.exemplars, copyExemplar(*s.Exemplar))
		}
	case RoleHistogramSum:
		acc := c.hist(s)
		if acc == nil {
			c.dropped++
			return nil
		}
		acc.sum, acc.hasSum = s.Value, true
	case RoleHistogramCount:
		if !validCount(s.Value) {
			c.malformed++
			return nil
		}
		acc := c.hist(s)
		if acc == nil {
			c.dropped++
			return nil
		}
		acc.count, acc.hasCount = uint64(s.Value), true
	case RoleSummaryQuantile:
		q, ok := labelFloat(s.Labels, "quantile")
		if !ok || !validQuantile(q) {
			// A summary-typed sample without a usable quantile label is
			// malformed; emitting it as a gauge would claim the family name and
			// block the family's real Summary metric (same name, other shape).
			// The range check is part of "usable": a `quantile` outside [0,1]
			// is not a quantile, and OTLP has nowhere to put it.
			c.malformed++
			return nil
		}
		acc := c.summ(s)
		if acc == nil || !c.charge(quantileRetainBytes) {
			c.dropped++
			return nil
		}
		acc.quantiles = append(acc.quantiles, quantileValue{q: q, v: s.Value})
	case RoleSummarySum:
		acc := c.summ(s)
		if acc == nil {
			c.dropped++
			return nil
		}
		acc.sum, acc.hasSum = s.Value, true
	case RoleSummaryCount:
		if !validCount(s.Value) {
			c.malformed++
			return nil
		}
		acc := c.summ(s)
		if acc == nil {
			c.dropped++
			return nil
		}
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
	// The accumulators below are emitted and pushed onto the freelists, so the
	// labelKey memo must not survive them.
	c.forgetLabelKey()
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
	// The budget is released with the state it charged. It must move with the
	// maps — a charge left standing over a cleared family would refuse the NEXT
	// family's accumulators, which is the "silently exports nothing after the
	// first big family" shape of the same bug.
	c.accBytes = 0
	return err
}

func (c *converter) hist(s Sample) *histAcc {
	if c.labelKey(s.Labels, "le") && c.lastHistAcc != nil {
		// Same label set as the previous component sample: keyBuf still holds
		// its fingerprint and the accumulator is already in hand.
		acc := c.lastHistAcc
		if s.TimestampMs > acc.ts {
			acc.ts = s.TimestampMs
		}
		return acc
	}
	acc, ok := c.hists[string(c.keyBuf)] // keyed lookup: no allocation
	if !ok {
		if !c.charge(accOverheadBytes + len(c.keyBuf) + labelRetainBytes*len(s.Labels)) {
			// The family's byte budget is spent, so this label set gets no
			// accumulator (the caller counts the drop). Clearing the memo is not
			// housekeeping: labelKey has already recorded THIS label set as the
			// last one seen, so leaving lastHistAcc pointing at the previously
			// admitted accumulator would make the next component sample of this
			// refused set — its _sum, say — fold into ANOTHER series' point.
			c.lastHistAcc, c.lastSummAcc = nil, nil
			return nil
		}
		key := string(c.keyBuf)
		if n := len(c.histFree); n > 0 {
			acc = c.histFree[n-1]
			c.histFree = c.histFree[:n-1]
		} else {
			acc = &histAcc{}
		}
		acc.labels = appendLabelsExcept(acc.labels[:0], s.Labels, "le")
		acc.meta = sampleMeta(s) // per family: any component series carries it
		c.hists[key] = acc
		c.order = append(c.order, key)
	}
	c.lastHistAcc, c.lastSummAcc = acc, nil
	if s.TimestampMs > acc.ts {
		acc.ts = s.TimestampMs
	}
	return acc
}

func (c *converter) summ(s Sample) *summAcc {
	if c.labelKey(s.Labels, "quantile") && c.lastSummAcc != nil {
		acc := c.lastSummAcc
		if s.TimestampMs > acc.ts {
			acc.ts = s.TimestampMs
		}
		return acc
	}
	acc, ok := c.summs[string(c.keyBuf)] // keyed lookup: no allocation
	if !ok {
		if !c.charge(accOverheadBytes + len(c.keyBuf) + labelRetainBytes*len(s.Labels)) {
			c.lastHistAcc, c.lastSummAcc = nil, nil // see hist: a stale memo folds one series into another
			return nil
		}
		key := string(c.keyBuf)
		if n := len(c.summFree); n > 0 {
			acc = c.summFree[n-1]
			c.summFree = c.summFree[:n-1]
		} else {
			acc = &summAcc{}
		}
		acc.labels = appendLabelsExcept(acc.labels[:0], s.Labels, "quantile")
		acc.meta = sampleMeta(s)
		c.summs[key] = acc
		c.order = append(c.order, key)
	}
	c.lastSummAcc, c.lastHistAcc = acc, nil
	if s.TimestampMs > acc.ts {
		acc.ts = s.TimestampMs
	}
	return acc
}

// labelKey builds a canonical fingerprint of the labels (excluding one name)
// into c.keyBuf, reusing c.keyLbl as sort scratch so the hot path does not
// allocate. Exposition label order is stable within a family in practice; the
// key is order-insensitive anyway to be safe.
//
// It returns true when the fingerprint is UNCHANGED from the previous call —
// same excluded name and same remaining labels in the same order — in which
// case keyBuf already holds it and the caller may reuse its cached accumulator
// instead of re-probing the map. A miss is always safe: it just does the full
// work, so a reordered label set costs correctness nothing.
func (c *converter) labelKey(labels []Label, except string) bool {
	c.keyLbl = c.keyLbl[:0]
	for _, l := range labels {
		if l.Name != except {
			c.keyLbl = append(c.keyLbl, l)
		}
	}
	if except == c.lastExcept && labelsEqual(c.keyLbl, c.lastLbl) {
		return true // keyBuf still fingerprints exactly this set
	}
	// Remember the set AS FED: the next call compares its own pre-sort filtered
	// list, so a memo saved post-sort would miss whenever the exposition's own
	// order is not already sorted.
	c.lastExcept = except
	c.lastLbl = append(c.lastLbl[:0], c.keyLbl...)
	// Sorting by (name, value) matches sorting the joined "name\x00value"
	// strings byte-wise, so the fingerprint is order-insensitive.
	slices.SortFunc(c.keyLbl, func(a, b Label) int {
		if r := strings.Compare(a.Name, b.Name); r != 0 {
			return r
		}
		return strings.Compare(a.Value, b.Value)
	})
	// LENGTH-PREFIXED, like the split and cadvisor resource keys: the text
	// format permits any byte but \, " and newline inside a quoted value, so
	// a value containing the delimiters could forge another series' key and
	// the two would merge into one data point (the duplicate-le dedupe then
	// destroys one of the values outright).
	c.keyBuf = c.keyBuf[:0]
	for _, l := range c.keyLbl {
		c.keyBuf = appendLP(c.keyBuf, l.Name)
		c.keyBuf = appendLP(c.keyBuf, l.Value)
	}
	return false
}

// labelsEqual reports whether two label lists are identical element-wise. The
// parser interns names and values per scrape, so the string comparisons are
// overwhelmingly pointer-equal.
func labelsEqual(a, b []Label) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].Value != b[i].Value {
			return false
		}
	}
	return true
}

// forgetLabelKey invalidates the labelKey memo. It MUST run wherever the cached
// accumulators stop being the right answer — the accumulators are recycled
// through the freelists, so a stale pointer would fold one label set's
// components into another's point.
func (c *converter) forgetLabelKey() {
	c.lastExcept = ""         // never a real except-name (only "le"/"quantile"), so it can never match
	c.lastLbl = c.lastLbl[:0] // drop the interned string refs too
	c.lastHistAcc, c.lastSummAcc = nil, nil
}

func appendLabelsExcept(dst []Label, labels []Label, except string) []Label {
	for _, l := range labels {
		if l.Name != except {
			dst = append(dst, l)
		}
	}
	return dst
}

// validCount reports whether a cumulative count/bucket value can be a uint64:
// uint64(negative, NaN, +Inf or ≥2^63) is implementation-defined (~9.2e18 on
// amd64), so such exposition is counted malformed instead of exported as
// garbage. The upper bound mirrors the OM timestamp guard in promparse: the
// float64 comparison against 2^63 is exact (both representable).
func validCount(v float64) bool {
	return v >= 0 && v < (1<<63) && !math.IsNaN(v)
}

// labelFloat parses a numeric label value — `le` on a histogram bucket, or
// `quantile` on a summary. Both decide where a point LANDS, so a nonsense value
// is not a nonsense label, it is a corrupted distribution.
//
// strconv.ParseFloat happily returns NaN for "NaN"/"nan" and ±Inf for
// "Inf"/"Infinity", and this was the only gate on either label. A single junk
// row from a scraped target — which is whatever a pod annotation or a
// ServiceMonitor points at, not necessarily anything the operator wrote —
// therefore entered the bucket accumulator with an unorderable bound, and the
// sort that establishes the cumulative bucket order put it wherever the
// comparison happened to fall. +Inf is the ONE exception and is legitimate: it
// is the histogram's mandatory overflow bucket.
func labelFloat(labels []Label, name string) (float64, bool) {
	v, err := strconv.ParseFloat(labelValue(labels, name), 64)
	if err != nil {
		return 0, false // missing label or unparseable value
	}
	if math.IsNaN(v) || math.IsInf(v, -1) {
		return 0, false
	}
	return v, true
}

// validQuantile bounds a summary's `quantile` label to the [0,1] the Prometheus
// exposition format defines. Outside it the value is not a quantile at all, and
// an OTLP consumer reading `quantile: 42` has no way to render it.
func validQuantile(q float64) bool { return q >= 0 && q <= 1 }

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

// remember indexes a newly created metric; the descriptor's bytes are charged
// at the creation site via chargeDescriptor.
func (b *batcher) remember(name string, m pmetric.Metric) {
	b.byName[name] = m
	b.lastName, b.lastMetric, b.lastOK = name, m, true
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

// addExponential appends one native-histogram point as an OTLP exponential
// histogram (cumulative; the schema IS the OTLP scale — both are base-2).
func (b *batcher) addExponential(family string, p expPoint) {
	m, ok := b.metricByName(family)
	if !ok {
		m = b.sm.Metrics().AppendEmpty()
		m.SetName(family)
		shapeExponentialHistogram(m)
		b.bytes += chargeDescriptor(m, family, p.meta)
		b.remember(family, m)
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

// addNumber emits a gauge or (monotonic cumulative) sum data point.
func (b *batcher) addNumber(s Sample, monotonic bool) {
	m, ok := b.metricByName(s.Name)
	if !ok {
		m = b.sm.Metrics().AppendEmpty()
		m.SetName(s.Name)
		shapeNumber(m, monotonic)
		b.bytes += chargeDescriptor(m, s.Name, sampleMeta(s))
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
		shapeHistogram(m)
		b.bytes += chargeDescriptor(m, family, acc.meta)
		b.remember(family, m)
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

// fillHistogramPoint converts accumulated cumulative buckets into the OTLP
// shape: bounds exclude +Inf, bucket counts are de-cumulated, the overflow
// bucket is derived from the total count.
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

// addSummary emits one Summary data point from accumulated quantiles.
func (b *batcher) addSummary(family string, acc *summAcc) {
	m, ok := b.metricByName(family)
	if !ok {
		m = b.sm.Metrics().AppendEmpty()
		m.SetName(family)
		shapeSummary(m)
		b.bytes += chargeDescriptor(m, family, acc.meta)
		b.remember(family, m)
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

// fillSummaryPoint sets count, sum and sorted quantile values.
func fillSummaryPoint(dp pmetric.SummaryDataPoint, acc *summAcc) {
	if acc.hasCount {
		dp.SetCount(acc.count)
	}
	if acc.hasSum {
		dp.SetSum(acc.sum)
	}
	slices.SortFunc(acc.quantiles, func(a, b quantileValue) int { return cmpFloat(a.q, b.q) })
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

// The shape funcs initialize a just-created metric's kind, shared by all three
// batchers. Top-level funcs, never per-point closures: the split and cadvisor
// batchers pass them as the `shape` argument on allocation-pinned paths.
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
	// Through pointTS, never a bare ms→ns multiplication: the parser bounds
	// timestamps to int64 MILLISECONDS, so a far-future exemplar timestamp
	// would wrap the nanosecond product exactly as a sample's would — same
	// guard, same scrape-time fallback.
	ex.SetTimestamp(pointTS(e.TimestampMs, fallbackTS))
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
