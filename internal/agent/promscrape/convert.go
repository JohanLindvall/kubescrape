package promscrape

import (
	"math"
	"slices"
	"strings"
)

// maxExemplarsPerPoint bounds the exemplars attached to one histogram data
// point (one per bucket line otherwise).
const maxExemplarsPerPoint = 16

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

// maxAccPresize caps converter.bucketHint/quantHint: a real histogram carries a
// few dozen buckets at most (Prometheus' defaults are 11 plus +Inf), and the
// presized capacity is uncharged, so a larger hint would only let a target
// that alternates wide and narrow series park slack the family budget does not
// see (at most maxAccPresize x bucketRetainBytes = 1 KiB per accumulator).
const maxAccPresize = 64

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
	// bucketHint and quantHint presize a NEW accumulator's buckets/quantiles
	// from the previous label set's count (every series of a family normally
	// carries the same bucket layout), so a fresh accumulator costs one
	// allocation instead of one per doubling. The freelists start empty on
	// every scrape — a converter lives for one — so without the hint the
	// largest histogram/summary family paid ~9 allocations per series.
	// Capped at maxAccPresize: presized capacity is not charged to the family
	// budget (nor is a recycled accumulator's retained capacity), so the cap is
	// what bounds that uncharged slack per accumulator.
	bucketHint, quantHint int
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
		if s.Name == s.Family {
			// The bare family name, which no histogram series carries (a real
			// bucket is `<family>_bucket` on both fronts). Refused before the
			// le parse, not after: with an `le` label of its own the stray line
			// would otherwise fold into the family as a BUCKET — fabricating a
			// point, or silently rewriting a real label set's counts.
			c.malformed++
			return nil
		}
		le, ok := labelFloat(s.Labels, "le")
		if !ok {
			c.malformed++ // bucket without le
			return nil
		}
		cum, ok := countOf(s.Value)
		if !ok {
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
		acc.buckets = append(acc.buckets, cumBucket{le: le, cum: cum})
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
		count, ok := countOf(s.Value)
		if !ok {
			c.malformed++
			return nil
		}
		acc := c.hist(s)
		if acc == nil {
			c.dropped++
			return nil
		}
		acc.count, acc.hasCount = count, true
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
		count, ok := countOf(s.Value)
		if !ok {
			c.malformed++
			return nil
		}
		acc := c.summ(s)
		if acc == nil {
			c.dropped++
			return nil
		}
		acc.count, acc.hasCount = count, true
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
			// Recycled with its slices' CAPACITY, so their contents are cleared
			// first — across the whole capacity, not just the length: accBytes
			// is released below, and a [:0] alone would keep every label and
			// exemplar string of this family alive on the freelist, uncharged,
			// for the rest of the scrape.
			clear(acc.labels[:cap(acc.labels)])
			clear(acc.exemplars[:cap(acc.exemplars)])
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
			clear(acc.labels[:cap(acc.labels)]) // see the histogram arm
			*acc = summAcc{labels: acc.labels[:0], quantiles: acc.quantiles[:0]}
			c.summFree = append(c.summFree, acc)
			if err == nil {
				err = c.check()
			}
		}
	}
	clear(c.order) // the keys are label fingerprints: text this family charged
	c.order = c.order[:0]
	clear(c.hists)
	clear(c.summs)
	// The budget is released with the state it charged. It must move with the
	// maps — a charge left standing over a cleared family would refuse the NEXT
	// family's accumulators, which is the "silently exports nothing after the
	// first big family" shape of the same bug. And the state must really GO
	// with it: every reuse buffer above is cleared before it is truncated, or a
	// scrape of families whose label count DECREASES keeps one long value per
	// position alive past the charge that bounded it (measured 130 MiB retained
	// from 128 in-budget histogram families).
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
		if prev := c.lastHistAcc; prev != nil {
			c.bucketHint = min(len(prev.buckets), maxAccPresize)
		}
		acc.labels = appendLabelsExcept(slices.Grow(acc.labels[:0], len(s.Labels)), s.Labels, "le")
		acc.buckets = slices.Grow(acc.buckets[:0], c.bucketHint)
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
		if prev := c.lastSummAcc; prev != nil {
			c.quantHint = min(len(prev.quantiles), maxAccPresize)
		}
		acc.labels = appendLabelsExcept(slices.Grow(acc.labels[:0], len(s.Labels)), s.Labels, "quantile")
		acc.quantiles = slices.Grow(acc.quantiles[:0], c.quantHint)
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
	// Resliced, not cleared, on this per-sample path: past this call's length
	// the scratch may still hold an earlier, longer label set — of THIS family
	// only, since forgetLabelKey clears its whole capacity when the family
	// closes (see flushFamily). lastLbl below follows the same rule.
	c.keyLbl = appendLabelsExcept(c.keyLbl[:0], labels, except)
	// The parser interns names and values per scrape, so the element-wise
	// string comparisons are overwhelmingly pointer-equal.
	if except == c.lastExcept && slices.Equal(c.keyLbl, c.lastLbl) {
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

// forgetLabelKey invalidates the labelKey memo. It MUST run wherever the cached
// accumulators stop being the right answer — the accumulators are recycled
// through the freelists, so a stale pointer would fold one label set's
// components into another's point.
func (c *converter) forgetLabelKey() {
	c.lastExcept = "" // never a real except-name (only "le"/"quantile"), so it can never match
	// Drop the string refs too, from the memo AND the sort scratch it was built
	// from, across their whole CAPACITY — a bare [:0] keeps them alive, and
	// labelKey only reslices, so the tail can hold any label set of the family
	// that is closing. The next labelKey rebuilds both.
	clear(c.lastLbl[:cap(c.lastLbl)])
	c.lastLbl = c.lastLbl[:0]
	clear(c.keyLbl[:cap(c.keyLbl)])
	c.keyLbl = c.keyLbl[:0]
	c.lastHistAcc, c.lastSummAcc = nil, nil
}

// countOf converts a cumulative count, or a bucket's count, into the uint64
// OTLP takes. It is the ONE conversion both fronts apply — the classic
// histogram/summary converter here and the native (exponential) histogram path
// in protoparse.go — so a float count converts identically whichever
// representation carried it. They used to differ: the classic side truncated
// and refused anything at or past 2^63, the native side rounded and accepted
// up to 2^64, so one float histogram exported different counts depending on
// its encoding.
//
// A negative, NaN or out-of-range value is not a count and is refused (counted
// malformed by the caller) rather than converted: uint64() of a value outside
// [0, 2^64) is implementation-dependent — ~9.2e18 on amd64 for a negative one
// — and would ship as a garbage bucket. [2^63, 2^64) IS representable, so it
// converts exactly. A fractional count (a float histogram) is rounded to
// nearest, the whole cost of accepting one. float64(math.MaxUint64) is exactly
// 2^64, so the bound compares exactly.
func countOf(v float64) (uint64, bool) {
	if math.IsNaN(v) || v < 0 || v >= math.MaxUint64 {
		return 0, false
	}
	return uint64(math.Round(v)), true
}

// validQuantile bounds a summary's `quantile` label to the [0,1] the Prometheus
// exposition format defines. Outside it the value is not a quantile at all, and
// an OTLP consumer reading `quantile: 42` has no way to render it.
func validQuantile(q float64) bool { return q >= 0 && q <= 1 }
