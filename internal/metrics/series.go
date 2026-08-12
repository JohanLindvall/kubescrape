package metrics

import (
	"log/slog"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

const leLabel = "le"

// drops counts observations the series store REFUSED, for the store that owns
// them. Every rejection path must bump one: a dropped observation that is only
// logged (at most hourly, per series) is invisible loss.
//
// These used to be PROCESS-GLOBAL atomics, purely to dodge an import cycle with
// obs (which imports this package, so the counters could not live there). The
// cycle dissolves the moment obs registers a getter over the instance, which is
// the pattern six other subsystems here already use — and the globals were not
// free: two DynamicMetricSets in one process silently merged their counts, the
// Registry's (essentially impossible) refusals landed on a metric documented as
// the log-metrics one, and six tests had to do before/after arithmetic to
// isolate themselves from every other test in the package.
type drops struct {
	capped   atomic.Uint64
	nan      atomic.Uint64
	retained atomic.Uint64

	mu       sync.Mutex
	byMetric map[string]uint64
}

// Capped counts observations rejected because the series' label-set
// cardinality cap was reached (a new label combination could not be admitted).
func (d *drops) Capped() uint64 { return d.capped.Load() }

// CappedByMetric reports cap-refused observations per metric name.
//
// The cap frees slots only through idleness, so one burst of high-cardinality
// labels blinds a metric for maxAge + the grace window — 24h by default. An
// aggregate counter says that happened; it does not say to WHICH metric, which
// is the only thing an operator can act on.
func (d *drops) CappedByMetric() map[string]float64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]float64, len(d.byMetric))
	for k, v := range d.byMetric {
		out[k] = float64(v)
	}
	return out
}

// NaN counts observations rejected because the extracted value was not
// finite — NaN or +/-Inf alike. (The name predates the Inf arm; both take this
// path, since neither is representable as a sample and both would poison every
// aggregation the series feeds.)
func (d *drops) NaN() uint64 { return d.nan.Load() }

// Retained counts undelivered export chunks dropped because the
// re-offer buffer was full — a collector outage lasting longer than
// maxRetainedResources can hold. These ARE lost observations, and they are the
// only ones the retention cannot save.
func (d *drops) Retained() uint64 { return d.retained.Load() }

// addRetained counts n dropped undelivered resources.
func (d *drops) addRetained(n uint64) { d.retained.Add(n) }

// recordCapped counts one cap-refused observation for metric.
func (d *drops) recordCapped(metric string) {
	d.capped.Add(1)
	d.mu.Lock()
	if d.byMetric == nil {
		d.byMetric = map[string]uint64{}
	}
	d.byMetric[metric]++
	d.mu.Unlock()
}

// seriesKind selects how observations accumulate and how the series exports.
type seriesKind int

const (
	kindCounter   seriesKind = iota // monotonic sum
	kindGauge                       // last value wins
	kindHistogram                   // bucketed distribution
	kindSummary                     // running sum + count
)

// gaugeAction selects how a gauge folds each observation. It is meaningless for
// other kinds (which always accumulate).
type gaugeAction int

const (
	actionSet gaugeAction = iota // gauge = value (default)
	actionInc                    // gauge += 1
	actionDec                    // gauge -= 1
	actionAdd                    // gauge += value
	actionSub                    // gauge -= value
	// The following aggregate values over a window: the aggregate is emitted on
	// every export and kept while no new value arrives; the next value after an
	// export starts a fresh window. actionMin must stay first of this group
	// (aggregating() tests action >= actionMin). value/count hold the
	// per-action running state (see record); snapshot renders the aggregate.
	// This set is deliberately closed: anything derivable from these (stddev,
	// range, delta, first, ...) belongs in backend recording rules, which
	// re-aggregate freely — not as more per-sample state here.
	actionMin   // window minimum (value)
	actionMax   // window maximum (value)
	actionAvg   // window mean (value = running sum, count = n)
	actionSum   // window total (value = running sum)
	actionCount // number of matching lines in the window (count; value ignored)
)

// aggregating reports whether the series is a windowed-aggregation gauge.
func (s *series) aggregating() bool { return s.kind == kindGauge && s.action >= actionMin }

// aggregateValue renders a window's stored state into the value to emit.
func (s *series) aggregateValue(samp *sample) float64 {
	n := float64(samp.count)
	switch s.action {
	case actionAvg:
		if samp.count > 0 {
			return samp.value / n
		}
		return 0
	case actionCount:
		return n
	default: // min, max, sum
		return samp.value
	}
}

// sample is one (resource, label combination) live value. labels is the
// serialized data-point label set and resource the serialized resource-attribute
// set (both via labels.String).
type sample struct {
	value    float64
	labels   string
	resource string
	count    uint64
	// counts holds a histogram's CUMULATIVE observation count per finite bucket
	// bound (series.buckets minus the +Inf entry; the +Inf figures are value/
	// count — the sum and total). A histogram keeps ONE sample per label set
	// rather than one per bucket stream: fifteen map entries and fifteen full
	// label strings per label set cost ~15x what a counter does, made
	// maxCardinality count bucket streams instead of the label sets it
	// documents (patched by the maxStreams translation this replaced), and let
	// a partially-admitted family export underflowed cumulative buckets. One
	// entry per label set makes a partial family unrepresentable. Nil for
	// every non-histogram.
	counts []uint64
	// start is the epoch second this cumulative stream began accumulating: the
	// admission of the sample, or the last idle reset that genuinely zeroed it.
	// It becomes StartTimeUnixNano on every exported point.
	//
	// It is NOT the export time. StartTimeUnixNano == TimeUnixNano is the OTLP
	// encoding for a point that RESET at that instant, and snapshot does not
	// reset counters — the values are cumulative since the sample was admitted.
	// Stamping the export time made every self-metric and every log-derived
	// counter declare itself a reset on every push: a cumulative-to-delta
	// consumer (Datadog, Dynatrace, AWS EMF) then reports the whole running
	// total as that interval's delta, and Google Cloud rejects a point whose
	// start is not strictly before its end.
	start   int64
	initial bool
	// sealed marks an aggregation window as already emitted; the next observed
	// value starts a fresh window (min/max/avg/first/last gauges).
	sealed bool
}

type expiringSample struct {
	sample
	when int64 // epoch seconds of the last observation
	// exported reports whether the CURRENT value has already reached an export.
	// The idle reset must never zero counts that no export has carried: maxAge
	// may legally be shorter than the export interval, in which case every
	// observation between two exports would otherwise be observed and destroyed
	// without ever being emitted.
	exported bool
}

// series holds the live values of one metric: a set of label combinations,
// each expiring after a period of inactivity and capped in number.
type series struct {
	mu   sync.Mutex
	db   map[xxh3.Uint128]*expiringSample
	name string
	desc string
	kind seriesKind

	// drops is the OWNING store's refusal counters (never nil; newSeries fills
	// it in). Per store, not per process: see the type's doc.
	drops *drops
	// now is the clock, in epoch seconds. nil takes the package's coarse clock,
	// which is what production always does; a test injects its own the way
	// store.now does, so there is no process-global override and hence no
	// atomic load per observation paying for one.
	now func() int64

	action  gaugeAction // gauge fold mode; ignored for other kinds
	maxSize int         // cap on distinct LABEL COMBINATIONS (config maxCardinality)
	// db is keyed per LABEL COMBINATION for every kind — a histogram is one
	// sample carrying its whole per-bucket distribution (sample.counts) — so
	// len(db) compares against maxSize directly. The store used to key
	// histograms per bucket STREAM and translate the cap through a derived
	// maxStreams budget; the translation is gone with the layout.
	expiration int64 // seconds of inactivity before a combination expires
	lastWarn   int64 // epoch seconds of the last cardinality warning
	log        *slog.Logger

	// buckets are the histogram boundaries with +Inf appended; nil for
	// non-histograms.
	buckets []float64
	// lastWarn rate-limits the cardinality-cap notice.
}

// seriesSpec configures a new series.
type seriesSpec struct {
	name, desc string
	kind       seriesKind
	action     gaugeAction
	maxSize    int
	expiration time.Duration
	buckets    []float64
	log        *slog.Logger
	// drops is the owning store's refusal counters; nil gets a private set,
	// which is what a bare newSeries in a test wants.
	drops *drops
	// now overrides the coarse clock (tests).
	now func() int64
}

var defaultBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10}

// expirationSeconds is the stored form of a configured maxAge: whole seconds,
// rounded UP so a sub-second window never truncates to "expire on every
// export". compileRule compares against it to reject two rules declaring
// different maxAges for one metric name, so the conversion must have one home.
func expirationSeconds(d time.Duration) int64 { return int64(math.Ceil(d.Seconds())) }

func newSeries(spec seriesSpec) *series {
	log := spec.log
	if log == nil {
		log = slog.Default()
	}
	dr := spec.drops
	if dr == nil {
		dr = &drops{}
	}
	if spec.now == nil {
		// Only a series that will actually read the coarse clock starts it.
		startEpochClock()
	}
	s := &series{
		db:         make(map[xxh3.Uint128]*expiringSample),
		drops:      dr,
		now:        spec.now,
		name:       spec.name,
		desc:       spec.desc,
		kind:       spec.kind,
		action:     spec.action,
		maxSize:    spec.maxSize,
		expiration: expirationSeconds(spec.expiration),
		log:        log,
	}
	if spec.kind == kindHistogram {
		s.initBuckets(spec.buckets)
	}
	return s
}

// sameBuckets reports whether a fresh registration of this kind with these
// bounds would produce the buckets this series already has. Only a histogram
// has any (newSeries ignores the field otherwise), and the comparison is
// against the NORMALIZED form: empty means defaultBuckets, and initBuckets
// appends the +Inf bound.
func (s *series) sameBuckets(kind seriesKind, buckets []float64) bool {
	if kind != kindHistogram {
		return true
	}
	if len(buckets) == 0 {
		buckets = defaultBuckets
	}
	return slices.Equal(s.buckets[:len(s.buckets)-1], buckets)
}

// epoch reads the series' clock: the injected one in tests, the process's
// coarse ten-second clock otherwise.
func (s *series) epoch() int64 {
	if s.now != nil {
		return s.now()
	}
	return coarseEpoch()
}

// initBuckets sorts out the histogram bucket bounds (+Inf appended).
func (s *series) initBuckets(buckets []float64) {
	if len(buckets) == 0 {
		buckets = defaultBuckets
	}
	s.buckets = append(append([]float64(nil), buckets...), math.Inf(1))
}

// bounds are the histogram's finite bucket bounds — what sample.counts indexes.
func (s *series) bounds() []float64 { return s.buckets[:len(s.buckets)-1] }

// observe records value for the given data-point label set, resource, and extra
// resource labels. The series is keyed by all three together (their hashes
// XOR-fold into the base accumulator), so per-resource series are distinct. A
// histogram is ONE sample per label set — the value folds into its sum/count
// and every counts slot whose bound it does not exceed — so every kind is a
// single hash and a single map probe per observation.
func (s *series) observe(lbls labels, value float64, resAccum xxh3.Uint128, res pcommon.Map, resLabels labels) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		// Inf too, and for the same reason: ParseFloat accepts "inf"/"Infinity"
		// from a log line, and Inf is ABSORBING under every accumulate path —
		// one such observation pins a counter, summary or histogram sum at Inf
		// for the whole maxAge (24h by default), which no later real value can
		// undo. Counted, never admitted.
		s.drops.nan.Add(1)
		return
	}
	now := s.epoch()
	base := s.baseAccum(lbls)
	base = xor128(base, xor128(resAccum, resLabelsAccum(res, resLabels)))

	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordSingle(mixHash(base), value, lbls, now, res, resLabels)
}

// observePreHashed is the registry fast path: the bound wrappers bump fixed
// label sets, so the accumulators AND the finalized hash are precomputed at
// construction; a bump pays neither the label rehash nor the avalanche.
func (s *series) observePreHashed(lbls labels, hash xxh3.Uint128, value float64, res pcommon.Map) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		s.drops.nan.Add(1)
		return
	}
	now := s.epoch()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordSingle(hash, value, lbls, now, res, nil)
}

// recordSingle folds one observation into its sample: it admits the sample on
// first sight, then records the value. The
// caller holds s.mu. Shared by observe and the registry's observePreHashed.
func (s *series) recordSingle(hash xxh3.Uint128, value float64, lbls labels, now int64, res pcommon.Map, resLabels labels) {
	samp := s.db[hash]
	if samp == nil {
		samp = s.admit(hash, lbls, now, res, resLabels)
		if samp == nil {
			return
		}
	}
	s.record(samp, value)
	samp.when = now
}

// materialize admits the sample for a fixed label set at ZERO, without
// recording an observation, so a series that has been BOUND exists before it
// first fires. It is the registry's half of the absent-vs-zero problem: a
// counter whose series appears only once something increments it makes "this
// never happened" indistinguishable from "this code path does not exist in
// this build / this policy is not configured", and every alert written over
// the absent form silently matches nothing.
//
// Idempotent, and never an OBSERVATION: value and count stay 0, so the first
// real bump is still the first bump. Only the Registry's bound wrappers reach
// it — a DynamicMetricSet's label sets come from log DATA, where a series
// nothing has observed is exactly what should not exist.
func (s *series) materialize(lbls labels, hash xxh3.Uint128) {
	now := s.epoch()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.db[hash]; ok {
		return
	}
	s.admit(hash, lbls, now, emptyResource, nil)
}

// baseAccum hashes the caller's data-point labels once (order-independent).
//
// It used to strip a caller-supplied "le" from a histogram's identity here, via
// the fold-out. That moved to the two doors an "le" can arrive through —
// rejectHistogramLe at config compile and EmitDirect for a script's label map —
// so the hot path no longer probes every histogram observation for a label that
// is now refused before it can be observed.
func (s *series) baseAccum(lbls labels) xxh3.Uint128 { return lbls.hashAccum() }

// admit inserts a new sample for a previously unseen label combination, or
// returns nil (warning at most hourly) when the cardinality cap is reached. It
// runs only on the cold path, so serializing the label set here is cheap.
func (s *series) admit(hash xxh3.Uint128, lbls labels, now int64, res pcommon.Map, resLabels labels) *expiringSample {
	if s.maxSize > 0 && len(s.db) >= s.maxSize {
		s.warnCapped(lbls, now)
		return nil
	}
	samp := &expiringSample{
		sample: sample{labels: lbls.String(), resource: resourceString(res, resLabels), initial: true, start: s.streamStart(now)},
		when:   now,
	}
	if s.kind == kindHistogram {
		samp.counts = make([]uint64, len(s.bounds()))
	}
	s.db[hash] = samp
	return samp
}

// counterBaselineSeconds backdates a COUNTER stream's declared start so that
// the two synthetic zero points renderNumber emits ahead of a series' first
// real value (one and two minutes before it — see there) both fall strictly
// after it. A point whose start equals its own timestamp encodes a reset, so
// stamping the zeros with their own timestamp would put the very defect this
// field exists to remove back on the one point that is easiest to get wrong.
// Three minutes is the two-minute backdate plus one more step of headroom; a
// counter was zero before its first observation, so the claim is true.
const counterBaselineSeconds = 3 * 60

// streamStart is the start-of-accumulation stamp a stream admitted (or reset)
// at now should carry.
func (s *series) streamStart(now int64) int64 {
	if s.kind == kindCounter {
		return now - counterBaselineSeconds
	}
	return now
}

// warnCapped counts the refused observation and logs the cardinality cap at
// most hourly (caller holds the lock).
func (s *series) warnCapped(lbls labels, now int64) {
	s.drops.recordCapped(s.name)
	if now-s.lastWarn >= 3600 {
		s.lastWarn = now
		s.log.Info("max label count reached for log metric",
			"metric", s.name, "labels", lbls.String(), "maxsize", s.maxSize)
	}
}

// record folds one observation into a sample. Gauges apply their action;
// counters, summaries and histograms accumulate (a histogram's value/count are
// its sum and total observation count, and each counts slot tallies the
// values within its bound).
func (s *series) record(samp *expiringSample, value float64) {
	samp.exported = false // a new value: an export must carry it before it may be reset
	if s.aggregating() {
		// A brand-new sample, or the first value after an emit, starts a fresh
		// window; the rest fold in.
		if samp.sealed || samp.count == 0 {
			samp.sealed = false
			samp.value = value
			samp.count = 1
			return
		}
		switch s.action {
		case actionMin:
			if value < samp.value {
				samp.value = value
			}
		case actionMax:
			if value > samp.value {
				samp.value = value
			}
		case actionAvg, actionSum:
			samp.value += value // running sum
		case actionCount:
			// only the tally matters
		}
		samp.count++
		return
	}
	if s.kind == kindGauge {
		switch s.action {
		case actionInc:
			samp.value++
		case actionDec:
			samp.value--
		case actionAdd:
			samp.value += value
		case actionSub:
			samp.value -= value
		default: // actionSet
			samp.value = value
		}
		samp.count++
		return
	}
	samp.value += value
	samp.count++
	if s.kind == kindHistogram {
		for i, bound := range s.bounds() {
			if value <= bound {
				samp.counts[i]++
			}
		}
	}
}

// emit copies a sample out for a snapshot's caller. The value copy alone is
// not enough for a histogram: counts aliases the live per-bucket array, which
// keeps counting (and is cleared on idle reset) after s.mu is released, and
// export retention legitimately holds emitted samples across intervals.
func (samp *expiringSample) emit() sample {
	out := samp.sample
	if out.counts != nil {
		out.counts = slices.Clone(out.counts)
	}
	return out
}

// snapshot returns the live samples. Combinations idle past their expiration
// are reset, and deleted after a further four-minute grace period, so stale
// series stop being exported.
func (s *series) snapshot() []sample {
	now := s.epoch()
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]sample, 0, len(s.db))
	for hash, samp := range s.db {
		idle := now - samp.when - s.expiration
		if idle >= 4*60 {
			// Deleting the sample: emit it first if this value never reached an
			// export. With the export interval past maxAge+grace (both legal and
			// unclamped) a sample observed just after one export is deleted at
			// the next, unseen — the same never-exported loss the idle-reset
			// branch below guards against, one branch up. Aggregating gauges emit
			// their windowed aggregate (as the aggregating branch does); a value
			// observed once, then idled straight past the grace before any
			// snapshot ran the aggregating branch, is otherwise destroyed unseen.
			if !samp.exported {
				emit := samp.emit()
				if s.aggregating() {
					emit.value = s.aggregateValue(&samp.sample)
				}
				out = append(out, emit)
			}
			delete(s.db, hash)
			continue
		}
		if s.aggregating() {
			// Keep emitting the aggregate even when idle; seal the window so the
			// next observed value starts a fresh one. Mark exported so the
			// later grace-DELETE branch (guarded by !exported) does not re-emit
			// this same aggregate a second time — that guard is meant to catch
			// a window NEVER snapshotted, which the aggregating branch has now
			// handled.
			emit := samp.sample
			emit.value = s.aggregateValue(&samp.sample)
			out = append(out, emit)
			samp.initial = false
			samp.sealed = true
			samp.exported = true
			continue
		}
		if idle > 0 {
			// Idle past its expiration: zero it so a later re-appearance starts a
			// fresh count. But emit it first if this value has never been
			// exported — with maxAge below the export interval, the observation
			// would otherwise be destroyed having never left the process.
			if !samp.exported {
				out = append(out, samp.emit())
			}
			samp.initial = false
			samp.count = 0
			samp.value = 0
			clear(samp.counts)
			// The ONE place a live sample's accumulation genuinely restarts, so
			// the one place start moves. The emit above copied the pre-reset
			// sample, so it keeps the old start; everything after this carries
			// the new one, which is what tells a consumer the drop to zero was a
			// reset and not a counter running backwards. (The grace-DELETE
			// branch above needs no equivalent: the sample is gone, and a later
			// re-appearance is a fresh admit.)
			samp.start = s.streamStart(now)
			samp.exported = true // the zero needs no further emission
			continue
		}
		out = append(out, samp.emit())
		samp.initial = false
		samp.exported = true
	}
	return out
}

// rearmInitial re-marks the zero-baseline flag on the samples of a FAILED
// export. snapshot consumes `initial` optimistically; the Registry has no
// retention (unlike DynamicMetricSet, whose retained raw samples re-render
// the baseline themselves), so without the re-arm a collector outage at the
// first export permanently ate every counter's synthetic zero point — the
// exact first-ramp loss the baselines exist to prevent. Failure-path only;
// the linear db walk is irrelevant at self-telemetry cardinality.
func (s *series) rearmInitial(samples []sample) {
	var want map[string]struct{}
	for i := range samples {
		if samples[i].initial {
			if want == nil {
				want = make(map[string]struct{})
			}
			want[samples[i].resource+"\x00"+samples[i].labels] = struct{}{}
		}
	}
	if want == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.db {
		if _, ok := want[e.resource+"\x00"+e.labels]; ok {
			e.initial = true
		}
	}
}
