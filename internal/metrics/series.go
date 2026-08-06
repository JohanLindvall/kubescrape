package metrics

import (
	"log/slog"
	"math"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
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
	capped    atomic.Uint64
	collision atomic.Uint64
	nan       atomic.Uint64
	retained  atomic.Uint64

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

// Collision counts observations rejected because their hash matched an
// existing sample of a DIFFERENT series (a 64-bit collision; merging them would
// corrupt both).
func (d *drops) Collision() uint64 { return d.collision.Load() }

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
// set (both via labels.String); bucket indexes into series.buckets for
// histograms.
type sample struct {
	value    float64
	labels   string
	resource string
	bucket   int
	count    uint64
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
	start int64
	// check is the independent second hash of the sample's identity; a lookup
	// whose primary hash matches but whose check differs is a 64-bit
	// collision between distinct series and is rejected instead of merged.
	check   uint64
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
	db   map[uint64]*expiringSample
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
	// maxStreams is maxSize expressed in db entries: db is keyed per BUCKET
	// STREAM, so a histogram's label set costs len(buckets) entries. Comparing
	// len(db) against maxSize directly divided the configured cap by the bucket
	// count behind the user's back — `maxCardinality: 10000` on a default
	// 15-stream histogram admitted 666 label combinations, and the config,
	// README and warning all said 10000. Counters and gauges have one stream,
	// so for them the two are equal and nothing changes.
	maxStreams int
	expiration int64 // seconds of inactivity before a combination expires
	lastWarn   int64 // epoch seconds of the last cardinality warning
	log        *slog.Logger

	// buckets are the histogram boundaries with +Inf appended; bucketStr the
	// matching "le" strings; bucketHash[i] = combineHash(hash("le"),
	// hash(bucketStr[i])), precomputed so observe folds a bucket's le label into
	// the base hash without materializing a per-bucket label set. All nil for
	// non-histograms, where the single "bucket" carries the value directly.
	buckets     []float64
	bucketStr   []string
	bucketHash  []uint64
	bucketCheck []uint64
	// lastWarn rate-limits the cardinality-cap notice; lastCollision the
	// hash-collision warn — separate so neither suppresses the other.
	lastCollision int64

	// probe is observe's per-observation histogram scratch, reused under s.mu:
	// the admission pre-pass and the record loop walk the same bucket streams,
	// so the pre-pass memoises each stream's hash and looked-up sample here and
	// the record loop reads them back. Without it every matched line hashed and
	// probed the map twice per bucket. Non-nil only for histograms.
	probe []bucketProbe
}

// bucketProbe is one bucket stream's memoised lookup (see series.probe).
type bucketProbe struct {
	hash uint64
	samp *expiringSample
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
		db:         make(map[uint64]*expiringSample),
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
	} else {
		// A single implicit bucket with an infinite bound so observe's loop
		// records every value once.
		s.buckets = []float64{math.Inf(1)}
		s.bucketStr = []string{""}
	}
	s.maxStreams = spec.maxSize * len(s.buckets)
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

// initBuckets sorts out the histogram bucket bounds and precomputes the "le"
// label strings and fold hashes.
func (s *series) initBuckets(buckets []float64) {
	if len(buckets) == 0 {
		buckets = defaultBuckets
	}
	s.buckets = append(append([]float64(nil), buckets...), math.Inf(1))
	s.bucketStr = make([]string, 0, len(s.buckets))
	for _, b := range buckets {
		s.bucketStr = append(s.bucketStr, strconv.FormatFloat(b, 'f', -1, 64))
	}
	s.bucketStr = append(s.bucketStr, "+Inf")

	leHash := xxhash.Sum64String(leLabel)
	s.bucketHash = make([]uint64, len(s.bucketStr))
	s.bucketCheck = make([]uint64, len(s.bucketStr))
	for i, bs := range s.bucketStr {
		hv := xxhash.Sum64String(bs)
		s.bucketHash[i] = combineHash(leHash, hv)
		s.bucketCheck[i] = combineCheck(leHash, hv)
	}
}

// observe records value for the given data-point label set, resource, and extra
// resource labels. The series is keyed by all three together (their hashes
// XOR-fold into the base accumulator), so per-resource series are distinct. For
// a histogram the value is counted into every bucket whose bound it does not
// exceed.
func (s *series) observe(lbls labels, value float64, resAccum resKey, res pcommon.Map, resLabels labels) {
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
	base, check := s.baseAccum(lbls)
	rl := resLabelsAccum(res, resLabels)
	base += resAccum.accum + rl.accum
	check += resAccum.check + rl.check

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.kind != kindHistogram {
		// Single stream (counter/gauge/summary, bound +Inf): one lookup and one
		// hash. The two-pass form below exists solely for histogram all-or-nothing
		// admission; running it here would pay a redundant lookup+avalanche per
		// matched line.
		s.recordSingle(s.streamHash(base, 0), check, value, lbls, now, res, resLabels)
		return
	}
	// Pre-pass over every bucket stream: histogram admission is all-or-nothing
	// (a partial set of streams exports underflowed cumulative counts), and a
	// check-hash mismatch anywhere drops the WHOLE observation for the same
	// reason — a mid-loop skip would leave sibling buckets recording.
	//
	// The record loop below needs the very same per-bucket hash and map entry,
	// so the pre-pass memoises both into s.probe: the cap check still sees
	// every stream before anything is admitted (the semantics the two passes
	// exist for), but a matched line now pays ONE hash and ONE map probe per
	// bucket instead of two.
	if cap(s.probe) < len(s.buckets) {
		s.probe = make([]bucketProbe, len(s.buckets))
	}
	probe := s.probe[:len(s.buckets)]
	{
		missing := 0
		for i := range s.buckets {
			h := s.streamHash(base, i)
			samp := s.db[h]
			probe[i] = bucketProbe{hash: h, samp: samp}
			if samp == nil {
				missing++
				continue
			}
			if samp.check != s.streamCheck(check, i) {
				// 64-bit collision between distinct series (~2^-64 per
				// pair): refuse to merge.
				s.drops.collision.Add(1)
				if now-s.lastCollision >= 3600 {
					s.lastCollision = now
					s.log.Warn("series hash collision, dropping observation", "metric", s.name)
				}
				return
			}
		}
		if s.maxStreams > 0 && missing > 0 && len(s.db)+missing > s.maxStreams {
			s.warnCapped(lbls, now)
			return
		}
	}
	for i, bound := range s.buckets {
		samp := probe[i].samp
		if samp == nil {
			// The pre-pass proved there is room for every missing stream, so
			// admit cannot refuse here; it re-checks anyway (it is shared with
			// the single-stream path) and a nil is still handled.
			samp = s.admit(probe[i].hash, s.streamCheck(check, i), lbls, i, now, res, resLabels)
			if samp == nil {
				continue
			}
		}
		if value <= bound {
			s.record(samp, value)
		} else {
			// An observation changes the whole histogram POINT, not just the
			// buckets it lands in: the unchanged lower buckets must still be
			// re-emitted so the exported cumulative distribution stays complete.
			// Mark them unexported too, or snapshot's per-bucket never-exported
			// guard would emit ONLY the touched buckets on an idle reset/delete
			// and silently drop the rest (a strict subset of the distribution).
			samp.exported = false
		}
		samp.when = now
	}
}

// streamCheck is bucket i's collision-check hash (the check-side mirror of
// streamHash).
func (s *series) streamCheck(check uint64, bucket int) uint64 {
	if s.kind == kindHistogram {
		return check + s.bucketCheck[bucket]
	}
	return check
}

// observePre is observe for callers with precomputed label accumulators and
// no resource attributes (the internal registry): hot counters skip rehashing
// their fixed label set on every bump.
// observePreHashed is the registry fast path: the bound wrappers bump fixed
// label sets, so the accumulators AND the finalized hash are precomputed at
// construction; a bump pays neither the label rehash nor the avalanche.
func (s *series) observePreHashed(lbls labels, hash, check uint64, value float64, res pcommon.Map) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		s.drops.nan.Add(1)
		return
	}
	if s.kind == kindHistogram {
		// Histograms fold per-bucket labels; take the general path.
		s.observe(lbls, value, resKey{}, res, nil)
		return
	}
	now := s.epoch()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordSingle(hash, check, value, lbls, now, res, nil)
}

// recordSingle folds one observation into a single-stream metric
// (counter/gauge/summary): it admits the sample on first sight and refuses a
// check-hash collision, then records the value. The caller holds s.mu. Shared by
// observe's non-histogram path and the registry's observePreHashed.
func (s *series) recordSingle(hash, check uint64, value float64, lbls labels, now int64, res pcommon.Map, resLabels labels) {
	samp := s.db[hash]
	if samp == nil {
		samp = s.admit(hash, check, lbls, 0, now, res, resLabels)
		if samp == nil {
			return
		}
	} else if samp.check != check {
		s.drops.collision.Add(1)
		if now-s.lastCollision >= 3600 {
			s.lastCollision = now
			s.log.Warn("series hash collision, dropping observation", "metric", s.name)
		}
		return
	}
	s.record(samp, value)
	samp.when = now
}

// baseAccum hashes the caller's data-point labels once (order-independent). For
// a histogram it strips any caller-provided "le" so the synthetic per-bucket one
// is the only contribution folded in per bucket.
func (s *series) baseAccum(lbls labels) (base, check uint64) {
	base, check = lbls.accums()
	if s.kind == kindHistogram {
		if v, ok := lbls.get(leLabel); ok {
			hk, hv := xxhash.Sum64String(leLabel), xxhash.Sum64String(v)
			base -= combineHash(hk, hv)
			check -= combineCheck(hk, hv)
		}
	}
	return base, check
}

// streamHash is the finalized hash of bucket i's series: the base accumulator
// XOR-folded with the bucket's "le" label for a histogram, or just the base.
func (s *series) streamHash(base uint64, bucket int) uint64 {
	if s.kind == kindHistogram {
		return mixHash(base + s.bucketHash[bucket])
	}
	return mixHash(base)
}

// admit inserts a new sample for a previously unseen stream, or returns nil
// (warning at most hourly) when the cardinality cap is reached. It runs only on
// the cold path, so materializing the full label set here is cheap.
func (s *series) admit(hash, check uint64, lbls labels, bucket int, now int64, res pcommon.Map, resLabels labels) *expiringSample {
	full := lbls
	if s.kind == kindHistogram {
		full = lbls.without(leLabel).set(leLabel, s.bucketStr[bucket])
	}
	if s.maxStreams > 0 && len(s.db) >= s.maxStreams {
		s.warnCapped(full, now)
		return nil
	}
	samp := &expiringSample{
		sample: sample{labels: full.String(), resource: resourceString(res, resLabels), bucket: bucket, check: check, initial: true, start: s.streamStart(now)},
		when:   now,
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
// counters, summaries and histograms accumulate.
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
				emit := samp.sample
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
				out = append(out, samp.sample)
			}
			samp.initial = false
			samp.count = 0
			samp.value = 0
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
		out = append(out, samp.sample)
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
