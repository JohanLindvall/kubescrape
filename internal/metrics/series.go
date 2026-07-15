package metrics

import (
	"log/slog"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

const leLabel = "le"

// Process-wide counts of observations the series store REFUSED. They are
// exported through obs (which imports this package, so the counters cannot live
// there) as kubescrape_log_metrics_dropped_*. Every rejection path must bump
// one of them: a dropped observation that is only logged (at most hourly, per
// series) is invisible loss.
var (
	droppedCapped    atomic.Uint64
	droppedCollision atomic.Uint64
	droppedNaN       atomic.Uint64
)

// DroppedCapped counts observations rejected because the series' label-set
// cardinality cap was reached (a new label combination could not be admitted).
func DroppedCapped() uint64 { return droppedCapped.Load() }

// DroppedCollision counts observations rejected because their hash matched an
// existing sample of a DIFFERENT series (a 64-bit collision; merging them would
// corrupt both).
func DroppedCollision() uint64 { return droppedCollision.Load() }

// DroppedNaN counts observations rejected because the extracted value was NaN.
func DroppedNaN() uint64 { return droppedNaN.Load() }

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
	// (aggregating() tests action >= actionMin). value/value2/count hold the
	// per-action running state (see record); snapshot renders the aggregate.
	actionMin    // window minimum (value)
	actionMax    // window maximum (value)
	actionAvg    // window mean (value = running sum, count = n)
	actionFirst  // first value of the window (value)
	actionSum    // window total (value = running sum)
	actionCount  // number of matching lines in the window (count; value ignored)
	actionStddev // window std deviation (value = running mean, value2 = M2; Welford)
	actionRange  // window max − min (value = min, value2 = max)
	actionDelta  // window last − first (value = first, value2 = last)
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
	case actionStddev:
		if samp.count == 0 {
			return 0
		}
		return math.Sqrt(samp.value2 / n)
	case actionRange, actionDelta:
		return samp.value2 - samp.value // max−min / last−first
	default: // min, max, first, sum
		return samp.value
	}
}

// sample is one (resource, label combination) live value. labels is the
// serialized data-point label set and resource the serialized resource-attribute
// set (both via labels.String); bucket indexes into series.buckets for
// histograms.
type sample struct {
	value    float64
	value2   float64 // aggregation-specific second accumulator (see gaugeAction)
	labels   string
	resource string
	bucket   int
	count    uint64
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

	action     gaugeAction // gauge fold mode; ignored for other kinds
	maxSize    int
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
}

var defaultBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10}

func newSeries(spec seriesSpec) *series {
	log := spec.log
	if log == nil {
		log = slog.Default()
	}
	s := &series{
		db:         make(map[uint64]*expiringSample),
		name:       spec.name,
		desc:       spec.desc,
		kind:       spec.kind,
		action:     spec.action,
		maxSize:    spec.maxSize,
		expiration: int64(math.Ceil(spec.expiration.Seconds())),
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
	return s
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
	if math.IsNaN(value) {
		droppedNaN.Add(1)
		return // NaN records into nothing; admitting it would emit fabricated zeros
	}
	now := loadEpoch()
	base, check := s.baseAccum(lbls)
	rl := resLabelsAccum(res, resLabels)
	base += resAccum.accum + rl.accum
	check += resAccum.check + rl.check

	s.mu.Lock()
	defer s.mu.Unlock()
	// Pre-pass over every bucket stream: histogram admission is all-or-nothing
	// (a partial set of streams exports underflowed cumulative counts), and a
	// check-hash mismatch anywhere drops the WHOLE observation for the same
	// reason — a mid-loop skip would leave sibling buckets recording.
	{
		missing := 0
		for i := range s.buckets {
			samp := s.db[s.streamHash(base, i)]
			if samp == nil {
				missing++
				continue
			}
			if samp.check != s.streamCheck(check, i) {
				// 64-bit collision between distinct series (~2^-64 per
				// pair): refuse to merge.
				droppedCollision.Add(1)
				if now-s.lastCollision >= 3600 {
					s.lastCollision = now
					s.log.Warn("series hash collision, dropping observation", "metric", s.name)
				}
				return
			}
		}
		if s.maxSize > 0 && missing > 0 && len(s.db)+missing > s.maxSize {
			s.warnCapped(lbls, now)
			return
		}
	}
	for i, bound := range s.buckets {
		hash := s.streamHash(base, i)
		samp := s.db[hash]
		if samp == nil {
			samp = s.admit(hash, s.streamCheck(check, i), lbls, i, now, res, resLabels)
			if samp == nil {
				continue // capped (single-bucket kinds only; histograms pre-checked)
			}
		}
		if value <= bound {
			s.record(samp, value)
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
	if math.IsNaN(value) {
		droppedNaN.Add(1)
		return
	}
	if s.kind == kindHistogram {
		// Histograms fold per-bucket labels; take the general path.
		s.observe(lbls, value, resKey{}, res, nil)
		return
	}
	now := loadEpoch()
	s.mu.Lock()
	defer s.mu.Unlock()
	samp := s.db[hash]
	if samp == nil {
		samp = s.admit(hash, check, lbls, 0, now, res, nil)
		if samp == nil {
			return
		}
	} else if samp.check != check {
		droppedCollision.Add(1)
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
	if s.maxSize > 0 && len(s.db) >= s.maxSize {
		s.warnCapped(full, now)
		return nil
	}
	samp := &expiringSample{
		sample: sample{labels: full.String(), resource: resourceString(res, resLabels), bucket: bucket, check: check, initial: true},
		when:   now,
	}
	s.db[hash] = samp
	return samp
}

// warnCapped counts the refused observation and logs the cardinality cap at
// most hourly (caller holds the lock).
func (s *series) warnCapped(lbls labels, now int64) {
	droppedCapped.Add(1)
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
		// window; the rest fold in. value2 seeds to value (correct for range's
		// max and delta's last); stddev needs value² instead.
		if samp.sealed || samp.count == 0 {
			samp.sealed = false
			samp.value = value
			samp.value2 = value
			if s.action == actionStddev {
				samp.value2 = 0 // M2 of a single-value window
			}
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
		case actionStddev:
			// Welford's update: the naive E[x²]−E[x]² form cancels
			// catastrophically for large values with small spread (values
			// ~1e9 reported stddev 0).
			n := float64(samp.count + 1)
			delta := value - samp.value
			samp.value += delta / n
			samp.value2 += delta * (value - samp.value)
		case actionRange:
			if value < samp.value {
				samp.value = value
			}
			if value > samp.value2 {
				samp.value2 = value
			}
		case actionDelta:
			samp.value2 = value // last (first stays in value)
		case actionFirst, actionCount:
			// first: keep the window's first value; count: only the tally matters
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
	now := loadEpoch()
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
			// next observed value starts a fresh one.
			emit := samp.sample
			emit.value = s.aggregateValue(&samp.sample)
			out = append(out, emit)
			samp.initial = false
			samp.sealed = true
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
			samp.exported = true // the zero needs no further emission
			continue
		}
		out = append(out, samp.sample)
		samp.initial = false
		samp.exported = true
	}
	return out
}
