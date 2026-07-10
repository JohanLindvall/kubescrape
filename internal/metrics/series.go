package metrics

import (
	"log/slog"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

const leLabel = "le"

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
)

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
	initial  bool
}

type expiringSample struct {
	sample
	when int64 // epoch seconds of the last observation
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
	buckets    []float64
	bucketStr  []string
	bucketHash []uint64
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
	for i, bs := range s.bucketStr {
		s.bucketHash[i] = combineHash(leHash, xxhash.Sum64String(bs))
	}
}

// observe records value for the given data-point label set, resource, and extra
// resource labels. The series is keyed by all three together (their hashes
// XOR-fold into the base accumulator), so per-resource series are distinct. For
// a histogram the value is counted into every bucket whose bound it does not
// exceed.
func (s *series) observe(lbls labels, value float64, resAccum uint64, res pcommon.Map, resLabels labels) {
	now := loadEpoch()
	base := s.baseAccum(lbls) ^ resAccum ^ resLabels.hashAccum()

	s.mu.Lock()
	defer s.mu.Unlock()
	for i, bound := range s.buckets {
		hash := s.streamHash(base, i)
		samp := s.db[hash]
		if samp == nil {
			samp = s.admit(hash, lbls, i, now, res, resLabels)
			if samp == nil {
				continue // capped
			}
		}
		if value <= bound {
			s.record(samp, value)
		}
		samp.when = now
	}
}

// baseAccum hashes the caller's data-point labels once (order-independent). For
// a histogram it strips any caller-provided "le" so the synthetic per-bucket one
// is the only contribution folded in per bucket.
func (s *series) baseAccum(lbls labels) uint64 {
	base := lbls.hashAccum()
	if s.kind == kindHistogram {
		if v, ok := lbls.get(leLabel); ok {
			base ^= combineHash(xxhash.Sum64String(leLabel), xxhash.Sum64String(v))
		}
	}
	return base
}

// streamHash is the finalized hash of bucket i's series: the base accumulator
// XOR-folded with the bucket's "le" label for a histogram, or just the base.
func (s *series) streamHash(base uint64, bucket int) uint64 {
	if s.kind == kindHistogram {
		return mixHash(base ^ s.bucketHash[bucket])
	}
	return mixHash(base)
}

// admit inserts a new sample for a previously unseen stream, or returns nil
// (warning at most hourly) when the cardinality cap is reached. It runs only on
// the cold path, so materializing the full label set here is cheap.
func (s *series) admit(hash uint64, lbls labels, bucket int, now int64, res pcommon.Map, resLabels labels) *expiringSample {
	full := lbls
	if s.kind == kindHistogram {
		full = lbls.without(leLabel).set(leLabel, s.bucketStr[bucket])
	}
	if s.maxSize > 0 && len(s.db) >= s.maxSize {
		if now-s.lastWarn >= 3600 {
			s.lastWarn = now
			s.log.Info("max label count reached for log metric",
				"metric", s.name, "labels", full.String(), "maxsize", s.maxSize)
		}
		return nil
	}
	samp := &expiringSample{
		sample: sample{labels: full.String(), resource: resourceString(res, resLabels), bucket: bucket, initial: true},
		when:   now,
	}
	s.db[hash] = samp
	return samp
}

// record folds one observation into a sample. Gauges apply their action;
// counters, summaries and histograms accumulate.
func (s *series) record(samp *expiringSample, value float64) {
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
		switch idle := now - samp.when - s.expiration; {
		case idle >= 4*60:
			delete(s.db, hash)
		case idle > 0:
			samp.initial = false
			samp.count = 0
			samp.value = 0
		default:
			out = append(out, samp.sample)
			samp.initial = false
		}
	}
	return out
}
