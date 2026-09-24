package metrics

import (
	"iter"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JohanLindvall/haste/xxh3"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

const leLabel = "le"

// drops counts observations the series store REFUSED, for the store that owns
// them. Every rejection path must bump a counter — one of these, or the
// series' own cappedDrops (below): a dropped observation that is only logged
// (at most hourly, per series) is invisible loss.
//
// These used to be PROCESS-GLOBAL atomics, purely to dodge an import cycle with
// obs (which imports this package, so the counters could not live there). The
// cycle dissolves the moment obs registers a getter over the instance, which is
// the pattern six other subsystems here already use — and the globals were not
// free: two DynamicMetricSets in one process silently merged their counts, the
// Registry's (essentially impossible) refusals landed on a metric documented as
// the log-metrics one, and six tests had to do before/after arithmetic to
// isolate themselves from every other test in the package.
//
// The CARDINALITY-CAP refusals are deliberately not here: they are counted per
// series (series.cappedDrops), because a cap refusal is per metric by nature
// and a set-wide count had to be kept beside a mutex-guarded per-name map that
// every refusal on every series of the set serialised on. See
// DynamicMetricSet.DroppedCappedByMetric.
type drops struct {
	nan      atomic.Uint64
	negative atomic.Uint64
	retained atomic.Uint64
}

// NaN counts observations rejected because the extracted value was not
// finite — NaN or +/-Inf alike. (The name predates the Inf arm; both take this
// path, since neither is representable as a sample and both would poison every
// aggregation the series feeds.)
func (d *drops) NaN() uint64 { return d.nan.Load() }

// Negative counts observations rejected because the value was negative on a
// series whose exported form is monotonic (counter, summary). See
// series.refuseNegative for why those two and not the other kinds.
func (d *drops) Negative() uint64 { return d.negative.Load() }

// Retained counts undelivered SAMPLES dropped because the re-offer buffer was
// full — a collector outage longer than maxRetainedSamples/maxRetainedResources
// can hold — or because the collector rejected their chunk permanently. These
// ARE lost observations, and they are the only ones the retention cannot save.
func (d *drops) Retained() uint64 { return d.retained.Load() }

// addRetained counts n dropped undelivered samples.
func (d *drops) addRetained(n uint64) { d.retained.Add(n) }

// seriesKind selects how observations accumulate and how the series exports.
type seriesKind int

const (
	kindCounter   seriesKind = iota // monotonic sum
	kindGauge                       // last value wins
	kindHistogram                   // bucketed distribution
	kindSummary                     // running sum + count
)

// seriesRole names which of this package's two products owns a series. It
// exists for the refusal LINES the two share, and for nothing else: series is
// one storage type serving a Registry of self-telemetry and a
// DynamicMetricSet of log-derived metrics (see the package doc), so a message
// hard-coded to one product's vocabulary sends the other product's operator
// looking for a rule, or a metric, that does not exist.
//
// The two differ on both halves of a refusal. The CAUSE differs: a log-derived
// value is extracted by an operator-written `value`/`valueRegexp` from
// tenant-authored text, so a refusal points at that rule; a self-metric value
// comes from this repo's own code, so a refusal is a BUG here and there is no
// configuration to correct. And the ACCOUNTING differs: the log-derived
// store's drops are published (obs.RegisterLogMetricsDrops), while the
// Registry's deliberately are not — a registry has no cardinality cap and its
// label sets come from code — so naming a kubescrape_log_metrics_dropped_*
// metric on a Registry refusal points the operator at a counter that can never
// move for it.
type seriesRole uint8

const (
	// roleLogMetric is the DynamicMetricSet's: log-derived, values extracted
	// from tenant-authored log content, refusals published.
	roleLogMetric seriesRole = iota
	// roleSelfMetric is the Registry's: this process's own telemetry, values
	// produced by code in this repo, refusals counted but not published.
	roleSelfMetric
)

// what names the kind of observation this role refuses, for a log line.
func (r seriesRole) what() string {
	if r == roleSelfMetric {
		return "self-metric"
	}
	return "log-metric"
}

// dropNote is the remedy a refused observation of this role carries, and the
// half that used to be wrong: it says WHERE the running total is published, so
// a Registry refusal does not send an operator grepping for a
// kubescrape_log_metrics_* series that is flat by construction (and, since
// obs.RegisterLogMetricsDrops registers only when a log-metrics SET exists,
// often absent altogether).
func (r seriesRole) dropNote() string {
	if r == roleSelfMetric {
		return "this is one of this process's OWN metrics, so the value came from code in kubescrape rather than from a logMetrics rule; " +
			"the running total is process-local and deliberately unpublished (a registry has no cardinality cap and its label sets come from code)"
	}
	return "check the rule's value/valueRegexp against the lines it matches; the running total is kubescrape_log_metrics_dropped_nan_total"
}

// negativeNote is dropNote for the negative-value refusal: same split, and for
// the log-derived half it also names the two kinds that legitimately take a
// negative, since choosing one of them is the whole remedy.
func (r seriesRole) negativeNote() string {
	if r == roleSelfMetric {
		return "this is one of this process's OWN metrics: a counter or summary here must never be given a negative value (see RegCounter.Add), so this is a bug in kubescrape and not a configuration error; " +
			"the running total is process-local and deliberately unpublished"
	}
	return "a signed quantity belongs on type: gauge (action: add/sub, or a min/max/avg/sum window) or on type: histogram with bounds covering it; " +
		"the running total is kubescrape_log_metrics_dropped_negative_total"
}

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
	// bound (series.bounds; the +Inf figures are value/
	// count — the sum and total). A histogram keeps ONE sample per label set
	// rather than one per bucket stream: fifteen map entries and fifteen full
	// label strings per label set cost ~15x what a counter does, made
	// maxCardinality count bucket streams instead of the label sets it
	// documents (patched by the maxStreams translation this replaced), and let
	// a partially-admitted family export underflowed cumulative buckets. One
	// entry per label set makes a partial family unrepresentable. Nil for
	// every non-histogram.
	counts []uint64
	// start is the epoch second this stream began accumulating: the admission of
	// the sample (see streamStart), or, for a GAUGE only, the last idle reset
	// that zeroed it — a cumulative kind is never zeroed while it lives (see
	// snapshot). It becomes StartTimeUnixNano on every exported point.
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
	// value starts a fresh window (the windowed gauge actions: min/max/avg/sum/
	// count — see gaugeAction).
	sealed bool
	// final is set only on an EMITTED copy (snapshot's output), never on a
	// stored sample: it says the store no longer holds this value for a later
	// export to re-read — the grace delete unlinked it, a gauge's idle reset
	// zeroed it, or it is the first emission of an aggregation window that the
	// next observation replaces. It is what a failed export decides on
	// (DynamicMetricSet.retain): a final sample has no other copy and is
	// retained; any other is re-read from the store at the next export and is
	// handed back to it instead (series.rearm).
	final bool

	// key is the sample's FULL 128-bit identity. On a stored sample it is what
	// series.find compares (with expiringSample.next chaining the samples whose
	// keys share a low half — together they are why series.db can be a
	// map[uint64] without narrowing identity to 64 bits; see there). An emitted
	// copy carries it too, which is what lets series.rearm find the live sample
	// a failed export read in one probe instead of a walk.
	key xxh3.Uint128
}

type expiringSample struct {
	sample
	when int64 // epoch seconds of the last observation
	// exported reports whether the CURRENT value has already reached an export.
	// The idle and grace-delete branches of snapshot emit a value no export has
	// carried before going quiet on it: maxAge may legally be shorter than the
	// export interval, in which case every observation between two exports
	// would otherwise be observed and dropped without ever being emitted. A
	// FAILED export clears it again (series.rearm), since the value it read
	// never arrived.
	exported bool

	// next chains the samples whose keys share a low half (see sample.key).
	next *expiringSample
}

// series holds the live values of one metric: a set of label combinations,
// each expiring after a period of inactivity and capped in number.
type series struct {
	mu sync.Mutex
	// db is the live samples, BUCKETED BY THE LOW HALF of the 128-bit series
	// key, with the full key kept on the sample and compared on every hit
	// (find) and the rare low-half collision chained through
	// expiringSample.next. Identity is still 128 bits — this is a bucket
	// index, not a narrowing, and no observation can be merged or refused by a
	// collision.
	//
	// The map key is uint64 and NOT the xxh3.Uint128 it indexes because Go
	// specialises map[uint64] to the mapaccess*_fast64 routines — a hash the
	// compiler inlines, no indirect Hasher call and no memequal — while a
	// 16-byte comparable key takes the generic mapaccess1 path. This is the
	// observe path's single map probe, so the difference is the shape of the
	// key and nothing else: measured in isolation over a pointer-valued map,
	// 30ns -> 9ns at one entry and 77ns -> 32ns at ten thousand, and end to
	// end on the fleet-scale observe benchmarks (see fleet_bench_test.go) the
	// whole probe is ~11% of a matched line's CPU.
	//
	// The chain is what makes the narrow bucket honest. Refusing a colliding
	// sample instead — which is what the old two-accumulator design did, on a
	// check hash — would put back a drop that can silently lose a series. The
	// chain cannot lose anything, and it costs a nil test per probe plus 16
	// bytes per sample: the full key (16) and the next pointer (8) on the
	// sample, against the 8 the map key no longer carries. At the 10000-series
	// cap that is 160 KiB for a metric whose samples already hold two
	// identity strings each.
	//
	// len(db) is therefore NOT the series count (a chain of two is one map
	// entry and two series): count is, and it is what the cardinality cap
	// reads.
	//
	// A SERIES is one (resource, label-combination) pair, since observeFold
	// XORs the resource's accumulator into the key, and that holds for every
	// kind: a histogram is one sample carrying its whole per-bucket
	// distribution (sample.counts). The store used to key histograms per
	// bucket STREAM and translate the cap through a derived maxStreams budget;
	// the translation is gone with the layout.
	//
	// The RESOURCE half is what makes the cap a memory bound (the store
	// retains a serialization of the whole resource per entry), and it is also
	// what surprises: one agent-wide set serves every pod on the node, so a
	// rule matching N pods draws its label combinations from ONE pool and the
	// per-pod budget is maxSize/N. Do not "fix" that by dropping the resource
	// from the key — a label-set-only cap is unbounded in resources, which is
	// exactly what cardinalityCap (compile.go) and maxStreamCap defend against.
	db    map[uint64]*expiringSample
	count int
	// cappedDrops counts the observations this series REFUSED because its
	// cardinality cap was reached (warnCapped, under mu). It is per series
	// rather than on the shared drops because a cap refusal is per metric by
	// nature — rules sharing a name share one series, so this IS the per-metric
	// count an operator acts on — and because the set-wide form needed a
	// mutex-guarded per-name map that every refusal on every series of a set
	// serialised on, which is exactly the shape a cardinality blow-up fed from
	// several ingest goroutines takes. Atomic because the export-time reader
	// (DynamicMetricSet.DroppedCappedByMetric) does not take mu.
	cappedDrops atomic.Uint64
	name        string
	desc        string
	kind        seriesKind
	// role selects the vocabulary the refusal lines use; see seriesRole.
	role seriesRole
	// refuseNegative is set for the kinds whose exported form is MONOTONIC, and
	// it is the whole of the negative-value guard (see refuse).
	//
	// A counter renders as an OTLP Sum with IsMonotonic(true) and cumulative
	// temporality, and a summary's sum reaches Prometheus as the counter-typed
	// <name>_sum: on both, a decrease on an unchanged StartTimestamp is a
	// counter RESET that this process never declared, which rate()/increase()
	// reads as one and adds the whole new value on top of everything already
	// counted. There is no reading of "count -500 requests" that recovers, so
	// the observation is refused rather than folded in.
	//
	// NOT set for a gauge (negative is its ordinary range: actionSub, a min/max
	// window over signed values) and NOT for a histogram (its bucket bounds are
	// operator-configured and validated only as INCREASING, so a distribution
	// over signed values is a configuration this package supports on purpose —
	// refusing there would silently delete points from a histogram designed for
	// them). A histogram's _sum inherits the same downstream caveat Prometheus
	// documents for negative observations; that is the operator's declared
	// choice of bounds, not an extraction accident.
	refuseNegative bool

	// drops is the OWNING store's refusal counters (never nil; newSeries fills
	// it in). Per store, not per process: see the type's doc.
	drops *drops
	// now is the clock, in epoch seconds. nil takes the package's coarse clock,
	// which is what production always does; a test injects its own the way
	// store.now does, so there is no process-global override and hence no
	// atomic load per observation paying for one.
	now func() int64

	action     gaugeAction // gauge fold mode; ignored for other kinds
	maxSize    int         // cap on distinct SERIES (config maxCardinality); count is what it reads
	expiration int64       // seconds of inactivity before a combination expires
	lastWarn   int64       // epoch seconds of the last cardinality warning (see hourly)
	// lastNonFinite is the epoch second of the last non-finite-value notice,
	// throttled the same way and for the same reason as lastWarn (see hourly).
	lastNonFinite int64
	// lastNegative is the epoch second of the last negative-value notice,
	// throttled separately from lastNonFinite: the two conditions co-occur on a
	// rule whose extraction is simply wrong, and one shared gate would let
	// whichever fired first silence the other for the hour.
	lastNegative int64
	// log is the logger the refusal lines go to; nil means slog.Default() AT
	// THE CALL (see logger). Never resolved at construction: every Registry
	// series is built in obs's package-level var blocks, before either main
	// calls slog.SetDefault, and a logger captured there is the stdlib bridge —
	// its lines come out as `level=INFO msg="WARN ..."` with every attribute
	// flattened into the message, or not at all under a Warn-level handler.
	log *slog.Logger

	// created is the epoch second this series was built, on its own clock. It
	// floors a counter's declared start (streamStart): the backdate is a claim
	// that the counter was zero for three minutes before its first observation,
	// which is true only of time THIS process could have observed — see
	// counterBaselineSeconds.
	created int64

	// bounds are a histogram's FINITE bucket bounds — what sample.counts
	// indexes — and nil for every other kind. The +Inf bucket is not stored:
	// its figures are the sample's own value/count (the sum and the total),
	// and every reader wanted the finite bounds alone.
	bounds []float64
}

// seriesSpec configures a new series.
type seriesSpec struct {
	name, desc string
	kind       seriesKind
	// role selects the refusal lines' vocabulary. Both doors state it
	// explicitly — compile.go passes roleLogMetric and registry.go
	// roleSelfMetric — but roleLogMetric IS the zero value, so omitting it at
	// compile.go changes nothing and only the Registry's door can regress
	// silently. That door is pinned end to end by
	// TestRegistryRefusalSpeaksTheSelfMetricVocabulary, which goes through
	// Registry.Counter rather than building a series by hand.
	role       seriesRole
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
	dr := spec.drops
	if dr == nil {
		dr = &drops{}
	}
	if spec.now == nil {
		// Only a series that will actually read the coarse clock starts it.
		startEpochClock()
	}
	s := &series{
		db:             make(map[uint64]*expiringSample),
		drops:          dr,
		now:            spec.now,
		name:           spec.name,
		desc:           spec.desc,
		kind:           spec.kind,
		role:           spec.role,
		refuseNegative: spec.kind == kindCounter || spec.kind == kindSummary,
		action:         spec.action,
		maxSize:        spec.maxSize,
		expiration:     expirationSeconds(spec.expiration),
		log:            spec.log,
	}
	s.created = s.epoch()
	if spec.kind == kindHistogram {
		s.bounds = slices.Clone(effectiveBuckets(spec.buckets))
	}
	return s
}

// logger is the series' logger, resolved at the CALL so a process that
// installs its handler after building its series (every Registry series is
// built at package init) still gets its refusal lines in that handler.
func (s *series) logger() *slog.Logger {
	if s.log != nil {
		return s.log
	}
	return slog.Default()
}

// effectiveBuckets is the ONE spelling of "no buckets configured means
// defaultBuckets", for a histogram's configured bounds: newSeries stores it,
// sameBuckets compares against it, and compileRule sizes the bucket-slot
// budget from it.
func effectiveBuckets(b []float64) []float64 {
	if len(b) == 0 {
		return defaultBuckets
	}
	return b
}

// sameBuckets reports whether a fresh registration of this kind with these
// bounds would produce the buckets this series already has. Only a histogram
// has any (newSeries ignores the field otherwise), and the comparison is
// against the NORMALIZED form (effectiveBuckets).
func (s *series) sameBuckets(kind seriesKind, buckets []float64) bool {
	if kind != kindHistogram {
		return true
	}
	return slices.Equal(s.bounds, effectiveBuckets(buckets))
}

// epoch reads the series' clock: the injected one in tests, the process's
// coarse ten-second clock otherwise.
func (s *series) epoch() int64 {
	if s.now != nil {
		return s.now()
	}
	return coarseEpoch()
}

// refusalOrigin says where a refused value came from, for the refusal line's
// remedy — the one part of the line that differs by source.
type refusalOrigin uint8

const (
	// originFeed is the series' own feed: a logMetrics rule's
	// value/valueRegexp for a log-derived series, code for a Registry one.
	originFeed refusalOrigin = iota
	// originScript is a transform script's emit_metric (EmitDirect), whose
	// value the script computed — hostobj hands a Starlark float through with
	// no finiteness or sign check — so a remedy naming "the rule's
	// value/valueRegexp" sent the operator to a source that played no part.
	originScript
)

// refuse reports whether value must not be admitted, counting and (at most
// hourly) naming the reason. It is the ONE value guard, shared by every
// observe door so they cannot drift — the per-line path and the registry's
// pre-hashed path used to spell the non-finite half separately, and
// EmitDirect checks through it with its own origin.
//
// Order matters: -Inf is negative AND non-finite, and it is reported as
// non-finite, which is the sharper diagnosis (the extraction produced a value
// that is not a number at all, rather than one with the wrong sign).
//
// The warm path is two predictable branches over a float already in a register
// and one bool field read; the counting and the log line live in the note*
// helpers, off this path. The allocation budgets in bench_test.go observe
// finite non-negative values and reach neither.
func (s *series) refuse(value float64, from refusalOrigin) bool {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		// Inf too, and for the same reason: ParseFloat accepts "inf"/"Infinity"
		// from a log line, and Inf is ABSORBING under every accumulate path —
		// one such observation pins a counter, summary or histogram sum at Inf
		// for the whole maxAge (24h by default), which no later real value can
		// undo. Counted, never admitted.
		//
		// The counter alone (kubescrape_log_metrics_dropped_nan_total) says the
		// extraction is producing garbage but not WHICH rule's, and a set holds
		// every metric on the node — so the operator had a rising number and no
		// way to reach the `value`/`valueRegexp` that produced it. The line is
		// the context the counter cannot carry; the counter stays the rate.
		s.noteRefused(&s.drops.nan, &s.lastNonFinite, value, "non-finite",
			"the value is NaN or Inf, which would poison every aggregate this metric feeds",
			s.dropNote(from))
		return true
	}
	if value < 0 && s.refuseNegative {
		// The counter says the rate; only the line can reach the rule. It is a
		// WARN and not a Debug because the alternative to refusing was a SILENT
		// lie: the value folded in, the exported cumulative sum went DOWN on an
		// unchanged StartTimestamp, and every rate() over it read an undeclared
		// counter reset and added the new total on top of the old one. Nothing
		// downstream can detect that, so the refusal is the only place it can
		// ever be reported.
		s.noteRefused(&s.drops.negative, &s.lastNegative, value, "negative",
			"this metric exports a MONOTONIC sum, and folding a decrease into it on an unchanged start timestamp is a counter reset that rate() would read as one and add on top of everything already counted",
			s.negativeNote(from))
		return true
	}
	return false
}

// dropNote is the remedy a refused non-finite value carries: the role's,
// unless a transform script supplied the value.
func (s *series) dropNote(from refusalOrigin) string {
	if from == originScript {
		return "the value was passed to a transform script's emit_metric(...), not extracted by a rule, so check what the script computes for this metric; " +
			"the running total is kubescrape_log_metrics_dropped_nan_total"
	}
	return s.role.dropNote()
}

// negativeNote is dropNote for the negative-value refusal.
func (s *series) negativeNote(from refusalOrigin) string {
	if from == originScript {
		return "the value was passed to a transform script's emit_metric(...), not extracted by a rule; a signed quantity belongs on a metric declared " +
			"type: gauge or type: histogram with bounds covering it; the running total is kubescrape_log_metrics_dropped_negative_total"
	}
	return s.role.negativeNote()
}

// observe records value for the given data-point label set, resource, and extra
// resource labels. The series is keyed by all three together (their hashes
// XOR-fold into the base accumulator), so per-resource series are distinct.
//
// This is the door for a caller holding only the map and its accumulator (the
// Registry's func gauges, EmitDirect, tests). The per-line path comes through
// observeFold with the resource's identity already resolved; passing it here
// would key the same series, so the difference is cost and not correctness —
// see resourceFold.
func (s *series) observe(lbls labels, value float64, resAccum xxh3.Uint128, res pcommon.Map, resLabels labels) {
	s.observeFold(lbls, value, resourceFold{res: res, accum: resAccum}, resLabels)
}

// observeFold is observe against a resource whose derivations the caller
// resolved once (Bind). A histogram is ONE sample per label set — the value
// folds into its sum/count and every counts slot whose bound it does not
// exceed — so every kind is a single hash and a single map probe per
// observation.
func (s *series) observeFold(lbls labels, value float64, res resourceFold, resLabels labels) {
	if s.refuse(value, originFeed) {
		return
	}
	now := s.epoch()
	// Order-independent. A histogram's caller-supplied "le" is refused at the
	// doors it can arrive through (rejectHistogramLe, EmitDirect), so the hot
	// path never probes for it.
	base := lbls.hashAccum()
	rk := res.accum
	if len(resLabels) > 0 {
		// Only a resource label can override a resource key, so only a rule
		// declaring one pays the cancel — or reaches for the identity it
		// resolves against. XOR is associative, so folding it in here rather
		// than into a combined resource term is the same bits.
		rk = xor128(rk, resLabelsAccum(res.identity(), resLabels))
	}
	base = xor128(base, rk)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordSingle(mixHash(base), value, lbls, now, res.res, resLabels)
}

// observePreHashed is the registry fast path: the bound wrappers bump fixed
// label sets, so the accumulators AND the finalized hash are precomputed at
// construction; a bump pays neither the label rehash nor the avalanche.
func (s *series) observePreHashed(lbls labels, hash xxh3.Uint128, value float64, res pcommon.Map) {
	if s.refuse(value, originFeed) {
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
	samp := s.find(hash)
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
	if s.find(hash) != nil {
		return
	}
	s.admit(hash, lbls, now, emptyResource, nil)
}

// admit inserts a new sample for a previously unseen label combination, or
// returns nil (warning at most hourly) when the cardinality cap is reached. It
// runs only on the cold path, so serializing the label set here is cheap.
func (s *series) admit(hash xxh3.Uint128, lbls labels, now int64, res pcommon.Map, resLabels labels) *expiringSample {
	if s.maxSize > 0 && s.count >= s.maxSize {
		s.warnCapped(lbls, now, res, resLabels)
		return nil
	}
	samp := &expiringSample{
		sample: sample{labels: lbls.String(), resource: resourceString(res, resLabels), initial: true, start: s.streamStart(now)},
		when:   now,
	}
	if s.kind == kindHistogram {
		samp.counts = make([]uint64, len(s.bounds))
	}
	s.link(samp, hash)
	return samp
}

// link inserts samp under its full key, at the head of the bucket its low half
// indexes. The caller holds s.mu.
func (s *series) link(samp *expiringSample, hash xxh3.Uint128) {
	samp.key = hash
	samp.next = s.db[hash.Lo]
	s.db[hash.Lo] = samp
	s.count++
}

// find returns the sample with this exact 128-bit key, or nil. The chain is a
// single element for every key a real deployment will ever hold — a low-half
// collision needs two of the 10000 permitted label combinations to agree on 64
// hash bits — so this is one fast64 probe and one 16-byte compare.
func (s *series) find(hash xxh3.Uint128) *expiringSample {
	samp := s.db[hash.Lo]
	for samp != nil && samp.key != hash {
		samp = samp.next
	}
	return samp
}

// unlink removes samp from its bucket chain and decrements the series count.
// The caller holds s.mu and must not use samp.next afterwards.
func (s *series) unlink(samp *expiringSample) {
	head := s.db[samp.key.Lo]
	if head == samp {
		if samp.next == nil {
			delete(s.db, samp.key.Lo)
		} else {
			s.db[samp.key.Lo] = samp.next
		}
	} else {
		for p := head; p != nil; p = p.next {
			if p.next == samp {
				p.next = samp.next
				break
			}
		}
	}
	samp.next = nil
	s.count--
}

// all iterates every live sample, walking the collision chains. Cold paths
// only (export, dump, the failed-export re-arm); the observe path goes through
// find.
func (s *series) all() iter.Seq[*expiringSample] {
	return func(yield func(*expiringSample) bool) {
		for _, head := range s.db {
			// next is read BEFORE the yield: snapshot unlinks the sample it is
			// looking at (the expiry delete), and unlink clears its next
			// pointer, so reading it afterwards would end the bucket's walk at
			// the first expired sample and silently skip the rest of its chain.
			for samp := head; samp != nil; {
				next := samp.next
				if !yield(samp) {
					return
				}
				samp = next
			}
		}
	}
}

// counterBaselineSeconds backdates a COUNTER stream's declared start so that
// the two synthetic zero points renderNumber emits ahead of a series' first
// real value (one and two minutes before it — see there) both fall strictly
// after it. A point whose start equals its own timestamp encodes a reset, so
// stamping the zeros with their own timestamp would put the very defect this
// field exists to remove back on the one point that is easiest to get wrong.
// Three minutes is the two-minute backdate plus one more step of headroom.
//
// The claim the backdate makes — "this counter was zero for those three
// minutes" — is true only of time THIS PROCESS was there to observe, so
// streamStart floors it at the series' construction. Without the floor it was
// false across every restart: the series identity survives one (a self-metric's
// instance is the node, a log-derived counter is keyed by the pod it describes)
// and a rolling update replaces a pod in well under a minute, so the new
// process's synthetic zeros — and the start they carry — landed BEFORE the old
// process's final samples of the same series. A backend without an
// out-of-order window refuses such a point (Prometheus refuses the whole
// request carrying it); one with a window stores a zero between two old
// samples, a fake reset that makes increase() count the pre-restart total a
// second time. renderNumber drops whichever zero the floor puts at or before
// the start.
const counterBaselineSeconds = 3 * 60

// streamStart is the start-of-accumulation stamp a stream admitted at now
// should carry. A gauge's idle reset stamps now itself (see snapshot); a
// cumulative kind is never reset while it lives.
func (s *series) streamStart(now int64) int64 {
	if s.kind == kindCounter {
		return max(now-counterBaselineSeconds, s.created)
	}
	return now
}

// warnCapped counts the refused observation and logs the cardinality cap at
// most hourly (caller holds the lock).
//
// The RESOURCE is on the line because the cap counts (resource, label-set)
// pairs: without it the message reads "max label count reached … labels=
// {path=\"/a\"} maxsize=10000" — ONE label set against a cap of ten thousand,
// which is self-contradictory on its face and leaves the operator unable to
// tell WHICH pod was refused, or that pods are what consumed the budget at
// all. Both strings are materialized only on the hourly branch, so the refusal
// path itself stays as cheap as it was.
func (s *series) warnCapped(lbls labels, now int64, res pcommon.Map, resLabels labels) {
	s.cappedDrops.Add(1)
	if hourly(&s.lastWarn, now) {
		// WARN, not Info: observations are being DROPPED and the cap frees
		// slots only through idleness, so the metric is blind for maxAge plus
		// the grace window (24h by default). Info is for lifecycle an operator
		// reads without asking; a refusal is the definition of a Warn here.
		s.logger().Warn("max series count reached for log metric; further label combinations are refused until existing ones idle out",
			"metric", s.name, "labels", lbls.String(), "resource", resourceString(res, resLabels),
			"series", s.count, "maxSeries", s.maxSize)
	}
}

// hourly reports whether a refusal line throttled on *last may be written at
// now, and claims the hour when it may (the caller holds the owning series'
// mu). The three refusal lines — the cardinality cap, a non-finite value, a
// negative one — each keep their OWN stamp: they co-occur on a rule whose
// extraction is simply wrong, and one shared gate would let whichever fired
// first silence the others for the hour.
//
// Hand-rolled rather than internal/logdedupe because this package's clock is
// INJECTABLE (series.now, the store.now pattern) and logdedupe reads time.Now
// directly: a throttle a test cannot step past would make these lines the only
// untestable behaviour in the file.
func hourly(last *int64, now int64) bool {
	if now-*last < 3600 {
		return false
	}
	*last = now
	return true
}

// noteRefused counts a refused observation on n and names the metric at most
// hourly (throttled on *last): "dropping a <adjective> <role> observation;
// <why>", with the value, the running total and the remedy.
//
// The lock is taken only on this branch (refuse's caller has not taken it yet,
// and an admissible observation never comes here), so the warm path is
// untouched — the allocation budgets in bench_test.go observe finite
// non-negative values and never reach it. The message is assembled only on
// the hourly branch, so a rule refusing every line pays one atomic add and one
// lock per line, not a string build. It throttles per SERIES because a
// workload feeding a bad value does it on every line, and one sweep goroutine
// serves every log file on the node.
//
// dropped is the owning store's total (the whole set's, for a log-derived
// metric), not this metric's: the per-metric
// breakdown exists only for the cardinality cap (series.cappedDrops), and a
// second per-metric counter on a refusal path would be one more write per bad
// line for a number the line itself already localises.
func (s *series) noteRefused(n *atomic.Uint64, last *int64, value float64, adjective, why, note string) {
	n.Add(1)
	now := s.epoch()
	s.mu.Lock()
	warn := hourly(last, now)
	s.mu.Unlock()
	if !warn {
		return
	}
	s.logger().Warn("dropping a "+adjective+" "+s.role.what()+" observation; "+why,
		"metric", s.name, "value", strconv.FormatFloat(value, 'g', -1, 64),
		"dropped", n.Load(), "note", note)
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
		for i, bound := range s.bounds {
			if value <= bound {
				samp.counts[i]++
			}
		}
	}
}

// emit copies a sample out for a snapshot's caller. The value copy alone is
// not enough for a histogram: counts aliases the live per-bucket array, which
// keeps counting after s.mu is released, and export retention legitimately
// holds emitted samples across intervals.
func (samp *expiringSample) emit() sample {
	out := samp.sample
	if out.counts != nil {
		out.counts = slices.Clone(out.counts)
	}
	return out
}

// emitFinal is emit for a value the store is about to stop holding (see
// sample.final).
func (samp *expiringSample) emitFinal() sample {
	out := samp.emit()
	out.final = true
	return out
}

// snapshot returns the live samples. A combination idle past its expiration
// stops being exported, and is deleted after a further four-minute grace
// period; a GAUGE is also zeroed when it goes idle.
func (s *series) snapshot() []sample {
	now := s.epoch()
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]sample, 0, s.count)
	for samp := range s.all() {
		idle := now - samp.when - s.expiration
		if idle >= 4*60 {
			// Deleting the sample: emit it first if this value never reached an
			// export. With the export interval past maxAge+grace (both legal and
			// unclamped) a sample observed just after one export is deleted at
			// the next, unseen — the same never-exported loss the idle branch
			// below guards against, one branch up. Aggregating gauges emit
			// their windowed aggregate (as the aggregating branch does); a value
			// observed once, then idled straight past the grace before any
			// snapshot ran the aggregating branch, is otherwise destroyed unseen.
			if !samp.exported {
				emit := samp.emitFinal()
				if s.aggregating() {
					emit.value = s.aggregateValue(&samp.sample)
				}
				out = append(out, emit)
			}
			s.unlink(samp)
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
			// Only a window's FIRST emission is final: the next observation
			// replaces it, so the store may never hold it again. A re-emission
			// of a sealed window the store still holds is not, and a failed
			// export of it needs nothing kept — the first emission already was.
			emit.final = !samp.sealed
			out = append(out, emit)
			samp.initial = false
			samp.sealed = true
			samp.exported = true
			continue
		}
		if idle > 0 {
			// Idle past its expiration: stop exporting it. But emit it first if
			// this value has never been exported — with maxAge below the export
			// interval, the observation would otherwise leave the export stream
			// having never left the process.
			if s.kind != kindGauge {
				// A CUMULATIVE kind keeps its value, its count and its start.
				// It used to be zeroed here, with the start moved, and the zero
				// never sent (it was marked exported) — so a series re-observed
				// inside the grace window went from its old total straight to
				// the new small one under a new start. Start-aware consumers read
				// that as a reset; the Prometheus-lineage backends this ships
				// into discard StartTimeUnixNano (see renderNumber) and read it
				// as the counter running backwards, so increase()/rate()
				// undercounted by the whole old total whenever the new total
				// caught up with it. A gap followed by the same cumulative
				// stream is valid OTLP for both. The grace delete, followed by
				// a fresh admit with its baseline zeros, stays the only real
				// reset of a cumulative stream.
				if !samp.exported {
					out = append(out, samp.emit())
				}
				samp.initial = false
				samp.exported = true
				continue
			}
			// A gauge is zeroed so a later re-appearance of a running
			// (inc/dec/add/sub) gauge starts from nothing; its emit is final,
			// since the value is gone from the store once this branch ends.
			if !samp.exported {
				out = append(out, samp.emitFinal())
			}
			samp.initial = false
			samp.count = 0
			samp.value = 0
			// The one place a live sample's accumulation restarts, so the one
			// place its start moves — to NOW. streamStart's backdate exists
			// only for a counter's synthetic zeros, and a gauge renders none.
			samp.start = now
			samp.exported = true // the zero needs no further emission
			continue
		}
		out = append(out, samp.emit())
		samp.initial = false
		samp.exported = true
	}
	return out
}

// rearm hands a FAILED export's claims back to the live samples it read.
// snapshot consumes two flags optimistically — `initial` (the counter baseline
// zeros ride on the first export) and `exported` (the idle and grace-delete
// branches emit only a value no export has carried) — and an export that never
// arrived must give both back, or the next one skips the baseline and a series
// that goes quiet before a delivery is never emitted again at all.
//
// Only a NON-final sample is re-armed: a final one is no longer in the store
// (the caller retains it instead — DynamicMetricSet.retain). An aggregating
// series needs nothing: its window's FIRST emission is final and retained, and
// clearing `exported` on a re-emission would have the grace delete emit that
// same window a second time. Failure-path only; one probe per sample.
func (s *series) rearm(samples []sample) {
	if s.aggregating() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range samples {
		if samples[i].final {
			continue
		}
		if e := s.find(samples[i].key); e != nil {
			e.exported = false
			if samples[i].initial {
				e.initial = true
			}
		}
	}
}
