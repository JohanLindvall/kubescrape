// Package tailbuffer is the BUFFERING/DECISION half of tail sampling: the layer
// that holds the spans arriving for a trace, calls the policy engine once the
// trace has had long enough to arrive, and then exports the whole trace or
// discards it. agent/tailsample is the other half — the pure verdict logic,
// which holds nothing — and Evaluator.Decide is the only seam between them.
//
// It runs on the trace tier's OWNING shard, which is the one place in this
// system where every span of a trace is in one process: applications push to the
// tier's Service, the receiving shard re-shards each span by trace id
// (agent/servicegraph), and the owner therefore sees the whole trace. Tail
// sampling anywhere else would judge a fragment.
//
// # The delivery contract: a buffered span is ACKED, not durable
//
// This layer breaks the ack-gated at-least-once rule the rest of this repo
// honours, on purpose, and that is the first thing to know about it.
//
// Everywhere else a payload is acked only once the collector has taken it — the
// tailer commits offsets after an export, the events reader writes its position
// after an export, the re-shard hop fails the application's push when the next
// hop fails. Here the ack necessarily comes FIRST: a decision cannot be made
// until the decision window has elapsed, and holding the sender's RPC open for
// five seconds would pin one of the receiver's in-flight slots
// (-ingest-max-in-flight) for the whole window, so a shard would stall every
// application in the cluster long before it ran out of memory. The buffer
// therefore returns success as soon as it has copied the spans, and from that
// moment the sender no longer holds them.
//
// The consequence, stated plainly: SPANS BUFFERED BUT NOT YET DECIDED ARE LOST
// IF THE SHARD DIES. Not delayed — lost. Nothing on disk, nothing to replay, no
// sender still holding a copy. The exposure is bounded in three ways and is
// visible before it is spent:
//
//   - by TIME: at most decisionWait (5s by default) of received spans;
//   - by SIZE: at most maxSpans of them, whatever the rate;
//   - by SHUTDOWN: a graceful stop (SIGTERM, a rolling update, an eviction)
//     calls Flush, which decides every buffered trace immediately and exports
//     the keeps before the exporter closes. Only a hard kill — SIGKILL, an OOM,
//     a node failure — loses anything.
//
// The likeliest hard kill is the OOM this buffer's own bounds cause, which is
// why maxSpans is sized against the container's memory limit rather than left to
// a number in a values file (see applyMemoryBudget).
//
// kubescrape_tail_sampling_buffered_spans and _buffered_traces are exactly the
// number a hard kill would lose at that instant, which is why they are gauges
// rather than a footnote.
//
// A trace that is DECIDED is a different case, and the answer depends on
// whether the workload runs a disk buffer:
//
//   - With -buffer-dir, a decided keep is SPOOLED. The payload is marked
//     otlpexport.Own — the seam's way of saying "this one is ours now" — and
//     otlpexport.Buffered appends it to a durable traces queue instead of
//     passing it through to the collector, exactly as it does for logs and
//     metrics. A collector outage is then a backlog that survives a restart,
//     not loss, and {outcome="lost"} moves only when the SPOOL itself refuses
//     the payload (full, or a failed fsync).
//   - Without one, it is retried a few times and then dropped and counted
//     (kubescrape_tail_sampling_spans_total{outcome="lost"}), because at that
//     point nobody else holds it either.
//
// Ownership is per PAYLOAD rather than a switch on the traces signal, because
// the same exporter carries plain forwarded traces from the tier's application
// listener — there the pushing SDK still holds the spans and its retry is the
// durability, so spooling would ack a sender for data that has not shipped and
// remove the only other copy. otlpexport/owned.go argues the whole distinction.
//
// The one place the sender can still help is a span arriving for an
// already-decided trace (see the decision cache below): those ride out on the
// receiving goroutine, and a failed send there DOES fail the push, so the
// sender's retry recovers them.
//
// Spooling at BUFFER time rather than at decision time — writing every span to
// disk as it arrives, so a hard kill lost nothing at all — was considered and
// rejected. It would put an fsync in the ack path the ack-first design exists to
// keep out (the in-flight-slot argument comes straight back, in a slower form),
// it writes 100% of received spans to disk to protect the ~1-10% a sampler
// keeps, and the queue is a FIFO with an in-order cursor: a payload could not
// retire until every trace in it was decided, and a restart would replay the
// undecided prefix and re-buffer traces that had already been exported. The
// bounded exposure below buys more than that costs.
//
// # Composition: tail sampling sits at the BOTTOM
//
// The owner chain is
//
//	pair the edge -> RED metrics -> head sample -> TAIL SAMPLE -> export
//
// and the order is not a preference. The service-graph pair tap and the
// span-metrics tap count REQUESTS; deriving them from the sampled subset would
// report a tenth of the traffic on series whose entire purpose is saying how
// much there is. Both therefore see 100% of spans, and this layer is the last
// thing above the exporter.
//
// The head sampler (agent/tracesample) runs ABOVE it, and the two NEST rather
// than compound: both hash the trace id with the same unsalted xxhash against
// the same threshold arithmetic, so a tailsample probabilistic policy at 50%
// keeps exactly the traces a head sampler at probability 0.5 already kept
// instead of independently discarding half of them again
// (tailsample.TestProbabilisticNestsWithTheHeadSampler pins it). One caveat that
// belongs here rather than there: the head sampler's guard rails (keepErrors,
// keepSlowerThan) are per SPAN, so with them on a trace can reach this buffer as
// a fragment — the error spans without their siblings. That is the same partial
// trace assembly can produce, and tailsample documents what every policy does
// with one (latency reads a lower bound, attribute policies under-match).
//
// # Bounds
//
// Three, all hard, none of which drops blind. At a bound the OLDEST trace is
// DECIDED EARLY — judged on the spans present and then exported or dropped —
// rather than evicted unjudged: tailsample treats a partial trace as a lower
// bound, so an early decision degrades gracefully (a slow trace can be missed, a
// fast one is never invented) where a blind eviction loses the trace outright
// including the errors it may have been about to reveal. Every early decision is
// counted separately from a normal one, with the bound that caused it as the
// reason.
package tailbuffer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailsample"
	"github.com/JohanLindvall/kubescrape/internal/config"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// Defaults. The memory ones are sized for a shard with a ~1 GiB limit: a
// buffered span costs ~365 B for a minimal one and ~1 KiB for a realistic one
// (bench_test.go measures both), plus ~470 B once per (pushed payload, trace)
// group for the ResourceSpans/ScopeSpans wrapper and its copy of the resource
// attributes — the part that surprises people. maxSpans 200k is therefore
// ~100-300 MiB of spans.
//
// It is not left at that number blindly: memory.go checks it against the
// container's actual memory limit at startup, lowers the DEFAULT when the limit
// cannot afford it, and refuses an explicit setting that could only end in an
// OOM. The sizing rule in one line is
//
//	maxSpans x 1 KiB must fit in a quarter of the pod's memory limit.
//
// The arithmetic an operator has to do on top of that: a shard receiving R
// spans/second holds R * decisionWait spans in the steady state. At 50k spans/s
// and a 5s window that is 250k — above the default, so the maxSpans bound would
// bind and decide the oldest traces about a second early. Raising maxSpans costs
// memory LINEARLY and raises the odds of the OOM that loses the whole buffer, so
// the answer above a pod's budget is more shards (the ring divides R by the
// shard count), not a bigger number; the early counter says which is happening.
const (
	defaultDecisionWait     = 5 * time.Second
	defaultMaxTraces        = 100_000
	defaultMaxSpansPerTrace = 1_000
	defaultMaxSpans         = 200_000
	defaultCacheSize        = 100_000
	defaultCacheTTL         = time.Minute
)

// Early-decision reasons: the bound that forced the decision, as the metric
// label reports it. Not a free-form string — these are the whole set.
const (
	reasonSpansPerTrace = "spans_per_trace"
	reasonMaxTraces     = "max_traces"
	reasonMaxSpans      = "max_spans"
	reasonShutdown      = "shutdown"
)

// sendAttempts bounds the decision loop's retry of one export. The spans are
// already acked, so there is no sender to hand the failure to and dropping them
// on the first blip would be gratuitous; but this goroutine also owes every
// other buffered trace its decision, so the retry cannot be unbounded either.
const sendAttempts = 3

// sendBackoff is the first retry delay; it doubles per attempt.
const sendBackoff = 250 * time.Millisecond

// warnEvery throttles the failed-export warning. A collector outage would
// otherwise write one line per tick per shard for as long as it lasts.
const warnEvery = 30 * time.Second

// Config is the agent config's tailSampling section: the policy list
// (agent/tailsample, embedded so the section reads as one thing) plus this
// layer's own memory and timing knobs.
//
// Every duration is a STRING parsed with time.ParseDuration, never a
// time.Duration: the agent config decodes through sigs.k8s.io/yaml ->
// encoding/json, which accepts only a raw nanosecond integer for a
// time.Duration, and because the file is UnmarshalStrict'ed one such field
// rejects the WHOLE config and the workload does not start. Same treatment as
// tailsample's own thresholds, servicegraph.Config.Wait and
// tracesample.Config.KeepSlowerThan.
type Config struct {
	// Config is the policy list — `policies`, evaluated in order, first match
	// wins. Embedded, so the section is one flat object and the policy syntax is
	// documented in exactly one place (agent/tailsample's package doc).
	tailsample.Config

	// DecisionWait is how long a trace is held from the moment its FIRST span
	// arrives before it is judged ("5s" by default). It is the assembly budget:
	// long enough to cover the slowest span's journey from its process to this
	// shard, short enough that the buffer's contents — which a hard kill loses —
	// stay small. It is not a latency budget for the trace itself; a trace whose
	// root span is still open when the window closes is judged on what arrived.
	DecisionWait string `json:"decisionWait,omitempty"`

	// MaxTraces caps distinct traces held at once (100000 default). At the cap
	// the oldest is decided early rather than evicted.
	MaxTraces int `json:"maxTraces,omitempty"`
	// MaxSpansPerTrace caps one trace's buffered spans (1000 default). At the cap
	// that trace is decided on the spans present and its remainder follows the
	// decision through the cache — which is what keeps one pathological trace
	// (a retry storm under a single id, an instrumented loop) from being the
	// whole buffer.
	MaxSpansPerTrace int `json:"maxSpansPerTrace,omitempty"`
	// MaxSpans caps the buffer's TOTAL spans (200000 default) — the bound that
	// actually determines the process's memory, since the other two multiply.
	//
	// Budget ~1 KiB per span and keep the product under a QUARTER of the pod's
	// memory limit (memory.go explains the share). Left unset, the default is
	// lowered at startup to whatever the limit affords; set explicitly it is
	// honoured, warned about above that budget, and refused outright when the
	// spans alone would need the whole limit — an OOM here loses every buffered
	// span at once, which is strictly worse than the early decisions a smaller
	// ceiling causes.
	MaxSpans int `json:"maxSpans,omitempty"`

	// DecisionCacheSize bounds the verdict cache used for spans arriving after
	// their trace decided (100000 default). At the cap the oldest verdict is
	// evicted and a later span for it starts a FRESH window — see put.
	DecisionCacheSize int `json:"decisionCacheSize,omitempty"`
	// DecisionCacheTTL is how long a verdict is remembered ("1m" default). It
	// bounds how late a straggler can still follow its trace's decision;
	// stragglers later than this are indistinguishable from a new trace.
	DecisionCacheTTL string `json:"decisionCacheTTL,omitempty"`
}

// Enabled reports whether tail sampling is configured. The knobs alone do not
// enable it: a section with bounds but no policies would drop every trace.
func (c *Config) Enabled() bool { return c != nil && c.Config.Enabled() }

// Validate reports a malformed section. Shape-only — no clock, no filesystem, no
// network — so -check-config runs it, and it validates the policy list by
// COMPILING it (tailsample.Config.Validate), so the dry run cannot drift from
// what a real start accepts.
func (c *Config) Validate() error {
	if c == nil {
		return nil
	}
	if !c.Enabled() {
		// Bounds without policies: the section reads as configured and samples
		// nothing, which is indistinguishable from the feature being off. The
		// cache knobs count too — `decisionCacheSize: 50000` with no policies
		// is the same misconfiguration as `maxTraces: 5` with none, and only
		// the latter used to be caught.
		if c.DecisionWait != "" || c.MaxTraces != 0 || c.MaxSpans != 0 || c.MaxSpansPerTrace != 0 ||
			c.DecisionCacheSize != 0 || c.DecisionCacheTTL != "" {
			return errors.New("tailSampling has buffer settings but no policies (an evaluator with no policies drops every trace, so the settings would silently sample nothing)")
		}
		return nil
	}
	if err := c.Config.Validate(); err != nil {
		return err
	}
	_, err := c.settings()
	return err
}

// settings resolves the config to its effective values, defaults applied. It is
// the ONE place a default or a bound check lives, shared by Validate and New so
// a dry run and a start cannot disagree.
func (c *Config) settings() (settings, error) {
	s := settings{
		wait:             defaultDecisionWait,
		maxTraces:        defaultMaxTraces,
		maxSpansPerTrace: defaultMaxSpansPerTrace,
		maxSpans:         defaultMaxSpans,
		cacheSize:        defaultCacheSize,
		cacheTTL:         defaultCacheTTL,
	}
	var err error
	// config.Duration is the one optional-duration reader (empty takes the
	// default, a negative value is an error naming the field and the value);
	// Positive folds in the bound check these two fields need, so the
	// explanation stays attached to the error rather than living in a separate
	// if below it.
	if s.wait, err = config.Duration("tailSampling.decisionWait", c.DecisionWait, defaultDecisionWait,
		config.Positive("a zero window decides every trace on its first span")); err != nil {
		return s, err
	}
	if s.cacheTTL, err = config.Duration("tailSampling.decisionCacheTTL", c.DecisionCacheTTL, defaultCacheTTL,
		config.Positive("with no cache every late span re-decides its trace")); err != nil {
		return s, err
	}
	for _, b := range []struct {
		field string
		val   int
		dst   *int
	}{
		{"maxTraces", c.MaxTraces, &s.maxTraces},
		{"maxSpansPerTrace", c.MaxSpansPerTrace, &s.maxSpansPerTrace},
		{"maxSpans", c.MaxSpans, &s.maxSpans},
		{"decisionCacheSize", c.DecisionCacheSize, &s.cacheSize},
	} {
		if b.val < 0 {
			return s, fmt.Errorf("tailSampling.%s %d is negative", b.field, b.val)
		}
		if b.val > 0 {
			*b.dst = b.val
		}
	}
	// A trace that cannot fit under the total ceiling could never be decided
	// normally: the per-trace bound would never be reached, and the ceiling would
	// early-decide it (and everything else) on every payload.
	if s.maxSpansPerTrace > s.maxSpans {
		return s, fmt.Errorf("tailSampling.maxSpansPerTrace %d is above maxSpans %d (one trace could never fit in the buffer)", s.maxSpansPerTrace, s.maxSpans)
	}
	return s, nil
}

// settings is the resolved config.
type settings struct {
	wait             time.Duration
	maxTraces        int
	maxSpansPerTrace int
	maxSpans         int
	cacheSize        int
	cacheTTL         time.Duration
}

// TracesExporter is the downstream exporter (otlpexport.Client, Buffered and the
// servicegraph/spanmetrics taps all satisfy it).
type TracesExporter interface {
	ExportTraces(ctx context.Context, td ptrace.Traces) error
}

// Buffer assembles traces, decides them and forwards the keeps.
//
// One mutex guards everything: the trace map, the arrival order, the span
// tallies, the decision cache and the two scratch maps the receive path groups
// with. ExportTraces runs on the receiver's concurrent handler goroutines and
// Run on its own, so there is no lock-free path here worth the reasoning — but
// no export ever happens while the mutex is held, because a slow collector would
// otherwise serialise every sender in the cluster behind one send.
type Buffer struct {
	ev   *tailsample.Evaluator
	next TracesExporter
	set  settings
	tick time.Duration
	log  *slog.Logger
	now  func() time.Time // injectable for tests

	mu    sync.Mutex
	trace map[pcommon.TraceID]*bufTrace
	// order is the arrival FIFO. Every trace waits the same decisionWait, so
	// first-seen order IS deadline order and the oldest — the next to decide
	// normally, and the one an early decision takes — is the front. Entries are
	// removed lazily: a decided trace's slot stays until it reaches the front,
	// where `gone` skips it, which is what keeps removal from the middle O(1).
	order []*bufTrace
	head  int
	spans int
	cache *decisionCache
	// res and cur group the payload being received: source ResourceSpans ->
	// per-trace destination, and source ScopeSpans -> per-trace destination.
	// Reused across calls (cleared per resource and per scope) so a receive
	// allocates no maps; safe because the whole walk happens under mu.
	res map[pcommon.TraceID]ptrace.ResourceSpans
	cur map[pcommon.TraceID]ptrace.ScopeSpans
	// scratch is the span view handed to Decide, refilled per decision. The
	// evaluator neither retains nor mutates it (tailsample.Trace says so), so one
	// buffer serves every decision and the decision path stays allocation-free.
	scratch []tailsample.Span

	// Counters resolved once at New, so neither the per-span receive path nor
	// the per-trace decision path pays a label lookup. byPolicy is pre-populated
	// from Evaluator.Names(): a decision counter whose series appears only once a
	// policy first fires makes "policy X kept nothing" indistinguishable from
	// "policy X does not exist".
	byPolicy   map[string]policyCounters
	spansKept  *metrics.RegCounter
	spansDrop  *metrics.RegCounter
	spansLost  *metrics.RegCounter
	lateKept   *metrics.RegCounter
	lateDrop   *metrics.RegCounter
	earlyCount map[string]*metrics.RegCounter

	warnGate logdedupe.Throttle
}

// policyCounters is one policy's two decision counters (it can keep by matching
// or, inverted, veto).
type policyCounters struct{ keep, drop *metrics.RegCounter }

// bufTrace is one assembling trace.
type bufTrace struct {
	id    pcommon.TraceID
	td    ptrace.Traces
	spans int
	first time.Time
	// gone marks the entry removed from the map, so its stale slot in `order` is
	// skipped rather than re-decided.
	gone bool
}

// New builds a Buffer in front of next. It compiles the policy list, so every
// config error surfaces here rather than at the first trace.
func New(cfg Config, next TracesExporter, log *slog.Logger) (*Buffer, error) {
	if !cfg.Enabled() {
		return nil, errors.New("tail sampling: no policies configured")
	}
	set, err := cfg.settings()
	if err != nil {
		return nil, err
	}
	ev, err := tailsample.New(cfg.Config)
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}
	// The span ceiling against memory that exists, not against a number in a
	// values file (memory.go). This is deliberately NOT in Config.Validate,
	// which is shape-only so -check-config touches no filesystem — but it IS at
	// startup, which is where a config implying an OOM has to be caught.
	limit, source := memLimit()
	if err := applyMemoryBudget(&set, cfg.MaxSpans > 0, limit, source, log); err != nil {
		return nil, err
	}
	b := &Buffer{
		ev:         ev,
		next:       next,
		set:        set,
		tick:       tickFor(set.wait),
		log:        log,
		now:        time.Now,
		trace:      make(map[pcommon.TraceID]*bufTrace, 1024),
		cache:      newDecisionCache(set.cacheSize, set.cacheTTL),
		res:        make(map[pcommon.TraceID]ptrace.ResourceSpans, 64),
		cur:        make(map[pcommon.TraceID]ptrace.ScopeSpans, 64),
		byPolicy:   make(map[string]policyCounters),
		spansKept:  obs.TailSampleSpans.WithLabelValues("kept"),
		spansDrop:  obs.TailSampleSpans.WithLabelValues("dropped"),
		spansLost:  obs.TailSampleSpans.WithLabelValues("lost"),
		lateKept:   obs.TailSampleLate.WithLabelValues("kept"),
		lateDrop:   obs.TailSampleLate.WithLabelValues("dropped"),
		earlyCount: make(map[string]*metrics.RegCounter, 4),
	}
	// "" is the no-policy-had-an-opinion default drop, which is a real and
	// important outcome — it is what a policy list that matches nothing looks
	// like — so it gets a series like any named policy.
	for _, name := range append(ev.Names(), "") {
		b.byPolicy[name] = policyCounters{
			keep: obs.TailSampleTraces.WithLabelValues("keep", policyLabel(name)),
			drop: obs.TailSampleTraces.WithLabelValues("drop", policyLabel(name)),
		}
	}
	for _, r := range []string{reasonSpansPerTrace, reasonMaxTraces, reasonMaxSpans, reasonShutdown} {
		b.earlyCount[r] = obs.TailSampleEarly.WithLabelValues(r)
	}
	return b, nil
}

// policyLabel renders an unattributed decision. "" would render as an empty
// label value, which in Prometheus is indistinguishable from the series not
// having the label at all.
func policyLabel(name string) string {
	if name == "" {
		return "none"
	}
	return name
}

// tickFor sizes the decision loop's cadence from the window itself, so the two
// cannot drift. A quarter of the window bounds a trace's decision at
// decisionWait + tick; the clamps stop a pathological config from either
// spinning (a millisecond window) or letting a due trace sit for minutes (an
// hour-long one).
func tickFor(wait time.Duration) time.Duration {
	d := wait / 4
	if d < 100*time.Millisecond {
		return 100 * time.Millisecond
	}
	if d > time.Second {
		return time.Second
	}
	return d
}

// Wait reports the effective decision window (for a startup log line).
func (b *Buffer) Wait() time.Duration { return b.set.wait }

// Stats snapshots what the buffer is holding — which is exactly what a hard kill
// would lose, hence a gauge rather than a debug field.
type Stats struct {
	Traces int
	Spans  int
}

// Stats returns the current occupancy.
func (b *Buffer) Stats() Stats {
	if b == nil {
		return Stats{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return Stats{Traces: len(b.trace), Spans: b.spans}
}

// ExportTraces takes a payload from the chain above: it buffers every span whose
// trace has not been decided yet and immediately forwards those whose trace was
// decided KEEP (dropping those decided DROP).
//
// It returns after copying, WITHOUT waiting for the trace's decision — see the
// package doc for why, and for what that costs.
//
// The error it can return is the forward of the already-decided spans. Failing
// the push there is deliberate: those spans are in the payload the sender still
// holds, so its retry recovers them, and the retry costs only duplicates (the
// re-pushed spans of a still-buffering trace are buffered twice, which is the
// same at-least-once trade the re-shard hop makes). Spans that came out of the
// BUFFER in the same send — an early decision forced by a bound — are ours, so
// the payload is marked otlpexport.Own and a disk buffer takes it; without one,
// a failure loses them and they are counted as lost rather than left to a retry
// that will not re-send them. (A few of those may in fact be recoverable: an
// early-decided trace can include spans from THIS push, which the retry will
// re-deliver against the cached verdict. Counting them lost over-reports a rare
// corner — a bound binding in the same instant the collector fails — in the
// direction that does not hide loss.)
func (b *Buffer) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	if td.SpanCount() == 0 {
		return nil
	}
	out, late, lateDropped, mine := b.take(td)
	if out.spans == 0 {
		// Nothing to forward means the push is acked right here — and the ack
		// is what makes its late-DROPPED spans final, so this is where they are
		// counted (see the success path below for the symmetric argument).
		if lateDropped > 0 {
			b.lateDrop.Add(float64(lateDropped))
		}
		return nil
	}
	if mine > 0 {
		// This payload carries spans that came out of the BUFFER (an early
		// decision forced by a bound), whose senders were acked seconds ago.
		// Marking it hands it to the disk buffer when one is open — see
		// otlpexport/owned.go. The late spans riding along are spooled too,
		// which is right: the sender is acked either way, so the spool has to
		// be the thing that carries them.
		ctx = otlpexport.Own(ctx)
	}
	if err := b.next.ExportTraces(ctx, out.td); err != nil {
		if mine > 0 {
			b.spansLost.Add(float64(mine))
		}
		b.warn("exporting tail-sampled spans failed", "spans", out.spans, "buffered", mine, "error", err)
		return err
	}
	// Counted only now: a late span the sender will re-push must not be counted
	// twice, and a decided trace's spans are "kept" only once they have landed.
	// The late-DROPPED tally defers to the same ack: those spans are not in
	// out.td, but a NACKed push is retransmitted whole, so counting them at
	// receive time tallied the same spans once per retry attempt — this return
	// is what stops the retransmissions that would re-present them.
	if lateDropped > 0 {
		b.lateDrop.Add(float64(lateDropped))
	}
	if late > 0 {
		b.lateKept.Add(float64(late))
	}
	if mine > 0 {
		b.spansKept.Add(float64(mine))
	}
	return nil
}

// take is the receive path's whole critical section: one walk of the payload
// that buffers, classifies and enforces. It returns the payload to forward, how
// many of its spans came from THIS push (the sender can re-send those), how
// many were dropped against a cached DROP verdict (tallied by the caller only
// once the push acks — a NACKed push is retransmitted and re-presents them),
// and how many came out of the buffer (the sender cannot re-send those).
func (b *Buffer) take(td ptrace.Traces) (out outbound, late, lateDropped, mine int) {
	now := b.now()

	b.mu.Lock()
	defer b.mu.Unlock()

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		clear(b.res)
		var lateRS ptrace.ResourceSpans
		haveRS := false
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j)
			clear(b.cur)
			var lateSS ptrace.ScopeSpans
			haveSS := false
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				sp := spans.At(k)
				id := sp.TraceID()
				if keep, ok := b.cache.get(id, now); ok {
					// A span for a trace already judged. Deciding it again would
					// re-charge rateLimiting and could answer differently, so the
					// cached verdict is applied instead of a second Decide.
					if !keep {
						lateDropped++
						continue
					}
					if !haveRS {
						lateRS = out.dest().ResourceSpans().AppendEmpty()
						rs.Resource().CopyTo(lateRS.Resource())
						lateRS.SetSchemaUrl(rs.SchemaUrl())
						haveRS = true
					}
					if !haveSS {
						lateSS = lateRS.ScopeSpans().AppendEmpty()
						ss.Scope().CopyTo(lateSS.Scope())
						lateSS.SetSchemaUrl(ss.SchemaUrl())
						haveSS = true
					}
					sp.CopyTo(lateSS.Spans().AppendEmpty())
					out.spans++
					late++
					continue
				}
				// Not decided (or the verdict aged out of the cache, in which
				// case this starts a FRESH window and the trace may be decided a
				// second time — which does NOT re-charge rateLimiting or
				// composite while the cache still remembers the trace at all;
				// see decide and cache.go's two lifetimes).
				e := b.add(id, sp, rs, ss, now)
				mine += b.enforce(&out, e, now)
			}
		}
	}
	return out, late, lateDropped, mine
}

// add copies one span into its trace's buffer, creating the trace and the
// destination ResourceSpans/ScopeSpans for the payload group being walked.
//
// Grouping is per (source resource, source scope, trace), so a trace collects
// one ResourceSpans per pushed payload that carried spans for it — the scope
// identity and the resource attributes the sender set are preserved exactly, at
// the cost of one resource copy per group (~320 B minimal, ~1040 B for the
// attribute set the tier's enricher stamps).
//
// Merging groups across payloads would mean hashing AND comparing attribute maps
// on the receive path, under the mutex that serialises every sender on the
// shard. BenchmarkAssembleByPushSize prices exactly that trade and argues it
// down: the resources of one trace are mostly distinct (different services), and
// how often ONE sender splits one trace across pushes is bounded by decisionWait
// against the SDK's batch interval — about +13% on a realistically enriched
// trace, for a comparison on every group of every push.
func (b *Buffer) add(id pcommon.TraceID, sp ptrace.Span, rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, now time.Time) *bufTrace {
	e := b.trace[id]
	if e == nil {
		e = &bufTrace{id: id, td: ptrace.NewTraces(), first: now}
		b.trace[id] = e
		b.order = append(b.order, e)
	}
	dst, ok := b.cur[id]
	if !ok {
		nrs, ok := b.res[id]
		if !ok {
			nrs = e.td.ResourceSpans().AppendEmpty()
			rs.Resource().CopyTo(nrs.Resource())
			nrs.SetSchemaUrl(rs.SchemaUrl())
			b.res[id] = nrs
		}
		dst = nrs.ScopeSpans().AppendEmpty()
		ss.Scope().CopyTo(dst.Scope())
		dst.SetSchemaUrl(ss.SchemaUrl())
		b.cur[id] = dst
	}
	sp.CopyTo(dst.Spans().AppendEmpty())
	e.spans++
	b.spans++
	return e
}

// enforce applies the three memory bounds after one span was admitted, deciding
// early where one binds, and returns how many buffered spans left as a result.
//
// The victim is always the OLDEST trace, never the largest. It is the one
// closest to its natural decision, so judging it early costs the least
// completeness; picking the largest would need a heap keyed by a count that
// changes on every span, and would systematically punish deep traces — exactly
// the ones a tail sampler exists to catch.
func (b *Buffer) enforce(out *outbound, e *bufTrace, now time.Time) int {
	moved := 0
	if e.spans >= b.set.maxSpansPerTrace && !e.gone {
		moved += b.decide(out, e, now, reasonSpansPerTrace)
	}
	for len(b.trace) > b.set.maxTraces {
		n, ok := b.decideOldest(out, now, reasonMaxTraces)
		if !ok {
			break
		}
		moved += n
	}
	// Terminates: every pass removes one trace, and with the map empty there is
	// nothing left to hold spans.
	for b.spans > b.set.maxSpans && len(b.trace) > 0 {
		n, ok := b.decideOldest(out, now, reasonMaxSpans)
		if !ok {
			break
		}
		moved += n
	}
	return moved
}

// decideOldest decides the front of the arrival FIFO, skipping the slots of
// traces that were already decided out from under it. It reports whether it
// found one at all.
func (b *Buffer) decideOldest(out *outbound, now time.Time, reason string) (int, bool) {
	for b.head < len(b.order) {
		e := b.order[b.head]
		if e.gone {
			b.head++
			continue
		}
		return b.decide(out, e, now, reason), true
	}
	return 0, false
}

// decide judges one trace, removes it from the buffer, remembers the verdict for
// late spans, and appends the spans to out when the verdict is keep. It returns
// the number of spans that left the buffer into out (0 for a drop).
//
// reason is "" for a decision made because the window elapsed; anything else is
// an early decision and names the bound that forced it.
func (b *Buffer) decide(out *outbound, e *bufTrace, now time.Time, reason string) int {
	b.scratch = b.scratch[:0]
	rss := e.td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		attrs := rs.Resource().Attributes()
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				b.scratch = append(b.scratch, tailsample.Span{Span: spans.At(k), Resource: attrs})
			}
		}
	}
	// Charged: an earlier decision for this trace BILLED the rateLimiting/
	// composite budgets (its verdict has since aged out, or it was decided early
	// and its remainder started a fresh window), so this one CHECKS them instead
	// of spending them again. The cache remembers the spend the evaluator
	// reported, not the decision — a decision the budget refused paid nothing,
	// and skipping the charge for it would admit spans free every fresh window.
	// See the two lifetimes in cache.go.
	d := b.ev.Decide(tailsample.Trace{TraceID: e.id, Spans: b.scratch, Charged: b.cache.charged(e.id)})
	// The evaluator retains nothing (tailsample.Trace says so), so every element
	// is dead the instant Decide returns — and they are HANDLES, not values: each
	// one pins a whole span message plus its group's resource attributes. `[:0]`
	// alone leaves the tail of the largest decision so far pointing into a trace
	// this call is about to release (the drop branch below nils e.td for exactly
	// that reason), and a workload of small traces would then never overwrite it.
	// clear() zeroes [0,len) and the refill above starts from the front, so
	// nothing past a decision's own fill is ever live. It allocates nothing, so
	// the decision path stays allocation-free.
	clear(b.scratch)

	c, ok := b.byPolicy[d.Policy]
	if !ok { // unreachable: Names() covers every name a Decision can carry
		c = policyCounters{
			keep: obs.TailSampleTraces.WithLabelValues("keep", policyLabel(d.Policy)),
			drop: obs.TailSampleTraces.WithLabelValues("drop", policyLabel(d.Policy)),
		}
		b.byPolicy[d.Policy] = c
	}
	if d.Sampled {
		c.keep.Inc()
	} else {
		c.drop.Inc()
	}
	if reason != "" {
		b.earlyCount[reason].Inc()
	}

	b.remove(e)
	// d.Charged, not "we decided it": a re-decision reads this back as
	// Trace.Charged, and only a budget that actually moved may be skipped.
	b.cache.put(e.id, d.Sampled, d.Charged, now)
	if !d.Sampled {
		b.spansDrop.Add(float64(e.spans))
		// Release the payload. remove() deliberately leaves this entry's
		// POINTER in its b.order slot to be skipped when it reaches the front,
		// and compact() only overwrites that prefix once the head passes
		// halfway — so without this the dropped trace's whole ptrace.Traces
		// stays reachable from the live slice for roughly one FIFO length of
		// subsequent decisions. Dropping is the NORMAL mode of a tail sampler,
		// so that is the steady state: the live heap was up to ~2x what
		// maxSpans and memory.go's sizing arithmetic promise, which is the
		// bound that decides whether the buffer OOMs the process it protects.
		//
		// The keep branch below gets this for free — MoveAndAppendTo nils the
		// source slice. This branch has to say it.
		e.td = ptrace.NewTraces()
		return 0
	}
	n := e.spans
	e.td.ResourceSpans().MoveAndAppendTo(out.dest().ResourceSpans())
	out.spans += n
	return n
}

// remove takes a trace out of the buffer. Its slot in `order` is left to be
// skipped when it reaches the front, and its scratch grouping entries are
// dropped so a later span of the same trace in the SAME payload cannot be
// appended into a payload that has already been moved out.
func (b *Buffer) remove(e *bufTrace) {
	if e.gone {
		return
	}
	e.gone = true
	delete(b.trace, e.id)
	delete(b.res, e.id)
	delete(b.cur, e.id)
	b.spans -= e.spans
	b.compact()
}

// compact drops the consumed prefix of the arrival FIFO once it is more than
// half the slice. Without it a long-running shard's `order` grows without bound
// even though the live entries never do — the fourth unbounded-slice bug in this
// repo's history, and the cheapest one to not write.
func (b *Buffer) compact() {
	if b.head < len(b.order)/2 || b.head == 0 {
		return
	}
	n := copy(b.order, b.order[b.head:])
	clear(b.order[n:]) // drop the tail's references so the entries can be freed
	b.order = b.order[:n]
	b.head = 0
}

// outbound accumulates everything leaving in one payload. The pdata payload is
// created lazily so the common case — a push whose traces are all still
// assembling — allocates nothing beyond the span copies.
type outbound struct {
	td    ptrace.Traces
	init  bool
	spans int
}

func (o *outbound) dest() ptrace.Traces {
	if !o.init {
		o.td = ptrace.NewTraces()
		o.init = true
	}
	return o.td
}

// Run decides traces as their windows close, until ctx is cancelled. The final
// pass belongs to Flush, which the shutdown path calls with a live context —
// this one's is already cancelled by then.
func (b *Buffer) Run(ctx context.Context) {
	t := time.NewTicker(b.tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// A tick that came ready while the previous sweep was exporting is
			// still sitting in the ticker's one-slot channel when the process is
			// cancelled — and a select with two ready cases picks at random, so
			// without this the loop runs one more sweep after shutdown began.
			// That sweep would pull due traces out of the buffer moments before
			// main's Flush, which is the pass with a budget of its own.
			if ctx.Err() != nil {
				return
			}
			b.Sweep(ctx)
		}
	}
}

// Sweep decides every trace whose window has closed and exports the keeps.
func (b *Buffer) Sweep(ctx context.Context) { b.drain(ctx, false) }

// Flush decides EVERY buffered trace immediately, whatever its window, and
// exports the keeps. It is the graceful-shutdown path: it is what bounds the
// loss this layer's contract admits to a hard kill (see the package doc), so it
// must run after the receivers have stopped and before the exporter closes.
func (b *Buffer) Flush(ctx context.Context) { b.drain(ctx, true) }

// drain decides the due traces (or all of them) and sends the keeps as ONE
// payload. Deciding happens under the mutex; the send never does.
func (b *Buffer) drain(ctx context.Context, all bool) {
	if b == nil {
		return
	}
	now := b.now()
	var out outbound

	b.mu.Lock()
	for b.head < len(b.order) {
		e := b.order[b.head]
		if e.gone {
			b.head++
			continue
		}
		reason := ""
		if !all {
			if now.Sub(e.first) < b.set.wait {
				break // the FIFO is deadline-ordered: nothing behind it is due
			}
		} else if now.Sub(e.first) < b.set.wait {
			// Judged before its window closed because the process is stopping.
			reason = reasonShutdown
		}
		b.head++
		b.decide(&out, e, now, reason)
	}
	b.mu.Unlock()

	if out.spans == 0 {
		return
	}
	// Every span here came out of the buffer, so nobody else holds a copy: the
	// payload is OURS (otlpexport/owned.go). With -buffer-dir open on this
	// workload the send below is an append to the traces spool and a collector
	// outage becomes a backlog; without one it is a direct send and a spent
	// retry budget is loss.
	//
	// Which is why the send does not run on the CALLER's cancellation — see
	// sendContext. Ours is the only copy from the moment decide() removed these
	// traces, so a ctx cancelled mid-send is not a retry, it is loss, and the
	// shutdown Flush cannot recover it: those traces are already out of the
	// buffer and their verdicts are already cached.
	sctx, cancel := sendContext(ctx)
	defer cancel()
	if err := b.sendRetry(otlpexport.Own(sctx), out.td); err != nil {
		b.spansLost.Add(float64(out.spans))
		b.warn("exporting tail-sampled traces failed; the spans are dropped (they were acked to their sender when they were buffered, and no disk buffer took them)",
			"spans", out.spans, "error", err)
		return
	}
	b.spansKept.Add(float64(out.spans))
}

// sendContext is the context a drain's SEND runs on: the caller's DEADLINE
// without the caller's CANCELLATION.
//
// The cancellation goes because the payload is already ours — the spans left the
// buffer before the send started — so propagating the process's shutdown into it
// destroys exactly the decided keeps the shutdown Flush exists to salvage, and
// destroys them where nothing can re-decide them. The DEADLINE stays because it
// is the caller's BUDGET rather than its cancellation: Flush runs on the
// shutdown step's and must not outlive the pod's termination grace, while the
// Run loop's own sweeps carry none and stay bounded by the export's retries as
// they always were.
//
// WithoutCancel rather than context.Background(), always: otlpexport.Own's
// durability marker is a context VALUE and a fresh Background would strip it,
// turning the send that a disk buffer would have spooled into one that is merely
// retried and then lost.
func sendContext(ctx context.Context) (context.Context, context.CancelFunc) {
	sctx := context.WithoutCancel(ctx)
	if d, ok := ctx.Deadline(); ok {
		return context.WithDeadline(sctx, d)
	}
	return sctx, func() {}
}

// sendRetry exports with a bounded backoff — otlpexport.Retry, THE bounded
// in-call retry shape: a permanent rejection is not retried (the collector
// will not change its mind) and a cancelled context ends it without another
// sleep — during shutdown the whole final flush is on a budget. Cold path (the
// drain has released the mutex), so the closure costs nothing that matters.
func (b *Buffer) sendRetry(ctx context.Context, td ptrace.Traces) error {
	return otlpexport.Retry(ctx, sendAttempts, sendBackoff, func() error {
		return b.next.ExportTraces(ctx, td)
	})
}

// warn logs at most once per warnEvery.
func (b *Buffer) warn(msg string, args ...any) {
	if !b.warnGate.Allow(warnEvery) {
		return
	}
	b.log.Warn(msg, args...)
}
