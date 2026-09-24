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
//   - by TIME: a trace is decided about decisionWait (5s by default) after
//     its first span, plus up to one decision tick (a quarter of the window,
//     clamped to 100ms-1s: see tickFor) — and plus however long the decision
//     loop's own EXPORT takes, because the loop decides nothing while its send
//     is in flight. Without -buffer-dir a send to a slow or blackholed
//     collector can last sendAttempts x the exporter's timeout plus ~750ms of
//     backoff, and for that long the buffer only fills, until the SIZE bound
//     below starts deciding the oldest traces early (the early-decision line
//     then names the slowest such export, so a stalled loop can be told from
//     an undersized bound);
//   - by SIZE: at most maxSpans of them, whatever the rate;
//   - by SHUTDOWN: a graceful stop (SIGTERM, a rolling update, an eviction)
//     calls Flush, which decides every buffered trace immediately and exports
//     the keeps before the exporter closes — and LATCHES the buffer: nothing
//     will flush it a second time, so a straggler push that outlives the
//     receivers' graceful stop (an active handler http.Server.Shutdown never
//     interrupts, an RPC gRPC's GracefulStop was still waiting on when the
//     producer-join budget expired) is decided inside its own take(), on the
//     spans present, with its keeps riding out on that push's own ack.
//     Without the latch those spans re-filled a buffer nobody would flush
//     again and were lost behind a 200. Only a hard kill — SIGKILL, an OOM,
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
//     point nobody else holds it either. That holds whichever path decided
//     it: a keep the sweep decided and a keep a bound forced out inside an
//     application's push (sendOwned) get the same retry, the same send
//     detached from the caller's cancellation, and the same loss report.
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
// than compound: both hash the trace id with the same unsalted rapidhash against
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
// reason — and only a genuinely early one: a trace whose window had already
// closed when a bound caught it was judged on its full window (see judge).
package tailbuffer

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailsample"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// sendAttempts bounds the retry of one export of spans that came out of the
// BUFFER (sendOwned) — the decision loop's, and a push's early decisions. The
// spans are already acked, so there is no sender to hand the failure to and
// dropping them on the first blip would be gratuitous; but the decision loop
// owes every other buffered trace its decision, and a push holds its sender's
// in-flight slot, so the retry cannot be unbounded either.
const sendAttempts = 3

// sendBackoff is the first retry delay; it doubles per attempt.
const sendBackoff = 250 * time.Millisecond

// warnEvery throttles the failed-export warning. A collector outage would
// otherwise write one line per tick per shard for as long as it lasts.
const warnEvery = 30 * time.Second

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

	mu sync.Mutex
	// flushed latches when Flush's drain runs and is never cleared: the final
	// flush is the LAST pass anyone makes over this buffer, so a span buffered
	// after it would sit here until the process died — acked, counted by no
	// counter, visible only as a gauge nobody is left to act on. take()
	// consults it to decide a post-Flush push's new traces on the spot
	// (reason=shutdown) instead of buffering them. Under mu, like everything
	// else: a push either lands before Flush's drain (which then decides it)
	// or sees the latch — there is no in-between where it buffers unseen.
	flushed bool
	trace   map[pcommon.TraceID]*bufTrace
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
	// buffer serves every decision — which is part of why a decision allocates
	// nothing beyond amortized slice growth (TestDropDecisionAllocationBudget,
	// TestKeepDecisionAllocationBudget).
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
	earlyCount [numEarlyReasons]*metrics.RegCounter // nil at reasonNone
	// earlyPending accumulates early decisions per reason since the last drain
	// that actually REPORTED them (a suppressed drain leaves them standing, so
	// the line describes the window it names), and earlyWarn throttles that
	// report on earlyEvery — a field only so tests can drive the window. The
	// DECISION path is allocation-budgeted (TestDropDecisionAllocationBudget,
	// TestKeepDecisionAllocationBudget) and runs per trace under the mutex, so
	// it may only bump a counter; the line belongs to the sweep, which is the
	// repo's rule for anything a hot path notices (see the tailer's per-line
	// counters).
	earlyPending [numEarlyReasons]int
	earlyWarn    logdedupe.Throttle
	earlyEvery   time.Duration
	// slowestExport is the longest decision-loop export (in nanoseconds) since
	// the last drain that either emitted the early-decision line or had nothing
	// to report (takeEarlyLocked), and rides on that line. The loop
	// decides nothing while its own send is in flight, so a slow collector
	// fills the buffer and makes maxSpans bind — an early decision caused by
	// EXPORT LATENCY, which the bounds alone would misdiagnose as a bound
	// sized below the shard's span rate. An atomic rather than a field under
	// mu: it is written after the send, with the mutex long released, and the
	// shutdown Flush can drain concurrently with a Run sweep's send.
	slowestExport atomic.Int64

	// TWO gates for the failed-export lines, never one. They describe the
	// same downstream condition and therefore CO-OCCUR, but they are not the
	// same event: nackWarn's line reports a NACK of a push that carried only
	// the SENDER's spans (late spans, spans decided on arrival) — nothing is
	// lost, the sender still holds every one and retransmits — while
	// lossWarn's reports spans DESTROYED: spans that came out of the BUFFER,
	// whose senders were acked at buffering time. Both places that send such
	// spans report there (sendOwned — the decision loop's final attempt, and
	// a push carrying keeps a bound forced out early), and it is the only
	// line that names them. Sharing one gate let the harmless line — emitted
	// from every concurrent receive goroutine, so far more frequent — claim
	// the slot and suppress the loss report for the whole window, leaving an
	// operator reading a log that says the senders have it covered while
	// buffered spans are being dropped. Same rule, and the same reason, as
	// the tailer's unresolved-file and metadata-budget pair.
	nackWarn logdedupe.Throttle
	lossWarn logdedupe.Throttle
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
		earlyEvery: earlyWarnEvery,
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
	for r := reasonNone + 1; r < numEarlyReasons; r++ {
		b.earlyCount[r] = obs.TailSampleEarly.WithLabelValues(r.String())
	}
	return b, nil
}

// policyLabel renders an unattributed decision as tailsample.NoPolicyLabel. ""
// would render as an empty label value, which in Prometheus is
// indistinguishable from the series not having the label at all.
func policyLabel(name string) string {
	if name == "" {
		return tailsample.NoPolicyLabel
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
// same at-least-once trade the re-shard hop makes).
//
// Spans that came out of the BUFFER in the same send — an early decision forced
// by a bound (`mine`) — are ours, and they leave through sendOwned exactly as a
// sweep's keeps do: marked otlpexport.Own so a disk buffer spools them, retried
// sendAttempts times, on a context detached from the SENDER's cancellation (its
// deadline kept), and counted lost on the final failure with the report on the
// loss gate. A push carrying them used to get one attempt on the sender's own
// context and report through the NACK gate, so an acked trace's delivery
// guarantee depended on which path happened to decide it. The trade, stated:
// during a downstream failure such a push holds its in-flight slot for up to
// sendAttempts attempts plus ~750ms of backoff — bounded, and back-pressure on a
// push that is failing anyway. (A few of the `mine` spans may in fact be
// recoverable: an early-decided trace can include spans from THIS push, which
// the retry will re-deliver against the cached verdict. Counting them lost
// over-reports a rare corner — a bound binding in the same instant the collector
// fails — in the direction that does not hide loss.)
//
// Spans decided ON ARRIVAL inside this push — every new trace once the shutdown
// Flush has latched the buffer, and the spans that carry no trace id at all —
// ride out in the same payload but are the SENDER's: both their tallies (kept
// and dropped) wait for this push's ack, they are never marked otlpexport.Own
// and never counted lost, because a NACK makes the sender retransmit them (a
// latched trace's retransmission then follows the verdict take() cached, as
// ordinary late spans; an id-less one is judged afresh). The straggler's ack is
// thereby honest: a 200 means its keeps went out on this very push.
func (b *Buffer) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	if td.SpanCount() == 0 {
		return nil
	}
	r := b.take(td)
	if r.out.spans > 0 {
		if r.mine > 0 {
			if err := b.sendOwned(ctx, &r.out, r.mine); err != nil {
				// sendOwned counted the `mine` spans lost and reported on the
				// loss gate. The rest of the payload is the sender's (late and
				// arrival-decided spans): NACK it, and its retransmission
				// re-presents them. Nothing else is tallied — see below.
				return err
			}
		} else if err := b.next.ExportTraces(ctx, r.out.td); err != nil {
			// Only the sender's spans rode this send, so nothing is lost here:
			// the NACK makes the sender retransmit them. No tally moves —
			// "kept" did not land, and "lost" for spans the sender still holds
			// would be the over-report, not the honesty.
			b.warn(&b.nackWarn, "forwarding tail-sampled spans failed; the push is NACKed and the sender's retry re-presents them (nothing is lost here)",
				"spans", r.out.spans, "error", err)
			return err
		}
	}
	// This push is acked. Every tally that describes the SENDER's spans is
	// taken only now: a NACKed push is retransmitted whole and would present
	// the same spans again, so counting at receive time tallied them once per
	// attempt. That covers the late spans (kept and dropped against a cached
	// verdict — a late-DROPPED span is not in out.td, but its push can still
	// be NACKed by a sibling's failed forward) and the spans decided on
	// arrival (heldKept/heldDropped). `mine` is counted kept here too: those
	// spans left the buffer by a decision, and this ack is what made them
	// real.
	if r.lateDropped > 0 {
		b.lateDrop.Add(float64(r.lateDropped))
	}
	if r.late > 0 {
		b.lateKept.Add(float64(r.late))
	}
	if r.heldDropped > 0 {
		b.spansDrop.Add(float64(r.heldDropped))
	}
	if r.mine+r.heldKept > 0 {
		b.spansKept.Add(float64(r.mine + r.heldKept))
	}
	return nil
}

// received is one push's walk: the payload to forward and how its spans divide
// by who still holds a copy, which decides how each is tallied and whether a
// failed forward is loss.
type received struct {
	out outbound
	// late and lateDropped are spans of already-decided traces that followed a
	// cached keep / drop verdict. The sender still holds them.
	late, lateDropped int
	// mine is the spans an early decision took out of the BUFFER (a bound
	// bound). Their senders were acked when they were buffered; nobody else
	// holds them.
	mine int
	// heldKept and heldDropped are THIS push's spans decided on arrival — after
	// the shutdown Flush latched the buffer, or because they carry no trace id
	// — kept / dropped. Like late spans the sender still holds them.
	heldKept, heldDropped int
}

// take is the receive path's whole critical section: one walk of the payload
// that buffers, classifies and enforces. See received for what it reports.
func (b *Buffer) take(td ptrace.Traces) (r received) {
	now := b.now()

	b.mu.Lock()
	defer b.mu.Unlock()

	// unkeyed is this push's pseudo-trace of spans with no trace id. The
	// OTLP-invalid all-zero id names no trace, so it is never buffered past
	// this walk and never cached: keyed like any other id, it merged the
	// id-less spans of unrelated senders into one trace, judged them once, and
	// cached that verdict under the zero id for decisionCacheTTL — so every
	// id-less span from ANY sender in that window followed a stranger's verdict
	// (an ERROR span dropped because another sender's OK span was judged
	// first). The resharder keeps such spans local rather than hashing them
	// (servicegraph SpansUnkeyed), which is how they reach this buffer at all.
	// Parked in the buffer like the post-Flush traces below, so the grouping
	// and the decision are the ordinary ones, and decided at the end of the
	// walk on exactly the spans this push carried.
	var unkeyed *bufTrace
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
				if id.IsEmpty() {
					// No enforce, for the post-Flush reason below: the entry is
					// transient inside this critical section, and a bound
					// firing on it would classify the sender's own spans as
					// `mine`.
					unkeyed = b.add(id, sp, rs, ss, now)
					continue
				}
				if keep, ok := b.cache.get(id, now); ok {
					// A span for a trace already judged. Deciding it again would
					// re-charge rateLimiting and could answer differently, so the
					// cached verdict is applied instead of a second Decide.
					if !keep {
						r.lateDropped++
						continue
					}
					if !haveRS {
						lateRS = r.out.dest().ResourceSpans().AppendEmpty()
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
					r.out.spans++
					r.late++
					continue
				}
				// Not decided (or the verdict aged out of the cache, in which
				// case this starts a FRESH window — holding only the stragglers,
				// since the first decision already moved the original spans
				// out — and the trace may be decided a second time, which
				// re-charges only the rateLimiting/composite buckets the earlier
				// decision did not SPEND, for as long as the cache still holds
				// its entry; see decide and cache.go's two lifetimes).
				e := b.add(id, sp, rs, ss, now)
				if b.flushed {
					// Post-Flush the span is only PARKED here until the end of
					// this walk, where its trace is decided on the spans
					// present (the loop below). enforce is skipped: the
					// occupancy is transient inside this critical section and
					// bounded by one push (the listeners cap payload bytes),
					// and a bound firing here would classify this push's spans
					// as `mine` — owned, spooled, counted lost on a failure —
					// when their sender in fact still holds every one of them.
					continue
				}
				r.mine += b.enforce(&r.out, e, now)
			}
		}
	}
	if b.flushed {
		// The final Flush has run, so nothing will ever drain this buffer
		// again: decide everything the walk above parked, each trace on the
		// spans this push carried for it. Every trace here IS from this push —
		// the buffer was empty when the walk began (Flush drained it, and
		// every earlier post-Flush take ended in this same loop) — so the
		// verdicts ride the push's own ack like late spans: a NACKed sender
		// retransmits, and the retransmission follows the verdicts cached here.
		// That is what keeps a straggler's ack honest and the buffered-spans
		// gauges at zero after Flush. The unkeyed pseudo-trace, if any, is one
		// of them.
		for e := b.frontLocked(); e != nil; e = b.frontLocked() {
			kept, dropped := b.judge(&r.out, e, now, reasonShutdown)
			r.heldKept += kept
			r.heldDropped += dropped
		}
	} else if unkeyed != nil && !unkeyed.gone {
		// Decided on arrival, not early: no window was ever going to assemble
		// more of it. (gone: a bound took it out mid-walk, and its spans are
		// then in `mine` — the take() imprecision the enforce skip above
		// otherwise avoids, reachable only when it was the FIFO's front.)
		kept, dropped := b.judge(&r.out, unkeyed, now, reasonNone)
		r.heldKept += kept
		r.heldDropped += dropped
	}
	return r
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
// maxSpansPerTrace decides the trace that just reached it, on the spans it
// holds. The two FIFO bounds (maxTraces, maxSpans) decide the OLDEST trace,
// never the largest: it is the one closest to its natural decision, so judging
// it early costs the least completeness; picking the largest would need a heap
// keyed by a count that changes on every span, and would systematically punish
// deep traces — exactly the ones a tail sampler exists to catch.
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

// decideOldest decides the front of the arrival FIFO (frontLocked). It reports
// whether it found a live trace at all.
func (b *Buffer) decideOldest(out *outbound, now time.Time, reason earlyReason) (int, bool) {
	e := b.frontLocked()
	if e == nil {
		return 0, false
	}
	return b.decide(out, e, now, reason), true
}

// frontLocked returns the oldest LIVE trace in the arrival FIFO, or nil when
// none is left, advancing head past the slots of traces already decided out
// from under it. It is the one walk every consumer of the FIFO's front shares
// (take's post-Flush loop, decideOldest, decideChunkLocked). Called with the
// mutex held.
//
// It does not consume the slot it returns, and needs not: every caller DECIDES
// that trace next, judge always remove()s it, and remove marks it gone — so the
// next call steps over it like any other decided slot, and compact() treats a
// gone slot at head the same as one behind it. A caller that returned without
// deciding would get the same trace back, which is the right answer.
func (b *Buffer) frontLocked() *bufTrace {
	for ; b.head < len(b.order); b.head++ {
		if e := b.order[b.head]; !e.gone {
			return e
		}
	}
	return nil
}

// decide judges one trace (judge) and tallies a DROP at once. It returns the
// number of spans that left the buffer into out (0 for a drop).
//
// Every caller but take()'s arrival-decided traces uses it: those spans are
// the SENDER's, so their drop tally has to wait for the push's ack like
// everything else about them (ExportTraces), and they call judge directly.
func (b *Buffer) decide(out *outbound, e *bufTrace, now time.Time, reason earlyReason) int {
	kept, dropped := b.judge(out, e, now, reason)
	if dropped > 0 {
		b.spansDrop.Add(float64(dropped))
	}
	return kept
}

// judge decides one trace, removes it from the buffer, remembers the verdict
// for late spans, and appends the spans to out when the verdict is keep. It
// returns how many spans left into out (kept) or were discarded (dropped); one
// of the two is always zero. It tallies everything about the DECISION (the
// policy and early counters) and nothing about the SPANS' fate, which is the
// caller's.
//
// reason names the bound that forced an early decision, or is reasonNone for
// a decision made because the window elapsed. A trace whose window has ALREADY
// elapsed is judged on its full window whatever the caller passed — it was only
// waiting for the next sweep tick — so it is not counted early: a bound
// catching a due trace (maxSpans between R*decisionWait and R*(decisionWait +
// tick), or a backlog during a chunked drain) would otherwise inflate
// kubescrape_tail_sampling_early_decisions_total and the "slow traces can be
// missed" warning with traces that missed nothing. decideChunkLocked's
// shutdown arm makes the same distinction.
func (b *Buffer) judge(out *outbound, e *bufTrace, now time.Time, reason earlyReason) (kept, dropped int) {
	if reason != reasonNone && now.Sub(e.first) >= b.set.wait {
		reason = reasonNone
	}
	// The all-zero trace id names no trace (take()'s unkeyed pseudo-trace), so
	// it has no verdict to remember and no earlier charge to read: caching
	// either would hand one sender's verdict to every other id-less span.
	keyed := !e.id.IsEmpty()
	var charged tailsample.ChargedMask
	if keyed {
		charged = b.cache.charged(e.id)
	}
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
	d := b.ev.Decide(tailsample.Trace{TraceID: e.id, Spans: b.scratch, Charged: charged})
	// The evaluator retains nothing (tailsample.Trace says so), so every element
	// is dead the instant Decide returns — and they are HANDLES, not values: each
	// one pins a whole span message plus its group's resource attributes. `[:0]`
	// alone leaves the tail of the largest decision so far pointing into a trace
	// this call is about to release (the drop branch below releases e.td for
	// exactly that reason), and a workload of small traces would then never
	// overwrite it. clear() zeroes [0,len) and the refill above starts from the
	// front, so nothing past a decision's own fill is ever live. clear()
	// allocates nothing, and neither does the rest of a decision beyond
	// amortized slice growth (TestDropDecisionAllocationBudget,
	// TestKeepDecisionAllocationBudget).
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
	if reason != reasonNone {
		b.earlyCount[reason].Inc()
		b.earlyPending[reason]++ // reported by the sweep, never from here
	}

	b.remove(e)
	if keyed {
		// d.Charged, not "we decided it": a re-decision reads this back as
		// Trace.Charged, and only a budget that actually moved may be skipped.
		b.cache.put(e.id, d.Sampled, d.Charged, now)
	}
	if !d.Sampled {
		dropped = e.spans
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
		//
		// The ZERO value, not ptrace.NewTraces(): nothing reads a gone entry's
		// td again (every FIFO walk skips gone slots, and add() only ever
		// reaches a live entry through the map; a future read would panic on
		// the nil message, which is the loud failure wanted over a silently
		// empty payload), and NewTraces was 2 of the 3 allocations of every
		// dropped trace's decision — under the mutex every receiver shares, on
		// the verdict a tail sampler reaches most
		// (TestDropDecisionAllocationBudget).
		e.td = ptrace.Traces{}
		return 0, dropped
	}
	kept = e.spans
	e.td.ResourceSpans().MoveAndAppendTo(out.dest().ResourceSpans())
	out.spans += kept
	return kept, 0
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
// exports the keeps — then leaves the buffer LATCHED: a straggler push that
// outlives the receivers' graceful stop (an active handler http.Server.Shutdown
// never interrupts, an RPC GracefulStop was still waiting on when the join
// budget expired) is decided inside its own take() rather than buffered where
// nothing will ever flush it. It is the graceful-shutdown path: it is what
// bounds the loss this layer's contract admits to a hard kill (see the package
// doc), so it runs after the receivers have been ASKED to stop and before the
// exporter closes.
func (b *Buffer) Flush(ctx context.Context) { b.drain(ctx, true) }

// drain decides the due traces (or all of them) and sends the keeps as ONE
// payload. Deciding happens under the mutex — in CHUNKS, so one sweep is not
// one hold (see decideChunk) — and the send never does.
func (b *Buffer) drain(ctx context.Context, all bool) {
	if b == nil {
		return
	}
	now := b.now()
	var out outbound

	b.mu.Lock()
	if all {
		// The final flush LATCHES the buffer before it drains: nothing runs a
		// flush after this one, so from here take() must never buffer again —
		// a post-Flush push's traces are decided inside its own take (see the
		// package doc's shutdown bullet). Set under the same mutex the receive
		// path holds, a push either lands before this drain (which then
		// decides it) or sees the latch; there is no in-between where spans
		// re-fill a buffer nobody will empty.
		b.flushed = true
	}
	// CHUNKED, on the SWEEP path: the mutex is dropped and re-taken every
	// decideChunk decisions, so one tick's whole due set is not a single hold.
	// Deciding still happens under the mutex (the evaluator reads b.scratch,
	// and remove() edits the map and the FIFO), but a decision can be
	// arbitrarily expensive — a `type: script` policy is a Starlark call — and
	// the receive path takes this same mutex for every push. Holding it across
	// a backlog's worth of decisions once per tick is the stall
	// internal/agent/servicegraph and internal/agent/spanmetrics already chunk
	// their renders out of.
	//
	// Releasing it mid-sweep is safe because nothing here is carried across
	// the gap in a form a concurrent push could invalidate: head and order are
	// re-read every iteration (a push may append, and remove()/compact() may
	// rewrite the prefix and reset head), `now` is sampled once ON PURPOSE so
	// the due set stays the one this drain started with, and out is this
	// goroutine's alone. A push landing in the gap buffers a trace this drain
	// has not reached, which the next tick decides.
	//
	// The gap has to be a real one, which is what the runtime.Gosched() is
	// for. sync.Mutex in normal mode lets the goroutine that unlocks re-acquire
	// ahead of the waiter it just woke (the waiter is only made runnable), so
	// an Unlock immediately followed by a Lock usually hands the next chunk to
	// this goroutine again and a push gets in only once it has waited past the
	// mutex's 1ms starvation threshold — measured at ~4ms worst-case push
	// waits against ~0.2-0.8ms with the yield. Yielding runs the woken waiter
	// (it sits in this P's runnext) before this goroutine re-takes the lock.
	// The cost is sweep throughput under contention, which is the priority
	// this chunking exists to set: receive latency over sweep latency.
	//
	// The FLUSH path is deliberately ONE hold. Its first act was to latch the
	// buffer, and take() answers that latch by deciding, itself, every trace
	// it finds — on the assertion that a latched buffer holds only what THAT
	// push just put there. A gap here would falsify it: a straggler landing
	// mid-flush would sweep up other senders' already-acked, still-undecided
	// traces and hand their spans out on ITS ack, where they are neither
	// marked owned (so a disk buffer never sees them) nor counted lost if the
	// push is NACKed. Shutdown is also the one moment contention does not
	// matter — the receivers have already been asked to stop.
	for !b.decideChunkLocked(&out, now, all) {
		if all {
			continue
		}
		b.mu.Unlock()
		runtime.Gosched()
		b.mu.Lock()
	}
	early, report := b.takeEarlyLocked()
	b.mu.Unlock()

	// Outside the mutex: the report must never hold the lock every concurrent
	// receive goroutine needs.
	if report {
		b.reportEarly(early)
	}

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
	//
	// Timed, because this send is the one thing that stops the loop deciding:
	// its duration rides on the early-decision line (slowestExport).
	start := b.now()
	err := b.sendOwned(ctx, &out, out.spans)
	b.noteExport(b.now().Sub(start))
	if err == nil {
		b.spansKept.Add(float64(out.spans))
	}
}

// sendOwned sends a payload carrying `owned` spans that came out of the BUFFER
// — spans whose senders were acked at buffering time, so nobody else holds a
// copy — and returns the final error. It is the ONE delivery path for such
// spans, used by the decision loop's drain and by a push that carried keeps a
// bound forced out early, so an acked trace gets the same guarantee whichever
// path decided it:
//
//   - the payload is marked otlpexport.Own, so a disk buffer spools it;
//   - the send runs on sendContext: the caller's DEADLINE without its
//     CANCELLATION, because a cancelled send of the only copy is loss, not a
//     retry;
//   - it is retried (sendRetry), a permanent rejection excepted;
//   - on the final failure the owned spans are counted lost and reported on
//     the LOSS gate, which the NACK line can never starve.
//
// out may carry spans that are NOT owned (a push's late spans ride along);
// they are the sender's, the caller NACKs the push, and they are neither
// counted lost here nor reported as such.
//
// One known over-report, accepted: downstream this send may be COMPOSITE (a
// routing fan-out, an otlpsplit into size-bounded parts), and a failure after
// some shares landed still counts every owned span into {outcome="lost"} —
// at-least-once delivered those shares for real, so the lost counter is an
// upper bound on loss, never an under-count.
func (b *Buffer) sendOwned(ctx context.Context, out *outbound, owned int) error {
	sctx, cancel := sendContext(ctx)
	defer cancel()
	err := b.sendRetry(otlpexport.Own(sctx), out.td)
	if err != nil {
		b.spansLost.Add(float64(owned))
		b.warn(&b.lossWarn, "exporting tail-sampled traces failed on the final attempt; the spans that came out of the buffer are dropped from this process (their senders were acked at buffering time) and counted lost — an upper bound: under a routed or size-split export, earlier shares may already have been delivered or spooled. Any of a push's own spans riding the same send are NACKed back to their sender",
			"spans", out.spans, "buffered", owned, "error", err)
	}
	return err
}

// noteExport folds one decision-loop export's duration into slowestExport.
// A CAS loop rather than a mutex: it runs after the send, and the shutdown
// Flush can race a Run sweep here.
func (b *Buffer) noteExport(d time.Duration) {
	for {
		cur := b.slowestExport.Load()
		if int64(d) <= cur || b.slowestExport.CompareAndSwap(cur, int64(d)) {
			return
		}
	}
}

// decideChunk bounds ONE lock hold of a drain to that many decisions.
//
// Smaller than servicegraph's and spanmetrics' snapChunk (512), deliberately:
// their unit is a value copy out of a map and this one is a POLICY
// EVALUATION, which for `type: script` is a Starlark call. The lock/unlock
// pair around each chunk costs tens of nanoseconds against at least that many
// microseconds of decisions, so the only thing the number really trades is
// how long a concurrent push can wait — 64 pairs to drain a backlog of 8192
// is free.
const decideChunk = 128

// decideChunkLocked decides at most decideChunk traces and reports whether the
// drain is FINISHED (nothing left, or the next trace is not due). Called with
// the mutex held and returns with it held, so the caller's loop is a plain
// unlock/lock between chunks — or, on the flush path, no unlock at all.
//
// Slots of already-decided traces are skipped without spending the budget:
// they cost a pointer test, and the budget exists to bound the expensive thing
// — Decide.
func (b *Buffer) decideChunkLocked(out *outbound, now time.Time, all bool) bool {
	for range decideChunk {
		e := b.frontLocked()
		if e == nil {
			return true
		}
		reason := reasonNone
		if now.Sub(e.first) < b.set.wait {
			if !all {
				// The FIFO is deadline-ordered: nothing behind it is due.
				return true
			}
			// Judged before its window closed because the process is stopping.
			reason = reasonShutdown
		}
		b.decide(out, e, now, reason)
	}
	return false
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

// warn logs at most once per warnEvery on ITS OWN gate. The gate is a
// parameter rather than a field of the Buffer because the callers report two
// different events about one condition — a NACK and a loss — and the loss
// report must never be starved by the harmless one; see the fields.
func (b *Buffer) warn(gate *logdedupe.Throttle, msg string, args ...any) {
	if !gate.Allow(warnEvery) {
		return
	}
	b.log.Warn(msg, args...)
}
