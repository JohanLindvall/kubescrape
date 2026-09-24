// Package events watches Kubernetes Events and exports them as OTLP log
// records, enriched with the involved object's Kubernetes identity so an
// OOMKilled or FailedScheduling event lands on the SAME resource attributes
// as that pod's logs and metrics — which is the whole point of collecting
// them here rather than as a separate stream.
//
// It is a cluster-singleton: exactly one replica may run it (see
// internal/leader), because N watchers would emit N copies. Delivery mirrors
// journald's: batch, export, and only after the collector acknowledges the
// batch advance and persist the position.
//
// # The resume window is bounded by the API server, not by us
//
// The "position" is a resourceVersion, which is only resumable while it is
// still in the API server's watch history — minutes, not hours. Past that the
// watch fails Gone and the only recovery is a fresh list, filtered by the
// persisted watermark. So a checkpoint here buys exact resumption across
// restarts, rolling updates and leader handover (the common cases) and
// explicitly cannot buy durability across a long outage. Events themselves
// also expire from the API (--event-ttl, an hour by default).
package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/backoff"
	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
	"github.com/JohanLindvall/kubescrape/internal/leader"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// Start modes for a cold start (no stored position), mirroring the tailer's
// -logs-unknown-files.
const (
	// StartEnd skips the backlog: take a resourceVersion and watch from there.
	StartEnd = "end"
	// StartBeginning replays everything still within the event TTL.
	StartBeginning = "start"
	// StartAuto resumes a stored position, else behaves like StartEnd.
	StartAuto = "auto"
)

// LogExporter sends one OTLP logs payload.
type LogExporter interface {
	ExportLogs(ctx context.Context, ld plog.Logs) error
}

// MetadataSource resolves pod metadata for enrichment; implemented by
// metaclient.Client.
type MetadataSource interface {
	PodByName(ctx context.Context, namespace, name string) (*kubemeta.Pod, error)
}

// Config configures the reader.
type Config struct {
	Client    kubernetes.Interface
	Positions PositionStore
	// StartMode is auto | end | start (see the Start* constants).
	StartMode string
	// Namespace restricts the watch (empty = cluster-wide).
	Namespace string
	// StopBudget bounds the final flush and position write after ctx is
	// cancelled. It MUST stay below the lease renew deadline the caller allows
	// this work to stop within, or a slow shutdown is reported as "did not
	// stop" and fails the process. Zero uses half the leader default.
	StopBudget time.Duration

	// BatchSize caps the batch by COUNT only — deliberately no twin of
	// journald's MaxBatchBytes: events are small (a message plus a handful of
	// metadata fields, nothing like a megabyte journal record), and the hard
	// per-payload wire bound lives in the exporter regardless
	// (otlpexport.Config.MaxSendBytes / pkg/otlpsplit).
	BatchSize     int
	FlushInterval time.Duration
	// PersistInterval rate-limits position writes: a write per event would be
	// an API-server write per event. The interval is therefore the bound on
	// how much gets REPLAYED after a hard kill (bounded duplicates, never
	// loss); a graceful stop always writes a final position.
	PersistInterval time.Duration

	// Meta resolves the involved pod so events share its resource identity.
	Meta MetadataSource
	// Chain is the per-record log chain the tailer and journald run too — the
	// same levers, in the same order (logchain.Config). Chain.Scrub is applied
	// where an event is ingested into the batch, before the record exists
	// (ingest), not by the chain.
	Chain logchain.Config

	Attrs *attrs.Builder

	Exporter LogExporter
	Logger   *slog.Logger
	// RestartBackoff is the initial delay before re-establishing a failed
	// watch, doubled to a 30s cap.
	RestartBackoff time.Duration
}

// Reader watches events and exports them. All fields are owned by the single
// Run goroutine.
type Reader struct {
	cfg Config
	log *slog.Logger

	batch []entry
	// pending is the batch's OTLP rendering, held across export retries under
	// logchain.Pending's convert-once/clear-with-the-batch discipline: cleared
	// with the batch (settle) and on a stream restart that re-reads it.
	pending logchain.Pending
	// rendered is the number of LEADING batch entries the current pending
	// payload covers, frozen when convert runs. On a redelivers=false restart
	// (cold, before the first commit) the batch and its rendering are retained
	// while the new watch appends FRESH entries past index `rendered` — those
	// are not in the exported payload, so settle must commit only over the
	// covered prefix and keep the tail, or a flush would advance the position
	// (and the watermark) past events it never exported, losing them silently.
	rendered int
	// committed is the position every exported batch has reached; pending is
	// the newest position SEEN (bookmarks included) but not yet exported.
	committed Position
	pendingRV string
	// seenRV is the highest revision this PROCESS has observed, whether or not
	// anything has been acked. It is the resume point for a cold stream restart
	// before the first commit — see startResourceVersion — and is deliberately
	// never persisted, so the ConfigMap position stays strictly ack-gated.
	seenRV string
	// now is the clock every pacing decision reads — the watermark clamp, the
	// flush retry (flushDue), the position cadence (persist), the export and
	// position-write re-warns, the pod-lookup pause and the failed-resolution
	// memo — injectable for tests (the store.now pattern) so none of them needs
	// a sleep to pin.
	now         func() time.Time
	lastPersist time.Time
	// lastSaved is the position this leadership term last WROTE, and
	// savedThisTerm whether it has written one; persist skips a write that
	// would record nothing new. Reset by Run, which each term calls.
	lastSaved     Position
	savedThisTerm bool
	// flushTicker is the "flush at least this often" ticker, held on the
	// Reader so tryFlush can Reset it after every flush — the interval is
	// measured from the LAST FLUSH, not the last tick. A fixed-period ticker
	// fires exactly FlushInterval after the PREVIOUS tick, i.e. microseconds
	// BEFORE that interval has elapsed since the flush that tick caused, so the
	// old "tick AND time.Since(lastFlush) >= interval" guard could never pass
	// on the tick it was meant for and delivery ran at ~2× the configured
	// interval. Same trap and same fix as journald's stream loop. nil during
	// replayBacklog (before the watch/ticker exist), so tryFlush guards it.
	flushTicker *time.Ticker
	// flushFailedAt is when the last export attempt failed, zero once one
	// succeeds. Past BatchSize the batch is RETAINED, so the count trigger
	// holds forever while the collector is down; pacing the retry against this
	// keeps it to one attempt per FlushInterval instead of one per event.
	flushFailedAt time.Time
	// exportOutage narrates a run of failed exports: the FIRST failure of a
	// run always warns, the repeats at most every flushWarnEvery, and the
	// recovery line sizes the run (failed attempts, and how long since the
	// first) the way journald's flushRetry and the tailer's failBatch report
	// theirs. obs.EventsExportFailures carries the magnitude.
	exportOutage logdedupe.Outage
	// replaying is set while the current stream is consuming the REPLAY LIST —
	// the backlog re-read that recovers a position we no longer have (see
	// replayBacklog). That is the ONLY situation in which the watermark filter
	// belongs: a replay re-reads events already exported, a positioned or live
	// watch never does. See wanted.
	//
	// It is per-STREAM and is NOT cleared by a commit. Clearing it there
	// disarmed the filter after the first acked batch — a couple of seconds
	// into a replay that delivers the whole event TTL — so the rest of the
	// backlog re-exported as duplicates, each Pod event costing a PodByName
	// lookup. It IS cleared when the last page has been read: everything the
	// watch that follows delivers is newer than the list's snapshot, and
	// filtering THAT against a boundary frozen at stream start drops, for the
	// rest of the watch, every event from a reporter whose clock trails it by
	// more than replaySlack.
	replaying bool
	// replayFrom is the watermark the CURRENT replay filters against, frozen
	// when the stream started.
	//
	// It cannot be committed.Watermark, which every flush advances to its
	// batch maximum: a relist delivers the backlog in STORE order, not time
	// order, so the first acked batch raised the live watermark past events
	// still to come and wanted() dropped them as "already exported" when they
	// never were. That destroyed exactly the gap the relist exists to recover,
	// silently and uncounted, and settle's committed max meant a restart could
	// not reach it either. Frozen at stream start, the filter answers the only
	// question it should: was this event already exported BEFORE this replay
	// began?
	replayFrom time.Time
	// relist forces the next stream to start from "" (the full TTL backlog)
	// after the API server dropped our resourceVersion (see expire), or after
	// the stored position could not be READ (see loadPosition). Both mean the
	// same thing — we do not know where the last delivery stopped — and a
	// replay is the only answer to that which cannot lose the gap.
	relist bool
	// replaySecured: the current replay's backlog is known FULLY exported, so
	// the position may advance again. Trivially true for a positioned or live
	// stream, and for a Reader that has not started one (New).
	//
	// While a replay is unsecured, settle HOLDS the position: the backlog
	// arrives in STORE order, so no commit made mid-replay bounds what is
	// still to come — committing the rendered prefix's maximum RV positioned
	// a restarted stream PAST undelivered lower-RV backlog entries (and the
	// advanced watermark made the next replay's wanted() drop the
	// undelivered older-timestamp remainder), silent and uncounted loss on
	// exactly the recovery path. The exported high-water accumulates in
	// heldWatermark instead and folds in when the replay secures; the cost
	// of a death mid-replay is a full re-replay — duplicates, which
	// at-least-once already tolerates, instead of loss.
	//
	// THE SECURING CONDITION IS OURS, NOT THE API SERVER'S. It used to be a
	// watch BOOKMARK, on the theory that one arrives once the watcher is caught
	// up. Bookmarks are discretionary by contract — "Servers that do not
	// implement bookmarks may ignore this flag ... nor may [clients] assume the
	// server will send any BOOKMARK event during a session"
	// (metav1.ListOptions.AllowWatchBookmarks) — and for core/v1 events
	// kube-apiserver serves the watch straight from etcd, with no watch cache,
	// which is where bookmarks come from. So none ever arrived: after the FIRST
	// relist the hold was permanent, the position ConfigMap never advanced
	// again, and every restart replayed the whole event TTL, forever. The
	// backlog is a paginated LIST now (replayBacklog) and its exhaustion plus
	// replayOwed == 0 is the proof — a fact this process observes rather than a
	// signal it waits for. A bookmark, where a cluster does send one, still
	// secures: it is a strictly stronger statement.
	replaySecured bool
	// replayRV is the revision of the LIST snapshot the current replay read. A
	// consistent list returns every event that existed at that revision, and
	// the watch that follows starts there, so it is a hard boundary: the
	// position the replay commits when it secures. The maximum over ingested
	// items cannot be that boundary — a list arrives in KEY order.
	replayRV string
	// replayOwed is how many LEADING batch entries came from the replay list
	// and have not settled yet. Entries are appended in order and the batch is
	// emptied before the list starts, so the owed ones are always a prefix;
	// settle and shedOldest are the only things that remove them.
	replayOwed int
	// heldWatermark is the wall-clamped high-water of what unsecured-replay
	// flushes exported, folded into the committed watermark by secureReplay.
	// It survives stream restarts: it only ever names exported entries.
	heldWatermark time.Time
	// overflowWarned rate-limits the shedOldest warning to one per OUTAGE:
	// obs.EventsOverflowDropped carries the ongoing magnitude, and re-arming it
	// on the flush that recovers is what makes a SECOND outage say so. Latched
	// for the process' life it reported only the first one, which on a
	// long-lived singleton is usually not the one being investigated.
	overflowWarned bool
	// unresolvedWarn throttles the "events are being exported without their
	// pod's identity" warning. The CONDITION persists for as long as the
	// metadata service is unreachable and is noticed once per distinct involved
	// object per batch (and per unresolvedRetryAfter), so it is exactly the
	// flood logdedupe exists for;
	// obs.EventsUnresolved carries the rate.
	unresolvedWarn logdedupe.Throttle
	// oddObjectWarn throttles the "the watch delivered something that is not an
	// Event" warning — a should-not-happen branch that, if it ever fires, fires
	// for every delivery.
	oddObjectWarn logdedupe.Throttle
	// lookupTimeout bounds one involved-pod lookup (the podLookupTimeout
	// constant when zero; a field so a test need not wait seconds), and
	// podLookupsPausedUntil is set when one runs out of it — see lookupPod.
	// lookupPauseWarn throttles the line announcing a pause: a metadata
	// service that stays hung trips one every podLookupPause.
	lookupTimeout         time.Duration
	podLookupsPausedUntil time.Time
	lookupPauseWarn       logdedupe.Throttle
	// persistOutage narrates a run of failed position writes: a Warn on the
	// first failure, a throttled re-warn (positionWarnEvery) while it
	// persists, and an Info on recovery carrying how many writes it cost and
	// how long it lasted.
	persistOutage logdedupe.Outage
	// resCache memoizes the involved object's resource for the life of the
	// batch (a failed pod resolution only for unresolvedRetryAfter), keyed by
	// the involved object's identity. See resource().
	resCache map[string]resEntry
	// observed is the set of batch entries the per-record chain has already
	// run over — the positional proof that lets a re-ingested event say
	// logchain.Input.Observed. See obsKey and markObserved.
	observed map[obsKey]struct{}
}

// entry is one event, already converted to the fields the record needs.
type entry struct {
	body     string
	ts       time.Time
	severity plog.SeverityNumber
	sevText  string
	// resource identifies the involved object; records group by it.
	resKey string
	res    pcommon.Resource
	// meta is the record attributes describing the event itself.
	meta eventMeta
	// rv is the event's revision. With ts (its occurrence time, eventTime)
	// it is the event's contribution to the position flush commits.
	rv string
	// okey identifies the occurrence for the observed set (see obsKey).
	okey obsKey
}

// New creates a Reader.
func New(cfg Config) *Reader {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 512
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 2 * time.Second
	}
	if cfg.PersistInterval <= 0 {
		cfg.PersistInterval = 10 * time.Second
	}
	if cfg.RestartBackoff <= 0 {
		cfg.RestartBackoff = time.Second
	}
	if cfg.StartMode == "" {
		cfg.StartMode = StartAuto
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	// replaySecured starts TRUE: no replay is in flight, so nothing holds the
	// position. The zero value would hold every commit of a Reader whose stream
	// never ran (every direct-drive test, and the window before the first
	// stream() call).
	return &Reader{cfg: cfg, log: cfg.Logger, now: time.Now, replaySecured: true}
}

// publishMetrics gives every counter this pipeline owns a series at zero, so
// ABSENT means "this replica is not running the events reader" (it does not
// hold the lease) and 0 means "running, nothing has happened". Without it a
// pipeline that has never lost, dropped or relisted anything is
// indistinguishable from one that is not there — and every alert written
// against those counters silently matches nothing on a healthy leader, which
// is precisely when the operator wants to see the zero.
//
// It runs from Run, i.e. exactly when the reader RUNS, following
// obs.RegisterSelfMetadata's rule: a follower replica must NOT publish a flat
// zero export rate it is not responsible for.
func publishMetrics() {
	obs.EventsExported.Add(0)
	obs.EventsDropped.Add(0)
	obs.EventsDroppedRecords.Add(0)
	obs.EventsOverflowDropped.Add(0)
	obs.EventsExportFailures.Add(0)
	obs.EventWatchRestarts.Add(0)
	for _, t := range eventTypeLabels {
		obs.EventsObserved.WithLabelValues(t).Add(0)
	}
	for _, stage := range []string{stageWatch, stageReplay} {
		obs.EventRelists.WithLabelValues(stage).Add(0)
	}
	// Only the WATCH stage can discard a gap: a replay-stage expiry always has
	// a relist to fall back on, being one already. Publishing a series that
	// nothing can ever write would be its own small lie.
	obs.EventGapDiscarded.WithLabelValues(stageWatch).Add(0)
	for _, op := range []string{opLoad, opSave} {
		obs.EventPositionErrors.WithLabelValues(op).Add(0)
	}
	for _, reason := range unresolvedReasons {
		obs.EventsUnresolved.WithLabelValues(reason).Add(0)
	}
}

// ValidateStartMode reports an unknown start mode.
func ValidateStartMode(mode string) error {
	switch mode {
	case "", StartAuto, StartEnd, StartBeginning:
		return nil
	}
	return fmt.Errorf("invalid events start mode %q (want auto, end or start)", mode)
}

// Run watches until ctx is done, then flushes and persists what it has. It is
// the leader-only work: it must return when ctx is cancelled.
func (r *Reader) Run(ctx context.Context) {
	publishMetrics()
	// A new term writes its first position whatever it holds, so Holder names
	// this leader (persist).
	r.savedThisTerm = false
	r.loadPosition(ctx)
	bo := backoff.New(r.cfg.RestartBackoff)
	for ctx.Err() == nil {
		started := time.Now()
		err := r.stream(ctx)
		if ctx.Err() != nil {
			break
		}
		lasted := time.Since(started)
		bo.ResetIfHealthy(started)
		obs.EventWatchRestarts.Inc()
		if routineWatchClose(err, lasted) {
			// The API server ends every watch opened without TimeoutSeconds after
			// a random 30-60 minutes (--min-request-timeout), so on a healthy
			// cluster this is 24-48 times a day: steady state, not something the
			// code "handled". The resume is from the last delivered revision and
			// re-delivers, so nothing is lost across it. The counter still moves.
			r.log.Debug("the api server closed the event watch; resuming", "lasted", lasted.Round(time.Second), "backoff", bo.Delay())
		} else {
			r.log.Warn("event watch stopped; restarting", "error", err, "backoff", bo.Delay())
		}
		// Kept even after a routine close: after a healthy stream it is the 1s
		// initial delay, and dropping it would hot-loop against an API server
		// that accepts watches and closes them at once.
		bo.Sleep(ctx)
	}
	// Final flush on a DETACHED context: ctx is already cancelled, and the
	// last batch must still reach the collector before the position is
	// written (the tailer's shutdown flush does the same). WithoutCancel, not
	// a fresh Background, so any context VALUE the export chain rides on (the
	// otlpexport.Own durability marker) survives the detach — the repo-wide
	// shutdown-context invariant.
	//
	// The budget must stay BELOW the lease renew deadline the caller allows
	// this work to stop within. It was 15s against leader.DefaultRenewDeadline
	// of 10s — deterministically too long — so a slow final flush or a
	// ConfigMap write against the same unavailable API server that cost us the
	// lease made leader.Run report "leader work did not stop", which
	// startEvents treats as fatal: the process exited non-zero and took the
	// co-located -azure-diagnostics consumer with it, contradicting the leader
	// package's own contract that losing the lease must not take the process
	// down.
	//
	// The two steps get SEPARATE slices of that budget. Sharing one deadline
	// let the flush spend all of it in the exporter's retries and leave
	// persist a Get+Update on an already-expired context, failing instantly —
	// so the successor replayed up to PersistInterval of events at exactly the
	// handover the ConfigMap position exists for. Bounded replay rather than
	// loss, but it was the first thing sacrificed.
	base := context.WithoutCancel(ctx)
	budget := r.shutdownBudget()
	flushBudget := budget * 2 / 3
	fctx, cancel := context.WithTimeout(base, flushBudget)
	err := r.flush(fctx)
	cancel()
	if err != nil {
		r.log.Warn("final event flush failed", "error", err)
	}
	// Started AFTER the flush, so a fast flush does not shorten the write; the
	// two together still stay under shutdownBudget, hence under the renew
	// deadline leader.elect joins this work within.
	pctx, pcancel := context.WithTimeout(base, budget-flushBudget)
	defer pcancel()
	r.persist(pctx, true)
}

// errWatchClosed is what stream returns when the API server closed the watch
// channel cleanly — its routine per-request timeout, or (indistinguishably to
// client-go's StreamWatcher) a connection that ended with a probable EOF.
var errWatchClosed = errors.New("watch channel closed")

// routineWatchClose reports whether a stream ended the way a healthy watch
// ends: a clean server close after it ran at least backoff.Cap. A close that
// came quickly is not routine — a server accepting watches and closing them at
// once is worth a Warn — and neither is any other error (Gone, a watch.Error, a
// failed Watch call, an export failure).
func routineWatchClose(err error, lasted time.Duration) bool {
	return errors.Is(err, errWatchClosed) && lasted >= backoff.Cap
}

// shutdownBudget is the ceiling on the final flush and position write: half of
// leader.DefaultRenewDeadline unless Config.StopBudget is set. Nothing in this
// package ties it to the renew deadline the caller ACTUALLY configures, so a
// caller that sets leader.Config.RenewDeadline must set StopBudget below it, or
// a slow shutdown reads as "leader work did not stop" — startEvents derives
// both from one value for exactly that reason.
func (r *Reader) shutdownBudget() time.Duration {
	d := r.cfg.StopBudget
	if d <= 0 {
		d = leader.DefaultRenewDeadline / 2
	}
	return d
}

// noteSeen advances the in-process resume point. Unlike committed.ResourceVersion
// this is NOT ack-gated: it only decides where a cold restart of the watch
// resumes within one process lifetime, and resuming too early costs duplicates
// while resuming too late loses events outright.
func (r *Reader) noteSeen(rv string) {
	if rv != "" && newerRV(rv, r.seenRV) {
		r.seenRV = rv
	}
}

// newerRV reports whether a is a later resourceVersion than b. Kubernetes
// treats these as opaque, but etcd's are decimal integers, so compare
// numerically where both parse and fall back to keeping what we have (the
// conservative direction: never move the position backwards on a guess).
func newerRV(a, b string) bool {
	if b == "" {
		return true
	}
	ai, aerr := strconv.ParseUint(a, 10, 64)
	bi, berr := strconv.ParseUint(b, 10, 64)
	if aerr != nil || berr != nil {
		return false
	}
	return ai > bi
}

// stream establishes one watch and consumes it until it ends.
func (r *Reader) stream(ctx context.Context) error {
	start, err := r.startResourceVersion(ctx)
	if err != nil {
		return err
	}
	// A replay re-reads the backlog by LIST before the watch; anything else is
	// positioned exactly by the API server and delivers only what follows.
	r.replaying = start.replay
	r.replaySecured = !start.replay
	r.replayRV, r.replayOwed = "", 0
	// Snapshot, not a live read: see replayFrom.
	//
	// With SLACK, because the boundary is a maximum over timestamps written by
	// different components with unsynchronised clocks and two different
	// precisions (metav1.Time is second-truncated, MicroTime is not). A strict
	// comparison against that maximum drops never-exported events on the one
	// path whose purpose is recovering them — an event stamped 10:00:00Z by a
	// reporter whose clock lags, arriving after one stamped 10:00:00.9, is
	// below the boundary and is silently discarded. The documented bias is
	// toward duplicates over loss, and duplicates are what at-least-once
	// already tolerates.
	r.replayFrom = r.committed.Watermark
	if !r.replayFrom.IsZero() {
		r.replayFrom = r.replayFrom.Add(-replaySlack)
	}
	if start.redelivers && len(r.batch) > 0 {
		// Everything buffered is AFTER the resume point (entries only outlive a
		// flush that failed, and the position never advanced past them), so
		// this stream delivers every one of them again — a positioned watch
		// re-sends them, and a replay's list snapshot still contains them.
		// Keeping the batch would duplicate the
		// whole backlog once per restart and grow memory without bound across
		// a long collector outage; dropping it loses nothing — the entries
		// are re-ingested from the re-delivery. Only the cold skip-backlog
		// path (redelivers=false) starts past the buffered entries and must
		// retain them.
		clear(r.batch)
		r.batch = r.batch[:0]
		clear(r.resCache)
		// The observed set is deliberately NOT cleared with them: those entries
		// are about to be re-ingested and it is the only record that the chain
		// already counted them, which is what keeps a collector outage from
		// multiplying the operator's log metrics by the number of restarts it
		// spans. It retires per entry instead, as the re-delivered batch settles
		// or sheds (forgetObserved).
		//
		// The converted payload described the batch just dropped; the watch is
		// about to re-deliver those entries and they will convert afresh
		// (logchain.Pending's restart-clear case — the loss journald had for
		// the same reason).
		r.dropRendering()
		r.pendingRV = "" // a bookmark from the dead stream vouches only for its own deliveries
	}
	rv := start.rv
	if start.replay {
		if rv, err = r.replayBacklog(ctx); err != nil {
			return err
		}
	}
	w, err := r.cfg.Client.CoreV1().Events(r.cfg.Namespace).Watch(ctx, metav1.ListOptions{
		ResourceVersion: rv,
		// Bookmarks advance the resourceVersion on an idle cluster, so the
		// persisted position stays inside the API server's watch window and a
		// restart resumes instead of falling back to a relist. Nothing DEPENDS
		// on one arriving — see replaySecured; for core/v1 events none does.
		AllowWatchBookmarks: true,
	})
	if err != nil {
		if isExpired(err) {
			r.expire(stageWatch)
			return fmt.Errorf("watch from %q expired: %w", rv, err)
		}
		return err
	}
	defer w.Stop()

	ticker := time.NewTicker(r.cfg.FlushInterval)
	r.flushTicker = ticker
	defer func() {
		ticker.Stop()
		r.flushTicker = nil
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// tryFlush Resets the ticker on success, so the cadence is measured
			// from the last flush (whatever triggered it), not from this tick.
			if len(r.batch) > 0 {
				r.tryFlush(ctx)
			}
			// Also on an EMPTY-batch tick: a stream that goes quiet after a
			// bookmark still has a position worth writing, and tryFlush (which
			// persists too) does not run here.
			r.persist(ctx, false)
		case ev, ok := <-w.ResultChan():
			if !ok {
				return errWatchClosed
			}
			if err := r.handle(ctx, ev); err != nil {
				return err
			}
		}
	}
}

// startPoint is where one stream begins.
type startPoint struct {
	// rv positions the watch. It is empty only when replay is set, and then it
	// is replayBacklog that resolves the revision the watch actually starts at.
	rv string
	// redelivers reports whether the new stream re-sends everything the reader
	// has already buffered — true for every path except the cold
	// skip-the-backlog List, whose revision is AFTER anything currently
	// batched.
	redelivers bool
	// replay reports that the backlog must be re-read before the watch,
	// because the position we would have watched from is gone (or was never
	// taken). See replayBacklog.
	replay bool
}

// startResourceVersion resolves where this stream begins: the committed
// position, a replay of the backlog, or the start policy on a cold start.
func (r *Reader) startResourceVersion(ctx context.Context) (startPoint, error) {
	if r.committed.ResourceVersion != "" {
		return startPoint{rv: r.committed.ResourceVersion, redelivers: true}, nil
	}
	if r.relist {
		// Recovering from a Gone, not starting cold: replay everything the API
		// server still holds and let the watermark drop what we already
		// exported. Taking the CURRENT revision instead (what the cold-start
		// policy does) would silently lose every event between the expired
		// version and now — precisely the window a relist exists to cover.
		// The flag is NOT consumed here: a watch attempt that fails before
		// the replay is secured must relist again, or the gap is lost after
		// all — it clears only once the whole backlog has been exported
		// (secureReplay; settle holds all commits until then).
		return startPoint{redelivers: true, replay: true}, nil
	}
	if r.cfg.StartMode == StartBeginning {
		// Replay everything the API server still holds (the event TTL).
		return startPoint{redelivers: true, replay: true}, nil
	}
	if r.seenRV != "" {
		// A stream that already ran in this process resumes where it stopped.
		//
		// Re-Listing here instead loses everything between the dead watch and
		// the new List: the List revision is the CURRENT store revision and was
		// never retained, so each restart began after whatever happened since.
		// The window is the whole interval before the first ACKED export —
		// which is exactly a collector outage on a fresh install, since any
		// export failure tears the stream down and Run backs off and re-Lists,
		// repeating for the length of the outage. Nothing counted it.
		//
		// Everything still buffered is OLDER than this revision, so it is not
		// re-delivered and must be retained — the same contract as the List
		// branch below. seenRV is deliberately NOT persisted: the ConfigMap
		// position stays strictly ack-gated, and this only closes the
		// in-process gap.
		return startPoint{rv: r.seenRV}, nil
	}
	// Skip the backlog: take a resourceVersion without the items. Persisting
	// this later is legitimate — everything after it is either exported or
	// replayed — but nothing before it was ever consumed.
	list, err := r.cfg.Client.CoreV1().Events(r.cfg.Namespace).List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		return startPoint{}, err
	}
	r.log.Info("starting events at the current revision", "resourceVersion", list.ResourceVersion)
	r.seenRV = list.ResourceVersion
	return startPoint{rv: list.ResourceVersion}, nil
}

// handle consumes one watch event.
func (r *Reader) handle(ctx context.Context, ev watch.Event) error {
	switch ev.Type {
	case watch.Error:
		err := apierrors.FromObject(ev.Object)
		if isExpired(err) {
			r.expire(stageWatch)
		}
		return fmt.Errorf("watch error: %w", err)
	case watch.Bookmark:
		// Position only. Apply it immediately when nothing is buffered;
		// otherwise it is AHEAD of unexported records and must wait for the
		// flush that covers them (the tailer's watermark discipline).
		if o, ok := ev.Object.(*corev1.Event); ok && o.ResourceVersion != "" {
			r.pendingRV = o.ResourceVersion
			r.noteSeen(o.ResourceVersion)
			if len(r.batch) == 0 {
				r.applyPendingBookmark()
			}
		}
		return nil
	case watch.Deleted:
		// TTL expiry, not an occurrence.
		return nil
	case watch.Added, watch.Modified:
		// Kubernetes AGGREGATES repeats into one object with a growing count,
		// so a Modified is a new occurrence — handling only Added would lose
		// "BackOff x47", the most diagnostically valuable event there is.
	default:
		return nil
	}
	e, ok := ev.Object.(*corev1.Event)
	if !ok {
		// Should not happen: this is an Events watch. If it ever does, it does
		// so for every delivery — the stream is decoding into something else —
		// and the pipeline goes silently to zero exported events with the watch
		// still healthy and no counter moving. Throttled, because a flood is
		// exactly the shape it would take.
		if r.oddObjectWarn.Allow(oddObjectWarnEvery) {
			r.log.Warn("the event watch delivered an object that is not a core/v1 Event; it is ignored, so nothing from this stream is being exported",
				"type", fmt.Sprintf("%T", ev.Object), "eventType", string(ev.Type))
		}
		return nil
	}
	// seenRV records where the STREAM is. Nothing filters here: every event a
	// watch delivers is newer than the revision it was positioned at — the
	// backlog is read by replayBacklog, not by the watch — and applying the
	// watermark to a live stream drops, permanently and silently, every event
	// from a reporter whose clock trails it (see wanted).
	r.noteSeen(e.ResourceVersion)
	r.ingest(ctx, e)
	r.flushIfFull(ctx)
	return nil
}

// oddObjectWarnEvery rate-limits the not-an-Event warning (see handle).
const oddObjectWarnEvery = 5 * time.Minute

func isExpired(err error) bool {
	return apierrors.IsResourceExpired(err) || apierrors.IsGone(err)
}
