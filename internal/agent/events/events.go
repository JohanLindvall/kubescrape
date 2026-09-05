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
	"os"
	"strconv"
	"strings"
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
	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/leader"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
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
	// Enrich, Scrub, LogAttrs, Rules and LogMetrics are the same levers the
	// tailer and journald apply, in the same order.
	Enrich     bool
	Scrub      *logscrub.Scrubber
	LogAttrs   *logattrs.Extractor
	Rules      *logline.LineFilter
	LogMetrics *metrics.DynamicMetricSet

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
	// now is the wall clock, injectable for tests (the store.now pattern). It
	// exists so the watermark clamp below is testable without sleeping.
	now         func() time.Time
	lastPersist time.Time
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
	// flushWarned rate-limits the export-failure warning to flushWarnEvery;
	// obs.LogExportFailures carries the magnitude.
	flushWarned time.Time
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
	// object per batch, so it is exactly the flood logdedupe exists for;
	// obs.EventsUnresolved carries the rate.
	unresolvedWarn logdedupe.Throttle
	// oddObjectWarn throttles the "the watch delivered something that is not an
	// Event" warning — a should-not-happen branch that, if it ever fires, fires
	// for every delivery.
	oddObjectWarn logdedupe.Throttle
	// persistFailedAt/persistWarned are the transition-warn state for position
	// writes, following cmd/kubescrape/apiserver.go: a Warn on the first
	// failure, a throttled re-warn while it persists, an Info on recovery.
	persistFailedAt time.Time
	persistWarned   time.Time
	// resCache memoizes the involved object's resource for the life of the
	// batch, keyed by the same identity the records group on. See resource().
	resCache map[string]pcommon.Resource
	// observed is the set of batch entries the per-record chain has already
	// run over — the positional proof that lets a re-ingested event say
	// logchain.Input.Observed. See obsKey and markObserved.
	observed map[obsKey]struct{}
}

// obsKey identifies an event OCCURRENCE exactly: the object's UID and the
// resourceVersion of the write that produced this delivery.
//
// It is a positional proof in the sense logchain.Input.Observed demands, and
// the two halves are both load-bearing. The resourceVersion is etcd's revision
// for that write, so a REPEAT — which Kubernetes aggregates into the same
// object re-sent as Modified with a growing count, the "BackOff x47" case — is
// a different revision and a different key: a genuine new occurrence is never
// mistaken for a re-delivery. The UID keeps two objects apart if a cluster ever
// hands out a revision this reader has seen on another key. An entry missing
// BOTH (a synthetic event with no metadata) claims nothing, because the
// asymmetry runs one way: under-claiming re-counts, over-claiming destroys
// observations invisibly.
type obsKey struct {
	uid string
	rv  string
}

func (k obsKey) valid() bool { return k.uid != "" || k.rv != "" }

// entry is one event, already converted to the fields the record needs.
type entry struct {
	body     string
	ts       time.Time
	severity plog.SeverityNumber
	sevText  string
	// resource identifies the involved object; records group by it.
	resKey string
	res    pcommon.Resource
	attrs  map[string]any
	// rv/when are this event's position contribution.
	rv   string
	when time.Time
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

// Stage labels for obs.EventRelists / obs.EventGapDiscarded: WHERE the
// resourceVersion was found to have aged out. They are metric label VALUES, so
// the set is named once here rather than spelled at each call site, and
// publishMetrics gives every one of them a series.
const (
	stageWatch  = "watch"  // the watch itself returned Gone / Expired
	stageReplay = "replay" // a paginated backlog list lost its snapshot mid-walk
)

// Op labels for obs.EventPositionErrors.
const (
	opLoad = "load"
	opSave = "save"
)

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
	r.loadPosition(ctx)
	bo := backoff.New(r.cfg.RestartBackoff)
	for ctx.Err() == nil {
		started := time.Now()
		err := r.stream(ctx)
		if ctx.Err() != nil {
			break
		}
		bo.ResetIfHealthy(started)
		obs.EventWatchRestarts.Inc()
		r.log.Warn("event watch stopped; restarting", "error", err, "backoff", bo.Delay())
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

// shutdownBudget is the ceiling on the final flush and position write. It is
// derived from the lease renew deadline the caller stops this work within, so
// the two cannot drift apart.
func (r *Reader) shutdownBudget() time.Duration {
	d := r.cfg.StopBudget
	if d <= 0 {
		d = leader.DefaultRenewDeadline / 2
	}
	return d
}

// replaySlack widens the relist replay boundary. It absorbs the second
// truncation of metav1.Time plus modest node clock skew; anything it lets
// through a second time is a duplicate, which the pipeline already tolerates,
// while anything it excludes is lost outright.
const replaySlack = time.Minute

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

// loadPosition reads the stored resume point and applies the start policy.
func (r *Reader) loadPosition(ctx context.Context) {
	if r.cfg.Positions == nil {
		return
	}
	pos, found, err := r.cfg.Positions.Load(ctx)
	switch {
	case err != nil:
		// An unreadable position is not a first run, and it must not be
		// answered with the LOSS direction. Two failure classes arrive here in
		// ONE shape — the store makes a single Get, so an undecodable document
		// and a 503 / timeout / expired credential are indistinguishable — and
		// under `auto` the cold-start policy takes the CURRENT revision: one
		// failed Get during a control-plane roll (exactly when leadership
		// churns) discards every event since the predecessor stopped, and the
		// first persist then overwrites the only record of where that was.
		//
		// So replay the TTL backlog instead. It is unfiltered — no watermark
		// loaded — so it costs duplicates, which at-least-once already
		// tolerates, and it is what the sibling this store's doc claims parity
		// with does: the tailer's positions store reports Corrupt() so `auto`
		// RE-READS rather than skipping every file to its end as history. An
		// explicit `end` or `start` is the operator naming a cold-start policy
		// outright and is honoured.
		obs.EventPositionErrors.WithLabelValues(opLoad).Inc()
		r.relist = r.cfg.StartMode == StartAuto
		r.log.Warn("event position unreadable", "error", err,
			"startMode", r.cfg.StartMode, "replayingBacklog", r.relist)
	case found:
		r.committed = pos
		r.log.Info("resuming events", "resourceVersion", pos.ResourceVersion, "watermark", pos.Watermark)
	}
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
		r.pending.Discard()
		r.rendered = 0
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
				return errors.New("watch channel closed")
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

// replayPageSize bounds one backlog page. The backlog is a whole --event-ttl
// window (an hour by default) and can be six figures on a busy cluster, so it
// is paged rather than materialised whole: the singleton runs against a 256Mi
// limit and one unbounded List response would be the OOM the retained-batch cap
// exists to prevent, arriving before a single entry is exported. It matches
// client-go's reflector default.
const replayPageSize = 500

// replayProgressEvery is how often the backlog walk reports progress while it
// holds the reader goroutine (see replayBacklog).
const replayProgressEvery = 10 * time.Second

// replayBacklog re-reads the events the watch cannot position us into and
// returns the revision the watch that follows must start at.
//
// WHY A LIST AND NOT A WATCH FROM "". A watch started at "" delivers the same
// backlog as synthetic ADDED events, and that is what this used to do — but
// those events arrive in STORE order carrying their own arbitrary revisions,
// and the stream never says where the backlog ENDS. So the position had to be
// held (see replaySecured) until something proved the watcher caught up, and
// the only such signal the API offers is a BOOKMARK, which for core/v1 events
// never comes. A List answers both questions itself: it TERMINATES (an empty
// Continue), and its snapshot revision is a boundary no item can exceed.
//
// The order the pages arrive in still tells us nothing, so the position stays
// held for the whole walk and until every listed entry has been exported —
// what changed is that the release is now reachable.
//
// WHY NOT client-go's pager.ListPager, which is in the module already. It pages
// exactly like this; the one behaviour it adds is FullListIfExpired, which
// recovers a compacted continue token by re-issuing the List UNPAGED — the
// single unbounded response the paging exists to prevent (a whole --event-ttl
// window materialised at once against the singleton's 256Mi limit). With that
// off it returns the same error this loop already handles, and its per-ITEM
// callback hides the page boundary where the continue token, the snapshot
// revision and the interleaved flush all live. Hand-rolled deliberately.
//
// THE COST OF THE LIST-THEN-WATCH SHAPE, accepted rather than fixed: the watch
// that follows is positioned at the FIRST page's snapshot, so the walk's own
// duration is charged against the API server's compaction window — the old
// watch-from-"" start had no such exposure, since it began at the CURRENT
// revision. This is the shape client-go's own reflector uses, and the recovery
// is the one it uses too: the watch fails Gone and expire arms another relist.
// What keeps that from re-exporting the backlog lap after lap is the flush at
// the END of this walk — a lap that COMPLETES commits the snapshot revision and
// the watermark together, so the next lap's wanted() drops everything this one
// shipped (bar the replaySlack window). A lap aborted mid-walk by the continue
// token expiring commits nothing and does re-export, which is at-least-once
// behaving as documented.
func (r *Reader) replayBacklog(ctx context.Context) (string, error) {
	// No ResourceVersion: a quorum read, so the snapshot is the most recent
	// state and the watch that resumes from it cannot start behind one.
	opts := metav1.ListOptions{Limit: replayPageSize}
	pages, listed, kept := 0, 0, 0
	walkStarted := time.Now()
	walkLogged := walkStarted
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		list, err := r.cfg.Client.CoreV1().Events(r.cfg.Namespace).List(ctx, opts)
		if err != nil {
			if isExpired(err) {
				// The snapshot the continue token pins aged out mid-walk. The
				// pages already read cannot be completed by a second snapshot
				// (that one starts at a different revision), so the replay is
				// abandoned and re-armed: expire counts it and keeps relist
				// set, and what was already exported is filtered out of the
				// next attempt by the watermark, exactly as after a Gone watch.
				r.expire(stageReplay)
			}
			return "", fmt.Errorf("listing the event backlog: %w", err)
		}
		if r.replayRV == "" {
			// Every page of a continued list is served from the FIRST page's
			// snapshot, so this revision covers the whole walk.
			r.replayRV = list.ResourceVersion
		}
		for i := range list.Items {
			if r.replayItem(ctx, &list.Items[i]) {
				kept++
			}
		}
		listed += len(list.Items)
		pages++
		// The walk BLOCKS the single reader goroutine — no watch is open, no
		// ticker is serviced — and an --event-ttl window on a busy cluster is
		// six figures, i.e. hundreds of pages. Without a progress line the only
		// thing an operator sees is a pipeline that exports nothing and says
		// nothing, which is indistinguishable from a wedge. Cadence-based
		// rather than page-based: what matters is how long the silence is, not
		// how many pages fit inside it.
		if time.Since(walkLogged) >= replayProgressEvery {
			walkLogged = time.Now()
			r.log.Info("replaying the event backlog", "pages", pages,
				"listed", listed, "kept", kept, "elapsed", time.Since(walkStarted).Round(time.Second))
		}
		if list.Continue == "" {
			break
		}
		if list.Continue == opts.Continue {
			// A token that does not advance is a walk that never terminates,
			// and this loop holds the single reader goroutine. It is an error
			// rather than a break: breaking would secure the replay over a
			// backlog only partly read, which is the silent loss the hold
			// exists to prevent. A page COUNT limit would do the same to a
			// legitimately huge backlog, so the guard is exactly "no progress".
			return "", fmt.Errorf("the event backlog list repeated its continue token after %d pages", pages)
		}
		opts.Continue = list.Continue
	}
	if r.replayRV == "" {
		// Without a boundary the watch could only be started at "" again, which
		// is the unbounded backlog this function exists to replace, and nothing
		// could ever secure the replay. Fail the stream instead: Run retries it
		// with the relist still armed.
		return "", errors.New("the event backlog list carries no resourceVersion; the watch after it cannot be positioned")
	}
	// The backlog is read: everything the watch delivers from here is NEWER
	// than the snapshot, so the watermark filter must not touch it.
	r.replaying = false
	// Read the boundary out BEFORE securing, which consumes r.replayRV: the
	// watch must be positioned at it, and returning the emptied field started
	// the watch at "" — the unbounded server-side backlog this replaced.
	rv := r.replayRV
	r.noteSeen(rv)
	// Flush the TAIL before the watch is established. The count trigger leaves
	// up to flushAt entries behind on every walk, and the next opportunity to
	// export them is the FlushInterval ticker inside the watch loop — i.e. only
	// if establishing the watch succeeds, which is exactly the step the stale
	// snapshot revision (see above) can fail. Owed entries there hold the
	// position, so a Gone watch re-lapped the whole backlog with nothing
	// committed and nothing filtering it. This is also what secures the replay
	// on the ordinary path, and what makes an aborted lap terminate.
	if len(r.batch) > 0 && r.flushDue() {
		r.tryFlush(ctx)
	}
	r.log.Info("replayed the event backlog", "resourceVersion", rv,
		"pages", pages, "listed", listed, "kept", kept, "awaitingExport", r.replayOwed,
		"elapsed", time.Since(walkStarted).Round(time.Millisecond))
	// A backlog that was empty, entirely filtered, or already flushed secures
	// right here — otherwise the flush that drains the last owed entry does it.
	r.maybeSecureReplay()
	return rv, nil
}

// replayItem consumes one backlog item, in the order the walk applies them: the
// watermark filter, the ingest, the owed count, then the COUNT-triggered
// export. It reports whether the item was kept.
//
// This walk BLOCKS the single reader goroutine, so the count trigger is the
// only one there is until it returns: the FlushInterval ticker and persist both
// live in the watch loop that has not started yet (persist would write nothing
// anyway — every commit is held while the replay is unsecured). That is why the
// trigger is flushAt rather than BatchSize, and why the tail flushes above.
//
// It is a method rather than the loop body it was so a test can drive the exact
// mid-walk state — an export with `replaying` still set — that no other path
// can produce: the flag is cleared before the watch opens, and driving a WATCH
// event with it set would pin the filter to the watch path, which is precisely
// where it must never be applied.
func (r *Reader) replayItem(ctx context.Context, e *corev1.Event) bool {
	if !r.wanted(e) {
		return false
	}
	r.ingest(ctx, e)
	// ingest may have SHED to make room, which already adjusted replayOwed;
	// this entry is the newest and is always in the batch.
	r.replayOwed++
	if len(r.batch) >= r.flushAt() && r.flushDue() {
		r.tryFlush(ctx)
	}
	return true
}

// expire drops the committed resourceVersion so the next stream relists,
// keeping the watermark to filter the replay.
func (r *Reader) expire(stage string) {
	r.committed.ResourceVersion = ""
	// The pending bookmark dies with the revision it was pending against, and
	// this is the only path into a relist that could be carrying one (nothing
	// else empties the committed revision). Carried into the replay, the first
	// flush that empties the batch would apply it through settle's pending
	// branch — which SECURES the replay, releasing the position hold over a
	// backlog only partly read. TestExpiryClearsResourceVersionButKeepsWatermark
	// pins it.
	r.pendingRV = ""
	// The revision aged out of the API server's watch window, so the
	// in-process memory of it is dead too and must not be resumed from.
	r.seenRV = ""
	// Never DISARM an armed relist: loadPosition arms one with a ZERO
	// watermark when the stored position is unreadable, and an unsecured
	// replay holds its exported high-water in heldWatermark precisely so the
	// committed watermark stays zero — recomputing from the committed
	// watermark alone read both as "nothing to recover" and restarted the
	// next stream at the CURRENT revision, silently discarding the gap on
	// exactly the recovery path. Beyond an armed flag, anything exported
	// justifies the replay (the committed watermark filters it; a held one
	// folds in when the replay secures); only a truly cold reader — nothing
	// exported, nothing armed — restarts per the start mode, so a cold
	// -events-start=end still skips the backlog the operator asked to skip.
	r.relist = r.relist || !r.committed.Watermark.IsZero() || !r.heldWatermark.IsZero()
	// Count the fall-back only where the next stream actually takes it: the
	// armed relist, or a -events-start=start whose cold policy replays anyway.
	// The counter is registered as watches that "fell back to a relist", and
	// bumping it before the decision made the two OPPOSITE outcomes of this
	// function indistinguishable to an alert — under auto/end a watermark-less
	// expiry restarts at the CURRENT revision and DISCARDS the gap, which
	// tryFlush's comment already names as harm ("whose name asserts a relist
	// that did not happen").
	if r.relist || r.cfg.StartMode == StartBeginning {
		obs.EventRelists.WithLabelValues(stage).Inc()
		// A relist is not loss, but it IS the explanation for the duplicate
		// burst that follows (the backlog is replayed and filtered by a
		// watermark with a minute of slack) and for the pause while the walk
		// runs. Info rather than Warn: it is the recovery working, and the
		// counter is what an alert reads.
		r.log.Info("the stored resourceVersion aged out of the api server's watch window; replaying the event backlog and filtering it by the exported watermark",
			"stage", stage, "watermark", r.committed.Watermark)
		return
	}
	// The discard arm is the pipeline's one silent-loss path: it must move a
	// counter of its own, not just a Warn on one pod's stderr.
	obs.EventGapDiscarded.WithLabelValues(stage).Inc()
	r.log.Warn("event watch expired before anything was exported; restarting per the start mode and discarding the gap",
		"stage", stage, "startMode", r.cfg.StartMode)
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
				r.committed.ResourceVersion = o.ResourceVersion
				r.relist = false // a bookmark covers everything before it
				r.secureReplay()
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
	if len(r.batch) >= r.flushAt() && r.flushDue() {
		r.tryFlush(ctx)
	}
	return nil
}

// flushWarnEvery rate-limits the export-failure warning during an outage.
const flushWarnEvery = time.Minute

// oddObjectWarnEvery rate-limits the not-an-Event warning (see handle).
const oddObjectWarnEvery = 5 * time.Minute

// flushDue reports whether the count trigger may attempt an export again. It
// is unrestricted while flushes are landing; after a failure the batch stays
// at or above flushAt (nothing settles), so the trigger holds for every event
// that arrives and would re-export per event.
func (r *Reader) flushDue() bool {
	return r.flushFailedAt.IsZero() || time.Since(r.flushFailedAt) >= r.cfg.FlushInterval
}

// tryFlush exports the batch and treats a TRANSIENT failure as a flush
// failure, not a stream failure.
//
// Propagating it tore down the API-server watch, and past BatchSize the batch
// is retained, so the count trigger held forever and every subsequent event
// cost another failing export and another teardown — 7681 of them to reach the
// retention cap from cold, with backoff.ResetIfHealthy needing a 30s-long
// stream so the backoff pinned at its 30s cap. Worse than the churn was what
// it did to the position: only one event per round was handled, so seenRV fell
// behind the cluster's event rate, aged out of the API server's watch window
// within minutes, and the resulting Gone expired into a relist that a COLD
// reader cannot take (relist needs a non-zero watermark) — so the watch
// silently restarted at the CURRENT revision and the whole gap was discarded,
// counted only by obs.EventRelists{stage="watch"}, whose name asserts a relist
// that did not happen.
//
// Keeping the watch open costs nothing the design does not already carry: the
// batch is bounded by retainCap, the collector's failure is counted by
// obs.LogExportFailures, and seenRV keeps tracking the live stream. A
// PERMANENT rejection is not seen here at all — flush settles it and returns
// nil, as everywhere else.
func (r *Reader) tryFlush(ctx context.Context) {
	if err := r.flush(ctx); err != nil {
		r.flushFailedAt = time.Now()
		if time.Since(r.flushWarned) >= flushWarnEvery {
			r.flushWarned = time.Now()
			r.log.Warn("event export failed; the watch stays open and the batch is retained",
				"error", err, "buffered", len(r.batch), "cap", r.retainCap())
		}
		return
	}
	// Measure the next flush from THIS flush, not from the last tick (see
	// flushTicker). nil during replayBacklog, before the watch's ticker exists.
	if r.flushTicker != nil {
		r.flushTicker.Reset(r.cfg.FlushInterval)
	}
	// Offer to persist after EVERY flush, not only the ticker-driven one.
	// persist self-throttles on PersistInterval, so this cannot write more
	// often than configured — but leaving it in the ticker branch alone meant
	// resetting that ticker here STARVED it: a rate that trips the count
	// trigger faster than FlushInterval reset the ticker before it could fire,
	// so the position was never written and the PersistInterval bound on
	// post-crash replay silently stopped holding (measured: 0 writes under a
	// count-triggered load that produced 10 with the trigger disabled).
	r.persist(ctx, false)
	if !r.flushFailedAt.IsZero() {
		r.log.Info("event export recovered", "buffered", len(r.batch))
		r.flushFailedAt, r.flushWarned = time.Time{}, time.Time{}
		// Re-arm the overflow warning with the recovery, not with the process:
		// the next outage that sheds events is a new loss, and a latch held for
		// the process' life reported only the first one — on a singleton that
		// runs for weeks, usually not the one being investigated.
		r.overflowWarned = false
	}
}

// wanted filters a REPLAYED event against the watermark. A replay re-reads
// everything still within the TTL; the watermark drops what was already
// exported. Timestamps come from the REPORTING component's clock, so the
// comparison is biased toward re-emitting (at-least-once).
//
// It applies ONLY to the backlog list, which is the only thing that can carry
// an already-exported event. Event timestamps come from whichever component
// reported the event, and a stream carries several — kubelet events are
// second-truncated metav1.Time, scheduler and controller-manager events are
// microsecond MicroTime — so applied to a WATCH this dropped any event whose
// reporter's clock trailed the watermark, permanently and silently (noteSeen
// has already advanced the resume point past it) and uncounted. Worse, one
// component with a fast clock latched a FUTURE watermark, which is persisted in
// the position ConfigMap: the blackout then survived restarts and leader
// handover.
//
// That is why the filter is scoped by `replaying`, which replayBacklog clears
// the moment the last page is read, rather than by the stream's lifetime: the
// watch that follows a replay is positioned at the list's snapshot and delivers
// nothing the replay could already have carried.
func (r *Reader) wanted(e *corev1.Event) bool {
	if !r.replaying || r.replayFrom.IsZero() {
		return true
	}
	when := eventTime(e)
	return when.IsZero() || !when.Before(r.replayFrom)
}

// flush exports the batch; the position advances only after the collector
// acknowledges it.
func (r *Reader) flush(ctx context.Context) error {
	if len(r.batch) == 0 {
		return nil
	}
	// Convert ONCE per batch, not once per export ATTEMPT — a batch is
	// RETAINED across a transient failure AND across a redelivers=false stream
	// restart, and re-converting re-observed the LogMetrics on every lap
	// (logchain.Pending owns that discipline). settle() clears the pair with
	// the batch, and stream()'s restart reset clears it too.
	ld := r.pending.Render(r.convert)
	// The high-water position over the RENDERED PREFIX, not the whole batch. A
	// relist delivers the backlog in store order and each object carries its
	// own resourceVersion, so the last entry is routinely older than one
	// earlier in the batch — committing it walked the position BACKWARDS
	// (redelivery on restart), and in the other order committed a high RV while
	// lower-RV entries of the same backlog were still undelivered (outright
	// loss on a kill right after). So take the maximum — but only over
	// [:rendered], the entries this payload actually carries: a redelivers=false
	// restart appends fresh entries past that boundary that are NOT exported
	// yet, and committing their RV would lose them (see the `rendered` field).
	covered := min(r.rendered, len(r.batch))
	newest := r.batch[0]
	for _, e := range r.batch[1:covered] {
		if newerRV(e.rv, newest.rv) {
			newest.rv = e.rv
		}
		if e.when.After(newest.when) {
			newest.when = e.when
		}
	}
	count := ld.LogRecordCount()
	if count > 0 {
		if err := r.cfg.Exporter.ExportLogs(ctx, ld); err != nil {
			// The records counted are the EXPORTED count (the rules may have
			// dropped some of the batch), matching EventsExported.
			if !logchain.SettlePermanent(err, r.log, "event batch", count,
				logchain.SettleCounters{Batches: obs.EventsDropped, Records: obs.EventsDroppedRecords},
				"events", len(r.batch)) {
				// This pipeline's OWN transient-failure counter, not
				// kubescrape_log_export_failures_total: that one documents
				// itself as the tailer's "files rewound", and the singleton
				// that collects events runs with -logs=false, so an operator
				// alerting on a rewinding tailer was paged by a pod that owns
				// no file. journald was given its own counter for exactly this
				// reason and its two siblings were left behind.
				obs.EventsExportFailures.Inc()
				return fmt.Errorf("exporting events: %w", err)
			}
		} else {
			obs.EventsExported.Add(float64(count))
		}
	}
	r.settle(newest, covered)
	return nil
}

// settle advances the position to what the exported payload covered and drops
// exactly those entries, RETAINING any appended past the rendered prefix (a
// redelivers=false restart's fresh, not-yet-exported entries — see the
// `rendered` field). covered is the prefix length the payload rendered.
func (r *Reader) settle(newest entry, covered int) {
	covered = min(covered, len(r.batch))
	emptied := covered == len(r.batch)
	// These entries are done with: the chain will not see them again unless a
	// relist replays them, which is a new DELIVERY and is observed again.
	r.forgetObserved(r.batch[:covered])
	// Slide the retained tail to the front (copy is memmove-safe for the
	// overlap) and clear the vacated slots so settled entries aren't pinned.
	n := copy(r.batch, r.batch[covered:])
	clear(r.batch[n:])
	r.batch = r.batch[:n]
	// The converted payload belongs to the prefix just settled; the tail
	// converts afresh (and is observed by log-metrics) on the next flush.
	r.pending.Discard()
	r.rendered = 0
	// The resource memo is per BATCH. Retained tail entries already hold the
	// resources they were built with, so dropping it costs at most one rebuild
	// per involved object on the next flush and keeps a settled object's
	// resource from outliving the entries that referenced it.
	clear(r.resCache)
	// The replay list's entries are the batch's LEADING ones, so a settled
	// prefix retires exactly that many of them. Reaching zero with the walk
	// finished is what secures the replay.
	r.replayOwed -= min(covered, r.replayOwed)
	r.maybeSecureReplay()
	// See replaySecured: a mid-replay commit positions a restarted stream
	// past backlog the store-order replay has not delivered yet, so the
	// position holds until the whole backlog has been exported.
	unsecured := !r.replaySecured
	if !unsecured && newest.rv != "" && newerRV(newest.rv, r.committed.ResourceVersion) {
		r.committed.ResourceVersion = newest.rv
	}
	// A bookmark seen while the batch was pending is covered only when this
	// flush EMPTIED the batch: its revision vouches for everything the stream
	// delivered before it, which includes entries retained past the rendered
	// prefix (a redelivers=false restart's fresh appends), so applying it over
	// a partial flush put the committed position ABOVE unexported tail entries
	// — a watch resumed from it never re-delivers them, and the persisted
	// position stopped being a lower bound on what was delivered. With a tail
	// retained the bookmark stays pending for the flush that covers it. When
	// it does apply, it must also be NEWER than what this flush committed:
	// bookmarks arrive interleaved with events, so one seen before the last
	// few entries carries an OLDER revision, and applying it unconditionally
	// walked the committed position backwards by up to a flush window — a
	// restart or leader handover then redelivered everything in between.
	if r.pendingRV != "" && emptied {
		if newerRV(r.pendingRV, r.committed.ResourceVersion) {
			r.committed.ResourceVersion = r.pendingRV
		}
		r.pendingRV = ""
		// The bookmark proves the backlog fully delivered — this flush's own
		// watermark may now commit directly (recomputed below).
		r.secureReplay()
	}
	unsecured = !r.replaySecured
	// Clamp to wall clock first. The watermark is a running MAXIMUM over
	// event timestamps written by whichever component reported each event,
	// so one reporter with a fast clock would otherwise latch a boundary in
	// the future and make the relay filter discard everything until real
	// time caught up — on the one path whose whole purpose is recovering
	// events that were never delivered. The clamp applies to the held
	// accumulator too — secureReplay folds it in verbatim.
	mark := &r.committed.Watermark
	if unsecured {
		mark = &r.heldWatermark
	}
	if newest.when.After(*mark) {
		if now := r.now(); newest.when.After(now) {
			newest.when = now
		}
		if newest.when.After(*mark) {
			*mark = newest.when
		}
	}
	if r.committed.ResourceVersion != "" {
		// The replay (if one was pending) is secured up to this position: a
		// restart resumes from it instead of relisting the full TTL again, and
		// what the stream delivers from here on is new rather than replayed.
		r.relist = false
	}
}

// maybeSecureReplay secures the replay once its backlog is fully accounted
// for: the last page read (replaying cleared) and every entry it contributed
// settled or shed. That is the securing condition the API server cannot give
// us — see replaySecured.
func (r *Reader) maybeSecureReplay() {
	if r.replaySecured || r.replaying || r.replayOwed > 0 {
		return
	}
	r.secureReplay()
}

// secureReplay marks the current replay's backlog fully exported: the position
// may advance again, it advances to the LIST SNAPSHOT's revision (the boundary
// no listed item can exceed, and where the following watch is positioned), and
// the held exported high-water folds into the committed watermark. Idempotent;
// a no-op outside a replay.
func (r *Reader) secureReplay() {
	if r.replaySecured && r.heldWatermark.IsZero() {
		return
	}
	r.replaySecured = true
	// The snapshot revision, not the maximum over the entries: a consistent
	// list returned everything that existed at it, so committing it skips
	// nothing — while the entry maximum would leave the position below events
	// the list proved are already handled, and on an idle cluster the watch
	// would age out of the API server's window again with nothing to advance
	// it (the whole reason bookmarks exist).
	if r.replayRV != "" && newerRV(r.replayRV, r.committed.ResourceVersion) {
		r.committed.ResourceVersion = r.replayRV
	}
	r.replayRV, r.replayOwed = "", 0
	if r.heldWatermark.After(r.committed.Watermark) {
		r.committed.Watermark = r.heldWatermark
	}
	r.heldWatermark = time.Time{}
	if r.committed.ResourceVersion != "" {
		// A restart resumes from the position instead of relisting the full
		// TTL again.
		r.relist = false
	}
}

// maxRetained bounds the batch across failed flushes, in entries.
//
// A collector outage settles nothing, so the batch grows for the whole length
// of the outage — at the CLUSTER's event rate, since the watch stays open
// across a failed export (tryFlush), which on a busy cluster fills this cap in
// minutes. Retention is ~2 KB per entry where every event names a distinct
// involved object (repeats share one built resource — see resource()), so the
// cap is ~16 MB against the singleton's 256Mi limit.
//
// The cap is what makes the loss bounded and OBSERVABLE
// (obs.EventsOverflowDropped) rather than an OOM that takes the whole batch,
// the whole outage window (StartMode=end after the restart) and the co-located
// pipelines with it.
const maxRetained = 8192

// maxRetainedCeiling is the absolute ceiling the floor below may not lift the
// cap past. -events-batch-size has no upper bound of its own, so the 2*BatchSize
// floor turned an extreme value into an extreme budget (100000 => ~400 MB
// retained against a 256Mi limit) — the cap stopped being a cap. Past this
// ceiling the count-triggered flush is simply unreachable and the FlushInterval
// ticker does the flushing: slower, never an OOM.
const maxRetainedCeiling = 2 * maxRetained

// retainCap is maxRetained, floored above BatchSize so the count-triggered
// flush stays reachable however BatchSize is tuned, and ceilinged so the floor
// cannot repeal the memory bound.
func (r *Reader) retainCap() int {
	return min(max(maxRetained, 2*r.cfg.BatchSize), maxRetainedCeiling)
}

// flushAt is the batch length that triggers an export: BatchSize, clamped to
// the retention cap because the batch can never grow past that.
//
// -events-batch-size has no upper bound, and the ceiling above stops retainCap
// following it, so a BatchSize over maxRetainedCeiling made the raw count
// trigger UNREACHABLE — the batch sat at the cap shedding the oldest entries
// forever. On the watch path that was survivable (the FlushInterval ticker
// flushes), but the backlog walk services no ticker: measured against a 30,000
// event backlog, BatchSize=512 exported 29,696 while BatchSize=20000 exported
// NOTHING and dropped 13,696 into kubescrape_events_overflow_dropped_total —
// documented as outright loss — before the watch was even opened. Both paths
// use this, so the trigger cannot be unreachable on either.
func (r *Reader) flushAt() int {
	return min(r.cfg.BatchSize, r.retainCap())
}

// shedChunk is how many entries one shed drops. Dropping exactly one per
// admitted event made the shed O(batch): at the cap EVERY event sheds, and
// each shed memmoved the whole post-prefix tail (measured 47.5 µs against
// 2.8 µs below the cap, 1.18 MB moved per event). Dropping a run amortises the
// move over that many admissions; the surplus loss is bounded by one chunk at
// the moment the collector recovers, against a cap of maxRetained.
const shedChunk = maxRetained / 64

// shedOldest drops a run of entries to admit new ones at the cap, starting at
// the oldest entry the pending payload does NOT cover. The rendered prefix is
// kept — its entries are not lost (they export when the collector recovers),
// and dropping inside it would make settle slide fresh entries as covered by a
// payload that never carried them. Only when EVERYTHING is rendered is the
// rendering discarded first, so the drop cannot orphan it; the re-render costs
// nothing but the conversion, since the surviving entries are already in the
// observed set and the chain will not count them twice.
func (r *Reader) shedOldest() {
	if r.rendered >= len(r.batch) {
		r.pending.Discard()
		r.rendered = 0
	}
	i := r.rendered
	n := min(shedChunk, len(r.batch)-i)
	if n <= 0 {
		return
	}
	// Shed entries are gone for good, so their observation proof goes too — it
	// would otherwise be a key no batch entry can ever retire.
	r.forgetObserved(r.batch[i : i+n])
	copy(r.batch[i:], r.batch[i+n:])
	clear(r.batch[len(r.batch)-n:])
	r.batch = r.batch[:len(r.batch)-n]
	// Shed entries the replay list contributed stop being OWED. They are
	// counted loss and no export can ever carry them, so holding the position
	// for them would wedge it exactly as waiting for a bookmark did — and the
	// snapshot revision the replay commits is no less honest than the shed
	// already is.
	if r.replayOwed > i {
		r.replayOwed -= min(i+n, r.replayOwed) - i
	}
	obs.EventsOverflowDropped.Add(float64(n))
	if !r.overflowWarned {
		r.overflowWarned = true
		r.log.Warn("event batch at capacity with nothing committing; dropping the oldest unexported events, which the watch will not re-deliver",
			"cap", r.retainCap(), "dropped", n, "perShed", shedChunk)
	}
}

// tracksObservation reports whether anything the chain counts is configured.
// With neither log metrics nor rules there is nothing to observe twice, so the
// set is not maintained at all and costs a nil map lookup.
func (r *Reader) tracksObservation() bool {
	return r.cfg.LogMetrics != nil || r.cfg.Rules != nil
}

// wasObserved reports whether an earlier convert already ran the per-record
// chain over this occurrence.
func (r *Reader) wasObserved(k obsKey) bool {
	if !k.valid() {
		return false
	}
	_, ok := r.observed[k]
	return ok
}

// markObserved records that the chain has run over these occurrences. It is
// called with the whole rendered batch AFTER the render, never entry by entry
// inside it: two entries of one batch carrying the same key (which no delivery
// path produces, but nothing structurally forbids) must both be counted, and
// marking as we went would suppress the second — the over-claiming direction,
// which destroys observations invisibly.
//
// Past the cap the set is CLEARED rather than grown. The live need is one key
// per un-settled batch entry, which retainCap already bounds; anything above
// that is keys for entries a re-delivery never brought back, and clearing them
// costs at most one re-observation of what is still buffered — the safe
// direction, and the behaviour that existed before the set did.
func (r *Reader) markObserved(entries []entry) {
	if !r.tracksObservation() {
		return
	}
	if r.observed == nil {
		r.observed = make(map[obsKey]struct{}, len(entries))
	} else if len(r.observed)+len(entries) > r.retainCap() {
		clear(r.observed)
	}
	for i := range entries {
		if k := entries[i].okey; k.valid() {
			r.observed[k] = struct{}{}
		}
	}
}

// forgetObserved drops the keys of entries LEAVING the batch: settled
// (delivered, all-dropped or permanently rejected) or shed.
//
// Delivery is at-least-once and observation is once per DELIVERY, so a settled
// entry that a later relist replays is a new delivery and is observed again;
// keeping its key would also make the set grow without any bound the batch
// provides. What must NOT be forgotten is a stream restart's clear of the batch
// (stream), which is precisely the case where the same delivery comes back.
func (r *Reader) forgetObserved(entries []entry) {
	if len(r.observed) == 0 {
		return
	}
	for i := range entries {
		delete(r.observed, entries[i].okey)
	}
}

// persist writes the position, rate-limited unless forced (shutdown).
func (r *Reader) persist(ctx context.Context, force bool) {
	if r.cfg.Positions == nil || r.committed.ResourceVersion == "" {
		return
	}
	if !force && time.Since(r.lastPersist) < r.cfg.PersistInterval {
		return
	}
	r.lastPersist = time.Now()
	pos := r.committed
	pos.Holder = hostname()
	if err := r.cfg.Positions.Save(ctx, pos); err != nil {
		obs.EventPositionErrors.WithLabelValues(opSave).Inc()
		// The transition-warn shape (cmd/kubescrape/apiserver.go): the write
		// retries every PersistInterval, so an unwritable ConfigMap — a lost
		// RBAC rule, an API server that is down — would otherwise be one line
		// every ten seconds for the length of the outage. The FIRST failure is
		// what an operator needs; the rest is the counter's job.
		if r.persistFailedAt.IsZero() || time.Since(r.persistWarned) >= positionWarnEvery {
			r.persistWarned = time.Now()
			r.log.Warn("writing the event position failed; a restart or leader handover will resume from the last position that was written, replaying what has happened since",
				"error", err, "eventsPositionConfigmap", r.positionRef(), "resourceVersion", pos.ResourceVersion)
		}
		if r.persistFailedAt.IsZero() {
			r.persistFailedAt = time.Now()
		}
		return
	}
	if !r.persistFailedAt.IsZero() {
		r.log.Info("writing the event position recovered", "resourceVersion", pos.ResourceVersion)
		r.persistFailedAt, r.persistWarned = time.Time{}, time.Time{}
	}
	// The position is the one piece of this pipeline's state that outlives the
	// process, and "is it advancing?" is the first question a handover raises.
	// Debug, not Info: it writes every PersistInterval forever.
	r.log.Debug("event position written", "resourceVersion", pos.ResourceVersion,
		"watermark", pos.Watermark, "forced", force)
}

// positionWarnEvery re-warns about an unwritable position at this cadence.
const positionWarnEvery = 5 * time.Minute

// positionRef names the ConfigMap the position lives in, for a log line that
// has to be actionable without the operator knowing the flag defaults. It is a
// best-effort read of the store's own fields: a test store is not one.
func (r *Reader) positionRef() string {
	if s, ok := r.cfg.Positions.(*ConfigMapStore); ok {
		return s.Namespace + "/" + s.Name
	}
	return ""
}

func isExpired(err error) bool {
	return apierrors.IsResourceExpired(err) || apierrors.IsGone(err)
}

// eventTime resolves an event's occurrence time across the two API shapes:
// core/v1 carries first/lastTimestamp, events.k8s.io carries eventTime and
// series.lastObservedTime, and either may be unset per reporting component.
func eventTime(e *corev1.Event) time.Time {
	if e.Series != nil && !e.Series.LastObservedTime.IsZero() {
		return e.Series.LastObservedTime.Time
	}
	if !e.LastTimestamp.IsZero() {
		return e.LastTimestamp.Time
	}
	if !e.EventTime.IsZero() {
		return e.EventTime.Time
	}
	if !e.FirstTimestamp.IsZero() {
		return e.FirstTimestamp.Time
	}
	return e.CreationTimestamp.Time
}

func hostname() string {
	h, _ := osHostname()
	return h
}

// osHostname is a variable so tests can pin the holder field.
var osHostname = func() (string, error) {
	return os.Hostname()
}

// severityOf maps the event type onto OTLP severity.
//
// Lowercase, like every other producer in this repo: it is what the enrich
// package writes (its level constants are "info"/"warn"/...), and
// logenrich.Apply OVERWRITES the severity here whenever it parses a level out
// of the message — so any other casing was contradicted one line later, on a
// subset of records that depends on their content. logchain.LowerSeverity
// lowercases before the rules see it, so the config surface reads lowercase
// too.
func severityOf(eventType string) (plog.SeverityNumber, string) {
	if strings.EqualFold(eventType, corev1.EventTypeWarning) {
		return plog.SeverityNumberWarn, "warn"
	}
	return plog.SeverityNumberInfo, "info"
}
