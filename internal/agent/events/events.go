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
	lastFlush   time.Time
	// flushFailedAt is when the last export attempt failed, zero once one
	// succeeds. Past BatchSize the batch is RETAINED, so the count trigger
	// holds forever while the collector is down; pacing the retry against this
	// keeps it to one attempt per FlushInterval instead of one per event.
	flushFailedAt time.Time
	// flushWarned rate-limits the export-failure warning to flushWarnEvery;
	// obs.LogExportFailures carries the magnitude.
	flushWarned time.Time
	// replaying is set while the current stream is a full-backlog replay (it
	// started from ""), which is the ONLY situation in which the watermark
	// filter belongs: a replay re-sends events already exported, a positioned
	// or live stream never does. See wanted.
	//
	// It is per-STREAM and is NOT cleared by a commit. Clearing it there
	// disarmed the filter after the first acked batch — a couple of seconds
	// into a replay that delivers the whole event TTL — so the rest of the
	// backlog re-exported as duplicates, each Pod event costing a PodByName
	// lookup. The next stream is positioned by the committed resourceVersion
	// and starts false on its own.
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
	// replaySecured: the current replay's backlog is known FULLY delivered —
	// a bookmark applied, and the API server only sends bookmarks once the
	// watcher is caught up. Trivially true for a positioned or live stream.
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
	replaySecured bool
	// heldWatermark is the wall-clamped high-water of what unsecured-replay
	// flushes exported, folded into the committed watermark by secureReplay.
	// It survives stream restarts: it only ever names exported entries.
	heldWatermark time.Time
	// overflowWarned rate-limits the shedOldest warning to one per process;
	// obs.EventsOverflowDropped carries the ongoing magnitude.
	overflowWarned bool
	// resCache memoizes the involved object's resource for the life of the
	// batch, keyed by the same identity the records group on. See resource().
	resCache map[string]pcommon.Resource
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
	attrs  map[string]any
	// rv/when are this event's position contribution.
	rv   string
	when time.Time
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
	return &Reader{cfg: cfg, log: cfg.Logger, now: time.Now}
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

// newerRV reports whether a is a later resourceVersion than b. Kubernetes
// treats these as opaque, but etcd's are decimal integers, so compare
// numerically where both parse and fall back to keeping what we have (the
// conservative direction: never move the position backwards on a guess).
// noteSeen advances the in-process resume point. Unlike committed.ResourceVersion
// this is NOT ack-gated: it only decides where a cold restart of the watch
// resumes within one process lifetime, and resuming too early costs duplicates
// while resuming too late loses events outright.
// replaySlack widens the relist replay boundary. It absorbs the second
// truncation of metav1.Time plus modest node clock skew; anything it lets
// through a second time is a duplicate, which the pipeline already tolerates,
// while anything it excludes is lost outright.
const replaySlack = time.Minute

func (r *Reader) noteSeen(rv string) {
	if rv != "" && newerRV(rv, r.seenRV) {
		r.seenRV = rv
	}
}

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
		obs.EventPositionErrors.WithLabelValues("load").Inc()
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
	rv, redelivers, err := r.startResourceVersion(ctx)
	if err != nil {
		return err
	}
	// An empty resourceVersion replays the whole TTL backlog (a relist, or a
	// cold start-from-beginning); anything else is positioned exactly by the
	// API server and delivers only what follows.
	r.replaying = rv == ""
	r.replaySecured = !r.replaying
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
	if redelivers && len(r.batch) > 0 {
		// Everything buffered is AFTER rv (entries only outlive a flush that
		// failed, and the position never advanced past them), so this watch
		// re-sends every one of them. Keeping the batch would duplicate the
		// whole backlog once per restart and grow memory without bound across
		// a long collector outage; dropping it loses nothing — the entries
		// are re-ingested from the re-delivery. Only the cold skip-backlog
		// path (redelivers=false) starts past the buffered entries and must
		// retain them.
		clear(r.batch)
		r.batch = r.batch[:0]
		clear(r.resCache)
		// The converted payload described the batch just dropped; the watch is
		// about to re-deliver those entries and they will convert afresh
		// (logchain.Pending's restart-clear case — the loss journald had for
		// the same reason).
		r.pending.Discard()
		r.rendered = 0
		r.pendingRV = "" // a bookmark from the dead stream vouches only for its own deliveries
	}
	w, err := r.cfg.Client.CoreV1().Events(r.cfg.Namespace).Watch(ctx, metav1.ListOptions{
		ResourceVersion: rv,
		// Bookmarks advance the resourceVersion on an idle cluster, so the
		// persisted position stays inside the API server's watch window and a
		// restart resumes instead of falling back to a relist.
		AllowWatchBookmarks: true,
	})
	if err != nil {
		if isExpired(err) {
			r.expire("watch")
			return fmt.Errorf("watch from %q expired: %w", rv, err)
		}
		return err
	}
	defer w.Stop()

	ticker := time.NewTicker(r.cfg.FlushInterval)
	defer ticker.Stop()
	r.lastFlush = time.Now()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if len(r.batch) > 0 && time.Since(r.lastFlush) >= r.cfg.FlushInterval {
				r.tryFlush(ctx)
			}
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

// startResourceVersion resolves where this watch begins: the committed
// position, or the start policy on a cold start. redelivers reports whether
// the new watch re-sends everything the reader has already buffered — true
// for every path except the cold skip-the-backlog List, whose revision is
// AFTER anything currently batched.
func (r *Reader) startResourceVersion(ctx context.Context) (rv string, redelivers bool, err error) {
	if r.committed.ResourceVersion != "" {
		return r.committed.ResourceVersion, true, nil
	}
	if r.relist {
		// Recovering from a Gone, not starting cold: replay everything the API
		// server still holds and let the watermark drop what we already
		// exported. Taking the CURRENT revision instead (what the cold-start
		// policy does) would silently lose every event between the expired
		// version and now — precisely the window a relist exists to cover.
		// The flag is NOT consumed here: a watch attempt that fails before
		// the replay is secured must relist again, or the gap is lost after
		// all — it clears only when a BOOKMARK proves the backlog fully
		// delivered (secureReplay; settle holds all commits until then).
		return "", true, nil
	}
	if r.cfg.StartMode == StartBeginning {
		// "" replays everything the API server still holds (the event TTL).
		return "", true, nil
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
		return r.seenRV, false, nil
	}
	// Skip the backlog: take a resourceVersion without the items. Persisting
	// this later is legitimate — everything after it is either exported or
	// replayed — but nothing before it was ever consumed.
	list, err := r.cfg.Client.CoreV1().Events(r.cfg.Namespace).List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		return "", false, err
	}
	r.log.Info("starting events at the current revision", "resourceVersion", list.ResourceVersion)
	r.seenRV = list.ResourceVersion
	return list.ResourceVersion, false, nil
}

// expire drops the committed resourceVersion so the next stream relists,
// keeping the watermark to filter the replay.
func (r *Reader) expire(stage string) {
	r.committed.ResourceVersion = ""
	r.pendingRV = ""
	// The revision aged out of the API server's watch window, so the
	// in-process memory of it is dead too and must not be resumed from.
	r.seenRV = ""
	// Only once something has been exported: with a zero watermark nothing
	// would filter the replay, and a cold -events-start=end would turn its
	// first expiry into the full backlog the operator asked to skip.
	r.relist = !r.committed.Watermark.IsZero()
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
		return
	}
	r.log.Warn("event watch expired before anything was exported; restarting per the start mode and discarding the gap",
		"stage", stage, "startMode", r.cfg.StartMode)
}

// handle consumes one watch event.
func (r *Reader) handle(ctx context.Context, ev watch.Event) error {
	switch ev.Type {
	case watch.Error:
		err := apierrors.FromObject(ev.Object)
		if isExpired(err) {
			r.expire("watch")
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
		return nil
	}
	// Before the wanted() filter: seenRV records where the STREAM is, which is
	// true of a replayed event the watermark drops just as much as of one we
	// keep. Recording it only for kept events would resume behind the filter.
	r.noteSeen(e.ResourceVersion)
	if !r.wanted(e) {
		return nil
	}
	r.ingest(ctx, e)
	if len(r.batch) >= r.cfg.BatchSize && r.flushDue() {
		r.tryFlush(ctx)
	}
	return nil
}

// flushWarnEvery rate-limits the export-failure warning during an outage.
const flushWarnEvery = time.Minute

// flushDue reports whether the count trigger may attempt an export again. It
// is unrestricted while flushes are landing; after a failure the batch stays
// AT or above BatchSize (nothing settles), so the trigger holds for every
// event that arrives and would re-export per event.
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
	if !r.flushFailedAt.IsZero() {
		r.log.Info("event export recovered", "buffered", len(r.batch))
		r.flushFailedAt, r.flushWarned = time.Time{}, time.Time{}
	}
}

// wanted filters a REPLAYED event against the watermark. After a relist we
// re-receive everything still within the TTL; the watermark drops what was
// already exported. Timestamps come from the REPORTING component's clock, so
// the comparison is biased toward re-emitting (at-least-once).
//
// It applies ONLY while replaying. Event timestamps come from whichever
// component reported the event, and a watch carries several — kubelet events
// are second-truncated metav1.Time, scheduler and controller-manager events are
// microsecond MicroTime — so on a live stream this dropped any event whose
// reporter's clock trailed the watermark, permanently and silently. Worse, one
// component with a fast clock latched a FUTURE watermark, which is persisted in
// the position ConfigMap: the blackout then survived restarts and leader
// handover.
//
// And it stops at the SECURING BOOKMARK, not at the end of the stream. The
// `replaying` flag is per-stream and deliberately survives a commit (see the
// field), but a bookmark proves the backlog FULLY delivered — the same proof
// settle already trusts to release the position hold, the strictly more
// dangerous of the two — so what the stream delivers after it is LIVE. Keeping
// the filter armed there is the live-stream drop above, one watch lifetime long
// after every relist: replayFrom is frozen at stream start, so a reporter whose
// clock trails it by more than replaySlack goes dark, silently (noteSeen has
// already advanced the resume point past the event) and uncounted.
func (r *Reader) wanted(e *corev1.Event) bool {
	if !r.replaying || r.replaySecured || r.replayFrom.IsZero() {
		return true
	}
	when := eventTime(e)
	return when.IsZero() || !when.Before(r.replayFrom)
}

// flush exports the batch; the position advances only after the collector
// acknowledges it.
func (r *Reader) flush(ctx context.Context) error {
	if len(r.batch) == 0 {
		r.lastFlush = time.Now()
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
				obs.LogExportFailures.Inc()
				return fmt.Errorf("exporting events: %w", err)
			}
		} else {
			obs.EventsExported.Add(float64(count))
		}
	}
	r.settle(newest, covered)
	r.lastFlush = time.Now()
	return nil
}

// settle advances the position to what the exported payload covered and drops
// exactly those entries, RETAINING any appended past the rendered prefix (a
// redelivers=false restart's fresh, not-yet-exported entries — see the
// `rendered` field). covered is the prefix length the payload rendered.
func (r *Reader) settle(newest entry, covered int) {
	covered = min(covered, len(r.batch))
	emptied := covered == len(r.batch)
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
	// See replaySecured: a mid-replay commit positions a restarted stream
	// past backlog the store-order replay has not delivered yet, so the
	// position holds until a bookmark proves the backlog complete.
	unsecured := r.replaying && !r.replaySecured
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
	unsecured = r.replaying && !r.replaySecured
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

// secureReplay marks the current replay's backlog fully delivered (a bookmark
// applied — see replaySecured) and folds the held exported high-water into
// the committed watermark. Idempotent; a no-op outside a replay.
func (r *Reader) secureReplay() {
	if r.replaySecured && r.heldWatermark.IsZero() {
		return
	}
	r.replaySecured = true
	if r.heldWatermark.After(r.committed.Watermark) {
		r.committed.Watermark = r.heldWatermark
	}
	r.heldWatermark = time.Time{}
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
// rendering discarded first, so the drop cannot orphan it; the re-render
// re-observes the LogMetrics once, the price of not wedging at the cap.
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
	copy(r.batch[i:], r.batch[i+n:])
	clear(r.batch[len(r.batch)-n:])
	r.batch = r.batch[:len(r.batch)-n]
	obs.EventsOverflowDropped.Add(float64(n))
	if !r.overflowWarned {
		r.overflowWarned = true
		r.log.Warn("event batch at capacity with nothing committing; dropping the oldest unexported events",
			"cap", r.retainCap(), "perShed", shedChunk)
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
		obs.EventPositionErrors.WithLabelValues("save").Inc()
		r.log.Warn("writing the event position", "error", err)
	}
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
	if strings.EqualFold(eventType, string(corev1.EventTypeWarning)) {
		return plog.SeverityNumberWarn, "warn"
	}
	return plog.SeverityNumberInfo, "info"
}
