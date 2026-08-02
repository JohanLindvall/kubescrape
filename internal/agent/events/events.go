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
	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
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
	// committed is the position every exported batch has reached; pending is
	// the newest position SEEN (bookmarks included) but not yet exported.
	committed   Position
	pendingRV   string
	lastPersist time.Time
	lastFlush   time.Time
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
	// after the API server dropped our resourceVersion. See expire.
	relist bool
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
	return &Reader{cfg: cfg, log: cfg.Logger}
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
	backoff := r.cfg.RestartBackoff
	for ctx.Err() == nil {
		started := time.Now()
		err := r.stream(ctx)
		if ctx.Err() != nil {
			break
		}
		if time.Since(started) >= 30*time.Second {
			backoff = r.cfg.RestartBackoff // a healthy stream resets the backoff
		}
		obs.EventWatchRestarts.Inc()
		r.log.Warn("event watch stopped; restarting", "error", err, "backoff", backoff)
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
	// Final flush on a DETACHED context: ctx is already cancelled, and the
	// last batch must still reach the collector before the position is
	// written (the tailer's shutdown flush does the same).
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
	fctx, cancel := context.WithTimeout(context.Background(), r.shutdownBudget())
	defer cancel()
	if err := r.flush(fctx); err != nil {
		r.log.Warn("final event flush failed", "error", err)
	}
	r.persist(fctx, true)
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
		// Unparseable: treat as a cold start but COUNT it — an undecodable
		// checkpoint must never masquerade as a first run.
		obs.EventPositionErrors.WithLabelValues("load").Inc()
		r.log.Warn("event position unreadable; starting per the start mode", "error", err)
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
	// Snapshot, not a live read: see replayFrom.
	r.replayFrom = r.committed.Watermark
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
				if err := r.flush(ctx); err != nil {
					return err
				}
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
		// anything commits must relist again, or the gap is lost after all —
		// it clears only when a commit secures the replay (settle/bookmark).
		return "", true, nil
	}
	if r.cfg.StartMode == StartBeginning {
		// "" replays everything the API server still holds (the event TTL).
		return "", true, nil
	}
	// Skip the backlog: take a resourceVersion without the items. Persisting
	// this later is legitimate — everything after it is either exported or
	// replayed — but nothing before it was ever consumed.
	list, err := r.cfg.Client.CoreV1().Events(r.cfg.Namespace).List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		return "", false, err
	}
	r.log.Info("starting events at the current revision", "resourceVersion", list.ResourceVersion)
	return list.ResourceVersion, false, nil
}

// expire drops the committed resourceVersion so the next stream relists,
// keeping the watermark to filter the replay.
func (r *Reader) expire(stage string) {
	obs.EventRelists.WithLabelValues(stage).Inc()
	r.committed.ResourceVersion = ""
	r.pendingRV = ""
	// Only once something has been exported: with a zero watermark nothing
	// would filter the replay, and a cold -events-start=end would turn its
	// first expiry into the full backlog the operator asked to skip.
	r.relist = !r.committed.Watermark.IsZero()
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
			if len(r.batch) == 0 {
				r.committed.ResourceVersion = o.ResourceVersion
				r.relist = false // a bookmark covers everything before it
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
	if !r.wanted(e) {
		return nil
	}
	r.ingest(ctx, e)
	if len(r.batch) >= r.cfg.BatchSize {
		return r.flush(ctx)
	}
	return nil
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
		r.lastFlush = time.Now()
		return nil
	}
	ld := r.convert()
	// The batch's HIGH-WATER entry, not its last one. A relist delivers the
	// backlog in store order and each object carries its own resourceVersion,
	// so the last entry is routinely older than one earlier in the batch —
	// committing it walked the position BACKWARDS (redelivery on restart), and
	// in the other order committed a high RV while lower-RV entries of the
	// same backlog were still undelivered (outright loss on a kill right
	// after). The whole batch has exported by the time settle runs, so the
	// maximum is exactly what is safe to commit.
	newest := r.batch[0]
	for _, e := range r.batch[1:] {
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
			if otlpexport.IsPermanent(err) {
				// Skipping past a poison batch, as journald does: re-reading it
				// forever would wedge the reader on one bad payload.
				obs.EventsDropped.Inc()
				// And the records, so the loss has a magnitude and not just an
				// occurrence — the rules may have dropped some of the batch,
				// so this is the exported count, matching EventsExported.
				obs.EventsDroppedRecords.Add(float64(count))
				r.log.Warn("event batch permanently rejected, skipping past it", "events", len(r.batch), "records", count, "error", err)
			} else {
				obs.LogExportFailures.Inc()
				return fmt.Errorf("exporting events: %w", err)
			}
		} else {
			obs.EventsExported.Add(float64(count))
		}
	}
	r.settle(newest)
	r.lastFlush = time.Now()
	return nil
}

// settle clears the batch and advances the position to what it covered.
func (r *Reader) settle(newest entry) {
	clear(r.batch)
	r.batch = r.batch[:0]
	if newest.rv != "" && newerRV(newest.rv, r.committed.ResourceVersion) {
		r.committed.ResourceVersion = newest.rv
	}
	// A bookmark seen while the batch was pending is now covered too — but only
	// if it is NEWER than what this flush just committed. Bookmarks arrive
	// interleaved with events, so one seen before the last few entries carries
	// an OLDER revision, and applying it unconditionally walked the committed
	// position backwards by up to a flush window: a restart or leader handover
	// then redelivered everything in between.
	if r.pendingRV != "" {
		if newerRV(r.pendingRV, r.committed.ResourceVersion) {
			r.committed.ResourceVersion = r.pendingRV
		}
		r.pendingRV = ""
	}
	if newest.when.After(r.committed.Watermark) {
		r.committed.Watermark = newest.when
	}
	if r.committed.ResourceVersion != "" {
		// The replay (if one was pending) is secured up to this position: a
		// restart resumes from it instead of relisting the full TTL again, and
		// what the stream delivers from here on is new rather than replayed.
		r.relist = false
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
