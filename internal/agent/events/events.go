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
	fctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := r.flush(fctx); err != nil {
		r.log.Warn("final event flush failed", "error", err)
	}
	r.persist(fctx, true)
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
	rv, err := r.startResourceVersion(ctx)
	if err != nil {
		return err
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
// position, or the start policy on a cold start.
func (r *Reader) startResourceVersion(ctx context.Context) (string, error) {
	if r.committed.ResourceVersion != "" {
		return r.committed.ResourceVersion, nil
	}
	if r.relist {
		// Recovering from a Gone, not starting cold: replay everything the API
		// server still holds and let the watermark drop what we already
		// exported. Taking the CURRENT revision instead (what the cold-start
		// policy does) would silently lose every event between the expired
		// version and now — precisely the window a relist exists to cover.
		r.relist = false
		return "", nil
	}
	if r.cfg.StartMode == StartBeginning {
		// "" replays everything the API server still holds (the event TTL).
		return "", nil
	}
	// Skip the backlog: take a resourceVersion without the items. Persisting
	// this later is legitimate — everything after it is either exported or
	// replayed — but nothing before it was ever consumed.
	list, err := r.cfg.Client.CoreV1().Events(r.cfg.Namespace).List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		return "", err
	}
	r.log.Info("starting events at the current revision", "resourceVersion", list.ResourceVersion)
	return list.ResourceVersion, nil
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

// wanted filters a replayed event against the watermark. After a relist we
// re-receive everything still within the TTL; the watermark drops what was
// already exported. Timestamps come from the REPORTING component's clock, so
// the comparison is biased toward re-emitting (at-least-once).
func (r *Reader) wanted(e *corev1.Event) bool {
	if r.committed.Watermark.IsZero() {
		return true
	}
	when := eventTime(e)
	return when.IsZero() || !when.Before(r.committed.Watermark)
}

// flush exports the batch; the position advances only after the collector
// acknowledges it.
func (r *Reader) flush(ctx context.Context) error {
	if len(r.batch) == 0 {
		r.lastFlush = time.Now()
		return nil
	}
	ld := r.convert()
	newest := r.batch[len(r.batch)-1]
	count := ld.LogRecordCount()
	if count > 0 {
		if err := r.cfg.Exporter.ExportLogs(ctx, ld); err != nil {
			if otlpexport.IsPermanent(err) {
				// Skipping past a poison batch, as journald does: re-reading it
				// forever would wedge the reader on one bad payload.
				obs.EventsDropped.Inc()
				r.log.Warn("event batch permanently rejected, skipping past it", "events", len(r.batch), "error", err)
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
	if newest.rv != "" {
		r.committed.ResourceVersion = newest.rv
	}
	// A bookmark seen while the batch was pending is now covered too.
	if r.pendingRV != "" {
		r.committed.ResourceVersion = r.pendingRV
		r.pendingRV = ""
	}
	if newest.when.After(r.committed.Watermark) {
		r.committed.Watermark = newest.when
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
func severityOf(eventType string) (plog.SeverityNumber, string) {
	if strings.EqualFold(eventType, string(corev1.EventTypeWarning)) {
		return plog.SeverityNumberWarn, "WARN"
	}
	return plog.SeverityNumberInfo, "INFO"
}
