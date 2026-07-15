// Package events exports Kubernetes Events as OTLP log records, enriched
// with pod metadata from the store when the event concerns a pod.
package events

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// LogExporter sends one OTLP logs payload; implemented by otlpexport.Client.
type LogExporter interface {
	ExportLogs(ctx context.Context, ld plog.Logs) error
}

// Config configures the exporter.
type Config struct {
	Store    *store.Store
	Exporter LogExporter
	// Owners resolves the involved pod's workload chain and namespace metadata,
	// exactly as the HTTP API does per request. Without it a pod's events would
	// carry no k8s.deployment.name/k8s.job.name and would derive service.name
	// from the POD name — so a workload's events would not share a service.name
	// with that same pod's logs (whose metadata is owner-resolved), and every
	// replica would mint a service.name that churns on each rollout.
	Owners        OwnerResolver
	BatchSize     int           // flush after this many events
	FlushInterval time.Duration // flush at least this often
	Logger        *slog.Logger
}

// OwnerResolver is the subset of owners.Resolver the exporter needs (the HTTP
// API's MetadataResolver, minus Node).
type OwnerResolver interface {
	Resolve(namespace string, refs []metav1.OwnerReference) []kubemeta.Owner
	Namespace(name string) *kubemeta.ObjectMeta
}

// Exporter converts and batches events. Informer handlers feed it; Run
// drains.
type Exporter struct {
	cfg   Config
	log   *slog.Logger
	ch    chan *corev1.Event
	start time.Time
	// lastDropWarn rate-limits the queue-full warning (unix nanos); every drop
	// still counts into obs.EventsDropped.
	lastDropWarn atomic.Int64
}

// New creates an Exporter. Events older than the start time (the informer's
// initial list replays history) are skipped.
func New(cfg Config) *Exporter {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 256
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 5 * time.Second
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Exporter{
		cfg: cfg,
		log: log,
		ch:  make(chan *corev1.Event, 4*cfg.BatchSize),
		// Event timestamps have second granularity; truncating the start
		// keeps a genuinely-new event created in the startup second from
		// being misclassified as pre-start history.
		start: time.Now().Truncate(time.Second),
	}
}

// OnAdd handles informer adds.
func (e *Exporter) OnAdd(obj any) {
	if ev, ok := obj.(*corev1.Event); ok {
		e.enqueue(ev)
	}
}

// OnUpdate handles informer updates. Recurrences surface as bumps to the
// legacy count/lastTimestamp OR — for events recorded through the
// events.k8s.io/v1 API (most modern controllers and the scheduler) — to
// series.count/series.lastObservedTime, while the legacy fields stay zero.
func (e *Exporter) OnUpdate(oldObj, newObj any) {
	oldEv, ok1 := oldObj.(*corev1.Event)
	newEv, ok2 := newObj.(*corev1.Event)
	if !ok1 || !ok2 {
		return
	}
	if newEv.Count > oldEv.Count || !newEv.LastTimestamp.Equal(&oldEv.LastTimestamp) ||
		seriesCount(newEv) > seriesCount(oldEv) {
		e.enqueue(newEv)
	}
}

func seriesCount(ev *corev1.Event) int32 {
	if ev.Series == nil {
		return 0
	}
	return ev.Series.Count
}

func (e *Exporter) enqueue(ev *corev1.Event) {
	if eventTime(ev).Before(e.start) {
		return // pre-start history from the initial list
	}
	select {
	case e.ch <- ev:
	default:
		obs.EventsDropped.Inc()
		// One warning per ~10s: a sustained overload would otherwise log per
		// dropped event.
		now := time.Now().UnixNano()
		if last := e.lastDropWarn.Load(); now-last >= int64(10*time.Second) &&
			e.lastDropWarn.CompareAndSwap(last, now) {
			e.log.Warn("event queue full, dropping events", "reason", ev.Reason)
		}
	}
}

// eventTime picks the most recent timestamp an event carries.
func eventTime(ev *corev1.Event) time.Time {
	t := ev.CreationTimestamp.Time
	if ev.LastTimestamp.After(t) {
		t = ev.LastTimestamp.Time
	}
	if ev.EventTime.After(t) {
		t = ev.EventTime.Time
	}
	if ev.Series != nil && ev.Series.LastObservedTime.After(t) {
		t = ev.Series.LastObservedTime.Time
	}
	return t
}

// Run batches and exports until ctx is done, then flushes what is pending.
func (e *Exporter) Run(ctx context.Context) {
	ticker := time.NewTicker(e.cfg.FlushInterval)
	defer ticker.Stop()
	var batch []*corev1.Event
	flush := func(ctx context.Context) {
		if len(batch) == 0 {
			return
		}
		if err := e.cfg.Exporter.ExportLogs(ctx, e.convert(batch)); err != nil {
			// Delivery is best-effort: there are no retries and no spool, so a
			// failed export loses the batch. Count it like a queue-full drop —
			// the shutdown flush suppresses even the warning, and a silent loss
			// is not acceptable.
			obs.EventsDropped.Add(float64(len(batch)))
			if ctx.Err() == nil {
				e.log.Warn("exporting events", "count", len(batch), "error", err)
			}
		} else {
			obs.EventsExported.Add(float64(len(batch)))
		}
		batch = batch[:0]
	}
	for {
		select {
		case <-ctx.Done():
			// Export the pending batch (and whatever is already queued) before
			// returning, on a short background timeout — mirroring the
			// self-metrics registry's final export.
			for {
				select {
				case ev := <-e.ch:
					batch = append(batch, ev)
					continue
				default:
				}
				break
			}
			fctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			flush(fctx)
			cancel()
			return
		case ev := <-e.ch:
			batch = append(batch, ev)
			if len(batch) >= e.cfg.BatchSize {
				flush(ctx)
			}
		case <-ticker.C:
			flush(ctx)
		}
	}
}

// convert builds the OTLP payload: one resource per involved object.
func (e *Exporter) convert(batch []*corev1.Event) plog.Logs {
	ld := plog.NewLogs()
	scopes := make(map[string]plog.ScopeLogs)
	now := pcommon.NewTimestampFromTime(time.Now())
	for _, ev := range batch {
		// UID is optional on ObjectReference; include kind+name so UID-less
		// events for different objects get their own (correctly attributed)
		// resources.
		key := ev.InvolvedObject.Namespace + "\x00" + string(ev.InvolvedObject.UID) +
			"\x00" + ev.InvolvedObject.Kind + "\x00" + ev.InvolvedObject.Name
		sl, ok := scopes[key]
		if !ok {
			rl := ld.ResourceLogs().AppendEmpty()
			e.fillResource(rl.Resource(), ev)
			sl = rl.ScopeLogs().AppendEmpty()
			sl.Scope().SetName("github.com/JohanLindvall/kubescrape/events")
			scopes[key] = sl
		}
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(eventTime(ev)))
		lr.SetObservedTimestamp(now)
		lr.Body().SetStr(ev.Message)
		switch ev.Type {
		case corev1.EventTypeWarning:
			lr.SetSeverityNumber(plog.SeverityNumberWarn)
			lr.SetSeverityText("Warning")
		default:
			lr.SetSeverityNumber(plog.SeverityNumberInfo)
			lr.SetSeverityText("Normal")
		}
		a := lr.Attributes()
		a.PutStr("k8s.event.reason", ev.Reason)
		// Modern events.k8s.io/v1 recorders keep the legacy Count at zero and put
		// occurrence multiplicity in Series.Count (see seriesCount, also used to
		// decide re-emission). Take the larger so a series event keeps its count.
		count := ev.Count
		if sc := seriesCount(ev); sc > count {
			count = sc
		}
		if count > 1 {
			a.PutInt("k8s.event.count", int64(count))
		}
		if src := ev.Source.Component; src != "" {
			a.PutStr("k8s.event.reporting_component", src)
		} else if ev.ReportingController != "" {
			a.PutStr("k8s.event.reporting_component", ev.ReportingController)
		}
		if ev.InvolvedObject.FieldPath != "" {
			a.PutStr("k8s.event.field_path", ev.InvolvedObject.FieldPath)
		}
	}
	return ld
}

// fillResource attributes the event to its involved object; pods get the
// full metadata set from the store.
func (e *Exporter) fillResource(res pcommon.Resource, ev *corev1.Event) {
	obj := ev.InvolvedObject
	if obj.Kind == "Pod" {
		if np, ok := e.cfg.Store.GetPodByName(obj.Namespace, obj.Name); ok &&
			(obj.UID == "" || string(obj.UID) == np.Pod.UID) {
			pod := np.Pod
			// The store never resolves the owner chain (it is lazy, per request,
			// everywhere else); resolve it here so an event's resource matches
			// the one that pod's logs and metrics get.
			if e.cfg.Owners != nil {
				pod.Owners = e.cfg.Owners.Resolve(pod.Namespace, np.OwnerRefs)
				pod.NamespaceMetadata = e.cfg.Owners.Namespace(pod.Namespace)
			}
			attrs.Pod(res, pod)
			attrs.Identity(res) // service.namespace + service.instance.id
			return
		}
	}
	a := res.Attributes()
	if obj.Namespace != "" {
		a.PutStr("k8s.namespace.name", obj.Namespace)
	}
	a.PutStr("k8s.object.kind", obj.Kind)
	a.PutStr("k8s.object.name", obj.Name)
	if obj.UID != "" {
		a.PutStr("k8s.object.uid", string(obj.UID))
	}
	// Attribute well-known workload kinds like the agent would.
	if attr, ok := attrs.KindAttribute(obj.Kind); ok {
		a.PutStr(attr, obj.Name)
	}
}
