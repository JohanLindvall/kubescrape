// Package events exports Kubernetes Events as OTLP log records, enriched
// with pod metadata from the store when the event concerns a pod.
package events

import (
	"context"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// LogExporter sends one OTLP logs payload; implemented by otlpexport.Client.
type LogExporter interface {
	ExportLogs(ctx context.Context, ld plog.Logs) error
}

// Config configures the exporter.
type Config struct {
	Store         *store.Store
	Exporter      LogExporter
	BatchSize     int           // flush after this many events
	FlushInterval time.Duration // flush at least this often
	Logger        *slog.Logger
}

// Exporter converts and batches events. Informer handlers feed it; Run
// drains.
type Exporter struct {
	cfg   Config
	log   *slog.Logger
	ch    chan *corev1.Event
	start time.Time
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
		cfg:   cfg,
		log:   log,
		ch:    make(chan *corev1.Event, 4*cfg.BatchSize),
		start: time.Now(),
	}
}

// OnAdd handles informer adds.
func (e *Exporter) OnAdd(obj any) {
	if ev, ok := obj.(*corev1.Event); ok {
		e.enqueue(ev)
	}
}

// OnUpdate handles informer updates (recurring events bump their count).
func (e *Exporter) OnUpdate(oldObj, newObj any) {
	oldEv, ok1 := oldObj.(*corev1.Event)
	newEv, ok2 := newObj.(*corev1.Event)
	if !ok1 || !ok2 {
		return
	}
	if newEv.Count > oldEv.Count || !newEv.LastTimestamp.Equal(&oldEv.LastTimestamp) {
		e.enqueue(newEv)
	}
}

func (e *Exporter) enqueue(ev *corev1.Event) {
	if eventTime(ev).Before(e.start) {
		return // pre-start history from the initial list
	}
	select {
	case e.ch <- ev:
	default:
		e.log.Warn("event queue full, dropping event", "reason", ev.Reason)
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
	return t
}

// Run batches and exports until ctx is done.
func (e *Exporter) Run(ctx context.Context) {
	ticker := time.NewTicker(e.cfg.FlushInterval)
	defer ticker.Stop()
	var batch []*corev1.Event
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := e.cfg.Exporter.ExportLogs(ctx, e.convert(batch)); err != nil {
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
			return
		case ev := <-e.ch:
			batch = append(batch, ev)
			if len(batch) >= e.cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// convert builds the OTLP payload: one resource per involved object.
func (e *Exporter) convert(batch []*corev1.Event) plog.Logs {
	ld := plog.NewLogs()
	scopes := make(map[string]plog.ScopeLogs)
	now := pcommon.NewTimestampFromTime(time.Now())
	for _, ev := range batch {
		key := ev.InvolvedObject.Namespace + "\x00" + string(ev.InvolvedObject.UID)
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
		if ev.Count > 1 {
			a.PutInt("k8s.event.count", int64(ev.Count))
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
			attrs.Pod(res, np.Pod)
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
	switch obj.Kind {
	case "Deployment":
		a.PutStr("k8s.deployment.name", obj.Name)
	case "ReplicaSet":
		a.PutStr("k8s.replicaset.name", obj.Name)
	case "StatefulSet":
		a.PutStr("k8s.statefulset.name", obj.Name)
	case "DaemonSet":
		a.PutStr("k8s.daemonset.name", obj.Name)
	case "Job":
		a.PutStr("k8s.job.name", obj.Name)
	case "CronJob":
		a.PutStr("k8s.cronjob.name", obj.Name)
	case "Node":
		a.PutStr("k8s.node.name", obj.Name)
	}
}
