package events

// Event -> OTLP log record. The involved object's identity becomes the
// RESOURCE, so an event about a pod carries that pod's full attribute set
// (owner chain, labels, node, service.name) and therefore correlates with its
// logs and metrics in one query. That resolution is what a flat
// events-as-logs stream cannot do.

import (
	"context"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
	"github.com/JohanLindvall/kubescrape/internal/agent/logenrich"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

// ingest converts one event into a batch entry, resolving its resource.
func (r *Reader) ingest(ctx context.Context, e *corev1.Event) {
	body := e.Message
	if body == "" {
		body = e.Reason
	}
	if r.cfg.Scrub != nil {
		// Scrub before anything copies from the body, as everywhere else.
		body = r.cfg.Scrub.Scrub(body)
	}
	sev, sevText := severityOf(e.Type)
	key, res := r.resource(ctx, e)

	ent := entry{
		body: body, ts: eventTime(e), severity: sev, sevText: sevText,
		resKey: key, res: res, rv: e.ResourceVersion, when: eventTime(e),
		attrs: eventAttrs(e),
	}
	if ent.ts.IsZero() {
		ent.ts = e.CreationTimestamp.Time
	}
	r.batch = append(r.batch, ent)
	obs.EventsObserved.WithLabelValues(eventTypeLabel(e.Type)).Inc()
}

// eventTypeLabel collapses Event.Type to the two documented values plus
// "other". It is the only obs label populated from cluster data, and the
// core/v1 create path does not enforce the Normal/Warning validation — while
// the Registry has neither expiry nor a cardinality cap, so an unbounded value
// here is an unbounded series count in the agent's own metrics.
func eventTypeLabel(t string) string {
	switch strings.ToLower(t) {
	case "normal":
		return "normal"
	case "warning":
		return "warning"
	default:
		return "other"
	}
}

// eventAttrs are the record-level attributes describing the event itself.
func eventAttrs(e *corev1.Event) map[string]any {
	a := map[string]any{
		"k8s.event.uid":                  string(e.UID),
		"k8s.event.name":                 e.Name,
		"k8s.event.reason":               e.Reason,
		"k8s.event.type":                 e.Type,
		"k8s.event.involved_object.kind": e.InvolvedObject.Kind,
		"k8s.event.involved_object.name": e.InvolvedObject.Name,
		"k8s.event.count":                int64(e.Count),
		"k8s.event.reporting_component":  reportingComponent(e),
	}
	if e.Series != nil && e.Series.Count > 0 {
		a["k8s.event.count"] = int64(e.Series.Count)
	}
	if e.Action != "" {
		a["k8s.event.action"] = e.Action
	}
	if e.InvolvedObject.UID != "" {
		a["k8s.event.involved_object.uid"] = string(e.InvolvedObject.UID)
	}
	if e.InvolvedObject.FieldPath != "" {
		a["k8s.event.involved_object.field_path"] = e.InvolvedObject.FieldPath
	}
	if e.ReportingInstance != "" {
		a["k8s.event.reporting_instance"] = e.ReportingInstance
	}
	for k, v := range a {
		if s, ok := v.(string); ok && s == "" {
			delete(a, k)
		}
	}
	return a
}

func reportingComponent(e *corev1.Event) string {
	if e.ReportingController != "" {
		return e.ReportingController
	}
	return e.Source.Component
}

// resource builds the involved object's resource, plus a grouping key. A Pod
// resolves through the metadata service to its full identity (tombstones keep
// events about just-deleted pods resolvable); anything else gets the
// identity the event itself carries.
func (r *Reader) resource(ctx context.Context, e *corev1.Event) (string, pcommon.Resource) {
	obj := e.InvolvedObject
	res := pcommon.NewResource()
	// No .Node: the node an event is ABOUT is a property of the involved
	// object (the resolved pod carries its own), never the singleton reader's
	// own node — which has nothing to do with where the event happened.
	actx := attrs.Context{}

	resolved := false
	if obj.Kind == "Pod" && obj.Name != "" && obj.Namespace != "" && r.cfg.Meta != nil {
		if pod, err := r.cfg.Meta.PodByName(ctx, obj.Namespace, obj.Name); err == nil && pod != nil {
			// Cross-check the UID: a recreated pod of the same name must not
			// lend its identity to an event about its predecessor.
			if obj.UID == "" || string(obj.UID) == pod.UID {
				actx.Pod = pod
				resolved = true
			}
		}
	}
	a := res.Attributes()
	if !resolved {
		if obj.Namespace != "" {
			a.PutStr("k8s.namespace.name", obj.Namespace)
		}
		if attr, ok := attrs.KindAttribute(obj.Kind); ok && obj.Name != "" {
			a.PutStr(attr, obj.Name)
		}
		if obj.Kind == "Pod" && obj.Name != "" {
			// Still correlate by name even when the pod is long gone.
			a.PutStr("k8s.pod.name", obj.Name)
		}
		if obj.Name != "" {
			a.PutStr("service.name", obj.Name)
		} else {
			a.PutStr("service.name", "kubernetes-events")
		}
	}
	r.cfg.Attrs.Build(res, actx)

	// Group by the resolved identity, not by the raw event: two events about
	// one pod share a ResourceLogs.
	var key strings.Builder
	key.WriteString(obj.Kind)
	key.WriteByte('\x00')
	key.WriteString(obj.Namespace)
	key.WriteByte('\x00')
	key.WriteString(obj.Name)
	key.WriteByte('\x00')
	key.WriteString(string(obj.UID))
	return key.String(), res
}

// convert groups the batch into one ResourceLogs per involved object,
// applying the same chain the tailer and journald apply in the same order:
// line attributes, enrichment, log-metrics (which see EVERY record), then the
// rules (which may drop it).
func (r *Reader) convert() plog.Logs {
	ld := plog.NewLogs()
	scopes := make(map[string]plog.ScopeLogs, 8)
	observed := pcommon.NewTimestampFromTime(time.Now())

	var (
		scratch  plog.LogRecordSlice
		resolver *logchain.Resolver
		bound    map[string]metrics.BoundResource
		resAttrs = make(map[string]pcommon.Map, 8)
	)
	if r.cfg.Rules != nil || r.cfg.LogMetrics != nil {
		resolver = logchain.New()
	}
	if r.cfg.Rules != nil {
		scratch = plog.NewLogRecordSlice()
	}
	if r.cfg.LogMetrics != nil {
		bound = make(map[string]metrics.BoundResource, 8)
	}

	for _, e := range r.batch {
		var extracted logattrs.Result
		key := e.resKey
		if r.cfg.LogAttrs != nil {
			extracted = r.cfg.LogAttrs.Extract(e.body)
			key = e.resKey + "\x01" + logattrs.Key(extracted.Resource) + "\x01" + logattrs.Key(extracted.Scope)
		}
		sl, ok := scopes[key]
		if !ok {
			rl := ld.ResourceLogs().AppendEmpty()
			e.res.CopyTo(rl.Resource())
			logattrs.Put(rl.Resource().Attributes(), extracted.Resource)
			sl = rl.ScopeLogs().AppendEmpty()
			sl.Scope().SetName("github.com/JohanLindvall/kubescrape/agent/events")
			sl.Scope().SetVersion(obs.ScopeVersion)
			logattrs.Put(sl.Scope().Attributes(), extracted.Scope)
			scopes[key] = sl
			resAttrs[key] = rl.Resource().Attributes()
		}

		var lr plog.LogRecord
		scratched := r.cfg.Rules != nil
		if scratched {
			scratch.RemoveIf(func(plog.LogRecord) bool { return true })
			lr = scratch.AppendEmpty()
		} else {
			lr = sl.LogRecords().AppendEmpty()
		}
		lr.SetTimestamp(pcommon.NewTimestampFromTime(e.ts))
		lr.SetObservedTimestamp(observed)
		lr.SetSeverityNumber(e.severity)
		lr.SetSeverityText(e.sevText)
		lr.Body().SetStr(e.body)
		putAttrs(lr.Attributes(), e.attrs)
		logattrs.Put(lr.Attributes(), extracted.Log)
		if r.cfg.Enrich {
			logenrich.Apply(lr, e.body)
		}
		if r.cfg.LogMetrics != nil {
			bm, ok := bound[key]
			if !ok {
				bm = r.cfg.LogMetrics.Bind(resAttrs[key])
				bound[key] = bm
			}
			resolver.Set(lr.Attributes(), resAttrs[key], resolver.Severity)
			bm.Add(resolver.ValueFn(), resolver.LabelFn(), e.body)
		}
		if scratched {
			resolver.Set(lr.Attributes(), resAttrs[key], logchain.LowerSeverity(lr.SeverityText()))
			if r.cfg.Rules.Keep(resolver.RuleFn(), e.body) {
				scratch.MoveAndAppendTo(sl.LogRecords())
			} else {
				scratch.RemoveIf(func(plog.LogRecord) bool { return true })
				obs.LogRulesDropped.Inc()
			}
		}
	}
	// An all-dropped group leaves an empty ResourceLogs behind.
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool { return sl.LogRecords().Len() == 0 })
		return rl.ScopeLogs().Len() == 0
	})
	return ld
}

func putAttrs(dst pcommon.Map, src map[string]any) {
	for k, v := range src {
		switch t := v.(type) {
		case string:
			dst.PutStr(k, t)
		case int64:
			dst.PutInt(k, t)
		case bool:
			dst.PutBool(k, t)
		default:
			dst.PutStr(k, strconv.Quote(k)) // unreachable: eventAttrs builds only the three kinds above
		}
	}
}
