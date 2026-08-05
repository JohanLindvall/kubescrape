package events

// Event -> OTLP log record. The involved object's identity becomes the
// RESOURCE, so an event about a pod carries that pod's full attribute set
// (owner chain, labels, node, service.name) and therefore correlates with its
// logs and metrics in one query. That resolution is what a flat
// events-as-logs stream cannot do.

import (
	"context"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// ScopeName is the OTLP instrumentation-scope name on every event record
// (wire-visible: changing it splits every event stream at the upgrade
// boundary).
const ScopeName = "github.com/JohanLindvall/kubescrape/agent/events"

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

// convert groups the batch into one ResourceLogs per involved object. The
// per-record half — line attributes, enrichment, log-metrics (which see EVERY
// record) and the keep/drop rules — is the shared chain every log producer in
// this repo runs, in the same order (internal/agent/logchain); what stays here
// is the involved object's RESOURCE and the grouping.
//
// Bodies are already scrubbed: ingest redacts where it builds the batch entry,
// before the record exists, so the chain's Scrub is nil.
func (r *Reader) convert() plog.Logs {
	// The payload covers exactly the entries present now; anything appended
	// afterward (a redelivers=false restart's new watch) is not in it and must
	// not be settled past. logchain.Pending renders once per epoch, so this is
	// set once per epoch too.
	r.rendered = len(r.batch)
	ld := plog.NewLogs()
	groups := logchain.NewGroups(ld, ScopeName, 8)
	sink := &recordSink{observed: pcommon.NewTimestampFromTime(time.Now())}
	chain := logchain.NewChain[string](logchain.Config{
		LogAttrs:   r.cfg.LogAttrs,
		Enrich:     r.cfg.Enrich,
		LogMetrics: r.cfg.LogMetrics,
		Rules:      r.cfg.Rules,
	}, false)

	for _, e := range r.batch {
		body, extracted := chain.Line(e.body)
		key := chain.GroupKey(e.resKey, extracted)
		// The group is built BEFORE the record because metric and rule
		// resolution reads the group's own resource; a group the rules empty is
		// pruned below.
		sink.e = e
		sink.e.body = body
		ent := groups.Get(key, extracted, sink)
		sink.sl = ent.SL
		chain.Emit(sink, logchain.Input[string]{
			Body: body, Lifted: extracted, Resource: ent.Res, BoundKey: key,
		})
	}
	// An all-dropped group leaves an empty ResourceLogs behind.
	logchain.Prune(ld)
	return ld
}

// recordSink is the chain's Producer for events: the group a kept record lands
// in, and what the events reader knows about the record.
type recordSink struct {
	sl       plog.ScopeLogs
	e        entry
	observed pcommon.Timestamp
}

func (s *recordSink) Dest() plog.LogRecordSlice { return s.sl.LogRecords() }

// FillResource builds a fresh group's resource: the involved object's
// identity, resolved at ingest time (entry.res).
func (s *recordSink) FillResource(res pcommon.Resource) { s.e.res.CopyTo(res) }

func (s *recordSink) Stamp(lr plog.LogRecord) {
	e := s.e
	lr.SetTimestamp(pcommon.NewTimestampFromTime(e.ts))
	lr.SetObservedTimestamp(s.observed)
	lr.SetSeverityNumber(e.severity)
	lr.SetSeverityText(e.sevText)
	lr.Body().SetStr(e.body)
	putAttrs(lr.Attributes(), e.attrs)
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
			// Unreachable: eventAttrs builds only the three kinds above. If a
			// fourth is ever added, DROP it rather than emit a wrong value —
			// this arm used to write the KEY as the value (strconv.Quote(k)),
			// which is plausible-looking garbage; a missing attribute is the
			// honest failure, and the new type needs its own case here.
		}
	}
}
