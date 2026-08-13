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
		okey:  obsKey{uid: string(e.UID), rv: e.ResourceVersion},
	}
	if ent.ts.IsZero() {
		ent.ts = e.CreationTimestamp.Time
	}
	if len(r.batch) >= r.retainCap() {
		r.shedOldest()
	}
	r.batch = append(r.batch, ent)
	obs.EventsObserved.WithLabelValues(eventTypeLabel(e.Type)).Inc()
}

// eventTypeLabels is every value eventTypeLabel can return, so publishMetrics
// can give each of them a series at zero. TestEventTypeLabelsAreComplete keeps
// the two in step: a value missing here is a series that appears only once the
// cluster happens to produce it, which is the absent-vs-zero defect the
// publication exists to close.
var eventTypeLabels = []string{"normal", "warning", "other"}

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

// maxResCache bounds the per-batch resource memo. It is cleared with the batch
// (settle) and on a stream restart, but a collector outage settles nothing, so
// it needs a bound of its own; past it the memo starts a fresh generation
// rather than growing alongside a batch that is itself capped.
const maxResCache = 1024

// resource returns the involved object's resource and a grouping key,
// memoized by that key for the life of the batch.
//
// The memo is not an optimisation of a per-event decision — there is no
// per-event decision to make. logchain.Groups.Get fills a group's resource
// from the FIRST entry that lands in it, so every later entry's freshly built
// resource was discarded unread; retained heap was FLAT in the number of
// distinct involved objects. Repeats dominate this pipeline by design:
// Kubernetes aggregates recurrences into ONE object re-sent as Modified, which
// is the "BackOff x47" case handle exists to catch. Measured at 1012 B and
// ~2.1 µs per build, a 256-event flush over 32 pods wasted 224 of them.
func (r *Reader) resource(ctx context.Context, e *corev1.Event) (string, pcommon.Resource) {
	obj := e.InvolvedObject
	key := resourceKey(&obj)
	if res, ok := r.resCache[key]; ok {
		return key, res
	}
	res := r.buildResource(ctx, e)
	if r.resCache == nil {
		r.resCache = make(map[string]pcommon.Resource, 8)
	} else if len(r.resCache) >= maxResCache {
		clear(r.resCache)
	}
	r.resCache[key] = res
	return key, res
}

// resourceKey identifies the involved object. Two events about one object
// share a ResourceLogs — and, through the memo above, one built resource.
func resourceKey(obj *corev1.ObjectReference) string {
	var key strings.Builder
	key.WriteString(obj.Kind)
	key.WriteByte('\x00')
	key.WriteString(obj.Namespace)
	key.WriteByte('\x00')
	key.WriteString(obj.Name)
	key.WriteByte('\x00')
	key.WriteString(string(obj.UID))
	return key.String()
}

// buildResource resolves the involved object's identity. A Pod resolves
// through the metadata service to its full identity (tombstones keep events
// about just-deleted pods resolvable); anything else gets the identity the
// event itself carries.
func (r *Reader) buildResource(ctx context.Context, e *corev1.Event) pcommon.Resource {
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
	return res
}

// convert groups the batch into one ResourceLogs per involved object. The
// per-record half — line attributes, enrichment, log-metrics (which see EVERY
// record) and the keep/drop rules — is the shared chain every log producer in
// this repo runs, in the same order (internal/agent/logchain); what stays here
// is the involved object's RESOURCE and the grouping.
//
// Bodies are already scrubbed: ingest redacts where it builds the batch entry,
// before the record exists, so the chain's Scrub is nil.
//
// An entry an earlier convert already ran the chain over says so
// (logchain.Input.Observed), and the observed set is what makes that decidable.
// logchain.Pending keeps ONE conversion per batch epoch, which covers the
// export retries — but not a WATCH RESTART, which clears the batch and its
// rendering precisely because the new stream re-delivers every buffered entry
// (stream). Those re-ingested events are converted afresh, so without this the
// operator's own log metrics and kubescrape_log_rules_dropped_total stepped
// once per restart or relist lap over the whole retained batch — a rate() spike
// during exactly the outage those series are read to diagnose, and a permanent
// upward bias afterwards. It is the defect journald fixed by retrying its batch
// in place; this reader cannot (the API server re-sends what it re-sends), so
// it takes the tailer's route instead: make the OBSERVATION idempotent, with
// the occurrence itself as the proof.
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
			Observed: r.wasObserved(e.okey),
		})
	}
	// After the loop, never inside it: see markObserved.
	r.markObserved(r.batch)
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
