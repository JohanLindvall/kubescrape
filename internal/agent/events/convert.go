package events

// Event -> OTLP log record. The involved object's identity becomes the
// RESOURCE, so an event about a pod carries that pod's full attribute set
// (owner chain, labels, node, service.name) and therefore correlates with its
// logs and metrics in one query. That resolution is what a flat
// events-as-logs stream cannot do.

import (
	"context"
	"errors"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
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
	okey := obsKey{uid: string(e.UID), rv: e.ResourceVersion}
	if sc := r.cfg.Chain.Scrub; sc != nil {
		// Scrub before anything copies from the body, as everywhere else. An
		// occurrence an earlier convert already ran the chain over (a watch
		// restart re-delivers it) is redacted the same but not counted again:
		// kubescrape_log_scrubbed_total is per RECORD (logchain.Input.Observed).
		if r.wasObserved(okey) {
			body = sc.ScrubUncounted(body)
		} else {
			body = sc.Scrub(body)
		}
	}
	sev, sevText := severityOf(e.Type)
	key, res := r.resource(ctx, e)

	ent := entry{
		body: body, ts: eventTime(e), severity: sev, sevText: sevText,
		resKey: key, res: res, rv: e.ResourceVersion,
		meta: newEventMeta(e),
		okey: okey,
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
	// EqualFold, as severityOf does: ToLower would allocate for every
	// capitalised value, which is every event the API server validates.
	switch {
	case strings.EqualFold(t, "normal"):
		return "normal"
	case strings.EqualFold(t, "warning"):
		return "warning"
	default:
		return "other"
	}
}

// eventMeta is the record-level attributes describing the event itself, held
// as plain fields on the batch entry and written in ONE fixed order (put).
//
// It used to be a map[string]any of boxed values, built per event and retained
// on every batch entry for as long as the batch is — up to retainCap entries
// across a collector outage — only to be type-switched back into the record at
// convert: 16 allocations and ~1.1 KB per event, about half the per-entry
// retention budget. And because the render RANGED a Go map, one event's
// attributes came out in a different order on every render (12 orders in 50
// renders of one event), so identical events never rendered byte-identically.
type eventMeta struct {
	uid, name, reason, action, typ                     string
	involvedKind, involvedName, involvedUID, fieldPath string
	reportingComponent, reportingInstance              string
	count                                              int64
}

// newEventMeta captures the event's own attributes.
func newEventMeta(e *corev1.Event) eventMeta {
	m := eventMeta{
		uid: string(e.UID), name: e.Name, reason: e.Reason, action: e.Action, typ: e.Type,
		involvedKind: e.InvolvedObject.Kind, involvedName: e.InvolvedObject.Name,
		involvedUID: string(e.InvolvedObject.UID), fieldPath: e.InvolvedObject.FieldPath,
		reportingComponent: reportingComponent(e), reportingInstance: e.ReportingInstance,
		count: int64(e.Count),
	}
	if e.Series != nil && e.Series.Count > 0 {
		m.count = int64(e.Series.Count)
	}
	return m
}

// put writes the attributes onto a record. An empty string is an UNSET field
// and is omitted rather than exported as ""; the count is always present.
func (m *eventMeta) put(dst pcommon.Map) {
	dst.EnsureCapacity(dst.Len() + 12)
	putStr(dst, "k8s.event.uid", m.uid)
	putStr(dst, "k8s.event.name", m.name)
	putStr(dst, "k8s.event.reason", m.reason)
	putStr(dst, "k8s.event.action", m.action)
	putStr(dst, "k8s.event.type", m.typ)
	dst.PutInt("k8s.event.count", m.count)
	putStr(dst, "k8s.event.involved_object.kind", m.involvedKind)
	putStr(dst, "k8s.event.involved_object.name", m.involvedName)
	putStr(dst, "k8s.event.involved_object.uid", m.involvedUID)
	putStr(dst, "k8s.event.involved_object.field_path", m.fieldPath)
	putStr(dst, "k8s.event.reporting_component", m.reportingComponent)
	putStr(dst, "k8s.event.reporting_instance", m.reportingInstance)
}

// putStr puts a non-empty value.
func putStr(dst pcommon.Map, k, v string) {
	if v != "" {
		dst.PutStr(k, v)
	}
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

// unresolvedRetryAfter is how long a FAILED pod resolution is memoized before
// the next event about that pod asks the metadata service again (resource()).
const unresolvedRetryAfter = 30 * time.Second

// unresolvedGroup suffixes the grouping key of a resource built WITHOUT the
// pod's resolved identity, so records carrying it never share a ResourceLogs
// with records about the same pod that did resolve (resource()).
const unresolvedGroup = "\x00unresolved"

// resEntry is one memoized involved-object resource.
type resEntry struct {
	res pcommon.Resource
	// group is the grouping key records carrying res are filed under.
	group string
	// retryAt is zero for a resource that is FINAL for the batch (resolved, or
	// an object that is never looked up) and, for a failed pod resolution, when
	// the memo stops answering for it.
	retryAt time.Time
}

// resolution is how buildResource came by a resource.
type resolution int

const (
	// resolvedFinal: the pod resolved, or the object is not one that is looked
	// up; nothing a later event could learn would change it.
	resolvedFinal resolution = iota
	// resolvedFailed: a pod whose lookup failed or answered another
	// incarnation; the resource carries only the event's own identity.
	resolvedFailed
	// resolvedInterrupted: the lookup was cut short by the reader's own
	// context — a shutdown or a lost lease, not a metadata failure.
	resolvedInterrupted
)

// resource returns the involved object's resource and a grouping key,
// memoized by the object for the life of the batch — a FAILED resolution only
// briefly.
//
// Within one render there is no per-event decision to make: logchain.Groups.Get
// fills a group's resource from the FIRST entry that lands in it, so every later
// entry's freshly built resource was discarded unread; retained heap was FLAT
// in the number of distinct involved objects. Repeats dominate this pipeline by
// design: Kubernetes aggregates recurrences into ONE object re-sent as
// Modified, which is the "BackOff x47" case handle exists to catch. Measured at
// 1012 B and ~2.1 µs per build, a 256-event flush over 32 pods wasted 224 of
// them.
//
// ACROSS renders there is one, and it is why a failure is not memoized for the
// batch: a collector outage settles nothing, so the batch — and a memo held for
// its life — lasts as long as the outage, and a pod lookup that failed once (a
// metadata service down at the same time) went on answering "unresolved" for
// every event about that pod ingested after the service came back. A failure is
// therefore retried after unresolvedRetryAfter — one lookup per pod per window,
// which keeps the bound that matters against a HANGING service (lookupPod's
// pause covers the rest) — and its records group apart (unresolvedGroup), or a
// pod's resolved events would land in the group its unresolved ones created and
// take their name-only resource after all. A lookup the reader's own context
// cut short is neither memoized nor reported: it is a shutdown, and the next
// leader asks again.
func (r *Reader) resource(ctx context.Context, e *corev1.Event) (string, pcommon.Resource) {
	obj := e.InvolvedObject
	key := resourceKey(&obj)
	if c, ok := r.resCache[key]; ok && (c.retryAt.IsZero() || r.now().Before(c.retryAt)) {
		return c.group, c.res
	}
	res, how := r.buildResource(ctx, e)
	c := resEntry{res: res, group: key}
	switch how {
	case resolvedInterrupted:
		return key + unresolvedGroup, res
	case resolvedFailed:
		c.group = key + unresolvedGroup
		c.retryAt = r.now().Add(unresolvedRetryAfter)
	}
	if r.resCache == nil {
		r.resCache = make(map[string]resEntry, 8)
	} else if _, ok := r.resCache[key]; !ok && len(r.resCache) >= maxResCache {
		clear(r.resCache)
	}
	r.resCache[key] = c
	return c.group, c.res
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
// event itself carries. The resolution says which happened (see resource()).
func (r *Reader) buildResource(ctx context.Context, e *corev1.Event) (pcommon.Resource, resolution) {
	obj := e.InvolvedObject
	res := pcommon.NewResource()
	// No .Node: the node an event is ABOUT is a property of the involved
	// object (the resolved pod carries its own), never the singleton reader's
	// own node — which has nothing to do with where the event happened.
	actx := attrs.Context{}

	resolved, how := false, resolvedFinal
	if obj.Kind == "Pod" && obj.Name != "" && obj.Namespace != "" && r.cfg.Meta != nil {
		pod, err := r.lookupPod(ctx, &obj)
		switch {
		case (err != nil || pod == nil) && ctx.Err() != nil:
			// Cut short by OUR context — a shutdown or a lost lease mid-page —
			// which says nothing about the metadata service: reporting it
			// raised a false kubescrape_events_unresolved_total{reason="lookup"}
			// and a Warn at every handover that landed mid-walk.
			how = resolvedInterrupted
		case err != nil || pod == nil:
			how = resolvedFailed
			// The correlation this pipeline exists for is what just failed, and
			// nothing downstream can tell: the event still exports, under the
			// identity it carries, so every other counter stays green. See
			// obs.EventsUnresolved.
			r.reportUnresolved(&obj, reasonLookup, err)
		case obj.UID != "" && string(obj.UID) != pod.UID:
			// A pod of that name exists but is a different incarnation, so
			// adopting it would attribute this event to the wrong pod. Refusing
			// is right; being silent about it is not — this arm issues a
			// SUCCESSFUL lookup, so kubescrape_metadata_requests_total cannot
			// show it either.
			how = resolvedFailed
			r.reportUnresolved(&obj, reasonUIDMismatch, nil)
		default:
			actx.Pod = pod
			resolved = true
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
	return res, how
}

// Pod lookup bounds (see lookupPod).
const (
	// podLookupTimeout bounds ONE lookup. /v1/pods/{ns}/{name} is a store read
	// on the metadata service with no server-side wait, so a healthy answer is
	// milliseconds; the shared client's own timeout (the larger of
	// -metadata-wait and -ingest-metadata-wait, plus 10s) is sized for lookups
	// the server is allowed to hold, not this one.
	podLookupTimeout = 2 * time.Second
	// podLookupPause is how long lookups stay off after one timed out.
	podLookupPause = 30 * time.Second
)

// errPodLookupsPaused is what an event skipped by the pause is reported with
// (reportUnresolved), so its Debug line says why no lookup was issued.
var errPodLookupsPaused = errors.New("pod lookups are paused: the metadata service did not answer an earlier one within " +
	podLookupTimeout.String())

// lookupPod resolves the involved Pod, bounded so a metadata service that
// HANGS — partitioned, blackholed, not refusing — costs attribution and never
// the stream.
//
// The lookup runs on the reader's only goroutine, the one that also services
// the watch and the flush ticker. Bounded only by the shared client's ~15s
// timeout, every distinct involved pod cost that much: the watch path fell to a
// few events a minute, and on the replay path the first page's lookups outlived
// the continue token, so every relist lap aborted with nothing exported. Two
// bounds, the promscrape rule (a metadata outage costs ATTRIBUTION, not data):
// each lookup gets podLookupTimeout, and one that runs out of it PAUSES lookups
// for podLookupPause, during which events export under the identity they carry
// and count kubescrape_events_unresolved_total{reason="lookup"} like any other
// failed lookup. Only a TIMEOUT pauses: a refused connection (the service's
// pod down, no endpoints) fails in microseconds and costs nothing to repeat,
// and pausing on it would throw away attribution the moment the service is
// back.
func (r *Reader) lookupPod(ctx context.Context, obj *corev1.ObjectReference) (*kubemeta.Pod, error) {
	if r.now().Before(r.podLookupsPausedUntil) {
		return nil, errPodLookupsPaused
	}
	timeout := r.lookupTimeout
	if timeout <= 0 {
		timeout = podLookupTimeout
	}
	lctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	pod, err := r.cfg.Meta.PodByName(lctx, obj.Namespace, obj.Name)
	if err != nil && ctx.Err() == nil && errors.Is(lctx.Err(), context.DeadlineExceeded) {
		r.podLookupsPausedUntil = r.now().Add(podLookupPause)
		if r.lookupPauseWarn.Allow(unresolvedWarnEvery) {
			r.log.Warn("the metadata service did not answer a pod lookup in time; pausing pod lookups so the event stream keeps flowing — events meanwhile keep the identity they carry",
				"timeout", timeout, "backoff", podLookupPause, "namespace", obj.Namespace, "pod", obj.Name, "error", err)
		}
	}
	return pod, err
}

// Reasons for obs.EventsUnresolved. Metric label VALUES, named once so
// publishMetrics can give each of them a series at zero — a reason that only
// appears the first time a cluster produces it is the absent-vs-zero defect the
// publication exists to close.
const (
	reasonLookup      = "lookup"
	reasonUIDMismatch = "uid_mismatch"
)

// unresolvedReasons is every value reportUnresolved can pass.
var unresolvedReasons = []string{reasonLookup, reasonUIDMismatch}

// unresolvedWarnEvery re-warns about unresolvable involved pods at this
// cadence. The condition is a state (the metadata service is unreachable, or a
// whole ReplicaSet is past its tombstone TTL), noticed once per distinct
// involved object per batch and per unresolvedRetryAfter — a flood
// proportional to the cluster's event rate without the throttle.
const unresolvedWarnEvery = 5 * time.Minute

// reportUnresolved records that an event about a Pod is being exported without
// that pod's resolved identity: a counter for the rate, a per-object Debug for
// "why did THIS one lose its labels", and a throttled Warn so the condition is
// visible without one.
//
// Debug is not guarded: every argument is a field read or an already-materialised
// error, and this runs once per failed resolution — per distinct involved
// object per batch, and per unresolvedRetryAfter while it keeps failing
// (resource() memoizes) — not per event.
func (r *Reader) reportUnresolved(obj *corev1.ObjectReference, reason string, err error) {
	obs.EventsUnresolved.WithLabelValues(reason).Inc()
	args := []any{"reason", reason, "namespace", obj.Namespace, "pod", obj.Name, "uid", string(obj.UID)}
	if err != nil {
		// Only when there IS one: the uid_mismatch arm has no error, and
		// error=<nil> on a line reads as an error nobody can look up.
		args = append(args, "error", err)
	}
	r.log.Debug("the involved pod did not resolve; the event keeps the identity it carries", args...)
	if r.unresolvedWarn.Allow(unresolvedWarnEvery) {
		r.log.Warn("events about pods are being exported without the pod's resolved identity (owner chain, labels, node, service.name), so they will not correlate with that pod's logs and metrics", args...)
	}
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
	cc := r.cfg.Chain
	cc.Scrub = nil // scrubbed at ingest
	chain := logchain.NewChain[string](cc, false)

	for _, e := range r.batch {
		observed := r.wasObserved(e.okey)
		body, extracted := chain.Line(e.body, observed) // Scrub is nil here: ingest redacted it
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
			Observed: observed,
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
	e := &s.e
	lr.SetTimestamp(pcommon.NewTimestampFromTime(e.ts))
	lr.SetObservedTimestamp(s.observed)
	lr.SetSeverityNumber(e.severity)
	lr.SetSeverityText(e.sevText)
	lr.Body().SetStr(e.body)
	e.meta.put(lr.Attributes())
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
