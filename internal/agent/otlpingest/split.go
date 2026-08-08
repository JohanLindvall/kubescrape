package otlpingest

import (
	"context"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// splitAndEnrich regroups every data point by the object ID on its own
// attributes, producing one ResourceMetrics per (input resource, object).
// Points without a resolvable ID stay under a copy of their original
// resource, unenriched. Metric identity (name/type/unit) and scope are
// preserved. The cache is the request's: lookups are memoized once per
// distinct ID across the whole batch, and the split budgets it carries span
// every input ResourceMetrics of the push.
func (e *Enricher) splitAndEnrich(ctx context.Context, cache *reqCache, md pmetric.Metrics) pmetric.Metrics {
	out := pmetric.NewMetrics()
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		g := &metricGrouper{
			enricher:    e,
			ctx:         ctx,
			srcResource: rm.Resource(),
			srcSchema:   rm.SchemaUrl(),
			srcSize:     resourceCopySize(rm.Resource(), rm.SchemaUrl()),
			cache:       cache,
			out:         out,
			rmByID:      map[string]pmetric.ResourceMetrics{},
			smByID:      map[idScope]pmetric.ScopeMetrics{},
			metByID:     map[idMetric]pmetric.Metric{},
		}
		// Points without their own ID fall back to the resource-level one, so
		// a mixed batch (auto mode) does not lose enrichment for resources
		// that carried the ID where it belongs.
		g.resToken = e.resolvableToken(ctx, cache, rm.Resource().Attributes())
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			ms := sm.Metrics()
			for k := 0; k < ms.Len(); k++ {
				g.route(sm, j, ms.At(k), k)
			}
		}
	}
	return out
}

type idScope struct {
	id    string
	scope int
}

type idMetric struct {
	id     string
	scope  int
	metric int
}

// metricGrouper accumulates one input ResourceMetrics' points into per-ID
// output resources. The dedup maps (rmByID/smByID/metByID) are per input
// resource — the same object described by two input resources gets two groups,
// one per source resource — but the BUDGETS are the request's (reqCache):
// per-grouper budgets re-armed the caps once per input ResourceMetrics, so the
// payload's own structure chose its bound.
type metricGrouper struct {
	enricher    *Enricher
	ctx         context.Context
	srcResource pcommon.Resource
	srcSchema   string
	srcSize     int    // estimated bytes one copy of the source resource retains
	resToken    string // resource-level ID, the fallback for ID-less points
	cache       *reqCache
	out         pmetric.Metrics
	rmByID      map[string]pmetric.ResourceMetrics
	smByID      map[idScope]pmetric.ScopeMetrics
	metByID     map[idMetric]pmetric.Metric
	refused     map[string]struct{} // IDs already counted split_capped (lazily allocated)
}

// route moves every data point of m into the output metric for its ID. The
// per-type loops copy directly (no per-point closures — this is the ingest
// hot path).
func (g *metricGrouper) route(sm pmetric.ScopeMetrics, scopeIdx int, m pmetric.Metric, metricIdx int) {
	if metricPointCount(m) == 0 {
		// No data points to route (an empty metric, or MetricTypeEmpty): the
		// per-type loops below would create no shell and the descriptor would be
		// dropped. Resource mode returns the metric in place, so preserve it here
		// too under the resource-level ID.
		g.metric(sm, scopeIdx, m, metricIdx, g.resToken)
		return
	}
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			dst := g.metricFor(sm, scopeIdx, m, metricIdx, dp.Attributes())
			dp.CopyTo(dst.Gauge().DataPoints().AppendEmpty())
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			dst := g.metricFor(sm, scopeIdx, m, metricIdx, dp.Attributes())
			dp.CopyTo(dst.Sum().DataPoints().AppendEmpty())
		}
	case pmetric.MetricTypeHistogram:
		dps := m.Histogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			dst := g.metricFor(sm, scopeIdx, m, metricIdx, dp.Attributes())
			dp.CopyTo(dst.Histogram().DataPoints().AppendEmpty())
		}
	case pmetric.MetricTypeExponentialHistogram:
		dps := m.ExponentialHistogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			dst := g.metricFor(sm, scopeIdx, m, metricIdx, dp.Attributes())
			dp.CopyTo(dst.ExponentialHistogram().DataPoints().AppendEmpty())
		}
	case pmetric.MetricTypeSummary:
		dps := m.Summary().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			dst := g.metricFor(sm, scopeIdx, m, metricIdx, dp.Attributes())
			dp.CopyTo(dst.Summary().DataPoints().AppendEmpty())
		}
	}
}

// metricPointCount is the number of data points on m, across its type (0 for
// MetricTypeEmpty).
func metricPointCount(m pmetric.Metric) int {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		return m.Gauge().DataPoints().Len()
	case pmetric.MetricTypeSum:
		return m.Sum().DataPoints().Len()
	case pmetric.MetricTypeHistogram:
		return m.Histogram().DataPoints().Len()
	case pmetric.MetricTypeExponentialHistogram:
		return m.ExponentialHistogram().DataPoints().Len()
	case pmetric.MetricTypeSummary:
		return m.Summary().DataPoints().Len()
	}
	return 0
}

// metricFor resolves one data point's ID (falling back to the resource-level
// one) and returns its output metric.
func (g *metricGrouper) metricFor(sm pmetric.ScopeMetrics, scopeIdx int, m pmetric.Metric, metricIdx int, dpAttrs pcommon.Map) pmetric.Metric {
	token := g.enricher.resolvableToken(g.ctx, g.cache, dpAttrs)
	if token == "" {
		token = g.resToken
	}
	return g.metric(sm, scopeIdx, m, metricIdx, token)
}

// metric returns the output metric for the given ID, creating the resource,
// scope and metric shells (and enriching the resource) on first use. It is the
// ONE entry to the creation chain (scope and resource are reached only from
// here), so the budget gate below covers every copy the splitter mints.
func (g *metricGrouper) metric(sm pmetric.ScopeMetrics, scopeIdx int, m pmetric.Metric, metricIdx int, id string) pmetric.Metric {
	mk := idMetric{id: id, scope: scopeIdx, metric: metricIdx}
	if dst, ok := g.metByID[mk]; ok {
		return dst
	}
	// The fallback ids are load-bearing, not decorative: each is a SINGLE
	// resource/scope/shell set per input resource, whose creations are bounded
	// by the input, so both are exempt from both budgets. Gating them would
	// recurse right here (a refused fallback folding back into itself): an
	// unbounded stack on an unauthenticated listener, i.e. any pod could crash
	// the agent with a >maxSplitGroups-object push.
	if !isFallbackID(id) && !g.admit(id) {
		return g.metric(sm, scopeIdx, m, metricIdx, g.foldTarget(id))
	}
	scope := g.scope(sm, scopeIdx, id)
	dst := scope.Metrics().AppendEmpty()
	dst.SetName(m.Name())
	dst.SetDescription(m.Description())
	dst.SetUnit(m.Unit())
	m.Metadata().CopyTo(dst.Metadata())
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dst.SetEmptyGauge()
	case pmetric.MetricTypeSum:
		s := dst.SetEmptySum()
		s.SetAggregationTemporality(m.Sum().AggregationTemporality())
		s.SetIsMonotonic(m.Sum().IsMonotonic())
	case pmetric.MetricTypeHistogram:
		dst.SetEmptyHistogram().SetAggregationTemporality(m.Histogram().AggregationTemporality())
	case pmetric.MetricTypeExponentialHistogram:
		dst.SetEmptyExponentialHistogram().SetAggregationTemporality(m.ExponentialHistogram().AggregationTemporality())
	case pmetric.MetricTypeSummary:
		dst.SetEmptySummary()
	}
	g.cache.splitCopied += metricShellSize(m)
	g.metByID[mk] = dst
	return dst
}

// overflowID keys the group a budget-refused id's points fall into. It is a
// group of its OWN rather than the id-less "" chain because the two describe
// different things: "" holds the SENDER's own points, so its copy keeps the
// sender's identity, while a refused id's points describe OTHER objects — the
// exporter's k8s.pod.name/service.name on them is the same misattribution the
// admitted foreign groups strip. The byte budget binds at
// maxSplitCopyBytes/maxSplitGroups = 8 KiB of resource estimate, so any
// attribute-rich sender reaches it long before the group cap and the fold was
// anything but rare. The points keep their own id attributes, so a downstream
// consumer can still re-resolve what this push could not afford to.
//
// The value cannot collide with a real token: those are tokContainer/tokPodUID
// followed by a non-empty value.
const overflowID = "\x00overflow"

// isFallbackID reports whether id names one of the two per-input-resource
// fallback chains rather than a described object.
func isFallbackID(id string) bool { return id == "" || id == overflowID }

// foldTarget picks the chain a refused id folds into. The sender's OWN id keeps
// the sender's resource: those points describe the exporter, so its identity IS
// theirs and stripping it would lose what the push got right. Anything else
// describes another object and takes the stripped overflow group.
//
// The test is token equality, not sameObject: resolving a refused id to compare
// objects would spend the very lookups the budgets are refusing to spend, and
// the case this covers — a sender labelling its own points with the id its
// resource already carries — is exactly equality.
func (g *metricGrouper) foldTarget(id string) string {
	if id == g.resToken {
		return ""
	}
	return overflowID
}

// admit reports whether copies keyed by id may still be minted, against the
// push-wide budgets: the group COUNT (a new id past maxSplitGroups shares the
// fallback) and the copy BYTES (past maxSplitCopyBytes even an EXISTING
// group's new scope/shell copies are refused — the byte bound is on the
// output, and a group admitted cheaply must not go on minting expensive
// descriptor copies). A refused id's points fold into the overflow fallback:
// forwarded under a copy of the source resource stripped of the sender's
// identity, unenriched. That is true for the GROUPED id too: the byte budget
// refuses the new (scope, metric) shell this point needs, so the point lands
// in the overflow group beside the never-grouped remainder, while the group's
// EXISTING shells keep taking points — the object's series end up split across
// its enriched resource and the stripped overflow one.
//
// Exempting an existing group from the byte gate — its resource copy is paid,
// and one more shell is small — was considered and rejected: shell bytes are
// sender-controlled (name, description, metadata) and MULTIPLY, up to
// #groups x #descriptors copies of descriptors the wire carries once, so a
// 16 MiB payload could mint gigabytes of exempt shells. The gate stays; the
// degradation is counted instead.
func (g *metricGrouper) admit(id string) bool {
	_, grouped := g.rmByID[id]
	over := g.cache.splitCopied >= maxSplitCopyBytes ||
		(!grouped && g.cache.splitGroups >= maxSplitGroups)
	if !over {
		return true
	}
	// Counted once per refused OBJECT, per input resource — grouped or not. A
	// grouped id is refused only its FURTHER descriptor copies, but those
	// points fold into the stripped overflow resource unenriched, which is
	// exactly what split_capped reports; leaving it uncounted (as this branch
	// once did, behind a comment claiming the points "still land on their own
	// enriched resource") made a mid-push byte-budget bind invisible: the
	// counter moved only for never-grouped ids while an admitted object's
	// series quietly forked onto the overflow resource.
	if _, seen := g.refused[id]; !seen {
		if g.refused == nil {
			g.refused = map[string]struct{}{}
		}
		g.refused[id] = struct{}{}
		obs.Ingested.WithLabelValues("split_capped").Inc()
	}
	return false
}

func (g *metricGrouper) scope(sm pmetric.ScopeMetrics, scopeIdx int, id string) pmetric.ScopeMetrics {
	sk := idScope{id: id, scope: scopeIdx}
	if dst, ok := g.smByID[sk]; ok {
		return dst
	}
	rm := g.resource(id)
	dst := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().CopyTo(dst.Scope())
	dst.SetSchemaUrl(sm.SchemaUrl())
	g.cache.splitCopied += scopeCopySize(sm)
	g.smByID[sk] = dst
	return dst
}

// senderIdentityAttrs are the resource attributes that name WHO a resource is.
// On a split group the resource describes a DIFFERENT object than the sender,
// so every one of these belongs to the described object or to nobody — never to
// the exporter that happened to report it.
//
// INVARIANT: this list must cover every fixed key the attrs builder can emit.
// The strip matters exactly for the keys the described object LACKS —
// overwriteAttrs replaces only what the builder emits for THAT object, so any
// builder-emittable key missing here survives from the EXPORTER onto the
// object it describes. It is therefore attrs.IdentityKeys() (the pinned union
// of every such key; a hand-copied list here had already drifted, letting a
// sender's k8s.service.name/uid leak onto every split-described object) plus
// the semconv siblings senders set that the builder never emits.
// TestSenderIdentityAttrsCoverEveryBuilderIdentityKey holds the superset.
//
// Known, accepted gap: the label attributes (k8s.pod.label.*,
// k8s.namespace.label.*) are keyed by DATA and not enumerable, so a sender's
// pod labels whose keys the described object lacks are not stripped.
var senderIdentityAttrs = append(attrs.IdentityKeys(),
	// container.name: the bare semconv sibling of k8s.container.name. The
	// builder never emits it, but SDK container detectors do, and it names the
	// sender's container as surely as the prefixed form.
	"container.name",
)

// stripSenderIdentity removes the sender's own identity from a resource that is
// about to be re-labelled as a described object's.
func stripSenderIdentity(a pcommon.Map) {
	for _, k := range senderIdentityAttrs {
		a.Remove(k)
	}
}

// maxSplitGroups bounds the per-object resources one PUSH may inflate into —
// across every input ResourceMetrics, which is why the count lives on the
// request's cache rather than the grouper. Each group is a full copy of the
// sender's resource, so the bound is on MEMORY, which the ingest byte budget
// cannot express — it counts the payload's raw bytes, and a small payload can
// name a great many distinct objects. Past the cap the remaining objects share
// the source resource — unenriched, but forwarded and counted, which is
// strictly better than an OOM the process cannot defend against on an
// unauthenticated listener.
const maxSplitGroups = 2048

// maxSplitCopyBytes bounds the estimated bytes of the copies the splitter
// mints per push: source-resource copies, scope copies and metric descriptor
// shells. The group cap alone cannot bound OUTPUT SIZE — every admitted group
// repeats the sender's resource, and every point routed to a group's new shell
// repeats its metric's descriptor — so a push carrying a few hundred KiB of
// resource attributes inflated to hundreds of MB: marshalled as one allocation
// by the disk buffer's enqueue, or re-sent as a thousand otlpsplit parts
// without one. 16 MiB is far above what real described-object pushes mint
// (thousands of groups times KiB-scale resources) and a handful of otlpexport
// part-splits; past it, creations fold into the "" fallback exactly as the
// group cap's overflow does, counted under the same outcome.
const maxSplitCopyBytes = 16 << 20

func (g *metricGrouper) resource(id string) pmetric.ResourceMetrics {
	if rm, ok := g.rmByID[id]; ok {
		return rm
	}
	// Admission against the push-wide budgets already happened in metric(), the
	// one entry to this chain; here every creation is only CHARGED.
	rm := g.out.ResourceMetrics().AppendEmpty()
	g.srcResource.CopyTo(rm.Resource())
	rm.SetSchemaUrl(g.srcSchema)
	g.cache.splitCopied += g.srcSize
	if id == overflowID {
		// These points describe objects this push could not afford a group for.
		// The copied resource is the SENDER's, and its identity names the
		// exporter — leaving it here labels every refused object's points with
		// the exporter's pod and service.name, which in Prometheus terms is N
		// objects' series under one job/instance. Strip it: an unenriched
		// resource is a visible gap, a confidently wrong one is not.
		g.stripIDAttrs(rm.Resource().Attributes())
		stripSenderIdentity(rm.Resource().Attributes())
	} else if id != "" {
		g.cache.splitGroups++
		// One same-object predicate for the whole enricher (Enricher.sameObject):
		// this used to be a local copy that disagreed with the auto-mode
		// decision's — see the war story on the shared one.
		if id != g.resToken && !g.enricher.sameObject(g.ctx, g.cache, id, g.resToken) {
			// This group is keyed by a point-level ID that differs from the
			// resource's own: the copied resource describes a DIFFERENT object.
			// Its ID attributes would mislabel (and mis-enrich downstream) every
			// point in the group — and so would the rest of the sender's identity
			// (k8s.pod.name, service.name, …), which names the EXPORTER, not the
			// object. The sender is authoritative about itself, not about others,
			// so the resolved identity OVERWRITES rather than merges here. Sender
			// attributes the builder does not supply (cluster name, SDK attrs,
			// custom) are untouched.
			g.stripIDAttrs(rm.Resource().Attributes())
			r := g.enricher.builtAttrs(g.ctx, g.cache, id)
			if !r.resolved {
				// The described object did NOT resolve. Overwriting nothing would
				// leave the copied SENDER identity (k8s.pod.name, service.name, …)
				// labeling a foreign object's points — misattribution, with the ID
				// stripped so downstream could never re-resolve it. Reduce the
				// resource to just the described object's raw ID instead.
				rm.Resource().Attributes().Clear()
				g.putIDAttr(rm.Resource().Attributes(), id)
			} else {
				// Clear the sender's OWN Kubernetes identity first. overwriteAttrs
				// replaces only the keys the builder emits, so any identity key
				// the described object happens to lack — a container name when the
				// described object is a pod, a different owner kind, the sender's
				// service.name — survived from the EXPORTER onto the object it is
				// describing, which is the exact mislabeling this branch exists to
				// prevent. Non-identity attributes (cluster name, SDK attrs,
				// custom) are still the sender's to keep.
				stripSenderIdentity(rm.Resource().Attributes())
				overwriteAttrs(r.built, rm.Resource().Attributes())
			}
			g.rmByID[id] = rm
			return rm
		}
		mergeAttrs(g.enricher.builtAttrs(g.ctx, g.cache, id).built, rm.Resource().Attributes())
	} else if g.resToken == "" {
		// No ID anywhere for these points: the opt-in peer-IP fallback still
		// attributes them to the pushing pod (resolved once per request). The
		// outcome — peer_ip, peer_ip_rejected or unresolved — is counted inside
		// peerFallback, the one accounting shared with the resource path; an
		// open-coded copy here counted nothing for a rejected peer.
		//
		// The resToken guard is what keeps that accounting honest. This chain is
		// only the "the sender named nothing" case when the RESOURCE named
		// nothing either; reaching it any other way (budget-refused ids used to
		// fold in here) tallied an unresolved sender for a push whose resource
		// had resolved perfectly.
		if built, resolved := g.enricher.peerFallback(g.ctx, g.cache); resolved {
			mergeAttrs(built, rm.Resource().Attributes())
		}
	}
	g.rmByID[id] = rm
	return rm
}

// putIDAttr re-stamps the described object's raw ID under its canonical
// (first-configured) attribute key, so unresolved split points remain
// re-attributable downstream.
func (g *metricGrouper) putIDAttr(a pcommon.Map, token string) {
	if len(token) <= len(tokContainer) {
		return
	}
	keys := g.enricher.containerIDKeys
	if strings.HasPrefix(token, tokPodUID) {
		keys = g.enricher.podUIDKeys
	}
	if len(keys) > 0 {
		a.PutStr(keys[0], token[len(tokContainer):])
	}
}

// stripIDAttrs removes the configured container-ID/pod-UID attribute keys.
func (g *metricGrouper) stripIDAttrs(a pcommon.Map) {
	for _, k := range g.enricher.containerIDKeys {
		a.Remove(k)
	}
	for _, k := range g.enricher.podUIDKeys {
		a.Remove(k)
	}
}

// resourceCopySize, scopeCopySize and metricShellSize estimate the HEAP one
// splitter-minted copy retains, charged against maxSplitCopyBytes. They are not
// wire-size estimators — the budget bounds memory, and the two differ in both
// directions: pcommon's CopyTo copies string HEADERS (a long value's bytes are
// shared, not re-allocated) while every copied attribute costs a slot and a
// boxed value the wire does not name. The string payloads are still charged,
// deliberately: they are the sender-controlled unbounded input, and
// over-charging a copy binds the ceiling early, which is the safe direction.
// Exactness is not needed; the SHAPE is (see keyValueOverheadBytes).
func resourceCopySize(res pcommon.Resource, schema string) int {
	return attrsSize(res.Attributes()) + len(schema) + 16
}

func scopeCopySize(sm pmetric.ScopeMetrics) int {
	sc := sm.Scope()
	return len(sc.Name()) + len(sc.Version()) + attrsSize(sc.Attributes()) + len(sm.SchemaUrl()) + 16
}

func metricShellSize(m pmetric.Metric) int {
	return len(m.Name()) + len(m.Description()) + len(m.Unit()) + attrsSize(m.Metadata()) + 16
}

// keyValueOverheadBytes is what one copied attribute costs REGARDLESS of its
// payload — its slot in the destination's KeyValue slice plus the copied
// value's own framing. Charging only the string payloads estimated a
// 200-attribute resource at under a quarter of what its copy really cost (4.6x
// for short values, 4.3x for ints; TestCopySizeEstimateBoundsRealMemory
// measures it), so the ceiling bound that much too late — and the effective
// bound is that, times -ingest-max-in-flight, on an unauthenticated listener.
const keyValueOverheadBytes = 48

func attrsSize(m pcommon.Map) int {
	n := 0
	m.Range(func(k string, v pcommon.Value) bool {
		n += len(k) + valueSize(v) + keyValueOverheadBytes
		return true
	})
	return n
}

// valueSize recurses like pcommon's own CopyTo does — no depth cap of its own,
// since any structure it can be handed has already been built (and will be
// copied) at that depth.
func valueSize(v pcommon.Value) int {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return len(v.Str()) + 8
	case pcommon.ValueTypeBytes:
		return v.Bytes().Len() + 8
	case pcommon.ValueTypeMap:
		return attrsSize(v.Map()) + 16
	case pcommon.ValueTypeSlice:
		n := 16
		sl := v.Slice()
		for i := 0; i < sl.Len(); i++ {
			n += valueSize(sl.At(i))
		}
		return n
	default:
		return 8
	}
}
