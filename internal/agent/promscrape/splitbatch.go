package promscrape

// The split batcher: the per-scrape runtime of a Splitter (split.go holds the
// operator-facing config, its compiler and target selection). It routes each
// series a rule matches onto a resource for the object the series DESCRIBES —
// one ResourceMetrics per identified object, resolved against the metadata
// service (resolveContext, the path the cadvisor rows take) when the rule asks
// for enrichment — and leaves every other series on the target's own resource.

import (
	"context"
	"slices"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// splitBatcher implements chunker for a split target: series matching a rule
// are routed to per-object resources; the rest stay on the target's own
// resource.
type splitBatcher struct {
	s        *Scraper
	ctx      context.Context
	target   kubemeta.ScrapeTarget
	sp       *Splitter
	startTS  pcommon.Timestamp
	scrapeTS pcommon.Timestamp

	// defaultPrefix is the describing target's own service.name, used as the
	// split resources' instance prefix unless a rule overrides it.
	defaultPrefix string

	md     pmetric.Metrics
	scopes map[string]splitDest
	byKey  map[string]pmetric.Metric
	points int
	bytes  int
	// keyBuf is the scratch buffer for resource/metric keys, the cadvisor
	// batcher's discipline: probes use map[string(keyBuf)] (no allocation) and
	// the string materializes only when a new resource or metric is inserted.
	// A kube-state-metrics exposition is FAMILY-major, so the last-seen memos
	// below miss on every row of it and this is the path that runs.
	keyBuf []byte

	// Last-seen memos, mirroring the plain batcher's metricByName/remember:
	// KSM series arrive grouped by family and object, so consecutive samples
	// usually route to the same resource and metric — a memcmp replaces the
	// key-building allocation and map probes.
	vals        []string // route scratch: one effective groupBy value per attr slot
	lastVals    []string // previous sample's per-slot values
	lastRule    *compiledSplitRule
	lastDest    splitDest
	lastRouteOK bool
	lastMName   string
	lastM       pmetric.Metric
	lastMOK     bool

	// Per-scrape regex memos (pure mappings, so they survive reset()): the
	// first-matching-rule per family name — ruleFor walks every rule's metrics
	// regex, and a family's series arrive as a run of samples — and the
	// dropLabels verdict per (rule, label name) — label names are a small
	// closed set, but putSplitLabels ran the regex per label per data point.
	ruleMemo map[string]*compiledSplitRule
	dropMemo map[dropKey]bool
	// budget is the key text those two memos hold alive, bounded by
	// maxMemoBytes through the same memoBudget filterSession uses, for the
	// identical reason and with the identical failure mode. Their entry caps
	// bound how MANY names are remembered, and a count is not a memory bound:
	// both keys are text the TARGET chooses (a series name, a label name),
	// capped only by the parser's line bound, and the memos deliberately
	// survive reset() so they accumulate for the whole scrape while the chunks
	// around them flush normally. ONE budget for the batcher, because it is the
	// batcher's retained heap that matters and either memo alone can spend it.
	budget memoBudget
}

// dropKey memoizes one rule's dropLabels verdict on one label name.
type dropKey struct {
	rule *compiledSplitRule
	name string
}

// splitDest is one resource's scope plus the attributes moved off that resource
// onto every one of its data points (rule.datapointAttr). They are always wanted
// together, so they share one map entry and one probe.
type splitDest struct {
	sm pmetric.ScopeMetrics
	// dp is charged to the batch estimate (labelBytes) with EVERY point it is
	// written onto. route() charges the resource AFTER removing these and the
	// per-point estimators only measure the sample's own labels, so #attrs ×
	// #points bytes were once encoded and counted nowhere. The default config
	// hid it: the same estimate over-charges the groupBy and dropLabels labels
	// it strips, and the two happened to cancel. A rule demoting several
	// attributes while grouping by one under-counts by ~1 MB at
	// BatchPoints=10000 — enough to push a 3 MiB estimate past the 4 MiB gRPC
	// limit the estimate exists to respect, at which point the collector
	// rejects every export of that target.
	dp []Label
}

func newSplitBatcher(ctx context.Context, s *Scraper, t kubemeta.ScrapeTarget, sp *Splitter, scrape time.Time) *splitBatcher {
	b := &splitBatcher{
		s: s, ctx: ctx, target: t, sp: sp,
		defaultPrefix: attrs.ServiceName(t.Pod),
		startTS:       pcommon.NewTimestampFromTime(s.cfg.StartTime),
		scrapeTS:      pcommon.NewTimestampFromTime(scrape),
		ruleMemo:      make(map[string]*compiledSplitRule, 64),
		dropMemo:      make(map[dropKey]bool, 64),
	}
	b.reset()
	return b
}

func (b *splitBatcher) reset() {
	b.md = pmetric.NewMetrics()
	if b.scopes == nil {
		b.scopes = make(map[string]splitDest)
		b.byKey = make(map[string]pmetric.Metric)
	} else {
		clear(b.scopes)
		clear(b.byKey)
	}
	b.points = 0
	b.bytes = 0
	// The memoized handles point into the previous batch's payload.
	b.lastRouteOK = false
	b.lastMOK = false
}

func (b *splitBatcher) take() pmetric.Metrics {
	md := b.md
	b.reset()
	return md
}

func (b *splitBatcher) count() int { return b.points }
func (b *splitBatcher) size() int  { return b.bytes }

// appendRouteKey appends the resource key of the current sample — the rule's
// identity plus the per-attribute effective groupBy values in b.vals — to b.
//
// The rule's identity is part of it because two rules with equal-cardinality
// groupBy sets must not merge objects whose values collide
// (kube_pod_info{pod="x"} vs kube_node_info{node="x"}). Values are
// length-prefixed (appendLP) so a label VALUE containing the separator cannot
// alias another tuple. It appends into a CALLER-OWNED scratch buffer, which is
// the whole point: an earlier attempt spelled this into a strings.Builder with
// appendLP on a fresh slice per call and cost +400 allocs/op, because the fresh
// slice — not appendLP — was the allocation.
func (b *splitBatcher) appendRouteKey(buf []byte, rule *compiledSplitRule) []byte {
	if rule == nil {
		return append(buf, "self"...)
	}
	buf = append(buf, rule.keyPrefix...)
	for _, v := range b.vals {
		buf = appendLP(buf, v)
	}
	return buf
}

// route returns the destination (scope plus the attributes moved onto its data
// points) and the rule for one series — rule nil for the target's own resource
// — and whether it is the SAME destination the previous call resolved. The
// previous sample's (rule, groupBy values) are memoized: a repeat costs value
// memcmps instead of rebuilding the key. A kube-state-metrics exposition is
// family-major, so the memo misses on every row and the keyed map probe below
// is the hot path.
func (b *splitBatcher) route(name string, labels []Label) (splitDest, *compiledSplitRule, bool) {
	rule, ok := b.ruleMemo[name]
	if !ok {
		rule = b.sp.ruleFor(name)
		if b.budget.admit(len(b.ruleMemo), maxTrackedFamilies, len(name)) {
			b.ruleMemo[name] = rule
		}
	}
	b.vals = b.vals[:0]
	if rule != nil {
		// One effective value per ATTRIBUTE slot: the last non-empty label
		// value in coalesce (sorted-label) order, container.id normalized.
		// This is the ONE place the coalesce happens: fillSplitResource renders
		// this very vector, so rows spelling one object through different
		// labels share one resource instead of minting byte-identical
		// duplicates (see groupMapping.slot).
		for range rule.slotAttrs {
			b.vals = append(b.vals, "")
		}
		for _, g := range rule.groupBy {
			v := labelValue(labels, g.label)
			if g.normalize {
				v = kubemeta.NormalizeContainerID(v)
			}
			if v != "" {
				b.vals[g.slot] = v
			}
		}
	}
	if b.lastRouteOK && rule == b.lastRule && slices.Equal(b.vals, b.lastVals) {
		return b.lastDest, rule, true
	}

	b.keyBuf = b.appendRouteKey(b.keyBuf[:0], rule)
	dest, ok := b.scopes[string(b.keyBuf)] // no alloc: map read elides the copy
	if !ok {
		key := string(b.keyBuf) // materialize once per new resource per batch
		rm := b.md.ResourceMetrics().AppendEmpty()
		if rule == nil {
			b.fillSelfResource(rm.Resource())
		} else {
			// b.vals still holds THIS sample's vector: lastVals is taken below,
			// and nothing on the way (the resolve included) touches it.
			b.fillSplitResource(rm.Resource(), rule, b.vals)
			// A split resource describes ANOTHER object (kube-state-metrics style);
			// the configured attributes (default k8s.node.name) are properties of
			// that object, not the exporter's identity, so move them off the resource
			// onto the data points — a queryable series label rather than part of the
			// resource / target_info.
			for _, attr := range rule.datapointAttr {
				if v, ok := rm.Resource().Attributes().Get(attr); ok {
					dest.dp = append(dest.dp, Label{Name: attr, Value: v.AsString()})
					rm.Resource().Attributes().Remove(attr)
				}
			}
		}
		dest.sm = rm.ScopeMetrics().AppendEmpty()
		dest.sm.Scope().SetName(scopeName)
		dest.sm.Scope().SetVersion(obs.ScopeVersion)
		b.scopes[key] = dest
		// One resource per described object: its attributes are a real part of
		// the encoded batch, not rounding error (see otlppoint.go).
		b.bytes += resourceBytes(rm.Resource(), scopeName)
	}
	b.lastVals = append(b.lastVals[:0], b.vals...)
	b.lastRule, b.lastDest, b.lastRouteOK = rule, dest, true
	return dest, rule, false
}

// fillSelfResource builds the target's own resource, as the plain batcher
// would.
func (b *splitBatcher) fillSelfResource(res pcommon.Resource) {
	b.s.fillTargetResource(res, b.target.URL, &b.target.Pod, b.target.Service)
}

// fillSplitResource builds the resource for one identified object from vals,
// the per-attribute effective groupBy values route computed for its key — one
// per rule.slotAttrs entry, already coalesced and normalized there.
func (b *splitBatcher) fillSplitResource(res pcommon.Resource, rule *compiledSplitRule, vals []string) {
	// The identity handed to enrichment is read off the same vector as the
	// attributes rendered below and the route key. It used to re-derive the
	// coalesce here, assigning unconditionally, so a LATER EMPTY label of a
	// coalescing rule blanked an earlier non-empty one and the row resolved (or
	// failed to) as an object other than the one its resource named.
	var namespace, pod, uid, container, containerID string
	for slot, attr := range rule.slotAttrs {
		switch attr {
		case "k8s.namespace.name":
			namespace = vals[slot]
		case "k8s.pod.name":
			pod = vals[slot]
		case "k8s.pod.uid":
			uid = vals[slot]
		case "k8s.container.name":
			container = vals[slot]
		case "container.id":
			containerID = vals[slot]
		}
	}

	var ctx attrs.Context
	resolved := false
	if rule.enrich {
		// The failure classification is the cgroup sampler's alone (it decides a
		// retry cadence); a split row is emitted with its label identity either
		// way.
		ctx, resolved, _ = b.s.resolveContext(b.ctx, containerID, namespace, pod, uid, container)
	}
	// The groupBy labels move onto the resource under their mapped attribute
	// names — ALWAYS, not only when enrichment failed. putSplitLabels strips
	// every groupBy label from the data points unconditionally, so writing them
	// here only in the !resolved branch DESTROYED any groupBy attribute outside
	// the fixed set attrs.Build produces: a rule grouping by, say, `resource`
	// (cpu/memory/ephemeral-storage) collapsed those rows onto one identical
	// resource, leaving indistinguishable duplicate points of which only one
	// survives downstream — the whole requests/limits breakdown, gone silently.
	// It also made a series' resource shape change during a metadata outage.
	//
	// When enrichment DID resolve, the looked-up identity is authoritative:
	// Build below OVERWRITES every attribute the resolved pod and container
	// name, so a label-derived value survives only where the resolve had
	// nothing to say — which is also how a pod whose document does not name the
	// container keeps k8s.container.name (resolveContext writes nothing on a
	// resource). One value per SLOT, each slot a DISTINCT attribute: this loop
	// used to render per LABEL and skip an attribute already present, which
	// could not tell a key the resolve had written from one the loop itself had
	// written an iteration earlier, so a coalescing groupBy — two labels mapped
	// onto one attribute — rendered FIRST-non-empty-wins while the route key was
	// LAST-non-empty-wins: two rows differing only in the later label keyed as
	// two resources and rendered byte-identical ones, the duplicate-resource
	// class the slot vector exists to prevent.
	for slot, attr := range rule.slotAttrs {
		if v := vals[slot]; v != "" {
			res.Attributes().PutStr(attr, v)
		}
	}
	b.s.attrsFor(pipelineTargets).Build(res, ctx)
	// Distinguish this described object's instance from its own self-scraped
	// metrics (same service.name/namespace) — default prefix is the describing
	// target's service.name, matching cmb-alloy's instance_prefix.
	prefix := b.defaultPrefix
	if rule.instancePrefix != nil {
		prefix = *rule.instancePrefix
	}
	attrs.PrefixInstance(res, prefix)
	// Rule fallbacks fill only what groupBy/enrichment left unset (e.g. a
	// service.name for resources derived purely from labels).
	for k, v := range rule.attributes {
		if _, ok := res.Attributes().Get(k); !ok {
			res.Attributes().PutStr(k, v)
		}
	}
	// A rule that ASKED for enrichment and did not get it must still name a
	// service. service.name is otherwise produced only as a side effect of
	// attrs.Pod running inside Build, which needs the resolved pod — so a 404, a
	// same-name pod failing the uid cross-check, or one unreachable metadata
	// service (negative-cached for a minute) left the resource with no
	// service.name at all, and the OTLP→Prometheus translation gives it no `job`
	// label. The SAME series therefore appeared and disappeared as rows resolved
	// and stopped resolving — unselectable by any job-scoped alert while it was
	// gone — rather than merely changing a label value. attrs.ServiceName's
	// documented fallback chain ends at the pod name, which is what the groupBy
	// already put on the resource; the cadvisor batcher makes exactly this
	// compensation for exactly this reason. It runs AFTER the rule's own
	// fallbacks so an explicit `attributes: {service.name: …}` still wins, and
	// only for enrich rules — a label-only rule never had a resolution to lose,
	// and minting a job label for it would change series identity for a shape
	// that never flapped.
	if rule.enrich && !resolved {
		a := res.Attributes()
		if _, ok := a.Get("service.name"); !ok {
			if v, ok := a.Get("k8s.pod.name"); ok && v.Str() != "" {
				a.PutStr("service.name", v.Str())
			}
		}
	}
	// These land AFTER Build, so the operator's enable/disable lists have not
	// seen them: re-apply the global filter, the way the tailer does for its
	// post-Build source statics. The filter is global by design, and a rule
	// fallback is exactly the kind of attribute a `disable` list is written to
	// drop. Nil-safe.
	b.s.attrsFor(pipelineTargets).FilterResource(res)
}

// putSplitLabels writes the non-grouped labels onto a data point (minus the
// rule's dropLabels), plus the attributes moved off the split resource (dp).
func (b *splitBatcher) putSplitLabels(attrsMap pcommon.Map, rule *compiledSplitRule, labels []Label, dp []Label) {
	for _, l := range labels {
		grouped := false
		if rule != nil {
			if rule.dropLabels != nil && b.dropped(rule, l.Name) {
				continue
			}
			for _, g := range rule.groupBy {
				if g.label == l.Name {
					grouped = true
					break
				}
			}
		}
		if !grouped {
			attrsMap.PutStr(l.Name, l.Value)
		}
	}
	for _, a := range dp {
		attrsMap.PutStr(a.Name, a.Value)
	}
}

// dropped memoizes rule.dropLabels.MatchString(name) per (rule, label name).
func (b *splitBatcher) dropped(rule *compiledSplitRule, name string) bool {
	key := dropKey{rule: rule, name: name}
	verdict, ok := b.dropMemo[key]
	if !ok {
		verdict = rule.dropLabels.MatchString(name)
		if b.budget.admit(len(b.dropMemo), maxInternedValues, len(name)) {
			b.dropMemo[key] = verdict
		}
	}
	return verdict
}

func (b *splitBatcher) metric(name string, meta metricMeta, labels []Label, shape func(pmetric.Metric)) (pmetric.Metric, *compiledSplitRule, []Label) {
	dest, rule, sameResource := b.route(name, labels)
	// Last-seen fast path: consecutive samples of the same object and family
	// skip the key building and the map probe entirely. route reports the
	// resource half of that (it holds the memo), so nothing here has to compare
	// — or even materialize — a resource key string.
	if b.lastMOK && sameResource && name == b.lastMName {
		return b.lastM, rule, dest.dp
	}
	b.keyBuf = b.appendRouteKey(b.keyBuf[:0], rule)
	b.keyBuf = append(b.keyBuf, 0)
	b.keyBuf = append(b.keyBuf, name...)
	m, ok := b.byKey[string(b.keyBuf)] // no alloc: map read elides the copy
	if !ok {
		key := string(b.keyBuf) // materialize once per new metric per batch
		m = dest.sm.Metrics().AppendEmpty()
		m.SetName(name)
		shape(m)
		b.byKey[key] = m
		// One descriptor per resource — including its description and unit,
		// which a split batch repeats for every described object.
		b.bytes += chargeDescriptor(m, name, meta)
	}
	b.lastMName, b.lastM, b.lastMOK = name, m, true
	return m, rule, dest.dp
}

func (b *splitBatcher) addNumber(s Sample, monotonic bool) {
	m, rule, dpa := b.metric(s.Name, sampleMeta(s), s.Labels, func(m pmetric.Metric) {
		shapeNumber(m, monotonic)
	})

	dp, ok := numberDataPoint(m, b.startTS)
	if !ok {
		return
	}
	dp.SetDoubleValue(s.Value)
	dp.SetTimestamp(pointTS(s.TimestampMs, b.scrapeTS))
	b.putSplitLabels(dp.Attributes(), rule, s.Labels, dpa)
	if s.Exemplar != nil {
		setExemplar(dp.Exemplars().AppendEmpty(), *s.Exemplar, b.scrapeTS)
	}
	b.points++
	b.bytes += numberBytes(s) + labelBytes(dpa)
}

func (b *splitBatcher) addHistogram(family string, acc *histAcc) {
	m, rule, dpa := b.metric(family, acc.meta, acc.labels, shapeHistogram)
	dp, ok := histogramDataPoint(m, b.startTS)
	if !ok {
		return
	}
	dp.SetTimestamp(pointTS(acc.ts, b.scrapeTS))
	fillHistogramPoint(dp, acc)
	b.putSplitLabels(dp.Attributes(), rule, acc.labels, dpa)
	for _, e := range acc.exemplars {
		setExemplar(dp.Exemplars().AppendEmpty(), e, b.scrapeTS)
	}
	b.points++
	b.bytes += histBytes(acc) + labelBytes(dpa)
}

// addExponential routes one native-histogram point exactly like the other
// kinds: the rule's groupBy labels move onto the (possibly enriched) split
// resource, the rest stay on the data point, and the byte estimate is charged
// with expHistBytes. This is what lets splitter-backed targets accept the
// protobuf exposition instead of being pinned to text.
func (b *splitBatcher) addExponential(family string, p expPoint) {
	m, rule, dpa := b.metric(family, p.meta, p.labels, shapeExponentialHistogram)
	dp, ok := exponentialDataPoint(m, b.startTS)
	if !ok {
		return
	}
	dp.SetTimestamp(pointTS(p.ts, b.scrapeTS))
	fillExponentialPoint(dp, p)
	b.putSplitLabels(dp.Attributes(), rule, p.labels, dpa)
	for _, e := range p.exemplars {
		setExemplar(dp.Exemplars().AppendEmpty(), e, b.scrapeTS)
	}
	b.points++
	b.bytes += expHistBytes(&p) + labelBytes(dpa)
}

func (b *splitBatcher) addSummary(family string, acc *summAcc) {
	m, rule, dpa := b.metric(family, acc.meta, acc.labels, shapeSummary)
	dp, ok := summaryDataPoint(m, b.startTS)
	if !ok {
		return
	}
	dp.SetTimestamp(pointTS(acc.ts, b.scrapeTS))
	fillSummaryPoint(dp, acc)
	b.putSplitLabels(dp.Attributes(), rule, acc.labels, dpa)
	b.points++
	b.bytes += summBytes(acc) + labelBytes(dpa)
}
