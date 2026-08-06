package promscrape

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// SplitterConfig re-attributes series of exposition-style targets whose
// samples describe OTHER objects — kube-state-metrics being the archetype:
// its series carry pod identity in labels (namespace/pod/uid/container), so
// they must be split into one OTLP resource per identified object instead of
// landing under the kube-state-metrics pod. Declared in the metrics config
// file:
//
//	splitters:
//	  - match:
//	      podLabels:
//	        app.kubernetes.io/name: kube-state-metrics
//	    rules:
//	      - metrics: 'kube_pod_.+'
//	        groupBy:
//	          namespace: k8s.namespace.name
//	          pod: k8s.pod.name
//	          uid: k8s.pod.uid
//	          container: k8s.container.name
//	        enrich: true
//	      - metrics: 'kube_.+_labels'
//	        groupBy:
//	          namespace: k8s.namespace.name
//
// Rules are evaluated in order per series (first metrics match wins); series
// matching no rule stay on the target's own resource. The groupBy labels move
// into the resource attributes under the mapped names; the remaining labels
// stay on the data points. datapointAttributes (default ["k8s.node.name"])
// lists resource attributes to emit on the data points instead of the resource
// — a described object's node is a property of the object, not the exporter's
// identity, so it must not become part of the resource / target_info; set it to
// [] to keep everything on the resource, or list more attributes to demote.
// With enrich, the resource is resolved through the metadata service (by
// container.id when mapped, else by k8s.namespace.name + k8s.pod.name,
// cross-checked against a mapped k8s.pod.uid) and carries the full metadata
// set; otherwise (or when resolution fails) the mapped label values are used
// as-is.
type SplitterConfig struct {
	Match SplitterMatch `json:"match"`
	Rules []SplitRule   `json:"rules"`
}

// SplitterMatch selects which scrape targets a splitter applies to. All set
// fields must match.
type SplitterMatch struct {
	// Namespace is an anchored regex on the target pod's namespace.
	Namespace string `json:"namespace,omitempty"`
	// PodName is an anchored regex on the target pod's name.
	PodName string `json:"podName,omitempty"`
	// PodLabels are exact-equality matchers on the target pod's labels.
	PodLabels map[string]string `json:"podLabels,omitempty"`
}

// SplitRule maps one family group onto per-object resources.
type SplitRule struct {
	// Metrics is an anchored regex on the series name; empty matches any.
	Metrics string `json:"metrics,omitempty"`
	// GroupBy maps series label names to resource attribute names.
	GroupBy map[string]string `json:"groupBy"`
	// DatapointAttributes lists resource attributes to emit on the data points
	// instead of the resource — a described object's node, for example, is a
	// property of the object, not the exporter's identity. nil defaults to
	// ["k8s.node.name"]; an explicit list (including []) overrides it.
	DatapointAttributes *[]string `json:"datapointAttributes,omitempty"`
	// InstancePrefix is prepended to each split resource's service.instance.id
	// (see attrs.PrefixInstance) so a described object's series don't collide
	// with its own self-scraped metrics. nil defaults to the describing
	// target's service.name (e.g. "kube-state-metrics"); "" disables it.
	// Applied after attrs.Builder.Build, so it layers on top of (rather than
	// replaces) a resourceAttributes instancePrefix the pipeline already
	// applied: "<ruleprefix>-<pipelineprefix>-<instance>".
	InstancePrefix *string `json:"instancePrefix,omitempty"`
	// DropLabels is an anchored regex on series label names; matching labels
	// are omitted from the data points (e.g. 'label_.+' to strip the object's
	// Kubernetes labels off kube_.+_labels series once grouped).
	DropLabels string `json:"dropLabels,omitempty"`
	// Attributes are set on the split resource only where absent — fallbacks
	// for what neither groupBy nor enrichment provided (e.g. a service.name
	// for label-derived resources).
	Attributes map[string]string `json:"attributes,omitempty"`
	// Enrich resolves the identified pod/container through the metadata
	// service.
	Enrich bool `json:"enrich,omitempty"`
}

// Splitter is a compiled SplitterConfig.
type Splitter struct {
	matchNS   *regexp.Regexp
	matchName *regexp.Regexp
	podLabels map[string]string
	rules     []compiledSplitRule
}

type compiledSplitRule struct {
	metrics *regexp.Regexp // nil matches any
	// keyPrefix stamps the rule's identity into the split resource key: two
	// rules with equal-cardinality groupBy sets must not merge objects whose
	// label VALUES collide (kube_pod_info{pod="x"} vs kube_node_info{node="x"}).
	keyPrefix      string
	groupBy        []groupMapping    // sorted by label for deterministic keys
	datapointAttr  []string          // resource attrs moved onto the data points
	instancePrefix *string           // nil = default to the target's service.name
	dropLabels     *regexp.Regexp    // data-point labels to omit; nil keeps all
	attributes     map[string]string // set-if-absent resource attributes
	enrich         bool
}

// defaultDatapointAttrs are moved from a split resource onto its data points
// when a rule does not specify DatapointAttributes.
var defaultDatapointAttrs = []string{"k8s.node.name"}

type groupMapping struct {
	label, attr string
}

// NewSplitters compiles splitter configs.
func NewSplitters(cfgs []SplitterConfig) ([]*Splitter, error) {
	var out []*Splitter
	for i, cfg := range cfgs {
		sp := &Splitter{podLabels: cfg.Match.PodLabels}
		var err error
		if cfg.Match.Namespace != "" {
			if sp.matchNS, err = regexp.Compile("^(?:" + cfg.Match.Namespace + ")$"); err != nil {
				return nil, fmt.Errorf("splitter %d namespace: %w", i, err)
			}
		}
		if cfg.Match.PodName != "" {
			if sp.matchName, err = regexp.Compile("^(?:" + cfg.Match.PodName + ")$"); err != nil {
				return nil, fmt.Errorf("splitter %d podName: %w", i, err)
			}
		}
		if sp.matchNS == nil && sp.matchName == nil && len(sp.podLabels) == 0 {
			return nil, fmt.Errorf("splitter %d: empty match would apply to every target", i)
		}
		if len(cfg.Rules) == 0 {
			return nil, fmt.Errorf("splitter %d: no rules", i)
		}
		for j, r := range cfg.Rules {
			cr := compiledSplitRule{keyPrefix: strconv.Itoa(j) + "\x00"}
			if r.Metrics != "" {
				if cr.metrics, err = regexp.Compile("^(?:" + r.Metrics + ")$"); err != nil {
					return nil, fmt.Errorf("splitter %d rule %d metrics: %w", i, j, err)
				}
			}
			if len(r.GroupBy) == 0 {
				return nil, fmt.Errorf("splitter %d rule %d: empty groupBy", i, j)
			}
			labels := make([]string, 0, len(r.GroupBy))
			for label := range r.GroupBy {
				labels = append(labels, label)
			}
			sort.Strings(labels)
			for _, label := range labels {
				cr.groupBy = append(cr.groupBy, groupMapping{label: label, attr: r.GroupBy[label]})
			}
			cr.datapointAttr = defaultDatapointAttrs
			if r.DatapointAttributes != nil {
				cr.datapointAttr = *r.DatapointAttributes
			}
			cr.instancePrefix = r.InstancePrefix
			if r.DropLabels != "" {
				if cr.dropLabels, err = regexp.Compile("^(?:" + r.DropLabels + ")$"); err != nil {
					return nil, fmt.Errorf("splitter %d rule %d dropLabels: %w", i, j, err)
				}
			}
			cr.attributes = r.Attributes
			cr.enrich = r.Enrich
			sp.rules = append(sp.rules, cr)
		}
		out = append(out, sp)
	}
	return out, nil
}

// matches reports whether the splitter applies to a scrape target.
func (sp *Splitter) matches(pod kubemeta.Pod) bool {
	if sp.matchNS != nil && !sp.matchNS.MatchString(pod.Namespace) {
		return false
	}
	if sp.matchName != nil && !sp.matchName.MatchString(pod.Name) {
		return false
	}
	for k, v := range sp.podLabels {
		// Kubernetes selector semantics: the key must be PRESENT and equal. A
		// bare map index reads a MISSING key as "", so a podLabels entry with an
		// empty value matched every pod LACKING the label — the opposite of the
		// intent. Third site of the same bug; services.selects and
		// tailer.wantLabels are the other two.
		got, ok := pod.Labels[k]
		if !ok || got != v {
			return false
		}
	}
	return true
}

// ruleFor returns the first rule matching a series name, nil if none.
func (sp *Splitter) ruleFor(name string) *compiledSplitRule {
	for i := range sp.rules {
		if sp.rules[i].metrics == nil || sp.rules[i].metrics.MatchString(name) {
			return &sp.rules[i]
		}
	}
	return nil
}

// splitterFor returns the first configured splitter matching a target.
func (s *Scraper) splitterFor(pod kubemeta.Pod) *Splitter {
	for _, sp := range s.cfg.Splitters {
		if sp.matches(pod) {
			return sp
		}
	}
	return nil
}

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
	vals        []string // route scratch: current sample's groupBy values
	lastVals    []string // previous sample's groupBy values
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
	dp []kv
}

type kv struct{ key, value string }

func newSplitBatcher(s *Scraper, ctx context.Context, t kubemeta.ScrapeTarget, sp *Splitter, scrape time.Time) *splitBatcher {
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
// identity plus the groupBy values in b.vals — to b.
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
		if len(b.ruleMemo) < maxTrackedFamilies {
			b.ruleMemo[name] = rule
		}
	}
	b.vals = b.vals[:0]
	if rule != nil {
		for _, g := range rule.groupBy {
			b.vals = append(b.vals, labelValue(labels, g.label))
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
			b.fillSplitResource(rm.Resource(), rule, labels)
			// A split resource describes ANOTHER object (kube-state-metrics style);
			// the configured attributes (default k8s.node.name) are properties of
			// that object, not the exporter's identity, so move them off the resource
			// onto the data points — a queryable series label rather than part of the
			// resource / target_info.
			for _, attr := range rule.datapointAttr {
				if v, ok := rm.Resource().Attributes().Get(attr); ok {
					dest.dp = append(dest.dp, kv{attr, v.AsString()})
					rm.Resource().Attributes().Remove(attr)
				}
			}
		}
		dest.sm = rm.ScopeMetrics().AppendEmpty()
		dest.sm.Scope().SetName(scopeName)
		dest.sm.Scope().SetVersion(obs.ScopeVersion)
		b.scopes[key] = dest
		// One resource per described object: its attributes are a real part of
		// the encoded batch, not rounding error (see convert.go).
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

// fillSplitResource builds the resource for one identified object.
func (b *splitBatcher) fillSplitResource(res pcommon.Resource, rule *compiledSplitRule, labels []Label) {
	var namespace, pod, uid, container, containerID string
	for _, g := range rule.groupBy {
		switch g.attr {
		case "k8s.namespace.name":
			namespace = labelValue(labels, g.label)
		case "k8s.pod.name":
			pod = labelValue(labels, g.label)
		case "k8s.pod.uid":
			uid = labelValue(labels, g.label)
		case "k8s.container.name":
			container = labelValue(labels, g.label)
		case "container.id":
			containerID = kubemeta.NormalizeContainerID(labelValue(labels, g.label))
		}
	}

	var ctx attrs.Context
	resolved := false
	if rule.enrich {
		ctx, resolved = b.s.resolveContext(b.ctx, containerID, namespace, pod, uid, container, res)
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
	// When enrichment DID resolve, the looked-up identity is authoritative, so
	// the label-derived value only fills what the resolve left unset.
	for _, g := range rule.groupBy {
		value := labelValue(labels, g.label)
		if g.attr == "container.id" {
			value = kubemeta.NormalizeContainerID(value)
		}
		if value == "" {
			continue
		}
		if resolved {
			if _, exists := res.Attributes().Get(g.attr); exists {
				continue
			}
		}
		res.Attributes().PutStr(g.attr, value)
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
	// These land AFTER Build, so the operator's enable/disable lists have not
	// seen them: re-apply the global filter, the way the tailer does for its
	// post-Build source statics. The filter is global by design, and a rule
	// fallback is exactly the kind of attribute a `disable` list is written to
	// drop. Nil-safe.
	b.s.attrsFor(pipelineTargets).FilterResource(res)
}

func labelValue(labels []Label, name string) string {
	for _, l := range labels {
		if l.Name == name {
			return l.Value
		}
	}
	return ""
}

// dpaBytes estimates the encoded size of the attributes moved off a split
// resource onto EVERY one of its data points.
//
// route() charges the resource AFTER removing them and the per-point
// estimators only measure the sample's own labels, so #attrs × #points bytes
// were encoded and counted nowhere. The default config hid it: the same
// estimate over-charges the groupBy and dropLabels labels it strips, and the
// two happened to cancel. A rule demoting several attributes while grouping by
// one under-counts by ~1 MB at BatchPoints=10000 — enough to push a 3 MiB
// estimate past the 4 MiB gRPC limit the estimate exists to respect, at which
// point the collector rejects every export of that target.
func dpaBytes(dpa []kv) int {
	n := 0
	for _, a := range dpa {
		n += len(a.key) + len(a.value) + attrOverheadBytes
	}
	return n
}

// putSplitLabels writes the non-grouped labels onto a data point (minus the
// rule's dropLabels), plus the attributes moved off the split resource (dp).
func (b *splitBatcher) putSplitLabels(attrsMap pcommon.Map, rule *compiledSplitRule, labels []Label, dp []kv) {
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
		attrsMap.PutStr(a.key, a.value)
	}
}

// dropped memoizes rule.dropLabels.MatchString(name) per (rule, label name).
func (b *splitBatcher) dropped(rule *compiledSplitRule, name string) bool {
	key := dropKey{rule: rule, name: name}
	verdict, ok := b.dropMemo[key]
	if !ok {
		verdict = rule.dropLabels.MatchString(name)
		if len(b.dropMemo) < maxInternedValues {
			b.dropMemo[key] = verdict
		}
	}
	return verdict
}

func (b *splitBatcher) metric(name string, meta metricMeta, labels []Label, shape func(pmetric.Metric)) (pmetric.Metric, *compiledSplitRule, []kv) {
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
	b.bytes += numberBytes(s) + dpaBytes(dpa)
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
	b.bytes += histBytes(acc) + dpaBytes(dpa)
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
	b.bytes += expHistBytes(&p) + dpaBytes(dpa)
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
	b.bytes += summBytes(acc) + dpaBytes(dpa)
}
