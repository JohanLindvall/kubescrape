package promscrape

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
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
	InstancePrefix *string `json:"instancePrefix,omitempty"`
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
	metrics        *regexp.Regexp // nil matches any
	groupBy        []groupMapping // sorted by label for deterministic keys
	datapointAttr  []string       // resource attrs moved onto the data points
	instancePrefix *string        // nil = default to the target's service.name
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
			var cr compiledSplitRule
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
		if pod.Labels[k] != v {
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
	scopes map[string]pmetric.ScopeMetrics
	byKey  map[string]pmetric.Metric
	// dpAttrs holds, per split resource key, the resource attributes moved onto
	// its data points (rule.datapointAttr).
	dpAttrs map[string][]kv
	points  int
}

type kv struct{ key, value string }

func newSplitBatcher(s *Scraper, ctx context.Context, t kubemeta.ScrapeTarget, sp *Splitter, scrape time.Time) *splitBatcher {
	b := &splitBatcher{
		s: s, ctx: ctx, target: t, sp: sp,
		defaultPrefix: attrs.ServiceName(t.Pod),
		startTS:       pcommon.NewTimestampFromTime(s.cfg.StartTime),
		scrapeTS:      pcommon.NewTimestampFromTime(scrape),
	}
	b.reset()
	return b
}

func (b *splitBatcher) reset() {
	b.md = pmetric.NewMetrics()
	b.scopes = make(map[string]pmetric.ScopeMetrics)
	b.byKey = make(map[string]pmetric.Metric)
	b.dpAttrs = make(map[string][]kv)
	b.points = 0
}

func (b *splitBatcher) take() pmetric.Metrics {
	md := b.md
	b.reset()
	return md
}

func (b *splitBatcher) count() int { return b.points }

// route returns the scope, resource key, and rule for one series (rule nil for
// the target's own resource), plus the resource attributes moved onto its data
// points (rule.datapointAttr — split resources only).
func (b *splitBatcher) route(name string, labels []Label) (pmetric.ScopeMetrics, string, *compiledSplitRule, []kv) {
	rule := b.sp.ruleFor(name)
	var key strings.Builder
	if rule == nil {
		key.WriteString("self")
	} else {
		for _, g := range rule.groupBy {
			key.WriteString(labelValue(labels, g.label))
			key.WriteByte(0)
		}
	}
	ks := key.String()
	if sm, ok := b.scopes[ks]; ok {
		return sm, ks, rule, b.dpAttrs[ks]
	}

	rm := b.md.ResourceMetrics().AppendEmpty()
	var dp []kv
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
				dp = append(dp, kv{attr, v.AsString()})
				rm.Resource().Attributes().Remove(attr)
			}
		}
		b.dpAttrs[ks] = dp
	}
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("github.com/JohanLindvall/kubescrape/agent/promscrape")
	b.scopes[ks] = sm
	return sm, ks, rule, dp
}

// fillSelfResource builds the target's own resource, as the plain batcher
// would.
func (b *splitBatcher) fillSelfResource(res pcommon.Resource) {
	res.Attributes().PutStr("url.full", b.target.URL)
	b.s.attrsFor(pipelineTargets).Build(res, attrs.Context{
		Pod: &b.target.Pod, Service: b.target.Service, Node: b.s.nodeInfo(),
	})
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

	ctx := attrs.Context{}
	resolved := false
	if rule.enrich {
		if containerID != "" {
			if md := b.s.containerMeta(b.ctx, containerID); md != nil {
				ctx.Pod, ctx.Container = &md.Pod, &md.Container
				resolved = true
			}
		}
		if !resolved && namespace != "" && pod != "" {
			if meta := b.s.podMeta(b.ctx, namespace, pod); meta != nil &&
				(uid == "" || meta.UID == uid) {
				ctx.Pod = meta
				resolved = true
				if container != "" {
					for i := range meta.Containers {
						if meta.Containers[i].Name == container {
							ctx.Container = &meta.Containers[i]
							break
						}
					}
					if ctx.Container == nil {
						res.Attributes().PutStr("k8s.container.name", container)
					}
				}
			}
		}
	}
	if !resolved {
		// Identity from the labels, under the mapped attribute names.
		for _, g := range rule.groupBy {
			value := labelValue(labels, g.label)
			if g.attr == "container.id" {
				value = kubemeta.NormalizeContainerID(value)
			}
			if value != "" {
				res.Attributes().PutStr(g.attr, value)
			}
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
}

func labelValue(labels []Label, name string) string {
	for _, l := range labels {
		if l.Name == name {
			return l.Value
		}
	}
	return ""
}

// putSplitLabels writes the non-grouped labels onto a data point, plus the
// attributes moved off the split resource (dp).
func putSplitLabels(attrsMap pcommon.Map, rule *compiledSplitRule, labels []Label, dp []kv) {
	for _, l := range labels {
		grouped := false
		if rule != nil {
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

func (b *splitBatcher) metric(name string, labels []Label, shape func(pmetric.Metric)) (pmetric.Metric, *compiledSplitRule, []kv) {
	sm, resKey, rule, dp := b.route(name, labels)
	key := resKey + "\x00" + name
	m, ok := b.byKey[key]
	if !ok {
		m = sm.Metrics().AppendEmpty()
		m.SetName(name)
		shape(m)
		b.byKey[key] = m
	}
	return m, rule, dp
}

func (b *splitBatcher) addNumber(s Sample, monotonic bool) {
	m, rule, dpa := b.metric(s.Name, s.Labels, func(m pmetric.Metric) {
		if monotonic {
			sum := m.SetEmptySum()
			sum.SetIsMonotonic(true)
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		} else {
			m.SetEmptyGauge()
		}
	})

	var dp pmetric.NumberDataPoint
	switch m.Type() {
	case pmetric.MetricTypeSum:
		dp = m.Sum().DataPoints().AppendEmpty()
		dp.SetStartTimestamp(b.startTS)
	case pmetric.MetricTypeGauge:
		dp = m.Gauge().DataPoints().AppendEmpty()
	default:
		return
	}
	dp.SetDoubleValue(s.Value)
	dp.SetTimestamp(b.pointTS(s.TimestampMs))
	putSplitLabels(dp.Attributes(), rule, s.Labels, dpa)
	if s.Exemplar != nil {
		setExemplar(dp.Exemplars().AppendEmpty(), *s.Exemplar, b.scrapeTS)
	}
	b.points++
}

func (b *splitBatcher) addHistogram(family string, acc *histAcc) {
	m, rule, dpa := b.metric(family, acc.labels, func(m pmetric.Metric) {
		m.SetEmptyHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	})
	if m.Type() != pmetric.MetricTypeHistogram {
		return
	}
	dp := m.Histogram().DataPoints().AppendEmpty()
	dp.SetStartTimestamp(b.startTS)
	dp.SetTimestamp(b.pointTS(acc.ts))
	fillHistogramPoint(dp, acc)
	putSplitLabels(dp.Attributes(), rule, acc.labels, dpa)
	for _, e := range acc.exemplars {
		setExemplar(dp.Exemplars().AppendEmpty(), e, b.scrapeTS)
	}
	b.points++
}

func (b *splitBatcher) addSummary(family string, acc *summAcc) {
	m, rule, dpa := b.metric(family, acc.labels, func(m pmetric.Metric) {
		m.SetEmptySummary()
	})
	if m.Type() != pmetric.MetricTypeSummary {
		return
	}
	dp := m.Summary().DataPoints().AppendEmpty()
	dp.SetStartTimestamp(b.startTS)
	dp.SetTimestamp(b.pointTS(acc.ts))
	fillSummaryPoint(dp, acc)
	putSplitLabels(dp.Attributes(), rule, acc.labels, dpa)
	b.points++
}

func (b *splitBatcher) pointTS(tsMs int64) pcommon.Timestamp {
	if tsMs != 0 {
		return pcommon.Timestamp(tsMs * int64(time.Millisecond))
	}
	return b.scrapeTS
}
