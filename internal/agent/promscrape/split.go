package promscrape

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"

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
// as-is. A mapped container.id the store does not know yet keeps its OWN value:
// the pod is still resolved for the owner/label enrichment, but its current
// container is a DIFFERENT incarnation and is never adopted for it.
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
	keyPrefix string
	groupBy   []groupMapping // sorted by label for deterministic keys
	// slotAttrs names each DISTINCT groupBy attribute by its slot (see
	// groupMapping.slot): its length is the length of the per-attribute value
	// vector the route key is built from and fillSplitResource renders, equal
	// to len(groupBy) unless the rule coalesces.
	slotAttrs      []string
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
	// slot indexes the rule's per-ATTRIBUTE value vector (splitBatcher.vals),
	// assigned per distinct attr in first-appearance (sorted-label) order.
	// Several groupBy labels may map to ONE attribute (a coalesce), so keying
	// the split resource on the per-LABEL value vector minted one resource per
	// SPELLING: two rows naming the same object through different labels
	// (namespace="ns1" vs exported_namespace="ns1") got distinct route keys,
	// rendered byte-identical attribute sets, and — putSplitLabels stripping
	// every groupBy label from the points — became byte-identical duplicate
	// series in one payload, the exact class the parser rejects per line. The
	// key is therefore the RENDERED identity: one effective value per
	// attribute, last non-empty label in coalesce order, computed ONCE by
	// route — and fillSplitResource renders that same vector, so the key and
	// the resource it names cannot disagree. Single-label attrs (the common
	// case) reduce to the old per-label vector.
	slot int
	// normalize applies kubemeta.NormalizeContainerID before the route key's
	// empty-skip: the rendered value is the NORMALIZED one (fillSplitResource
	// renders the vector route builds), so keying the raw label split the
	// spellings of one rendered resource ("containerd://<id>" vs "<id>").
	normalize bool
}

// NewSplitters compiles splitter configs.
func NewSplitters(cfgs []SplitterConfig) ([]*Splitter, error) {
	var out []*Splitter
	for i, cfg := range cfgs {
		sp := &Splitter{podLabels: cfg.Match.PodLabels}
		var err error
		if cfg.Match.Namespace != "" {
			if sp.matchNS, err = compileAnchored(cfg.Match.Namespace); err != nil {
				return nil, fmt.Errorf("splitter %d namespace: %w", i, err)
			}
		}
		if cfg.Match.PodName != "" {
			if sp.matchName, err = compileAnchored(cfg.Match.PodName); err != nil {
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
				if cr.metrics, err = compileAnchored(r.Metrics); err != nil {
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
			slices.Sort(labels)
			slotOf := make(map[string]int, len(labels))
			for _, label := range labels {
				attr := r.GroupBy[label]
				// An empty attribute name is a silent dimension loss, not a no-op:
				// putSplitLabels strips a grouped label from every data point keyed on
				// the LABEL name, so the label leaves the points regardless, and its
				// value is then written to the resource under the empty KEY — where
				// nothing can query it. Refused here, beside the sibling regexes, so
				// -check-config catches the typo instead of a missing dimension doing.
				if attr == "" {
					return nil, fmt.Errorf("splitter %d rule %d groupBy %q: empty attribute name", i, j, label)
				}
				slot, seen := slotOf[attr]
				if !seen {
					slot = len(slotOf)
					slotOf[attr] = slot
					cr.slotAttrs = append(cr.slotAttrs, attr)
				}
				cr.groupBy = append(cr.groupBy, groupMapping{
					label: label, attr: attr,
					slot: slot, normalize: attr == "container.id",
				})
			}
			cr.datapointAttr = defaultDatapointAttrs
			if r.DatapointAttributes != nil {
				cr.datapointAttr = *r.DatapointAttributes
			}
			cr.instancePrefix = r.InstancePrefix
			if r.DropLabels != "" {
				if cr.dropLabels, err = compileAnchored(r.DropLabels); err != nil {
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
