package servicemonitors

// The monitor kinds' shared record (monitorBase), the ServiceMonitor kind, the
// spec shapes, and the ONE parse skeleton both kinds go through, with the
// monitor-level ceilings (endpoint list, selectors) it enforces.

import (
	"encoding/json"
	"fmt"
	"maps"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GVR is the ServiceMonitor resource.
var GVR = schema.GroupVersionResource{
	Group:    "monitoring.coreos.com",
	Version:  "v1",
	Resource: "servicemonitors",
}

// Monitor is one parsed ServiceMonitor: the shared monitorBase record, whose
// Selector selects SERVICES by their labels and whose endpoints' Port names a
// Service port.
type Monitor struct{ monitorBase }

// ServiceNamespaces returns the namespaces the monitor selects Services in;
// nil means all.
func (m *Monitor) ServiceNamespaces() []string { return m.namespaces() }

// namespaceSelector is the CRD's namespaceSelector clause, shared verbatim by
// the ServiceMonitor and PodMonitor specs (it was an anonymous struct in each).
type namespaceSelector struct {
	Any        bool     `json:"any"`
	MatchNames []string `json:"matchNames"`
}

// resolve returns the namespaces the selector covers for a monitor living in
// ownNS: nil for "all namespaces", the explicit matchNames when given, else
// the monitor's own namespace (the CRD default). ServiceNamespaces and
// PodNamespaces both answer through this (monitorBase.namespaces) — the rule
// existed twice, verbatim.
func (s namespaceSelector) resolve(ownNS string) []string {
	if s.Any {
		return nil
	}
	if len(s.MatchNames) > 0 {
		return s.MatchNames
	}
	return []string{ownNS}
}

// specLimits are the monitor-LEVEL guard rails, shared by ServiceMonitor and
// PodMonitor. They are parsed only to be REPORTED as uninterpreted: they were
// dropped at parse time, so a user who set sampleLimit specifically to fence
// off a cardinality bomb got no protection and no warning — and sampleLimit is
// the very example IgnoredFields' own doc comment cites.
type specLimits struct {
	SampleLimit           *uint64  `json:"sampleLimit"`
	TargetLimit           *uint64  `json:"targetLimit"`
	LabelLimit            *uint64  `json:"labelLimit"`
	LabelNameLengthLimit  *uint64  `json:"labelNameLengthLimit"`
	LabelValueLengthLimit *uint64  `json:"labelValueLengthLimit"`
	KeepDroppedTargets    *uint64  `json:"keepDroppedTargets"`
	JobLabel              string   `json:"jobLabel"`
	TargetLabels          []string `json:"targetLabels"`
	PodTargetLabels       []string `json:"podTargetLabels"`
	// Set but not interpreted, and previously not even PARSED — so they
	// produced no warning and no MonitorFieldsIgnored bump, breaching the
	// no-silent-partial-application contract the Ignored machinery exists for.
	//
	// filterRunning is NOT here: the CRD puts it on the ENDPOINT (on both
	// kinds), so it lives on endpointSpec. It sat in this struct for a while,
	// where the API server's pruning of unknown spec properties meant it never
	// arrived.
	BodySizeLimit  string          `json:"bodySizeLimit"`
	AttachMetadata *map[string]any `json:"attachMetadata"`
	ScrapeClass    string          `json:"scrapeClass"`
	// The exposition-format and native-histogram clauses, parsed for the same
	// reason as the guard rails above them.
	//
	// scrapeProtocols/fallbackScrapeProtocol are the loudest of these: the agent
	// negotiates its own Accept header (OpenMetrics when exemplars are on,
	// protobuf when native histograms are), so an operator who pinned
	// PrometheusText0.0.4 because a target's OpenMetrics output is broken gets
	// the opposite of what the CR says. The nativeHistogram* pair sits in the
	// CRD beside sampleLimit/targetLimit/labelLimit and guards the same thing —
	// cardinality — and all three of those ARE reported.
	ScrapeProtocols                []string        `json:"scrapeProtocols"`
	FallbackScrapeProtocol         string          `json:"fallbackScrapeProtocol"`
	SelectorMechanism              string          `json:"selectorMechanism"`
	NativeHistogramBucketLimit     *uint64         `json:"nativeHistogramBucketLimit"`
	NativeHistogramMinBucketFactor json.RawMessage `json:"nativeHistogramMinBucketFactor"`
	ConvertClassicHistogramsToNHCB *bool           `json:"convertClassicHistogramsToNHCB"`
	ScrapeClassicHistograms        *bool           `json:"scrapeClassicHistograms"`
}

// ignored lists the monitor-level fields that are set but not interpreted.
func (s specLimits) ignored() []string {
	var out []string
	add := func(name string, set bool) {
		if set {
			out = append(out, name)
		}
	}
	add("sampleLimit", s.SampleLimit != nil)
	add("targetLimit", s.TargetLimit != nil)
	add("labelLimit", s.LabelLimit != nil)
	add("labelNameLengthLimit", s.LabelNameLengthLimit != nil)
	add("labelValueLengthLimit", s.LabelValueLengthLimit != nil)
	add("keepDroppedTargets", s.KeepDroppedTargets != nil)
	add("jobLabel", s.JobLabel != "")
	add("targetLabels", len(s.TargetLabels) > 0)
	add("podTargetLabels", len(s.PodTargetLabels) > 0)
	add("bodySizeLimit", s.BodySizeLimit != "")
	add("attachMetadata", s.AttachMetadata != nil)
	add("scrapeClass", s.ScrapeClass != "")
	add("scrapeProtocols", len(s.ScrapeProtocols) > 0)
	add("fallbackScrapeProtocol", s.FallbackScrapeProtocol != "")
	add("selectorMechanism", s.SelectorMechanism != "")
	add("nativeHistogramBucketLimit", s.NativeHistogramBucketLimit != nil)
	add("nativeHistogramMinBucketFactor", len(s.NativeHistogramMinBucketFactor) > 0)
	add("convertClassicHistogramsToNHCB", s.ConvertClassicHistogramsToNHCB != nil)
	add("scrapeClassicHistograms", s.ScrapeClassicHistograms != nil)
	return out
}

// smSpec mirrors the ServiceMonitor spec fields we interpret.
type smSpec struct {
	Selector          metav1.LabelSelector `json:"selector"`
	NamespaceSelector namespaceSelector    `json:"namespaceSelector"`
	Endpoints         []endpointSpec       `json:"endpoints"`
	specLimits        `json:",inline"`
}

func (s *smSpec) labelSelector() *metav1.LabelSelector { return &s.Selector }
func (s *smSpec) nsSelector() namespaceSelector        { return s.NamespaceSelector }
func (s *smSpec) endpointSpecs() []endpointSpec        { return s.Endpoints }
func (s *smSpec) endpointsField() string               { return "endpoints" }
func (s *smSpec) monitorIgnored() []string             { return s.ignored() }

// monitorSpec is what the shared parse skeleton needs from a kind's decoded
// spec shape: its label selector, its namespace selector, its endpoint list
// (the two kinds spell the JSON key differently, hence endpointsField beside
// it) and its monitor-level ignored-fields report. A new monitor arm
// implements these five accessors and gets the WHOLE skeleton — including the
// per-endpoint security step in parseMonitorSpec — for free.
type monitorSpec interface {
	labelSelector() *metav1.LabelSelector
	nsSelector() namespaceSelector
	endpointSpecs() []endpointSpec
	// endpointsField is the CRD's OWN name for that list, used only to report
	// the maxEndpointsPerMonitor refusal. The two kinds spell it differently
	// and the report names a field an operator will go and edit, so it is the
	// kind's word for it and not a shared approximation.
	endpointsField() string
	monitorIgnored() []string
}

// monitorBase is the record a parse produces, and the WHOLE of both kinds:
// Monitor and PodMonitor embed it and add only their doc and the name of their
// namespace accessor. One struct and no copy — the two parsers used to build
// this and then copy it field by field into each kind, so a field added here
// had to be remembered in both copies, and a resourceVersion missed in one is
// the resync-invalidates-the-memo defect upsertMonitor documents.
//
// The exported fields are promoted, so m.Namespace, m.Selector and
// m.Endpoints read as before on either kind; the unexported ones are not part
// of either kind's API.
type monitorBase struct {
	Namespace string
	Name      string
	// Selector selects Services (ServiceMonitor) or PODS (PodMonitor) by label.
	Selector labels.Selector
	// Endpoints are the monitor's endpoint declarations. An endpoint's Port
	// names a Service port (ServiceMonitor) or a CONTAINER port (PodMonitor).
	Endpoints []Endpoint
	// nsSel is the CRD's namespaceSelector clause; namespaces resolves it.
	nsSel namespaceSelector
	// resourceVersion is the object this record was parsed from, carried so
	// the index can tell a re-delivery that changes nothing from a real update
	// (see upsertMonitor). Not part of the model and never served.
	resourceVersion string
}

// version reports the resourceVersion a record was parsed from. It is the
// constraint upsertMonitor takes, and both kinds satisfy it through the
// embedding — so a kind cannot exist without it, the alternative being a kind
// whose re-deliveries silently invalidate the server's memo again.
func (b *monitorBase) version() string { return b.resourceVersion }

// namespaces returns the namespaces the monitor selects in; nil means all.
// ServiceNamespaces and PodNamespaces are its per-kind names.
func (b *monitorBase) namespaces() []string { return b.nsSel.resolve(b.Namespace) }

// key is the (namespace, name) pair sortedMonitors orders by.
func (b *monitorBase) key() (namespace, name string) { return b.Namespace, b.Name }

// maxEndpointsPerMonitor bounds the ENDPOINT LIST itself — the dimension every
// other per-endpoint ceiling in this package was measured underneath (its two
// SELECTORS are the other unbounded dimension, bounded by maxSelectorTerms and
// its siblings below). Each endpoint STRING is bounded (enforceFieldBounds),
// each relabel chain is bounded (maxRelabelRules/maxRelabelChainBytes), each
// per-endpoint and per-monitor report is bounded (maxRelabelIgnored,
// maxIgnoredFields) and the merged target's contributor list is bounded one
// layer up (scrape.MaxContributorsPerTarget) — and a bound on what ONE entry
// costs is not a bound on the list, which is this package's own lesson ("a
// bound on ENTRIES is not a bound on BYTES") read in the other direction.
//
// The cost is paid in three places, none of them the served document — which is
// why nothing downstream noticed. Measured through the real code (go1.26, 13th
// Gen i7-1360P) with a tenant-authored `selector: {}` + `namespaceSelector.any:
// true` monitor, the shape the default -monitor-namespaces honours:
//
//   - RETAINED, for the life of the CR: 100,000 endpoints of `{"port":"a"}`
//     marshal to 1,300,015 bytes — inside etcd's object limit — and parse into
//     37.9 MB of Endpoint records held by the index, in a singleton the chart
//     requests 128Mi for with no limit.
//   - MULTIPLIED, in the server's monitor→services memo, which holds one entry
//     per (monitor, matched Service, endpoint): 10,000 endpoints over 100
//     matched Services measured 1,000,000 entries / 26.1 MB / 65.96 ms per
//     rebuild, under the memo's lock, triggered by ANY Service or monitor
//     change and with the old and new map both alive across it.
//   - WALKED, per pod per matched Service on every node-targets derivation:
//     ONE 110-pod node with 10,000 endpoints over 10 Services took 1.69 s, per
//     node per poll whenever that node's change-token memo lapses — which any
//     pod upsert cluster-wide does. The RESPONSE stays small (the merge,
//     contributor and byte ceilings bound it), so the only symptoms are the
//     singleton's RSS and every agent's targets poll timing out: a cluster-wide
//     loss of scrape-target discovery from an input any namespace-scoped tenant
//     can write.
//
// The PREFIX is kept and the tail refused, like the aggregate relabel ceilings
// and for the same reason: rejecting the CR would take every target its earlier
// endpoints contribute with it, which is a bigger outage than the tail is a
// risk. The refusal rides the monitor-level Ignored list, so it counts into
// kubescrape_monitor_fields_ignored_total and names itself in the per-upsert
// warning rather than being silent.
//
// 128 is far above anything legitimate — kube-prometheus-stack's largest
// monitors carry single digits, and one pod cannot hold more than
// scrape.MaxPortsPerPod (16) targets however many endpoints resolve to it — and
// far below where any of the three costs above bites.
//
// The cap is applied to the RAW list, before the typed decode
// (parseMonitorSpec), so a pathological CR costs one bounded decode rather
// than a full one per delivery: measured on a 100,000-endpoint CR, ~216 ms, 37
// MB and ~100k allocations per Parse against ~0.5 ms and ~400 allocations
// with the cut first. What this does NOT bound, stated so it is not mistaken
// for closed: the informer still caches the whole unstructured CR, and the
// JSON decode that built it — both apimachinery's, before this package sees
// the object.
const maxEndpointsPerMonitor = 128

// Ceilings on a monitor's two SELECTORS, the dimension maxEndpointsPerMonitor
// left open. Both are tenant-authored and both are walked on the hot paths:
// spec.selector is matched against every in-scope Service per monitor in the
// server's monitor→services rebuild (under its lock, on ANY Service or monitor
// change) and, for a PodMonitor, against every pod of every node-targets
// derivation; namespaceSelector.matchNames is scanned linearly per pod per
// PodMonitor. Neither is bounded upstream — the CRD schema carries no maxItems,
// and apimachinery validates each key and value but not the COUNT — so one CR
// could carry ~25,000 `DoesNotExist` requirements (measured at 744 µs per
// Matches call, since every requirement matches and the whole list is walked:
// ~82 ms per 110-pod node derivation per such PodMonitor, ~1.5 s per rebuild at
// 2,000 in-scope Services) or a 150,000-entry matchNames list retained whole.
//
// Over any ceiling the MONITOR is refused — a parse error, counted in
// kubescrape_monitor_parse_errors_total and kubescrape_monitors_rejected, and
// removed from the index — never trimmed to a prefix like the endpoint list.
// The asymmetry is the whole point: dropping a selector REQUIREMENT widens what
// the monitor selects (the opposite of the endpoint cap's fail-safe), and a
// monitor silently selecting more than its CR says is the worse failure. One
// rule for matchNames too, although a prefix there would only narrow.
//
// Every ceiling is far above anything legitimate: kube-prometheus-stack's
// selectors carry one to three matchLabels, and a platform monitor listing
// namespaces lists tens. Label keys and values are length-validated by
// apimachinery (a value is at most 63 bytes), so the term and value COUNTS bound
// the selector's bytes; a matchNames entry is not validated at all, so it gets
// a byte bound of its own (a namespace name is a DNS-1123 label, at most 63).
const (
	maxSelectorTerms      = 64
	maxSelectorValues     = 1024
	maxSelectorNamespaces = 1024
	maxSelectorNameBytes  = 63
)

// checkSelectorBounds refuses a spec whose selectors are over the ceilings
// above. It runs BEFORE metav1.LabelSelectorAsSelector, whose conversion
// allocates, validates and sorts one requirement per term.
func checkSelectorBounds(sel *metav1.LabelSelector, nss namespaceSelector) error {
	if n := len(sel.MatchLabels) + len(sel.MatchExpressions); n > maxSelectorTerms {
		return fmt.Errorf("selector has %d matchLabels+matchExpressions terms, over the limit of %d", n, maxSelectorTerms)
	}
	values := 0
	for _, e := range sel.MatchExpressions {
		values += len(e.Values)
	}
	if values > maxSelectorValues {
		return fmt.Errorf("selector.matchExpressions carry %d values, over the limit of %d", values, maxSelectorValues)
	}
	if n := len(nss.MatchNames); n > maxSelectorNamespaces {
		return fmt.Errorf("namespaceSelector.matchNames has %d entries, over the limit of %d", n, maxSelectorNamespaces)
	}
	for _, ns := range nss.MatchNames {
		if len(ns) > maxSelectorNameBytes {
			return fmt.Errorf("namespaceSelector.matchNames has a %d-byte entry, longer than any namespace name (%d)",
				len(ns), maxSelectorNameBytes)
		}
	}
	return nil
}

// parseMonitorSpec is the ONE parse skeleton of both monitor kinds: the
// no-spec error, the unstructured decode into the kind's spec shape, the
// selector bounds (checkSelectorBounds) and conversion, the endpoint-list cap,
// and — per endpoint — the monitor-level ignored-fields
// append plus namespaceSecretRefs with the MONITOR's namespace.
//
// The last step is a security boundary, which is why it lives here rather than
// in each kind's parser: namespacing every secret ref with the monitor's own
// namespace is what confines a monitor to its own secrets and so bounds what
// /v1/scrape-auth will serve (see Endpoint.secretRefs). The two hand-written
// copies of this loop agreed; a THIRD arm that forgot the call would have
// compiled fine and shipped an unnamespaced — unmatchable, so silently
// unauthenticated — credential ref. An arm cannot skip it now without also
// rewriting the decode, the selector conversion and the error strings.
func parseMonitorSpec(u *unstructured.Unstructured, kind string, spec monitorSpec) (monitorBase, error) {
	var b monitorBase
	specRaw, ok := u.Object["spec"].(map[string]any)
	if !ok {
		return b, fmt.Errorf("%s %s/%s: no spec", kind, u.GetNamespace(), u.GetName())
	}
	// The endpoint list is cut BEFORE the typed decode, not after it: the
	// decode is where a 100,000-endpoint CR costs 37 MB and 100k allocations
	// per delivery, only for all but the first maxEndpointsPerMonitor to be
	// thrown away. The map is the INFORMER's, so it is shallow-cloned and never
	// written, and the slice is re-capped with a full slice expression because
	// apimachinery sizes its destination by cap — a plain raw[:max] still
	// allocated the whole list. A malformed element in the refused tail is
	// therefore never decoded either, which is consistent with refusing it: the
	// kept prefix is served whatever the tail holds. (A raw list that is not a
	// []any, which only a hand-built object produces, falls through to the
	// post-decode cut below.)
	field := spec.endpointsField()
	capped := false
	if raw, ok := specRaw[field].([]any); ok && len(raw) > maxEndpointsPerMonitor {
		specRaw = maps.Clone(specRaw)
		specRaw[field] = raw[:maxEndpointsPerMonitor:maxEndpointsPerMonitor]
		capped = true
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(specRaw, spec); err != nil {
		return b, fmt.Errorf("%s %s/%s: %w", kind, u.GetNamespace(), u.GetName(), err)
	}
	nss := spec.nsSelector()
	if err := checkSelectorBounds(spec.labelSelector(), nss); err != nil {
		return b, fmt.Errorf("%s %s/%s: %w", kind, u.GetNamespace(), u.GetName(), err)
	}
	sel, err := metav1.LabelSelectorAsSelector(spec.labelSelector())
	if err != nil {
		return b, fmt.Errorf("%s %s/%s selector: %w", kind, u.GetNamespace(), u.GetName(), err)
	}
	b = monitorBase{
		Namespace:       u.GetNamespace(),
		Name:            u.GetName(),
		Selector:        sel,
		nsSel:           nss,
		resourceVersion: u.GetResourceVersion(),
	}
	specIgnored := spec.monitorIgnored()
	eps := spec.endpointSpecs()
	if len(eps) > maxEndpointsPerMonitor {
		// Belt and braces behind the pre-decode cut above, for a list that did
		// not arrive as a []any.
		eps = eps[:maxEndpointsPerMonitor]
		capped = true
	}
	if capped {
		// The prefix is kept and the tail refused; see maxEndpointsPerMonitor.
		// Reported at the MONITOR level, so it rides every kept endpoint and
		// IgnoredFields dedupes it to one entry however many that is.
		specIgnored = append(specIgnored, field+cappedSuffix)
	}
	for _, ep := range eps {
		e := ep.toEndpoint()
		// Monitor-level ignored fields ride on every endpoint; IgnoredFields
		// dedupes across endpoints, so they are reported exactly once.
		e.Ignored = append(e.Ignored, specIgnored...)
		// Every secret reference is namespaced with the MONITOR's namespace: a
		// monitor may only name secrets in its own namespace, which is what
		// bounds what /v1/scrape-auth will serve. The FIELD LIST lives on
		// Endpoint (secretRefs), shared with both parsers and with
		// AuthSecretRefs — see its doc for why it must be exactly one list.
		e.namespaceSecretRefs(b.Namespace)
		b.Endpoints = append(b.Endpoints, e)
	}
	return b, nil
}

// Parse converts an unstructured ServiceMonitor.
func Parse(u *unstructured.Unstructured) (*Monitor, error) {
	b, err := parseMonitorSpec(u, "servicemonitor", &smSpec{})
	if err != nil {
		return nil, err
	}
	return &Monitor{b}, nil
}
