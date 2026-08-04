// Package servicemonitors indexes Prometheus-Operator ServiceMonitor and
// PodMonitor custom resources so their targets can be served alongside
// annotation-discovered ones. Only pod-backed Services are supported: targets
// resolve through the selected Services' pod selectors, which keeps scraping
// node-local.
//
// A documented SUBSET of the CRD is interpreted: endpoint port/targetPort/
// path/scheme, per-endpoint interval/scrapeTimeout, basicAuth, authorization,
// bearerTokenSecret, secret-backed tlsConfig (ca/cert/keySecret/serverName and
// insecureSkipVerify), and the keep/drop subset of metricRelabelings.
// Everything else parsed here exists to be REPORTED as uninterpreted through
// Endpoint.Ignored — see IgnoredFields — because a narrower implementation is
// a choice and a silently partially-applied CR is not.
//
// This file also owns Endpoint.secretRefs, which is a security boundary: it is
// the ONE list of the endpoint fields carrying secret references, and both the
// monitor-namespacing and the /v1/scrape-auth allowlist derive from it. Adding
// a secret-bearing field means adding it there.
package servicemonitors

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// GVR is the ServiceMonitor resource.
var GVR = schema.GroupVersionResource{
	Group:    "monitoring.coreos.com",
	Version:  "v1",
	Resource: "servicemonitors",
}

// Endpoint is one scrape endpoint declaration of a monitor.
type Endpoint struct {
	// Port is the Service port name (ServiceMonitor) or container port name
	// (PodMonitor).
	Port string
	// TargetPort overrides the pod port directly (number or container port
	// name); nil defers to the service port's targetPort.
	TargetPort *intstr.IntOrString
	Path       string
	Scheme     string
	// InsecureSkipVerify comes from the endpoint's tlsConfig; agents scrape
	// https targets without verifying the certificate when set.
	InsecureSkipVerify bool
	// BearerSecret references the endpoint's bearerTokenSecret as
	// "namespace/name/key" (namespace = the monitor's). Served to agents by
	// the metadata service only when -scrape-auth-secrets is enabled.
	BearerSecret string
	// MetricRelabelings holds the keep/drop subset of the endpoint's
	// metricRelabelings; other actions are ignored (documented).
	MetricRelabelings []RelabelRule
	// BasicAuthUser/Pass, AuthType/AuthCredentials and the TLS* refs carry the
	// endpoint's remaining auth material as "namespace/name/key" secret
	// references (namespace = the monitor's), resolved by agents through the
	// same -scrape-auth-secrets channel as BearerSecret. kube-prometheus-stack's
	// own control-plane monitors (etcd, scheduler, controller-manager) use
	// client certificates, and anything behind a private CA was previously
	// scrapeable only by turning verification off entirely.
	BasicAuthUser   string
	BasicAuthPass   string
	AuthType        string // "authorization.type", default Bearer
	AuthCredentials string
	TLSCA           string
	TLSCert         string
	TLSKey          string
	TLSServerName   string
	// Interval and ScrapeTimeout are the endpoint's own cadence (Go duration
	// strings, empty = the agent's -scrape-interval/-scrape-timeout). Honouring
	// them matters on migration: a kube-prometheus-stack shop routinely has
	// monitors at 10s (ingress, mesh) and 5m (expensive exporters), and
	// collapsing both onto one global interval silently coarsens the first and
	// multiplies the sample bill of the second.
	Interval      string
	ScrapeTimeout string
	// Ignored names the endpoint fields kubescrape parsed but does NOT
	// interpret. They are reported once per monitor so a partially-applied CR
	// is visible: "narrower than prometheus-operator" is a documented choice,
	// "silently does something different" is not.
	Ignored []string
}

// secretRefs returns POINTERS to every field of this endpoint that carries a
// secret reference. It is the ONE list of them, and it is a security boundary:
//
//   - Both parsers namespace these fields with the MONITOR's namespace, which
//     is what confines a monitor to secrets in its own namespace.
//   - AuthSecretRefs harvests the same fields into the allowlist
//     /v1/scrape-auth will serve, which is what keeps -scrape-auth-secrets from
//     being a general secret-read API.
//
// Those three loops used to be written out by hand — twice for namespacing
// (ServiceMonitor and PodMonitor, verbatim copies) and once for harvesting.
// They agreed, but adding an eighth secret-bearing field and updating two of
// three fails only at RUNTIME and only for the targets that use it: a ref
// namespaced but not allowlisted 404s, a ref allowlisted but not namespaced can
// never match, and either way the target scrapes unauthenticated and reports
// up=0. Returning pointers from one method makes a new field a COMPILE-VISIBLE
// omission at exactly one site.
//
// Deliberately NOT here: non-secret fields (TLSServerName, AuthType — no
// material, no allowlist entry) and tlsConfig's configMap arm, which is
// reported as ignored rather than resolved because the agent reads secret keys
// through one channel only.
func (e *Endpoint) secretRefs() []*string {
	return []*string{
		&e.BearerSecret,
		&e.BasicAuthUser,
		&e.BasicAuthPass,
		&e.AuthCredentials,
		&e.TLSCA,
		&e.TLSCert,
		&e.TLSKey,
	}
}

// namespaceSecretRefs prefixes every set secret reference with ns, turning the
// endpoint's "name/key" refs into the "namespace/name/key" form the rest of the
// system uses. Both parsers call it; nothing else may.
func (e *Endpoint) namespaceSecretRefs(ns string) {
	for _, p := range e.secretRefs() {
		if *p != "" {
			*p = ns + "/" + *p
		}
	}
}

// RelabelRule is the keep/drop subset of a Prometheus relabel_config,
// evaluated per sample against sourceLabels joined by ";" (Prometheus
// semantics; "__name__" refers to the metric name).
type RelabelRule struct {
	Action       string   `json:"action"`
	SourceLabels []string `json:"sourceLabels"`
	Regex        string   `json:"regex"`
}

// endpointSpec is the shared endpoint shape of ServiceMonitor endpoints and
// PodMonitor podMetricsEndpoints.
type endpointSpec struct {
	Port          string              `json:"port"`
	TargetPort    *intstr.IntOrString `json:"targetPort"`
	Path          string              `json:"path"`
	Scheme        string              `json:"scheme"`
	Interval      string              `json:"interval"`
	ScrapeTimeout string              `json:"scrapeTimeout"`
	TLSConfig     *struct {
		InsecureSkipVerify bool        `json:"insecureSkipVerify"`
		CA                 *secretOrCM `json:"ca"`
		Cert               *secretOrCM `json:"cert"`
		KeySecret          *secretRef  `json:"keySecret"`
		ServerName         string      `json:"serverName"`
		// Parsed only to be REPORTED as uninterpreted. These are the
		// file-path arms of prometheus-operator's TLSConfig, used by every
		// kube-prometheus-stack control-plane monitor (etcd, kube-scheduler,
		// kube-controller-manager). The agent reads credentials through the
		// service's /v1/scrape-auth channel and has no access to files on the
		// Prometheus pod, so they cannot be honoured — but leaving them
		// unparsed made an https target silently fall back to the system
		// trust store and fail every scrape with up=0 as the only signal.
		CAFile   string `json:"caFile"`
		CertFile string `json:"certFile"`
		KeyFile  string `json:"keyFile"`
	} `json:"tlsConfig"`
	BasicAuth *struct {
		Username *secretRef `json:"username"`
		Password *secretRef `json:"password"`
	} `json:"basicAuth"`
	Authorization *struct {
		Type        string     `json:"type"`
		Credentials *secretRef `json:"credentials"`
	} `json:"authorization"`
	// Parsed only to be REPORTED as uninterpreted (see Endpoint.Ignored).
	OAuth2   json.RawMessage `json:"oauth2"`
	ProxyURL string          `json:"proxyUrl"`
	// Parsed only to be REPORTED as uninterpreted.
	BearerTokenFile string `json:"bearerTokenFile"`
	// filterRunning is an ENDPOINT field (PodMetricsEndpoint), not a spec one
	// — it lived in specLimits, where it could never decode, so the branch
	// reporting it was unreachable and the one field singled out below as most
	// likely to surprise was the one silently dropped. ServiceMonitor
	// endpoints have no such field, so it simply never sets there.
	//
	// Only a FALSE value differs from what kubescrape does: scrape.Scrapeable
	// already excludes finished and terminating pods, which is filterRunning's
	// default behaviour. `filterRunning: false` asks for the OPPOSITE.
	FilterRunning            *bool           `json:"filterRunning"`
	FollowRedirects          *bool           `json:"followRedirects"`
	EnableHTTP2              *bool           `json:"enableHttp2"`
	HonorTimestamps          *bool           `json:"honorTimestamps"`
	TrackTimestampsStaleness *bool           `json:"trackTimestampsStaleness"`
	Params                   json.RawMessage `json:"params"`
	HonorLabels              *bool           `json:"honorLabels"`
	Relabelings              json.RawMessage `json:"relabelings"`
	BearerTokenSecret        *struct {
		Name string `json:"name"`
		Key  string `json:"key"`
	} `json:"bearerTokenSecret"`
	MetricRelabelings []struct {
		Action       string   `json:"action"`
		SourceLabels []string `json:"sourceLabels"`
		Regex        string   `json:"regex"`
		// Parsed to be REPORTED, and to SUPPRESS the rule rather than apply
		// it wrongly: the agent joins sourceLabels with a hardcoded ';', so
		// honouring a rule that asked for a different separator would build a
		// different string than the user's regex was written against. For a
		// keep rule that inverts the intent — it matches nothing and drops
		// everything the user meant to keep.
		Separator string `json:"separator"`
	} `json:"metricRelabelings"`
}

// secretRef is a SecretKeySelector.
type secretRef struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

// ref renders "name/key" (the namespace is prefixed by the caller); empty when
// incomplete.
func (r *secretRef) ref() string {
	if r == nil || r.Name == "" || r.Key == "" {
		return ""
	}
	return r.Name + "/" + r.Key
}

// secretOrCM is prometheus-operator's SecretOrConfigMap. Only the secret arm is
// resolvable: the agent reads secret keys through the metadata service's
// -scrape-auth-secrets channel, and adding a parallel configMap channel is not
// worth it while every CA can be stored in a secret.
type secretOrCM struct {
	Secret    *secretRef `json:"secret"`
	ConfigMap *struct {
		Name string `json:"name"`
		Key  string `json:"key"`
	} `json:"configMap"`
}

func (s *secretOrCM) ref() string {
	if s == nil {
		return ""
	}
	return s.Secret.ref()
}

// usesConfigMap reports the unsupported arm, so it is reported as ignored.
func (s *secretOrCM) usesConfigMap() bool {
	return s != nil && s.ConfigMap != nil && s.ConfigMap.Name != ""
}

// ignoredFields lists the endpoint fields that are set but not interpreted.
func (ep endpointSpec) ignoredFields() []string {
	var out []string
	add := func(name string, set bool) {
		if set {
			out = append(out, name)
		}
	}
	add("oauth2", len(ep.OAuth2) > 0)
	add("bearerTokenFile", ep.BearerTokenFile != "")
	add("followRedirects", ep.FollowRedirects != nil)
	add("enableHttp2", ep.EnableHTTP2 != nil)
	add("honorTimestamps", ep.HonorTimestamps != nil)
	add("trackTimestampsStaleness", ep.TrackTimestampsStaleness != nil)
	add("proxyUrl", ep.ProxyURL != "")
	add("params", len(ep.Params) > 0)
	add("honorLabels", ep.HonorLabels != nil && *ep.HonorLabels)
	add("relabelings", len(ep.Relabelings) > 0)
	add("filterRunning", ep.FilterRunning != nil && !*ep.FilterRunning)
	if ep.TLSConfig != nil {
		// Only the configMap arm is unsupported; secret-backed CA/cert are
		// interpreted below.
		add("tlsConfig.ca.configMap", ep.TLSConfig.CA.usesConfigMap())
		add("tlsConfig.cert.configMap", ep.TLSConfig.Cert.usesConfigMap())
		// The file-path arms cannot be honoured (the agent has no access to
		// the Prometheus pod's filesystem) and their absence is not benign:
		// the target falls back to the system trust store and every scrape
		// fails verification. Say so.
		add("tlsConfig.caFile", ep.TLSConfig.CAFile != "")
		add("tlsConfig.certFile", ep.TLSConfig.CertFile != "")
		add("tlsConfig.keyFile", ep.TLSConfig.KeyFile != "")
	}
	for _, r := range ep.MetricRelabelings {
		if r.Action != "keep" && r.Action != "drop" {
			out = append(out, "metricRelabelings.action="+r.Action)
			continue
		}
		if r.Separator != "" && r.Separator != ";" {
			out = append(out, "metricRelabelings.separator="+r.Separator)
		}
	}
	return out
}

// toEndpoint converts the spec shape (BearerSecret namespace filled by the
// caller).
func (ep endpointSpec) toEndpoint() Endpoint {
	out := Endpoint{
		Port: ep.Port, TargetPort: ep.TargetPort, Path: ep.Path, Scheme: ep.Scheme,
		Interval: ep.Interval, ScrapeTimeout: ep.ScrapeTimeout,
		Ignored: ep.ignoredFields(),
	}
	if ep.TLSConfig != nil {
		out.InsecureSkipVerify = ep.TLSConfig.InsecureSkipVerify
		out.TLSServerName = ep.TLSConfig.ServerName
		out.TLSCA = ep.TLSConfig.CA.ref()
		out.TLSCert = ep.TLSConfig.Cert.ref()
		out.TLSKey = ep.TLSConfig.KeySecret.ref()
	}
	if ep.BasicAuth != nil {
		out.BasicAuthUser = ep.BasicAuth.Username.ref()
		out.BasicAuthPass = ep.BasicAuth.Password.ref()
	}
	if ep.Authorization != nil {
		out.AuthType = ep.Authorization.Type
		out.AuthCredentials = ep.Authorization.Credentials.ref()
	}
	if ep.BearerTokenSecret != nil && ep.BearerTokenSecret.Name != "" && ep.BearerTokenSecret.Key != "" {
		out.BearerSecret = ep.BearerTokenSecret.Name + "/" + ep.BearerTokenSecret.Key
	}
	for _, r := range ep.MetricRelabelings {
		if r.Action != "keep" && r.Action != "drop" {
			continue
		}
		// A custom separator is reported (below) and the rule SKIPPED: the
		// agent joins sourceLabels with ';', so applying the rule anyway
		// would test the user's regex against a string it was never written
		// for — silently inverting a keep into a drop-everything.
		if r.Separator != "" && r.Separator != ";" {
			continue
		}
		out.MetricRelabelings = append(out.MetricRelabelings, RelabelRule{
			Action:       r.Action,
			SourceLabels: r.SourceLabels,
			Regex:        r.Regex,
		})
	}
	return out
}

// Monitor is one parsed ServiceMonitor.
type Monitor struct {
	Namespace string
	Name      string
	// Selector selects Services by their labels.
	Selector labels.Selector
	// NamespaceAny selects Services in all namespaces; otherwise Namespaces
	// (defaulting to the monitor's own) applies.
	NamespaceAny bool
	Namespaces   []string
	Endpoints    []Endpoint
}

// ServiceNamespaces returns the namespaces the monitor selects Services in;
// nil means all.
func (m *Monitor) ServiceNamespaces() []string {
	if m.NamespaceAny {
		return nil
	}
	if len(m.Namespaces) > 0 {
		return m.Namespaces
	}
	return []string{m.Namespace}
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
	// filterRunning is NOT here: the CRD puts it on the ENDPOINT
	// (PodMetricsEndpoint), so it lives on endpointSpec. It sat in this struct
	// for a while, where it could never decode.
	BodySizeLimit  string          `json:"bodySizeLimit"`
	AttachMetadata *map[string]any `json:"attachMetadata"`
	ScrapeClass    string          `json:"scrapeClass"`
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
	return out
}

// smSpec mirrors the ServiceMonitor spec fields we interpret.
type smSpec struct {
	Selector          metav1.LabelSelector `json:"selector"`
	NamespaceSelector struct {
		Any        bool     `json:"any"`
		MatchNames []string `json:"matchNames"`
	} `json:"namespaceSelector"`
	Endpoints  []endpointSpec `json:"endpoints"`
	specLimits `json:",inline"`
}

// Parse converts an unstructured ServiceMonitor.
func Parse(u *unstructured.Unstructured) (*Monitor, error) {
	specRaw, ok := u.Object["spec"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("servicemonitor %s/%s: no spec", u.GetNamespace(), u.GetName())
	}
	var spec smSpec
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(specRaw, &spec); err != nil {
		return nil, fmt.Errorf("servicemonitor %s/%s: %w", u.GetNamespace(), u.GetName(), err)
	}
	sel, err := metav1.LabelSelectorAsSelector(&spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("servicemonitor %s/%s selector: %w", u.GetNamespace(), u.GetName(), err)
	}
	m := &Monitor{
		Namespace:    u.GetNamespace(),
		Name:         u.GetName(),
		Selector:     sel,
		NamespaceAny: spec.NamespaceSelector.Any,
		Namespaces:   spec.NamespaceSelector.MatchNames,
	}
	specIgnored := spec.ignored()
	for _, ep := range spec.Endpoints {
		e := ep.toEndpoint()
		// Monitor-level ignored fields ride on every endpoint; IgnoredFields
		// dedupes across endpoints, so they are reported exactly once.
		e.Ignored = append(e.Ignored, specIgnored...)
		// Every secret reference is namespaced with the MONITOR's namespace: a
		// monitor may only name secrets in its own namespace, which is what
		// bounds what /v1/scrape-auth will serve. The FIELD LIST lives on
		// Endpoint (secretRefs), shared with the PodMonitor parser and with
		// AuthSecretRefs — see its doc for why it must be exactly one list.
		e.namespaceSecretRefs(m.Namespace)
		m.Endpoints = append(m.Endpoints, e)
	}
	return m, nil
}

// Index is the thread-safe monitor store fed by the informer.
type Index struct {
	mu          sync.RWMutex
	monitors    map[string]*Monitor
	podMonitors map[string]*PodMonitor
}

// NewIndex creates an empty index.
func NewIndex() *Index {
	return &Index{
		monitors:    make(map[string]*Monitor),
		podMonitors: make(map[string]*PodMonitor),
	}
}

// Upsert parses and stores a monitor. A monitor UPDATED to an unparseable spec
// is removed rather than kept: silently serving the previous version forever
// would diverge from what the manifest declares (prometheus-operator likewise
// generates no config for an invalid monitor).
func (ix *Index) Upsert(u *unstructured.Unstructured) error {
	m, err := Parse(u)
	if err != nil {
		ix.Delete(u.GetNamespace(), u.GetName())
		return err
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.monitors[m.Namespace+"/"+m.Name] = m
	return nil
}

// Delete removes a monitor.
func (ix *Index) Delete(namespace, name string) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	delete(ix.monitors, namespace+"/"+name)
}

// All returns the current monitors, ordered by namespace/name: map iteration
// order must not decide which monitor a URL-deduped target is attributed to
// (the same determinism the server enforces for services).
func (ix *Index) All() []*Monitor {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	out := make([]*Monitor, 0, len(ix.monitors))
	for _, m := range ix.monitors {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// AuthSecretRefs returns the set of "namespace/name/key" bearerTokenSecret
// references across all indexed ServiceMonitor and PodMonitor endpoints. The
// scrape-auth endpoint serves ONLY these, so a direct HTTP caller cannot use
// it to read arbitrary cluster secrets — only the tokens a monitor actually
// references.
func (x *Index) AuthSecretRefs() map[string]struct{} {
	x.mu.RLock()
	defer x.mu.RUnlock()
	out := map[string]struct{}{}
	add := func(eps []Endpoint) {
		for i := range eps {
			// Every secret an endpoint references, so the metadata service
			// serves exactly the keys some monitor actually names — the
			// allowlist that keeps -scrape-auth-secrets from being a general
			// secret-read API. The FIELD LIST is Endpoint.secretRefs, the same
			// one both parsers namespace with: an allowlist that could disagree
			// with the namespacing is a 404 on one side or an unmatchable ref on
			// the other, and both scrape unauthenticated.
			//
			// Indexed, not ranged by value: secretRefs takes the address of the
			// endpoint's fields, and a loop copy's addresses are the copy's.
			for _, ref := range eps[i].secretRefs() {
				if *ref != "" {
					out[*ref] = struct{}{}
				}
			}
		}
	}
	for _, m := range x.monitors {
		add(m.Endpoints)
	}
	for _, m := range x.podMonitors {
		add(m.Endpoints)
	}
	return out
}

// IgnoredFields returns the distinct endpoint fields present on these
// endpoints that kubescrape does not interpret, sorted.
//
// kubescrape deliberately implements a SUBSET of the ServiceMonitor and
// PodMonitor spec, which is fine — but a partially-applied CR must not be
// silent. An operator who applies a monitor with `relabelings` renaming a
// label, or a `sampleLimit` guarding against a cardinality bomb, otherwise
// sees targets appear and never learns those clauses did nothing.
func IgnoredFields(eps []Endpoint) []string {
	seen := map[string]bool{}
	var out []string
	for _, ep := range eps {
		for _, f := range ep.Ignored {
			if !seen[f] {
				seen[f] = true
				out = append(out, f)
			}
		}
	}
	sort.Strings(out)
	return out
}

// Endpoints returns a stored ServiceMonitor's endpoints (nil when absent).
func (ix *Index) Endpoints(namespace, name string) []Endpoint {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if m := ix.monitors[namespace+"/"+name]; m != nil {
		return m.Endpoints
	}
	return nil
}

// PodEndpoints returns a stored PodMonitor's endpoints (nil when absent).
func (ix *Index) PodEndpoints(namespace, name string) []Endpoint {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if m := ix.podMonitors[namespace+"/"+name]; m != nil {
		return m.Endpoints
	}
	return nil
}
