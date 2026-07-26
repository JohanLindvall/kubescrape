// Package servicemonitors indexes Prometheus-Operator ServiceMonitor custom
// resources so their targets can be served alongside annotation-discovered
// ones. Only pod-backed Services are supported: targets resolve through the
// selected Services' pod selectors, which keeps scraping node-local.
// Per-endpoint authentication, relabelings and interval overrides are not
// interpreted.
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
	BearerTokenFile          string          `json:"bearerTokenFile"`
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
	if ep.TLSConfig != nil {
		// Only the configMap arm is unsupported; secret-backed CA/cert are
		// interpreted below.
		add("tlsConfig.ca.configMap", ep.TLSConfig.CA.usesConfigMap())
		add("tlsConfig.cert.configMap", ep.TLSConfig.Cert.usesConfigMap())
	}
	for _, r := range ep.MetricRelabelings {
		if r.Action != "keep" && r.Action != "drop" {
			out = append(out, "metricRelabelings.action="+r.Action)
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
		if r.Action == "keep" || r.Action == "drop" {
			out.MetricRelabelings = append(out.MetricRelabelings, RelabelRule(r))
		}
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

// smSpec mirrors the ServiceMonitor spec fields we interpret.
type smSpec struct {
	Selector          metav1.LabelSelector `json:"selector"`
	NamespaceSelector struct {
		Any        bool     `json:"any"`
		MatchNames []string `json:"matchNames"`
	} `json:"namespaceSelector"`
	Endpoints []endpointSpec `json:"endpoints"`
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
	for _, ep := range spec.Endpoints {
		e := ep.toEndpoint()
		// Every secret reference is namespaced with the MONITOR's namespace: a
		// monitor may only name secrets in its own namespace, which is what
		// bounds what /v1/scrape-auth will serve.
		for _, p := range []*string{
			&e.BearerSecret, &e.BasicAuthUser, &e.BasicAuthPass,
			&e.AuthCredentials, &e.TLSCA, &e.TLSCert, &e.TLSKey,
		} {
			if *p != "" {
				*p = m.Namespace + "/" + *p
			}
		}
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
		for _, e := range eps {
			// Every secret an endpoint references, so the metadata service
			// serves exactly the keys some monitor actually names — the
			// allowlist that keeps -scrape-auth-secrets from being a general
			// secret-read API.
			for _, ref := range []string{
				e.BearerSecret, e.BasicAuthUser, e.BasicAuthPass,
				e.AuthCredentials, e.TLSCA, e.TLSCert, e.TLSKey,
			} {
				if ref != "" {
					out[ref] = struct{}{}
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
