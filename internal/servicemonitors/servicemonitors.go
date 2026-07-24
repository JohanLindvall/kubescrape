// Package servicemonitors indexes Prometheus-Operator ServiceMonitor custom
// resources so their targets can be served alongside annotation-discovered
// ones. Only pod-backed Services are supported: targets resolve through the
// selected Services' pod selectors, which keeps scraping node-local.
// Per-endpoint authentication, relabelings and interval overrides are not
// interpreted.
package servicemonitors

import (
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
	Port       string              `json:"port"`
	TargetPort *intstr.IntOrString `json:"targetPort"`
	Path       string              `json:"path"`
	Scheme     string              `json:"scheme"`
	TLSConfig  *struct {
		InsecureSkipVerify bool `json:"insecureSkipVerify"`
	} `json:"tlsConfig"`
	BearerTokenSecret *struct {
		Name string `json:"name"`
		Key  string `json:"key"`
	} `json:"bearerTokenSecret"`
	MetricRelabelings []struct {
		Action       string   `json:"action"`
		SourceLabels []string `json:"sourceLabels"`
		Regex        string   `json:"regex"`
	} `json:"metricRelabelings"`
}

// toEndpoint converts the spec shape (BearerSecret namespace filled by the
// caller).
func (ep endpointSpec) toEndpoint() Endpoint {
	out := Endpoint{Port: ep.Port, TargetPort: ep.TargetPort, Path: ep.Path, Scheme: ep.Scheme}
	if ep.TLSConfig != nil {
		out.InsecureSkipVerify = ep.TLSConfig.InsecureSkipVerify
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
		if e.BearerSecret != "" {
			e.BearerSecret = m.Namespace + "/" + e.BearerSecret
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
	probes      map[string]*Probe
}

// NewIndex creates an empty index.
func NewIndex() *Index {
	return &Index{
		monitors:    make(map[string]*Monitor),
		podMonitors: make(map[string]*PodMonitor),
		probes:      make(map[string]*Probe),
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
