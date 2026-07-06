// Package servicemonitors indexes Prometheus-Operator ServiceMonitor custom
// resources so their targets can be served alongside annotation-discovered
// ones. Only pod-backed Services are supported: targets resolve through the
// selected Services' pod selectors, which keeps scraping node-local.
// Per-endpoint authentication, relabelings and interval overrides are not
// interpreted.
package servicemonitors

import (
	"fmt"
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
	// Port is the Service port name.
	Port string
	// TargetPort overrides the pod port directly (number or container port
	// name); nil defers to the service port's targetPort.
	TargetPort *intstr.IntOrString
	Path       string
	Scheme     string
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
	Endpoints []struct {
		Port       string              `json:"port"`
		TargetPort *intstr.IntOrString `json:"targetPort"`
		Path       string              `json:"path"`
		Scheme     string              `json:"scheme"`
	} `json:"endpoints"`
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
		m.Endpoints = append(m.Endpoints, Endpoint{
			Port: ep.Port, TargetPort: ep.TargetPort, Path: ep.Path, Scheme: ep.Scheme,
		})
	}
	return m, nil
}

// Index is the thread-safe monitor store fed by the informer.
type Index struct {
	mu       sync.RWMutex
	monitors map[string]*Monitor
}

// NewIndex creates an empty index.
func NewIndex() *Index {
	return &Index{monitors: make(map[string]*Monitor)}
}

// Upsert parses and stores a monitor.
func (ix *Index) Upsert(u *unstructured.Unstructured) error {
	m, err := Parse(u)
	if err != nil {
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

// All returns the current monitors.
func (ix *Index) All() []*Monitor {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	out := make([]*Monitor, 0, len(ix.monitors))
	for _, m := range ix.monitors {
		out = append(out, m)
	}
	return out
}
