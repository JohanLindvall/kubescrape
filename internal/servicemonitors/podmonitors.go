package servicemonitors

// PodMonitor support: prometheus-operator's pod-selecting discovery CRD.
// PodMonitors select PODS directly (no Service hop) — endpoints name
// container ports. (Probes are deliberately not supported: blackbox
// probing has no node affinity and does not fit the node-local model.)

import (
	"fmt"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
)

// PodGVR is the PodMonitor resource.
var PodGVR = GVR.GroupVersion().WithResource("podmonitors")

// PodMonitor is one parsed PodMonitor: a pod label selector plus container
// port endpoints.
type PodMonitor struct {
	Namespace    string
	Name         string
	Selector     labels.Selector // selects PODS by label
	NamespaceAny bool
	Namespaces   []string
	Endpoints    []Endpoint // Port names a CONTAINER port
}

// PodNamespaces returns the namespaces the monitor selects pods in; nil
// means all.
func (m *PodMonitor) PodNamespaces() []string {
	if m.NamespaceAny {
		return nil
	}
	if len(m.Namespaces) > 0 {
		return m.Namespaces
	}
	return []string{m.Namespace}
}

// pmSpec mirrors the PodMonitor spec fields we interpret.
type pmSpec struct {
	Selector          metav1.LabelSelector `json:"selector"`
	NamespaceSelector struct {
		Any        bool     `json:"any"`
		MatchNames []string `json:"matchNames"`
	} `json:"namespaceSelector"`
	PodMetricsEndpoints []endpointSpec `json:"podMetricsEndpoints"`
}

// ParsePodMonitor converts an unstructured PodMonitor.
func ParsePodMonitor(u *unstructured.Unstructured) (*PodMonitor, error) {
	specRaw, ok := u.Object["spec"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("podmonitor %s/%s: no spec", u.GetNamespace(), u.GetName())
	}
	var spec pmSpec
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(specRaw, &spec); err != nil {
		return nil, fmt.Errorf("podmonitor %s/%s: %w", u.GetNamespace(), u.GetName(), err)
	}
	sel, err := metav1.LabelSelectorAsSelector(&spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("podmonitor %s/%s selector: %w", u.GetNamespace(), u.GetName(), err)
	}
	m := &PodMonitor{
		Namespace:    u.GetNamespace(),
		Name:         u.GetName(),
		Selector:     sel,
		NamespaceAny: spec.NamespaceSelector.Any,
		Namespaces:   spec.NamespaceSelector.MatchNames,
	}
	for _, ep := range spec.PodMetricsEndpoints {
		e := ep.toEndpoint()
		if e.BearerSecret != "" {
			e.BearerSecret = m.Namespace + "/" + e.BearerSecret
		}
		m.Endpoints = append(m.Endpoints, e)
	}
	return m, nil
}

// UpsertPodMonitor parses and stores a PodMonitor.
func (x *Index) UpsertPodMonitor(u *unstructured.Unstructured) error {
	m, err := ParsePodMonitor(u)
	x.mu.Lock()
	defer x.mu.Unlock()
	if err != nil {
		delete(x.podMonitors, u.GetNamespace()+"/"+u.GetName())
		return err
	}
	x.podMonitors[m.Namespace+"/"+m.Name] = m
	return nil
}

// DeletePodMonitor removes one.
func (x *Index) DeletePodMonitor(namespace, name string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	delete(x.podMonitors, namespace+"/"+name)
}

// PodMonitors returns all pod monitors (shared, treat as immutable).
func (x *Index) PodMonitors() []*PodMonitor {
	x.mu.RLock()
	defer x.mu.RUnlock()
	out := make([]*PodMonitor, 0, len(x.podMonitors))
	for _, m := range x.podMonitors {
		out = append(out, m)
	}
	// Sorted like Monitor.All(): when two PodMonitors select the same pod and
	// mint the same URL, the URL-dedup in handleNodeTargets keeps the FIRST,
	// so map-iteration order must not decide which monitor's name / auth /
	// relabelings ride on the surviving target.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out
}
