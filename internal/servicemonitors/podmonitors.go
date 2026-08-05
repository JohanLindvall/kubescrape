package servicemonitors

// PodMonitor support: prometheus-operator's pod-selecting discovery CRD.
// PodMonitors select PODS directly (no Service hop) — endpoints name
// container ports. (Probes are deliberately not supported: blackbox
// probing has no node affinity and does not fit the node-local model.)

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
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
	return namespaceSelector{Any: m.NamespaceAny, MatchNames: m.Namespaces}.resolve(m.Namespace)
}

// pmSpec mirrors the PodMonitor spec fields we interpret.
type pmSpec struct {
	Selector            metav1.LabelSelector `json:"selector"`
	NamespaceSelector   namespaceSelector    `json:"namespaceSelector"`
	PodMetricsEndpoints []endpointSpec       `json:"podMetricsEndpoints"`
	specLimits          `json:",inline"`
}

func (s *pmSpec) labelSelector() *metav1.LabelSelector { return &s.Selector }
func (s *pmSpec) nsSelector() namespaceSelector        { return s.NamespaceSelector }
func (s *pmSpec) endpointSpecs() []endpointSpec        { return s.PodMetricsEndpoints }
func (s *pmSpec) monitorIgnored() []string             { return s.ignored() }

// ParsePodMonitor converts an unstructured PodMonitor. The skeleton —
// including the per-endpoint secret-ref namespacing that bounds
// /v1/scrape-auth — is parseMonitorSpec, shared with Parse.
func ParsePodMonitor(u *unstructured.Unstructured) (*PodMonitor, error) {
	b, err := parseMonitorSpec(u, "podmonitor", &pmSpec{})
	if err != nil {
		return nil, err
	}
	return &PodMonitor{
		Namespace:    b.Namespace,
		Name:         b.Name,
		Selector:     b.Selector,
		NamespaceAny: b.NamespaceAny,
		Namespaces:   b.Namespaces,
		Endpoints:    b.Endpoints,
	}, nil
}

// UpsertPodMonitor parses and stores a PodMonitor (see upsertMonitor for the
// invalid-update-removes policy — which matters doubly here, because the
// endpoints being dropped carry the bearerTokenSecret refs AuthSecretRefs
// allowlists, so a stale monitor would keep /v1/scrape-auth willing to serve
// a Secret the live spec no longer names).
func (x *Index) UpsertPodMonitor(u *unstructured.Unstructured) error {
	m, err := ParsePodMonitor(u)
	return upsertMonitor(x, x.podMonitors, u.GetNamespace()+"/"+u.GetName(), m, err)
}

// DeletePodMonitor removes one.
func (x *Index) DeletePodMonitor(namespace, name string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	delete(x.podMonitors, namespace+"/"+name)
}

// PodMonitors returns all pod monitors (shared, treat as immutable).
//
// Sorted like Monitor.All(): when two PodMonitors select the same pod and
// mint the same URL, the URL-dedup in handleNodeTargets keeps the FIRST, so
// map-iteration order must not decide which monitor's name / auth /
// relabelings ride on the surviving target.
func (x *Index) PodMonitors() []*PodMonitor {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return sortedMonitors(x.podMonitors, func(m *PodMonitor) (string, string) { return m.Namespace, m.Name })
}
