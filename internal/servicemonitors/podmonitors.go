package servicemonitors

// PodMonitor support: prometheus-operator's pod-selecting discovery CRD.
// PodMonitors select PODS directly (no Service hop) — endpoints name
// container ports. (Probes are deliberately not supported: blackbox
// probing has no node affinity and does not fit the node-local model.)

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// PodGVR is the PodMonitor resource.
var PodGVR = GVR.GroupVersion().WithResource("podmonitors")

// PodMonitor is one parsed PodMonitor: the shared monitorBase record, whose
// Selector selects PODS by label and whose endpoints' Port names a CONTAINER
// port.
type PodMonitor struct{ monitorBase }

// PodNamespaces returns the namespaces the monitor selects pods in; nil
// means all.
func (m *PodMonitor) PodNamespaces() []string { return m.namespaces() }

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
func (s *pmSpec) endpointsField() string               { return "podMetricsEndpoints" }
func (s *pmSpec) monitorIgnored() []string             { return s.ignored() }

// ParsePodMonitor converts an unstructured PodMonitor. The skeleton —
// including the per-endpoint secret-ref namespacing that bounds
// /v1/scrape-auth — is parseMonitorSpec, shared with Parse.
func ParsePodMonitor(u *unstructured.Unstructured) (*PodMonitor, error) {
	b, err := parseMonitorSpec(u, "podmonitor", &pmSpec{})
	if err != nil {
		return nil, err
	}
	return &PodMonitor{b}, nil
}

// UpsertPodMonitor parses and stores a PodMonitor (see upsertMonitor for the
// invalid-update-removes policy — which matters doubly here, because the
// endpoints being dropped carry the secret refs (bearer, basicAuth,
// authorization, TLS CA/cert/key) AuthSecretRefs allowlists, so a stale monitor would keep /v1/scrape-auth willing to serve
// a Secret the live spec no longer names).
func (ix *Index) UpsertPodMonitor(u *unstructured.Unstructured) error {
	_, _, err := ix.UpsertPodMonitorChanged(u)
	return err
}

// UpsertPodMonitorChanged is UpsertPodMonitor, additionally reporting whether
// the delivery was news, with the parsed endpoints — Index.UpsertChanged's
// mirror, and for the same caller.
func (ix *Index) UpsertPodMonitorChanged(u *unstructured.Unstructured) (eps []Endpoint, news bool, err error) {
	m, err := ParsePodMonitor(u)
	news, err = upsertMonitor(ix, ix.podMonitors, ix.rejectedPodMonitors,
		u.GetNamespace()+"/"+u.GetName(), u.GetResourceVersion(), m, err)
	if err != nil {
		return nil, news, err
	}
	return m.Endpoints, news, nil
}

// DeletePodMonitor removes one.
func (ix *Index) DeletePodMonitor(namespace, name string) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	deleteMonitor(ix, ix.podMonitors, ix.rejectedPodMonitors, namespace+"/"+name)
}

// PodMonitors returns all pod monitors (shared, treat as immutable).
//
// Sorted like Monitor.All(): when two PodMonitors select the same pod and
// mint the same URL, the server serves ONE target with the SECOND monitor's
// endpoint configuration merged into the first's (scrape.MergeMonitorEndpoint)
// — so map-iteration order must not decide which monitor names the target,
// whose relabel chain runs first, or whose auth wins a conflict.
func (ix *Index) PodMonitors() []*PodMonitor {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return sortedMonitors(ix.podMonitors)
}
