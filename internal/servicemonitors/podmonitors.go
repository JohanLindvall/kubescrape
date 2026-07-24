package servicemonitors

// PodMonitor and Probe support: the other two prometheus-operator discovery
// CRDs. PodMonitors select PODS directly (no Service hop) — endpoints name
// container ports. Probes resolve through their PROBER's Service to the
// prober's pods, so probing stays node-local: the agent on the prober pod's
// node scrapes http://<prober-pod>:<port>/<path>?module=<m>&target=<t> for
// each static target.

import (
	"fmt"
	"net"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// PodGVR is the PodMonitor resource.
var PodGVR = GVR.GroupVersion().WithResource("podmonitors")

// ProbeGVR is the Probe resource.
var ProbeGVR = GVR.GroupVersion().WithResource("probes")

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

// Probe is one parsed Probe (staticConfig targets only; ingress targets are
// not interpreted). ProberService/ProberPort locate the prober's backing
// pods so probing stays node-local.
type Probe struct {
	Namespace     string
	Name          string
	ProberService string // service name parsed from prober.url
	ProberNS      string // service namespace (prober.url DNS form, default probe's own)
	ProberPort    *intstr.IntOrString
	ProberPath    string // default /probe
	ProberScheme  string
	Module        string
	StaticTargets []string
	StaticLabels  map[string]string
}

// probeSpec mirrors the Probe spec fields we interpret.
type probeSpec struct {
	Prober struct {
		URL    string `json:"url"`
		Scheme string `json:"scheme"`
		Path   string `json:"path"`
	} `json:"prober"`
	Module  string `json:"module"`
	Targets struct {
		StaticConfig struct {
			Static []string          `json:"static"`
			Labels map[string]string `json:"labels"`
		} `json:"staticConfig"`
	} `json:"targets"`
}

// ParseProbe converts an unstructured Probe. The prober URL must be the
// DNS form of a Service ("name.namespace[.svc...][:port]" or "name:port" in
// the probe's own namespace) — that is what makes the probe schedulable to
// the node running a prober pod.
func ParseProbe(u *unstructured.Unstructured) (*Probe, error) {
	specRaw, ok := u.Object["spec"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("probe %s/%s: no spec", u.GetNamespace(), u.GetName())
	}
	var spec probeSpec
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(specRaw, &spec); err != nil {
		return nil, fmt.Errorf("probe %s/%s: %w", u.GetNamespace(), u.GetName(), err)
	}
	if spec.Prober.URL == "" {
		return nil, fmt.Errorf("probe %s/%s: prober.url is required", u.GetNamespace(), u.GetName())
	}
	if len(spec.Targets.StaticConfig.Static) == 0 {
		return nil, fmt.Errorf("probe %s/%s: only staticConfig targets are supported", u.GetNamespace(), u.GetName())
	}
	host := spec.Prober.URL
	var port *intstr.IntOrString
	if h, p, err := net.SplitHostPort(spec.Prober.URL); err == nil {
		host = h
		v := intstr.Parse(p)
		port = &v
	}
	svcName, svcNS := host, u.GetNamespace()
	if parts := strings.Split(host, "."); len(parts) >= 2 {
		svcName, svcNS = parts[0], parts[1]
	}
	path := spec.Prober.Path
	if path == "" {
		path = "/probe"
	}
	return &Probe{
		Namespace:     u.GetNamespace(),
		Name:          u.GetName(),
		ProberService: svcName,
		ProberNS:      svcNS,
		ProberPort:    port,
		ProberPath:    path,
		ProberScheme:  spec.Prober.Scheme,
		Module:        spec.Module,
		StaticTargets: spec.Targets.StaticConfig.Static,
		StaticLabels:  spec.Targets.StaticConfig.Labels,
	}, nil
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
	return out
}

// UpsertProbe parses and stores a Probe.
func (x *Index) UpsertProbe(u *unstructured.Unstructured) error {
	p, err := ParseProbe(u)
	x.mu.Lock()
	defer x.mu.Unlock()
	if err != nil {
		delete(x.probes, u.GetNamespace()+"/"+u.GetName())
		return err
	}
	x.probes[p.Namespace+"/"+p.Name] = p
	return nil
}

// DeleteProbe removes one.
func (x *Index) DeleteProbe(namespace, name string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	delete(x.probes, namespace+"/"+name)
}

// Probes returns all probes (shared, treat as immutable).
func (x *Index) Probes() []*Probe {
	x.mu.RLock()
	defer x.mu.RUnlock()
	out := make([]*Probe, 0, len(x.probes))
	for _, p := range x.probes {
		out = append(out, p)
	}
	return out
}
