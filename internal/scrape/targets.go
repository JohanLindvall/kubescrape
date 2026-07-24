// Package scrape derives Prometheus scrape targets from pod and service
// metadata using the conventional prometheus.io/* annotations.
package scrape

import (
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// Conventional annotations, as used by the classic Prometheus
// kubernetes_sd relabeling configuration. The port annotation accepts a
// comma-separated list of entries.
const (
	AnnotationScrape = "prometheus.io/scrape"
	AnnotationPath   = "prometheus.io/path"
	AnnotationPort   = "prometheus.io/port"
	AnnotationScheme = "prometheus.io/scheme"
)

// PodTargets returns the scrape targets derived from a pod's own
// annotations, or nil if the pod is not annotated for scraping or is not
// scrapeable (no IP, already finished).
//
// Each entry of the port annotation may be a port number or the name of a
// declared container port. Without a port annotation, every declared
// container port becomes a target. The pod (including any owners the caller
// resolved) is embedded in each target.
func PodTargets(pod kubemeta.Pod) []kubemeta.ScrapeTarget {
	if pod.Annotations[AnnotationScrape] != "true" || !Scrapeable(pod) {
		return nil
	}
	scheme, path := schemeAndPath(pod.Annotations)
	var targets []kubemeta.ScrapeTarget
	for _, port := range podPorts(pod) {
		t := makeTarget(pod, scheme, path, port)
		t.Source = "pod"
		targets = append(targets, t)
	}
	return targets
}

// ServiceTargets returns the scrape targets for a pod derived from the
// annotations of a Service that selects it, or nil if the service is not
// annotated for scraping or the pod is not scrapeable.
//
// Each entry of the service's port annotation may be a service port number
// or a service port name; without the annotation every service port is used.
// Service ports are translated to pod ports via their targetPort (named
// container port, explicit number, or the port itself).
func ServiceTargets(pod kubemeta.Pod, svc *services.Service) []kubemeta.ScrapeTarget {
	if svc == nil || svc.Annotations[AnnotationScrape] != "true" || !Scrapeable(pod) {
		return nil
	}
	scheme, path := schemeAndPath(svc.Annotations)
	info := serviceInfo(svc)
	var targets []kubemeta.ScrapeTarget
	seen := make(map[int32]struct{})
	for _, sp := range selectServicePorts(svc) {
		port, ok := TargetPodPort(pod, sp)
		if !ok {
			continue
		}
		if _, dup := seen[port]; dup {
			continue
		}
		seen[port] = struct{}{}
		t := makeTarget(pod, scheme, path, port)
		t.Source = "service"
		t.Service = info
		targets = append(targets, t)
	}
	return targets
}

// MonitorTargets returns the scrape targets for a pod derived from one
// ServiceMonitor endpoint of a Service selecting it. The endpoint's port
// names a Service port; targetPort (number or container-port name)
// overrides the pod port directly.
func MonitorTargets(pod kubemeta.Pod, svc *services.Service, monitor string, ep servicemonitors.Endpoint) []kubemeta.ScrapeTarget {
	if svc == nil || !Scrapeable(pod) {
		return nil
	}
	scheme, path := defaultSchemePath(ep.Scheme, ep.Path)

	port, ok := monitorPodPort(pod, svc, ep)
	if !ok {
		return nil
	}
	info := serviceInfo(svc)
	t := makeTarget(pod, scheme, path, port)
	t.Source = "servicemonitor"
	t.Service = info
	t.Monitor = monitor
	stampEndpoint(&t, ep)
	return []kubemeta.ScrapeTarget{t}
}

// stampEndpoint copies the endpoint's auth/TLS/relabeling declarations onto
// a target.
func stampEndpoint(t *kubemeta.ScrapeTarget, ep servicemonitors.Endpoint) {
	t.InsecureSkipVerify = ep.InsecureSkipVerify
	t.AuthSecret = ep.BearerSecret
	for _, r := range ep.MetricRelabelings {
		t.MetricRelabelings = append(t.MetricRelabelings, kubemeta.RelabelRule{
			Action: r.Action, SourceLabels: r.SourceLabels, Regex: r.Regex,
		})
	}
}

// PodMonitorTargets derives the targets a PodMonitor endpoint declares on a
// pod (already namespace- and selector-matched by the caller). The endpoint
// Port names a CONTAINER port; targetPort (deprecated) is a number or
// container port name.
func PodMonitorTargets(pod kubemeta.Pod, monitor string, ep servicemonitors.Endpoint) []kubemeta.ScrapeTarget {
	if !Scrapeable(pod) {
		return nil
	}
	if ep.Port == "" && ep.TargetPort == nil {
		return nil // same phantom-target guard as ServiceMonitors
	}
	scheme, path := defaultSchemePath(ep.Scheme, ep.Path)
	var port int32
	switch {
	case ep.Port != "":
		p, ok := containerPortByName(pod, ep.Port)
		if !ok {
			return nil
		}
		port = p
	default:
		if n, ok := MonitorPortNumber(*ep.TargetPort); ok {
			port = n
		} else if p, ok := containerPortByName(pod, ep.TargetPort.StrVal); ok {
			port = p
		} else {
			return nil
		}
	}
	t := makeTarget(pod, scheme, path, port)
	t.Source = "podmonitor"
	t.Monitor = monitor
	stampEndpoint(&t, ep)
	return []kubemeta.ScrapeTarget{t}
}

// containerPortByName finds a declared container port by name.
func containerPortByName(pod kubemeta.Pod, name string) (int32, bool) {
	if name == "" {
		return 0, false
	}
	for _, c := range pod.Containers {
		for _, p := range c.Ports {
			if p.Name == name {
				return p.Port, true
			}
		}
	}
	return 0, false
}

// ProbeTargets derives the targets a Probe declares against ONE prober pod
// (a backing pod of the prober Service, matched by the caller): one target
// per static entry, scraping the prober with module/target params.
func ProbeTargets(proberPod kubemeta.Pod, probe *servicemonitors.Probe, proberPort int32) []kubemeta.ScrapeTarget {
	if !Scrapeable(proberPod) {
		return nil
	}
	scheme, path := defaultSchemePath(probe.ProberScheme, probe.ProberPath)
	var out []kubemeta.ScrapeTarget
	for _, static := range probe.StaticTargets {
		t := makeTarget(proberPod, scheme, path, proberPort)
		q := url.Values{}
		if probe.Module != "" {
			q.Set("module", probe.Module)
		}
		q.Set("target", static)
		t.URL += "?" + q.Encode()
		t.Source = "probe"
		t.Monitor = probe.Namespace + "/" + probe.Name
		out = append(out, t)
	}
	return out
}

// MonitorPortNumber extracts a numeric targetPort, bounds-checked to the
// valid port range; string values that do not parse fall through to the
// port-name path.
func MonitorPortNumber(tp intstr.IntOrString) (int32, bool) {
	var n int64
	switch tp.Type {
	case intstr.Int:
		n = int64(tp.IntVal)
	default:
		parsed, err := strconv.ParseInt(tp.StrVal, 10, 32)
		if err != nil {
			return 0, false
		}
		n = parsed
	}
	if n < 1 || n > 65535 {
		return 0, false
	}
	return int32(n), true
}

// monitorPodPort resolves the pod port a ServiceMonitor endpoint targets.
func monitorPodPort(pod kubemeta.Pod, svc *services.Service, ep servicemonitors.Endpoint) (int32, bool) {
	// An endpoint that names neither port nor targetPort resolves to nothing:
	// prometheus-operator cannot reference a port and emits no scrape config, so
	// we must not either. Without this guard an empty ep.Port ("") matches a
	// Service's unnamed port by "" == "" and fabricates a phantom target.
	if ep.Port == "" && ep.TargetPort == nil {
		return 0, false
	}
	// When BOTH are set, `port` wins — prometheus-operator's precedence
	// (targetPort is its deprecated fallback); differing would scrape a
	// different pod port than the operator for the same manifest.
	if ep.Port != "" {
		for _, sp := range svc.Ports {
			if sp.Name == ep.Port {
				return TargetPodPort(pod, sp)
			}
		}
		return 0, false
	}
	// Port was empty, so TargetPort is non-nil (the guard above returned for
	// the neither-set case). IntValue() on a string-typed value Atoi's it
	// ignoring the error and returns a full int, so parse and bound explicitly:
	// "4294967297" must be rejected, not truncated to port 1.
	if n, ok := MonitorPortNumber(*ep.TargetPort); ok {
		return n, true
	}
	// Only a String-typed, non-empty targetPort may resolve by container port
	// NAME: an Int-typed value always has StrVal == "" (a rejected number like
	// 0 or 70000 names nothing), and matching "" against an UNNAMED container
	// port by "" == "" would fabricate a phantom target.
	if ep.TargetPort.Type != intstr.String || ep.TargetPort.StrVal == "" {
		return 0, false
	}
	for _, c := range pod.Containers {
		for _, p := range c.Ports {
			if p.Name == ep.TargetPort.StrVal {
				return p.Port, true
			}
		}
	}
	return 0, false
}

// Scrapeable reports whether a pod can yield scrape targets at all: it must
// be live (not deleted, not Succeeded/Failed) and have a pod IP.
func Scrapeable(pod kubemeta.Pod) bool {
	if pod.PodIP == "" || pod.DeletedAt != nil {
		return false
	}
	return pod.Phase != "Succeeded" && pod.Phase != "Failed"
}

func schemeAndPath(annotations map[string]string) (scheme, path string) {
	return defaultSchemePath(annotations[AnnotationScheme], annotations[AnnotationPath])
}

// defaultSchemePath applies the scrape scheme/path defaults: anything but
// "https" becomes "http", an empty path becomes "/metrics", and a path is given
// a leading slash.
func defaultSchemePath(scheme, path string) (string, string) {
	if scheme != "https" {
		scheme = "http"
	}
	if path == "" {
		path = "/metrics"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return scheme, path
}

// serviceInfo is the kubemeta.Service view stamped onto a service-derived target.
func serviceInfo(svc *services.Service) *kubemeta.Service {
	return &kubemeta.Service{
		Name:        svc.Name,
		Namespace:   svc.Namespace,
		UID:         svc.UID,
		Labels:      svc.Labels,
		Annotations: svc.Annotations,
	}
}

func makeTarget(pod kubemeta.Pod, scheme, path string, port int32) kubemeta.ScrapeTarget {
	address := net.JoinHostPort(pod.PodIP, strconv.Itoa(int(port)))
	return kubemeta.ScrapeTarget{
		URL:     scheme + "://" + address + path,
		Scheme:  scheme,
		Address: address,
		Port:    port,
		Path:    path,
		Pod:     pod,
	}
}

// podPorts resolves the pod's port annotation (each entry a number or a
// named container port); without an annotation, all declared container
// ports. Entries that resolve to nothing are skipped.
func podPorts(pod kubemeta.Pod) []int32 {
	var ports []int32
	seen := make(map[int32]struct{})
	add := func(p int32) {
		if _, ok := seen[p]; ok || p < 1 || p > 65535 {
			return
		}
		seen[p] = struct{}{}
		ports = append(ports, p)
	}

	ann, ok := pod.Annotations[AnnotationPort]
	if !ok || strings.TrimSpace(ann) == "" {
		for _, c := range pod.Containers {
			for _, p := range c.Ports {
				add(p.Port)
			}
		}
		return ports
	}
	for _, entry := range splitList(ann) {
		// ParseInt with a 32-bit size: values overflowing int32 must be
		// skipped, not truncated into a different (valid-looking) port.
		if n, err := strconv.ParseInt(entry, 10, 32); err == nil {
			add(int32(n))
			continue
		}
		for _, c := range pod.Containers {
			for _, p := range c.Ports {
				if p.Name == entry {
					add(p.Port)
				}
			}
		}
	}
	return ports
}

// selectServicePorts resolves the service's port annotation (each entry a
// service port number or name) against its declared ports; without an
// annotation, all service ports.
func selectServicePorts(svc *services.Service) []services.Port {
	ann, ok := svc.Annotations[AnnotationPort]
	if !ok || strings.TrimSpace(ann) == "" {
		return svc.Ports
	}
	var out []services.Port
	for _, entry := range splitList(ann) {
		// As in podPorts: reject rather than truncate values beyond int32.
		n, numeric := int64(0), false
		if v, err := strconv.ParseInt(entry, 10, 32); err == nil {
			n, numeric = v, true
		}
		for _, sp := range svc.Ports {
			if sp.Name == entry || (numeric && sp.Port == int32(n)) {
				out = append(out, sp)
			}
		}
	}
	return out
}

// TargetPodPort translates a service port to the pod port it targets.
func TargetPodPort(pod kubemeta.Pod, sp services.Port) (int32, bool) {
	if sp.TargetPortName != "" {
		for _, c := range pod.Containers {
			for _, p := range c.Ports {
				if p.Name == sp.TargetPortName {
					return p.Port, true
				}
			}
		}
		return 0, false
	}
	if sp.TargetPortNum != 0 {
		return sp.TargetPortNum, true
	}
	return sp.Port, true
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
