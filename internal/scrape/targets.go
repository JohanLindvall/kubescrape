// Package scrape derives Prometheus scrape targets from pod and service
// metadata using the conventional prometheus.io/* annotations.
package scrape

import (
	"net"
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
		port, ok := targetPodPort(pod, sp)
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
	return []kubemeta.ScrapeTarget{t}
}

// monitorPortNumber extracts a numeric targetPort, bounds-checked to the
// valid port range; string values that do not parse fall through to the
// port-name path.
func monitorPortNumber(tp intstr.IntOrString) (int32, bool) {
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
	if ep.TargetPort != nil {
		// IntValue() on a string-typed value Atoi's it ignoring the error and
		// returns a full int, so parse and bound explicitly: "4294967297"
		// must be rejected, not truncated to port 1.
		if n, ok := monitorPortNumber(*ep.TargetPort); ok {
			return n, true
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
	for _, sp := range svc.Ports {
		if sp.Name == ep.Port {
			return targetPodPort(pod, sp)
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

// targetPodPort translates a service port to the pod port it targets.
func targetPodPort(pod kubemeta.Pod, sp services.Port) (int32, bool) {
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
