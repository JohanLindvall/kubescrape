// Package scrape derives Prometheus scrape targets from pod and service
// metadata using the conventional prometheus.io/* annotations.
package scrape

import (
	"net"
	"strconv"
	"strings"

	"github.com/JohanLindvall/kubescrape/internal/cli"
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
// declared container port (ONE declaration of it — containerPortByName's rule,
// shared with every other path that resolves a name). Without a port
// annotation, every declared container port becomes a target. The pod
// (including any owners the caller resolved) is embedded in each target.
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
// container port — resolved to ONE declaration by containerPortByName, exactly
// as the endpoints controller and a ServiceMonitor endpoint resolve it —
// explicit number, or the port itself).
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
	scheme, path, port, ok := monitorEndpoint(pod, svc, ep)
	if !ok {
		return nil
	}
	t := makeTarget(pod, scheme, path, port)
	t.Source = "servicemonitor"
	t.Service = serviceInfo(svc)
	t.Monitor = monitor
	stampEndpoint(&t, ep)
	return []kubemeta.ScrapeTarget{t}
}

// MonitorTargetURL is MonitorTargets' IDENTITY half: the URL the endpoint
// resolves to on this pod, or false when it resolves to nothing.
//
// It exists so a caller can dedup BEFORE materialising. A ScrapeTarget embeds
// the whole pod document and allocates a Service view and a relabeling copy,
// and a cluster-wide monitor set makes the served list a tiny fraction of what
// the loop builds: 50 ServiceMonitors selecting the same Services produced 125
// targets per pod of which 2 survived the URL dedup, at 641 KB and 1,206
// allocations per additional monitor for zero additional response bytes.
//
// The URL is a pure function of the pod IP, the scheme, the resolved port and
// the path — all of which are known here — and it goes through the same
// resolution and the same renderer as MonitorTargets, so the two cannot
// disagree about what a target is called.
func MonitorTargetURL(pod kubemeta.Pod, svc *services.Service, ep servicemonitors.Endpoint) (string, bool) {
	scheme, path, port, ok := monitorEndpoint(pod, svc, ep)
	if !ok {
		return "", false
	}
	return targetURL(pod, scheme, path, port), true
}

// monitorEndpoint resolves a ServiceMonitor endpoint against a pod behind a
// Service: the scheme/path defaults and the pod port the endpoint names.
func monitorEndpoint(pod kubemeta.Pod, svc *services.Service, ep servicemonitors.Endpoint) (scheme, path string, port int32, ok bool) {
	if svc == nil || !Scrapeable(pod) {
		return "", "", 0, false
	}
	port, ok = monitorPodPort(pod, svc, ep)
	if !ok {
		return "", "", 0, false
	}
	scheme, path = defaultSchemePath(ep.Scheme, ep.Path)
	return scheme, path, port, true
}

// stampEndpoint copies the endpoint's auth/TLS/relabeling declarations onto
// a target.
func stampEndpoint(t *kubemeta.ScrapeTarget, ep servicemonitors.Endpoint) {
	t.InsecureSkipVerify = ep.InsecureSkipVerify
	t.AuthSecret = ep.BearerSecret
	t.Interval = ep.Interval
	t.ScrapeTimeout = ep.ScrapeTimeout
	t.BasicAuthUser = ep.BasicAuthUser
	t.BasicAuthPass = ep.BasicAuthPass
	t.AuthType = ep.AuthType
	t.AuthCredentials = ep.AuthCredentials
	t.TLSCA = ep.TLSCA
	t.TLSCert = ep.TLSCert
	t.TLSKey = ep.TLSKey
	t.TLSServerName = ep.TLSServerName
	// servicemonitors.RelabelRule IS kubemeta.RelabelRule (a type alias — the
	// wire contract owns the shape), so the old field-by-field copy here was a
	// third place a new relabel field had to be remembered, and forgetting it
	// compiled clean while silently dropping the field from every served
	// target.
	t.MetricRelabelings = append(t.MetricRelabelings, ep.MetricRelabelings...)
}

// PodMonitorTargets derives the targets a PodMonitor endpoint declares on a
// pod (already namespace- and selector-matched by the caller). The endpoint
// Port names a CONTAINER port; targetPort (deprecated) is a number or
// container port name.
func PodMonitorTargets(pod kubemeta.Pod, monitor string, ep servicemonitors.Endpoint) []kubemeta.ScrapeTarget {
	scheme, path, port, ok := podMonitorEndpoint(pod, ep)
	if !ok {
		return nil
	}
	t := makeTarget(pod, scheme, path, port)
	t.Source = "podmonitor"
	t.Monitor = monitor
	stampEndpoint(&t, ep)
	return []kubemeta.ScrapeTarget{t}
}

// PodMonitorTargetURL is PodMonitorTargets' identity half; see MonitorTargetURL
// for why the two halves exist.
func PodMonitorTargetURL(pod kubemeta.Pod, ep servicemonitors.Endpoint) (string, bool) {
	scheme, path, port, ok := podMonitorEndpoint(pod, ep)
	if !ok {
		return "", false
	}
	return targetURL(pod, scheme, path, port), true
}

// podMonitorEndpoint resolves a PodMonitor endpoint against a pod: Port names a
// CONTAINER port, targetPort (deprecated) is a number or a container port name.
func podMonitorEndpoint(pod kubemeta.Pod, ep servicemonitors.Endpoint) (scheme, path string, port int32, ok bool) {
	if !Scrapeable(pod) {
		return "", "", 0, false
	}
	if ep.Port == "" && ep.TargetPort == nil {
		return "", "", 0, false // same phantom-target guard as ServiceMonitors
	}
	switch {
	case ep.Port != "":
		p, found := containerPortByName(pod, ep.Port)
		if !found {
			return "", "", 0, false
		}
		port = p
	default:
		if n, numeric := MonitorPortNumber(*ep.TargetPort); numeric {
			port = n
		} else if p, found := containerPortByName(pod, ep.TargetPort.StrVal); found {
			port = p
		} else {
			return "", "", 0, false
		}
	}
	scheme, path = defaultSchemePath(ep.Scheme, ep.Path)
	return scheme, path, port, true
}

// containerPortByName resolves a container-port NAME to a pod port: the first
// declaration on a REGULAR container, and only if none carries the name, the
// first on an init/sidecar or ephemeral container.
//
// It is the ONE resolver for that question, and every path in this package
// that asks it goes through here — the pod annotation's named entry
// (podPorts), a Service port's named targetPort (TargetPodPort, hence
// ServiceTargets), a ServiceMonitor endpoint's targetPort and a PodMonitor
// endpoint's port/targetPort (monitorPodPort/podMonitorEndpoint). One function
// because one pod must get ONE answer: a pod may legally declare one name on
// two containers (Kubernetes only WARNS about it at admission), and while the
// paths each open-coded the walk they disagreed about exactly that pod — the
// annotation resolved the name to every declaration and the Service's
// targetPort to the first, two answers in one response for one name on one
// pod. TestEveryPortPathAgreesOnADuplicateName pins the agreement.
//
// ONE declaration, not every one, because that is what the rest of the stack
// resolves: the endpoints/EndpointSlice controller translates a named
// targetPort with podutil.FindPort, which returns the first container port
// carrying the name, so the EndpointSlice carries ONE port — and both the
// classic prometheus.io/* Service convention (Prometheus' endpoints role) and
// prometheus-operator's generated jobs (endpointslice role, a keep on the
// endpoint's port name) scrape exactly that one. Resolving every declaration
// would make kubescrape scrape a port neither of them does, and — since the
// monitor path resolves ONE URL by contract (MonitorTargetURL is the identity
// the server dedups and merges on) — it would do it through a target no
// monitor endpoint could ever upgrade: the live shape was a monitor-derived
// target on the first port carrying the CR's bearer token and drop rules, and
// a bare service-source target on the second carrying neither. A pod that
// wants both declarations scraped names them by NUMBER
// ("prometheus.io/port: 9100,9200") or drops the annotation — without one
// every declared container port is a target, which is unchanged; explain says
// so on any name a pod declares twice.
//
// REGULAR CONTAINERS FIRST is the other half of that fidelity argument, and it
// is not the order the pod document is in: kubeconvert.FromPod appends
// spec.initContainers BEFORE spec.containers (it builds the model in the spec's
// own order, which is what the container endpoints report), while FindPort
// iterates spec.Containers ONLY. A native sidecar — an initContainer with
// restartPolicy: Always, the recommended sidecar shape since 1.29 — that
// declares the app's port name (a service mesh's "metrics" is the live case)
// therefore came first in this walk and won, so kubescrape scraped the mesh
// proxy's 15020 where Kubernetes, Prometheus and prometheus-operator all
// resolve the app's 9090. Preferring the regular containers restores exactly
// FindPort's answer wherever FindPort has one.
//
// A name NO regular container declares still resolves, from the init/ephemeral
// pass, and that is deliberate: it is the one case where FindPort has no answer
// at all (an EndpointSlice would carry nothing), so nothing is being contradicted
// — while dropping it would silently stop scraping a metrics port a sidecar
// legitimately owns, which is a common shape for a proxy or an exporter running
// as a native sidecar in a pod whose app declares no ports. Second pass, not
// first: the fallback must never outrank a regular container's declaration.
//
// The empty-name check is NOT a redundant nil-guard, and must not be removed
// as one: it is the phantom-target guard PodMonitorTargets depends on. That
// path passes a degenerate string targetPort straight here, so without it an
// endpoint with `targetPort: ""` would match the first UNNAMED container port
// and mint a scrape target the user never declared. MonitorTargets states the
// same precondition explicitly at its own call site; this is where the
// PodMonitor half of it lives.
func containerPortByName(pod kubemeta.Pod, name string) (int32, bool) {
	if name == "" {
		return 0, false
	}
	if p, ok := portByName(pod, name, true); ok {
		return p, true
	}
	return portByName(pod, name, false)
}

// portByName is containerPortByName's one pass: the first declaration of the
// name among the regular containers, or among the others.
func portByName(pod kubemeta.Pod, name string, regular bool) (int32, bool) {
	for _, c := range pod.Containers {
		if regularContainer(c) != regular {
			continue
		}
		for _, p := range c.Ports {
			if p.Name == name {
				return p.Port, true
			}
		}
	}
	return 0, false
}

// regularContainer reports whether this is one of spec.containers — the list
// podutil.FindPort walks.
//
// It tests for the two OTHER types rather than for "container", so a Pod model
// built without the field (a hand-written one in a test, or a future producer
// that leaves it empty) keeps the whole-document order this walk had before the
// two passes existed. An unstamped container mis-sorted into the fallback pass
// would be a silent port change; mis-sorted into the first pass it is exactly
// the old behaviour.
func regularContainer(c kubemeta.Container) bool {
	return c.Type != "init" && c.Type != "ephemeral"
}

// containerPortDeclarations counts how many containers declare a port under
// this name. Nothing derives a target from it — it exists so explain can say
// that a duplicated name resolved to ONE of its declarations and how to reach
// the others (containerPortByName's rule, which is silent in the target list).
func containerPortDeclarations(pod kubemeta.Pod, name string) int {
	if name == "" {
		return 0
	}
	n := 0
	for _, c := range pod.Containers {
		for _, p := range c.Ports {
			if p.Name == name {
				n++
			}
		}
	}
	return n
}

// MonitorPortNumber extracts a numeric targetPort, bounds-checked to the
// valid port range; string values that do not parse fall through to the
// port-name path.
func MonitorPortNumber(tp intstr.IntOrString) (int32, bool) {
	if tp.Type != intstr.Int {
		return parsePort(tp.StrVal)
	}
	if n := tp.IntVal; 1 <= n && n <= 65535 {
		return n, true
	}
	return 0, false
}

// parsePort parses one decimal port entry under the parse-don't-truncate
// policy every annotation/endpoint port shares: ParseInt with a 32-bit size,
// so a value overflowing int32 ("4294967297") is rejected, never truncated
// into a different valid-looking port; and bounded to the real 1-65535 port
// range. A rejected entry falls back to whatever name matching its caller
// does, which is safe because a Kubernetes port name must contain a letter —
// an all-digit string can never name a declared port.
func parsePort(entry string) (int32, bool) {
	n, err := strconv.ParseInt(entry, 10, 32)
	if err != nil || n < 1 || n > 65535 {
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
	// Only a String-typed targetPort may resolve by container port NAME: an
	// Int-typed value always has StrVal == "" (a rejected number like 0 or
	// 70000 names nothing). containerPortByName carries the other half of the
	// guard — matching "" against an UNNAMED container port by "" == "" would
	// fabricate a phantom target.
	if ep.TargetPort.Type != intstr.String {
		return 0, false
	}
	return containerPortByName(pod, ep.TargetPort.StrVal)
}

// Scrapeable reports whether a pod can yield scrape targets at all: it must
// be live (not deleted, not terminating, not Succeeded/Failed) and have a pod
// IP.
//
// TERMINATING pods (deletionTimestamp set) are excluded even though their
// phase stays Running for the whole grace period: the container is already
// being shut down while the pod still appears in the node's pod list, so
// scraping it yields connection failures — an `up=0` churn spike on every
// rollout, plus a scrape target that outlives the workload. Prometheus'
// endpoints discovery drops terminating endpoints for the same reason. They
// remain resolvable by container ID / UID / name; only TARGETS are affected.
func Scrapeable(pod kubemeta.Pod) bool {
	if pod.PodIP == "" || pod.DeletedAt != nil || pod.DeletionTimestamp != nil {
		return false
	}
	return !kubemeta.FinishedPhase(pod.Phase)
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
	address := podAddress(pod, port)
	return kubemeta.ScrapeTarget{
		URL:     renderURL(scheme, address, path),
		Scheme:  scheme,
		Address: address,
		Port:    port,
		Path:    path,
		Pod:     pod,
	}
}

// targetURL is what makeTarget would name this target, without building it.
// Both go through renderURL: the URL is a scrape target's IDENTITY (it is what
// the server dedups on and what the agent keys a scrape by), so the cheap half
// and the full half must not be able to spell it differently.
func targetURL(pod kubemeta.Pod, scheme, path string, port int32) string {
	return renderURL(scheme, podAddress(pod, port), path)
}

func podAddress(pod kubemeta.Pod, port int32) string {
	return net.JoinHostPort(pod.PodIP, strconv.Itoa(int(port)))
}

func renderURL(scheme, address, path string) string {
	return scheme + "://" + address + path
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
		if n, ok := parsePort(entry); ok {
			add(n)
			continue
		}
		// Through the ONE name resolver, not a fourth open-coded walk: this
		// used to add EVERY declaration of the name while the Service and
		// monitor paths took the first, so one pod got two answers for one
		// name in one response (containerPortByName's doc has the whole rule).
		if p, ok := containerPortByName(pod, entry); ok {
			add(p)
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
		n, numeric := parsePort(entry)
		for _, sp := range svc.Ports {
			if sp.Name == entry || (numeric && sp.Port == n) {
				out = append(out, sp)
			}
		}
	}
	return out
}

// TargetPodPort translates a service port to the pod port it targets — the ONE
// answer both callers of a Service port get: the service-annotation path
// (ServiceTargets) and a ServiceMonitor endpoint naming that port
// (monitorPodPort). A named targetPort goes through containerPortByName, whose
// doc carries the first-declaration rule and why it is the whole stack's.
func TargetPodPort(pod kubemeta.Pod, sp services.Port) (int32, bool) {
	if sp.TargetPortName != "" {
		return containerPortByName(pod, sp.TargetPortName)
	}
	if sp.TargetPortNum != 0 {
		return sp.TargetPortNum, true
	}
	return sp.Port, true
}

// splitList delegates to the one comma-list reader both binaries share.
func splitList(s string) []string {
	return cli.SplitList(s)
}
