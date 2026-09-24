// Package scrape derives Prometheus scrape targets from pod and Service
// metadata. It is pure functions over kubemeta, services and servicemonitors
// values — no store, no locks, no logging — which is what lets /v1/explain
// replay the derivation exactly. It covers:
//
//   - the conventional prometheus.io/* annotations on pods and Services
//     (PodTargets, ServiceTargets, ServiceDoor), port names resolved one way
//     on every path;
//   - ServiceMonitor and PodMonitor endpoint resolution (MonitorTargets,
//     PodMonitorTargets and their URL-only pre-checks), stamping each
//     endpoint's auth/TLS refs, cadence and metricRelabelings on the target;
//   - the fold that serves two monitors resolving to one URL as ONE target
//     (MergeMonitorEndpoint, merge.go), with its merged-chain and contributor
//     ceilings;
//   - per-pod byte accounting against MaxTargetBytesPerPod (docsize.go);
//   - the exported-identity collision scan (InstanceScan, instance.go);
//   - the /v1/explain mirrors and the notes shared with the derivation
//     (explain.go), kept beside the parsers they mirror so explanation and
//     derivation cannot drift.
//
// The per-pod ceilings are defined and measured here but ENFORCED where
// targets are accumulated, in internal/server's targetDedup.
package scrape

import (
	"iter"
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

// OptedIn reports whether a pod's or a Service's annotations opt it into
// scraping: prometheus.io/scrape="true", exactly. The ONE spelling of the
// opt-in — the doors here, nodeTargets' pre-check and /v1/explain's head all
// ask it — so no reader can come to accept a value another refuses.
func OptedIn(annotations map[string]string) bool {
	return annotations[AnnotationScrape] == "true"
}

// portAnnotation reads a door's prometheus.io/port annotation and whether it is
// EXPLICIT: present and not all-blank. An absent or all-blank annotation falls
// back (every declared container port on the pod door, every service port on
// the Service door); an explicit one never does, even one whose entries all
// split away (","), which selects nothing. The ONE fallback predicate: the
// derivation (podPorts, selectServicePorts) and its explain mirrors
// (ExplainPodPorts, ExplainServicePorts) all ask it, so the two cannot disagree
// about which shape a door is in.
func portAnnotation(annotations map[string]string) (raw string, explicit bool) {
	raw, ok := annotations[AnnotationPort]
	return raw, ok && strings.TrimSpace(raw) != ""
}

// MaxPortsPerPod bounds how many scrape targets ONE POD may produce, across
// every door that produces them. Every ScrapeTarget embeds the WHOLE pod
// document by value (kubemeta.ScrapeTarget.Pod), so N targets carry N copies of
// the pod's annotations; a tenant who can annotate a pod in their own namespace
// — or author a ServiceMonitor with many endpoints, which needs no annotation
// on the pod at all — can otherwise make the singleton metadata service marshal
// an O(N²) /v1/nodes/{node}/targets body and OOM, taking target discovery for
// the whole fleet with it.
//
// It is ENFORCED where targets are ACCUMULATED (server.targetDedup.add), not
// here. That is the correction of an earlier attempt that capped only the two
// doors below: a ServiceMonitor endpoint list bypassed them entirely and
// reopened the same O(N²) response (measured 20.8 MiB at 1024 endpoints, and it
// multiplies per pod the Service selects). The doors below still bound how many
// PORTS one annotation resolves, which keeps the intermediate slices small, but
// the ceiling that matters is the one on the accumulated output.
//
// A refused target is NOT scraped, so it is counted (obs.ScrapeTargetsCapped)
// and named by /v1/explain rather than dropped silently. A real workload
// scrapes a handful of ports; this is far above any legitimate use.
//
// It bounds the COUNT against a per-target cost it MODELS as "the ~2 KiB pod
// document" — and a model is not a bound. MaxTargetBytesPerPod (docsize.go) is
// the sibling that measures the document instead of assuming it, applied at the
// same seam and counted into the same counter; the two refusals are told apart
// by the operator through cappedTargetsBySize and SizeCeilingNote. Neither
// subsumes the other: this one is what keeps a pod with a tiny document from
// producing eight hundred targets inside that budget.
const MaxPortsPerPod = 16

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
	if !OptedIn(pod.Annotations) || !Scrapeable(pod) {
		return nil
	}
	scheme, path, ok := schemeAndPath(pod.Annotations)
	if !ok {
		return nil
	}
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
	if svc == nil || !Scrapeable(pod) {
		return nil
	}
	door := NewServiceDoor(svc)
	return door.Targets(pod)
}

// ServiceDoor is ServiceTargets' POD-INDEPENDENT half, resolved once: whether
// the Service opts in at all, its scheme and path, which of its ports the
// port annotation selects, and the Service view every target carries.
//
// It exists because none of that depends on the pod, while nodeTargets asks
// the question once per (pod, matched Service): a Service's prometheus.io/port
// list is tenant-authored and up to kubemeta.MaxAnnotationValueBytes long
// (~4,000 entries), and re-resolving it for every pod behind the Service turned
// one annotation into a per-pod cost — measured at 110 pods, ~70-90 ms and
// 75 MB per derivation for an `80,80,...` list that yields one target. The
// server memoises one door per Service per derivation; ServiceTargets is the
// composition, so every other caller is unchanged.
//
// A door is a value and treat-as-immutable once built: its Service view is
// SHARED by every target it produces, on every pod, exactly as one call of
// ServiceTargets always shared it across that call's targets.
type ServiceDoor struct {
	open   bool
	scheme string
	path   string
	ports  []services.Port
	info   *kubemeta.Service
}

// NewServiceDoor resolves a Service's annotation door. A Service that is not
// annotated prometheus.io/scrape="true", or whose path annotation is over
// MaxTargetPathBytes, yields a door that produces nothing — and costs nothing
// to build.
func NewServiceDoor(svc *services.Service) ServiceDoor {
	if svc == nil || !OptedIn(svc.Annotations) {
		return ServiceDoor{}
	}
	scheme, path, ok := schemeAndPath(svc.Annotations)
	if !ok {
		return ServiceDoor{}
	}
	return ServiceDoor{
		open:   true,
		scheme: scheme,
		path:   path,
		ports:  selectServicePorts(svc),
		info:   serviceInfo(svc),
	}
}

// Targets derives the door's targets on one pod: ServiceTargets(pod, svc)
// exactly, without re-resolving anything that does not depend on the pod.
func (d *ServiceDoor) Targets(pod kubemeta.Pod) []kubemeta.ScrapeTarget {
	if !d.open || !Scrapeable(pod) {
		return nil
	}
	var targets []kubemeta.ScrapeTarget
	var seen map[int32]struct{}
	for _, sp := range d.ports {
		if len(targets) >= MaxPortsPerPod {
			break // anti-abuse: every target embeds the whole pod (MaxPortsPerPod)
		}
		port, ok := targetPodPort(pod, sp)
		if !ok {
			continue
		}
		if _, dup := seen[port]; dup {
			continue
		}
		if seen == nil {
			seen = make(map[int32]struct{}, len(d.ports))
		}
		seen[port] = struct{}{}
		t := makeTarget(pod, d.scheme, d.path, port)
		t.Source = "service"
		t.Service = d.info
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
	// A REFUSED endpoint yields no target on any pod: one of its strings was
	// over servicemonitors' ceilings, and none of them has a safe shorter form
	// (see servicemonitors.Endpoint.Refused). Checked before the port so the
	// refusal is what an operator is told about, not a downstream symptom of
	// the blanked port.
	if svc == nil || ep.Refused != "" || !Scrapeable(pod) {
		return "", "", 0, false
	}
	port, ok = monitorPodPort(pod, svc, ep)
	if !ok {
		return "", "", 0, false
	}
	scheme, path, ok = defaultSchemePath(monitorScheme(ep.Scheme), ep.Path)
	if !ok {
		return "", "", 0, false
	}
	return scheme, path, port, true
}

// monitorScheme folds a monitor endpoint's `scheme` to the spelling
// defaultSchemePath tests for. prometheus-operator's CRD admits
// `http`/`https`/`HTTP`/`HTTPS` (its own SchemeHTTPS constant is the upper-case
// one, and its field doc says "Supported values are HTTP and HTTPS"), and
// defaultSchemePath maps anything but the exact string "https" to plaintext —
// so an upper-case `HTTPS` was served as http:// with up=0 and nothing pointing
// at the scheme, tlsConfig silently unused, and any bearer, basicAuth or
// authorization credential sent in cleartext on the pod network.
//
// ONLY the monitor doors fold: the annotation door's prometheus.io/scheme is
// documented lower-case, matching Prometheus' classic relabel regex. And it
// never copies the tenant's string — the length gate keeps EqualFold from
// walking an arbitrarily long value, and the result is one of the two
// constants defaultSchemePath emits anyway (which is why Scheme needs no field
// ceiling of its own).
func monitorScheme(scheme string) string {
	if len(scheme) == len("https") && strings.EqualFold(scheme, "https") {
		return "https"
	}
	return scheme
}

// stampEndpoint copies the endpoint's auth/TLS/relabeling declarations onto
// a target.
func stampEndpoint(t *kubemeta.ScrapeTarget, ep servicemonitors.Endpoint) {
	// ONE assignment for the whole auth/TLS group: the endpoint carries the
	// very kubemeta.ScrapeAuth the target does, so a new auth field cannot be
	// parsed and then forgotten here.
	t.ScrapeAuth = ep.ScrapeAuth
	t.Interval = ep.Interval
	t.ScrapeTimeout = ep.ScrapeTimeout
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
	// See monitorEndpoint: a refused endpoint resolves to nothing.
	if ep.Refused != "" || !Scrapeable(pod) {
		return "", "", 0, false
	}
	port, ok = podMonitorPodPort(pod, ep)
	if !ok {
		return "", "", 0, false
	}
	scheme, path, ok = defaultSchemePath(monitorScheme(ep.Scheme), ep.Path)
	if !ok {
		return "", "", 0, false
	}
	return scheme, path, port, true
}

// podMonitorPodPort resolves the pod port a PodMonitor endpoint targets — the
// port half of podMonitorEndpoint, split out (like monitorPodPort, its
// ServiceMonitor sibling) so the explain note can ask the port question on a
// pod the derivation short-circuits on before it ever reaches the port.
func podMonitorPodPort(pod kubemeta.Pod, ep servicemonitors.Endpoint) (int32, bool) {
	if ep.Port == "" && ep.TargetPort == nil {
		return 0, false // same phantom-target guard as ServiceMonitors
	}
	if ep.Port != "" {
		return containerPortByName(pod, ep.Port)
	}
	return targetPortOnPod(pod, *ep.TargetPort)
}

// targetPortOnPod resolves a monitor endpoint's targetPort against a pod: a
// number (bounds-checked by monitorPortNumber, which also reads a numeric
// STRING), else a container-port NAME through containerPortByName. It is the
// ONE spelling of that question for both monitor kinds — the ServiceMonitor
// fallback (monitorPodPort) and the PodMonitor one (podMonitorPodPort) — which
// open-coded it twice and agreed only through a contract between two
// functions: the PodMonitor copy had no Type check and leaned on
// containerPortByName's empty-name guard alone.
//
// Only a String-typed targetPort may resolve by name: an Int-typed value
// always has StrVal == "" (a rejected number like 0 or 70000 names nothing).
// containerPortByName still carries the other half of the guard, for a
// String-typed `targetPort: ""`.
func targetPortOnPod(pod kubemeta.Pod, tp intstr.IntOrString) (int32, bool) {
	// IntValue() on a string-typed value Atoi's it ignoring the error and
	// returns a full int, so parse and bound explicitly: "4294967297" must be
	// rejected, not truncated to port 1.
	if n, ok := monitorPortNumber(tp); ok {
		return n, true
	}
	if tp.Type != intstr.String {
		return 0, false
	}
	return containerPortByName(pod, tp.StrVal)
}

// containerPortByName resolves a container-port NAME to a pod port: the first
// declaration on a REGULAR container, and only if none carries the name, the
// first on an init/sidecar or ephemeral container.
//
// It is the ONE resolver for that question, and every path in this package
// that asks it goes through here — the pod annotation's named entry
// (podPorts), a Service port's named targetPort (targetPodPort, hence
// ServiceTargets), a ServiceMonitor endpoint's targetPort and a PodMonitor
// endpoint's port/targetPort (targetPortOnPod, podMonitorPodPort). One function
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
// as one: it is the phantom-target guard for a degenerate String-typed
// `targetPort: ""`, which targetPortOnPod (both monitor kinds) passes straight
// here — its own Type check refuses only an Int-typed value. Without it such an
// endpoint would match the first UNNAMED container port by "" == "" and mint a
// scrape target the user never declared.
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

// monitorPortNumber extracts a numeric targetPort, bounds-checked to the
// valid port range; string values that do not parse fall through to the
// port-name path.
func monitorPortNumber(tp intstr.IntOrString) (int32, bool) {
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
//
// It accepts EXACTLY what strconv.ParseInt(entry, 10, 32) followed by that
// range check accepts — one optional sign, then ASCII digits, leading zeros
// included ("+80", "000080") — and is hand-written rather than a call for one
// reason: ParseInt's error path allocates a *NumError and a copy of the input,
// and most entries it sees are REJECTED ones. Every named entry ("metrics") on
// the ordinary path paid that per derivation, and a tenant-authored list of
// ~4,000 non-numeric entries paid it ~4,000 times per (pod, Service).
// TestParsePortAgreesWithStrconv and FuzzParsePortAgreesWithStrconv pin the
// accepted set against ParseInt itself.
func parsePort(entry string) (int32, bool) {
	s := entry
	if s != "" && (s[0] == '+' || s[0] == '-') {
		if s[0] == '-' {
			// Every value ParseInt reads after a minus is <= 0: out of range.
			return 0, false
		}
		s = s[1:]
	}
	if s == "" {
		return 0, false
	}
	var n int32
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		// Past 65535 the entry is rejected whatever follows — out of range if
		// the rest are digits, a syntax error if not — so stop there, which
		// also keeps n far from int32 overflow.
		if n = n*10 + int32(c-'0'); n > 65535 {
			return 0, false
		}
	}
	if n < 1 {
		return 0, false
	}
	return n, true
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
				return targetPodPort(pod, sp)
			}
		}
		return 0, false
	}
	// Port was empty, so TargetPort is non-nil (the guard above returned for
	// the neither-set case).
	return targetPortOnPod(pod, *ep.TargetPort)
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
	return unscrapeableReasons(&pod) == 0
}

// unscrapeable is the set of Scrapeable's conditions a pod fails. The
// conditions are defined HERE and nowhere else: the derivation's yes/no
// (Scrapeable) and /v1/explain's list of reasons (ScrapeableReasons) are two
// readings of this one check, so explain's head cannot say a pod is scrapeable
// while nodeTargets serves it nothing, or the reverse — which a second if-chain
// shaped like this one could, the moment a condition was added to only one of
// them. TestScrapeableAndItsReasonsAreOneCheck pins the two readings together
// over every combination.
type unscrapeable uint8

const (
	unscrapeableNoIP        unscrapeable = 1 << iota // no pod IP yet
	unscrapeableDeleted                              // tombstoned
	unscrapeableTerminating                          // deletionTimestamp set
	unscrapeableFinished                             // Succeeded or Failed
)

// unscrapeableReasons evaluates every condition (no short circuit: the answer
// is the whole set, and each is a field compare). Allocation-free, on the
// per-pod path of every target derivation.
func unscrapeableReasons(pod *kubemeta.Pod) unscrapeable {
	var u unscrapeable
	if pod.PodIP == "" {
		u |= unscrapeableNoIP
	}
	if pod.DeletedAt != nil {
		u |= unscrapeableDeleted
	}
	if pod.DeletionTimestamp != nil {
		u |= unscrapeableTerminating
	}
	if kubemeta.FinishedPhase(pod.Phase) {
		u |= unscrapeableFinished
	}
	return u
}

// MaxTargetPathBytes bounds the scrape PATH a target may carry, at every door
// that names one. The path is copied into BOTH t.URL and t.Path of every target
// the door produces, and /v1/nodes/{node}/targets marshals every target of
// every pod on the node into one body on every agent poll — so an unbounded
// path is an unbounded body, which is the "a bound on ENTRIES is not a bound on
// BYTES" lesson MaxPortsPerPod already records: that ceiling bounds the target
// COUNT against a per-target cost it models as the ~2 KiB pod document — the
// model MaxTargetBytesPerPod now measures. This bound still earns its keep
// beside that one (docsize.go says why in full): it refuses a FIELD, with a
// diagnostic naming the annotation and its size, and it binds on a pod's first
// target, which the byte budget deliberately never refuses.
//
// The monitor door has its OWN ceiling at parse time
// (servicemonitors.enforceFieldBounds, which carries the measurement and the
// argument for refusing rather than truncating). This one covers the
// ANNOTATION door, whose path is likewise attacker-supplied by anyone who can
// annotate a pod or a Service — bounded by the API server only at the 256 KiB
// total-annotations limit, i.e. two orders of magnitude above anything a real
// path needs, and then multiplied by the targets the pod produces.
// TestMonitorPathDoorIsNoLooserThanTheAnnotationDoor pins the two together.
//
// Over the ceiling the door yields NO targets rather than a truncated or
// defaulted path: see enforceFieldBounds for why those two are the worse
// outcomes. /v1/explain reports it through the same port-entry verdicts as
// every other refusal (pathRefusedNote).
const MaxTargetPathBytes = 2 << 10

// schemeAndPath resolves a door's scheme/path annotations, reporting false when
// the path is over MaxTargetPathBytes and the door therefore yields nothing.
func schemeAndPath(annotations map[string]string) (scheme, path string, ok bool) {
	return defaultSchemePath(annotations[AnnotationScheme], annotations[AnnotationPath])
}

// defaultSchemePath applies the scrape scheme/path defaults: anything but
// "https" becomes "http", an empty path becomes "/metrics", and a path is given
// a leading slash. It reports false for a path over MaxTargetPathBytes, which
// no caller may serve.
//
// The scheme needs no ceiling: it comes out of here as one of two constants,
// so its size can never reach a target however long it was written.
func defaultSchemePath(scheme, path string) (string, string, bool) {
	if len(path) > MaxTargetPathBytes {
		return "", "", false
	}
	if scheme != "https" {
		scheme = "http"
	}
	if path == "" {
		path = "/metrics"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return scheme, path, true
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
	var buf [urlScratch]byte
	b, from, to := appendTargetURL(buf[:0], scheme, pod.PodIP, port, path)
	url := string(b)
	return kubemeta.ScrapeTarget{
		URL:    url,
		Scheme: scheme,
		// A SUBSTRING of the URL rather than a second string: it is the same
		// bytes, and the two fields are born and die together in this struct,
		// so sharing the backing array pins nothing that was not already alive.
		// (The usual hazard of slicing a Go string — a short label holding a
		// huge original alive, which internal/agent/cumagg.Retain exists for —
		// needs the two lifetimes to DIFFER, and here they cannot.)
		Address: url[from:to],
		Port:    port,
		Path:    path,
		Pod:     pod,
	}
}

// targetURL is what makeTarget would name this target, without building it.
//
// Both go through appendTargetURL: the URL is a scrape target's IDENTITY (it is
// what the server dedups on and what the agent keys a scrape by), so the cheap
// half and the full half must not be able to spell it differently.
// targeturl_test.go drives both over every resolution shape.
func targetURL(pod kubemeta.Pod, scheme, path string, port int32) string {
	var buf [urlScratch]byte
	b, _, _ := appendTargetURL(buf[:0], scheme, pod.PodIP, port, path)
	return string(b)
}

// urlScratch is the stack buffer appendTargetURL builds into: a scheme, "://",
// a bracketed IPv6 literal, a port and a short path all fit, so the rendered
// URL costs ONE allocation (the string) instead of the three the obvious
// spelling costs — net.JoinHostPort, strconv.Itoa and the concatenation.
//
// That is not micro-optimisation for its own sake: resolving a URL is the
// MARGINAL cost of one more monitor endpoint resolving to a pod, which is the
// whole point of the URL pre-check in the server's merge, and an alloc profile
// of 100 colliding monitors found renderURL + net.JoinHostPort + FormatInt at
// 78% of every object the derivation allocated. A path longer than this simply
// grows onto the heap and is still correct.
const urlScratch = 128

// appendTargetURL appends scheme://host:port + path to dst, reporting where the
// host:port substring begins and ends within the result.
//
// The host:port half is net.JoinHostPort's spelling, restated here rather than
// called, because JoinHostPort takes the port as a STRING and so forces an
// strconv.Itoa allocation that this can append in place. Its whole rule is the
// IPv6 bracket, which is the one line below.
func appendTargetURL(dst []byte, scheme, ip string, port int32, path string) (out []byte, from, to int) {
	dst = append(dst, scheme...)
	dst = append(dst, "://"...)
	from = len(dst)
	if strings.IndexByte(ip, ':') >= 0 {
		dst = append(dst, '[')
		dst = append(dst, ip...)
		dst = append(dst, ']')
	} else {
		dst = append(dst, ip...)
	}
	dst = append(dst, ':')
	dst = strconv.AppendInt(dst, int64(port), 10)
	to = len(dst)
	return append(dst, path...), from, to
}

// podPorts resolves the pod's port annotation (each entry a number or a
// named container port); without an annotation, all declared container
// ports. Entries that resolve to nothing are skipped.
func podPorts(pod kubemeta.Pod) []int32 {
	var ports []int32
	seen := make(map[int32]struct{})
	// capped bounds the target count per pod (MaxPortsPerPod): every target
	// embeds the whole pod, so an unbounded port list is a quadratic response.
	add := func(p int32) {
		if _, ok := seen[p]; ok || p < 1 || p > 65535 || len(ports) >= MaxPortsPerPod {
			return
		}
		seen[p] = struct{}{}
		ports = append(ports, p)
	}

	ann, explicit := portAnnotation(pod.Annotations)
	if !explicit {
		for _, c := range pod.Containers {
			for _, p := range c.Ports {
				add(p.Port)
			}
		}
		return ports
	}
	for entry := range listEntries(ann) {
		if len(ports) >= MaxPortsPerPod {
			break // the cap is reached; stop parsing a hostile-length list
		}
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
//
// Each service port is selected AT MOST ONCE, in the order the entries first
// name it (and, within one entry, in the Service's own port order). That is
// not a change of answer: ServiceTargets dedupes by resolved pod port in
// selection order, and a repeated service port always resolves to the pod port
// its first selection already claimed, so a repeat never produced a target.
// What it did produce was cost — the output was entries x matching ports long,
// and the only bound on the loop consuming it counts TARGETS, which repeats
// never add — so an 8 KiB `8,8,...,8` list cost ~3 MB and milliseconds per
// (pod, Service) to yield one target. Now the output is bounded by the
// Service's port count, the walk stops once every port is selected, and the
// entry-to-port match goes through servicePortIndex, so a long list against a
// many-ported Service is O(entries + ports) rather than their product.
// TestServicePortSelectionMatchesTheNestedLoop pins the order against the
// original nested loop over randomised inputs.
func selectServicePorts(svc *services.Service) []services.Port {
	ann, explicit := portAnnotation(svc.Annotations)
	if !explicit {
		return svc.Ports
	}
	ix := newServicePortIndex(svc.Ports)
	var picked []bool
	var out []services.Port
	var buf [8]int32
	for entry := range listEntries(ann) {
		for _, i := range ix.appendMatches(buf[:0], entry) {
			if picked == nil {
				picked = make([]bool, len(svc.Ports))
			}
			if !picked[i] {
				picked[i] = true
				out = append(out, svc.Ports[i])
			}
		}
		if len(out) == len(svc.Ports) {
			break // every port is selected; the rest of the list can add nothing
		}
	}
	return out
}

// servicePortIndex answers "which of this Service's ports does annotation
// entry E name?" — a port whose Name is E, or, when E is a port number, a port
// whose Port equals it — in the Service's own port order. It is the ONE
// predicate for that question: the derivation (selectServicePorts) and its
// explain mirror (ExplainServicePorts) both ask it here, so the two cannot
// disagree about what an entry selects.
//
// A small port list is scanned linearly, which allocates nothing. Past
// indexServicePortsOver ports it is indexed — first index per name and per
// number, with a chain to the next index carrying the same key — because the
// entry list and the port list are BOTH tenant-authored, and an unindexed walk
// is their product: measured at 4,001 entries, 0.83 ms per call at one port and
// 31.8 ms at 1,000.
type servicePortIndex struct {
	ports    []services.Port
	byName   map[string]int32 // first index carrying the name; nil when scanned linearly
	byNum    map[int32]int32  // first index carrying the number
	nextName []int32          // next index with the same name, or -1
	nextNum  []int32          // next index with the same number, or -1
}

// indexServicePortsOver is the port count past which servicePortIndex builds
// its maps: below it a linear scan per entry is cheaper than the index costs to
// build, and it keeps the ordinary Service allocation-free.
const indexServicePortsOver = 16

func newServicePortIndex(ports []services.Port) servicePortIndex {
	ix := servicePortIndex{ports: ports}
	if len(ports) <= indexServicePortsOver {
		return ix
	}
	ix.byName = make(map[string]int32, len(ports))
	ix.byNum = make(map[int32]int32, len(ports))
	next := make([]int32, 2*len(ports))
	ix.nextName, ix.nextNum = next[:len(ports)], next[len(ports):]
	// Built back to front, so each map holds the FIRST index for its key and
	// every chain runs in ascending port order — the order the linear scan
	// yields.
	for i := len(ports) - 1; i >= 0; i-- {
		sp := &ports[i]
		ix.nextName[i] = -1
		if j, ok := ix.byName[sp.Name]; ok {
			ix.nextName[i] = j
		}
		ix.byName[sp.Name] = int32(i)
		ix.nextNum[i] = -1
		if j, ok := ix.byNum[sp.Port]; ok {
			ix.nextNum[i] = j
		}
		ix.byNum[sp.Port] = int32(i)
	}
	return ix
}

// appendMatches appends to dst the indexes of the ports entry names, in
// ascending order, each once. entry is never empty (listEntries and
// cli.SplitList drop blank entries), so an unnamed port cannot match by
// "" == "".
func (ix *servicePortIndex) appendMatches(dst []int32, entry string) []int32 {
	n, numeric := parsePort(entry)
	if ix.byName == nil {
		for i := range ix.ports {
			if sp := &ix.ports[i]; sp.Name == entry || (numeric && sp.Port == n) {
				dst = append(dst, int32(i))
			}
		}
		return dst
	}
	a, b := int32(-1), int32(-1)
	if j, ok := ix.byName[entry]; ok {
		a = j
	}
	if numeric {
		if j, ok := ix.byNum[n]; ok {
			b = j
		}
	}
	// Merge the two ascending chains: a port both a name and a number select
	// is ONE match, exactly as the linear scan's `||` makes it.
	for a >= 0 || b >= 0 {
		switch {
		case b < 0 || (a >= 0 && a < b):
			dst = append(dst, a)
			a = ix.nextName[a]
		case a < 0 || b < a:
			dst = append(dst, b)
			b = ix.nextNum[b]
		default:
			dst = append(dst, a)
			a, b = ix.nextName[a], ix.nextNum[b]
		}
	}
	return dst
}

// targetPodPort translates a service port to the pod port it targets — the ONE
// answer both callers of a Service port get: the service-annotation path
// (ServiceTargets) and a ServiceMonitor endpoint naming that port
// (monitorPodPort). A named targetPort goes through containerPortByName, whose
// doc carries the first-declaration rule and why it is the whole stack's.
func targetPodPort(pod kubemeta.Pod, sp services.Port) (int32, bool) {
	if sp.TargetPortName != "" {
		return containerPortByName(pod, sp.TargetPortName)
	}
	if sp.TargetPortNum != 0 {
		return sp.TargetPortNum, true
	}
	return sp.Port, true
}

// listEntries yields exactly cli.SplitList(s)'s entries — split on commas,
// trimmed, blanks dropped — without materialising the list. The derivation
// reads a tenant-authored port annotation of up to
// kubemeta.MaxAnnotationValueBytes per (pod, Service) and usually stops early
// (at the ceiling, or once every port is selected), so building a ~4,000
// element slice first was the whole cost of the walk it then abandoned.
// TestListEntriesMatchesSplitList holds the two readers equal.
func listEntries(s string) iter.Seq[string] {
	return func(yield func(string) bool) {
		for part := range strings.SplitSeq(s, ",") {
			if part = strings.TrimSpace(part); part != "" && !yield(part) {
				return
			}
		}
	}
}
