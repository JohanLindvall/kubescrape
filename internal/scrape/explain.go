package scrape

// The explain half of the package: per-decision diagnostics for the
// GET /v1/explain/{ns}/{pod} endpoint. Each function here mirrors one of the
// target-derivation functions in targets.go and MUST give the same verdict —
// they live in this package precisely so the explanation and the derivation
// read the same parsers (parsePort, containerPortByName, TargetPodPort) and
// cannot drift into explaining a decision the server does not make.

import (
	"fmt"
	"strconv"

	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// PortVerdict explains one port entry — an entry of a prometheus.io/port
// annotation, a declared container port standing in for a missing annotation,
// or a Service port — and what pod ports it resolves to. An empty Ports means
// the entry yields no target; Note says why (or carries a caveat on a
// resolving entry).
type PortVerdict struct {
	Entry string  `json:"entry"`
	Ports []int32 `json:"ports,omitempty"`
	Note  string  `json:"note,omitempty"`
}

// ScrapeableReasons returns why a pod cannot yield scrape targets — one
// reason per failed condition of Scrapeable, empty when it is scrapeable.
func ScrapeableReasons(pod kubemeta.Pod) []string {
	var reasons []string
	if pod.PodIP == "" {
		reasons = append(reasons, "pod has no IP (not scheduled/started yet, or hostNetwork without a reported address)")
	}
	if pod.DeletedAt != nil {
		reasons = append(reasons, "pod is deleted (resolvable via its tombstone, but never a target)")
	}
	if pod.DeletionTimestamp != nil {
		reasons = append(reasons, "pod is terminating (deletionTimestamp set; it stays phase Running for its whole grace period, but scraping it is up=0 churn)")
	}
	if kubemeta.FinishedPhase(pod.Phase) {
		reasons = append(reasons, fmt.Sprintf("pod phase %s is finished", pod.Phase))
	}
	return reasons
}

// DeclaredPort is one containerPort declaration, for the explain response.
type DeclaredPort struct {
	Container string `json:"container"`
	Name      string `json:"name,omitempty"`
	Port      int32  `json:"port"`
}

// DeclaredPorts lists every containerPort the pod's containers declare.
func DeclaredPorts(pod kubemeta.Pod) []DeclaredPort {
	var out []DeclaredPort
	for _, c := range pod.Containers {
		for _, p := range c.Ports {
			out = append(out, DeclaredPort{Container: c.Name, Name: p.Name, Port: p.Port})
		}
	}
	return out
}

// ExplainPodPorts explains what podPorts does with this pod: one verdict per
// annotation entry, or — with no (non-blank) annotation — one per declared
// container port. annotated reports which of the two shapes applied.
func ExplainPodPorts(pod kubemeta.Pod) (verdicts []PortVerdict, annotated bool) {
	ann, ok := pod.Annotations[AnnotationPort]
	if !ok || len(splitList(ann)) == 0 {
		for _, dp := range DeclaredPorts(pod) {
			entry := strconv.Itoa(int(dp.Port))
			if dp.Name != "" {
				entry = dp.Name
			}
			verdicts = append(verdicts, PortVerdict{
				Entry: entry,
				Ports: []int32{dp.Port},
				Note:  "declared container port (no prometheus.io/port annotation; every declared port is a target)",
			})
		}
		return verdicts, false
	}
	for _, entry := range splitList(ann) {
		verdicts = append(verdicts, explainPodPortEntry(pod, entry))
	}
	return verdicts, true
}

// explainPodPortEntry is podPorts' per-entry logic with its verdict spelled
// out — including the caveat the user cannot see from the target list alone:
// a NUMERIC entry resolves whether or not any container declares the port.
func explainPodPortEntry(pod kubemeta.Pod, entry string) PortVerdict {
	if n, ok := parsePort(entry); ok {
		v := PortVerdict{Entry: entry, Ports: []int32{n}}
		if !containerDeclaresNumber(pod, n) {
			v.Note = fmt.Sprintf("no container declares port %d — the target is still served (containerPort declarations are optional), but if nothing listens there the scrape will fail", n)
		}
		return v
	}
	if allDigits(entry) {
		return PortVerdict{Entry: entry, Note: "not a valid port number (1-65535), and an all-digit string can never name a declared port; the entry resolves to nothing"}
	}
	var ports []int32
	for _, c := range pod.Containers {
		for _, p := range c.Ports {
			if p.Name == entry {
				ports = append(ports, p.Port)
			}
		}
	}
	if len(ports) == 0 {
		return PortVerdict{Entry: entry, Note: fmt.Sprintf("no container declares a port named %q; the entry resolves to nothing", entry)}
	}
	return PortVerdict{Entry: entry, Ports: ports}
}

// ExplainServicePorts explains what ServiceTargets does with this pod behind
// this service: which service ports its annotation selects and what pod port
// each translates to. annotated reports whether a port annotation narrowed
// the selection.
func ExplainServicePorts(pod kubemeta.Pod, svc *services.Service) (verdicts []PortVerdict, annotated bool) {
	ann, hasAnn := svc.Annotations[AnnotationPort]
	entries := splitList(ann)
	annotated = hasAnn && len(entries) > 0
	if annotated {
		// Per annotation entry: does it name a service port at all?
		for _, entry := range entries {
			n, numeric := parsePort(entry)
			matched := false
			for _, sp := range svc.Ports {
				if sp.Name == entry || (numeric && sp.Port == n) {
					matched = true
					verdicts = append(verdicts, servicePortVerdict(pod, entry, sp))
				}
			}
			if !matched {
				verdicts = append(verdicts, PortVerdict{Entry: entry, Note: "no service port has this name or number; the entry resolves to nothing"})
			}
		}
		return verdicts, true
	}
	for _, sp := range svc.Ports {
		entry := strconv.Itoa(int(sp.Port))
		if sp.Name != "" {
			entry = sp.Name
		}
		verdicts = append(verdicts, servicePortVerdict(pod, entry, sp))
	}
	return verdicts, false
}

// servicePortVerdict explains one selected service port's translation to a
// pod port (TargetPodPort's decision).
func servicePortVerdict(pod kubemeta.Pod, entry string, sp services.Port) PortVerdict {
	port, ok := TargetPodPort(pod, sp)
	if !ok {
		return PortVerdict{
			Entry: entry,
			Note:  fmt.Sprintf("service port %d targets container port name %q, which no container of this pod declares; it resolves to nothing", sp.Port, sp.TargetPortName),
		}
	}
	v := PortVerdict{Entry: entry, Ports: []int32{port}}
	if sp.TargetPortName == "" && !containerDeclaresNumber(pod, port) {
		v.Note = fmt.Sprintf("no container declares port %d — the target is still served, but if nothing listens there the scrape will fail", port)
	}
	return v
}

// containerDeclaresNumber reports whether any container declares this port
// number. Purely informational: numeric resolution never requires it.
func containerDeclaresNumber(pod kubemeta.Pod, n int32) bool {
	for _, c := range pod.Containers {
		for _, p := range c.Ports {
			if p.Port == n {
				return true
			}
		}
	}
	return false
}

func allDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return len(s) > 0
}
