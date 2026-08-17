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
	"strings"

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

// portFilter mirrors podPorts' `add`: a pod port becomes a target ONCE, and
// only inside 1..65535. It is threaded across every verdict of one pod rather
// than applied per entry, because podPorts dedupes across the WHOLE resolution
// — two containers declaring the same number, `prometheus.io/port: "8080,8080"`
// or a name and a number naming one port each yield ONE target, and explain
// over-reported them as two. The mirrored `add` is the reason this file exists
// (the package header's "cannot drift"); it was the one part not mirrored.
type portFilter struct {
	seen map[int32]struct{}
	// kept counts the ports that became targets for this pod, so the mirror
	// can report the per-pod ceiling the same way the derivation applies it.
	// Without this, explain reported a capped pod's ports as WORKING targets —
	// inverting the one question it exists to answer ("why is my 17th port not
	// scraped?" answered with "it is") and drifting from the derivation the
	// package header says it cannot drift from.
	kept int
}

// keep splits the ports one entry matched into those that actually become
// targets and a note explaining the rest.
func (f *portFilter) keep(ports []int32) (kept []int32, note string) {
	if f.seen == nil {
		f.seen = map[int32]struct{}{}
	}
	var dup, bad, capped []string
	for _, p := range ports {
		_, seen := f.seen[p]
		switch {
		case p < 1 || p > 65535:
			bad = append(bad, strconv.Itoa(int(p)))
		case seen:
			dup = append(dup, strconv.Itoa(int(p)))
		case f.kept >= MaxPortsPerPod:
			capped = append(capped, strconv.Itoa(int(p)))
		default:
			f.seen[p] = struct{}{}
			f.kept++
			kept = append(kept, p)
		}
	}
	var notes []string
	if len(dup) > 0 {
		notes = append(notes, fmt.Sprintf("port %s already resolved through an earlier entry or declaration; this one adds no target", strings.Join(dup, ", ")))
	}
	if len(bad) > 0 {
		notes = append(notes, fmt.Sprintf("port %s is outside 1-65535 and resolves to nothing", strings.Join(bad, ", ")))
	}
	if len(capped) > 0 {
		notes = append(notes, fmt.Sprintf("port %s is over the per-pod ceiling of %d targets and is NOT scraped", strings.Join(capped, ", "), MaxPortsPerPod))
	}
	return kept, strings.Join(notes, "; ")
}

// ExplainPodPorts explains what podPorts does with this pod: one verdict per
// annotation entry, or — with no (non-blank) annotation — one per declared
// container port. annotated reports which of the two shapes applied.
func ExplainPodPorts(pod kubemeta.Pod) (verdicts []PortVerdict, annotated bool) {
	filter := &portFilter{}
	// The fallback predicate must be EXACTLY podPorts': absent or all-blank
	// falls back to declared ports; a present annotation never does, even one
	// whose entries all split away (","), which selects nothing.
	ann, ok := pod.Annotations[AnnotationPort]
	if !ok || strings.TrimSpace(ann) == "" {
		for _, dp := range DeclaredPorts(pod) {
			entry := strconv.Itoa(int(dp.Port))
			if dp.Name != "" {
				entry = dp.Name
			}
			ports, note := filter.keep([]int32{dp.Port})
			if note == "" {
				note = "declared container port (no prometheus.io/port annotation; every declared port is a target)"
			}
			verdicts = append(verdicts, PortVerdict{Entry: entry, Ports: ports, Note: note})
		}
		return verdicts, false
	}
	entries := splitList(ann)
	if len(entries) == 0 {
		return []PortVerdict{{
			Entry: ann,
			Note:  "annotation is present but contains no entries (commas and whitespace only); a present annotation never falls back to the declared container ports, so it resolves to nothing",
		}}, true
	}
	for _, entry := range entries {
		verdicts = append(verdicts, explainPodPortEntry(pod, entry, filter))
	}
	return verdicts, true
}

// explainPodPortEntry is podPorts' per-entry logic with its verdict spelled
// out — including the caveat the user cannot see from the target list alone:
// a NUMERIC entry resolves whether or not any container declares the port.
func explainPodPortEntry(pod kubemeta.Pod, entry string, filter *portFilter) PortVerdict {
	if n, ok := parsePort(entry); ok {
		ports, note := filter.keep([]int32{n})
		v := PortVerdict{Entry: entry, Ports: ports, Note: note}
		if note == "" && !containerDeclaresNumber(pod, n) {
			v.Note = fmt.Sprintf("no container declares port %d — the target is still served (containerPort declarations are optional), but if nothing listens there the scrape will fail", n)
		}
		return v
	}
	if allDigits(entry) {
		return PortVerdict{Entry: entry, Note: "not a valid port number (1-65535), and an all-digit string can never name a declared port; the entry resolves to nothing"}
	}
	// Through containerPortByName, the derivation's own resolver: mirroring it
	// with a second walk is how explain came to affirm every declaration of a
	// duplicated name while podPorts served the first.
	port, found := containerPortByName(pod, entry)
	if !found {
		return PortVerdict{Entry: entry, Note: fmt.Sprintf("no container declares a port named %q; the entry resolves to nothing", entry)}
	}
	ports, note := filter.keep([]int32{port})
	if note == "" {
		note = duplicateNameNote(pod, entry, port, podAnnotationRemedy)
	}
	return PortVerdict{Entry: entry, Ports: ports, Note: note}
}

// ExplainServicePorts explains what ServiceTargets does with this pod behind
// this service: which service ports its annotation selects and what pod port
// each translates to. annotated reports whether a port annotation narrowed
// the selection.
func ExplainServicePorts(pod kubemeta.Pod, svc *services.Service) (verdicts []PortVerdict, annotated bool) {
	filter := servicePortFilter{}
	// The predicate must be EXACTLY selectServicePorts': absent or all-blank
	// selects every service port; a present annotation never falls back, even
	// one whose entries all split away (","), which selects nothing.
	ann, hasAnn := svc.Annotations[AnnotationPort]
	annotated = hasAnn && strings.TrimSpace(ann) != ""
	if annotated {
		entries := splitList(ann)
		if len(entries) == 0 {
			return []PortVerdict{{
				Entry: ann,
				Note:  "annotation is present but contains no entries (commas and whitespace only); a present annotation never falls back to the declared service ports, so it selects nothing",
			}}, true
		}
		// Per annotation entry: does it name a service port at all?
		for _, entry := range entries {
			n, numeric := parsePort(entry)
			matched := false
			for _, sp := range svc.Ports {
				if sp.Name == entry || (numeric && sp.Port == n) {
					matched = true
					verdicts = append(verdicts, servicePortVerdict(pod, entry, sp, filter))
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
		verdicts = append(verdicts, servicePortVerdict(pod, entry, sp, filter))
	}
	return verdicts, false
}

// servicePortFilter mirrors ServiceTargets' `seen`: one Service yields ONE
// target per resolved pod port, however many of its service ports translate
// to it. Keyed by the resolved pod port like the derivation's map, threaded
// across the whole resolution in selectServicePorts' order (annotation
// entries or the all-ports fallback) — two service ports sharing a
// targetPort, or two entries naming one service port, yield one target, and
// explain over-reported them as two. The value remembers the claiming
// SERVICE port for the note. portFilter is deliberately not reused: podPorts'
// `add` also range-checks, ServiceTargets does not, and that extra branch
// would explain away a target the derivation serves.
type servicePortFilter map[int32]int32

// claim registers a resolved pod port, or says why a service port resolving
// to an already-claimed one adds no target.
func (f servicePortFilter) claim(podPort, svcPort int32) (note string, dup bool) {
	if claimer, taken := f[podPort]; taken {
		return fmt.Sprintf("resolves to pod port %d, already claimed by service port %d; this entry adds no target", podPort, claimer), true
	}
	f[podPort] = svcPort
	return "", false
}

// servicePortVerdict explains one selected service port's translation to a
// pod port (TargetPodPort's decision, then ServiceTargets' seen-port dedup).
func servicePortVerdict(pod kubemeta.Pod, entry string, sp services.Port, filter servicePortFilter) PortVerdict {
	port, ok := TargetPodPort(pod, sp)
	if !ok {
		return PortVerdict{
			Entry: entry,
			Note:  fmt.Sprintf("service port %d targets container port name %q, which no container of this pod declares; it resolves to nothing", sp.Port, sp.TargetPortName),
		}
	}
	if note, dup := filter.claim(port, sp.Port); dup {
		return PortVerdict{Entry: entry, Note: note}
	}
	v := PortVerdict{Entry: entry, Ports: []int32{port}}
	switch {
	case sp.TargetPortName == "" && !containerDeclaresNumber(pod, port):
		v.Note = fmt.Sprintf("no container declares port %d — the target is still served, but if nothing listens there the scrape will fail", port)
	case sp.TargetPortName != "":
		v.Note = duplicateNameNote(pod, sp.TargetPortName, port, servicePortRemedy)
	}
	return v
}

// nameRemedy identifies the path a duplicate-name note is attached to, which
// is what decides the REMEDY: the other declarations are reached differently
// depending on who resolved the name, and a note offering the wrong one sends
// an operator to edit a field that cannot change the outcome.
type nameRemedy int

const (
	// podAnnotationRemedy: the pod's own prometheus.io/port named the port, so
	// the pod's annotation can name the others by number.
	podAnnotationRemedy nameRemedy = iota
	// servicePortRemedy: a SERVICE port's named targetPort resolved the name.
	// Numbers in the pod annotation are a different discovery path entirely
	// (source "pod", not "service"), and they cannot reach a second declaration
	// THROUGH this Service — the fix belongs in the Service.
	servicePortRemedy
)

// duplicateNameNote is the caveat a duplicated container-port name earns on
// every path that resolves one: containerPortByName resolves ONE declaration,
// the target list shows one port, and nothing in it says the pod declares the
// name twice. Empty when the name is declared once, which is the normal case.
func duplicateNameNote(pod kubemeta.Pod, name string, resolved int32, remedy nameRemedy) string {
	n := containerPortDeclarations(pod, name)
	if n < 2 {
		return ""
	}
	fix := "Name the ports by number in prometheus.io/port to scrape the others"
	if remedy == servicePortRemedy {
		// Not "name them by number in the pod annotation": that would be a
		// second, pod-source target rather than this Service's, and it does not
		// exist unless the pod is itself scrape-annotated.
		fix = "A named targetPort can only ever reach this one declaration; give the Service a second port whose targetPort is another declaration's NUMBER to scrape it"
	}
	// Not "the FIRST declaration": the resolver prefers a REGULAR container's
	// over an init/sidecar or ephemeral one, so on the shape this note exists
	// for — a native sidecar declaring the app's port name — the winner is not
	// the first one the pod document lists.
	// The EndpointSlice-controller comparison holds only while a REGULAR
	// container declares the name: podutil.FindPort walks spec.Containers and
	// has no answer at all for a name only an init/sidecar or ephemeral
	// container declares, which is the case this resolver deliberately still
	// answers (see containerPortByName). Claiming the comparison there would be
	// telling an operator their sidecar port is what Kubernetes routes to.
	_, regularHasIt := portByName(pod, name, true)
	agrees := ""
	if regularHasIt {
		agrees = ", which is what a Service's named targetPort, a monitor endpoint and the EndpointSlice controller all resolve to"
	} else {
		agrees = "; no REGULAR container declares this name, so Kubernetes' own named-targetPort resolution has no answer for it and this target exists only because kubescrape falls back to the sidecar's declaration"
	}
	return fmt.Sprintf("%d containers declare a port named %q; port %d resolves — the resolver prefers a REGULAR container's declaration over an init/sidecar or ephemeral one and then takes the first%s. %s", n, name, resolved, agrees, fix)
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
