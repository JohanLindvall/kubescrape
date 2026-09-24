package server

// The target derivation's throttled warnings and their counters: monitor
// merge conflicts and ceilings, unresolved endpoints, per-pod ceilings,
// trimmed Service views and identity collisions. Only the served derivation
// raises them: /v1/explain shares the accumulator and the offer step but calls
// none of these (see targetDedup.diagnostic and explain.go's package comment).

import (
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/clip"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// reportMerge reports what a merge into a URL's holder could not honour — the
// three reportable verdicts (auth conflict, relabel chain capped, contributor
// list capped). One function for the ServiceMonitor and PodMonitor doors: they
// used to spell the same checks twice, which is how one door gains a verdict
// the other lacks. The accounting half is mergeInto's and has already run.
func (s *Server) reportMerge(d *targetDedup, kind, monitor, url string, held *kubemeta.ScrapeTarget, rep scrape.MergeReport) {
	if rep.AuthConflict {
		s.reportAuthConflict(kind, d.servingAuth(url, held.Monitor), monitor, url)
	}
	if rep.RelabelCapped {
		s.reportRelabelCapped(kind, monitor, url)
	}
	if rep.ContributorsCapped {
		s.reportContributorsCapped(kind, monitor, url, d.firstContribCap(kind, url))
	}
}

// shadowWarnEvery bounds how often one conflicting (winner, loser) pair may
// log. Like the scrape-auth throttle, this is a STEADY state rather than an
// event: the pair is re-decided on every target DERIVATION of every node that
// holds the pod — which the change-token memo skips while nothing changes, but
// a churning cluster rebuilds every node's list — so an unthrottled line is a
// flood proportional to fleet size. The counter carries the cumulative count
// (read its increase over a long window, not a short rate: see
// obs.MonitorTargetShadowed).
const shadowWarnEvery = 30 * time.Minute

// maxShadowedWarnPairs bounds the throttle table. Keys are monitor PAIRS, so
// they are bounded by the indexed monitors; this is belt and braces against a
// monitor set that churns.
const maxShadowedWarnPairs = 1024

// reportAuthConflict records a monitor endpoint whose auth/TLS material could
// not be merged into the target already holding the same URL on that pod:
// both declare it and it differs, so the holder's is served (see
// scrape.MergeMonitorEndpoint — every other endpoint group merges, silently).
// The scrape is now running with a credential/TLS config one of its CRs did
// not choose, which must not be discoverable only by a packet capture. The
// winner may equal the loser: one monitor's two endpoints resolving to one URL
// with different material is the same unhonoured declaration.
func (s *Server) reportAuthConflict(kind, winner, loser, url string) {
	obs.MonitorTargetShadowed.WithLabelValues(kind).Inc()
	if !s.allowKeyed(s.warnShadowed, kind+"\x00"+winner+"\x00"+loser,
		"conflicting-monitor", "pairs", maxShadowedWarnPairs) {
		return
	}
	s.log().Warn("two monitor endpoints declare different auth/TLS for one scrape URL; the first monitor's is served",
		"kind", kind, "serving", winner, "conflicting", loser, "url", url,
		"note", "the conflicting endpoint's auth/TLS is NOT applied (its other configuration merges); "+
			"further warnings for this pair are suppressed for "+shadowWarnEvery.String())
}

// relabelCappedWarnEvery and maxRelabelCappedWarnKeys bound the merged-chain
// ceiling warning. Like the scrape-auth and conflicting-monitor throttles this
// is a STEADY state, not an event: the merge is re-decided on every target
// DERIVATION of every node holding one of the pods (skipped by the
// change-token memo while nothing changes, repeated on every rebuild when the
// cluster churns), so an unthrottled line is a flood proportional to fleet
// size. Keys are monitor names, so the live set is bounded by the indexed
// monitors; the cap is belt and braces against churn.
const (
	relabelCappedWarnEvery   = 30 * time.Minute
	maxRelabelCappedWarnKeys = 1024
)

// reportRelabelCapped records a monitor endpoint whose metricRelabelings were
// only partly folded into the target already holding its URL: the merged chain
// hit scrape.MaxRelabelChainRules/MaxRelabelChainBytes and the rest of the
// chain filters nothing.
//
// It needs a line as well as the counter for the same reason the per-pod
// ceiling does: the refusal is invisible in the data. A drop rule that was not
// applied does not fail a scrape and does not log on the agent — the series the
// operator asked to drop simply arrive, at whatever cardinality they have, and
// nothing anywhere says which CR stopped being honoured.
func (s *Server) reportRelabelCapped(kind, monitor, url string) {
	obs.MonitorRelabelChainCapped.WithLabelValues(kind).Inc()
	if !s.allowKeyed(s.warnRelabelCapped, kind+"\x00"+monitor,
		"relabel-ceiling", "monitors", maxRelabelCappedWarnKeys) {
		return
	}
	s.log().Warn("monitor metricRelabelings only partly applied: the merged chain for this scrape URL is at the per-target ceiling",
		"kind", kind, "monitor", monitor, "url", url,
		"rules", scrape.MaxRelabelChainRules, "bytes", scrape.MaxRelabelChainBytes,
		"note", "every monitor resolving to one URL on one pod is served as ONE scrape, so their chains "+
			"concatenate; the rules that fit are applied and the rest filter nothing. Either the chain is "+
			"enormous or several monitors target this URL — GET /v1/explain/<ns>/<pod> says which. Further "+
			"warnings for this monitor are suppressed for "+relabelCappedWarnEvery.String())
}

// contribCappedWarnEvery and maxContribCappedWarnKeys bound the contributor-list
// ceiling warning. It gets its OWN throttle table rather than sharing the
// relabel one beside it: the two conditions are independent (the attack that
// fills the contributor list carries no relabel rules at all), and a shared
// gate would let whichever fired first suppress the other for half an hour on
// exactly the monitor an operator is looking at. Same steady-state reasoning as
// its siblings — the merge is re-decided on every target derivation of every
// node holding one of the pods.
//
// Keys are the URL, NOT the monitor the two siblings key by, because the
// condition is a property of the URL: "too many monitors resolve here". The
// refused monitor is a symptom, and there are as many of them as the tenant
// cares to create — keying by monitor turned one pile-up into one line per
// refused CR (1,968 of them in the regression test) saying the same thing.
// /v1/explain is where the per-monitor answer lives, and the line points at it.
const (
	contribCappedWarnEvery   = 30 * time.Minute
	maxContribCappedWarnKeys = 1024
)

// reportContributorsCapped records a monitor whose endpoint MERGED into the
// target holding its URL but which is not listed among that target's
// contributors, the list being at scrape.MaxContributorsPerTarget.
//
// Nothing about the scrape changed, which is the entire reason this needs a
// line and a counter: an operator reading the served target, or the series it
// produces, has no way to tell that a monitor they can see being honoured is
// missing from the attribution — and the shape that fills the list (many
// monitors resolving to one URL on one pod) is worth reconciling on its own.
// The monitor is named as the EXAMPLE it is: it is whichever one happened to
// arrive first past the ceiling, not the cause.
// firstForPod is the caller's per-derivation gate (targetDedup.firstContribCap):
// the condition is a property of the URL and fires once per monitor past the
// ceiling, so on a pile-up this used to build one throttle key — an allocation
// — and take one dedupe-table mutex per REFUSED MONITOR, i.e. the guard that
// bounds the abuse allocated in proportion to it. The COUNTER still moves on
// every refusal, because it is the rate an operator alerts on; only the line,
// which says the same thing every time, is folded to once per URL per pod.
func (s *Server) reportContributorsCapped(kind, monitor, url string, firstForPod bool) {
	obs.MonitorContributorsCapped.WithLabelValues(kind).Inc()
	if !firstForPod {
		return
	}
	if !s.allowKeyed(s.warnContribCapped, kind+"\x00"+url,
		"contributor-ceiling", "scrape URLs", maxContribCappedWarnKeys) {
		return
	}
	s.log().Warn("more monitors resolve to one scrape URL than its contributor list can carry; the rest merge unattributed",
		"kind", kind, "url", url, "monitors", scrape.MaxContributorsPerTarget, "example", monitor,
		"note", "the endpoints' metricRelabelings, interval and auth DO merge — only the attribution is "+
			"refused, so the scrape is unaffected. Every monitor resolving to one URL on one pod is served "+
			"as ONE scrape; GET /v1/explain/<ns>/<pod> lists every monitor and says which ones are not "+
			"contributors. Further warnings for this URL are suppressed for "+contribCappedWarnEvery.String())
}

// unresolvedWarnEvery and maxUnresolvedWarnKeys bound the unresolved-endpoint
// warning. Keys are (kind, monitor) — cluster OBJECTS — so the live set is
// bounded by the monitor CRs however many pods they select, however often
// those pods are replaced, and however many endpoints each CR declares.
//
// The port SPELLING used to be the third part of the key, and it is content
// rather than identity: one ServiceMonitor may declare as many endpoints as
// fit in an etcd object, each naming a distinct port string of a length its
// author picks. That mints keys — arbitrarily many, arbitrarily long — in a
// bounded table that SUPPRESSES on saturation (internal/logdedupe's rule), so
// anyone able to write one monitor in one namespace could stop this warning
// from ever reporting anyone else's broken endpoint. The port still rides the
// LINE, clipped: the same trade internal/agent/promscrape's warnOnce made when
// it took a monitor's duration value out of its key. The cost is that a
// monitor with several unresolved endpoints reports one of them per window
// instead of each — the line names which, and the remedy is the same CR.
const (
	unresolvedWarnEvery   = 30 * time.Minute
	maxUnresolvedWarnKeys = 1024
)

// maxLoggedPortBytes bounds the port spelling on the line. A CR field is
// bounded only by the object's own size, and a megabyte of it in a log record
// is a second flood in the shape of one line. The sibling constant is
// internal/agent/promscrape's maxLoggedValueBytes, which bounds the same class
// of value for the same reason.
const maxLoggedPortBytes = 96

// clipPort renders a CR-supplied port spelling for a log attribute: bounded,
// and cut on a rune boundary (internal/clip) so a clipped UTF-8 sequence does
// not reach the log pipeline as a replacement character.
func clipPort(v string) string { return clip.Ellipsis(v, maxLoggedPortBytes) }

// reportUnresolvedEndpoint warns that a monitor endpoint names a port the pod
// its selector matched does not declare, so that pod produces no target for it.
//
// This is the commonest ServiceMonitor mistake there is — a `port:` naming the
// Service port that does not exist, or a container port that was renamed — and
// it was the least visible outcome on the whole derivation: prometheus-operator
// emits no scrape config for such an endpoint either, so there is no failing
// scrape, no up=0, and no counter anywhere. The pod is just missing, which
// looks exactly like a pod nobody asked to scrape. /v1/explain says so per pod,
// but only to someone who already suspects this pod.
//
// An endpoint naming NEITHER port nor targetPort is deliberately skipped: that
// one already rides Endpoint.Ignored ("port(unset)"), which the metadata
// service logs once per changed monitor and counts as
// kubescrape_monitor_fields_ignored_total. Reporting it here too would say the
// same thing per pod per cycle.
//
// Callers reach it through noteUnresolved, which decides per POD (a
// ServiceMonitor endpoint may fail on one matched Service and resolve through
// another) and asks the throttle at most once per monitor per derivation.
func (s *Server) reportUnresolvedEndpoint(kind, monitor string, ep *servicemonitors.Endpoint, pod *kubemeta.Pod) {
	if !reportableUnresolved(ep) {
		return
	}
	port := endpointPortSpelling(ep)
	// IDENTITY only: see maxUnresolvedWarnKeys for why the port spelling is on
	// the line but not in the key.
	if !s.allowKeyed(s.warnUnresolved, kind+"\x00"+monitor,
		"unresolved-endpoint", "endpoints", maxUnresolvedWarnKeys) {
		return
	}
	s.log().Warn("monitor endpoint names a port the selected pod does not declare, so that pod yields no scrape target for it",
		"kind", kind, "monitor", monitor, "port", clipPort(port),
		"namespace", pod.Namespace, "pod", pod.Name,
		"note", "the pod is simply absent from this endpoint's targets — no scrape fails and nothing is exported for it. "+
			"GET /v1/explain/"+pod.Namespace+"/"+pod.Name+" lists the pod's declared ports beside this verdict. "+
			"A ServiceMonitor `port` names a SERVICE port (resolved to a pod port through its targetPort) on ANY of "+
			"the Services it selects in front of the pod; a PodMonitor `port` names a CONTAINER port. Further "+
			"warnings for this monitor are suppressed for "+unresolvedWarnEvery.String())
}

// reportableUnresolved is reportUnresolvedEndpoint's own precondition, split
// out so noteUnresolved can test it BEFORE claiming the monitor's slot in the
// derivation's asked set: an unset endpoint claiming it would silence a
// genuinely unresolved sibling of the same monitor for the whole request.
func reportableUnresolved(ep *servicemonitors.Endpoint) bool {
	return ep.Port != "" || ep.TargetPort != nil
}

// unresolvedReports is one derivation's bookkeeping for the unresolved-endpoint
// warning.
//
// pending is PER POD: the ServiceMonitor endpoints that failed to resolve on
// some matched Service. A monitor that selects two Services in front of one pod
// (a ClusterIP and a headless Service sharing labels) offers each endpoint
// through both, and an endpoint naming a port only ONE of them declares still
// yields the pod's target — so a failure on one Service is not a verdict on the
// pod, and warning there said "the pod is absent from the target list" about a
// pod the same response was serving, while claiming the monitor's 30-minute
// throttle slot away from a genuinely broken pod.
//
// asked is PER REQUEST: the (kind, monitor) keys this derivation has already
// put to the throttle. The throttle key has no pod component, so within one
// derivation every question after the first for a key is answered "no" by a
// table that took its mutex, read the clock and allocated the key to say so —
// measured at 128 broken endpoints x 110 pods, 14,098 allocations and about
// half the derivation's time. Asking once per key per request gives exactly the
// same lines.
//
// offeredEPs is PER POD too, and built only when a pending endpoint needs it:
// the endpoint declarations that resolved to SOME url through the pod's matched
// Services, read once off that pod's monitorOffers (whose keys are exactly the
// resolved (url, endpoint) pairs). A scan of the offers per pending endpoint
// instead was quadratic in the monitors a pod's Services attract — each one a
// tenant-authored CR — on the one path that exists for a misconfiguration.
//
// All three are lazily allocated: a healthy node never builds any of them.
type unresolvedReports struct {
	pending    []monitorEndpoint
	asked      map[unresolvedKey]struct{}
	offeredEPs map[*servicemonitors.Endpoint]struct{}
	collected  bool
}

// done reports whether this derivation has already put (kind, monitor) to the
// throttle.
func (u *unresolvedReports) done(kind, monitor string) bool {
	_, ok := u.asked[unresolvedKey{kind: kind, monitor: monitor}]
	return ok
}

// unresolvedKey is the throttle's identity, as a comparable struct so the
// lookup that decides allocates nothing (contribCapKey's reasoning).
type unresolvedKey struct{ kind, monitor string }

// resetPod starts a pod: pending and offeredEPs are per pod, asked is not.
func (u *unresolvedReports) resetPod() {
	u.pending = u.pending[:0]
	if u.collected {
		clear(u.offeredEPs)
		u.collected = false
	}
}

// resolved reports whether ep resolved to any url for the current pod — through
// at least one of its matched Services — which is the question to ask before
// blaming the pod for an endpoint that failed on one of them.
func (u *unresolvedReports) resolved(o *monitorOffers, ep *servicemonitors.Endpoint) bool {
	if !u.collected {
		if u.offeredEPs == nil {
			u.offeredEPs = make(map[*servicemonitors.Endpoint]struct{}, len(o.seen))
		}
		for k := range o.seen {
			u.offeredEPs[k.ep] = struct{}{}
		}
		u.collected = true
	}
	_, ok := u.offeredEPs[ep]
	return ok
}

// failed records a ServiceMonitor endpoint that did not resolve through one of
// the pod's matched Services.
func (u *unresolvedReports) failed(sme monitorEndpoint) { u.pending = append(u.pending, sme) }

// noteUnresolved reports an endpoint that resolved to nothing on this pod
// through every door it was offered, asking the throttle at most once per
// (kind, monitor) per derivation.
func (s *Server) noteUnresolved(u *unresolvedReports, kind, monitor string, ep *servicemonitors.Endpoint, pod *kubemeta.Pod) {
	if !reportableUnresolved(ep) {
		return
	}
	if u.done(kind, monitor) {
		return
	}
	k := unresolvedKey{kind: kind, monitor: monitor}
	if u.asked == nil {
		u.asked = make(map[unresolvedKey]struct{}, 1)
	}
	u.asked[k] = struct{}{}
	s.reportUnresolvedEndpoint(kind, monitor, ep, pod)
}

// endpointPortSpelling renders the port an endpoint names, the way its CR
// spells it, so the warning and the CR can be matched up by eye.
func endpointPortSpelling(ep *servicemonitors.Endpoint) string {
	if ep.Port != "" {
		return ep.Port
	}
	if ep.TargetPort != nil {
		return "targetPort:" + ep.TargetPort.String()
	}
	return ""
}

// cappedWarnEvery and maxCappedWarnKeys bound the per-pod ceiling warning.
//
// The refusal is re-derived on every target derivation of every node that
// holds the pod (skipped by the change-token memo while nothing changes,
// repeated on every rebuild when the cluster churns), so this is a STEADY state
// exactly like the collision and shadowed-monitor warnings beside it, and the
// same throttle applies. Thirty
// minutes because the remedy is a configuration change, and nothing about the
// condition changes in between.
const (
	cappedWarnEvery   = 30 * time.Minute
	maxCappedWarnKeys = 1024
)

// cappedWarnKey identifies the ceiling refusal by the WORKLOAD, not by the pod:
// every replica of a Deployment carries the same annotations and the same
// monitors select all of them, so keying on the pod name would emit one line
// per replica and another full set on every rollout — the mistake already
// recorded three times (the agent's warnTarget, reportAuthConflict,
// collisionWarnKey).
// attrs.ServiceName resolves the workload owner (the Deployment, not the
// per-revision ReplicaSet), so the key survives a rollout.
func cappedWarnKey(pod *kubemeta.Pod) string {
	return pod.Namespace + "\x00" + attrs.ServiceName(*pod)
}

// reportPodCapped warns that ONE pod produced more scrape targets than the
// per-pod ceiling admits, so some of its endpoints are not scraped at all.
//
// obs.ScrapeTargetsCapped already carries the rate, and its help sends the
// operator to /v1/explain — which needs a pod NAME the counter cannot supply.
// A refused endpoint is indistinguishable from one that was never configured:
// the target simply is not in the list, no scrape fails, and the series it
// would have produced never appear. This line is the only thing that names the
// pod to look at.
func (s *Server) reportPodCapped(pod *kubemeta.Pod, d *targetDedup) {
	if !s.allowKeyed(s.warnPodCapped, cappedWarnKey(pod),
		"per-pod target-ceiling", "workloads", maxCappedWarnKeys) {
		return
	}
	// Both ceilings on one line, because a reader has to be able to tell which
	// one bound: "dropped=14 limit=16" against a pod serving TWO targets is a
	// wild-goose chase, and the byte ceiling binds at a target count that looks
	// entirely ordinary. podBytes is the pod document's measured size — a
	// number derived from the pod's annotations, never any of their bytes.
	s.log().Warn("pod produced more scrape targets than the per-pod ceilings admit; the excess endpoints are NOT scraped",
		"namespace", pod.Namespace, "pod", pod.Name, "dropped", d.capped,
		"droppedBySize", d.cappedBySize, "limit", scrape.MaxPortsPerPod,
		"podBytes", d.podBytes, "byteLimit", scrape.MaxTargetBytesPerPod,
		"note", "GET /v1/explain/"+pod.Namespace+"/"+pod.Name+" names the refused ports and endpoints; the "+
			"ceilings are per POD across every door (pod and Service annotations, ServiceMonitor and PodMonitor "+
			"endpoints), and they exist because every target embeds the whole pod document — so a pod contributes "+
			"at most 16 targets AND at most 256 KiB of them, whichever binds first. droppedBySize is how many the "+
			"BYTE ceiling refused: those are answered by shrinking the pod's labels and annotations (podBytes "+
			"measures them) or by splitting its ports across workloads, not by declaring fewer ports. The first "+
			"target of a pod is always served. Further warnings for "+
			"this workload are suppressed for "+cappedWarnEvery.String())
}

// viewTrimmedWarnEvery and maxViewTrimmedWarnKeys bound the trimmed-Service-view
// warning. Keyed by the SERVICE, because that is the object whose size is the
// cause and whose owner has the remedy: every pod behind it and every agent
// poll re-derives the same refusal, so the pod is on the line and not in the
// key. Services are cluster objects, so the live set is bounded by the
// cluster's configuration.
const (
	viewTrimmedWarnEvery   = 30 * time.Minute
	maxViewTrimmedWarnKeys = 1024
)

// reportViewsTrimmed warns that a pod's swap arms reduced a Service view to its
// identity because the full view did not fit the per-pod byte budget (see
// targetDedup.boundSwapView). Nothing is unscraped, so this moves no refusal
// counter — but a resource-attribute template reading .Service.Labels finds
// nothing on those targets, and without this line nothing says why.
//
// One line per Service still trimmed on a served target, each counting only
// ITS targets: the throttle is keyed by the Service, so a line naming one
// Service with another's count would both mislead and suppress the second
// Service's own warning. Cold — reached only when a view was trimmed, over at
// most scrape.MaxPortsPerPod entries.
func (s *Server) reportViewsTrimmed(pod *kubemeta.Pod, d *targetDedup) {
	for i, tr := range d.trimmed {
		if trimmedFor(d.trimmed[:i], tr.svc) > 0 {
			continue // reported with its first target
		}
		s.reportViewTrimmed(pod, tr.svc, trimmedFor(d.trimmed[i:], tr.svc))
	}
}

// trimmedFor counts the entries of trimmed that name the Service svc names.
func trimmedFor(trimmed []viewTrim, svc *kubemeta.Service) int {
	n := 0
	for _, tr := range trimmed {
		if tr.svc.Namespace == svc.Namespace && tr.svc.Name == svc.Name {
			n++
		}
	}
	return n
}

// reportViewTrimmed is reportViewsTrimmed's line for ONE Service, whose view
// was reduced to its identity on n of the pod's targets.
func (s *Server) reportViewTrimmed(pod *kubemeta.Pod, svc *kubemeta.Service, n int) {
	if !s.allowKeyed(s.warnViewTrimmed, svc.Namespace+"\x00"+svc.Name,
		"trimmed-Service-view", "Services", maxViewTrimmedWarnKeys) {
		return
	}
	s.log().Warn("a Service view did not fit the pod's scrape-target byte budget; those targets carry the Service's name, namespace and UID only",
		"namespace", svc.Namespace, "service", svc.Name, "pod", pod.Name, "targets", n,
		"byteLimit", scrape.MaxTargetBytesPerPod,
		"note", "every target that reaches the pod through this Service carries a copy of the Service's labels and "+
			"annotations, and copying them onto these targets would have taken the pod's targets past the per-pod "+
			"budget (the first target of a pod always keeps the full copy). The targets are still scraped and still carry k8s.service.name and k8s.service.uid; only the labels and "+
			"annotations are left off, so a resource-attribute template reading .Service.Labels sees none there. "+
			"Shrinking the Service's labels restores them. GET /v1/explain/"+pod.Namespace+"/"+pod.Name+
			" marks the affected targets. Further warnings for this Service are suppressed for "+
			viewTrimmedWarnEvery.String())
}

// collisionWarnEvery bounds how often ONE colliding configuration may log. Like
// the shadowed-monitor warning this is a STEADY state rather than an event: the
// collision is re-derived on every target derivation of every node holding one
// of the pods (skipped by the change-token memo while nothing changes, repeated
// on every rebuild when the cluster churns), so an unthrottled line is a flood
// proportional to fleet size.
const collisionWarnEvery = 30 * time.Minute

// maxCollisionWarnKeys bounds the throttle table. Every component of a key is
// something an operator WROTE (see collisionWarnKey), so the live set is
// bounded by the cluster's configuration: a workload contributes one key per
// annotated port per set of colliding declarations, whatever its replica count,
// however often its pods are replaced and however many nodes it runs on. This
// is belt and braces against a cluster that churns those declarations.
const maxCollisionWarnKeys = 1024

// collisionWarnKey identifies a collision by the CONFIGURATION that produced
// it, never by the pods it currently lands on — the mistake this repo has
// already recorded twice (the agent's warnTarget: "The URL embeds the pod IP,
// so keying on it was wrong twice over: the table grew one entry per pod
// incarnation for the process' whole life, and the warning re-fired on every
// pod restart"; reportAuthConflict keys on the monitor PAIR because pairs are
// bounded by the indexed monitors).
//
// Keying on the pod and its address was the same mistake a third time: measured
// at 20 replicas of one misconfigured Deployment, 20 WARN lines — and 20 more
// the moment a rollout gave them new names and new IPs, with the table at 40.
// The throttle's own doc says it exists because an unthrottled line is a flood
// proportional to fleet size, which is what that key left it as.
//
// What an operator has to fix is a workload's annotation, a Service's or a
// monitor CR's, so the key is what those say: the JOB (namespace plus the
// workload owner's name, which outlives every pod under it — attrs.ServiceName
// takes the Deployment, not the per-revision ReplicaSet, so a rollout keeps the
// key), the PORT, and the SET of colliding declarations — each one's scheme,
// path, source and monitor. The pod IP is the one part of the address that is
// an incarnation, and it is exactly what is cut out.
//
// A SET, sorted and deduplicated, not the members in order: a group lists one
// member per colliding TARGET, so identical hostNetwork replicas of one
// workload on one node contribute one identical tuple EACH, and a key written
// member by member changed — and grew — with the per-node replica count. Nodes
// running different counts, and every reschedule that changed a count, minted
// a fresh key and a fresh warning inside the throttle window, for one
// configuration. Deduplicating merges nothing an operator would fix
// differently: two members of ONE pod can never share a tuple (the URL dedup is
// per pod), so an identical tuple is always the same declaration on another
// replica.
func collisionWarnKey(c scrape.InstanceCollision) string {
	decls := make([][4]string, 0, len(c.Targets))
	for _, ct := range c.Targets {
		scheme, path := splitAtAddress(ct.URL, c.Address)
		decls = append(decls, [4]string{scheme, path, ct.Source, ct.Monitor})
	}
	slices.SortFunc(decls, func(a, b [4]string) int {
		for i := range a {
			if c := strings.Compare(a[i], b[i]); c != 0 {
				return c
			}
		}
		return 0
	})
	decls = slices.Compact(decls)
	var b strings.Builder
	b.WriteString(c.Job)
	b.WriteByte(0)
	b.WriteString(portOf(c.Address))
	for _, decl := range decls {
		for _, part := range decl {
			b.WriteByte(0)
			b.WriteString(part)
		}
	}
	return b.String()
}

// portOf is the port half of a host:port. The host half is a pod IP, which is
// an incarnation rather than configuration; the port was annotated.
func portOf(address string) string {
	if i := strings.LastIndexByte(address, ':'); i >= 0 {
		return address[i+1:]
	}
	return ""
}

// splitAtAddress cuts a member's URL around the address the whole group shares,
// leaving its two CONFIGURED halves — the scheme prefix and the path. They are
// substrings, so this allocates nothing. A URL not containing the address is
// unreachable for a derived target (the address is what built it); it degrades
// to keying on the whole URL rather than dropping the member from the key.
func splitAtAddress(url, address string) (scheme, path string) {
	before, after, ok := strings.Cut(url, address)
	if !ok {
		return url, ""
	}
	return before, after
}

// reportInstanceCollision warns that two served targets collapse onto one
// exported series identity. BOTH are still served — scrape/instance.go carries
// the argument for that — so this warning is the operator's only notice that
// the two are about to overwrite each other.
//
// Every other symptom is anonymous: `up` alternating 0 and 1 at one timestamp,
// a shared counter alternating between two endpoints' values so rate() reads
// resets, or a backend's duplicate-sample rejection — none of which names the
// pods, and none of which names the two declarations.
func (s *Server) reportInstanceCollision(c scrape.InstanceCollision) {
	// Counted BEFORE the throttle, like MonitorTargetShadowed: the log line is
	// suppressed for collisionWarnEvery per configuration, so a counter behind
	// the throttle would move once every half hour and there would be nothing
	// an operator could alert on between. The price is that the rate tracks
	// how often the node's target list is rebuilt rather than how broken the
	// configuration is, which the metric's own help says in as many words.
	obs.TargetIdentityCollisions.Inc()
	if !s.allowKeyed(s.warnCollide, collisionWarnKey(c),
		"colliding-target", "collisions", maxCollisionWarnKeys) {
		return
	}
	// Each member names its pod: a group can span pods (two hostNetwork
	// replicas of one workload), and those members agree on everything else.
	//
	// BOUNDED, and the bound is on the log line rather than on the group: a
	// collision group is every target on the node sharing one (job, instance),
	// which on a hostNetwork workload is one member per replica per port —
	// ~1,760 on a full node, each ~100 bytes, in ONE record. Two examples name
	// the configuration (a collision needs two), the count says how big it
	// really is, and the pair the operator fixes is in the same place either
	// way. Same lesson as the ceilings this campaign added: a throttle bounds
	// how OFTEN a line is written, never how LARGE it is.
	const maxMembers = 2
	members := make([]string, 0, min(len(c.Targets), maxMembers))
	for _, ct := range c.Targets[:min(len(c.Targets), maxMembers)] {
		who := ct.Source
		if ct.Monitor != "" {
			who += " " + ct.Monitor
		}
		members = append(members, ct.URL+" ["+who+" on "+ct.Pod+"]")
	}
	if over := len(c.Targets) - len(members); over > 0 {
		members = append(members, "and "+strconv.Itoa(over)+" more targets on this identity")
	}
	s.log().Warn("two scrape targets export the same series identity; both are scraped and their samples collide",
		"job", c.Job, "instance", c.Address, "targets", strings.Join(members, ", "),
		"note", "the exported identity is the pod's workload plus host:port and does NOT include the path, so up, "+
			"scrape_duration_seconds and scrape_samples_scraped arrive once per target with one identity and one "+
			"timestamp, and any metric name the endpoints share becomes one series carrying both their values; "+
			"give the endpoints separate container ports, or drop one declaration — and where the two targets name "+
			"different pods, the two hostNetwork pods are annotated for one port on one node. Further warnings for "+
			"this configuration are suppressed for "+collisionWarnEvery.String())
}
