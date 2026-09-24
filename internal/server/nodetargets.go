package server

// GET /v1/nodes/{node}/targets: the node-local scrape-target derivation and
// the memoised inputs it reads (the monitor→Service and PodMonitor snapshots).
// The per-pod accumulator is targetdedup.go; the throttled warnings the
// derivation raises are targetwarn.go.

import (
	"net/http"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/labels"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// handleNodeTargets serves GET /v1/nodes/{node}/targets.
func (s *Server) handleNodeTargets(w http.ResponseWriter, r *http.Request) {
	if !s.requireReady(w, "") {
		return
	}
	node := r.PathValue("node")
	// A conditional GET answered from the ETag memo never builds the response
	// at all — see nodeTargetsNotModified. With every source's change token
	// wired (targetsValidity), a DaemonSet agent polling on its scrape interval
	// reaches it on every poll while nothing its node's list reads has changed;
	// only the wall-clock fallback (an unwired source) is out of reach at that
	// cadence, since its window is the max-age the agent was handed.
	if s.nodeTargetsNotModified(w, r, node) {
		return
	}
	// Every answer that is not the memo's is a derivation, and the pair is
	// what says whether the memo reaches the fleet (obs.NodeTargetsBuilds).
	obs.NodeTargetsBuilds.WithLabelValues("built").Inc()
	// Sampled before the derivation reads a single source (see targetsValidity).
	valid := s.targetsValidity(node)
	targets, built := s.nodeTargets(node)
	// Cached like the other metadata 200s (Cache-Control max-age + ETag): every
	// agent re-fetches its target list every cycle, and the response embeds the
	// COMPLETE pod document per target — without revalidation that is the whole
	// node's pod set re-sent 30x/min regardless of change, the one metadata
	// route that had no 304 path. Staleness is bounded by the TTL (default 10s,
	// under the default scrape interval and additive to the agent's own polling
	// lag); the server-side list itself still drops deleted/finished/terminating
	// pods immediately (the invariant is about what a fresh response contains).
	doc := kubemeta.NodeTargets{Node: node, Targets: targets}
	if s.cacheTTL <= 0 {
		s.writeCached(w, r, doc, false) // the plain uncached write
		return
	}
	body, etag, ok := s.encodeCached(w, doc, "node targets")
	if !ok {
		return
	}
	// Memoised BEFORE the response is written: the client may revalidate the
	// instant it holds the ETag, and remembering afterwards leaves a window in
	// which the tag this server just handed out is one it cannot recognise.
	if built {
		s.rememberNodeTargets(node, etag, valid)
	}
	s.writeCachedBody(w, r, body, etag, false)
}

// nodeTargets derives the scrape targets of every scrapeable pod on a node,
// deduped and in a deterministic order. built reports whether the node has pods
// at all — the ETag memo is only for nodes that do (see rememberNodeTargets).
func (s *Server) nodeTargets(node string) (targets []kubemeta.ScrapeTarget, built bool) {
	s.targetBuilds.Add(1)
	pods := s.store.PodsOnNode(node)
	targets = make([]kubemeta.ScrapeTarget, 0)
	if len(pods) == 0 { // an empty node cannot match any monitored service
		return targets, false
	}
	monitored := s.monitoredServices()
	// Hoisted out of the per-pod loop: PodMonitors() copies and SORTS the whole
	// monitor list on every call, so a node with 110 pods and 200 monitors did
	// 110 sorts and 22k selector evaluations per request, per agent, per scrape
	// cycle. The sibling ServiceMonitor match is memoised for the same reason.
	allPodMonitors := s.allPodMonitors()
	// ONE snapshot of each namespace's Services for the whole request, NARROWED
	// to the ones that can opt a pod in. The per-pod Matching() call the
	// snapshot replaced took the index RLock and walked every Service in the
	// pod's namespace, so a 1,000-Service namespace cost 110 lock round trips
	// and 110 map walks per request; the narrowing is what takes the remaining
	// per-pod WALK off the same shape (see optInServices).
	optIn := optInServices(s.services.InNamespaces(podNamespaces(pods)), monitored)

	var d targetDedup
	// The monitor endpoints are swept once per matched SERVICE, so a pod behind
	// two Services reaches the merge twice with the same declaration; the merge
	// is a fold and cannot tell (monitorOffers' doc has the whole story).
	var offers monitorOffers
	var matched []*services.Service
	var podMonitors []podMonitorRef
	// enrich is memoised for the request: a node's pods share a handful of
	// owner chains and one or two namespaces (see enrichCache).
	var enr enrichCache
	// doors memoises each scrape-annotated Service's pod-independent half for
	// the request (scrape.ServiceDoor): its port selection walks a
	// tenant-authored list that nothing about the pod changes.
	var doors serviceDoors
	// unresolved decides the unresolved-endpoint warning per POD and asks the
	// throttle at most once per monitor per request (see unresolvedReports).
	var unresolved unresolvedReports
	var evals int64
	for _, np := range pods {
		if !scrape.Scrapeable(np.Pod) {
			continue // finished/deleted pods can never yield targets
		}
		// Cheap pre-check before the (per-pod) enrichment work: does the pod
		// or any service selecting it opt into scraping? matched holds only
		// opt-in Services, so its emptiness IS "no Service opts this pod in" —
		// the separate scan for an annotated-or-monitored member is what the
		// narrowing above replaced.
		inNamespace := optIn[np.Pod.Namespace]
		evals += int64(len(inNamespace))
		matched = matchingServices(inNamespace, np.Pod.Labels, matched[:0])
		podAnnotated := scrape.OptedIn(np.Pod.Annotations)
		podMonitors = podMonitorsFor(np.Pod, allPodMonitors, podMonitors[:0])
		if !podAnnotated && len(matched) == 0 && len(podMonitors) == 0 {
			continue
		}
		s.enrichCached(&enr, &np.Pod, np.OwnerRefs)

		d.reset(&targets)
		offers.reset()
		unresolved.resetPod()
		for _, t := range scrape.PodTargets(np.Pod) {
			d.add(t)
		}
		for _, svc := range matched {
			for _, t := range doors.targets(np.Pod, svc) {
				d.add(t)
			}
			for _, sme := range monitored[svc.UID] {
				o := d.offerServiceMonitor(&offers, &np.Pod, svc, sme)
				switch {
				case !o.resolved:
					// The endpoint names a port THIS Service does not carry
					// through to the pod (or it was refused at parse). That is
					// not yet a verdict on the pod: the monitor may select a
					// second Service in front of it that does — a ClusterIP and
					// a headless Service sharing labels is the ordinary shape —
					// and then the pod IS in the target list. Decided once the
					// pod's every matched Service has offered, below.
					unresolved.failed(sme)
				case o.merged:
					// Only auth/TLS material both monitors declare differently
					// is a loss worth reporting, attributed to the monitor whose
					// material is actually served (which may be a merged
					// contributor's, not the URL holder's).
					s.reportMerge(&d, "servicemonitor", sme.monitor, o.url, o.held, o.rep)
				}
			}
		}
		// An endpoint that failed on one matched Service and resolved through
		// another is honoured — it is in offers — so only one that resolved
		// through NONE of them leaves the pod out of the list.
		for _, sme := range unresolved.pending {
			// The asked set first: once a monitor has been put to the throttle
			// this request, every later pod's answer is already known.
			if unresolved.done("servicemonitor", sme.monitor) || unresolved.resolved(&offers, sme.endpoint) {
				continue
			}
			s.noteUnresolved(&unresolved, "servicemonitor", sme.monitor, sme.endpoint, &np.Pod)
		}
		for _, pm := range podMonitors {
			for i := range pm.monitor.Endpoints {
				ep := &pm.monitor.Endpoints[i]
				o := d.offerPodMonitor(&np.Pod, pm.name, ep)
				switch {
				case !o.resolved:
					// A PodMonitor selects the pod directly — no enclosing
					// per-Service loop — so a failure here IS the pod's verdict.
					s.noteUnresolved(&unresolved, "podmonitor", pm.name, ep, &np.Pod)
				case o.merged:
					s.reportMerge(&d, "podmonitor", pm.name, o.url, o.held, o.rep)
				}
			}
		}
		// After every door has offered: the ceiling binds across all of them,
		// so no single door can report it (targetDedup.capped's own comment).
		// Reported HERE and not inside add, so /v1/explain — which derives
		// through the same accumulator — stays read-only, like the two sibling
		// decision signals it suppresses by not calling them.
		if d.capped > 0 {
			s.reportPodCapped(&np.Pod, &d)
		}
		if len(d.trimmed) > 0 {
			s.reportViewsTrimmed(&np.Pod, &d)
		}
	}
	s.svcSelectorEvals.Add(evals)
	// Deterministic order: PodsOnNode iterates a map, and writeCached's ETag is
	// a body hash — an order that shuffled per request would mint a fresh ETag
	// every time and defeat the 304 revalidation entirely. URL first, then
	// monitor/source, then the embedded pod's UID: the UID tiebreak is what
	// makes the order TOTAL in the one case URL+monitor+source cannot separate
	// (two hostNetwork pods sharing the node IP with the same annotated port —
	// identical URL, empty Monitor, Source "pod", but different pod documents).
	//
	// Sorted through an index PERMUTATION rather than by moving the targets
	// themselves. A ScrapeTarget is 616 bytes and embeds the whole pod
	// document by value, so a comparison sort over the elements does its
	// ~n log n swaps as 616-byte typedmemmoves — every one of them a write
	// barrier, taken while this same request is allocating the pod copies that
	// keep the GC marking. Sorting int32 indices does those swaps 8 bytes at a
	// time and applies the result in n element moves, and it drops sort.Slice's
	// reflect swapper (3 allocs) with it.
	sortTargets(targets)
	// Every declaration on the node has been offered, so the served list is
	// final and the identities it exports can be checked against each other.
	// AFTER the sort, so the members of a group are listed in the order the
	// response lists them and one collision reads the same way on every request.
	for _, c := range d.inst.Collisions(targets) {
		s.reportInstanceCollision(c)
	}
	return targets, true
}

// sortTargets orders a node's target list by (URL, Monitor, Source, pod UID)
// — the total order handleNodeTargets' ETag depends on — without ever moving a
// ScrapeTarget through a comparison sort.
//
// The element is 616 bytes and embeds the pod document, so sorting the slice
// directly pays ~n log n write-barriered 616-byte moves; sorting a permutation
// of int32 indices pays them 8 bytes at a time and then applies the answer in
// n moves. The MACHINE-INDEPENDENT half of that, which is what this repo quotes:
// 3 allocations (sort.Slice's reflect swapper) become 1, pinned by
// TestSortTargetsDoesNotAllocateAReflectSwapper. A CPU profile of the route put
// sort.Slice at 9.1% of handleNodeTargets with 7.0 points of that in
// typedmemmove and write-barrier flushing, and an isolated single-run
// comparison at n=110 read 62 µs against 19 µs — indicative only, on a machine
// whose demonstrated benchmark noise floor is far larger than that gap.
//
// slices.SortFunc over the targets THEMSELVES is not the fix and measured worse
// than sort.Slice: its comparator takes the element BY VALUE, so every
// comparison copies 616 bytes.
func sortTargets(targets []kubemeta.ScrapeTarget) {
	if len(targets) < 2 {
		return
	}
	idx := make([]int32, len(targets))
	for i := range idx {
		idx[i] = int32(i)
	}
	slices.SortFunc(idx, func(a, b int32) int {
		// One Compare per field, not an inequality test followed by a Compare:
		// the URLs of two targets usually DIFFER, so the guard form paid the
		// string comparison twice on the field that decides almost every call.
		x, y := &targets[a], &targets[b]
		if c := strings.Compare(x.URL, y.URL); c != 0 {
			return c
		}
		if c := strings.Compare(x.Monitor, y.Monitor); c != 0 {
			return c
		}
		if c := strings.Compare(x.Source, y.Source); c != 0 {
			return c
		}
		return strings.Compare(x.Pod.UID, y.Pod.UID)
	})
	permuteTargets(targets, idx)
}

// permuteTargets rewrites s so that s[i] becomes the element idx[i] named,
// following each cycle of the permutation with one element of scratch. It
// CONSUMES idx (visited slots are marked -1), which is why it is unexported and
// called only from sortTargets.
func permuteTargets(s []kubemeta.ScrapeTarget, idx []int32) {
	for i := range idx {
		if idx[i] < 0 {
			continue // already moved as part of an earlier cycle
		}
		j := int32(i)
		tmp := s[i]
		for {
			k := idx[j]
			idx[j] = -1
			if k == int32(i) {
				s[j] = tmp // the cycle closes on the element we lifted out
				break
			}
			s[j] = s[k]
			j = k
		}
	}
}

// podNamespaces lists the distinct namespaces of a node's pods, for the one
// Services snapshot the whole request works from.
func podNamespaces(pods []store.NodePod) []string {
	seen := make(map[string]struct{}, 8)
	out := make([]string, 0, 8)
	for _, np := range pods {
		if _, dup := seen[np.Pod.Namespace]; dup {
			continue
		}
		seen[np.Pod.Namespace] = struct{}{}
		out = append(out, np.Pod.Namespace)
	}
	return out
}

// optInServices narrows each namespace's Service snapshot to the Services that
// can OPT A POD IN — scrape-annotated, or selected by a ServiceMonitor.
//
// It is the difference between O(pods × services) and O(services) per request.
// The selector scan below runs per pod, and it ran over the namespace's ENTIRE
// Service population before deciding the pod was uninteresting: a node whose
// pods are neither annotated nor selected by anything paid the whole scan on
// every request of every scrape cycle — measured at 110 pods, on the derivation
// alone: 26 µs with no Services in the namespace, 8.0 ms at 1,000, 18.5 ms at
// 2,000 and 38 ms at 4,000, for a response of `{"node":"node1","targets":[]}`.
// A Service population is a namespace-wide property while the pods on a node
// are a sliver of it, so the term that grows is the one that had no business
// being inside the loop.
//
// Narrowing here cannot change what is served, because a Service that opts into
// nothing contributes nothing downstream: scrape.ServiceTargets returns nil
// unless the Service carries prometheus.io/scrape="true", and the monitor sweep
// iterates monitored[svc.UID], which is empty by construction for the rest. It
// cannot reorder anything either — the snapshot is sorted by name and filtering
// preserves order, which is what keeps the encounter order (hence which monitor
// names a merged target, and which Service the dedup's carryForward donates)
// deterministic.
//
// /v1/explain deliberately does NOT narrow: it reports every Service whose
// selector matches, annotated or not, because "this Service selects your pod
// and opts into nothing" is exactly the answer an operator came for. It bounds
// how many it LISTS instead (maxExplainServices, with the remainder counted
// into servicesNotShown) — an unnarrowed walk over a tenant-grown population
// is the right answer; materialising all of it into an unauthenticated
// response is not.
func optInServices(byNamespace map[string][]*services.Service, monitored map[string][]monitorEndpoint) map[string][]*services.Service {
	var out map[string][]*services.Service
	for ns, list := range byNamespace {
		var kept []*services.Service
		for _, svc := range list {
			if serviceOptsIn(svc, monitored) {
				kept = append(kept, svc)
			}
		}
		if len(kept) == 0 {
			// A namespace with no opt-in Service is absent rather than empty:
			// reading a missing key yields the nil slice the loop wants, and
			// most namespaces on most nodes are this case.
			continue
		}
		if out == nil {
			out = make(map[string][]*services.Service, len(byNamespace))
		}
		out[ns] = kept
	}
	return out
}

// serviceOptsIn reports whether a Service can opt a pod it selects into
// scraping at all: it is scrape-annotated (the Service door) or a ServiceMonitor
// selects it (the monitor door). The ONE spelling of the question, asked by
// optInServices to narrow the derivation's snapshot and by /v1/explain for its
// "nothing opts this pod in" hint, so the two cannot disagree about which
// Services count.
func serviceOptsIn(svc *services.Service, monitored map[string][]monitorEndpoint) bool {
	return scrape.OptedIn(svc.Annotations) || len(monitored[svc.UID]) > 0
}

// matchingServices filters a namespace's Services down to the ones selecting a
// pod, appending into a caller-owned scratch slice (one per request, not one
// per pod). The snapshot is already sorted by name, so the result is too: map
// iteration order must not decide which Service a URL-deduped target is
// attributed to.
func matchingServices(inNamespace []*services.Service, podLabels map[string]string, out []*services.Service) []*services.Service {
	for _, svc := range inNamespace {
		if svc.Selects(podLabels) {
			out = append(out, svc)
		}
	}
	return out
}

// serviceDoors memoises scrape.ServiceDoor per Service for ONE derivation.
// Services in a request's snapshot are immutable, so a pointer is the key; a
// Service that does not opt in yields a closed door that costs one map slot.
// Lazily allocated: a node with no matched Service never builds it.
type serviceDoors map[*services.Service]scrape.ServiceDoor

// targets is scrape.ServiceTargets(pod, svc), resolving the Service's half once.
func (m *serviceDoors) targets(pod kubemeta.Pod, svc *services.Service) []kubemeta.ScrapeTarget {
	door, ok := (*m)[svc]
	if !ok {
		door = scrape.NewServiceDoor(svc)
		if *m == nil {
			*m = make(serviceDoors, 4)
		}
		(*m)[svc] = door
	}
	return door.Targets(pod)
}

// monitorEndpoint pairs a ServiceMonitor endpoint with its monitor name.
//
// The endpoint is a POINTER into the indexed monitor, which is treat-as-
// immutable and outlives this map; stampEndpoint only reads it. By value the
// pair was 304 bytes and the memo holds one per (monitor, service, endpoint) —
// a cluster-wide 50 monitors over 2,000 Services is 100,000 pairs, measured at
// 40.2 MB resident for as long as the memo lives, with both the old and the new
// map alive across a rebuild (an 80 MB transient peak) against a 128Mi request.
type monitorEndpoint struct {
	monitor  string
	endpoint *servicemonitors.Endpoint
}

// monitoredServices maps Service UIDs to the ServiceMonitor endpoints
// selecting them. It is rebuilt only when the monitor or Service index has
// changed since the last build; callers must treat it as read-only.
func (s *Server) monitoredServices() map[string][]monitorEndpoint {
	if s.monitors == nil {
		return nil
	}
	// Read the change tokens BEFORE the build: a change landing during it is
	// then recorded as unbuilt and rebuilds on the next call, rather than being
	// stamped as already-included and lost until the next unrelated change.
	return s.monitoredServicesAt(s.monitors.Generation(), s.services.Generation())
}

// monitoredServicesAt is monitoredServices for a caller that sampled the two
// change tokens as monGen and svcGen — split out so a test can be the caller
// whose sample is OLDER than the memo's stamp, which is the case the ordering
// below exists for and which otherwise needs a lock-queue race to reach.
func (s *Server) monitoredServicesAt(monGen, svcGen uint64) map[string][]monitorEndpoint {
	s.monMu.Lock()
	defer s.monMu.Unlock()
	// ORDER, not equality: both tokens only ever advance (every writer is
	// gen.Add(1)), so a cache stamped AT OR PAST the caller's sample already
	// includes everything that caller could have seen. An equality test made a
	// caller that sampled just before a change — and then queued behind the
	// build that included it — rebuild and stamp its OLDER token, which forced
	// the next current caller to rebuild again: two extra full cross products
	// under monMu (20-89 ms each at scale, with every node-targets and explain
	// request waiting on the lock) for every request straddling a change.
	if !s.monValid || s.monGen < monGen || s.svcGen < svcGen {
		s.monCache = s.buildMonitoredServices()
		// The componentwise max of the old stamp and this caller's sample: both
		// were taken before this build started, so the build includes both,
		// and a stamp may never move backwards.
		s.monGen, s.svcGen, s.monValid = max(s.monGen, monGen), max(s.svcGen, svcGen), true
	}
	return s.monCache
}

// buildMonitoredServices resolves the monitor→services match from scratch.
//
// The Service snapshot is taken once per RUN of monitors sharing a namespace
// set, not once per monitor. services.All allocates and fills a slice of every
// Service in the named namespaces, and the shape this cross product exists for
// is a fleet of cluster-wide monitors (`namespaceSelector.any: true`, what
// kube-prometheus-stack ships): 200 of them over 2,000 Services took 200 copies
// of the same 2,000-element list — measured 41.5 MB and ~89 ms per rebuild, of
// which the snapshots are the overwhelming majority, and a rebuild is triggered
// by ANY Service change while monMu is held against every concurrent poll.
//
// ONE entry rather than a map of them, deliberately: Index.All returns monitors
// in (namespace, name) order, which puts monitors sharing a namespace set in a
// run for both shapes that matter — every monitor cluster-wide (one run), and
// monitors selecting their own namespace (one run per namespace) — so a
// one-entry cache collapses the repeats without ever holding more than a single
// snapshot alive. A map keyed by the namespace set would retain one snapshot
// per distinct set for the whole build, which on a heavily overlapping set of
// matchNames is the same 41.5 MB, live at once instead of collectable.
//
// The monitor ORDER is untouched, and must be: the order endpoints land in
// out[uid] is the encounter order MergeMonitorEndpoint folds in, so grouping
// monitors by namespace set — the obvious alternative — would silently change
// which monitor names a merged target and how its relabel chains concatenate.
//
// The monitors of one run now see ONE point-in-time view of their namespaces
// instead of a fresh read apiece, which is if anything more coherent; a Service
// change arriving mid-build is handled where it always was, by monitoredServices
// reading the change tokens BEFORE the build and rebuilding on the next call.
func (s *Server) buildMonitoredServices() map[string][]monitorEndpoint {
	s.monBuilds.Add(1)
	out := map[string][]monitorEndpoint{}
	var (
		runKey   []byte
		runSvcs  []*services.Service
		runValid bool
		key      []byte
	)
	for _, m := range s.monitors.All() {
		name := m.Namespace + "/" + m.Name
		namespaces := m.ServiceNamespaces()
		key = appendNamespaceSetKey(key[:0], namespaces)
		if !runValid || string(key) != string(runKey) {
			runSvcs = s.services.All(namespaces)
			runKey = append(runKey[:0], key...)
			runValid = true
		}
		for _, svc := range runSvcs {
			if !m.Selector.Matches(labels.Set(svc.Labels)) {
				continue
			}
			for i := range m.Endpoints {
				out[svc.UID] = append(out[svc.UID], monitorEndpoint{monitor: name, endpoint: &m.Endpoints[i]})
			}
		}
	}
	return out
}

// appendNamespaceSetKey renders a monitor's resolved namespace set into dst.
//
// nil means EVERY namespace and is not the same question as any explicit list,
// so it gets its own marker byte rather than an empty join — otherwise a
// cluster-wide monitor and one naming no namespace at all would share a
// snapshot, and the cluster-wide one would be answered with nothing.
func appendNamespaceSetKey(dst []byte, namespaces []string) []byte {
	if namespaces == nil {
		return append(dst, 0x01)
	}
	dst = append(dst, 0x02)
	for _, ns := range namespaces {
		dst = append(dst, ns...)
		dst = append(dst, 0x00)
	}
	return dst
}

// podMonitorRef is one indexed PodMonitor with its "namespace/name" already
// rendered: the name is stamped on every target the monitor produces, and
// building it per (pod, monitor) is one allocation per pair on the targets path.
type podMonitorRef struct {
	monitor *servicemonitors.PodMonitor
	name    string
	// namespaces is monitor.PodNamespaces() resolved ONCE per request: the
	// common no-namespaceSelector shape allocates a one-element slice per
	// call, and podMonitorsFor runs per (pod, monitor) pair on the targets
	// hot path — the same per-pair cost `name` was hoisted for.
	namespaces []string
}

// allPodMonitors returns the indexed PodMonitors, rendered once per change of
// the monitor index rather than once per request (see pmMu).
//
// THE RESULT IS SHARED AND MUST BE TREATED AS READ-ONLY — the same contract
// monitoredServices and servicemonitors.PodMonitors already carry.
// podMonitorsFor only reads it, copying the refs it keeps into a caller-owned
// slice.
func (s *Server) allPodMonitors() []podMonitorRef {
	if s.monitors == nil {
		return nil
	}
	// The token BEFORE the lock, exactly as monitoredServices does: a change
	// landing during the render is then recorded as unbuilt and re-rendered on
	// the next call, rather than stamped as already-included.
	return s.allPodMonitorsAt(s.monitors.Generation())
}

// allPodMonitorsAt is allPodMonitors for a caller that sampled the monitor
// token as gen (see monitoredServicesAt).
func (s *Server) allPodMonitorsAt(gen uint64) []podMonitorRef {
	s.pmMu.Lock()
	defer s.pmMu.Unlock()
	// By ORDER, for monitoredServices' reason: a snapshot stamped at or past
	// this caller's sample already includes what it could have seen.
	if s.pmValid && s.pmGen >= gen {
		return s.pmCache
	}
	s.pmBuilds.Add(1)
	all := s.monitors.PodMonitors()
	out := make([]podMonitorRef, 0, len(all))
	for _, m := range all {
		out = append(out, podMonitorRef{monitor: m, name: m.Namespace + "/" + m.Name, namespaces: m.PodNamespaces()})
	}
	s.pmCache, s.pmGen, s.pmValid = out, max(s.pmGen, gen), true
	return out
}

// podMonitorsFor filters the request's PodMonitors down to the ones selecting a
// pod (namespace + label selector), appending into a caller-owned scratch slice.
func podMonitorsFor(pod kubemeta.Pod, all []podMonitorRef, out []podMonitorRef) []podMonitorRef {
	for _, ref := range all {
		if ref.namespaces != nil && !slices.Contains(ref.namespaces, pod.Namespace) {
			continue
		}
		if !ref.monitor.Selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		out = append(out, ref)
	}
	return out
}
