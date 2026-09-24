package server

// targetDedup, the per-pod target accumulator nodeTargets and /v1/explain
// share: URL dedup, the monitor-endpoint offer step, the merge arm and the
// per-pod count and byte ceilings.

import (
	"slices"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// targetDedup collapses the duplicate targets ONE pod can produce, in the order
// they are offered (pod annotations, then per Service the service annotations
// and the ServiceMonitor endpoints, then the PodMonitors). One URL on one pod
// yields exactly one target.
//
//   - The same endpoint reachable via pod and service annotations is ONE target
//     described twice; keep the first (pod source wins), and let a monitor
//     UPGRADE it — a ServiceMonitor/PodMonitor endpoint's bearerTokenSecret,
//     insecureSkipVerify and metricRelabelings live only on the monitor-derived
//     target, and an org-wide `prometheus.io/scrape: "true"` annotation
//     coexisting with prometheus-operator CRDs is common. Dropping the monitor
//     target meant scraping a token-protected endpoint unauthenticated (401 —
//     total metric loss, visible only as up=0) and, worse, exporting the very
//     series a drop rule asked to remove.
//
//   - Two MONITORS resolving to one URL are two independent scrape declarations
//     for a scrape that happens ONCE: prometheus-operator emits a job per
//     (monitor, endpoint) and distinguishes their series by the `job` label,
//     while a kubescrape target's exported identity is
//     (url.full, service.instance.id = host:port, pod, service) with NO monitor
//     component — so serving both would scrape the endpoint twice and export two
//     byte-identical series identities in one payload, which a backend reads as
//     a conflict rather than as two targets (promscrape's fillTargetResource
//     comment describes fixing exactly that, and scheduleKey's says the metadata
//     service dedupes same-URL targets within a pod). The single target instead
//     honours BOTH declarations: the first monitor by encounter order —
//     matched Services sorted by name first, then the monitor index's
//     (namespace, name) order within each Service, ServiceMonitors before
//     PodMonitors, so deterministically — names the target, and every later
//     endpoint MERGES
//     into it (scrape.MergeMonitorEndpoint: relabel chains concatenate, the
//     finer explicit cadence wins with its own timeout, one-sided auth/TLS is
//     adopted; a bare or identical endpoint merges silently). Only auth/TLS
//     material both sides declare DIFFERENTLY cannot be honoured twice by one
//     scrape: the holder's is served and the loser is COUNTED and LOGGED
//     (obs.MonitorTargetShadowed), because a scrape running with a credential
//     one of its CRs did not choose must not be something only a packet
//     capture can reveal.
type targetDedup struct {
	// out is the destination slice, re-pointed per pod (dedup is per pod: two
	// hostNetwork pods legitimately share the node IP and every URL on it).
	out *[]kubemeta.ScrapeTarget
	// urlOwner indexes the entry currently HOLDING a URL — the annotation
	// target, or the monitor one that claimed it.
	urlOwner map[string]int
	// authOwner names the monitor whose auth/TLS a URL's target actually
	// SERVES, when it was ADOPTED from a merged contributor rather than
	// declared by the URL holder itself (MergeMonitorEndpoint's authAdopted).
	// The conflict warning names this monitor as "serving" — naming the
	// holder pointed operators at a monitor with no auth material at all.
	// Lazily allocated: most pods never merge an auth group.
	authOwner map[string]string
	// base is where this pod's targets start in *out — the dedup is per pod
	// while the slice accumulates across the node, and collisions() needs the
	// pod's own window.
	base int
	// inst is the identity scan's scratch, kept here so ONE map serves a whole
	// request: the targets path scans the node's finished list once, explain
	// scans the one pod it derived.
	inst scrape.InstanceScan
	// capped counts THIS pod's targets refused by the per-pod ceiling. The
	// ceiling lives here, at the accumulation seam, rather than at any single
	// door: scrape.MaxPortsPerPod was first applied only where the pod
	// ANNOTATION resolves ports, which left the ServiceMonitor endpoint list —
	// a second door needing no annotation on the pod at all — free to produce
	// unbounded full-pod targets and reopen the O(N²) response it was added to
	// close. Everything that materialises a target for a pod goes through add.
	//
	// It is READ by explainPod, which puts it on the document (cappedTargets)
	// beside the per-entry notes add's verdict feeds: for a long time it was
	// write-only, and the metric's own help text ("see /v1/explain for the
	// pod") sent the operator to a document that reported every refused
	// endpoint as resolving.
	capped int
	// cappedBySize is how many of capped were refused by the BYTE ceiling
	// (scrape.MaxTargetBytesPerPod) rather than by the count one. Both are
	// per-pod ceilings on the same accumulation and both move the same
	// counter, but they are different questions to an operator — "you declared
	// more than 16 ports" versus "your pod document is too big to copy that
	// many times" — and the second one binds at a target count that looks
	// perfectly ordinary. It is what the warning names and what /v1/explain
	// reports beside the total.
	cappedBySize int
	// podBytes is scrape.PodDocBytes for the pod currently being derived,
	// measured ONCE per pod (lazily, off the first target offered) rather than
	// once per target: every target of a pod embeds the same document, and this
	// derivation runs for every pod on the node on every agent poll. The walk
	// is allocation-free and touches the map ENTRIES, not their bytes, so a
	// 200 KiB annotation costs one addition — 0 allocs/op and ~O(labels +
	// annotations + containers) per pod, against the json.Marshal per pod the
	// obvious implementation would pay.
	//
	// Zero means "not measured yet": PodDocBytes has a fixed floor and can
	// never return 0, so the sentinel cannot be confused with an answer.
	podBytes int
	// bytes is what THIS pod's accepted targets have charged against
	// scrape.MaxTargetBytesPerPod: the whole target document on the NEW-URL
	// arm, and what a held target GREW by on the merge arm (charge) and the two
	// swap arms (chargeSwap). Only the new-URL arm refuses a target; the merge
	// arm refuses nothing (its growth has its own two ceilings, the merged chain
	// and the contributor list), and the swap arms refuse only a Service view's
	// tenant-sized half (boundSwapView).
	bytes int
	// diagnostic marks a derivation run for /v1/explain rather than for a
	// served response: capped still counts (the document names the refusals),
	// but obs.ScrapeTargetsCapped must NOT move. The counter is per-derivation
	// on the targets path, so a browser or a dashboard polling explain would
	// otherwise add a second, human-driven source to a rate an operator reads
	// as "endpoints refused per scrape cycle" — and the counter's own help
	// text points at explain, so the inflation lands exactly on the pods being
	// investigated. The two sibling decision signals on this derivation
	// (obs.TargetIdentityCollisions via reportInstanceCollision, and the
	// auth-conflict warn/obs.MonitorTargetShadowed) are suppressed by explain
	// simply not calling them; this one is inside add, so it needs the flag.
	//
	// Set once by explainPod BEFORE reset and never cleared: it is a property
	// of the derivation, not of the pod being reset onto.
	diagnostic bool
	// contribCapReported holds the (kind, URL) pairs this POD has already
	// reported as contributor-capped, so the LOG side of that report runs once
	// per URL per derivation instead of once per refused monitor.
	//
	// It exists because the report's throttle key had to be BUILT before the
	// throttle could refuse it: `kind + "\x00" + url` is an allocation, and the
	// condition fires once for every monitor past scrape.MaxContributorsPerTarget
	// — so the guard that bounds an abuse allocated in proportion to it
	// (measured +68 allocs/op on a 100-monitor pile-up, exactly N-32, and 68
	// mutex round trips through the dedupe table with them). The counter still
	// moves per refusal; only the line is folded.
	//
	// A struct key rather than a concatenated one: Go hashes a comparable
	// struct without materialising it, so the lookup that decides is free and
	// only the first refusal of a URL pays anything. Lazily allocated — a pod
	// that never fills a contributor list never allocates it — and bounded by
	// the pod's own URL count, which scrape.MaxPortsPerPod already caps.
	contribCapReported map[contribCapKey]struct{}
	// trimmed names THIS pod's targets whose Service view a swap arm reduced
	// to its identity because the full view did not fit the byte budget
	// (boundSwapView) — one entry per target however many swaps refused it,
	// bounded by the pod's URL count, which scrape.MaxPortsPerPod caps.
	// Nothing is unscraped, so it is not a capped target and moves no counter
	// — but attribution was refused, so /v1/explain reports it
	// (serviceViewsTrimmed, and per target) and nodeTargets warns naming each
	// Service.
	//
	// Each entry carries the Service it was trimmed FOR, rather than the pod
	// remembering one Service beside a URL list: a later swap that replaces a
	// trimmed view removes the entry (untrimView), and the Service has to go
	// with it, or the warning names a Service trimmed on no served target
	// while the one that IS trimmed goes unnamed.
	trimmed []viewTrim
}

// viewTrim is one target whose Service view boundSwapView reduced to its
// identity: the target's URL and the identity view it now serves.
type viewTrim struct {
	url string
	svc *kubemeta.Service
}

// contribCapKey identifies one contributor-ceiling report within a pod's
// derivation. The kind is part of it because obs.MonitorContributorsCapped is
// labelled by kind and the two monitor kinds reach a shared target
// independently.
type contribCapKey struct{ kind, url string }

// firstContribCap reports whether this pod's derivation has yet logged the
// contributor ceiling for (kind, url), recording it if not.
func (d *targetDedup) firstContribCap(kind, url string) bool {
	k := contribCapKey{kind: kind, url: url}
	if _, dup := d.contribCapReported[k]; dup {
		return false
	}
	if d.contribCapReported == nil {
		d.contribCapReported = make(map[contribCapKey]struct{}, 1)
	}
	d.contribCapReported[k] = struct{}{}
	return true
}

// reset points the dedup at a fresh pod. The maps are reused across pods:
// allocated once per request rather than once per pod.
func (d *targetDedup) reset(out *[]kubemeta.ScrapeTarget) {
	d.out = out
	d.base = len(*out)
	d.capped = 0
	d.cappedBySize = 0
	d.podBytes = 0
	d.bytes = 0
	clear(d.trimmed)
	d.trimmed = d.trimmed[:0]
	if d.urlOwner == nil {
		d.urlOwner = make(map[string]int, 4)
	} else {
		clear(d.urlOwner)
	}
	clear(d.authOwner)
	clear(d.contribCapReported)
}

// collisions reports the served targets of THIS pod that the URL dedup keeps
// apart and the exported (job, instance) cannot — same address, different path
// or scheme (scrape/instance.go carries the whole argument, including why they
// are served rather than merged away).
//
// It is the same scan the targets path runs, over the dedup's own window, so
// the two cannot disagree about what a collision IS: explain exists to explain
// what nodeTargets serves, and a second implementation shaped like this one is
// exactly the drift explain_parity_test.go was written after.
//
// The SCOPE does differ, and honestly: this endpoint derives one pod, so a
// collision whose other member is a DIFFERENT pod — two hostNetwork replicas of
// one workload — is invisible here and is reported by the targets path, which
// holds the node's whole list.
func (d *targetDedup) collisions() []scrape.InstanceCollision {
	return d.inst.Collisions((*d.out)[d.base:])
}

// adoptedAuth records/answers who supplies a URL's served auth (see authOwner).
func (d *targetDedup) adoptedAuth(url, monitor string) {
	if d.authOwner == nil {
		d.authOwner = make(map[string]string, 1)
	}
	d.authOwner[url] = monitor
}

func (d *targetDedup) servingAuth(url, holder string) string {
	if m, ok := d.authOwner[url]; ok {
		return m
	}
	return holder
}

// mergeInto folds a monitor endpoint into the target already holding its URL
// (scrape.MergeMonitorEndpoint) and does the merge's ACCOUNTING: the byte
// charge against the pod's budget and the record of an adopted credential. The
// verdicts come back on the report for the caller to act on — the served
// derivation REPORTS them (reportMerge), /v1/explain renders them — which is
// why the two halves are separate: explain must run every accounting step and
// none of the reporting, and an accounting step spelled by hand at each call
// site is one a future step would silently miss.
func (d *targetDedup) mergeInto(held *kubemeta.ScrapeTarget, url, monitor string, ep *servicemonitors.Endpoint) scrape.MergeReport {
	rep := scrape.MergeMonitorEndpoint(held, monitor, ep)
	d.charge(rep.Bytes)
	if rep.AuthAdopted {
		d.adoptedAuth(url, monitor)
	}
	return rep
}

// monitorHolder returns the MONITOR-derived target already holding a URL, when
// there is one: an incoming monitor endpoint for that URL merges into it
// instead of being materialised. It is the cheap check the caller makes BEFORE
// building anything: the answer needs the URL and nothing else, and
// MergeMonitorEndpoint's own bare-endpoint gate keeps the common shape — N
// cluster-wide monitors declaring nothing beyond the scrape itself — at a map
// lookup instead of a 592-byte target with the whole pod document embedded.
//
// A URL held by an ANNOTATION target is deliberately not a hit — the monitor
// target upgrades it wholesale (see add), and that upgrade is why the caller
// must still build one.
func (d *targetDedup) monitorHolder(url string) (*kubemeta.ScrapeTarget, bool) {
	i, taken := d.urlOwner[url]
	if !taken {
		return nil, false
	}
	// By POINTER: a ScrapeTarget is 592 bytes with the pod document embedded,
	// and this runs per (pod, monitor endpoint). The pointer is into d.out and
	// dies before the next append can reallocate it.
	held := &(*d.out)[i]
	if !configuredTarget(held) {
		return nil, false
	}
	return held, true
}

// endpointOffer is what offering ONE monitor endpoint to a pod's accumulator
// did. The step — resolve the URL, suppress a repeated declaration, merge into
// a monitor target already holding the URL, else materialise and add — is the
// one nodeTargets and /v1/explain must take identically, so it is written once
// (offerServiceMonitor, offerPodMonitor) and its outcome returned as data for
// each path to act on its own way: the served derivation REPORTS
// (unresolvedReports, reportMerge), explain writes a note. It used to be
// written twice, held equal only by explain_parity_test.go, and the copies had
// already drifted once (the monitorOffers dedup existed on one side only).
//
// Returned by value: every field is a scalar, a string header or a pointer, so
// the hot path allocates nothing for it.
type endpointOffer struct {
	// url is the endpoint's scrape URL on this pod; empty when !resolved.
	url string
	// resolved reports that the endpoint resolves to a URL on this pod at all.
	// An unresolved ServiceMonitor endpoint is not yet the POD's verdict — a
	// second Service selecting the pod may carry the port through (see
	// unresolvedReports) — while an unresolved PodMonitor endpoint is.
	resolved bool
	// repeat reports that exactly this declaration was already offered to this
	// URL through an earlier Service selecting the pod (monitorOffers), so the
	// offer did nothing. ServiceMonitor endpoints only.
	repeat bool
	// merged reports that a monitor-derived target already held the URL and
	// the endpoint folded into it (mergeInto); held and rep describe that fold.
	// held points into the accumulator's slice and is valid only until the next
	// add, which may reallocate it — callers use it at once or not at all.
	merged bool
	held   *kubemeta.ScrapeTarget
	rep    scrape.MergeReport
	// refused is the last verdict other than targetAccepted that add returned
	// for a target this endpoint materialised — targetAccepted (the zero value)
	// when every one was accepted or none was offered.
	refused targetVerdict
}

// offerServiceMonitor offers one ServiceMonitor endpoint, reached through the
// matched Service svc, to the pod's accumulator — nodeTargets' step and
// /v1/explain's, in that order: see endpointOffer. offers is the pod's
// declaration dedup (monitorOffers). The pod is taken by pointer so the offer
// itself copies nothing; the resolvers it calls take it as they always have.
func (d *targetDedup) offerServiceMonitor(offers *monitorOffers, pod *kubemeta.Pod, svc *services.Service, sme monitorEndpoint) endpointOffer {
	// The URL — a scrape target's identity — resolves without building the
	// target, so a monitor endpoint this pod already holds costs a map lookup
	// instead of a 592-byte target with the whole pod document embedded, a
	// fresh Service view and a copy of the relabeling rules. Sweeping N
	// cluster-wide ServiceMonitors over the same Services, the response is
	// byte-identical at every N while the loop used to build 125 targets per
	// pod to keep 2.
	url, ok := scrape.MonitorTargetURL(*pod, svc, *sme.endpoint)
	if !ok {
		return endpointOffer{}
	}
	// Two Services selecting one pod offer each of their monitors' endpoints
	// twice; the merge below is a fold and would serve the union doubled and
	// count the conflict twice.
	if !offers.first(url, sme.endpoint) {
		return endpointOffer{url: url, resolved: true, repeat: true}
	}
	if held, taken := d.monitorHolder(url); taken {
		// The endpoint's configuration merges into the holder — no target is
		// materialised either way.
		return endpointOffer{url: url, resolved: true, merged: true, held: held,
			rep: d.mergeInto(held, url, sme.monitor, sme.endpoint)}
	}
	o := endpointOffer{url: url, resolved: true}
	for _, t := range scrape.MonitorTargets(*pod, svc, sme.monitor, *sme.endpoint) {
		if v := d.add(t); !v.ok() {
			o.refused = v
		}
	}
	return o
}

// offerPodMonitor offers one PodMonitor endpoint to the pod's accumulator: the
// ServiceMonitor step minus the declaration dedup, which a PodMonitor does not
// need — it selects PODS, so each of its endpoints is offered once per pod with
// no enclosing per-Service loop (monitorOffers' doc).
func (d *targetDedup) offerPodMonitor(pod *kubemeta.Pod, name string, ep *servicemonitors.Endpoint) endpointOffer {
	url, ok := scrape.PodMonitorTargetURL(*pod, *ep)
	if !ok {
		return endpointOffer{}
	}
	if held, taken := d.monitorHolder(url); taken {
		return endpointOffer{url: url, resolved: true, merged: true, held: held,
			rep: d.mergeInto(held, url, name, ep)}
	}
	o := endpointOffer{url: url, resolved: true}
	for _, t := range scrape.PodMonitorTargets(*pod, name, *ep) {
		if v := d.add(t); !v.ok() {
			o.refused = v
		}
	}
	return o
}

// targetVerdict is add's answer: accepted, or WHICH of the two per-pod ceilings
// refused it. A bool cannot carry the second half, and the second half is the
// whole difference between "you declared more ports than the ceiling admits"
// and "your pod document is too large to copy this many times" — two different
// remedies, reported to the operator through two different wordings
// (scrape.CeilingNote and scrape.SizeCeilingNote).
type targetVerdict int

const (
	targetAccepted targetVerdict = iota
	targetRefusedCount
	targetRefusedBytes
)

// ok reports whether the target was accepted into the served list.
func (v targetVerdict) ok() bool { return v == targetAccepted }

// note is the /v1/explain wording for this verdict against subject ("this
// endpoint", "port 8080, 8081"), empty when the target was accepted. The
// wordings are internal/scrape's — the one spelling, beside the ceilings
// themselves.
func (v targetVerdict) note(subject string, podBytes int) string {
	switch v {
	case targetRefusedCount:
		return scrape.CeilingNote(subject)
	case targetRefusedBytes:
		return scrape.SizeCeilingNote(subject, podBytes)
	}
	return ""
}

// add offers a target to the accumulator, reporting whether it was ACCEPTED and
// — when it was not — which ceiling refused it, so this pod will not be scraped
// on that URL. The verdict is returned rather than merely counted because the
// ceilings bind HERE and nowhere else: with two doors contributing, each
// individually under both, no door can tell which of its own targets survived,
// so /v1/explain can only name a refused port or endpoint by asking the
// accumulator. The served path ignores the result (it reports the refusal
// through obs.ScrapeTargetsCapped and reportPodCapped).
func (d *targetDedup) add(t kubemeta.ScrapeTarget) targetVerdict {
	i, taken := d.urlOwner[t.URL]
	if !taken {
		// Both per-pod ceilings, applied on the NEW-URL arm only: a target that
		// merges into a URL this pod already holds costs no extra response
		// bytes, so it must not be refused (16 entries collapsing to 3 URLs
		// legitimately yields 3). Refusing here loses that endpoint — which is
		// why it is counted and reported by /v1/explain rather than silent.
		if len(*d.out)-d.base >= scrape.MaxPortsPerPod {
			return d.refuse(targetRefusedCount)
		}
		// Measured once per pod, off the first target offered: every target of
		// this pod embeds the same document (see podBytes).
		if d.podBytes == 0 {
			d.podBytes = scrape.PodDocBytes(&t.Pod)
		}
		// The pod's FIRST target is unconditional. The byte budget exists to
		// bound the MULTIPLIER — N copies of one document — and a pod whose
		// annotations are large but honest must still be scraped somewhere,
		// or the ceiling silently stops collecting from a workload that did
		// nothing wrong. Its cost is still CHARGED, so the second target is
		// measured against the truth.
		first := len(*d.out) == d.base
		// The pod document is the FLOOR of any target's cost, so once it alone
		// no longer fits, nothing offered afterwards can: refuse without
		// walking the target's own fields. That is the arm a pile-up of
		// distinct URLs runs down, and the cost of refusing must not scale
		// with what is being refused.
		if !first && d.bytes+d.podBytes > scrape.MaxTargetBytesPerPod {
			return d.refuse(targetRefusedBytes)
		}
		cost := scrape.TargetDocBytes(&t, d.podBytes)
		// And the whole document, for the target whose OWN fields are what
		// overflow: a 2 KiB path beside a 16 KiB merged chain is inside every
		// per-field ceiling and still 18 KiB per target.
		if !first && d.bytes+cost > scrape.MaxTargetBytesPerPod {
			return d.refuse(targetRefusedBytes)
		}
		d.bytes += cost
		d.urlOwner[t.URL] = len(*d.out)
		*d.out = append(*d.out, t)
		return targetAccepted
	}
	// pod source wins over service source, and a monitor wins over both.
	held := &(*d.out)[i]
	if configuredTarget(&t) && !configuredTarget(held) {
		before, svcBefore := scrape.TargetOwnBytes(held), held.Service
		carryForward(&t, held)
		// The UPGRADE is a swap, and what it swaps in is exactly the expensive
		// half: a monitor target carries a merged relabel chain (up to 16 KiB),
		// a contributor list and auth references that the annotation target it
		// displaces never had, and a Service VIEW the pod door's target never
		// carried. The configuration is charged, never refused — refusing it
		// would drop the monitor's declaration rather than bound a response,
		// and the URL is served either way — so the pod's remaining budget
		// stays honest and the next NEW url is measured against the truth. The
		// VIEW is the one part that is refused, when it does not fit (see
		// boundSwapView): its labels are bounded by nothing but the Service
		// object's size, so a charge alone let one fat Service ride sixteen
		// swaps into the response.
		grown := d.boundSwapView(i, &t, svcBefore, scrape.TargetOwnBytes(&t)-before)
		*held = t
		d.chargeSwap(grown)
		return targetAccepted
	}
	// The holder keeps the URL — and carries forward from the target it
	// displaces, on this path too. The preference is about which DECLARATION
	// wins, never about discarding a view of the endpoint the winner has no way
	// to hold: a pod-annotation target arrives first and carries no Service, so
	// the service-annotation target for the same URL used to be dropped whole
	// and every sample of that endpoint lost k8s.service.name and
	// k8s.service.uid — the identical loss carryForward was written for on the
	// replace path, on the arm nobody had looked at. Which Service donates is
	// deterministic: matchingServices preserves the snapshot's name order.
	//
	// carryForward fills an ABSENT view and nothing else, so a holder that
	// already has one — or an offer that brings none — leaves the target
	// exactly as it is, with nothing to measure or charge.
	if held.Service != nil || t.Service == nil {
		return targetAccepted
	}
	carryForward(held, &t)
	// The Service view arrives on a target the pod door has already charged
	// without one, and it is the tenant-sized part: bounded here exactly as on
	// the arm above. It is also the ONLY part of the target this arm changes,
	// so its own size is exactly what the target grew by — one walk of the
	// view's maps, where measuring the whole target before and after walked
	// everything else twice for a difference that could only ever be the view.
	d.chargeSwap(d.boundSwapView(i, held, nil, scrape.ServiceViewBytes(held.Service)))
	return targetAccepted
}

// boundSwapView keeps a swap arm from carrying a Service view past the pod's
// byte budget, and returns what the held target GREW by after that decision,
// for chargeSwap. cand is what the held target at index i becomes; svcBefore
// is the held target's Service view BEFORE the swap, and grown is how many
// bytes the swap adds to its own size (scrape.TargetOwnBytes) as offered.
//
// The swap arms refuse nothing else, and must not: the monitor's relabel,
// auth and cadence declaration is configuration, and the URL is served either
// way. But the Service VIEW is not configuration — it is attribution copied
// from services.Index, whose labels are bounded by nothing but the Service
// object's size limit (the WHAT REMAINS note at scrape.MaxTargetBytesPerPod),
// and a charge alone cannot bound it: measured, one scrape-annotated Service
// carrying ~600 KB of labels in front of a 16-port pod put a copy on every one
// of the sixteen targets through these arms, a 9.7 MB node-targets body
// against the 256 KiB budget, where the new-URL arm would have held the same
// view to one target.
//
// So when the view is what grows and the growth does not fit, the target keeps
// the view's IDENTITY — name, namespace, UID (scrape.ServiceIdentityView) — and
// loses its labels and annotations: k8s.service.name and k8s.service.uid still
// reach every sample, and only the tenant-sized half is refused. The pod's
// FIRST target is exempt, as it is from every other byte refusal (its whole
// document is unconditional). The refusal is recorded per TARGET (trimmed)
// for /v1/explain and warned per Service by nodeTargets.
func (d *targetDedup) boundSwapView(i int, cand *kubemeta.ScrapeTarget, svcBefore *kubemeta.Service, grown int) int {
	if cand.Service == svcBefore {
		// The view is the one the target already had (carried forward onto a
		// PodMonitor target): whatever was decided for it stands.
		return grown
	}
	// A different view replaces the old one whole, so an earlier refusal on
	// this URL no longer describes what is served; the decision below does.
	d.untrimView(cand.URL)
	if i == d.base || cand.Service == nil || d.bytes+grown <= scrape.MaxTargetBytesPerPod {
		return grown
	}
	view := scrape.ServiceViewBytes(cand.Service)
	if view <= scrape.ServiceViewBytes(svcBefore) {
		return grown // the view is not what grows; the rest is charged, never refused
	}
	cand.Service = scrape.ServiceIdentityView(cand.Service)
	d.trimmed = append(d.trimmed, viewTrim{url: cand.URL, svc: cand.Service})
	return grown - view + scrape.ServiceViewBytes(cand.Service)
}

// untrimView forgets an earlier view refusal on url, and the Service it named
// (see boundSwapView). Cold: the list is empty unless a view has been refused
// on this pod.
func (d *targetDedup) untrimView(url string) {
	if i := d.trimmedAt(url); i >= 0 {
		d.trimmed = slices.Delete(d.trimmed, i, i+1)
	}
}

// trimmedAt is url's index in trimmed, or -1. A URL appears at most once:
// boundSwapView untrims a URL before it can record it again.
func (d *targetDedup) trimmedAt(url string) int {
	for i := range d.trimmed {
		if d.trimmed[i].url == url {
			return i
		}
	}
	return -1
}

// charge spends n bytes of THIS pod's byte budget on a target the accumulator
// already holds — the MERGE arm, where nothing is appended and the count
// ceiling has nothing to look at.
//
// The budget's claim is that it charges the whole target document, and until
// this existed that claim stopped at the merge: a target N monitors fold into
// grows by its merged relabel chain and its contributor list, both bounded but
// both on TOP of the budget (see scrape.MergeReport.Bytes for the size). It
// refuses nothing — a refused merge would drop relabel rules a monitor asked
// for, i.e. change what is EXPORTED in order to bound a response — it only
// makes the pod's remaining budget honest, so the next NEW url is measured
// against what is really being served.
func (d *targetDedup) charge(n int) { d.bytes += n }

// chargeSwap spends what a target GREW by when the accumulator replaced it, or
// enriched it in place, on a URL it already held: add's upgrade and
// holder-keeps arms, which change what is served without appending anything
// (after boundSwapView has refused a view that would not fit).
//
// The growth is floored at zero rather than refunded. A swap that shrinks the
// target is not a reason to hand budget back — the budget bounds the RESPONSE,
// and a door that can lower it invites a shrink-then-grow sequence to spend
// more than the ceiling admits — and the direction that matters is the other
// one anyway: nothing here may leave the budget understating what is served.
func (d *targetDedup) chargeSwap(grown int) {
	if grown > 0 {
		d.bytes += grown
	}
}

// refuse records one refusal by ceiling v and returns it. Both ceilings move
// the SAME counter — a refused target is a refused target, and the rate an
// operator alerts on is "endpoints this node is not scraping" — while
// cappedBySize keeps the two apart for the warning and for /v1/explain, which
// have to name the remedy.
func (d *targetDedup) refuse(v targetVerdict) targetVerdict {
	d.capped++
	if v == targetRefusedBytes {
		d.cappedBySize++
	}
	if !d.diagnostic {
		// See diagnostic: explain derives through this same seam, and a
		// read-only diagnostic must not move a decision counter.
		obs.ScrapeTargetsCapped.Inc()
	}
	return v
}

// carryForward moves the fields the losing target had and the winner lacks.
//
// Today that is exactly the Service: a PODMONITOR selects pods directly and its
// targets carry none, so replacing a service-annotation target with one wholesale
// stripped k8s.service.name and k8s.service.uid from every sample of that
// endpoint (promscrape's fillTargetResource reads target.Service). The
// preference's own justification — that the winner carries strictly MORE — is
// true of the ServiceMonitor arm, which sets Service, and false both of the
// PodMonitor one and of a pod-annotation target displacing a service-annotation
// one, which is why add calls this on both of its paths.
func carryForward(winner, loser *kubemeta.ScrapeTarget) {
	if winner.Service == nil {
		winner.Service = loser.Service
	}
}

// configuredTarget reports whether t carries endpoint configuration that only
// a ServiceMonitor/PodMonitor can supply. Losing any of it to URL dedup changes
// what is scraped or what is exported: without the bearer token the scrape 401s
// (total metric loss for the target), without insecureSkipVerify an https
// endpoint with a private CA fails, and without the metricRelabelings the
// series a drop rule targets are exported anyway.
func configuredTarget(t *kubemeta.ScrapeTarget) bool {
	// Monitor != "" is the test, NOT a list of the config fields that happen to
	// exist today: an enumeration silently goes stale every time an endpoint
	// field is interpreted (it already had, for basicAuth, authorization, the
	// tlsConfig material and interval/scrapeTimeout — each of which was being
	// dropped by the dedup this function exists to prevent). Only monitor-derived
	// targets carry endpoint configuration at all, so the source IS the signal.
	return t.Monitor != ""
}
