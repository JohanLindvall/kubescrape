package server

import "github.com/JohanLindvall/kubescrape/internal/servicemonitors"

// monitorOffers records which monitor endpoint DECLARATIONS have already been
// offered to which URL while building one pod's targets, so each is offered
// exactly once.
//
// It exists because nodeTargets walks the monitor endpoints once per matched
// SERVICE — a monitor selects Services, and the endpoint's `port` names a
// Service port, so the sweep cannot simply be hoisted out of that loop — while
// scrape.MergeMonitorEndpoint is a FOLD: the second offer of the same
// declaration appends its relabel chain a second time and reports its auth
// conflict a second time. A pod behind two Services therefore served the union
// doubled, behind three tripled, where the API promises the UNION; and one
// conflicting monitor counted obs.MonitorTargetShadowed once per Service while
// the throttled warning beside it (correctly) fired once. With ONE Service the
// fold is exactly the union, which is why nothing caught either for so long.
//
// The dedup lives here rather than inside the merge because this is the only
// layer that can be EXACT. The merge sees a target and an endpoint; "has this
// declaration already been folded in" is not a question the served target can
// answer — nothing on the wire records fold boundaries — so every test derived
// from the accumulated chain is a guess, and the guess that was tried (the
// endpoint's chain appearing as a contiguous run) dropped genuine contributors
// from the wire-visible `monitors` list. Here the identity is the declaration
// itself: the same *servicemonitors.Endpoint, which is a stable pointer into
// the indexed monitor (monitoredServices hands out &m.Endpoints[i], the same
// address for every Service that monitor selects), keyed together with the URL
// because the SAME endpoint legitimately resolves to different URLs through
// two Services that spell its port differently — those are two targets, not a
// repeat.
//
// The PodMonitor sweep does not consult this and does not need to: a PodMonitor
// selects PODS, so nodeTargets walks each matching monitor's endpoints exactly
// once per pod with no enclosing per-Service loop, and there is no second offer
// to suppress. A PodMonitor endpoint colliding with a ServiceMonitor one is a
// different declaration and merges, as it should.
type monitorOffers struct {
	seen map[monitorOfferKey]struct{}
}

type monitorOfferKey struct {
	url string
	ep  *servicemonitors.Endpoint
}

// reset points the set at a fresh pod, and MUST be called per pod: the dedup
// is per pod for the same reason targetDedup's is — two hostNetwork pods
// legitimately share the node IP and every URL on it, and one monitor endpoint
// describes both, so a set carried across pods would suppress the second pod's
// genuine first offer. The map itself is reused (clear, not a new map), so a
// request allocates one whatever the pod count; clear on a nil map is a no-op,
// so the allocation stays where first() puts it and a node with no monitors
// never makes one.
func (o *monitorOffers) reset() {
	clear(o.seen)
}

// first reports whether this is the FIRST time this endpoint declaration is
// offered to this URL for the current pod, and records it. A false answer
// means the caller has already claimed or merged exactly this declaration and
// must skip it entirely.
func (o *monitorOffers) first(url string, ep *servicemonitors.Endpoint) bool {
	if o.seen == nil {
		o.seen = make(map[monitorOfferKey]struct{}, 4)
	}
	k := monitorOfferKey{url: url, ep: ep}
	if _, dup := o.seen[k]; dup {
		return false
	}
	o.seen[k] = struct{}{}
	return true
}
