package scrape

import "github.com/JohanLindvall/kubescrape/pkg/kubemeta"

// MaxTargetBytesPerPod bounds the BYTES one pod's scrape targets may add to the
// /v1/nodes/{node}/targets document, beside MaxPortsPerPod's bound on their
// COUNT. Like that one it is enforced where targets ACCUMULATE
// (server.targetDedup.add), so it covers every door at once.
//
// MaxPortsPerPod models the per-target cost as "the ~2 KiB pod document", and
// that model is the hole: every ScrapeTarget embeds the WHOLE pod by value and
// the document re-marshals it once per target, while the pod's own annotations
// and labels were bounded by the API server only at its 256 KiB per-object
// limit. (Annotations are bounded at the SOURCE now — kubemeta.MaxAnnotationBytes
// — but labels still are not, and cannot be; see WHAT REMAINS below.) Measured through the real derivation, with every other ceiling on this
// path (the path bytes, the per-endpoint chain, the merged chain, the
// contributor list) fully respected: ONE pod carrying a 200 KiB annotation,
// prometheus.io/scrape=true and a 16-entry port annotation yielded 16 targets
// and a 3,283,798-byte body — ten such pods on a node is ~33 MB per agent poll,
// re-derived and re-marshalled every cycle, in a singleton the chart requests
// 128Mi for and deliberately gives no memory limit. writeCached must build the
// body to hash its ETag, so the 304 path does not save it either.
//
// This is the ceiling that ends that shape by CONSTRUCTION rather than by
// adding a sixth per-field bound: the charge is the whole target document, so
// every present and future per-target string is inside it.
//
// The FIRST target of a pod is unconditional — the budget bounds the
// MULTIPLIER, never the workload. Pod annotations are legitimate attribution
// data (kubemeta.FilterAnnotations already drops the deploy-tool copies of the
// applied object, which are the only ones that are pure bulk), and refusing a
// fat-but-honest pod's ONLY target would silently stop scraping a workload that
// did nothing wrong. So a pod's contribution is bounded by
// max(one target, this budget) and the node's response stays proportional to
// the pods on it — which is what the store already holds and /v1/pods/... already
// serves — instead of to the pods times their ports.
//
// 256 KiB: at the ~2 KiB document MaxPortsPerPod models, all 16 targets fit
// eight times over, and a pod whose document measures 16 KiB — already far past
// any real one — still gets all 16. The attack shape above gets exactly one.
//
// A refused target is NOT scraped, so it is counted (obs.ScrapeTargetsCapped,
// the same counter the count ceiling moves) and named by /v1/explain through
// SizeCeilingNote, which says the pod's measured size so the two refusals are
// never confused.
const MaxTargetBytesPerPod = 256 << 10

// THE OTHER CEILINGS ON THIS PATH, and why a byte budget does not retire any of
// them. Five bounds now overlap here, which is worth stating deliberately
// rather than leaving to be rediscovered:
//
//   - MaxPortsPerPod (16 targets) bounds the COUNT, and count is not bytes in
//     either direction: a pod with a 300-byte document would fit ~800 targets
//     inside this budget, and a target's cost is not only its bytes — it is a
//     scrape the agent schedules, an entry in the dedup map, and a member of
//     every per-pod scan. KEEP.
//   - MaxTargetPathBytes (2 KiB) and the monitor door's parse-time field bounds
//     (servicemonitors.enforceFieldBounds) refuse a FIELD, and a per-field
//     refusal is the better DIAGNOSTIC: PathRefusedNote names the annotation and
//     its size, where a byte refusal can only say the pod is too large overall.
//     They also bind where this one cannot — on the pod's FIRST target, which is
//     unconditional, and (for the monitor door) on what the INDEX retains
//     between requests rather than on one response. KEEP.
//   - The merged-chain bounds (MaxRelabelChainRules/MaxRelabelChainBytes) and
//     MaxContributorsPerTarget bound growth on the MERGE arm, where no target
//     is added. This budget now CHARGES that growth too (MergeReport.Bytes →
//     server.targetDedup.charge), but it cannot refuse it: a refused merge
//     drops relabel rules a monitor asked for, i.e. changes what is EXPORTED to
//     bound a response. So those two remain the only thing that can say no on
//     that arm. KEEP.
//
//     This bullet used to say the merge arm charges nothing "because a merge
//     costs no copy of the pod document". True of the pod document, incomplete
//     about the target: a fully merged target grows by 16 KiB of chain plus 32
//     contributor names of up to 317 bytes, ~26 KiB, and MaxPortsPerPod of them
//     ~400 KiB — bounded, deterministic and needing >=32 monitors on one URL, so
//     never the unbounded shape, but strictly on top of a budget whose claim was
//     that it charges the whole target document. A comment that claims something
//     narrower than the truth is how the next sibling hides, which is the whole
//     lesson of this campaign; the claim is now true instead.
//
// So: nothing is redundant, and the division of labour is now nameable — this
// budget is the BACKSTOP that holds whatever the per-field bounds have not
// thought of, and they remain the DIAGNOSTICS and the merge-arm REFUSALS.
//
// WHAT REMAINS, said plainly.
//
// The first target of every pod is unconditional, so the response is at least
// one pod document per scrapeable pod on the node. That base cost is inherent
// to the endpoint — it is the store's own content, which /v1/pods/{ns}/{name}
// already serves — so what matters is that ONE pod document is now a constant
// this service chose rather than a product of numbers a tenant picks. It was
// not, and the byte ceiling could not see it: a pod document carries the pod's
// own annotations, its NAMESPACE's (copied per pod, since /v1/pods must serve a
// self-contained document), and ONE set per resolved ownerReference — and
// Kubernetes bounds neither the ownerReferences count nor what an owner may
// annotate, while one fat owner is shared by every pod that names it. Measured:
// 100 fat ReplicaSets named by every pod put ~25 MB in each pod document and
// ~125 MB in a 5-pod node's response. Three ceilings at the SOURCE close it —
// kubemeta.MaxAnnotationValueBytes (8 KiB, one value, refused whole),
// kubemeta.MaxAnnotationBytes (16 KiB, one object's set) and owners.MaxOwners
// (8 chain entries) — so a pod document's related-object metadata is bounded by
// (8 owners + namespace + the pod) x 16 KiB, and the response by that times the
// pods on the node.
//
// LABELS are NOT bounded, deliberately, and that is the honest remainder. They
// are selection input — Service selectors and PodMonitor selectors match on
// them — so no filter can know which label is load-bearing, and refusing one
// would silently change WHAT IS SCRAPED in order to bound a response. Each
// label is small by API-server validation (a value of at most 63 bytes), but
// their COUNT is bounded only by the object's ~1.5 MiB ceiling, so a pod, an
// owner or a namespace can still carry ~1 MiB of them. That is what this budget
// binds on today (internal/server's TestFatPodAnnotationCannotMultiplyIntoThe
// NodeTargetsDocument builds exactly that shape), and it is why the budget is
// still the backstop and not a retired guard.

// jsonMemberOverhead is the framing one JSON member costs beyond the bytes of
// its key and value: a quote pair on each, the colon, and the comma. Exact for
// every member but the last of an object, which pays no comma — so charging it
// unconditionally keeps the estimate on the safe side.
const jsonMemberOverhead = 6

// The members of each document whose size does not depend on any input: the
// member NAMES, the RFC3339 timestamps, the booleans and the numbers. Charged
// as one constant per document rather than key by key, and each is an
// over-estimate of the empty document (TestDocBytesIsNeverUnderTheMarshalledSize
// pins that, and pins the constants against the real encoder).
const (
	podFixedBytes       = 512
	containerFixedBytes = 288
	ownerFixedBytes     = 128
	portFixedBytes      = 64
	targetFixedBytes    = 448
	serviceFixedBytes   = 96
	relabelFixedBytes   = 64
)

// PodDocBytes estimates the marshalled size of ONE pod document — the cost
// MaxTargetBytesPerPod charges per target, since every ScrapeTarget embeds it
// by value.
//
// It is an ESTIMATE by design, and an over-estimate for any document whose
// strings need no JSON escaping: the alternative is a json.Marshal per pod,
// which allocates the whole document (200 KiB for the shape this bound exists
// for) on a path that runs once per pod per agent poll per node. This walk
// allocates nothing and touches only the entries, not their bytes.
//
// The one direction it can UNDER-count is escaping: a value made entirely of
// quotes or control characters marshals to 2-6x its raw length, so the
// effective ceiling in that corner is that much looser. It is still a bound,
// and still the multiplier closed — the 16x this exists to remove is larger
// than the 6x it concedes.
//
// Every string-bearing field of kubemeta.Pod is charged here;
// TestDocSizeChargesEveryField fails when one is added and this is not updated.
func PodDocBytes(p *kubemeta.Pod) int {
	n := podFixedBytes +
		len(p.Name) + len(p.Namespace) + len(p.UID) + len(p.NodeName) +
		len(p.PodIP) + len(p.HostIP) + len(p.Phase)
	for _, ip := range p.PodIPs {
		n += len(ip) + 3 // "ip",
	}
	n += stringMapBytes(p.Labels) + stringMapBytes(p.Annotations)
	// OwnersOmitted is an int, so it costs its member framing and a handful of
	// digits — inside podFixedBytes, which over-estimates the empty document
	// (TestDocBytesIsNeverUnderTheMarshalledSize pins that). It is charged
	// explicitly here so TestDocSizeChargesEveryField cannot pass by accident
	// on a field that later grows a string.
	_ = p.OwnersOmitted
	if m := p.NamespaceMetadata; m != nil {
		n += objectMetaBytes(m)
	}
	for i := range p.Owners {
		o := &p.Owners[i]
		n += ownerFixedBytes + len(o.APIVersion) + len(o.Kind) + len(o.Name) + len(o.UID) +
			stringMapBytes(o.Labels) + stringMapBytes(o.Annotations)
	}
	for i := range p.Containers {
		c := &p.Containers[i]
		n += containerFixedBytes + len(c.Name) + len(c.Type) + len(c.ID) + len(c.RuntimeID) +
			len(c.Image) + len(c.ImageID) + len(c.State) + len(c.WaitingReason)
		for j := range c.Ports {
			n += portFixedBytes + len(c.Ports[j].Name) + len(c.Ports[j].Protocol)
		}
	}
	return n
}

// TargetDocBytes estimates the marshalled size of one whole ScrapeTarget: its
// own fields plus the pod document it embeds. podBytes is PodDocBytes for that
// pod, passed in because every target of a derivation embeds the SAME pod and
// the accumulator measures it once (see server.targetDedup.podBytes).
func TargetDocBytes(t *kubemeta.ScrapeTarget, podBytes int) int {
	return podBytes + TargetOwnBytes(t)
}

// TargetOwnBytes estimates the marshalled size of a ScrapeTarget MINUS its
// embedded pod: the URL, the path, the auth references, the merged relabel
// chain, the contributor list and the Service view.
//
// These are the fields the four per-field ceilings on this path bound
// individually (MaxTargetPathBytes, the per-endpoint and merged chain bounds,
// MaxContributorsPerTarget). Charging them here as well is what makes the byte
// budget hold BY CONSTRUCTION for fields nobody has thought of yet: a new
// per-target string is inside the budget the day it is added, whether or not it
// ever gets a ceiling of its own.
func TargetOwnBytes(t *kubemeta.ScrapeTarget) int {
	n := targetFixedBytes +
		len(t.URL) + len(t.Scheme) + len(t.Address) + len(t.Path) + len(t.Source) +
		len(t.Monitor) + len(t.AuthSecret) + len(t.BasicAuthUser) + len(t.BasicAuthPass) +
		len(t.AuthType) + len(t.AuthCredentials) + len(t.TLSCA) + len(t.TLSCert) +
		len(t.TLSKey) + len(t.TLSServerName) + len(t.Interval) + len(t.ScrapeTimeout)
	for _, m := range t.Monitors {
		n += len(m) + 3
	}
	for i := range t.MetricRelabelings {
		r := &t.MetricRelabelings[i]
		n += relabelFixedBytes + len(r.Action) + len(r.Regex)
		for _, l := range r.SourceLabels {
			n += len(l) + 3
		}
	}
	if s := t.Service; s != nil {
		n += serviceFixedBytes + len(s.Name) + len(s.Namespace) + len(s.UID) +
			stringMapBytes(s.Labels) + stringMapBytes(s.Annotations)
	}
	return n
}

// objectMetaBytes is the namespace-metadata half of a pod document.
func objectMetaBytes(m *kubemeta.ObjectMeta) int {
	return jsonMemberOverhead + len("namespaceMetadata") + len(`{"uid":""}`) + len(m.UID) +
		stringMapBytes(m.Labels) + stringMapBytes(m.Annotations)
}

// stringMapBytes is the marshalled size of a labels/annotations map. It walks
// the ENTRIES and not their bytes, so a 200 KiB annotation costs one addition.
func stringMapBytes(m map[string]string) int {
	if len(m) == 0 {
		return 0
	}
	n := jsonMemberOverhead + len("annotations") + 2 // the member itself, and {}
	for k, v := range m {
		n += len(k) + len(v) + jsonMemberOverhead
	}
	return n
}
