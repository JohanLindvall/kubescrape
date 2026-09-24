package server

// GET /v1/explain/{namespace}/{name}: why is this pod (not) scraped?
//
// The single most common operational question this service answers badly from
// its normal responses — a pod missing from a target list looks the same
// whether the annotation is absent, the port entry resolves to nothing, the
// pod is terminating, or a Service selector misses. This handler walks the
// SAME decision chain nodeTargets walks, one pod at a time, and reports every
// verdict along the way. Diagnostic and read-only as far as the TARGET
// DERIVATION goes: it moves none of that derivation's decision counters and
// emits none of its warnings (reporting a monitor auth conflict from here would
// double-count what the targets path already reports), and it is deliberately
// per-pod and unindexed — the cost is one enrichment and a handful of matches.
//
// What it does NOT suppress is the owner and namespace ENRICHMENT it shares
// with /v1/pods: that resolves through owners.Resolver exactly as that route
// does, so an owner that fails to resolve counts
// kubescrape_owner_resolve_failures_total (owners_capped included) and an
// oversized annotation set kubescrape_metadata_annotations_omitted_total, once
// per request, and the resolver may log its own throttled Warn. Those report
// real object state rather than a decision this handler makes, and /v1/pods
// moves them the same way; a counter-free enrichment path would buy nothing.
// Like every route it also moves kubescrape_http_requests_total. And it answers
// 200 only once the caches have synced — before that, requireReady's 503.
//
// Two of the three decision signals on this derivation are suppressed simply by
// not being CALLED from here (reportInstanceCollision, hence
// obs.TargetIdentityCollisions; the auth-conflict warn and
// obs.MonitorTargetShadowed). The third lives INSIDE targetDedup.add, so it
// takes a flag — d.diagnostic below — which suppresses the counter while still
// filling d.capped, because the refusals themselves are exactly what this
// document has to report (doc.CappedTargets and the per-entry notes). Without
// it a browser form or a dashboard polling this endpoint added a second,
// human-driven source to a rate operators read as "endpoints refused per scrape
// cycle" — on the very pods the counter's help text sends them here to inspect.

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/JohanLindvall/kubescrape/internal/clip"
	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// explainDoc is the response shape. Fields are omitted when they do not
// apply, so the document reads top-down as the decision chain: found →
// scrapeable → pod annotations → services → monitors → final targets.
type explainDoc struct {
	Namespace string `json:"namespace"`
	Pod       string `json:"pod"`
	Found     bool   `json:"found"`
	// Hint carries the one next step when the walk ends early (pod not
	// found, not scrapeable, nothing annotated).
	Hint string `json:"hint,omitempty"`

	// AnnotationsOmitted is the pod's own kubescrape.io/annotations-omitted
	// note, present only when kubemeta's annotation ceilings refused part of
	// its annotations. Lifted onto the head of the document because it is the
	// answer to a question this endpoint would otherwise answer wrongly: an
	// oversized prometheus.io/* annotation is refused at the SOURCE, so
	// podAnnotated reads false and every field below describes a pod nobody
	// annotated. The note names the keys and the ceiling. It is bounded at the
	// source (kubemeta.maxOmittedNamedBytes) and clipped again here like every
	// other echoed value.
	AnnotationsOmitted string `json:"annotationsOmitted,omitempty"`
	// OwnersOmitted mirrors kubemeta.Pod.OwnersOmitted: how many
	// ownerReferences owners.MaxOwners refused to describe. It does not change
	// what is scraped, but it changes attribution (service.name and the
	// workload labels come off the chain), so a reader diagnosing a pod's
	// identity has to be able to see that the chain is short.
	OwnersOmitted int `json:"ownersOmitted,omitempty"`

	Scrapeable       bool                  `json:"scrapeable,omitempty"`
	NotScrapeableWhy []string              `json:"notScrapeableWhy,omitempty"`
	PodAnnotated     bool                  `json:"podAnnotated,omitempty"`
	ScrapeAnnotation string                `json:"scrapeAnnotation,omitempty"`
	PortAnnotation   string                `json:"portAnnotation,omitempty"`
	PortAnnotated    bool                  `json:"portAnnotated,omitempty"`
	PortEntries      []scrape.PortVerdict  `json:"portEntries,omitempty"`
	DeclaredPorts    []scrape.DeclaredPort `json:"declaredPorts,omitempty"`
	Services         []explainService      `json:"services,omitempty"`
	MonitorsEnabled  bool                  `json:"monitorsEnabled"`
	PodMonitors      []explainMonitor      `json:"podMonitors,omitempty"`
	Targets          []explainTarget       `json:"targets"`

	// The ...NotShown counters below are the ONE way a truncated list is told
	// from a complete one: every list this document materialises is bounded
	// (see the explainBudget block), and a reader who cannot tell "there are
	// three" from "the first three of two thousand" is worse off than one
	// given nothing. NotShown is their sum, for a reader who only skims the
	// head — the shape CappedTargets already has.
	NotShown              int `json:"notShown,omitempty"`
	PortEntriesNotShown   int `json:"portEntriesNotShown,omitempty"`
	DeclaredPortsNotShown int `json:"declaredPortsNotShown,omitempty"`
	ServicesNotShown      int `json:"servicesNotShown,omitempty"`
	PodMonitorsNotShown   int `json:"podMonitorsNotShown,omitempty"`

	// MergeCeilings reports, ONCE PER URL, the merge ceilings the endpoints
	// folding into that URL hit — and how many endpoints each one bound. The
	// wording used to be appended to every refused endpoint's note, which is
	// ~370 bytes per endpoint on exactly the input the ceilings exist for: the
	// change that bounded the SERVED document measurably enlarged this one.
	// Bounded by construction — an entry exists only for a URL that HOLDS a
	// target, and targets are capped at scrape.MaxPortsPerPod.
	MergeCeilings []explainMergeCeiling `json:"mergeCeilings,omitempty"`
	// CappedTargets counts the targets the per-pod ceilings refused for this
	// pod — scrape.MaxPortsPerPod on the count and scrape.MaxTargetBytesPerPod
	// on the bytes, the same refusals kubescrape_scrape_targets_capped_total
	// counts on the served path, whose help text sends the operator HERE. Zero (and
	// absent) on the overwhelming majority of pods. The individual refusals
	// are named where they were made — portEntries, services[].portEntries,
	// services[].serviceMonitors[].note, podMonitors[].note — because the
	// count alone cannot say WHICH endpoint went unscraped; this field exists
	// so a reader who only skims the head of the document still learns that
	// the target list below is short by design rather than by misconfiguration.
	CappedTargets int `json:"cappedTargets,omitempty"`
	// CappedTargetsBySize is how many of CappedTargets the BYTE ceiling
	// (scrape.MaxTargetBytesPerPod) refused rather than the count one. The two
	// have different remedies and the byte one binds at a target count that
	// looks perfectly ordinary — a pod serving two targets and reporting
	// cappedTargets: 14 reads as a bug in this document unless it also says
	// which ceiling bound. The per-entry notes carry the full wording,
	// including this pod's measured document size; this field is for the
	// reader who only skims the head.
	CappedTargetsBySize int `json:"cappedTargetsBySize,omitempty"`
	// PodDocumentBytes is the pod document's measured size — the quantity the
	// byte ceiling charges once per target. Reported only when that ceiling
	// actually bound, since it is a derivation detail everywhere else.
	PodDocumentBytes int `json:"podDocumentBytes,omitempty"`
	// ServiceViewsTrimmed counts the targets whose Service view the byte
	// ceiling reduced to the Service's name, namespace and UID: a target that
	// took on a Service view by merging with another door's declaration of the
	// same URL, where the view's labels would have spent more than the pod's
	// budget allows (targetDedup.boundSwapView). Those targets ARE served and
	// still carry k8s.service.name and k8s.service.uid; only the labels and
	// annotations are left off. targets[].serviceViewTrimmed marks which.
	ServiceViewsTrimmed int `json:"serviceViewsTrimmed,omitempty"`
}

// explainService is one Service whose selector matches the pod.
type explainService struct {
	Name          string               `json:"name"`
	Annotated     bool                 `json:"annotated"`
	PortAnnotated bool                 `json:"portAnnotated,omitempty"`
	PortEntries   []scrape.PortVerdict `json:"portEntries,omitempty"`
	// ServiceMonitors selecting this Service, one entry per endpoint.
	Monitors []explainMonitor `json:"serviceMonitors,omitempty"`

	PortEntriesNotShown int `json:"portEntriesNotShown,omitempty"`
	MonitorsNotShown    int `json:"serviceMonitorsNotShown,omitempty"`
}

// explainMergeCeiling is one URL's merge-ceiling summary: how many monitor
// endpoints folding into that URL had something refused by each ceiling.
type explainMergeCeiling struct {
	URL                string `json:"url"`
	RelabelCapped      int    `json:"relabelCapped,omitempty"`
	ContributorsCapped int    `json:"contributorsCapped,omitempty"`
	// Note carries the ceiling wordings themselves, ONCE for this URL. It is
	// where the explanation lives now: the endpoint entries below it may be
	// truncated (they are bounded, and the ceiling binds precisely when there
	// are more of them than the document lists), so an explanation that only
	// existed on a per-endpoint note could vanish exactly when it is needed.
	Note string `json:"note,omitempty"`
}

// explainMonitor is one monitor ENDPOINT's verdict against this pod.
type explainMonitor struct {
	Monitor  string `json:"monitor"`
	Resolved bool   `json:"resolved"`
	URL      string `json:"url,omitempty"`
	Note     string `json:"note,omitempty"`
}

// explainTarget is one final target the node response would carry for this pod.
type explainTarget struct {
	URL      string   `json:"url"`
	Source   string   `json:"source"`
	Monitor  string   `json:"monitor,omitempty"`
	Monitors []string `json:"monitors,omitempty"`
	// CollidesWith lists the OTHER served targets of this pod that export the
	// same series identity as this one — same host:port, different path or
	// scheme, which the exported (job, instance) cannot tell apart
	// (scrape.InstanceCollisions). Absent on the overwhelming majority of
	// targets, which collide with nothing. The endpoint's whole purpose is
	// "why is this pod (not) scraped, and what does it produce?", and a pod
	// whose two targets overwrite each other's `up` was answered with a list
	// of two targets and no hint that they collapse.
	CollidesWith []string `json:"collidesWith,omitempty"`
	// ServiceViewTrimmed marks a served target whose Service view the byte
	// ceiling reduced to its identity (see explainDoc.ServiceViewsTrimmed).
	ServiceViewTrimmed bool `json:"serviceViewTrimmed,omitempty"`
}

// What one /v1/explain document may MATERIALISE. The derivation below is not
// bounded by any of these — every door is still walked, every target still
// derived, so the parity with nodeTargets holds — only what is written into
// the response is.
//
// This route is unauthenticated by design and per POD, so it is not the fleet
// multiplier the node-targets document is; but "smaller" is not "bounded", and
// every list here is grown by input a namespace tenant supplies: a monitor
// endpoint each, a matching Service each, an entry of a `prometheus.io/port`
// annotation each. Measured through the real handler before these bounds, with
// the served document beside it: 2,000 colliding ServiceMonitors gave a 2,777
// byte targets document and a 750,369 byte explanation; 200 label-matching
// Services of 50 ports each gave a 29 byte targets document (nothing is
// annotated, so nothing is scraped) and a 1,169,615 byte explanation; a
// 20,000-entry port annotation gave 2,099,738 bytes of per-entry verdicts. Any
// pod in the cluster can issue that ~100-byte GET in a loop.
//
// The caps are ceilings on the pathological, not budgets for the normal: a
// real pod is behind one or two Services with a handful of ports, and the
// SERVED ceiling above all of this is scrape.MaxPortsPerPod = 16 targets.
// Everything refused is COUNTED into the ...NotShown fields, never dropped
// silently — the question this endpoint exists to answer ("which of my
// monitors stopped contributing?") is answered wrong by a short list that
// looks complete.
//
// A truncation moves no counter and writes no log line, deliberately and for
// two different reasons. The counter: this handler moves none of the target
// derivation's decision counters (the package comment above says why, and
// which enrichment counters it does share with /v1/pods;
// targetDedup.diagnostic exists to keep it that way), and the pathologies that
// reach these bounds are already counted where they are DERIVED —
// kubescrape_monitor_contributors_capped_total,
// kubescrape_scrape_targets_capped_total, kubescrape_monitor_fields_ignored_total.
// The log: the trigger is an unauthenticated request, so a line per truncation
// is an amplifier of exactly the kind being closed. The report is the document
// itself, which is the one channel whose volume the CALLER pays for.
const (
	// Per DOC / per LIST, in pairs: the doc bound is what holds however the
	// entries are distributed, the list bound is what keeps one Service (or
	// one monitor pile-up) from consuming all of it.
	maxExplainServices          = 16 // doc; a Service is one list
	maxExplainPortEntriesPerDoc = 96
	maxExplainPortEntries       = 32 // per portEntries list
	maxExplainDeclaredPorts     = 64 // doc; declaredPorts is one list
	maxExplainMonitorsPerDoc    = 64
	maxExplainMonitors          = 32 // per serviceMonitors/podMonitors list
	// maxExplainValueBytes bounds one echoed ANNOTATION value. The annotation
	// is the pod's own and is served whole elsewhere, but it is echoed here
	// per request on an unauthenticated route, and 512 bytes is far past any
	// real prometheus.io/port list.
	maxExplainValueBytes = 512
)

// explainBudget bounds one KIND of repeated entry across the document: perList
// keeps one Service (or one monitor) from consuming the whole allowance, and
// perDoc bounds the document however the entries are distributed.
type explainBudget struct {
	perList, perDoc int
	used, hidden    int
}

// room reports whether one more entry may be listed, given how many the
// current list already holds, and charges the document budget when it says
// yes. A `false` is the CALLER's to count into its own ...NotShown field —
// through hide, so the document total stays honest.
func (b *explainBudget) room(inList int) bool {
	if inList >= b.perList || b.used >= b.perDoc {
		return false
	}
	b.used++
	return true
}

// hide records n entries this budget refused.
func (b *explainBudget) hide(n int) { b.hidden += n }

// clipList keeps the prefix of v the budget allows, returning what it refused.
// It is the same decision as room for a list that is already built (the port
// verdicts, which the internal/scrape mirrors produce whole).
func clipList[T any](b *explainBudget, v []T) ([]T, int) {
	room := max(min(b.perList, b.perDoc-b.used), 0)
	if len(v) <= room {
		b.used += len(v)
		return v, 0
	}
	b.used += room
	b.hide(len(v) - room)
	return v[:room], len(v) - room
}

// clipValue bounds an echoed annotation value (see maxExplainValueBytes), on a
// rune boundary: the document is JSON, and a bare byte cut put an invalid
// string into it.
func clipValue(v string) string { return clip.Marked(v, maxExplainValueBytes, "…(truncated)") }

// mergeCeilingRef is what EVERY endpoint a merge ceiling bound carries in its
// own note — the first one on a URL included — in place of the ceiling's
// wording. The full wordings live in internal/scrape (the one spelling), run
// to ~370 bytes each, and were appended to every refused endpoint — so a
// pile-up of colliding monitors, the exact input the ceilings exist for, paid
// for the explanation once per endpoint. The wording is now written once per
// (URL, ceiling), into that URL's mergeCeilings[].note, with the counts beside
// it.
const mergeCeilingRef = "; a merge ceiling bound it — see mergeCeilings for this URL"

// ceilingLeadIn is the subject the scrape wordings are suffixes to; see note.
const ceilingLeadIn = "each endpoint counted here merged into the target for this URL"

// explainCeilings collects the per-URL merge-ceiling summary: the counts, and
// the ceiling wordings emitted ONCE for the URL rather than once per endpoint.
type explainCeilings struct {
	byURL map[string]*explainMergeCeiling
	order []string
}

// note records what rep refused into the URL's mergeCeilings entry — each
// ceiling's wording written there once per (URL, ceiling), every hit counted —
// and returns the suffix for THIS endpoint's note: mergeCeilingRef for every
// endpoint a ceiling bound, "" when nothing was refused. The endpoint's note
// never carries the wording itself, not even the first time.
func (c *explainCeilings) note(url string, rep scrape.MergeReport) string {
	if !rep.RelabelCapped && !rep.ContributorsCapped {
		return ""
	}
	e := c.byURL[url]
	if e == nil {
		if c.byURL == nil {
			c.byURL = map[string]*explainMergeCeiling{}
		}
		e = &explainMergeCeiling{URL: url}
		c.byURL[url] = e
		c.order = append(c.order, url)
	}
	// The wordings are scrape's, verbatim and unrepeated: they are SUFFIXES
	// written to follow a merge note, and "each endpoint counted here merged
	// …; its metricRelabelings are only PARTLY applied — …" is that same
	// sentence with the subject moved to the document level. A second spelling
	// here is the drift internal/scrape/explain.go exists to prevent.
	if rep.RelabelCapped && e.RelabelCapped == 0 {
		e.Note += scrape.MergedRelabelCeilingNote()
	}
	if rep.ContributorsCapped && e.ContributorsCapped == 0 {
		e.Note += scrape.MergedContributorCeilingNote()
	}
	if rep.RelabelCapped {
		e.RelabelCapped++
	}
	if rep.ContributorsCapped {
		e.ContributorsCapped++
	}
	return mergeCeilingRef
}

// list renders the summary in first-encounter order, so the document is stable
// across map iteration like every other part of it.
func (c *explainCeilings) list() []explainMergeCeiling {
	if len(c.order) == 0 {
		return nil
	}
	out := make([]explainMergeCeiling, 0, len(c.order))
	for _, url := range c.order {
		e := *c.byURL[url]
		e.Note = ceilingLeadIn + e.Note
		out = append(out, e)
	}
	return out
}

func (s *Server) handleExplain(w http.ResponseWriter, r *http.Request) {
	if !s.requireReady(w, "") {
		return
	}
	doc, _ := s.explainPod(r.PathValue("namespace"), r.PathValue("name"))
	// 200 even for a miss: the explanation IS the resource this endpoint
	// serves, and `curl -f` hiding the body on a 404 would defeat its purpose.
	s.writeJSON(w, http.StatusOK, doc)
}

// explainPod builds the document, returning the ScrapeTargets it derived
// alongside it. The targets are returned — not merely summarised into the
// document — so explain_parity_test.go can compare them field for field against
// the ones nodeTargets serves: the document exposes the URL, source and monitor
// list, while a fold divergence first shows up in the merged relabel chain and
// cadence, which are not on the document at all.
func (s *Server) explainPod(namespace, name string) (explainDoc, []kubemeta.ScrapeTarget) {
	doc := explainDoc{
		Namespace:       namespace,
		Pod:             name,
		MonitorsEnabled: s.monitors != nil,
		Targets:         []explainTarget{},
	}
	np, ok := s.store.GetPodByName(namespace, name)
	if !ok {
		doc.Hint = "pod not found in the store (wrong namespace/name, or deleted longer than -cache-ttl ago); the store is filled from the pod informer, so a very recently created pod may appear within a second"
		return doc, nil
	}
	doc.Found = true
	pod := np.Pod
	s.enrich(&pod, np.OwnerRefs)

	doc.NotScrapeableWhy = scrape.ScrapeableReasons(pod)
	doc.Scrapeable = len(doc.NotScrapeableWhy) == 0
	doc.PodAnnotated = scrape.OptedIn(pod.Annotations)
	// Echoed, so clipped: an annotation is the pod's own bytes and it is
	// served whole elsewhere, but this route is unauthenticated and re-renders
	// it per request.
	doc.ScrapeAnnotation = clipValue(pod.Annotations[scrape.AnnotationScrape])
	doc.PortAnnotation = clipValue(pod.Annotations[scrape.AnnotationPort])
	// What the pod's document is NOT carrying, and why. Without these two the
	// endpoint answers "why is this pod not scraped?" with a description of a
	// pod whose annotations it silently never saw.
	doc.AnnotationsOmitted = clipValue(pod.Annotations[kubemeta.OmittedAnnotation])
	doc.OwnersOmitted = pod.OwnersOmitted
	// One budget per KIND of list; see the explainBudget block. The doors
	// below are walked in full whatever these say — only the document is
	// bounded.
	portBudget := explainBudget{perList: maxExplainPortEntries, perDoc: maxExplainPortEntriesPerDoc}
	declaredBudget := explainBudget{perList: maxExplainDeclaredPorts, perDoc: maxExplainDeclaredPorts}
	svcBudget := explainBudget{perList: maxExplainServices, perDoc: maxExplainServices}
	monBudget := explainBudget{perList: maxExplainMonitors, perDoc: maxExplainMonitorsPerDoc}
	var ceilings explainCeilings

	doc.DeclaredPorts, doc.DeclaredPortsNotShown = clipList(&declaredBudget, scrape.DeclaredPorts(pod))
	doc.PortEntries, doc.PortAnnotated = scrape.ExplainPodPorts(pod)

	// The same request-scoped snapshots nodeTargets takes.
	monitored := s.monitoredServices()
	svcs := s.services.InNamespaces([]string{pod.Namespace})
	matched := matchingServices(svcs[pod.Namespace], pod.Labels, nil)
	podMonitors := podMonitorsFor(pod, s.allPodMonitors(), nil)

	// Targets, through the same dedup/merge machinery as the node response —
	// minus its decision counters and warnings (see the package comment above).
	var targets []kubemeta.ScrapeTarget
	var d targetDedup
	// Read-only: the ceiling's refusals still fill d.capped, which the document
	// reports below, but obs.ScrapeTargetsCapped belongs to the served
	// derivation (see targetDedup.diagnostic and the package comment).
	d.diagnostic = true
	d.reset(&targets)
	// The SAME offer dedup nodeTargets uses, not a second one shaped like it:
	// the monitor endpoints are swept once per matched SERVICE here too, and
	// scrape.MergeMonitorEndpoint is a fold, so without it a pod behind two
	// Services was explained with a contributor list and a relabel chain the
	// served target does not have — the drift this endpoint exists to make
	// impossible. explain_parity_test.go drives both paths over 1, 2 and 3
	// Services.
	var offers monitorOffers
	offers.reset()
	// refused is reused per door: the ports THIS door produced and the
	// accumulator turned away. Only the accumulator knows — the mirrors in
	// internal/scrape resolve one door at a time and the ceiling spans them
	// all, so a pod annotation and a Service each individually under it can
	// still overflow together (see targetDedup.add).
	// The VERDICT and not just the fact of refusal: the two per-pod ceilings
	// have two wordings and two remedies (targetVerdict.note).
	refused := map[int32]targetVerdict{}
	for _, t := range scrape.PodTargets(pod) {
		if v := d.add(t); !v.ok() {
			refused[t.Port] = v
		}
	}
	// The COUNT ceiling cannot bind here — podPorts pre-caps at MaxPortsPerPod
	// and this door adds first, at d.base — but the BYTE one can, from this
	// pod's SECOND target onwards: only the FIRST is unconditional (see
	// targetDedup.add), so a pod carrying a large document and a multi-entry
	// port annotation has entries 2..N refused at this very door.
	// podbytesbomb_test.go's TestFatPodAnnotationCannotMultiplyIntoTheNode
	// TargetsDocument is the case that reaches it, and without these notes it
	// would answer "why is my 16th port not scraped?" with `ports: [n]` and no
	// note at all — the "it is" inversion the ceiling notes exist to prevent.
	noteCapped(doc.PortEntries, refused, d.podBytes)
	// And the door-level opt-in, the one whole-door gate the port mirrors
	// leave to their caller (see scrape.NoteNotOptedIn): PodTargets refuses an
	// unannotated pod before it resolves a port, so its declared ports would
	// read "every declared port is a target" beside an empty target list. The
	// pod-level exclusion is the mirror's own and wins when both apply — on a
	// pod that is not scrapeable its entries carry no ports left to retract.
	if doc.Scrapeable && !doc.PodAnnotated {
		scrape.NoteNotOptedIn(doc.PortEntries, "the pod")
	}
	// Clipped AFTER the verdicts are written, never before: the ceiling's
	// refusals land on the TAIL of the list, so clipping first would hide
	// exactly the entries the notes exist for.
	doc.PortEntries, doc.PortEntriesNotShown = clipList(&portBudget, doc.PortEntries)
	// Whether ANY matched Service opts this pod in, accumulated as the
	// derivation runs. The hint below must not read this back off doc.Services:
	// that list is clipped to svcBudget, so a pod whose only annotated Service
	// sorts 17th would be told, flatly, that nothing opts it into scraping —
	// the false negative the ...NotShown counters exist to prevent, in the one
	// field an operator reads first.
	svcOptIn := false
	for _, svc := range matched {
		// Whether this Service is LISTED. The derivation below runs either
		// way — a Service that opts the pod in still contributes its targets,
		// and skipping that would make the explanation disagree with the node
		// response, which is the one thing this endpoint must never do.
		show := svcBudget.room(len(doc.Services))
		es := explainService{
			Name:      svc.Name,
			Annotated: scrape.OptedIn(svc.Annotations),
		}
		if serviceOptsIn(svc, monitored) {
			svcOptIn = true
		}
		if show {
			es.PortEntries, es.PortAnnotated = scrape.ExplainServicePorts(pod, svc)
		}
		clear(refused)
		for _, t := range scrape.ServiceTargets(pod, svc) {
			if v := d.add(t); !v.ok() {
				refused[t.Port] = v
			}
		}
		noteCapped(es.PortEntries, refused, d.podBytes)
		if doc.Scrapeable && !es.Annotated {
			scrape.NoteNotOptedIn(es.PortEntries, "this Service")
		}
		es.PortEntries, es.PortEntriesNotShown = clipList(&portBudget, es.PortEntries)
		for _, sme := range monitored[svc.UID] {
			// Same rule: the endpoint is offered, merged and counted whatever
			// the budget says — only the VERDICT may be left out.
			em := s.explainMonitorEndpoint(&d, &offers, &ceilings, &pod, svc, sme)
			if show && monBudget.room(len(es.Monitors)) {
				es.Monitors = append(es.Monitors, em)
			} else {
				es.MonitorsNotShown++
			}
		}
		monBudget.hide(es.MonitorsNotShown)
		if !show {
			doc.ServicesNotShown++
			continue
		}
		doc.Services = append(doc.Services, es)
	}
	svcBudget.hide(doc.ServicesNotShown)
	// No offer dedup on this sweep, exactly as in nodeTargets: a PodMonitor
	// selects PODS, so each of its endpoints is offered once per pod with no
	// enclosing per-Service loop and there is no repeat to suppress.
	for _, pm := range podMonitors {
		for i := range pm.monitor.Endpoints {
			ep := &pm.monitor.Endpoints[i]
			em := explainMonitor{Monitor: pm.name}
			if o := d.offerPodMonitor(&pod, pm.name, ep); o.resolved {
				em.Resolved, em.URL = true, o.url
				em.Note = explainOfferNote(&d, &ceilings, o)
			} else {
				// The wording (and the SIZE-refusal case it also covers) is
				// scrape's: see scrape.MonitorEndpointNote.
				em.Note = scrape.PodMonitorEndpointNote(pod, *ep)
			}
			if monBudget.room(len(doc.PodMonitors)) {
				doc.PodMonitors = append(doc.PodMonitors, em)
			} else {
				doc.PodMonitorsNotShown++
			}
		}
	}
	monBudget.hide(doc.PodMonitorsNotShown)
	// The same collision set the targets path warns about, read off the same
	// dedup rather than recomputed here: explain must not be able to describe a
	// target list the server does not serve.
	collidesWith := map[string][]string{}
	for _, c := range d.collisions() {
		for _, ct := range c.Targets {
			for _, other := range c.Targets {
				if other.URL != ct.URL {
					collidesWith[ct.URL] = append(collidesWith[ct.URL], other.URL)
				}
			}
		}
	}
	for _, t := range targets {
		doc.Targets = append(doc.Targets, explainTarget{
			URL: t.URL, Source: t.Source, Monitor: t.Monitor, Monitors: t.Monitors,
			CollidesWith:       collidesWith[t.URL],
			ServiceViewTrimmed: d.trimmedAt(t.URL) >= 0,
		})
	}
	doc.ServiceViewsTrimmed = len(d.trimmed)

	// The accumulator's own count, which is what the served path reports as
	// kubescrape_scrape_targets_capped_total for this pod.
	doc.CappedTargets = d.capped
	doc.CappedTargetsBySize = d.cappedBySize
	if d.cappedBySize > 0 {
		doc.PodDocumentBytes = d.podBytes
	}
	doc.MergeCeilings = ceilings.list()
	doc.NotShown = portBudget.hidden + declaredBudget.hidden + svcBudget.hidden + monBudget.hidden

	if len(doc.Targets) == 0 && doc.Hint == "" {
		// svcOptIn was accumulated over EVERY selector-matching Service (see
		// its declaration) and podMonitors is the unclipped match set, for the
		// same reason: only a Service that is scrape-annotated or selected by a
		// ServiceMonitor is an opt-in, so a pod whose sole matches are
		// unannotated Services still gets the "nothing opts this pod in" hint
		// rather than a phantom opt-in — and a pod whose opt-in was clipped out
		// of the document must not get that hint at all.
		switch {
		case !doc.Scrapeable:
			doc.Hint = "pod is not scrapeable; see notScrapeableWhy"
		case !doc.PodAnnotated && !svcOptIn && len(podMonitors) == 0:
			doc.Hint = "nothing opts this pod into scraping: no prometheus.io/scrape=\"true\" pod annotation, no scrape-annotated or monitor-selected Service selecting it, and no PodMonitor matching it"
		default:
			// A monitor may be the ONLY opt-in (an unannotated pod behind a
			// monitor-selected Service), and then the answer is in the
			// monitor's endpoint note, not in any door's port entries.
			doc.Hint = "an opt-in exists but no port resolved; see portEntries / services[].portEntries for the " +
				"annotation doors' entry-by-entry verdicts and services[].serviceMonitors[].note / podMonitors[].note " +
				"for the monitor endpoints'"
		}
	}
	return doc, targets
}

// noteCapped rewrites the verdicts of the ports one door produced and the
// accumulator REFUSED. The mirrors in internal/scrape resolve each entry
// against its own door and cannot see the ceiling — it binds across every door
// at once — so a verdict that survived its own door may still describe a
// target the server does not serve, which is not a caveat but the inversion
// /v1/explain exists to prevent ("why is my 17th port not scraped?" answered
// with "it is"). The refused port loses its `ports`, exactly as the
// pod-annotation door's own capped verdicts do, and the note REPLACES whatever
// caveat the mirror wrote: on the Service door that caveat was "the target is
// still served, but if nothing listens there the scrape will fail", i.e. an
// affirmative false statement plus a wild-goose chase.
//
// Keyed by pod port because that is what both sides agree on: every door
// dedupes by resolved pod port, so at most one verdict of a door can claim any
// given port, and a refused target carries it (kubemeta.ScrapeTarget.Port).
func noteCapped(verdicts []scrape.PortVerdict, refused map[int32]targetVerdict, podBytes int) {
	if len(refused) == 0 {
		return
	}
	for i := range verdicts {
		v := &verdicts[i]
		// One list per CEILING, because one entry can resolve to several ports
		// and a door can hit both ceilings in one pass (the count one on the
		// port that fills the sixteenth slot, the byte one on every port after
		// the budget is spent). Merging them into one wording would name a
		// remedy that is wrong for half the ports it lists.
		var overCount, overBytes []string
		kept := v.Ports[:0]
		for _, p := range v.Ports {
			switch refused[p] {
			case targetRefusedCount:
				overCount = append(overCount, strconv.Itoa(int(p)))
			case targetRefusedBytes:
				overBytes = append(overBytes, strconv.Itoa(int(p)))
			default:
				kept = append(kept, p)
			}
		}
		if len(overCount) == 0 && len(overBytes) == 0 {
			continue
		}
		v.Ports = kept
		if len(v.Ports) == 0 {
			v.Ports = nil // omitempty: an empty array reads as "resolves, to nothing"
		}
		v.Note = ""
		if len(overCount) > 0 {
			v.Note = scrape.CeilingNote("port " + strings.Join(overCount, ", "))
		}
		if len(overBytes) > 0 {
			if v.Note != "" {
				v.Note += "; "
			}
			v.Note += scrape.SizeCeilingNote("port "+strings.Join(overBytes, ", "), podBytes)
		}
	}
}

// explainMonitorEndpoint runs one ServiceMonitor endpoint through the SAME
// offer step the targets path takes (targetDedup.offerServiceMonitor), recording
// the verdict. The step is shared rather than mirrored, so it is nodeTargets'
// step in nodeTargets' order by construction — the counters and the conflict
// warning are the only things left out (see the package comment), and they are
// left out by not calling reportMerge / unresolvedReports.
func (s *Server) explainMonitorEndpoint(d *targetDedup, offers *monitorOffers, ceilings *explainCeilings, pod *kubemeta.Pod, svc *services.Service, sme monitorEndpoint) explainMonitor {
	em := explainMonitor{Monitor: sme.monitor}
	o := d.offerServiceMonitor(offers, pod, svc, sme)
	if !o.resolved {
		em.Note = scrape.MonitorEndpointNote(*pod, svc, *sme.endpoint)
		return em
	}
	em.Resolved, em.URL = true, o.url
	if o.repeat {
		// Resolved, and honoured — through the earlier Service. Reporting it as
		// a fresh merge would be reporting a second fold that does not happen.
		em.Note = "already folded in through an earlier Service selecting this pod; each monitor endpoint is honoured once per URL, so this repeat changes nothing"
		return em
	}
	em.Note = explainOfferNote(d, ceilings, o)
	return em
}

// explainOfferNote is the note for a RESOLVED monitor endpoint that was
// offered (not a repeat), for both monitor kinds.
func explainOfferNote(d *targetDedup, ceilings *explainCeilings, o endpointOffer) string {
	if o.merged {
		// The adopted/conflict verdicts are the counter's and the warning's,
		// which this endpoint does not move: the targets path reports the
		// conflict once, and reporting it from here would double-count it. The
		// two ceilings move nothing here either, for the same reason — but they
		// ARE part of the explanation, since nothing else can tell an operator
		// that half their chain stopped being honoured, or that the monitor
		// they are looking at merged without being listed.
		//
		// The ceiling wording is emitted ONCE PER URL (see explainCeilings):
		// appending ~370 bytes to every refused endpoint made the document grow
		// with the pile-up the ceiling exists to refuse.
		return "merged into the target already held for this URL" + ceilings.note(o.url, o.rep)
	}
	// Resolved and offered, and possibly REFUSED: the endpoint resolves to a
	// URL (which is what Resolved reports, exactly as it does on the merged
	// arm) but the pod is at the ceiling and it is not scraped. An empty note
	// there made a refused endpoint byte-identical to a served one — the
	// inversion this endpoint exists to prevent. Empty when it was accepted.
	return o.refused.note("this endpoint", d.podBytes)
}
