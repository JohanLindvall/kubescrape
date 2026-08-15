package server

// GET /v1/explain/{namespace}/{name}: why is this pod (not) scraped?
//
// The single most common operational question this service answers badly from
// its normal responses — a pod missing from a target list looks the same
// whether the annotation is absent, the port entry resolves to nothing, the
// pod is terminating, or a Service selector misses. This handler walks the
// SAME decision chain nodeTargets walks, one pod at a time, and reports every
// verdict along the way. Diagnostic and read-only: it moves no obs counters
// and emits no warnings (reporting a monitor auth conflict from here would
// double-count what the targets path already reports), and it is deliberately
// per-pod and unindexed — the cost is one enrichment and a handful of matches.

import (
	"net/http"

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
}

// explainService is one Service whose selector matches the pod.
type explainService struct {
	Name          string               `json:"name"`
	Annotated     bool                 `json:"annotated"`
	PortAnnotated bool                 `json:"portAnnotated,omitempty"`
	PortEntries   []scrape.PortVerdict `json:"portEntries,omitempty"`
	// ServiceMonitors selecting this Service, one entry per endpoint.
	Monitors []explainMonitor `json:"serviceMonitors,omitempty"`
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
}

func (s *Server) handleExplain(w http.ResponseWriter, r *http.Request) {
	if !s.requireReady(w, "") {
		return
	}
	doc, _ := s.explainPod(r.PathValue("namespace"), r.PathValue("name"))
	// 200 even for a miss: the explanation IS the resource this endpoint
	// serves, and `curl -f` hiding the body on a 404 would defeat its purpose.
	writeJSON(w, http.StatusOK, doc)
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
	doc.ScrapeAnnotation = pod.Annotations[scrape.AnnotationScrape]
	doc.PodAnnotated = doc.ScrapeAnnotation == "true"
	doc.PortAnnotation = pod.Annotations[scrape.AnnotationPort]
	doc.DeclaredPorts = scrape.DeclaredPorts(pod)
	doc.PortEntries, doc.PortAnnotated = scrape.ExplainPodPorts(pod)

	// The same request-scoped snapshots nodeTargets takes.
	monitored := s.monitoredServices()
	svcs := s.services.InNamespaces([]string{pod.Namespace})
	matched := matchingServices(svcs[pod.Namespace], pod.Labels, nil)
	podMonitors := podMonitorsFor(pod, s.allPodMonitors(), nil)

	// Targets, through the same dedup/merge machinery as the node response —
	// minus the obs counters and warnings (see the package comment above).
	var targets []kubemeta.ScrapeTarget
	var d targetDedup
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
	for _, t := range scrape.PodTargets(pod) {
		d.add(t)
	}
	for _, svc := range matched {
		es := explainService{
			Name:      svc.Name,
			Annotated: svc.Annotations[scrape.AnnotationScrape] == "true",
		}
		es.PortEntries, es.PortAnnotated = scrape.ExplainServicePorts(pod, svc)
		for _, t := range scrape.ServiceTargets(pod, svc) {
			d.add(t)
		}
		for _, sme := range monitored[svc.UID] {
			es.Monitors = append(es.Monitors, s.explainMonitorEndpoint(&d, &offers, pod, svc, sme))
		}
		doc.Services = append(doc.Services, es)
	}
	// No offer dedup on this sweep, exactly as in nodeTargets: a PodMonitor
	// selects PODS, so each of its endpoints is offered once per pod with no
	// enclosing per-Service loop and there is no repeat to suppress.
	for _, pm := range podMonitors {
		for i := range pm.monitor.Endpoints {
			ep := &pm.monitor.Endpoints[i]
			em := explainMonitor{Monitor: pm.name}
			if url, ok := scrape.PodMonitorTargetURL(pod, *ep); ok {
				em.Resolved, em.URL = true, url
				if held, taken := d.monitorHolder(url); taken {
					scrape.MergeMonitorEndpoint(held, pm.name, ep)
					em.Note = "merged into the target already held for this URL"
				} else {
					for _, t := range scrape.PodMonitorTargets(pod, pm.name, *ep) {
						d.add(t)
					}
				}
			} else {
				em.Note = "endpoint resolves to no pod port (port must name a declared container port; targetPort a number or declared name)"
			}
			doc.PodMonitors = append(doc.PodMonitors, em)
		}
	}
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
			CollidesWith: collidesWith[t.URL],
		})
	}

	if len(doc.Targets) == 0 && doc.Hint == "" {
		// doc.Services lists EVERY selector-matching Service; only one that is
		// scrape-annotated or selected by a ServiceMonitor is an opt-in, so a
		// pod whose sole matches are unannotated Services still gets the
		// "nothing opts this pod in" hint rather than a phantom opt-in.
		svcOptIn := false
		for _, es := range doc.Services {
			if es.Annotated || len(es.Monitors) > 0 {
				svcOptIn = true
				break
			}
		}
		switch {
		case !doc.Scrapeable:
			doc.Hint = "pod is not scrapeable; see notScrapeableWhy"
		case !doc.PodAnnotated && !svcOptIn && len(doc.PodMonitors) == 0:
			doc.Hint = "nothing opts this pod into scraping: no prometheus.io/scrape=\"true\" pod annotation, no scrape-annotated or monitor-selected Service selecting it, and no PodMonitor matching it"
		default:
			doc.Hint = "an opt-in exists but no port resolved; see portEntries / services[].portEntries for the entry-by-entry verdicts"
		}
	}
	return doc, targets
}

// explainMonitorEndpoint runs one ServiceMonitor endpoint through the same
// resolve-then-offer-then-dedup the targets path takes, recording the verdict.
// Every step here is nodeTargets' step, in nodeTargets' order — the counters
// and the conflict warning are the only things left out (see the package
// comment); a step skipped here explains a target the server does not serve.
func (s *Server) explainMonitorEndpoint(d *targetDedup, offers *monitorOffers, pod kubemeta.Pod, svc *services.Service, sme monitorEndpoint) explainMonitor {
	em := explainMonitor{Monitor: sme.monitor}
	url, ok := scrape.MonitorTargetURL(pod, svc, *sme.endpoint)
	if !ok {
		em.Note = "endpoint resolves to no pod port (port must name a Service port, targetPort a number or declared container-port name; an endpoint naming neither resolves to nothing)"
		return em
	}
	em.Resolved, em.URL = true, url
	if !offers.first(url, sme.endpoint) {
		// Resolved, and honoured — through the earlier Service. Reporting it as
		// a fresh merge would be reporting a second fold that does not happen.
		em.Note = "already folded in through an earlier Service selecting this pod; each monitor endpoint is honoured once per URL, so this repeat changes nothing"
		return em
	}
	if held, taken := d.monitorHolder(url); taken {
		// The adopted/conflict verdicts are the counter's and the warning's,
		// which this endpoint does not move: the targets path reports the
		// conflict once, and reporting it from here would double-count it.
		scrape.MergeMonitorEndpoint(held, sme.monitor, sme.endpoint)
		em.Note = "merged into the target already held for this URL"
		return em
	}
	for _, t := range scrape.MonitorTargets(pod, svc, sme.monitor, *sme.endpoint) {
		d.add(t)
	}
	return em
}
