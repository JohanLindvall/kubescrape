package main

// ServiceMonitor/PodMonitor wiring (-servicemonitors): CRD discovery, the
// dynamic informers, the -monitor-namespaces gate and the reporting of what a
// monitor asked for that is refused or ignored.

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"

	"github.com/JohanLindvall/kubescrape/internal/cli"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
)

// startServiceMonitors sets up and starts the dynamic ServiceMonitor informer.
// When the CRD is unavailable the feature is disabled with a warning and a nil
// Index is returned (not an error). The dynamic client is the caller's so a
// test can drive the whole wiring against a fake one.
func startServiceMonitors(ctx context.Context, dynClient dynamic.Interface, disco discovery.DiscoveryInterface, resync time.Duration, allowNS map[string]bool, log *slog.Logger) (*servicemonitors.Index, []syncGate, error) {
	// Distinguish "the CRD is genuinely absent" from "we could not ask". The
	// pre-check exists so an unused feature does not wedge readiness behind an
	// informer that can never sync — it is NOT licence to treat a 503 from a
	// rolling API server, a throttled request or a dropped connection as an
	// answer. Doing so silently disabled an EXPLICITLY requested feature for
	// the whole process lifetime: every monitor-derived target in the cluster
	// vanished and /v1/scrape-auth 404'd every credential, with one log line
	// at startup as the only trace. An operator who asked for -servicemonitors
	// gets a hard failure instead, and can retry.
	// Both CRDs are OPTIONAL and install independently, so ask about both and
	// disable only when NEITHER is served. Gating the whole function on the
	// ServiceMonitor CRD alone returned before the PodMonitor check further
	// down ever ran, so a cluster that serves only PodMonitors — which is a
	// supported prometheus-operator install — got no monitor discovery at all,
	// and the log said "servicemonitors requested but the CRD is unavailable"
	// while the CRD the operator actually had was sitting right there.
	served, err := monitoringResources(disco)
	if err != nil {
		return nil, nil, fmt.Errorf("checking for the monitoring CRDs: %w", err)
	}
	haveSM, havePM := served[servicemonitors.GVR.Resource], served[servicemonitors.PodGVR.Resource]
	if !haveSM && !havePM {
		log.Warn("servicemonitors requested but neither the ServiceMonitor nor the PodMonitor CRD is available; disabling")
		return nil, nil, nil
	}
	dynFactory := dynamicinformer.NewDynamicSharedInformerFactory(dynClient, resync)
	monitors := servicemonitors.NewIndex()
	// The rejected-monitor STATE, registered here because this is the one
	// place that knows the feature is genuinely ON (the CRD pre-check passed)
	// and WHICH kinds are watched. kubescrape_monitor_parse_errors_total is
	// news-gated by design, so without this a monitor that stays broken is one
	// warn line and one increment, and "still broken now" is unalertable.
	obs.RegisterMonitorsRejected(monitorsRejectedHook(monitors, haveSM, havePM))
	var synced []syncGate
	if haveSM {
		smSynced, err := monitorInformer(dynFactory, servicemonitors.GVR, kindServiceMonitor, allowNS, log,
			monitors.UpsertChanged, monitors.Delete)
		if err != nil {
			return nil, nil, err
		}
		// Readiness must cover every cache a request can read, so collect the
		// handler registrations rather than returning the ServiceMonitor's alone:
		// /v1/nodes/{node}/targets reads the PodMonitor index too, and leaving it
		// out let /readyz report 200 — advancing a rollout — while that index was
		// empty, or permanently so when podmonitors RBAC is missing and its LIST
		// 403-loops forever.
		synced = append(synced, syncGate{kindServiceMonitor, smSynced})
	}

	// PodMonitors are an optional sibling, watched when the cluster serves it —
	// independently of the ServiceMonitor CRD above. (Probes are deliberately
	// not supported at all: blackbox probing has no node affinity and does not
	// fit the node-local model.)
	if havePM {
		pmSynced, err := monitorInformer(dynFactory, servicemonitors.PodGVR, kindPodMonitor, allowNS, log,
			monitors.UpsertPodMonitorChanged, monitors.DeletePodMonitor)
		if err != nil {
			return nil, nil, err
		}
		synced = append(synced, syncGate{kindPodMonitor, pmSynced})
		log.Info("podmonitor discovery enabled")
	}
	dynFactory.Start(ctx.Done())
	// Name what is actually watched: with only one CRD installed, claiming
	// "servicemonitor discovery enabled" was wrong half the time.
	log.Info("monitor discovery enabled", "servicemonitors", haveSM, "podmonitors", havePM)
	return monitors, synced, nil
}

// monitorUpsert is one monitor kind's index upsert
// (servicemonitors.Index.UpsertChanged / UpsertPodMonitorChanged): the parsed
// endpoints, whether the delivery was news, and the parse error.
type monitorUpsert = func(*unstructured.Unstructured) (eps []servicemonitors.Endpoint, news bool, err error)

// monitorInformer wires one monitor-kind informer arm — the shared transform,
// the watch-error counter, and the handler chain every monitor kind gets: the
// -monitor-namespaces gate, the upsert with its parse-error counter and
// warning, the ignored-fields report, and the delete. The ServiceMonitor and
// PodMonitor arms were verbatim copies differing only in gvr/kind and the
// index methods; keeping the chain ONE piece of code means a future arm
// cannot lose a link (the namespace gate is the multi-tenant boundary, and
// the parse-error counter is the alert on a monitor being DROPPED).
func monitorInformer(
	dynFactory dynamicinformer.DynamicSharedInformerFactory,
	gvr schema.GroupVersionResource,
	kind string,
	allowNS map[string]bool,
	log *slog.Logger,
	upsert monitorUpsert,
	del func(namespace, name string),
) (cache.InformerSynced, error) {
	inf := dynFactory.ForResource(gvr).Informer()
	// Unstructured objects retain managedFields unless stripped, like the
	// typed informers' transform does. stripManagedFields goes through
	// apimeta.Accessor, which handles *unstructured.Unstructured, so ONE
	// transform serves every informer here — this used to be a bespoke
	// closure, and its PodMonitor sibling was a copy that simply never got
	// written, leaving that one cache carrying full managedFields trees.
	if err := inf.SetTransform(stripManagedFields); err != nil {
		return nil, fmt.Errorf("%s informer transform: %w", kind, err)
	}
	if err := watchErrors(inf, gvr.Resource); err != nil {
		return nil, fmt.Errorf("%s watch error handler: %w", kind, err)
	}
	refused := newMonitorRefusals()
	reg, err := inf.AddEventHandler(typedHandler(freshness.slot(gvr.Resource),
		func(u *unstructured.Unstructured) {
			if !monitorAllowed(allowNS, refused, kind, u, log) {
				return
			}
			eps, news, err := upsert(u)
			if err != nil {
				// Counted, not just logged: an unparseable monitor DELETES it
				// from the index, dropping every target it contributed. That
				// is strictly more severe than the "some endpoint fields were
				// ignored" case, which does get a metric — so the severe one
				// must not be the unalertable one.
				//
				// …and, being the same shape of report as that sibling, gated
				// the same way: this one described an EVENT while firing per
				// DELIVERY too, so a single monitor nobody ever fixes re-logged
				// and re-incremented every resync period forever. news is what
				// separates the first sighting of a broken monitor — which must
				// always be reported, and which is NOT a change to the index,
				// since a monitor that never parsed was never in it — from the
				// resync re-delivering it (see upsertMonitor).
				if news {
					obs.MonitorParseErrors.WithLabelValues(kind).Inc()
					log.Warn("parsing "+kind, "error", err,
						"namespace", u.GetNamespace(), "name", u.GetName())
				}
				return
			}
			if news {
				// Only on a real change. An informer resync re-delivers every
				// object it holds, and the ignored-fields report is a statement
				// about an EVENT: unthrottled, it made the WARN and
				// kubescrape_monitor_fields_ignored_total repeat once per
				// monitor per resync period, forever — the same repetition the
				// namespace refusal above is gated against (monitorRefusals).
				// The report reads the endpoints this very upsert parsed,
				// not a second lookup of the index.
				warnIgnored(log, kind, u, eps)
			}
		},
		func(u *unstructured.Unstructured) {
			refused.forget(u.GetNamespace(), u.GetName())
			del(u.GetNamespace(), u.GetName())
		},
	))
	if err != nil {
		return nil, fmt.Errorf("registering %s handler: %w", kind, err)
	}
	return reg.HasSynced, nil
}

// parseNamespaceSet turns a comma-separated flag value into a lookup set. An
// empty or whitespace-only value yields nil, meaning "no restriction".
func parseNamespaceSet(s string) map[string]bool {
	nss := cli.SplitList(s)
	if len(nss) == 0 {
		return nil
	}
	out := make(map[string]bool, len(nss))
	for _, ns := range nss {
		out[ns] = true
	}
	return out
}

// monitorAllowed reports whether a monitor's namespace is one the operator
// permits to drive scrapes. A nil set allows everything (the default).
//
// This is applied at INDEXING time rather than at target-derivation time so a
// disallowed monitor never occupies memory, and — more importantly — never
// contributes to AuthSecretRefs, which is the allowlist bounding what
// /v1/scrape-auth will read. A gate that let the monitor into the index and
// only filtered its targets would still widen the set of Secrets this process
// is willing to fetch.
// It is also the one outcome on this code path that used to be entirely
// silent — no metric, no log — which on a multi-tenant cluster makes an
// admin's deliberate refusal indistinguishable from a selector typo, a missing
// CRD, or a monitor that simply matches nothing. Counted per kind and logged at
// Info, both ONCE PER CHANGE of the refused monitor (refused.news). An informer
// re-delivers every object it holds on a resync and on every relist, and the
// report describes an event: counted per delivery, the counter's rate was the
// resync period rather than anyone's edits, and the line had to stay at Debug
// so its repeats could not flood. The sibling reports (parse errors, ignored
// fields) are gated on the index's news for the same reason; a refused monitor
// never reaches the index, so it carries its own record.
func monitorAllowed(allowNS map[string]bool, refused *monitorRefusals, kind string, u *unstructured.Unstructured, log *slog.Logger) bool {
	if allowNS == nil || allowNS[u.GetNamespace()] {
		return true
	}
	if refused.news(u) {
		obs.MonitorNamespaceRefused.WithLabelValues(kind).Inc()
		log.Info("monitor ignored: its namespace is not permitted by -monitor-namespaces",
			"kind", kind, "monitor", u.GetNamespace()+"/"+u.GetName())
	}
	return false
}

// monitorRefusals records, per refused monitor, the resourceVersion whose
// refusal has already been reported — upsertMonitor's rejected map, for the
// monitors that never reach the index. It is bounded by the refused monitors
// the informer itself is caching (a namespace cannot change, and the allow set
// is fixed at startup, so a key is refused for its whole life), and the delete
// arm clears each entry, so a monitor deleted and re-created is reported again.
//
// A mutex rather than relying on client-go's one-goroutine-per-registration
// delivery: the cost is nil on this path, and the guarantee would otherwise be
// an unstated property of how monitorInformer registers its handler.
type monitorRefusals struct {
	mu   sync.Mutex
	seen map[string]string // namespace/name -> reported resourceVersion
}

func newMonitorRefusals() *monitorRefusals {
	return &monitorRefusals{seen: make(map[string]string)}
}

// news reports whether u's refusal has not been reported at its current
// resourceVersion: the first delivery, or an edit. An empty resourceVersion is
// always news — nothing can tell a re-delivery of it from a change.
func (r *monitorRefusals) news(u *unstructured.Unstructured) bool {
	key, rv := u.GetNamespace()+"/"+u.GetName(), u.GetResourceVersion()
	r.mu.Lock()
	defer r.mu.Unlock()
	prev, seen := r.seen[key]
	r.seen[key] = rv
	return !seen || rv == "" || prev != rv
}

// forget drops a deleted monitor's record.
func (r *monitorRefusals) forget(namespace, name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.seen, namespace+"/"+name)
}

// The kind label every monitor metric shares. Constants because three series
// families key on them (parse errors, fields ignored, monitors rejected) from
// two files, and a drifted literal would split one kind across two labels.
const (
	kindServiceMonitor = "servicemonitor"
	kindPodMonitor     = "podmonitor"
)

// monitorsRejectedHook adapts the index's Rejected counts to the kinds this
// process actually watches: an unwatched kind is ABSENT from the map — hence
// from the exposition — rather than a forever-0 series claiming a CRD nobody
// serves is clean.
func monitorsRejectedHook(monitors *servicemonitors.Index, haveSM, havePM bool) func() map[string]int {
	return func() map[string]int {
		sm, pm := monitors.Rejected()
		out := make(map[string]int, 2)
		if haveSM {
			out[kindServiceMonitor] = sm
		}
		if havePM {
			out[kindPodMonitor] = pm
		}
		return out
	}
}

// monitoringResources lists which monitoring.coreos.com resources the
// cluster serves (servicemonitors and podmonitors may be installed
// independently). The group/version existing is not enough: another
// monitoring.coreos.com/v1 CRD (e.g. PrometheusRule alone) registers the
// group while servicemonitor LISTs would fail forever, wedging readiness
// behind an informer that can never sync — hence per-RESOURCE answers.
// A missing group/version is reported as an empty set and no
// error — that is an answer ("nothing is installed"), not a failure to reach
// the API server, and only the latter should be fatal to the caller.
func monitoringResources(d discovery.DiscoveryInterface) (map[string]bool, error) {
	list, err := d.ServerResourcesForGroupVersion(servicemonitors.GVR.GroupVersion().String())
	if err != nil {
		if apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err) {
			return map[string]bool{}, nil
		}
		return nil, err
	}
	out := map[string]bool{}
	for _, r := range list.APIResources {
		out[r.Name] = true
	}
	return out, nil
}

// warnIgnored reports the endpoint fields of a monitor that kubescrape parsed
// but does not interpret. Implementing a documented SUBSET of the CRD is a
// deliberate choice; applying part of a user's CR without saying so is not —
// they would see targets appear and never learn that their relabelings or
// sampleLimit did nothing.
func warnIgnored(log *slog.Logger, kind string, u *unstructured.Unstructured, eps []servicemonitors.Endpoint) {
	if fields := servicemonitors.IgnoredFields(eps); len(fields) > 0 {
		obs.MonitorFieldsIgnored.WithLabelValues(kind).Inc()
		log.Warn("monitor uses fields kubescrape does not interpret; those clauses have no effect",
			"kind", kind, "monitor", u.GetNamespace()+"/"+u.GetName(),
			"fields", strings.Join(fields, ","))
	}
}
