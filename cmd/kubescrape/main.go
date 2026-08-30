// Command kubescrape serves Kubernetes pod and container metadata over HTTP.
//
// It builds an in-memory view of all pods via a single LIST followed by a
// WATCH (shared informers), plus metadata-only informers for ReplicaSets and
// Jobs so pod owner chains (Deployment, CronJob, ...) can be resolved without
// caching full objects.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/metadata/metadatainformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/bearer"
	"github.com/JohanLindvall/kubescrape/internal/cli"
	"github.com/JohanLindvall/kubescrape/internal/cli/kubecfg"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/owners"
	"github.com/JohanLindvall/kubescrape/internal/selfmeta"
	"github.com/JohanLindvall/kubescrape/internal/server"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

func main() {
	// The process logger cannot exist until -log-level is parsed, and a refusal
	// can precede it (an unparseable flag, an unknown level). Without this that
	// one line — the one that says why the pod will not start — went out through
	// slog's stdlib default, which is not logfmt. Replaced by the leveled logger
	// a few statements into run().
	slog.SetDefault(slog.New(cli.NewLogfmtHandler(os.Stderr, slog.LevelInfo)))
	if err := run(); err != nil {
		slog.Error("kubescrape failed", "error", err)
		os.Exit(1)
	}
}

// typedHandler builds informer callbacks that type-assert every payload to T:
// Add and Update call upsert; Delete unwraps a DeletedFinalStateUnknown
// tombstone first, then calls del. A payload of the wrong type is ignored.
func typedHandler[T any](upsert, del func(T)) cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if v, ok := obj.(T); ok {
				upsert(v)
			}
		},
		UpdateFunc: func(_, obj any) {
			if v, ok := obj.(T); ok {
				upsert(v)
			}
		},
		DeleteFunc: func(obj any) {
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			if v, ok := obj.(T); ok {
				del(v)
			}
		},
	}
}

// registerCoreInformers wires the pod and service informers into the store
// and the service index, returning their HasSynced funcs.
//
// These are the HANDLER REGISTRATIONS' HasSynced, never the informer's. The
// informer's flips as soon as its DeltaFIFO has drained the initial LIST,
// which says nothing about whether OUR handlers have run: the shared
// processor delivers to each listener asynchronously through a pending-
// notification ring, and everything a request actually reads — the store's
// indexes, the service index — is filled from inside those handlers. Gating
// readiness on the informer therefore lets /readyz report 200 while the store
// is still filling, and an agent that polls in that window gets a 200 with a
// half-built (or empty) target list instead of a 503 telling it to come back.
func registerCoreInformers(factory informers.SharedInformerFactory, st *store.Store, svcIndex *services.Index) ([]syncGate, error) {
	podInformer := factory.Core().V1().Pods().Informer()
	if err := watchErrors(podInformer, "pods"); err != nil {
		return nil, fmt.Errorf("pod watch error handler: %w", err)
	}
	podReg, err := podInformer.AddEventHandler(typedHandler(
		func(pod *corev1.Pod) { st.UpsertPod(pod) },
		func(pod *corev1.Pod) { st.DeletePod(pod.UID) },
	))
	if err != nil {
		return nil, fmt.Errorf("registering pod event handler: %w", err)
	}

	// Services are matched against pods for service-annotation based scrape
	// discovery; their specs are small, so the full objects are cached.
	svcInformer := factory.Core().V1().Services().Informer()
	if err := watchErrors(svcInformer, "services"); err != nil {
		return nil, fmt.Errorf("service watch error handler: %w", err)
	}
	svcReg, err := svcInformer.AddEventHandler(typedHandler(
		func(svc *corev1.Service) { svcIndex.Upsert(svc) },
		func(svc *corev1.Service) { svcIndex.Delete(svc.Namespace, svc.UID) },
	))
	if err != nil {
		return nil, fmt.Errorf("registering service event handler: %w", err)
	}
	// NAMED, because readiness is what a rolling update advances on and "some
	// cache has not synced" is the half of that message that cannot be acted
	// on (waitForCaches).
	return []syncGate{{"pods", podReg.HasSynced}, {"services", svcReg.HasSynced}}, nil
}

// registerOwnerInformers wires a metadata-only informer per owner GVR,
// returning the listers the owner resolver reads and their HasSynced funcs.
func registerOwnerInformers(metaFactory metadatainformer.SharedInformerFactory) (map[schema.GroupVersionResource]cache.GenericLister, []syncGate, error) {
	listers := make(map[schema.GroupVersionResource]cache.GenericLister, len(owners.AllGVRs))
	var synced []syncGate
	for _, gvr := range owners.AllGVRs {
		inf := metaFactory.ForResource(gvr)
		if err := inf.Informer().SetTransform(stripManagedFields); err != nil {
			return nil, nil, fmt.Errorf("setting %s informer transform: %w", gvr.Resource, err)
		}
		if err := watchErrors(inf.Informer(), gvr.Resource); err != nil {
			return nil, nil, fmt.Errorf("%s watch error handler: %w", gvr.Resource, err)
		}
		listers[gvr] = inf.Lister()
		synced = append(synced, syncGate{gvr.Resource, inf.Informer().HasSynced})
	}
	return listers, synced, nil
}

// startServiceMonitors sets up and starts the dynamic ServiceMonitor informer.
// When the CRD is unavailable the feature is disabled with a warning and a nil
// Index is returned (not an error).
func startServiceMonitors(ctx context.Context, cfg *rest.Config, disco discovery.DiscoveryInterface, resync time.Duration, allowNS map[string]bool, log *slog.Logger) (*servicemonitors.Index, []syncGate, error) {
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
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("creating dynamic client: %w", err)
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
			monitors.UpsertChanged, monitors.Delete, monitors.Endpoints)
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
			monitors.UpsertPodMonitorChanged, monitors.DeletePodMonitor, monitors.PodEndpoints)
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

// monitorInformer wires one monitor-kind informer arm — the shared transform,
// the watch-error counter, and the handler chain every monitor kind gets: the
// -monitor-namespaces gate, the upsert with its parse-error counter and
// warning, the ignored-fields report, and the delete. The ServiceMonitor and
// PodMonitor arms were verbatim copies differing only in gvr/kind and the
// three index methods; keeping the chain ONE piece of code means a future arm
// cannot lose a link (the namespace gate is the multi-tenant boundary, and
// the parse-error counter is the alert on a monitor being DROPPED).
func monitorInformer(
	dynFactory dynamicinformer.DynamicSharedInformerFactory,
	gvr schema.GroupVersionResource,
	kind string,
	allowNS map[string]bool,
	log *slog.Logger,
	upsert func(*unstructured.Unstructured) (news bool, err error),
	del func(namespace, name string),
	endpoints func(namespace, name string) []servicemonitors.Endpoint,
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
	reg, err := inf.AddEventHandler(typedHandler(
		func(u *unstructured.Unstructured) {
			if !monitorAllowed(allowNS, kind, u, log) {
				return
			}
			news, err := upsert(u)
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
				// namespace refusal above was demoted to Debug for.
				warnIgnored(log, kind, u, endpoints(u.GetNamespace(), u.GetName()))
			}
		},
		func(u *unstructured.Unstructured) { del(u.GetNamespace(), u.GetName()) },
	))
	if err != nil {
		return nil, fmt.Errorf("registering %s handler: %w", kind, err)
	}
	return reg.HasSynced, nil
}

// k8sSecretReader resolves Secret keys on demand with a short cache (tokens
// rotate; per-scrape-cycle lookups must not hammer the API server).
type k8sSecretReader struct {
	client kubernetes.Interface
	mu     sync.Mutex
	cache  map[string]secretCacheEntry
}

type secretCacheEntry struct {
	value   string
	err     error
	fetched time.Time
}

// secretCacheTTL bounds how long a resolved Secret value is reused. Entries
// past it are not merely re-fetched but DROPPED (see evictExpiredLocked): this
// process holds cluster-wide `secrets: get`, so every value it has ever
// resolved — bearer tokens, CA bundles, client private keys — was otherwise
// pinned in its heap for the lifetime of the pod, growing by one permanent
// entry per distinct ns/name/key ever seen. Monitor churn alone (GitOps
// renames, per-release secret names) makes that unbounded.
const secretCacheTTL = time.Minute

// secretFailureTTL bounds how long a FAILED resolution is remembered. Failures
// were not cached at all, so a single monitor referencing a key that does not
// exist — allowlisted by AuthSecretRefs, so it passes the handler's check and
// reaches the API server — turned into one `secrets get` per agent per scrape
// cycle, indefinitely, against the client's QPS=50 budget and one audit entry
// apiece. The reader's own doc comment ("per-scrape-cycle lookups must not
// hammer the API server") was true only on the success path.
//
// Much shorter than secretCacheTTL, deliberately: a cached failure DELAYS
// recovery after the operator fixes the RBAC grant or creates the key, and
// that repair is the moment responsiveness matters most.
const secretFailureTTL = 10 * time.Second

// secretCacheKey identifies one Secret key in the reader's cache.
//
// NUL-separated, not "/"-joined: a Secret namespace, name and key cannot
// contain a NUL, so two distinct triples can never share one cached VALUE. The
// "/" join could — ("a/b","c","d") and ("a","b/c","d") render identically —
// and this cache is what a repeated scrape-auth lookup reads instead of the
// API server. The same ambiguity in the /v1/scrape-auth allowlist was the
// security half of it; this is the caching half.
func secretCacheKey(namespace, name, key string) string {
	return namespace + "\x00" + name + "\x00" + key
}

func (r *k8sSecretReader) Get(ctx context.Context, namespace, name, key string) (string, error) {
	ck := secretCacheKey(namespace, name, key)
	r.mu.Lock()
	if e, ok := r.cache[ck]; ok && time.Since(e.fetched) < e.ttl() {
		r.mu.Unlock()
		return e.value, e.err
	}
	r.evictExpiredLocked()
	r.mu.Unlock()
	sec, err := r.client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		// Cache the failure — but only when it says something about the
		// CLUSTER. ctx here is the inbound REQUEST's context, so a caller that
		// disconnects mid-flight (an agent restart, a rolling update, a client
		// timeout) produces context.Canceled/DeadlineExceeded that describes
		// that one caller and nothing else. Remembering it would let a single
		// agent going away make its credential unresolvable — a 502 — for
		// every OTHER agent in the fleet for the whole failure TTL.
		if ctx.Err() == nil {
			r.remember(ck, secretCacheEntry{err: err, fetched: time.Now()})
		}
		return "", err
	}
	val, ok := sec.Data[key]
	if !ok {
		// Wrapped, not bare: handleScrapeAuth distinguishes this client-caused
		// miss (404) from a cluster-caused failure like a forbidden read (502,
		// retryable). See server.ErrSecretKeyNotFound.
		err := fmt.Errorf("%w: %q", server.ErrSecretKeyNotFound, key)
		r.remember(ck, secretCacheEntry{err: err, fetched: time.Now()})
		return "", err
	}
	r.remember(ck, secretCacheEntry{value: string(val), fetched: time.Now()})
	return string(val), nil
}

// ttl is how long this entry stays usable: failures are held far more briefly
// than successes, so a fixed RBAC grant or a created key takes effect within
// seconds rather than a minute.
func (e secretCacheEntry) ttl() time.Duration {
	if e.err != nil {
		return secretFailureTTL
	}
	return secretCacheTTL
}

func (r *k8sSecretReader) remember(ck string, e secretCacheEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cache == nil {
		r.cache = map[string]secretCacheEntry{}
	}
	// A failure never displaces a LIVE success. Fetches happen on a miss only,
	// so a usable success can be sitting here just one way: the lock is
	// released across the API call, and two concurrent misses for one ref raced
	// back in the other order. Storing the slower error would serve every agent
	// in the fleet a 502 for the whole failure TTL, for a credential this
	// process has in hand.
	if cur, ok := r.cache[ck]; ok && e.err != nil && cur.err == nil && time.Since(cur.fetched) < cur.ttl() {
		return
	}
	r.cache[ck] = e
}

// evictExpiredLocked drops every entry past the TTL. It runs on the miss path
// only: a hit is the hot path and an expired entry is about to be replaced
// anyway, so the cost lands where an API round-trip is already being paid. The
// map is bounded by the AuthSecretRefs allowlist at any instant, and this is
// what keeps it bounded over TIME as monitors come and go.
func (r *k8sSecretReader) evictExpiredLocked() {
	for k, e := range r.cache {
		if time.Since(e.fetched) >= e.ttl() {
			delete(r.cache, k)
		}
	}
}

// newScrapeAuthTokens opens the shared bearer token guarding /v1/scrape-auth.
//
// Every failure mode is fatal by design: -scrape-auth-secrets turns the service
// into a reader of every Secret key a monitor references, so "no token file",
// "unreadable file" and "empty file" must all stop the process rather than
// quietly leave the endpoint open to the whole cluster. That is exactly
// bearer.NewRotating's contract; only the messages are ours, because they name
// the flag the operator set.
//
// Past startup the two sides re-read on DIFFERENT cadences, and the asymmetry
// is deliberate. This RECEIVER refreshes its accept set at request time on
// bearer.DefaultRefreshInterval (a second; Rotating.Run's
// bearer.DefaultReadInterval ticker only covers a receiver with no traffic at
// all, and a FAILED read backs off to it), because a receiver that has not yet
// read a rotated token has no grace to fall back on — it is a hard 401 for as
// long as its copy lags. The other direction, an agent still presenting the
// PREVIOUS token, is what bearer.DefaultGrace covers: agents re-read the copy
// they present on their own bearer.DefaultReadInterval minute and the
// predecessor stays accepted for five, so nobody has to flip in lockstep with
// the service.
func newScrapeAuthTokens(path string, log *slog.Logger) (*bearer.Rotating, error) {
	rt, err := bearer.NewRotating(path, log)
	switch {
	case errors.Is(err, bearer.ErrNoPath):
		return nil, errors.New("-scrape-auth-secrets requires -scrape-auth-token-file: " +
			"/v1/scrape-auth serves monitor Secret keys and must not be reachable unauthenticated")
	case err != nil:
		return nil, fmt.Errorf("reading -scrape-auth-token-file: %w", err)
	}
	return rt, nil
}

// The metadata service's flag surface. Package-level like the agent's, so the
// flag set exists at init: a test can then check that every flag the shipped
// manifests pass is actually defined (see internal/manifestcheck), which is
// impossible while the declarations live inside run().
var (
	listen = flag.String("listen", ":8080", "HTTP listen address")

	// The dry run. It runs validateConfig — the same call a real start makes
	// before it acquires anything — prints the same startup summary, and exits.
	// The agent has had one since it grew a config file; without this the
	// SERVICE half of an install was the half that could only be checked by
	// rolling it out, which is backwards: it is the singleton every agent in
	// the fleet blocks on.
	checkConfig = flag.Bool("check-config", false, "validate the flags (bounds, listener addresses, the -monitor-namespaces list, the -scrape-auth-secrets/-scrape-auth-token-file pair), print the same startup summary a real start prints, and exit — no listeners, no informers, no API-server traffic. For CI and pre-rollout checks")

	// The process-observability block (metrics/pprof listeners, self-metrics
	// cadence, logger) is registered through internal/cli, SHARED with the
	// agent: one registration, so defaults and help text cannot drift between
	// the binaries again. The two parameters are the hints that genuinely
	// differ per binary.
	obsFlags        = cli.RegisterObsFlags(flag.CommandLine, "service", "the API")
	pprofListen     = obsFlags.PprofListen
	metricsListen   = obsFlags.MetricsListen
	selfMetricsIntv = obsFlags.SelfMetricsInterval
	logLevel        = obsFlags.LogLevel

	kubeconfig = flag.String("kubeconfig", "", "path to a kubeconfig; defaults to in-cluster config, then $KUBECONFIG/~/.kube/config")
	maxWait    = flag.Duration("wait-timeout", 5*time.Second, "default and maximum time a container lookup blocks waiting for metadata to appear (shorten per request with ?wait=)")
	cacheTTL   = flag.Duration("cache-ttl", 5*time.Minute, "how long metadata of deleted pods and replaced container IDs stays resolvable")

	// The blocked-lookup cap, as a NUMBER because that is the thing the process
	// can enforce, but chosen as MEMORY: store.DefaultMaxWaiters is
	// store.WaiterBudgetBytes / store.WaiterCostBytes, and the cost is measured
	// (internal/server/waitercost_test.go) rather than assumed. It is a flag
	// because the default is derived from the memory the DEFAULT chart gives
	// this pod, and a cluster large enough to reach the cap has already been
	// given more than that — the two are raised together or not at all, which
	// is what the help text says.
	maxWaiters = flag.Int("max-blocked-lookups", store.DefaultMaxWaiters,
		fmt.Sprintf("how many container lookups may be blocked waiting for metadata at once; over it, /v1/containers answers 503 + Retry-After "+
			"(counted kubescrape_container_lookups_shed_total) instead of parking another handler. This is a MEMORY bound wearing a count: "+
			"each parked lookup is an HTTP handler held for up to -wait-timeout, measured at %d KiB for an agent's poll and budgeted at %d KiB "+
			"for the worst request the header bound admits, so the default spends %d MiB — a quarter of the 128Mi the chart requests for this pod. "+
			"On the DEFAULT agent configuration legitimate demand is at most one blocked lookup per NODE (the tailer resolves on one sweep "+
			"goroutine and the cadvisor path never waits); an agent run with -ingest-metadata-wait (default 0) also blocks in its ingest "+
			"handlers, one lookup at a time per push, adding up to its -ingest-max-in-flight (default 32) per node. So raise this for a fleet "+
			"bigger than the default, or for agents that wait on ingest, and add n x %d KiB to the pod's memory in the same change",
			30, store.WaiterCostBytes>>10, store.WaiterBudgetBytes>>20, store.WaiterCostBytes>>10))
	metaCacheTTL = flag.Duration("metadata-cache-ttl", 10*time.Second, "max-age sent on metadata responses (Cache-Control + ETag) so agents cache lookups client-side; 0 disables the cache headers. The server-side ServiceMonitor->Service memo is exact (invalidated by index generation, not this TTL), so 0 no longer costs a cross-product rebuild per request")
	resync       = flag.Duration("resync", 0, "informer resync period (0 disables periodic resync; the watch stream keeps the cache current)")

	// The ACTIVE reachability signal for the API-server connection. While the
	// server is merely unreachable client-go retries the watch internally and
	// never relists, so the informer watch-error counter can stay flat for the
	// whole outage, and readiness latches at the initial sync (see
	// cmd/kubescrape/apiserver.go for the mechanism and the measurements):
	// without this probe a cluster-wide outage can pass unremarked in this
	// process' telemetry and its logs.
	apiserverProbeInterval = flag.Duration("apiserver-probe-interval", 30*time.Second,
		"how often to probe API-server reachability with a metadata-only LIST of one namespace, publishing kubescrape_apiserver_reachable and kubescrape_apiserver_probe_failures_total (0 disables the probe, and then neither metric is published). It probes a NEW connection, so it reports reachability rather than whether the caches are advancing: readiness latches at the initial sync, and client-go retries a refused watch without relisting, so the watch-error counter is not a dependable substitute")

	// ServiceMonitor CRDs (opt-in).
	monitorsOn = flag.Bool("servicemonitors", false, "serve targets for monitoring.coreos.com ServiceMonitors (pod-backed Services) and PodMonitors. Endpoint port/targetPort/path/scheme, per-endpoint interval/scrapeTimeout, basicAuth/authorization/bearerTokenSecret and secret-backed tlsConfig (needs -scrape-auth-secrets), and the keep/drop subset of metricRelabelings are interpreted; everything else is reported through kubescrape_monitor_fields_ignored_total and a startup warning")

	// Which namespaces' monitors are HONOURED. Empty keeps every monitor
	// in the cluster, which is the historical behaviour and stays the
	// default so an upgrade cannot silently stop scraping.
	//
	// It is worth setting. A ServiceMonitor is an instruction to every
	// node agent to issue a GET, and kubescrape has no equivalent of
	// prometheus-operator's admin-owned serviceMonitorSelector — so
	// without this, anyone who can create a ServiceMonitor in a namespace
	// they own can point `selector: {}` + `namespaceSelector.any: true` at
	// an arbitrary path across the whole cluster, at whatever interval
	// they choose. Restricting it to the namespaces that legitimately
	// declare monitoring turns that back into an admin decision.
	monitorNamespaces = flag.String("monitor-namespaces", "", "comma-separated namespaces whose ServiceMonitors/PodMonitors are honoured (empty = all; a monitor is an instruction to every agent to scrape, so restricting this to admin-owned namespaces is recommended on multi-tenant clusters)")

	// Serve monitor endpoints' bearerTokenSecret values to agents (opt-in:
	// needs secrets get RBAC; tokens travel the cluster-internal HTTP).
	scrapeAuthOn        = flag.Bool("scrape-auth-secrets", false, "serve the Secret keys ServiceMonitor/PodMonitor endpoints reference — bearerTokenSecret, basicAuth username/password, authorization credentials and tlsConfig ca/cert/keySecret (a CLIENT PRIVATE KEY) — to agents on /v1/scrape-auth. Only keys some indexed monitor actually names are served. Requires cluster-wide `secrets get` RBAC and -scrape-auth-token-file")
	scrapeAuthTokenFile = flag.String("scrape-auth-token-file", "", "file holding the shared bearer token that clients must present on /v1/scrape-auth (Authorization: Bearer <token>); REQUIRED with -scrape-auth-secrets")

	// Self-metrics -> OTLP (the service's only OTLP producer). The -otlp-*
	// block is the shared registration (internal/cli); only the endpoint help
	// is this binary's own, saying what it sends there.
	selfAttrs            = flag.Bool("self-attributes", true, "add THIS pod's Kubernetes resource attributes (namespace, pod, uid, owners, labels) to the service's own exported metrics. Resolved from the service's OWN store — its pod name is the hostname, its namespace comes from $POD_NAMESPACE or the ServiceAccount projection — so it needs no API traffic and no extra manifest wiring. Attributes already set (service.name, service.instance.id) are never overwritten; a process that is not a pod of that name simply gets none")
	otlpFlags            = cli.RegisterOTLPFlags(flag.CommandLine, "OTLP endpoint for self-metrics: host:port for grpc, base URL for http")
	otlpEndpoint         = otlpFlags.Endpoint
	otlpProtocol         = otlpFlags.Protocol
	otlpCompression      = otlpFlags.Compression
	otlpCompressionLevel = otlpFlags.CompressionLevel
	otlpInsecure         = otlpFlags.Insecure
	otlpSkipTLS          = otlpFlags.InsecureSkipVerify
	otlpCAFile           = otlpFlags.CAFile
	otlpBearer           = otlpFlags.BearerTokenFile
	otlpTimeout          = otlpFlags.Timeout
)

// otlpHeaders is registered in init() rather than in the var block above:
// flag.Var needs the value to exist first. init() rather than run() so the
// registration is visible to reflection over flag.CommandLine — the FLAGS.md
// generator (flagsdoc_test.go) walks the flag set without calling run().
var otlpHeaders headerFlags

func init() {
	flag.Var(&otlpHeaders, "otlp-header", "static key=value header sent on every self-metrics export (HTTP header / gRPC metadata, e.g. X-Scope-OrgID=tenant); repeatable")
}

// serviceSelfResource is this process's own OTLP resource identity, carried by
// its self-metrics export and its final export alike (the agent's twin is
// agentSelfResource).
//
// It ends with attrs.Identity for the same reason the agent's does: Identity is
// the sole producer of service.namespace, which is half the Prometheus job, and
// the ONLY other call is inside the self-metadata stamp — which builds a FRESH
// resource from the resolved pod, so with the lookup slow (or permanently
// impossible: an overridden spec.hostname, hostNetwork) the job stayed the
// unqualified `kubescrape` while the agent under the identical flags reported
// `<namespace>/kubescrape-agent`. Identity returns early on the keys already
// set here, so the hostname keeps naming the instance.
func serviceSelfResource() pcommon.Resource {
	res := pcommon.NewResource()
	a := res.Attributes()
	a.PutStr("service.name", "kubescrape")
	a.PutStr("service.version", obs.BuildVersion())
	if host, err := os.Hostname(); err == nil {
		a.PutStr("service.instance.id", host)
	}
	// Known without any lookup, and set here rather than left to the
	// self-metadata stamp: attrs.Identity derives service.namespace from
	// it, and that is half the Prometheus job. Learning it later would
	// rename the job of already-running cumulative series mid-flight.
	if ns := selfmeta.Namespace(); ns != "" {
		a.PutStr("k8s.namespace.name", ns)
	}
	attrs.Identity(res)
	return res
}

// newMetadataStore builds the store with the operator's blocked-lookup cap
// applied. One function rather than two lines in run() so the wiring is
// testable: the cap only exists as a knob if the flag reaches the shed decision,
// and before -max-blocked-lookups store.SetMaxWaiters had no production caller
// at all.
func newMetadataStore(ttl time.Duration, maxWaiters int) *store.Store {
	st := store.New(ttl)
	st.SetMaxWaiters(maxWaiters)
	return st
}

// checkWaiterCap validates -max-blocked-lookups and reports the one thing an
// operator cannot see from the number itself: what it costs.
//
// A cap of 0 or less sheds EVERY blocking lookup, which turns the ~1s gap
// between a container starting and the kubelet posting its ID into a 503 on the
// first log line of every container on every node. Nothing else in the process
// would name that as the cause, so it is refused rather than served.
//
// Above the budget is NOT an error — an operator who has given the pod the
// memory is entitled to spend it, and a cluster big enough to need a bigger cap
// has outgrown the 128Mi the default was derived against anyway. But the
// arithmetic is the whole point of the number, so a cap whose saturation exceeds
// that budget says so once, with the multiplication already done.
func checkWaiterCap(n int, log *slog.Logger) error {
	if n < 1 {
		return fmt.Errorf("-max-blocked-lookups %d: must be at least 1 (every blocking container lookup would be shed; "+
			"the default %d is store.WaiterBudgetBytes/store.WaiterCostBytes)", n, store.DefaultMaxWaiters)
	}
	if over := int64(n) * store.WaiterCostBytes; over > store.WaiterBudgetBytes {
		log.Warn("-max-blocked-lookups is above the memory budget its default was derived from",
			"waiters", n, "saturatedMiB", over>>20, "budgetMiB", int64(store.WaiterBudgetBytes)>>20,
			"perWaiterKiB", store.WaiterCostBytes>>10,
			"note", "parked lookups are unauthenticated HTTP handlers held for up to -wait-timeout; raise the pod's memory to match")
	}
	return nil
}

func run() error {
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The process logger, and every other logger in the process routed into it:
	// client-go's klog, and grpc-go's grpclog (this binary dials a collector
	// for its self-metrics, and grpc's default logger writes its connection
	// failures to stderr in its own format).
	log, err := cli.SetupLogging(*logLevel)
	if err != nil {
		return err
	}
	// First line of every run: without a build identity a panic trace, a
	// metric anomaly or a half-finished rollout cannot be tied to a commit.
	log.Info("kubescrape starting", "version", obs.BuildVersion(), "built", obs.BuildTime())
	// Bound the Go heap goal by this container's memory limit, before anything
	// large is allocated. A no-op for THIS binary as shipped — the chart gives
	// the metadata service no memory limit on purpose — but the call is
	// unconditional so a cluster that has measured its own footprint and set
	// one gets the insurance without also having to discover an env var.
	cli.SetMemoryLimit(log)

	// Every refusal this process makes before it acquires anything, and the
	// warnings beside them (validate.go). -check-config runs exactly this call
	// and nothing else, so a dry run cannot accept a command line the rollout
	// then refuses.
	if err := validateConfig(log); err != nil {
		return err
	}

	cfg, err := kubecfg.KubeConfig(*kubeconfig)
	if *checkConfig {
		// The dry run ends here, before the first client exists. A kubeconfig
		// it cannot resolve is NOT a verdict on the configuration — the
		// pre-flight is a local binary (docs/FIRST-RUN.md), where in-cluster
		// credentials are absent by definition — so the destination is named
		// as unresolved and the check still passes. The environment is the
		// real start's to judge, the same line the token file is on.
		host := "(unresolved)"
		if err != nil {
			log.Warn("no API server could be resolved here, so the dry run cannot say whether this deployment will reach one",
				"error", err, "kubeconfig", *kubeconfig,
				"note", "expected off-cluster: neither in-cluster credentials nor a kubeconfig are present")
		} else {
			host = cfg.Host
		}
		logStartupSummary(log, host)
		// The verdict, on its own line: everything above it is a description,
		// and a dry run's exit status is not visible in a CI log's scrollback.
		log.Info("config is valid")
		return nil
	}
	if err != nil {
		return fmt.Errorf("building kubernetes client config: %w", err)
	}
	cfg.UserAgent = "kubescrape"
	// The informers are watch-driven; the higher limits only matter for the
	// initial (paginated) list and for relists after watch gaps.
	cfg.QPS = 50
	cfg.Burst = 100

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("creating kubernetes client: %w", err)
	}
	// The effective configuration, emitted once the API server it will talk to
	// is resolved (that destination comes from the environment or a kubeconfig,
	// so it is the one nobody can read off the command line). Everything below
	// this line acquires something; everything above it is what an operator
	// needs to see when what follows fails.
	logStartupSummary(log, cfg.Host)
	metaClient, err := metadata.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("creating metadata client: %w", err)
	}

	st := newMetadataStore(*cacheTTL, *maxWaiters)
	obs.RegisterStoreStats(st.Stats)
	obs.RegisterWaiterStats(st.BlockedLookups, st.ShedLookups, st.DrainedLookups)

	// Full objects (spec+status are both read), minus what nothing reads:
	// trimPod drops managedFields from everything and, for PODS ONLY, the bulk
	// of the spec that never reaches kubemeta.Pod. Service specs are left
	// intact — services.Index reads them.
	factory := informers.NewSharedInformerFactoryWithOptions(client, *resync,
		informers.WithTransform(trimPod))
	svcIndex := services.NewIndex()
	// The two index guards that keep the served data correct while a Delete is
	// missed, and the pod-IP recycle race. Both are decided under a write lock
	// on the informer goroutine, where nothing may log — see
	// obs.RegisterStoreAnomalies for what each one means.
	obs.RegisterStoreAnomalies(st.NameReuses, svcIndex.NameReuses, st.ContestedPodIPs)
	synced, err := registerCoreInformers(factory, st, svcIndex)
	if err != nil {
		return err
	}

	// Metadata-only informers (PartialObjectMetadata) for owner-chain and
	// namespace enrichment: labels/annotations/ownerRefs only, no specs
	// cached.
	metaFactory := metadatainformer.NewSharedInformerFactory(metaClient, *resync)
	listers, ownerSynced, err := registerOwnerInformers(metaFactory)
	if err != nil {
		return err
	}
	synced = append(synced, ownerSynced...)
	resolver := owners.NewFromListers(listers)

	var monitors *servicemonitors.Index
	if *monitorsOn {
		idx, smSynced, err := startServiceMonitors(ctx, cfg, client.Discovery(), *resync, parseNamespaceSet(*monitorNamespaces), log)
		if err != nil {
			return err
		}
		if idx != nil {
			monitors = idx
			synced = append(synced, smSynced...)
		}
	}

	var exporter *otlpexport.Client
	if *selfMetricsIntv > 0 {
		var err error
		exporter, err = otlpexport.New(otlpexport.Config{
			Endpoint:           *otlpEndpoint,
			Protocol:           *otlpProtocol,
			Compression:        *otlpCompression,
			CompressionLevel:   *otlpCompressionLevel,
			Insecure:           *otlpInsecure,
			InsecureSkipVerify: *otlpSkipTLS,
			CAFile:             *otlpCAFile,
			BearerTokenFile:    *otlpBearer,
			Timeout:            *otlpTimeout,
			Headers:            otlpHeaders.m,
		})
		if err != nil {
			return fmt.Errorf("creating OTLP exporter: %w", err)
		}
		defer func() {
			// Swallowed until now: a failing Close is the last thing this
			// process can say about its connection to the collector, at exactly
			// the moment an operator is reading the log to find out what the
			// shutdown lost.
			if err := exporter.Close(); err != nil {
				log.Warn("closing the OTLP exporter", "error", err)
			}
		}()
	}
	// The self-metrics goroutine joins this group; run waits for its final
	// export before returning, so it finishes before the deferred
	// exporter.Close fires (mirrors the agent).
	var wg sync.WaitGroup
	// One DEADLINE for the whole shutdown sequence, rather than a fixed budget
	// per step (the agent's shutdownBy, same reasoning). The steps below are
	// each individually reasonable and their SUM was not: 10s draining the HTTP
	// server, then up to metrics.FinalExportTimeout inside the producers' join
	// for Registry.Run's own shutdown export, then another 10s for the final
	// export — 30s before the deferred stopPprof/stopMetrics spend 5s each.
	// This Deployment sets no terminationGracePeriodSeconds (unlike the agent's
	// manifests, which set 60), so it runs on Kubernetes' 30s DEFAULT and was
	// SIGKILLed mid-sequence against an unreachable collector, losing the very
	// exports the budgets exist to fit inside it. Sharing a deadline means a
	// slow step spends what the later ones would have had, and nothing overruns.
	var shutdownBy time.Time
	// stepBudget is what remains of the deadline, capped per step. A zero
	// shutdownBy means we are NOT on the signal path — an early return, where
	// nothing is racing a termination grace — and each step gets its full
	// budget.
	deadlineWarned := false
	stepBudget := func() time.Duration {
		if shutdownBy.IsZero() {
			return shutdownStep
		}
		budget := max(0, min(shutdownStep, time.Until(shutdownBy)))
		// A step reached with nothing left does not fail loudly — it gets a
		// dead context and returns instantly — so the shared deadline being
		// blown is otherwise indistinguishable from a fast, clean shutdown.
		// Once, not per step: what is lost after the first zero is every step
		// after it, and the operator needs the fact, not five copies of it.
		if budget == 0 && !deadlineWarned {
			deadlineWarned = true
			log.Warn("shutdown deadline exceeded; the remaining shutdown steps get no budget and their final exports are lost",
				"budget", shutdownTotal)
		}
		return budget
	}
	// Registered AFTER exporter.Close (LIFO): an early `return err` below must
	// stop and drain the started goroutines BEFORE the exporter is closed under
	// them. The normal path's inline join makes this a no-op there — and it is
	// bounded by the same deadline, or a join that gave up on the signal path
	// would simply block here instead, spending the budget twice.
	defer func() {
		stop()
		_ = waitFor(&wg, stepBudget())
	}()
	var selfRes pcommon.Resource
	// selfOut is the exporter plus this pod's own Kubernetes attributes,
	// filled in where the identity below left a key unset. Used by BOTH the
	// periodic run and the final export — the last data point of a series must
	// not carry a different resource than the rest. It stays nil while
	// self-metrics are off, so a future use outside that guard fails loudly
	// rather than exporting through a nil client.
	var selfOut selfmeta.Exporter
	if *selfMetricsIntv > 0 {
		selfRes = serviceSelfResource()
		selfOut = exporter
		if *selfAttrs {
			// This process's own pod, out of its own store — no HTTP hop, no
			// downward API. The first lookups retry from 5s, since the
			// informers only fill in after this point; the slow refresh past
			// that picks up relabelling.
			selfPod := selfmeta.StartPod(ctx, selfResolver(st, resolver), selfmeta.DefaultRefresh, log)
			obs.RegisterSelfMetadata(func() bool { return selfPod() != nil })
			selfOut = selfmeta.Wrap(selfOut, selfPod, selfBuild)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			obs.Registry.Run(ctx, selfOut, *selfMetricsIntv, selfRes, log)
		}()
		log.Info("self-metrics export started", "interval", *selfMetricsIntv)
	}

	factory.Start(ctx.Done())
	metaFactory.Start(ctx.Done())
	go st.Run(ctx)

	// The only ACTIVE check that the API server is still there. Everything else
	// in this process is watch-driven, and a watch that keeps failing retriably
	// reports nothing at all, so the passive signals cannot be relied on for a
	// connection that has gone away (apiserver.go carries the mechanism).
	if startAPIServerWatchdog(ctx, apiserverProbe(metaClient), *apiserverProbeInterval, log) != nil {
		log.Info("api server probe started", "interval", *apiserverProbeInterval)
	}

	// Readiness LATCHES here, and that is deliberate — do not "fix" it by
	// re-evaluating the informers' health per request.
	//
	// /readyz gates the Deployment's Service endpoints. Flipping it to 503
	// when the API server later becomes unreachable would DELETE those
	// endpoints and cut the whole agent fleet off a cache that is still
	// serving useful data: pods do not vanish because the API server did, and
	// a slightly stale answer beats no answer for every consumer here (log
	// attribution, scrape targets, ingest enrichment). Availability is the
	// right trade here.
	//
	// What was missing is the OTHER half: making the staleness visible without
	// also withdrawing the service. That is kubescrape_apiserver_reachable (the
	// watchdog above), and it is where the alert belongs — a gauge can say
	// "stale" without also saying "go away".
	//
	// The WAIT is what says which cache is holding it: client-go's
	// WaitForCacheSync takes bare funcs and can only ever report "not synced",
	// which for the failure this actually has (one resource 403-looping on a
	// missing RBAC rule) is the half of the message nobody can act on.
	ready := make(chan struct{})
	// One gauge per cache, so a replica stuck here is visible to an alert and
	// not only to whoever can curl its /readyz — which, for a Deployment that
	// never becomes ready and therefore has no Service endpoints, is nobody.
	obs.RegisterReadiness(gateStates(synced))
	go waitForCaches(ctx, synced, st, log, ready)

	// HTTPServer sets the full hardened timeout set (ReadHeaderTimeout,
	// Read/WriteTimeout > MaxWait, IdleTimeout); see its doc comment.
	var secretReader server.SecretReader
	var scrapeAuthTokens func() []string
	if *scrapeAuthOn {
		// Read the token BEFORE anything starts serving: an unauthenticated
		// /v1/scrape-auth is a cluster-wide secret leak, so a missing or empty
		// token file is a startup failure, never a warning. After startup the
		// file is re-read periodically and a rotated token keeps its
		// predecessor valid for a grace window (see newScrapeAuthTokens).
		rt, err := newScrapeAuthTokens(*scrapeAuthTokenFile, log)
		if err != nil {
			return err
		}
		scrapeAuthTokens = rt.Tokens
		// Detection runs on a CLOCK, not only on request traffic — see
		// bearer.Rotating.Run, which owns that decision now. It was a goroutine
		// here and nowhere else, and the receiver that did not copy it (the
		// trace tier's authenticated hop) accepted a revoked token indefinitely.
		wg.Add(1)
		go func() {
			defer wg.Done()
			rt.Run(ctx)
		}()
		secretReader = &k8sSecretReader{client: client}
		log.Info("scrape auth secrets enabled", "tokenFile", *scrapeAuthTokenFile)
	}
	serverCfg := server.Config{
		Store:            st,
		Services:         svcIndex,
		Monitors:         monitors,
		Resolver:         resolver,
		MaxWait:          *maxWait,
		CacheTTL:         *metaCacheTTL,
		Ready:            ready,
		Secrets:          secretReader,
		ScrapeAuthTokens: scrapeAuthTokens,
		Log:              log,
	}
	if err := serverCfg.Validate(); err != nil {
		return err
	}
	api := server.New(serverCfg)

	// WHO MAY READ THIS PORT, stated deliberately rather than left to be
	// inferred from the absence of a gate. The agent's -listen port carries a
	// bearer/local gate (cmd/kubescrape-agent/debugauth.go) because /debug/otlp
	// streams the node's telemetry BODIES — every tenant's log lines. Nothing
	// on this port is of that class, and the difference is worth writing down:
	//
	//   - the /v1 metadata routes are UNAUTHENTICATED BY DESIGN. Every agent in
	//     the cluster polls them on a cycle, they return pod/owner/namespace
	//     metadata the Kubernetes API already serves to anything holding a read
	//     token, and they carry no log line, no metric sample and no credential
	//     — kubemeta.FilterAnnotations drops the deploy-tool annotations that
	//     would smuggle an applied spec through. /v1/explain is in that set: it
	//     explains a scrape decision about a pod, which is the same metadata in
	//     narrative form, and an operator debugging a rollout must be able to
	//     curl it.
	//   - /v1/scrape-auth is the ONE exception and the one authenticated route
	//     (-scrape-auth-token-file, checked before the AuthSecretRefs
	//     allowlist), because it hands back resolved Secret material.
	//   - /debug here is a STATIC page of forms that navigate to the /v1 routes
	//     above; it holds no data of its own, which is what
	//     TestMetadataServiceServesNoDataBearingDebugSurface pins.
	//   - /healthz and /readyz stay open so the kubelet's probes and a rolling
	//     update work at all.
	//
	// Adding a route here that serves telemetry BODIES — a tap, a dump, a live
	// stream — would need the agent's gate, not this comment.
	srv := api.HTTPServer(*listen)

	// With the OTLP self-metrics push disabled (-self-metrics-interval=0) the
	// kubescrape_* metrics ride the /metrics scrape instead — the service then
	// needs no OTLP endpoint at all.
	stopMetrics, err := obs.ServeMetrics(*metricsListen, *selfMetricsIntv <= 0, log)
	if err != nil {
		// Fatal, like the ingest listener: with the OTLP push off this port is
		// the only path every kubescrape_* metric has.
		return err
	}
	defer stopMetrics()
	stopPprof, err := obs.ServePprof(*pprofListen, log)
	if err != nil {
		// An operator who asked for a profiling port and did not get one should
		// not have to find that out in the log.
		return err
	}
	defer stopPprof()

	errCh := make(chan error, 1)
	go func() {
		log.Info("metadata api listening", "addr", *listen)
		errCh <- srv.ListenAndServe()
	}()

	var runErr error
	select {
	case err := <-errCh:
		runErr = fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
		log.Info("shutting down")
		shutdownBy = time.Now().Add(shutdownTotal)
		// WithoutCancel(ctx) rather than a bare Background: ctx is already
		// cancelled, but its VALUES must survive into every shutdown step (the
		// repo-wide rule — otlpexport's ownership marker rides on a context).
		// api.Drain, not st.Drain: a container lookup parks in TWO places and
		// both have to be released (the server's wait for the initial sync is
		// the one a SIGTERM during startup hits, when the store has no waiters
		// at all).
		runErr = shutdownHTTP(context.WithoutCancel(ctx), srv, api.Drain, api.InFlight, stepBudget(), log)
	}
	// Cancel ctx (a no-op on the signal path) and wait for the exporting
	// goroutines' final flushes before the deferred exporter.Close fires.
	// BOUNDED: Registry.Run's shutdown branch opens its own FinalExportTimeout
	// that this process cannot shorten, so an unreachable collector would
	// otherwise spend it in full and leave nothing for the export below.
	// Missing the deadline costs nothing that is not already lost — the
	// goroutines' own exports are what is being waited for.
	stop()
	if !waitFor(&wg, stepBudget()) {
		log.Warn("exporting goroutines did not stop within the shutdown budget; continuing with the final export")
	}
	if *selfMetricsIntv > 0 {
		// Registry.Run's own final export raced the final flushes inside
		// wg.Wait (the events drain, the last batches); counters they bumped
		// would otherwise die unexported. One more export now that all are done.
		// Bounded here, by us: ctx is cancelled by this point, and a dead
		// collector must not hold the process past its termination grace.
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stepBudget())
		obs.Registry.FinalExport(fctx, selfOut, selfRes, log)
		cancel()
	}
	return runErr
}

// shutdownTotal bounds the WHOLE shutdown sequence run() controls and
// shutdownStep any single step of it.
//
// The total is deliberately well under the 30s the kubelet gives a pod that
// names no terminationGracePeriodSeconds — which is what this Deployment is:
// the two deferred listener stoppers (obs.ServeMetrics/ServePprof, 5s each) run
// AFTER this sequence, so 15 + 5 + 5 = 25s leaves the kubelet's own overhead
// and the exporter close inside the grace. shutdownStep is
// metrics.FinalExportTimeout, so nothing changes on the path that has the whole
// budget to itself.
const (
	shutdownTotal = 15 * time.Second
	shutdownStep  = metrics.FinalExportTimeout
)

// shutdownHTTP releases the parked container lookups and then drains the HTTP
// server within budget. It returns an error only for a shutdown failure that is
// not the deadline; a missed deadline is reported as a WARN.
//
// The ORDER is the fix, and the mechanism is worth stating exactly, because the
// obvious reading of it is wrong. srv.Shutdown stops the listeners, closes the
// IDLE connections and then waits for the active handlers; at its deadline it
// merely RETURNS context.DeadlineExceeded — it does not touch an active
// connection (only srv.Close does), and a handler that finishes afterwards
// still writes its response to a client that is still attached. What actually
// cut the observed request was the PROCESS EXITING a few steps later: run
// returns, the sockets die with it, and the client sees "Empty reply from
// server" — no status, no body, nothing an agent can classify — while the
// process exited 0 with four INFO lines and no hint anything had been dropped.
// Draining FIRST turns every parked lookup into a 503 + Retry-After that
// finishes well inside the step, and the deadline — which used to be swallowed
// on purpose — now WARNS with what is still in flight and about to be cut by
// the exit, because that silence is what made this invisible in the first place.
func shutdownHTTP(ctx context.Context, srv *http.Server, drain func() int, inFlight func() int64, budget time.Duration, log *slog.Logger) error {
	if n := drain(); n > 0 {
		// Worth a line of its own: these clients got a refusal rather than the
		// metadata they asked for, and it is the one loss a graceful shutdown
		// causes. The count spans BOTH parking spots (the store's per-ID waiters
		// and the requests waiting on the initial sync);
		// kubescrape_container_lookups_drained_total carries the store's share
		// into the final export.
		log.Info("released blocked container lookups so they can be answered", "lookups", n)
	}
	sctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	err := srv.Shutdown(sctx)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.DeadlineExceeded):
		// Shutdown gave up WAITING; it did not close anything. These handlers
		// keep running and their clients stay attached until the process exits
		// moments later, which is what cuts them without a response.
		log.Warn("http shutdown budget exceeded; requests still in flight will be cut without a response when the process exits",
			"budget", budget, "requestsInFlight", inFlight())
		return nil
	default:
		return fmt.Errorf("http shutdown: %w", err)
	}
}

// waitFor waits for wg with a deadline, reporting whether it finished in time.
func waitFor(wg *sync.WaitGroup, budget time.Duration) bool {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(budget):
		return false
	}
}

// headerFlags collects repeatable -otlp-header key=value flags. Repeatable
// rather than comma-separated so a header VALUE may contain commas.
type headerFlags struct{ m map[string]string }

func (h *headerFlags) String() string { return fmt.Sprint(h.m) }

func (h *headerFlags) Set(v string) error {
	key, value, ok := strings.Cut(v, "=")
	key = strings.TrimSpace(key)
	if !ok || key == "" {
		return fmt.Errorf("want key=value, got %q", v)
	}
	if h.m == nil {
		h.m = map[string]string{}
	}
	h.m[key] = value
	return nil
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
// CRD, or a monitor that simply matches nothing. Counted per kind, and logged
// at Debug (an informer resync re-delivers every object, so Info would repeat
// the same line for every refused monitor on every resync).
func monitorAllowed(allowNS map[string]bool, kind string, u *unstructured.Unstructured, log *slog.Logger) bool {
	if allowNS == nil || allowNS[u.GetNamespace()] {
		return true
	}
	obs.MonitorNamespaceRefused.WithLabelValues(kind).Inc()
	log.Debug("monitor ignored: its namespace is not permitted by -monitor-namespaces",
		"kind", kind, "monitor", u.GetNamespace()+"/"+u.GetName())
	return false
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

// stripManagedFields drops managedFields — and the annotations this API refuses
// to serve — before objects are stored in the informer caches. It goes through
// apimeta.Accessor, so it serves the typed, metadata-only AND unstructured
// informers alike — every one of them must call it.
//
// The annotations are dropped here for the reason trimPod drops the pod spec:
// they are resident for the process lifetime and can NEVER be read, since
// owners.Resolve and the namespace/node lookups funnel everything through
// kubemeta.CopyMeta → FilterAnnotations. A kubectl- or kapp-managed cluster
// with a few thousand Deployments/ReplicaSets/Jobs/CronJobs carries several
// megabytes of them in PartialObjectMetadata alone, against a 128Mi request.
func stripManagedFields(obj any) (any, error) {
	acc, err := apimeta.Accessor(obj)
	if err != nil {
		// A "should not happen" branch that used to be a silent `err == nil`
		// guard: the object goes into the cache UNTRIMMED, so the only symptom
		// is RSS climbing with managedFields nothing can ever read. Throttled,
		// because it would fire once per object per resync if it ever fired at
		// all, and typed rather than dumped — the value is an informer object
		// and this line must not print a whole pod.
		if transformWarn.Allow(transformWarnEvery) {
			slog.Default().Warn("informer transform could not read an object's metadata; it is cached untrimmed",
				"error", err, "type", fmt.Sprintf("%T", obj))
		}
		return obj, nil
	}
	acc.SetManagedFields(nil)
	// SetAnnotations only when something was actually removed: the unstructured
	// accessor rebuilds the whole map on a set, and the overwhelmingly common
	// object carries neither key.
	if ann := acc.GetAnnotations(); kubemeta.StripDroppedAnnotations(ann) {
		acc.SetAnnotations(ann)
	}
	return obj, nil
}

// The transform's failure throttle. One line per interval for a condition that
// would otherwise repeat per object per resync — the repo's keyless throttle
// (internal/logdedupe), never a hand-rolled one.
var transformWarn logdedupe.Throttle

const transformWarnEvery = 5 * time.Minute

// trimPod is the pod informer's transform: strip managedFields like every
// other informer, then drop the parts of the SPEC and the STATUS nothing here
// reads.
//
// The pod cache is the service's dominant memory cost, and kubeconvert.FromPod
// consumes a thin slice of the spec — NodeName, HostNetwork, and each
// container's Name/Image/Ports. Everything else the API server sends (env
// vars, volumes and their mounts, resource requirements, the three probes,
// lifecycle hooks, affinity, tolerations, scheduling gates) is retained
// verbatim for the process lifetime and never read: on a large cluster that is
// tens of megabytes of resident heap against a 128Mi request. trimPodStatus
// below does the same for the status, which on a current kubelet is nearly
// HALF of what this trim leaves behind.
//
// It runs as a TYPE SWITCH rather than trimming unconditionally because the
// Service informer shares this factory and services.Index genuinely reads
// Service specs (selector and ports) — trimming those would break scrape
// discovery. Anything that is not a *corev1.Pod — Services — falls through to
// the managedFields strip alone.
//
// Tombstones do NOT reach here: both FIFO implementations skip the transformer
// for DeletedFinalStateUnknown (delta_fifo.go and the_real_fifo.go in client-go
// v0.36.3), and the tombstone's inner object was already transformed on its way
// into the store.
//
// Trimming is idempotent, which matters: client-go may apply a transform to an
// object more than once.
func trimPod(obj any) (any, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return stripManagedFields(obj)
	}
	pod.ManagedFields = nil
	// The same never-readable annotations stripManagedFields drops for every
	// other informer: kubeconvert.FromPod serves pod annotations through
	// CopyMeta, which filters them out, so a cached copy is pure resident cost.
	kubemeta.StripDroppedAnnotations(pod.Annotations)

	trimContainers(pod.Spec.Containers)
	trimContainers(pod.Spec.InitContainers)
	for i := range pod.Spec.EphemeralContainers {
		// EphemeralContainerCommon is field-identical to Container by
		// construction (upstream keeps them in lockstep), so ONE trim serves
		// both. It was written out twice, and the copy was exercised for a
		// single field — exactly the shape that drifts.
		trimContainer((*corev1.Container)(&pod.Spec.EphemeralContainers[i].EphemeralContainerCommon))
	}

	// Pod-level spec fields, none of which reach kubemeta.Pod.
	pod.Spec.Volumes = nil
	pod.Spec.Tolerations = nil
	pod.Spec.Affinity = nil
	pod.Spec.NodeSelector = nil
	pod.Spec.SecurityContext = nil
	pod.Spec.ImagePullSecrets = nil
	pod.Spec.TopologySpreadConstraints = nil
	pod.Spec.ReadinessGates = nil
	pod.Spec.SchedulingGates = nil
	pod.Spec.ResourceClaims = nil
	pod.Spec.Overhead = nil
	pod.Spec.DNSConfig = nil
	pod.Spec.HostAliases = nil

	trimPodStatus(&pod.Status)
	return pod, nil
}

func trimContainers(cs []corev1.Container) {
	for i := range cs {
		trimContainer(&cs[i])
	}
}

// trimContainer drops the fields of one container that never reach
// kubemeta.Container (which takes Name, Image and Ports, and nothing else).
func trimContainer(c *corev1.Container) {
	c.Command = nil
	c.Args = nil
	c.WorkingDir = ""
	c.Env = nil
	c.EnvFrom = nil
	c.Resources = corev1.ResourceRequirements{}
	c.ResizePolicy = nil
	c.VolumeMounts = nil
	c.VolumeDevices = nil
	c.LivenessProbe = nil
	c.ReadinessProbe = nil
	c.StartupProbe = nil
	c.Lifecycle = nil
	c.SecurityContext = nil
}

// trimPodStatus is the STATUS half of the trim: the spec trim above left the
// status whole, and every release since 1.31 has widened ContainerStatus with
// a per-container field nothing here reads. Measured as RETAINED heap over
// protobuf-decoded pods (two containers — the shape client-go actually
// caches), spec-trimmed and then status-trimmed:
//
//	pre-1.31 kubelet                          5896 ->  5400 B/pod
//	+ volumeMounts        (1.31, beta/on)     6792 ->  5416 B/pod
//	+ user/resources      (1.33, beta/on)     9832 ->  5416 B/pod  = 88 MB @20k pods
//	+ allocatedResources  (alpha)            11256 ->  5416 B/pod  = 117 MB @20k pods
//	a CrashLoopBackOff pod, 1.33               10824 ->  5848 B/pod
//
// against a chart that requests 128Mi and sets no limit. The 1.33 row is 45%
// of what the informer cache holds per pod — an unchanged 20k-pod deployment
// goes from 256 MB to 168 MB of pods, cache plus store. Nothing names the
// waste while it accumulates: the store holds none of it, so
// kubescrape_store_pods is unchanged and the only symptom is RSS.
//
// What it costs is one allocation and ~450 ns per pod event, on the informer
// goroutine (indicative; measured on a loaded machine).
//
// These are KEEP-lists, not drop-lists, and deliberately so: the drop-list
// style trimContainer uses above has to be extended by hand for every field a
// future k8s adds, and the fields being added are exactly the fat ones
// (in-place resize alone put a ResourceRequirements on every container status,
// worth 1.4 KB/container). Rebuilding the struct from the fields
// kubeconvert.FromPod reads means a new API field costs nothing the day it
// ships. It is a struct assignment, so it allocates nothing; only the kept
// condition does, and it frees a four-element array to do it.
//
// What FromPod reads is the whole of the keep-list below and nothing else —
// TestTrimPodPreservesEverythingFromPodReads converts the fat pod and the
// trimmed pod and requires the results to be identical, over a fixture that
// populates every field named here (and gives the status a DIFFERENT resolved
// image than the spec, so a future read of ContainerStatus.Image fails the
// guard instead of matching by coincidence).
func trimPodStatus(st *corev1.PodStatus) {
	trimContainerStatuses(st.InitContainerStatuses)
	trimContainerStatuses(st.ContainerStatuses)
	trimContainerStatuses(st.EphemeralContainerStatuses)
	*st = corev1.PodStatus{
		Phase:                      st.Phase,
		Conditions:                 readyConditionOnly(st.Conditions),
		HostIP:                     st.HostIP,
		PodIP:                      st.PodIP,
		PodIPs:                     st.PodIPs,
		StartTime:                  st.StartTime,
		InitContainerStatuses:      st.InitContainerStatuses,
		ContainerStatuses:          st.ContainerStatuses,
		EphemeralContainerStatuses: st.EphemeralContainerStatuses,
	}
}

// readyConditionOnly reduces the condition list to the one condition
// kubemeta.Pod.Ready is derived from, carrying only its Status.
//
// A fresh one-element slice rather than a reslice of the original: the
// kubelet reports four conditions on every healthy pod, and a reslice keeps
// the whole four-element array (with both timestamps and the reason/message
// strings of the three dropped entries) alive for the process lifetime, which
// is the cost this is here to avoid.
func readyConditionOnly(cs []corev1.PodCondition) []corev1.PodCondition {
	for i := range cs {
		if cs[i].Type == corev1.PodReady {
			return []corev1.PodCondition{{Type: cs[i].Type, Status: cs[i].Status}}
		}
	}
	return nil
}

func trimContainerStatuses(cs []corev1.ContainerStatus) {
	for i := range cs {
		trimContainerStatus(&cs[i])
	}
}

// trimContainerStatus keeps the fields convertContainer and
// previousIncarnation fold into kubemeta.Container. Image is NOT among them:
// the model's Image comes from the SPEC container, and the status copy is the
// runtime-resolved duplicate of it.
func trimContainerStatus(c *corev1.ContainerStatus) {
	trimContainerState(&c.State)
	trimContainerState(&c.LastTerminationState)
	*c = corev1.ContainerStatus{
		Name:                 c.Name,
		State:                c.State,
		LastTerminationState: c.LastTerminationState,
		Ready:                c.Ready,
		RestartCount:         c.RestartCount,
		ImageID:              c.ImageID,
		ContainerID:          c.ContainerID,
	}
}

// trimContainerState keeps the arms in place (replacing them would allocate on
// every informer event) and clears the fields inside them that no reader
// takes. The two casualties are the ones worth naming: a waiting container's
// Message is the "back-off 5m0s restarting failed container=..." line, and a
// terminated one's is up to 4 KiB of termination message — per container, on
// exactly the pods a struggling cluster has most of.
//
// Terminated.ContainerID is kept in BOTH states although only
// LastTerminationState's is read (it is the previous incarnation's ID). One
// trim serves both arms, and the cost is one string on terminated containers.
func trimContainerState(s *corev1.ContainerState) {
	if s.Waiting != nil {
		*s.Waiting = corev1.ContainerStateWaiting{Reason: s.Waiting.Reason}
	}
	if s.Terminated != nil {
		*s.Terminated = corev1.ContainerStateTerminated{
			ExitCode:    s.Terminated.ExitCode,
			StartedAt:   s.Terminated.StartedAt,
			FinishedAt:  s.Terminated.FinishedAt,
			ContainerID: s.Terminated.ContainerID,
		}
	}
	// ContainerStateRunning holds StartedAt and nothing else; there is
	// nothing to drop, and a keep-list here would be a struct copy onto
	// itself.
}

// watchErrors installs a watch-error handler that counts before delegating to
// client-go's default (which keeps the standard logging and the
// expired-resourceVersion handling).
//
// Readiness LATCHES: /readyz gates on the initial sync and is never
// re-evaluated. So a list/watch that breaks AFTER that — revoked RBAC, a
// deleted CRD, an apiserver rejecting the watch — leaves the reflector
// retrying forever while /readyz stays 200 and every response is served from a
// cache that has quietly stopped advancing. Nor do the store gauges freeze at
// plausible values: the tombstone sweeper keeps running over a store nothing
// refills, so kubescrape_store_pods DECAYS (measured 89 -> 85 over five
// minutes) and an alert on a FLAT gauge reads healthy exactly when it must not.
// Without this the only trace is a klog line, which is not alertable; the
// startup half of exactly this failure was already found and fixed once (the
// PodMonitor informer that 403-looped behind a green /readyz).
//
// It covers the refusals the API server ANSWERS, and a failed relist. It does
// NOT reliably cover an UNREACHABLE server — that is what the reachability
// probe in apiserver.go is for; obs.InformerWatchErrors carries the mechanism.
//
// Must be called before the informer is started.
func watchErrors(inf cache.SharedInformer, resource string) error {
	return inf.SetWatchErrorHandlerWithContext(func(ctx context.Context, r *cache.Reflector, err error) {
		obs.InformerWatchErrors.WithLabelValues(resource).Inc()
		cache.DefaultWatchErrorHandler(ctx, r, err)
	})
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
