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
	"k8s.io/klog/v2"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/bearer"
	"github.com/JohanLindvall/kubescrape/internal/cli"
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
func registerCoreInformers(factory informers.SharedInformerFactory, st *store.Store, svcIndex *services.Index) ([]cache.InformerSynced, error) {
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
	return []cache.InformerSynced{podReg.HasSynced, svcReg.HasSynced}, nil
}

// registerOwnerInformers wires a metadata-only informer per owner GVR,
// returning the listers the owner resolver reads and their HasSynced funcs.
func registerOwnerInformers(metaFactory metadatainformer.SharedInformerFactory) (map[schema.GroupVersionResource]cache.GenericLister, []cache.InformerSynced, error) {
	listers := make(map[schema.GroupVersionResource]cache.GenericLister, len(owners.AllGVRs))
	var synced []cache.InformerSynced
	for _, gvr := range owners.AllGVRs {
		inf := metaFactory.ForResource(gvr)
		if err := inf.Informer().SetTransform(stripManagedFields); err != nil {
			return nil, nil, fmt.Errorf("setting %s informer transform: %w", gvr.Resource, err)
		}
		if err := watchErrors(inf.Informer(), gvr.Resource); err != nil {
			return nil, nil, fmt.Errorf("%s watch error handler: %w", gvr.Resource, err)
		}
		listers[gvr] = inf.Lister()
		synced = append(synced, inf.Informer().HasSynced)
	}
	return listers, synced, nil
}

// startServiceMonitors sets up and starts the dynamic ServiceMonitor informer.
// When the CRD is unavailable the feature is disabled with a warning and a nil
// Index is returned (not an error).
func startServiceMonitors(ctx context.Context, cfg *rest.Config, disco discovery.DiscoveryInterface, resync time.Duration, allowNS map[string]bool, log *slog.Logger) (*servicemonitors.Index, cache.InformerSynced, error) {
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
	var synced []cache.InformerSynced
	if haveSM {
		smSynced, err := monitorInformer(dynFactory, servicemonitors.GVR, "servicemonitor", allowNS, log,
			monitors.Upsert, monitors.Delete, monitors.Endpoints)
		if err != nil {
			return nil, nil, err
		}
		// Readiness must cover every cache a request can read, so collect the
		// handler registrations rather than returning the ServiceMonitor's alone:
		// /v1/nodes/{node}/targets reads the PodMonitor index too, and leaving it
		// out let /readyz report 200 — advancing a rollout — while that index was
		// empty, or permanently so when podmonitors RBAC is missing and its LIST
		// 403-loops forever.
		synced = append(synced, smSynced)
	}

	// PodMonitors are an optional sibling, watched when the cluster serves it —
	// independently of the ServiceMonitor CRD above. (Probes are deliberately
	// not supported at all: blackbox probing has no node affinity and does not
	// fit the node-local model.)
	if havePM {
		pmSynced, err := monitorInformer(dynFactory, servicemonitors.PodGVR, "podmonitor", allowNS, log,
			monitors.UpsertPodMonitor, monitors.DeletePodMonitor, monitors.PodEndpoints)
		if err != nil {
			return nil, nil, err
		}
		synced = append(synced, pmSynced)
		log.Info("podmonitor discovery enabled")
	}
	dynFactory.Start(ctx.Done())
	// Name what is actually watched: with only one CRD installed, claiming
	// "servicemonitor discovery enabled" was wrong half the time.
	log.Info("monitor discovery enabled", "servicemonitors", haveSM, "podmonitors", havePM)
	return monitors, func() bool {
		for _, s := range synced {
			if !s() {
				return false
			}
		}
		return true
	}, nil
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
	upsert func(*unstructured.Unstructured) error,
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
			if err := upsert(u); err != nil {
				// Counted, not just logged: an unparseable monitor DELETES it
				// from the index, dropping every target it contributed. That
				// is strictly more severe than the "some endpoint fields were
				// ignored" case, which does get a metric — so the severe one
				// must not be the unalertable one.
				obs.MonitorParseErrors.WithLabelValues(kind).Inc()
				log.Warn("parsing "+kind, "error", err,
					"namespace", u.GetNamespace(), "name", u.GetName())
				return
			}
			warnIgnored(log, kind, u, endpoints(u.GetNamespace(), u.GetName()))
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
// Past startup the file is re-read on bearer.DefaultReadInterval and a rotated
// token keeps its predecessor accepted for bearer.DefaultGrace, so agents —
// which re-read their copy on their own cadence — never have to flip in
// lockstep with the service.
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
	listen        = flag.String("listen", ":8080", "HTTP listen address")
	pprofListen   = flag.String("pprof-listen", "", "listen address for net/http/pprof under /debug/pprof, on its own port (empty disables); profiles expose goroutine stacks and heap contents")
	metricsListen = flag.String("metrics-listen", ":9090", "listen address for the Prometheus /metrics endpoint (Go runtime and process metrics; with -self-metrics-interval=0 also the kubescrape_* internal metrics, replacing the OTLP push with a scrape; empty disables). Separate from -listen so the API and the scrape target can be exposed independently")
	kubeconfig    = flag.String("kubeconfig", "", "path to a kubeconfig; defaults to in-cluster config, then $KUBECONFIG/~/.kube/config")
	maxWait       = flag.Duration("wait-timeout", 5*time.Second, "default and maximum time a container lookup blocks waiting for metadata to appear (shorten per request with ?wait=)")
	cacheTTL      = flag.Duration("cache-ttl", 5*time.Minute, "how long metadata of deleted pods and replaced container IDs stays resolvable")
	metaCacheTTL  = flag.Duration("metadata-cache-ttl", 10*time.Second, "max-age sent on metadata responses (Cache-Control + ETag) so agents cache lookups client-side; 0 disables the cache headers. The server-side ServiceMonitor->Service memo is exact (invalidated by index generation, not this TTL), so 0 no longer costs a cross-product rebuild per request")
	resync        = flag.Duration("resync", 0, "informer resync period (0 disables periodic resync; the watch stream keeps the cache current)")
	logLevel      = flag.String("log-level", "info", "log level: debug, info, warn, error")
	logFormat     = flag.String("log-format", "text", "log format: text or json")

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

	// Self-metrics -> OTLP (the service's only OTLP producer).
	selfMetricsIntv      = flag.Duration("self-metrics-interval", time.Minute, "export the service's own metrics over OTLP at this interval (0 disables)")
	selfAttrs            = flag.Bool("self-attributes", true, "add THIS pod's Kubernetes resource attributes (namespace, pod, uid, owners, labels) to the service's own exported metrics. Resolved from the service's OWN store — its pod name is the hostname, its namespace comes from $POD_NAMESPACE or the ServiceAccount projection — so it needs no API traffic and no extra manifest wiring. Attributes already set (service.name, service.instance.id) are never overwritten; a process that is not a pod of that name simply gets none")
	otlpEndpoint         = flag.String("otlp-endpoint", "otel-collector.monitoring:4317", "OTLP endpoint for self-metrics: host:port for grpc, base URL for http")
	otlpProtocol         = flag.String("otlp-protocol", "grpc", "OTLP transport: grpc or http")
	otlpCompression      = flag.String("otlp-compression", "gzip", "OTLP payload compression: gzip or none")
	otlpCompressionLevel = flag.Int("otlp-compression-level", 0, "gzip level 1 (fastest, ~2-3x less CPU for ~10% larger payloads) to 9 (smallest); 0 = library default")
	otlpInsecure         = flag.Bool("otlp-insecure", true, "use a plaintext gRPC connection")
	otlpSkipTLS          = flag.Bool("otlp-tls-insecure-skip-verify", false, "skip TLS certificate verification towards the collector")
	otlpCAFile           = flag.String("otlp-tls-ca-file", "", "PEM CA bundle for verifying the collector")
	otlpBearer           = flag.String("otlp-bearer-token-file", "", "file with a bearer token sent on every export (re-read periodically)")
	otlpTimeout          = flag.Duration("otlp-timeout", 15*time.Second, "per-export timeout")
)

// otlpHeaders is registered in run(): flag.Var needs the value to exist first.
var otlpHeaders headerFlags

func run() error {
	flag.Var(&otlpHeaders, "otlp-header", "static key=value header sent on every self-metrics export (HTTP header / gRPC metadata, e.g. X-Scope-OrgID=tenant); repeatable")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log, err := cli.NewLogger(*logLevel, *logFormat)
	if err != nil {
		return err
	}
	slog.SetDefault(log)
	// client-go logs through klog; route it into the same slog handler.
	klog.SetSlogLogger(log)
	// First line of every run: without a build identity a panic trace, a
	// metric anomaly or a half-finished rollout cannot be tied to a commit.
	log.Info("kubescrape starting", "version", obs.BuildVersion(), "built", obs.BuildTime())

	// A negative resync is not a mode, it is a typo: client-go treats the
	// period as a deadline that is always in the past, so every informer
	// resyncs continuously — replaying UpsertPod for every pod in the cluster
	// under the store's write lock, in a loop, with no flag named anywhere in
	// the symptoms. 0 legitimately means "no periodic resync".
	if *resync < 0 {
		return fmt.Errorf("-resync %v: must not be negative (0 disables periodic resync)", *resync)
	}
	// -scrape-auth-secrets derives its allowlist from INDEXED monitors, so
	// without -servicemonitors nothing is ever indexed and every request to
	// /v1/scrape-auth 404s ("no monitors indexed") — while the deployment
	// still carries the cluster-wide `secrets: get` grant the flag requires.
	// Warned rather than refused: -servicemonitors with the CRD absent
	// legitimately leaves the index nil too, and that degradation is
	// deliberate, so a hard failure here would refuse a legal configuration.
	if *scrapeAuthOn && !*monitorsOn {
		log.Warn("-scrape-auth-secrets has no effect without -servicemonitors: " +
			"the served allowlist is derived from indexed monitors, so every request will 404 — " +
			"the cluster-wide `secrets: get` grant it requires is unused")
	}

	cfg, err := cli.KubeConfig(*kubeconfig)
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
	metaClient, err := metadata.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("creating metadata client: %w", err)
	}

	st := store.New(*cacheTTL)
	obs.RegisterStoreStats(st.Stats)
	obs.RegisterWaiterStats(st.BlockedLookups, st.ShedLookups)

	// Full objects (spec+status are both read), minus what nothing reads:
	// trimPod drops managedFields from everything and, for PODS ONLY, the bulk
	// of the spec that never reaches kubemeta.Pod. Service specs are left
	// intact — services.Index reads them.
	factory := informers.NewSharedInformerFactoryWithOptions(client, *resync,
		informers.WithTransform(trimPod))
	svcIndex := services.NewIndex()
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
			synced = append(synced, smSynced)
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
		defer func() { _ = exporter.Close() }()
	}
	// The self-metrics goroutine joins this group; run waits for its final
	// export before returning, so it finishes before the deferred
	// exporter.Close fires (mirrors the agent).
	var wg sync.WaitGroup
	// Registered AFTER exporter.Close (LIFO): an early `return err` below must
	// stop and drain the started goroutines BEFORE the exporter is closed under
	// them. The normal path's inline wg.Wait makes this a no-op there.
	defer func() {
		stop()
		wg.Wait()
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
		selfRes = pcommon.NewResource()
		a := selfRes.Attributes()
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

	ready := make(chan struct{})
	go func() {
		if !cache.WaitForCacheSync(ctx.Done(), synced...) {
			return // shutting down
		}
		pods, containers := st.Stats()
		log.Info("informer caches synced", "pods", pods, "containers", containers)
		close(ready)
	}()

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
	srv := server.New(serverCfg).HTTPServer(*listen)

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
		log.Info("listening", "addr", *listen)
		errCh <- srv.ListenAndServe()
	}()

	var runErr error
	select {
	case err := <-errCh:
		runErr = fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
		log.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			runErr = fmt.Errorf("http shutdown: %w", err)
		}
	}
	// Cancel ctx (a no-op on the signal path) and wait for the exporting
	// goroutines' final flushes before the deferred exporter.Close fires.
	stop()
	wg.Wait()
	if *selfMetricsIntv > 0 {
		// Registry.Run's own final export raced the final flushes inside
		// wg.Wait (the events drain, the last batches); counters they bumped
		// would otherwise die unexported. One more export now that all are done.
		// Bounded here, by us: ctx is cancelled by this point, and a dead
		// collector must not hold the process past its termination grace.
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), metrics.FinalExportTimeout)
		obs.Registry.FinalExport(fctx, selfOut, selfRes, log)
		cancel()
	}
	return runErr
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
	if acc, err := apimeta.Accessor(obj); err == nil {
		acc.SetManagedFields(nil)
		// SetAnnotations only when something was actually removed: the
		// unstructured accessor rebuilds the whole map on a set, and the
		// overwhelmingly common object carries neither key.
		if ann := acc.GetAnnotations(); kubemeta.StripDroppedAnnotations(ann) {
			acc.SetAnnotations(ann)
		}
	}
	return obj, nil
}

// trimPod is the pod informer's transform: strip managedFields like every
// other informer, then drop the parts of the SPEC nothing here reads.
//
// The pod cache is the service's dominant memory cost, and kubeconvert.FromPod
// consumes a thin slice of the spec — NodeName, HostNetwork, and each
// container's Name/Image/Ports. Everything else the API server sends (env
// vars, volumes and their mounts, resource requirements, the three probes,
// lifecycle hooks, affinity, tolerations, scheduling gates) is retained
// verbatim for the process lifetime and never read: on a large cluster that is
// tens of megabytes of resident heap against a 128Mi request.
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

// watchErrors installs a watch-error handler that counts before delegating to
// client-go's default (which keeps the standard logging and the
// expired-resourceVersion handling).
//
// Readiness LATCHES: /readyz gates on the initial sync and is never
// re-evaluated. So a list/watch that breaks AFTER that — revoked RBAC, a
// deleted CRD, an apiserver rejecting the watch — leaves the reflector
// retrying forever while /readyz stays 200, the store gauges freeze at
// plausible values, and every response is served from a cache that has quietly
// stopped advancing. Without this the only trace is a klog line, which is not
// alertable; the startup half of exactly this failure was already found and
// fixed once (the PodMonitor informer that 403-looped behind a green /readyz).
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
