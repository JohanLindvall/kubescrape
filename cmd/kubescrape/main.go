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
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/owners"
	"github.com/JohanLindvall/kubescrape/internal/selfmeta"
	"github.com/JohanLindvall/kubescrape/internal/server"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
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
	switch present, err := serviceMonitorCRDPresent(disco); {
	case err != nil:
		return nil, nil, fmt.Errorf("checking for the servicemonitor CRD: %w", err)
	case !present:
		log.Warn("servicemonitors requested but the CRD is unavailable; disabling")
		return nil, nil, nil
	}
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("creating dynamic client: %w", err)
	}
	dynFactory := dynamicinformer.NewDynamicSharedInformerFactory(dynClient, resync)
	smInformer := dynFactory.ForResource(servicemonitors.GVR).Informer()
	// Unstructured objects retain managedFields unless stripped, like
	// the typed informers' transform does.
	if err := smInformer.SetTransform(func(obj any) (any, error) {
		if u, ok := obj.(*unstructured.Unstructured); ok {
			unstructured.RemoveNestedField(u.Object, "metadata", "managedFields")
		}
		return obj, nil
	}); err != nil {
		return nil, nil, fmt.Errorf("servicemonitor informer transform: %w", err)
	}
	monitors := servicemonitors.NewIndex()
	smReg, err := smInformer.AddEventHandler(typedHandler(
		func(u *unstructured.Unstructured) {
			if !monitorAllowed(allowNS, u) {
				return
			}
			if err := monitors.Upsert(u); err != nil {
				// Counted, not just logged: an unparseable monitor DELETES it
				// from the index, dropping every target it contributed. That
				// is strictly more severe than the "some endpoint fields were
				// ignored" case, which does get a metric — so the severe one
				// must not be the unalertable one.
				obs.MonitorParseErrors.WithLabelValues("servicemonitor").Inc()
				log.Warn("parsing servicemonitor", "error", err,
					"namespace", u.GetNamespace(), "name", u.GetName())
				return
			}
			warnIgnored(log, "servicemonitor", u, monitors.Endpoints(u.GetNamespace(), u.GetName()))
		},
		func(u *unstructured.Unstructured) { monitors.Delete(u.GetNamespace(), u.GetName()) },
	))
	if err != nil {
		return nil, nil, fmt.Errorf("registering servicemonitor handler: %w", err)
	}
	// Readiness must cover every cache a request can read, so collect the
	// handler registrations rather than returning the ServiceMonitor's alone:
	// /v1/nodes/{node}/targets reads the PodMonitor index too, and leaving it
	// out let /readyz report 200 — advancing a rollout — while that index was
	// empty, or permanently so when podmonitors RBAC is missing and its LIST
	// 403-loops forever.
	synced := []cache.InformerSynced{smReg.HasSynced}

	// PodMonitors and Probes are optional siblings — watch whichever the
	// cluster serves. This is the same idempotent discovery GET as the
	// pre-check above, so an error here is as fatal as one there.
	served, err := monitoringResources(disco)
	if err != nil {
		return nil, nil, fmt.Errorf("listing monitoring.coreos.com resources: %w", err)
	}
	if served[servicemonitors.PodGVR.Resource] {
		pmInformer := dynFactory.ForResource(servicemonitors.PodGVR).Informer()
		pmReg, err := pmInformer.AddEventHandler(typedHandler(
			func(u *unstructured.Unstructured) {
				if !monitorAllowed(allowNS, u) {
					return
				}
				if err := monitors.UpsertPodMonitor(u); err != nil {
					obs.MonitorParseErrors.WithLabelValues("podmonitor").Inc()
					log.Warn("parsing podmonitor", "error", err,
						"namespace", u.GetNamespace(), "name", u.GetName())
					return
				}
				warnIgnored(log, "podmonitor", u, monitors.PodEndpoints(u.GetNamespace(), u.GetName()))
			},
			func(u *unstructured.Unstructured) { monitors.DeletePodMonitor(u.GetNamespace(), u.GetName()) },
		))
		if err != nil {
			return nil, nil, fmt.Errorf("registering podmonitor handler: %w", err)
		}
		synced = append(synced, pmReg.HasSynced)
		log.Info("podmonitor discovery enabled")
	}
	dynFactory.Start(ctx.Done())
	log.Info("servicemonitor discovery enabled")
	return monitors, func() bool {
		for _, s := range synced {
			if !s() {
				return false
			}
		}
		return true
	}, nil
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

func (r *k8sSecretReader) Get(ctx context.Context, namespace, name, key string) (string, error) {
	ck := namespace + "/" + name + "/" + key
	r.mu.Lock()
	if e, ok := r.cache[ck]; ok && time.Since(e.fetched) < secretCacheTTL {
		r.mu.Unlock()
		return e.value, nil
	}
	r.evictExpiredLocked()
	r.mu.Unlock()
	sec, err := r.client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	val, ok := sec.Data[key]
	if !ok {
		return "", fmt.Errorf("key %q not in secret", key)
	}
	r.mu.Lock()
	if r.cache == nil {
		r.cache = map[string]secretCacheEntry{}
	}
	r.cache[ck] = secretCacheEntry{value: string(val), fetched: time.Now()}
	r.mu.Unlock()
	return string(val), nil
}

// evictExpiredLocked drops every entry past the TTL. It runs on the miss path
// only: a hit is the hot path and an expired entry is about to be replaced
// anyway, so the cost lands where an API round-trip is already being paid. The
// map is bounded by the AuthSecretRefs allowlist at any instant, and this is
// what keeps it bounded over TIME as monitors come and go.
func (r *k8sSecretReader) evictExpiredLocked() {
	for k, e := range r.cache {
		if time.Since(e.fetched) >= secretCacheTTL {
			delete(r.cache, k)
		}
	}
}

// loadScrapeAuthToken reads the shared bearer token guarding
// /v1/scrape-auth. Every failure mode is fatal by design: -scrape-auth-secrets
// turns the service into a reader of every Secret key a monitor references, so
// "no token file", "unreadable file" and "empty file" must all stop the
// process rather than quietly leave the endpoint open to the whole cluster.
func loadScrapeAuthToken(path string) (string, error) {
	if path == "" {
		return "", errors.New("-scrape-auth-secrets requires -scrape-auth-token-file: " +
			"/v1/scrape-auth serves monitor Secret keys and must not be reachable unauthenticated")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("reading -scrape-auth-token-file: %w", err)
	}
	// Trim: a Secret-mounted token written with a trailing newline (or an
	// `echo`-created file) must still work — every client sends the trimmed
	// value in the header.
	token := strings.TrimSpace(string(b))
	if token == "" {
		return "", fmt.Errorf("-scrape-auth-token-file %q is empty", path)
	}
	return token, nil
}

// scrapeAuthReadInterval bounds how often the token file is re-read on the
// request path (Kubernetes projects rotated Secret contents into the mounted
// file); scrapeAuthGrace keeps the PREVIOUS token accepted after a rotation,
// so agents — which re-read their copy on their own per-minute cadence — never
// have to flip in lockstep with the service. Rotation is thereby a non-event:
// update the Secret, and both sides converge within the grace window with no
// restarts and no 401 storm.
const (
	scrapeAuthReadInterval = time.Minute
	scrapeAuthGrace        = 5 * time.Minute
)

// rotatingToken serves the current scrape-auth token plus, for the grace
// window after a change, the previous one. A failed or empty re-read keeps the
// last good value (a transient error during a Secret swap must not 401 the
// fleet); the INITIAL read stays fatal in run().
type rotatingToken struct {
	path string
	log  *slog.Logger

	mu        sync.Mutex
	cur, prev string
	prevUntil time.Time
	fetched   time.Time
}

// tokens returns the accepted token set, re-reading the file when stale.
func (r *rotatingToken) tokens() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if time.Since(r.fetched) >= scrapeAuthReadInterval {
		r.fetched = time.Now()
		if next, err := loadScrapeAuthToken(r.path); err != nil {
			r.log.Warn("re-reading -scrape-auth-token-file; keeping the last good token", "error", err)
		} else if next != r.cur {
			r.prev, r.prevUntil = r.cur, time.Now().Add(scrapeAuthGrace)
			r.cur = next
			r.log.Info("scrape-auth token rotated; previous token accepted for the grace window", "grace", scrapeAuthGrace)
		}
	}
	if r.prev != "" && time.Now().Before(r.prevUntil) {
		return []string{r.cur, r.prev}
	}
	return []string{r.cur}
}

// newLogger builds the process logger (mirrors the agent's).
func newLogger(level, format string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("log level %q: %w", level, err)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var handler slog.Handler
	switch format {
	case "text":
		handler = slog.NewTextHandler(os.Stderr, opts)
	case "json":
		handler = slog.NewJSONHandler(os.Stderr, opts)
	default:
		return nil, fmt.Errorf("log format %q (want text or json)", format)
	}
	return slog.New(handler), nil
}

func run() error {
	var (
		listen        = flag.String("listen", ":8080", "HTTP listen address")
		pprofListen   = flag.String("pprof-listen", "", "listen address for net/http/pprof under /debug/pprof, on its own port (empty disables); profiles expose goroutine stacks and heap contents")
		metricsListen = flag.String("metrics-listen", ":9090", "listen address for the Prometheus /metrics endpoint (Go runtime and process metrics; with -self-metrics-interval=0 also the kubescrape_* internal metrics, replacing the OTLP push with a scrape; empty disables). Separate from -listen so the API and the scrape target can be exposed independently")
		kubeconfig    = flag.String("kubeconfig", "", "path to a kubeconfig; defaults to in-cluster config, then $KUBECONFIG/~/.kube/config")
		maxWait       = flag.Duration("wait-timeout", 5*time.Second, "default and maximum time a container lookup blocks waiting for metadata to appear (shorten per request with ?wait=)")
		cacheTTL      = flag.Duration("cache-ttl", 5*time.Minute, "how long metadata of deleted pods and replaced container IDs stays resolvable")
		metaCacheTTL  = flag.Duration("metadata-cache-ttl", 10*time.Second, "max-age sent on metadata responses (Cache-Control + ETag) so agents cache lookups client-side; 0 disables")
		resync        = flag.Duration("resync", 0, "informer resync period (0 disables periodic resync; the watch stream keeps the cache current)")
		logLevel      = flag.String("log-level", "info", "log level: debug, info, warn, error")
		logFormat     = flag.String("log-format", "text", "log format: text or json")

		// ServiceMonitor CRDs (opt-in).
		monitorsOn = flag.Bool("servicemonitors", false, "serve targets for monitoring.coreos.com ServiceMonitors selecting pod-backed Services (no per-endpoint auth or relabelings)")

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
		scrapeAuthOn        = flag.Bool("scrape-auth-secrets", false, "serve ServiceMonitor/PodMonitor bearerTokenSecret values to agents on /v1/scrape-auth (requires secrets get RBAC)")
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
	var otlpHeaders headerFlags
	flag.Var(&otlpHeaders, "otlp-header", "static key=value header sent on every self-metrics export (HTTP header / gRPC metadata, e.g. X-Scope-OrgID=tenant); repeatable")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log, err := newLogger(*logLevel, *logFormat)
	if err != nil {
		return err
	}
	slog.SetDefault(log)
	// client-go logs through klog; route it into the same slog handler.
	klog.SetSlogLogger(log)
	// First line of every run: without a build identity a panic trace, a
	// metric anomaly or a half-finished rollout cannot be tied to a commit.
	log.Info("kubescrape starting", "version", obs.BuildVersion(), "built", obs.BuildTime())

	cfg, err := buildConfig(*kubeconfig)
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

	// Full pods (spec+status are needed); managedFields are dropped before
	// the objects enter the informer cache.
	factory := informers.NewSharedInformerFactoryWithOptions(client, *resync,
		informers.WithTransform(stripManagedFields))
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
		// predecessor valid for a grace window (see rotatingToken).
		token, err := loadScrapeAuthToken(*scrapeAuthTokenFile)
		if err != nil {
			return err
		}
		rt := &rotatingToken{path: *scrapeAuthTokenFile, log: log, cur: token, fetched: time.Now()}
		scrapeAuthTokens = rt.tokens
		// Detection runs on a CLOCK, not only on request traffic: lazily-only,
		// a rotation on a quiet endpoint would be noticed by the first request
		// AFTER it — anchoring the previous (revoked) token's grace window at
		// that request instead of within a minute of the file change, and
		// stretching its acceptance far past the documented 5 minutes.
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(scrapeAuthReadInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					rt.tokens()
				}
			}
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
	}
	if err := serverCfg.Validate(); err != nil {
		return err
	}
	srv := server.New(serverCfg).HTTPServer(*listen)

	// With the OTLP self-metrics push disabled (-self-metrics-interval=0) the
	// kubescrape_* metrics ride the /metrics scrape instead — the service then
	// needs no OTLP endpoint at all.
	stopMetrics := obs.ServeMetrics(ctx, *metricsListen, *selfMetricsIntv <= 0, log)
	defer stopMetrics()
	stopPprof := obs.ServePprof(ctx, *pprofListen, log)
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
		obs.Registry.FinalExport(selfOut, selfRes, log)
	}
	return runErr
}

// buildConfig prefers an explicit kubeconfig, then in-cluster config, then
// the default kubeconfig loading rules ($KUBECONFIG, ~/.kube/config).
func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig == "" {
		if cfg, err := rest.InClusterConfig(); err == nil {
			return cfg, nil
		}
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = kubeconfig
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
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
	out := map[string]bool{}
	for _, ns := range strings.Split(s, ",") {
		if ns = strings.TrimSpace(ns); ns != "" {
			out[ns] = true
		}
	}
	if len(out) == 0 {
		return nil
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
func monitorAllowed(allowNS map[string]bool, u *unstructured.Unstructured) bool {
	return allowNS == nil || allowNS[u.GetNamespace()]
}

// serviceMonitorCRDPresent reports whether the ServiceMonitor CRD is actually
// served. The group/version existing is not enough: another
// monitoring.coreos.com/v1 CRD (e.g. PrometheusRule alone) registers the group
// while servicemonitor LISTs would fail forever, wedging readiness behind an
// informer that can never sync.
//
// A false return means the cluster answered and does not serve the resource.
// An error means the cluster could not be ASKED, which is not the same thing
// and must not silently disable the feature — see the caller.
func serviceMonitorCRDPresent(d discovery.DiscoveryInterface) (bool, error) {
	served, err := monitoringResources(d)
	if err != nil {
		return false, err
	}
	return served[servicemonitors.GVR.Resource], nil
}

// monitoringResources lists which monitoring.coreos.com resources the
// cluster serves (servicemonitors, podmonitors, probes may be installed
// independently). A missing group/version is reported as an empty set and no
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

// stripManagedFields drops managedFields before objects are stored in the
// informer caches; they are large and unused here.
func stripManagedFields(obj any) (any, error) {
	if acc, err := apimeta.Accessor(obj); err == nil {
		acc.SetManagedFields(nil)
	}
	return obj, nil
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
