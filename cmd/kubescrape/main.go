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
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	corev1 "k8s.io/api/core/v1"
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
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/events"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/owners"
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
func registerCoreInformers(factory informers.SharedInformerFactory, st *store.Store, svcIndex *services.Index) ([]cache.InformerSynced, error) {
	podInformer := factory.Core().V1().Pods().Informer()
	if _, err := podInformer.AddEventHandler(typedHandler(
		func(pod *corev1.Pod) { st.UpsertPod(pod) },
		func(pod *corev1.Pod) { st.DeletePod(pod.UID) },
	)); err != nil {
		return nil, fmt.Errorf("registering pod event handler: %w", err)
	}

	// Services are matched against pods for service-annotation based scrape
	// discovery; their specs are small, so the full objects are cached.
	svcInformer := factory.Core().V1().Services().Informer()
	if _, err := svcInformer.AddEventHandler(typedHandler(
		func(svc *corev1.Service) { svcIndex.Upsert(svc) },
		func(svc *corev1.Service) { svcIndex.Delete(svc.Namespace, svc.UID) },
	)); err != nil {
		return nil, fmt.Errorf("registering service event handler: %w", err)
	}
	return []cache.InformerSynced{podInformer.HasSynced, svcInformer.HasSynced}, nil
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
func startServiceMonitors(ctx context.Context, cfg *rest.Config, disco discovery.DiscoveryInterface, resync time.Duration, log *slog.Logger) (*servicemonitors.Index, cache.InformerSynced, error) {
	if err := checkServiceMonitorCRD(disco); err != nil {
		log.Warn("servicemonitors requested but the CRD is unavailable; disabling", "error", err)
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
	if _, err := smInformer.AddEventHandler(typedHandler(
		func(u *unstructured.Unstructured) {
			if err := monitors.Upsert(u); err != nil {
				log.Warn("parsing servicemonitor", "error", err)
			}
		},
		func(u *unstructured.Unstructured) { monitors.Delete(u.GetNamespace(), u.GetName()) },
	)); err != nil {
		return nil, nil, fmt.Errorf("registering servicemonitor handler: %w", err)
	}
	// PodMonitors and Probes are optional siblings — watch whichever the
	// cluster serves.
	served, _ := monitoringResources(disco)
	if served[servicemonitors.PodGVR.Resource] {
		pmInformer := dynFactory.ForResource(servicemonitors.PodGVR).Informer()
		if _, err := pmInformer.AddEventHandler(typedHandler(
			func(u *unstructured.Unstructured) {
				if err := monitors.UpsertPodMonitor(u); err != nil {
					log.Warn("parsing podmonitor", "error", err)
				}
			},
			func(u *unstructured.Unstructured) { monitors.DeletePodMonitor(u.GetNamespace(), u.GetName()) },
		)); err != nil {
			return nil, nil, fmt.Errorf("registering podmonitor handler: %w", err)
		}
		log.Info("podmonitor discovery enabled")
	}
	if served[servicemonitors.ProbeGVR.Resource] {
		prInformer := dynFactory.ForResource(servicemonitors.ProbeGVR).Informer()
		if _, err := prInformer.AddEventHandler(typedHandler(
			func(u *unstructured.Unstructured) {
				if err := monitors.UpsertProbe(u); err != nil {
					log.Warn("parsing probe", "error", err)
				}
			},
			func(u *unstructured.Unstructured) { monitors.DeleteProbe(u.GetNamespace(), u.GetName()) },
		)); err != nil {
			return nil, nil, fmt.Errorf("registering probe handler: %w", err)
		}
		log.Info("probe discovery enabled")
	}
	dynFactory.Start(ctx.Done())
	log.Info("servicemonitor discovery enabled")
	return monitors, smInformer.HasSynced, nil
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

func (r *k8sSecretReader) Get(ctx context.Context, namespace, name, key string) (string, error) {
	ck := namespace + "/" + name + "/" + key
	r.mu.Lock()
	if e, ok := r.cache[ck]; ok && time.Since(e.fetched) < time.Minute {
		r.mu.Unlock()
		return e.value, nil
	}
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
		listen       = flag.String("listen", ":8080", "HTTP listen address")
		kubeconfig   = flag.String("kubeconfig", "", "path to a kubeconfig; defaults to in-cluster config, then $KUBECONFIG/~/.kube/config")
		maxWait      = flag.Duration("wait-timeout", 5*time.Second, "default and maximum time a container lookup blocks waiting for metadata to appear (shorten per request with ?wait=)")
		cacheTTL     = flag.Duration("cache-ttl", 5*time.Minute, "how long metadata of deleted pods and replaced container IDs stays resolvable")
		metaCacheTTL = flag.Duration("metadata-cache-ttl", 10*time.Second, "max-age sent on metadata responses (Cache-Control + ETag) so agents cache lookups client-side; 0 disables")
		resync       = flag.Duration("resync", 0, "informer resync period (0 disables periodic resync; the watch stream keeps the cache current)")
		logLevel     = flag.String("log-level", "info", "log level: debug, info, warn, error")
		logFormat    = flag.String("log-format", "text", "log format: text or json")

		// ServiceMonitor CRDs (opt-in).
		monitorsOn = flag.Bool("servicemonitors", false, "serve targets for monitoring.coreos.com ServiceMonitors selecting pod-backed Services (no per-endpoint auth or relabelings)")

		// HA: with >1 replica, gate the events exporter behind a Lease so
		// exactly one replica exports (reads are served by every replica from
		// its own informer caches — no election needed there).
		leaderElect = flag.Bool("leader-elect", false, "use a Lease to elect one replica as the events exporter (required when running >1 replica with -events)")
		leaseNs     = flag.String("leader-elect-namespace", "monitoring", "namespace of the leader-election Lease")
		leaseName   = flag.String("leader-elect-name", "kubescrape-events", "name of the leader-election Lease")

		// Serve monitor endpoints' bearerTokenSecret values to agents (opt-in:
		// needs secrets get RBAC; tokens travel the cluster-internal HTTP).
		scrapeAuthOn = flag.Bool("scrape-auth-secrets", false, "serve ServiceMonitor/PodMonitor bearerTokenSecret values to agents on /v1/scrape-auth (requires secrets get RBAC)")

		// Kubernetes events -> OTLP logs (opt-in).
		eventsOn             = flag.Bool("events", false, "export Kubernetes events as OTLP log records")
		selfMetricsIntv      = flag.Duration("self-metrics-interval", time.Minute, "export the service's own metrics over OTLP at this interval (0 disables)")
		otlpEndpoint         = flag.String("otlp-endpoint", "otel-collector.monitoring:4317", "OTLP endpoint for the events exporter: host:port for grpc, base URL for http")
		otlpProtocol         = flag.String("otlp-protocol", "grpc", "OTLP transport: grpc or http")
		otlpCompression      = flag.String("otlp-compression", "gzip", "OTLP payload compression: gzip or none")
		otlpCompressionLevel = flag.Int("otlp-compression-level", 0, "gzip level 1 (fastest, ~2-3x less CPU for ~10% larger payloads) to 9 (smallest); 0 = library default")
		otlpInsecure         = flag.Bool("otlp-insecure", true, "use a plaintext gRPC connection")
		otlpSkipTLS          = flag.Bool("otlp-tls-insecure-skip-verify", false, "skip TLS certificate verification towards the collector")
		otlpCAFile           = flag.String("otlp-tls-ca-file", "", "PEM CA bundle for verifying the collector")
		otlpBearer           = flag.String("otlp-bearer-token-file", "", "file with a bearer token sent on every export (re-read periodically)")
		otlpTimeout          = flag.Duration("otlp-timeout", 15*time.Second, "per-export timeout")
	)
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
		idx, smSynced, err := startServiceMonitors(ctx, cfg, client.Discovery(), *resync, log)
		if err != nil {
			return err
		}
		if idx != nil {
			monitors = idx
			synced = append(synced, smSynced)
		}
	}

	var exporter *otlpexport.Client
	if *eventsOn || *selfMetricsIntv > 0 {
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
		})
		if err != nil {
			return fmt.Errorf("creating OTLP exporter: %w", err)
		}
		defer func() { _ = exporter.Close() }()
	}
	// Exporting goroutines join this group; run waits for them (the events
	// final flush, the self-metrics final export) before returning, so they
	// finish before the deferred exporter.Close fires (mirrors the agent).
	var wg sync.WaitGroup
	// Registered AFTER exporter.Close (LIFO): an early `return err` below must
	// stop and drain the started goroutines BEFORE the exporter is closed under
	// them. The normal path's inline wg.Wait makes this a no-op there.
	defer func() {
		stop()
		wg.Wait()
	}()
	var selfRes pcommon.Resource
	if *selfMetricsIntv > 0 {
		selfRes = pcommon.NewResource()
		a := selfRes.Attributes()
		a.PutStr("service.name", "kubescrape")
		if host, err := os.Hostname(); err == nil {
			a.PutStr("service.instance.id", host)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			obs.Registry.Run(ctx, exporter, *selfMetricsIntv, selfRes, log)
		}()
		log.Info("self-metrics export started", "interval", *selfMetricsIntv)
	}

	if *eventsOn {
		ev := events.New(events.Config{Store: st, Exporter: exporter, Owners: resolver, Logger: log})
		if *leaderElect {
			// Only the leader exports; every replica still watches (cheap) so
			// failover needs no informer warmup. Losing the lease deactivates
			// immediately; the successor picks up the live stream (no replay,
			// mirroring the skip-initial-history startup semantics).
			ev.SetActive(false)
			id, _ := os.Hostname()
			lock := &resourcelock.LeaseLock{
				LeaseMeta:  metav1.ObjectMeta{Name: *leaseName, Namespace: *leaseNs},
				Client:     client.CoordinationV1(),
				LockConfig: resourcelock.ResourceLockConfig{Identity: id},
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
					Lock:            lock,
					LeaseDuration:   15 * time.Second,
					RenewDeadline:   10 * time.Second,
					RetryPeriod:     2 * time.Second,
					ReleaseOnCancel: true,
					Callbacks: leaderelection.LeaderCallbacks{
						OnStartedLeading: func(context.Context) { ev.SetActive(true) },
						OnStoppedLeading: func() { ev.SetActive(false) },
					},
				})
			}()
			log.Info("leader election enabled for events", "lease", *leaseNs+"/"+*leaseName, "id", id)
		}
		evInformer := factory.Core().V1().Events().Informer()
		if _, err := evInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    ev.OnAdd,
			UpdateFunc: ev.OnUpdate,
		}); err != nil {
			return fmt.Errorf("registering event handler: %w", err)
		}
		synced = append(synced, evInformer.HasSynced)
		wg.Add(1)
		go func() {
			defer wg.Done()
			ev.Run(ctx)
		}()
		log.Info("kubernetes events exporter started", "endpoint", *otlpEndpoint)
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
	if *scrapeAuthOn {
		secretReader = &k8sSecretReader{client: client}
		log.Info("scrape auth secrets enabled")
	}
	srv := server.New(server.Config{
		Store:    st,
		Services: svcIndex,
		Monitors: monitors,
		Resolver: resolver,
		MaxWait:  *maxWait,
		CacheTTL: *metaCacheTTL,
		Ready:    ready,
		Logger:   log,
		Secrets:  secretReader,
	}).HTTPServer(*listen)

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
		obs.Registry.FinalExport(exporter, selfRes, log)
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

// checkServiceMonitorCRD verifies the ServiceMonitor CRD is actually served.
// The group/version existing is not enough: another monitoring.coreos.com/v1
// CRD (e.g. PrometheusRule alone) registers the group while servicemonitor
// LISTs would fail forever, wedging readiness behind an informer that can
// never sync.
func checkServiceMonitorCRD(d discovery.DiscoveryInterface) error {
	_, err := monitoringResources(d)
	return err
}

// monitoringResources lists which monitoring.coreos.com resources the
// cluster serves (servicemonitors, podmonitors, probes may be installed
// independently).
func monitoringResources(d discovery.DiscoveryInterface) (map[string]bool, error) {
	list, err := d.ServerResourcesForGroupVersion(servicemonitors.GVR.GroupVersion().String())
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, r := range list.APIResources {
		out[r.Name] = true
	}
	if !out[servicemonitors.GVR.Resource] {
		return nil, fmt.Errorf("resource %q not served by %s", servicemonitors.GVR.Resource, servicemonitors.GVR.GroupVersion())
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
