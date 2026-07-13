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
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
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

		// Kubernetes events -> OTLP logs (opt-in).
		eventsOn        = flag.Bool("events", false, "export Kubernetes events as OTLP log records")
		selfMetricsIntv = flag.Duration("self-metrics-interval", time.Minute, "export the service's own metrics over OTLP at this interval (0 disables)")
		otlpEndpoint    = flag.String("otlp-endpoint", "otel-collector.monitoring:4317", "OTLP endpoint for the events exporter: host:port for grpc, base URL for http")
		otlpProtocol    = flag.String("otlp-protocol", "grpc", "OTLP transport: grpc or http")
		otlpCompression = flag.String("otlp-compression", "gzip", "OTLP payload compression: gzip or none")
		otlpInsecure    = flag.Bool("otlp-insecure", true, "use a plaintext gRPC connection")
		otlpSkipTLS     = flag.Bool("otlp-tls-insecure-skip-verify", false, "skip TLS certificate verification towards the collector")
		otlpCAFile      = flag.String("otlp-tls-ca-file", "", "PEM CA bundle for verifying the collector")
		otlpBearer      = flag.String("otlp-bearer-token-file", "", "file with a bearer token sent on every export (re-read periodically)")
		otlpTimeout     = flag.Duration("otlp-timeout", 15*time.Second, "per-export timeout")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("log level %q: %w", *logLevel, err)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var handler slog.Handler
	switch *logFormat {
	case "text":
		handler = slog.NewTextHandler(os.Stderr, opts)
	case "json":
		handler = slog.NewJSONHandler(os.Stderr, opts)
	default:
		return fmt.Errorf("log format %q (want text or json)", *logFormat)
	}
	log := slog.New(handler)
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
	podInformer := factory.Core().V1().Pods().Informer()
	if _, err := podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if pod, ok := obj.(*corev1.Pod); ok {
				st.UpsertPod(pod)
			}
		},
		UpdateFunc: func(_, obj any) {
			if pod, ok := obj.(*corev1.Pod); ok {
				st.UpsertPod(pod)
			}
		},
		DeleteFunc: func(obj any) {
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			if pod, ok := obj.(*corev1.Pod); ok {
				st.DeletePod(pod.UID)
			}
		},
	}); err != nil {
		return fmt.Errorf("registering pod event handler: %w", err)
	}

	// Services are matched against pods for service-annotation based scrape
	// discovery; their specs are small, so the full objects are cached.
	svcIndex := services.NewIndex()
	svcInformer := factory.Core().V1().Services().Informer()
	if _, err := svcInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if svc, ok := obj.(*corev1.Service); ok {
				svcIndex.Upsert(svc)
			}
		},
		UpdateFunc: func(_, obj any) {
			if svc, ok := obj.(*corev1.Service); ok {
				svcIndex.Upsert(svc)
			}
		},
		DeleteFunc: func(obj any) {
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			if svc, ok := obj.(*corev1.Service); ok {
				svcIndex.Delete(svc.Namespace, svc.UID)
			}
		},
	}); err != nil {
		return fmt.Errorf("registering service event handler: %w", err)
	}

	// Metadata-only informers (PartialObjectMetadata) for owner-chain and
	// namespace enrichment: labels/annotations/ownerRefs only, no specs
	// cached.
	metaFactory := metadatainformer.NewSharedInformerFactory(metaClient, *resync)
	listers := make(map[schema.GroupVersionResource]cache.GenericLister, len(owners.AllGVRs))
	synced := []cache.InformerSynced{podInformer.HasSynced, svcInformer.HasSynced}
	for _, gvr := range owners.AllGVRs {
		inf := metaFactory.ForResource(gvr)
		if err := inf.Informer().SetTransform(stripManagedFields); err != nil {
			return fmt.Errorf("setting %s informer transform: %w", gvr.Resource, err)
		}
		listers[gvr] = inf.Lister()
		synced = append(synced, inf.Informer().HasSynced)
	}
	resolver := owners.NewFromListers(listers)

	var monitors *servicemonitors.Index
	if *monitorsOn {
		if err := checkServiceMonitorCRD(client.Discovery()); err != nil {
			log.Warn("servicemonitors requested but the CRD is unavailable; disabling", "error", err)
		} else {
			dynClient, err := dynamic.NewForConfig(cfg)
			if err != nil {
				return fmt.Errorf("creating dynamic client: %w", err)
			}
			dynFactory := dynamicinformer.NewDynamicSharedInformerFactory(dynClient, *resync)
			smInformer := dynFactory.ForResource(servicemonitors.GVR).Informer()
			// Unstructured objects retain managedFields unless stripped, like
			// the typed informers' transform does.
			if err := smInformer.SetTransform(func(obj any) (any, error) {
				if u, ok := obj.(*unstructured.Unstructured); ok {
					unstructured.RemoveNestedField(u.Object, "metadata", "managedFields")
				}
				return obj, nil
			}); err != nil {
				return fmt.Errorf("servicemonitor informer transform: %w", err)
			}
			monitors = servicemonitors.NewIndex()
			upsert := func(obj any) {
				if u, ok := obj.(*unstructured.Unstructured); ok {
					if err := monitors.Upsert(u); err != nil {
						log.Warn("parsing servicemonitor", "error", err)
					}
				}
			}
			if _, err := smInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
				AddFunc:    upsert,
				UpdateFunc: func(_, obj any) { upsert(obj) },
				DeleteFunc: func(obj any) {
					if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
						obj = tombstone.Obj
					}
					if u, ok := obj.(*unstructured.Unstructured); ok {
						monitors.Delete(u.GetNamespace(), u.GetName())
					}
				},
			}); err != nil {
				return fmt.Errorf("registering servicemonitor handler: %w", err)
			}
			dynFactory.Start(ctx.Done())
			synced = append(synced, smInformer.HasSynced)
			log.Info("servicemonitor discovery enabled")
		}
	}

	var exporter *otlpexport.Client
	if *eventsOn || *selfMetricsIntv > 0 {
		var err error
		exporter, err = otlpexport.New(otlpexport.Config{
			Endpoint:           *otlpEndpoint,
			Protocol:           *otlpProtocol,
			Compression:        *otlpCompression,
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
	if *selfMetricsIntv > 0 {
		res := pcommon.NewResource()
		a := res.Attributes()
		a.PutStr("service.name", "kubescrape")
		if host, err := os.Hostname(); err == nil {
			a.PutStr("service.instance.id", host)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			obs.Registry.Run(ctx, exporter, *selfMetricsIntv, res, log)
		}()
		log.Info("self-metrics export started", "interval", *selfMetricsIntv)
	}

	if *eventsOn {
		ev := events.New(events.Config{Store: st, Exporter: exporter, Logger: log})
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

	srv := &http.Server{
		Addr: *listen,
		Handler: server.New(server.Config{
			Store:    st,
			Services: svcIndex,
			Monitors: monitors,
			Resolver: resolver,
			MaxWait:  *maxWait,
			CacheTTL: *metaCacheTTL,
			Ready:    ready,
			Logger:   log,
		}).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

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
	list, err := d.ServerResourcesForGroupVersion(servicemonitors.GVR.GroupVersion().String())
	if err != nil {
		return err
	}
	for _, r := range list.APIResources {
		if r.Name == servicemonitors.GVR.Resource {
			return nil
		}
	}
	return fmt.Errorf("resource %q not served by %s", servicemonitors.GVR.Resource, servicemonitors.GVR.GroupVersion())
}

// stripManagedFields drops managedFields before objects are stored in the
// informer caches; they are large and unused here.
func stripManagedFields(obj any) (any, error) {
	if acc, err := apimeta.Accessor(obj); err == nil {
		acc.SetManagedFields(nil)
	}
	return obj, nil
}
