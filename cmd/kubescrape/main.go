// Command kubescrape serves Kubernetes pod and container metadata over HTTP.
//
// It builds an in-memory view of all pods via a single LIST followed by a
// WATCH (shared informers), plus a Service informer (service-annotation scrape
// discovery) and metadata-only informers for every owners.AllGVRs resource —
// the owner kinds and Namespaces and Nodes — so pod owner chains (Deployment,
// CronJob, ...) and namespace/node metadata resolve without caching full
// objects. With -servicemonitors it also watches ServiceMonitors and
// PodMonitors through dynamic informers.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/metadata/metadatainformer"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/cli"
	"github.com/JohanLindvall/kubescrape/internal/cli/kubecfg"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/owners"
	"github.com/JohanLindvall/kubescrape/internal/selfmeta"
	"github.com/JohanLindvall/kubescrape/internal/server"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

func main() {
	// The process logger cannot exist until -log-level is parsed, and a refusal
	// can precede it (an unknown level). Without this that one line — the one
	// that says why the pod will not start — went out through slog's stdlib
	// default, which is not logfmt. Replaced by the leveled logger a few
	// statements into run(). A flag-PARSE error never reaches it: flag.Parse
	// prints its own message and usage and exits 2 itself.
	slog.SetDefault(slog.New(cli.NewLogfmtHandler(os.Stderr, slog.LevelInfo)))
	if err := run(); err != nil {
		slog.Error("kubescrape failed", "error", err)
		os.Exit(1)
	}
}

// selfExportConfig is the self-metrics exporter's configuration, assembled
// from the -otlp-* flags in ONE place — the agent's baseExportConfig, for this
// binary's surface. Both run() (which builds the exporter from it) and
// validateConfig (which judges it without building anything) call it, so a
// -check-config cannot accept a transport a real start then refuses with
// "creating OTLP exporter": the dry run returns before run() reaches the
// constructor, and a check that assembled its own copy of the Config could
// drift from the one the constructor receives.
func selfExportConfig() otlpexport.Config {
	cfg := otlpexport.ConfigFromFlags(otlpFlags)
	cfg.Headers = otlpHeaders.m
	return cfg
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
	a.PutStr("service.name", serviceName)
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
// applied. A function rather than an inline call in run() so the wiring is
// testable: the cap only exists as a knob if the flag reaches the shed decision,
// and before -max-blocked-lookups the cap had no production caller at all.
func newMetadataStore(ttl time.Duration, maxWaiters int) *store.Store {
	return store.New(ttl, store.WithMaxWaiters(maxWaiters))
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

	// stop cancels ctx WITHOUT releasing the signal handler: the normal path
	// calls it to join the exporting goroutines before the final export, and a
	// SIGTERM landing after that must still be absorbed, not act on Go's
	// default (cli.ShutdownContext).
	ctx, stop, releaseSignals := cli.ShutdownContext()
	defer releaseSignals() // registered FIRST, so it runs LAST
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
	// The informers' last-event clock, registered before any of them is wired
	// so every slot the wiring adds is in the export from its first event.
	obs.RegisterInformerFreshness(freshness.snapshot)
	synced, err := registerCoreInformers(factory, st, svcIndex)
	if err != nil {
		return err
	}

	// Metadata-only informers (PartialObjectMetadata) for owner-chain and
	// namespace enrichment: labels/annotations/ownerRefs only, no specs
	// cached.
	metaFactory := metadatainformer.NewSharedInformerFactory(metaClient, *resync)
	ownerChanges := &owners.Changes{}
	listers, ownerSynced, err := registerOwnerInformers(metaFactory, ownerChanges)
	if err != nil {
		return err
	}
	synced = append(synced, ownerSynced...)
	resolver := owners.NewFromListers(listers)

	var monitors *servicemonitors.Index
	if *monitorsOn {
		dynClient, err := dynamic.NewForConfig(cfg)
		if err != nil {
			return fmt.Errorf("creating dynamic client: %w", err)
		}
		idx, smSynced, err := startServiceMonitors(ctx, dynClient, client.Discovery(), *resync, parseNamespaceSet(*monitorNamespaces), log)
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
		exporter, err = otlpexport.New(selfExportConfig())
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
	// export — 30s before the deferred stopPprof/stopMetrics spend
	// obs.ListenerShutdownTimeout (5s) each.
	// This Deployment sets no terminationGracePeriodSeconds (unlike the agent's
	// manifests, which set 60), so it runs on Kubernetes' 30s DEFAULT and was
	// SIGKILLed mid-sequence against an unreachable collector, losing the very
	// exports the budgets exist to fit inside it. Sharing a deadline means a
	// slow step spends what the later ones would have had, and nothing overruns.
	var shutdownBy, shutdownStart time.Time
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
	// them. The normal path's inline join sets joined, which makes this a TRUE
	// no-op there (joinOnEarlyReturn says why "it would find nothing to join"
	// was not enough) — and it is bounded by the same deadline, or a join that
	// gave up on the signal path would simply block here instead, spending the
	// budget twice.
	joined := false
	defer func() { joinOnEarlyReturn(joined, stop, &wg, stepBudget) }()
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
			// downward API. The first lookup waits for the POD cache to sync:
			// the informers only start below, so a lookup made now misses the
			// empty store by construction, and did — a Warn, an error-outcome
			// lookup and a recovery line on every clean start. The pod gate
			// alone, not readiness as a whole: the lookup reads nothing else
			// that must be complete (an owner or namespace cache still filling
			// costs one refresh of partial chain, not a miss), and a monitor
			// informer stuck on RBAC must not hold the attributes back. The
			// slow refresh past that picks up relabelling.
			selfPod := selfmeta.StartPodAfter(ctx, selfResolver(st, resolver), selfmeta.DefaultRefresh,
				gatesSynced(ctx, synced, podGate), log)
			obs.RegisterSelfMetadata(func() bool { return selfPod() != nil })
			selfOut = selfmeta.Wrap(selfOut, selfPod, selfBuild)
		}
		wg.Go(func() {
			obs.Registry.Run(ctx, selfOut, *selfMetricsIntv, selfRes, log)
		})
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
		wg.Go(func() {
			rt.Run(ctx)
		})
		secretReader = &k8sSecretReader{client: client}
		log.Info("scrape auth secrets enabled", "tokenFile", *scrapeAuthTokenFile)
	}
	serverCfg := server.Config{
		Store:            st,
		Services:         svcIndex,
		Monitors:         monitors,
		Resolver:         resolver,
		OwnerGeneration:  ownerChanges.Generation,
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
	// UNCONDITIONAL, because the release must not be tied to one of the two
	// arms below. A container lookup parks with no deadline but its own
	// -wait-timeout and nothing else can wake it: the request contexts are not
	// derived from ctx (no BaseContext), so stop() cannot reach a parked
	// handler, and the process EXITING is what cuts it — "Empty reply from
	// server", no status, nothing counted. The ctx.Done arm below drains
	// through shutdownHTTP, which is the ordering that matters (before
	// srv.Shutdown); this defer is the backstop for the ListenAndServe-error
	// arm, which drains nothing at all. Draining twice is a no-op — the second
	// call finds both parking spots empty — so the two do not conflict.
	defer api.Drain()

	var runErr error
	select {
	case err := <-errCh:
		runErr = fmt.Errorf("http server: %w", err)
		// Here rather than only in the deferred backstop: the deferred one runs
		// after the remaining shutdown steps have spent their budgets, and a
		// parked lookup wants its 503 now. The listener is gone, so nothing
		// will ever answer one.
		if n := api.Drain(); n > 0 {
			log.Info("released blocked container lookups so they can be answered", "lookups", n)
		}
	case <-ctx.Done():
		log.Info("shutting down")
		shutdownStart = time.Now()
		shutdownBy = shutdownStart.Add(shutdownTotal)
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
	if !cli.WaitFor(&wg, stepBudget()) {
		log.Warn("exporting goroutines did not stop within the shutdown budget; continuing with the final export")
	}
	// Joined, whichever way the wait above ended: a goroutine that outlived it
	// will not be caught by a second wait on a budget that is now smaller.
	joined = true
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
	if !shutdownStart.IsZero() {
		// The one line that says how the shutdown FIT: an operator sizing
		// terminationGracePeriodSeconds, or reading a pod that was SIGKILLed,
		// needs the elapsed time against the budget, and the deadline warning
		// above fires only once it has already been blown.
		log.Info("shutdown complete", "elapsed", time.Since(shutdownStart).Round(time.Millisecond),
			"budget", shutdownTotal, "deadlineExceeded", deadlineWarned)
	}
	return runErr
}
