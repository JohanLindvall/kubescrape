package main

// The metadata service's startup visibility: the effective-configuration dump
// an operator greps on a first live run, and the readiness watchdog that says
// WHICH cache a stuck /readyz is waiting for.

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"k8s.io/client-go/tools/cache"

	"github.com/JohanLindvall/kubescrape/internal/cli"
	"github.com/JohanLindvall/kubescrape/internal/selfmeta"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// serviceName is the service.name this process's own metrics carry
// (serviceSelfResource sets it); named here so the summary cannot drift.
const serviceName = "kubescrape"

// logStartupSummary is the EFFECTIVE CONFIGURATION dump: what this process will
// serve, what it will talk to, what it binds, who it thinks it is, and the
// knobs most likely to be wrong. It is the agent's printConfigSummary in the
// shape this binary has — there is no -config file, so the whole surface is
// flags.
//
// Emitted by a real start AND by -check-config (validate.go), which is the same
// discipline validateConfig holds for the refusals: a dry run and a rollout
// must not describe different processes. The one line that legitimately differs
// is the API server, which the dry run may not be able to resolve from the
// machine it runs on — it is passed in for exactly that reason.
//
// A few lines rather than one: an operator greps a message ("effective
// destinations") and reads the pairs under it. No line carries a credential —
// only the PATHS credentials are read from (internal/cli's "never log a
// secret"), which is why the token flags appear as `*File` keys.
func logStartupSummary(log *slog.Logger, apiServer string) {
	log.Info("effective configuration",
		"servicemonitors", *monitorsOn,
		"monitorNamespaces", strings.Join(parseNamespaceList(*monitorNamespaces), ","),
		"scrapeAuthSecrets", *scrapeAuthOn,
		"scrapeAuthTokenFile", *scrapeAuthTokenFile,
		"selfAttributes", *selfAttrs,
		"logLevel", *logLevel,
	)
	// What this process will TALK to. The API server is the one destination it
	// cannot work without and the one nobody passes explicitly — it comes from
	// the in-cluster environment or a kubeconfig — so a summary that omitted it
	// would leave the commonest "why is nothing in the store?" unanswerable
	// from the log.
	dest := []any{
		"apiServer", apiServer,
		"kubeconfig", *kubeconfig,
	}
	if *selfMetricsIntv > 0 {
		dest = append(dest,
			"otlpEndpoint", *otlpEndpoint,
			"otlpProtocol", *otlpProtocol,
			"otlpInsecure", *otlpInsecure,
			"otlpCAFile", *otlpCAFile,
			"otlpBearerTokenFile", *otlpBearer,
		)
	}
	log.Info("effective destinations", dest...)
	// An address already in use names itself at startup; a listener left EMPTY
	// is never bound and says nothing at all, which is how a deployment ends up
	// with no /metrics and nobody wondering why.
	log.Info("effective listeners",
		"listen", *listen,
		"metricsListen", *metricsListen,
		"pprofListen", *pprofListen,
	)
	// Who this process is on the series it produces about itself. The hostname
	// is also how it finds its OWN pod for -self-attributes, so a mismatch here
	// explains an unresolved self-metadata gauge.
	host, _ := os.Hostname()
	log.Info("effective identity",
		"namespace", selfmeta.Namespace(),
		"serviceName", serviceName,
		"instance", host,
		"selfMetricsInterval", *selfMetricsIntv,
	)
	// The knobs whose wrong value is expensive and quiet. -wait-timeout and
	// -max-blocked-lookups together decide how much of the fleet can be parked
	// in this process at once; the two TTLs decide how stale an answer can be.
	log.Info("effective limits",
		"waitTimeout", *maxWait,
		"maxBlockedLookups", *maxWaiters,
		"cacheTTL", *cacheTTL,
		"metadataCacheTTL", *metaCacheTTL,
		"resync", *resync,
		"apiserverProbeInterval", *apiserverProbeInterval,
	)
}

// syncGate is one informer cache readiness waits for, carrying the NAME that a
// stuck rollout needs. client-go's WaitForCacheSync takes bare funcs and can
// therefore only ever report "not synced" — which, for the failure this is
// about (a missing RBAC rule 403-looping one resource behind a green-looking
// rollout), is the half of the message that does not matter.
type syncGate struct {
	name   string
	synced cache.InformerSynced
}

// How long the caches may take before it is worth a line, and how often to
// repeat it afterwards. The grace covers a genuinely cold cluster's initial
// LIST; anything past it is a resource that is not going to sync on its own.
// Vars, not consts, so a test can drive the warn without sleeping through the
// grace — the same reason the store's clock is injectable.
var (
	cacheSyncGrace  = 30 * time.Second
	cacheSyncReWarn = 2 * time.Minute
)

// waitForCaches closes ready once every gate has synced, and — the part
// client-go cannot do — says which gates are still pending while it waits.
//
// /readyz LATCHES on this channel, so a gate that never syncs is a 503 for the
// process lifetime: the Deployment never becomes ready, the Service has no
// endpoints, and every agent in the fleet blocks on a lookup that never
// resolves. Before this, the only trace was a klog line from the reflector.
func waitForCaches(ctx context.Context, gates []syncGate, st *store.Store, log *slog.Logger, ready chan<- struct{}) {
	start := time.Now()
	// A steady poll, never a backoff: readiness must be announced as soon as the
	// caches sync (this is what gates the Service endpoints), and the check is
	// two bool reads — the same 100ms cadence client-go's own WaitForCacheSync
	// uses. Only the WARNING is throttled.
	wait := min(100*time.Millisecond, cacheSyncGrace)
	var lastWarn time.Time
	t := time.NewTimer(wait)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			if pending := pendingGates(gates); len(pending) > 0 {
				log.Warn("shutting down before the informer caches synced",
					"caches", strings.Join(pending, ","), "waited", time.Since(start).Round(time.Second))
			}
			return
		case <-t.C:
		}
		pending := pendingGates(gates)
		if len(pending) == 0 {
			pods, containers := st.Stats()
			log.Info("informer caches synced", "pods", pods, "containers", containers,
				"waited", time.Since(start).Round(time.Millisecond))
			close(ready)
			return
		}
		if elapsed := time.Since(start); elapsed >= cacheSyncGrace &&
			(lastWarn.IsZero() || time.Since(lastWarn) >= cacheSyncReWarn) {
			log.Warn("not ready: informer caches have not synced, so /readyz is 503 and this replica has no Service endpoints",
				"caches", strings.Join(pending, ","), "waited", elapsed.Round(time.Second),
				"note", "a cache that never syncs is usually a missing RBAC rule for that resource; the reflector retries it forever")
			lastWarn = time.Now()
		}
		t.Reset(wait)
	}
}

// pendingGates is the unsynced gates, sorted by registration order (which is
// the order they were wired, so the list reads like the startup sequence).
func pendingGates(gates []syncGate) []string {
	var out []string
	for _, g := range gates {
		if !g.synced() {
			out = append(out, g.name)
		}
	}
	return out
}

// gateStates adapts the gates to obs.RegisterReadiness: one gauge per cache,
// 1 once it has synced. The metric exists because the probe body only reaches
// whoever can curl the pod, and an unready replica is one nothing routes to.
func gateStates(gates []syncGate) func() map[string]bool {
	return func() map[string]bool {
		out := make(map[string]bool, len(gates))
		for _, g := range gates {
			out[g.name] = g.synced()
		}
		return out
	}
}

// parseNamespaceList is parseNamespaceSet's ordered twin, for the summary: a
// map iterates randomly, and a line whose value reorders between restarts is a
// diff nobody can read.
func parseNamespaceList(s string) []string {
	nss := cli.SplitList(s)
	if len(nss) == 0 {
		return []string{"(all)"}
	}
	return nss
}
