package main

// The command-line flags, registered on the default flag set at package init.

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/cli"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

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
	selfAttrs    = flag.Bool("self-attributes", true, "add THIS pod's Kubernetes resource attributes (namespace, pod, uid, owners, labels) to the service's own exported metrics. Resolved from the service's OWN store — its pod name is the hostname, its namespace comes from $POD_NAMESPACE or the ServiceAccount projection — so it needs no API traffic and no extra manifest wiring. Attributes already set (service.name, service.instance.id) are never overwritten; a process that is not a pod of that name simply gets none")
	otlpFlags    = cli.RegisterOTLPFlags(flag.CommandLine, "OTLP endpoint for self-metrics: host:port for grpc, base URL for http")
	otlpEndpoint = otlpFlags.Endpoint
	otlpProtocol = otlpFlags.Protocol
)

// otlpHeaders is registered in init() rather than in the var block above:
// flag.Var needs the value to exist first. init() rather than run() so the
// registration is visible to reflection over flag.CommandLine — the FLAGS.md
// generator (flagsdoc_test.go) walks the flag set without calling run().
var otlpHeaders headerFlags

func init() {
	flag.Var(&otlpHeaders, "otlp-header", "static key=value header sent on every self-metrics export (HTTP header / gRPC metadata, e.g. X-Scope-OrgID=tenant); repeatable")
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
