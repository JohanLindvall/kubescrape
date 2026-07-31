// Command kubescrape-agent runs on every node (DaemonSet). It tails
// containerd container logs and scrapes the node's Prometheus targets
// (discovered through the kubescrape metadata service), exporting both as
// OTLP over gRPC to an OpenTelemetry collector, enriched with Kubernetes
// resource attributes from the metadata service.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/azurediag"
	"github.com/JohanLindvall/kubescrape/internal/agent/events"
	"github.com/JohanLindvall/kubescrape/internal/agent/journald"
	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpingest"
	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"github.com/JohanLindvall/kubescrape/internal/agent/promscrape"
	"github.com/JohanLindvall/kubescrape/internal/agent/route"
	"github.com/JohanLindvall/kubescrape/internal/agent/spanmetrics"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailer"
	"github.com/JohanLindvall/kubescrape/internal/agent/tracesample"
	"github.com/JohanLindvall/kubescrape/internal/agent/transform"
	"github.com/JohanLindvall/kubescrape/internal/leader"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/selfmeta"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	if err := run(); err != nil {
		slog.Error("kubescrape-agent failed", "error", err)
		os.Exit(1)
	}
}

// agentSelfResource is the agent's own OTLP resource identity, shared by its
// self-metrics and span-metrics exporters (a described service is carried as a
// data-point dimension, not on this resource).
func agentSelfResource(node string) pcommon.Resource {
	res := pcommon.NewResource()
	a := res.Attributes()
	a.PutStr("service.name", "kubescrape-agent")
	a.PutStr("service.version", obs.BuildVersion())
	a.PutStr("k8s.node.name", node)
	// The namespace is known WITHOUT any lookup ($POD_NAMESPACE or the
	// ServiceAccount projection), and it has to be set here rather than left to
	// the self-metadata stamp: attrs.Identity derives service.namespace from
	// it, and that is half the Prometheus job. Learning it later — when
	// /v1/self first answers, if it ever does — would rename the job of
	// already-running CUMULATIVE series mid-flight, and leave it renamed on
	// some nodes and not others.
	if ns := selfmeta.Namespace(); ns != "" {
		a.PutStr("k8s.namespace.name", ns)
	}
	// A cluster-singleton (-events / -azure-diagnostics) runs this same binary
	// under this same service.name, and would otherwise take the identity of
	// the node it happens to sit on — the same (job, instance) as that node's
	// DaemonSet agent. Two processes on one series: their counters interleave,
	// and now that each stamps its own pod, their target_info flaps between two
	// identities. A singleton's instance is its POD.
	if *eventsOn || *azureOn {
		a.PutStr("service.instance.id", selfPodName())
	}
	attrs.Identity(res)
	return res
}

// The agent's flag surface. Package-level so the per-pipeline start
// functions can read them directly; main parses.
var (
	configFile           = flag.String("config", "", "unified YAML config file with resourceAttributes, logs, logAttributes, logMetrics, logScrubbing, metrics, routing, traceMetrics, traceSampling and export sections")
	nodeName             = flag.String("node-name", os.Getenv("NODE_NAME"), "name of the node this agent runs on (default $NODE_NAME)")
	metricsListen        = flag.String("metrics-listen", ":9090", "listen address for the Prometheus /metrics endpoint (Go runtime and process metrics; with -self-metrics-interval=0 also the kubescrape_* internal metrics, replacing the OTLP push with a scrape; empty disables). Separate from -listen so the debug/health surface and the scrape target can be exposed independently")
	listen               = flag.String("listen", ":8081", "HTTP listen address for /healthz, /readyz, /debug/tailer and /debug/targets (empty disables)")
	selfMetricsIntv      = flag.Duration("self-metrics-interval", time.Minute, "export the agent's own metrics over OTLP at this interval (0 disables)")
	metadataURL          = flag.String("metadata-endpoint", "http://kubescrape.monitoring", "base URL of the kubescrape metadata service")
	metadataWait         = flag.Duration("metadata-wait", 5*time.Second, "how long the metadata service may block waiting for a new container")
	scrapeAuthToken      = flag.String("scrape-auth-token-file", "", "bearer token file for the metadata service's /v1/scrape-auth endpoint (re-read periodically); required when the service runs -scrape-auth-secrets")
	otlpEndpoint         = flag.String("otlp-endpoint", "otel-collector.monitoring:4317", "OTLP endpoint: host:port for grpc, base URL for http")
	otlpProtocol         = flag.String("otlp-protocol", "grpc", "OTLP transport: grpc or http")
	otlpCompression      = flag.String("otlp-compression", "gzip", "OTLP payload compression: gzip or none")
	otlpCompressionLevel = flag.Int("otlp-compression-level", 0, "gzip level 1 (fastest, ~2-3x less CPU for ~10% larger payloads) to 9 (smallest); 0 = library default")
	otlpInsecure         = flag.Bool("otlp-insecure", true, "use a plaintext gRPC connection (for http, use an http:// endpoint)")
	otlpSkipTLS          = flag.Bool("otlp-tls-insecure-skip-verify", false, "skip TLS certificate verification towards the collector")
	otlpCAFile           = flag.String("otlp-tls-ca-file", "", "PEM CA bundle for verifying the collector")
	otlpBearer           = flag.String("otlp-bearer-token-file", "", "file with a bearer token sent on every export (re-read periodically)")
	otlpTimeout          = flag.Duration("otlp-timeout", 15*time.Second, "per-export-attempt timeout")
	otlpRetries          = flag.Int("otlp-retry-attempts", 3, "tries per metrics export (logs retry via the tailer's rewind)")
	otlpBackoff          = flag.Duration("otlp-retry-backoff", time.Second, "initial backoff between metric export retries, doubled per attempt")
	otlpMaxSendBytes     = flag.Int("otlp-max-send-bytes", 0, "cap on one exported payload's encoded protobuf size; a larger payload is split into parts before sending (0 = default ~3.75 MiB, under the 4 MiB gRPC limit; negative disables)")

	transformsFile = flag.String("transforms-file", "", "Starlark transforms file applied to exported logs/metrics/traces at the exporter seam; hot-reloaded on change (mount its ConfigMap as a directory, not subPath). Empty disables")

	nativeHists = flag.Bool("scrape-native-histograms", false, "offer the Prometheus protobuf exposition to scrape targets and convert native histograms to OTLP exponential histograms")
	checkConfig = flag.Bool("check-config", false, "validate -config and -transforms-file (every section compiled: templates, regexes, selectors, globs) plus the flags, print a summary and exit — no listeners, log files, positions file, spools or network. For CI and pre-rollout checks: a DaemonSet's bad ConfigMap otherwise surfaces as a fleet-wide CrashLoop")
	testConfig  = flag.String("test-config", "", "run the YAML test cases in this file through the compiled log pipeline (scrub → logAttributes → enrich → logMetrics → logs.rules → transforms) and exit non-zero on failure — CI proof of what a rule/scrub/transform edit does to sample lines, with nothing acquired (like -check-config)")
	pprofListen = flag.String("pprof-listen", "", "listen address for net/http/pprof under /debug/pprof, on its own port (empty disables). Off by default and separate from -listen and -metrics-listen: profiles expose goroutine stacks and heap contents, so this is the port to firewall or bind to localhost")
	logLevel    = flag.String("log-level", "info", "log level: debug, info, warn, error")
	logFormat   = flag.String("log-format", "text", "log format: text or json")

	// One switch for all three log-producing paths. They were three separate
	// flags (-logs-enrich/-journald-enrich/-ingest-logs-enrich) for one
	// feature, all defaulting to true; nothing wanted them to disagree.
	enrichOn          = flag.Bool("enrich", true, "parse per-line metadata (timestamp, severity, trace/span IDs, exception details) into the OTLP record fields via github.com/JohanLindvall/enrich, for container logs, journald, Kubernetes events, Azure diagnostics and pushed OTLP log bodies alike")
	logDir            = flag.String("log-dir", "/var/log/containers", "directory of containerd log symlinks (the default source when the config's logs section is unset)")
	positionsFile     = flag.String("positions-file", "", "single file persisting BOTH log offsets and the journald cursor across restarts (empty disables persistence)")
	logsBatch         = flag.Int("logs-batch-size", 1024, "flush logs after this many entries")
	logsFlush         = flag.Duration("logs-flush-interval", 2*time.Second, "flush logs at least this often")
	maxEntryBytes     = flag.Int("logs-max-entry-bytes", 1<<20, "truncate assembled log entries beyond this size")
	multilineOn       = flag.Bool("logs-multiline", true, "join application-level multi-line entries (stack traces, ...)")
	multilineWait     = flag.Duration("logs-multiline-timeout", time.Second, "flush incomplete multi-line groups after this long")
	excludeNs         = flag.String("logs-exclude-namespaces", "", "comma-separated namespaces whose container logs are not tailed")
	logsRateLimit     = flag.Float64("logs-rate-limit", 0, "per-file line rate limit in lines/second (0 disables); exhausted files pause until tokens refill")
	logsRateBurst     = flag.Float64("logs-rate-burst", 0, "rate-limit token bucket size (0 = 2x -logs-rate-limit)")
	logsRateDrop      = flag.Bool("logs-rate-drop", false, "discard lines over -logs-rate-limit instead of pausing the file")
	logsIdleClose     = flag.Duration("logs-idle-close", 0, "close the fd of a fully-caught-up file after this much inactivity (0 = never, the default). The open fd is the only way to drain a rotated-away or deleted file, so enabling this trades the zero-loss guarantee for bounded fd usage")
	logsUnknownFiles  = flag.String("logs-unknown-files", "auto", "where a file with no checkpoint entry starts at startup: end (skip as history), start (read whole), auto (start when the checkpoint store has entries — it appeared while the agent was down — else end)")
	logsFileAttrs     = flag.Bool("logs-file-attributes", false, "stamp log.file.name and log.file.position (byte offset) on every log record, for each file source")
	bufferDir         = flag.String("buffer-dir", "", "directory for a disk-backed export buffer (logs and metrics); a collector outage spools here instead of pinning the tailer to old offsets or dropping metrics (empty disables)")
	bufferMax         = flag.Int("buffer-max-bytes", 1<<30, "per-signal cap on the undelivered on-disk buffer; producers back-pressure (the tailer rewinds) when full")
	logsMetricsEvery  = flag.Duration("logs-metrics-interval", 30*time.Second, "export interval for log-derived metrics")
	logsMetricsBytes  = flag.Int("logs-metrics-max-bytes", 3<<20, "export log-derived metrics in chunks below this many bytes (0 = one payload)")
	logsMetricsPrefix = flag.String("logs-metrics-name-prefix", "", "prefix prepended to every log-derived metric name")
	logsWatch         = flag.Bool("logs-watch", true, "use file events (fsnotify) to trigger reads and discovery; polling remains the fallback")
	logsPoll          = flag.Duration("logs-poll-interval", 500*time.Millisecond, "fallback sweep interval for the log tailer")
	logsFingerprint   = flag.Int("logs-fingerprint-bytes", 1024, "file-head hash length used with the inode as file identity (negative = inode only)")

	journaldOn    = flag.Bool("journald", false, "read the systemd journal natively via libsystemd/sdjournal (the image must provide libsystemd)")
	journaldDir   = flag.String("journald-dir", "", "read a specific journal directory; empty opens the default system journal")
	journaldUnits = flag.String("journald-units", "", "comma-separated systemd units to read (empty reads everything)")
	journaldBatch = flag.Int("journald-batch-size", 1024, "flush journal entries after this many")
	journaldBytes = flag.Int("journald-max-batch-bytes", 1<<20, "flush journal entries before a batch's summed message bytes exceed this")
	journaldFlush = flag.Duration("journald-flush-interval", 2*time.Second, "flush journal entries at least this often")

	// Kubernetes events. A CLUSTER-SINGLETON pipeline: deploy it as its own
	// single-replica Deployment with the other pipelines off, never as part of
	// the DaemonSet — N agents would each need cluster-wide API credentials and
	// would poll the election Lease N times per RetryPeriod.
	eventsOn        = flag.Bool("events", false, "watch Kubernetes Events and export them as OTLP logs, enriched with the involved object's identity. Cluster-singleton: exactly one replica runs it (leader election), so deploy it as its own Deployment with -logs=false -metrics=false -cadvisor=false -node-metrics=false, NOT in the DaemonSet")
	eventsNamespace = flag.String("events-namespace", "", "namespace to watch (empty = cluster-wide)")
	eventsStart     = flag.String("events-start", "auto", "where a cold start begins: end (skip the backlog), start (replay everything still within the API server's event TTL), auto (resume the stored position, else end)")
	eventsBatch     = flag.Int("events-batch-size", 512, "flush events after this many")
	eventsFlush     = flag.Duration("events-flush-interval", 2*time.Second, "flush events at least this often")
	eventsPersist   = flag.Duration("events-position-interval", 10*time.Second, "how often the position is written to its ConfigMap. A write per event would be an API-server write per event, so this is the bound on how much is REPLAYED after a hard kill (bounded duplicates, never loss); a graceful stop always writes a final position")
	eventsConfigMap = flag.String("events-position-configmap", "kubescrape-events-position", "ConfigMap holding the resume position. NOT a node-local file: the leader moves, so the successor must be able to read it")
	eventsLease     = flag.String("events-lease", "kubescrape-cluster-leader", "Lease coordinating the cluster-singleton pipelines")
	eventsLeaseNS   = flag.String("events-lease-namespace", "", "namespace for the Lease and position ConfigMap (default: this pod's own, via $POD_NAMESPACE or the ServiceAccount projection)")
	kubeconfig      = flag.String("kubeconfig", "", "path to a kubeconfig for the events watch; defaults to in-cluster config (only used with -events)")

	// Azure diagnostics: another cluster-scoped pipeline for the singleton
	// Deployment — but unlike -events it needs NO leader election, because
	// the Kafka consumer-group protocol is its coordination (each Event Hubs
	// partition is owned by exactly one group member).
	azureOn        = flag.Bool("azure-diagnostics", false, "consume Azure diagnostic-settings output (resource logs AND platform metrics) from an Event Hubs namespace over its Kafka endpoint and export it as OTLP. Cluster-scoped: run it in the same singleton Deployment as -events, not in the DaemonSet")
	azureNamespace = flag.String("azure-eventhub-namespace", "", "Event Hubs namespace host (myns.servicebus.windows.net); derived from the connection string's Endpoint when -azure-eventhub-connection-string-file is set")
	azureTopics    = flag.String("azure-eventhub-topics", "", "comma-separated event hubs to consume; empty consumes every hub matching ^insights-.* (the names diagnostic settings create by default)")
	azureGroup     = flag.String("azure-eventhub-group", "$Default", "Kafka consumer group; its committed offsets ARE the resume position, shared across restarts and replicas")
	azureConnFile  = flag.String("azure-eventhub-connection-string-file", "", "file holding an Event Hubs connection string (SASL PLAIN; re-read per connection, so rotation needs no restart); empty authenticates with managed identity (OAUTHBEARER via AKS workload identity when its env is present, else IMDS)")
	azureClientID  = flag.String("azure-client-id", "", "user-assigned managed identity / workload identity client id (default $AZURE_CLIENT_ID)")
	azureTenantID  = flag.String("azure-tenant-id", "", "Microsoft Entra tenant for workload identity (default $AZURE_TENANT_ID)")
	azureStart     = flag.String("azure-start", "end", "where a consumer group with NO committed offsets starts: end (skip the backlog) or start (replay everything the hubs retain)")
	azurePrefix    = flag.String("azure-metric-prefix", "azure.", "prefix for converted Azure metric names (<prefix><metricname>.<aggregation>)")

	scrapeInterval    = flag.Duration("scrape-interval", 30*time.Second, "Prometheus scrape interval")
	scrapeTimeout     = flag.Duration("scrape-timeout", 15*time.Second, "per-target scrape timeout")
	scrapeConcurrency = flag.Int("scrape-concurrency", 4, "concurrent target scrapes")
	metricsBatch      = flag.Int("metrics-batch-size", 10000, "export metrics in chunks of this many data points")
	metricsBatchBytes = flag.Int("metrics-batch-bytes", 3<<20, "also flush a metrics chunk once its estimated encoded size reaches this many bytes (0 = only -metrics-batch-size). The collector's gRPC receive limit applies to the DECOMPRESSED message (4 MiB by default), and a label-rich target can exceed it well before the point limit — every export of that target would then fail")
	maxSamples        = flag.Int("scrape-max-samples", 0, "abort a single scrape beyond this many samples (0 = unlimited)")
	exemplars         = flag.Bool("scrape-exemplars", false, "negotiate OpenMetrics and attach exemplars to counter and histogram data points")
	healthMetrics     = flag.Bool("scrape-health-metrics", true, "export synthetic up/scrape_duration_seconds/scrape_samples_scraped gauges per target")

	kubeletEndpoint = flag.String("kubelet-endpoint", "", "kubelet base URL, e.g. https://$(NODE_IP):10250 (empty disables the cadvisor and node-metrics scrapes)")
	kubeletToken    = flag.String("kubelet-token-file", "/var/run/secrets/kubernetes.io/serviceaccount/token", "bearer token file for the kubelet (re-read per scrape)")
	kubeletInsecure = flag.Bool("kubelet-insecure-tls", true, "skip TLS verification for the kubelet (its serving certificate is typically self-signed)")

	nodeRefresh      = flag.Duration("node-metadata-refresh", time.Minute, "refresh interval for the node's labels/annotations used in attribute templates (0 disables the lookup)")
	selfAttrsOn      = flag.Bool("self-attributes", true, "add THIS pod's Kubernetes resource attributes (namespace, pod, uid, owners, labels, plus the resourceAttributes section's static/template attributes for the `self` pipeline) to the metrics the agent generates about itself — its self-metrics and span metrics. Resolved from the metadata service's GET /v1/self, which attributes the request by its source address. Attributes the agent already set (service.name, service.instance.id, ...) are never overwritten; a caller the service cannot attribute to a live pod (hostNetwork, an address-rewriting hop) simply gets none. kubescrape_self_metadata_resolved reports whether it resolved")
	selfAttrsRefresh = flag.Duration("self-attributes-refresh", selfmeta.DefaultRefresh, "how often to re-read this pod's own metadata, so an edited pod or namespace label reaches the metrics it stamps (0 disables the lookup entirely, as -node-metadata-refresh=0 does for the node's). Cheap by construction: GET /v1/self carries `private, max-age` + ETag, so the client serves a fresh entry locally and revalidates a stale one as a conditional GET — a 304 whenever nothing changed. Retries before the first success start at 5s and back off to this")

	// Pipeline toggles.
	logsOn     = flag.Bool("logs", true, "tail container logs")
	metricsOn  = flag.Bool("metrics", true, "scrape annotation-discovered pod/service targets")
	cadvisorOn = flag.Bool("cadvisor", true, "scrape <kubelet-endpoint>/metrics/cadvisor (per-container metrics)")
	rollupsOn  = flag.Bool("cadvisor-rollups", true, "include cadvisor rollup series: cgroups above pod level and pod-level rows of container-scoped families")
	nodeOn     = flag.Bool("node-metrics", true, "scrape <kubelet-endpoint>/metrics (kubelet/node metrics)")

	// OTLP ingest (apps push telemetry to the local agent for enrichment).
	ingestOn      = flag.Bool("ingest", false, "receive pushed OTLP logs/metrics/traces and enrich them with k8s attributes before forwarding")
	ingestGRPC    = flag.String("ingest-grpc-endpoint", ":4317", "listen address for pushed OTLP/gRPC (empty disables)")
	ingestHTTP    = flag.String("ingest-http-endpoint", ":4318", "listen address for pushed OTLP/HTTP protobuf on /v1/logs, /v1/metrics and /v1/traces (empty disables)")
	ingestWait    = flag.Duration("ingest-metadata-wait", 0, "how long an ingest metadata lookup may block for not-yet-known objects")
	ingestMetrics = flag.String("ingest-metrics-mode", "auto", "how pushed metrics resolve their object: resource (id on the resource), datapoint (id on each point, split into per-object resources), or auto")
	ingestCidKeys = flag.String("ingest-container-id-keys", "container.id,k8s.container.id", "comma-separated attribute keys inspected for a container id")
	ingestUIDKeys = flag.String("ingest-pod-uid-keys", "k8s.pod.uid", "comma-separated attribute keys inspected for a pod uid")
	ingestTraces  = flag.Bool("ingest-traces", true, "accept pushed traces (gRPC + /v1/traces), enrich their resources and pass them through")
	spanMetrics   = flag.Bool("ingest-span-metrics", false, "derive RED (calls + duration histogram) metrics from ingested spans, dimensioned by service.name/span.name/span.kind/status.code; exported over OTLP (tune via the traceMetrics config section)")
	spanMetricsIv = flag.Duration("ingest-span-metrics-interval", time.Minute, "export interval for span metrics")
	ingestPeerIP  = flag.Bool("ingest-peer-ip-fallback", false, "attribute pushed telemetry whose resource carries no container id / pod uid to the pod owning the connection's peer IP (hostNetwork senders never resolve)")
)

// pipelines bundles what the per-pipeline start functions share: the
// lifecycle primitives (ctx/wg/stop), the common sinks and sources, and the
// parsed config. All flag reads stay in the start functions themselves.
type pipelines struct {
	ctx  context.Context
	wg   *sync.WaitGroup
	stop context.CancelFunc
	log  *slog.Logger
	out  otlpexport.Exporter
	// selfOut is `out` for metrics the agent generates about ITSELF: it fills
	// in this pod's own Kubernetes resource attributes (see selfattrs.go).
	selfOut      selfmeta.Exporter
	meta         *metaclient.Client
	nodeInfo     func() *attrs.NodeInfo
	attrBuilders *attrs.Builders
	fileCfg      agentConfig
	posStore     *positions.Store
	ready        *readiness
	logAttrs     *logattrs.Extractor
	scrub        *logscrub.Scrubber
	transforms   *transform.Wrapper
	logMetrics   *metrics.DynamicMetricSet
	// journalRules is the compiled logs.rules chain, applied to journal entries
	// as well as container logs (same section, same semantics).
	journalRules *logline.LineFilter
	// spanMetricsGen is published by startIngest so run() can export the last
	// aggregation window after every producer has joined.
	spanMetricsGen *spanmetrics.Generator
	spanMetricsRes pcommon.Resource
	ingestMode     otlpingest.MetricsMode
	filters        *promscrape.MetricFilters
	splitters      []*promscrape.Splitter
	// fatalErr receives a pipeline's fatal failure (currently only the ingest
	// listener); wg.Wait orders the write before run() reads it.
	fatalErr *error
}

// spawn runs fn on the shared WaitGroup.
func (p *pipelines) spawn(fn func()) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		fn()
	}()
}

func run() error {
	flag.Parse()

	if *nodeName == "" {
		return fmt.Errorf("node name is required (set -node-name or $NODE_NAME)")
	}
	ingestMode := otlpingest.MetricsMode(*ingestMetrics)
	switch ingestMode {
	case otlpingest.MetricsResource, otlpingest.MetricsDatapoint, otlpingest.MetricsAuto:
	default:
		return fmt.Errorf("invalid -ingest-metrics-mode %q (want resource, datapoint or auto)", *ingestMetrics)
	}
	switch *logsUnknownFiles {
	case "auto", "end", "start":
	default:
		return fmt.Errorf("invalid -logs-unknown-files %q (want auto, end or start)", *logsUnknownFiles)
	}
	if *ingestOn && *ingestGRPC == "" && *ingestHTTP == "" {
		return fmt.Errorf("-ingest is set but both -ingest-grpc-endpoint and -ingest-http-endpoint are empty")
	}
	if err := events.ValidateStartMode(*eventsStart); err != nil {
		return err
	}
	if err := azurediag.ValidateStartMode(*azureStart); err != nil {
		return err
	}
	if *azureOn && *azureNamespace == "" && *azureConnFile == "" {
		return fmt.Errorf("-azure-diagnostics is set but neither -azure-eventhub-namespace nor -azure-eventhub-connection-string-file is")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log, err := newLogger(*logLevel, *logFormat)
	if err != nil {
		return err
	}
	slog.SetDefault(log)
	// First line of every run: without a build identity a panic trace, a
	// metric anomaly or a half-finished rollout cannot be tied to a commit.
	log.Info("kubescrape-agent starting", "version", obs.BuildVersion(), "built", obs.BuildTime())

	// All YAML config lives in one file; each section is optional.
	var fileCfg agentConfig
	if *configFile != "" {
		c, err := loadAgentConfig(*configFile)
		if err != nil {
			return fmt.Errorf("config: %w", err)
		}
		fileCfg = *c
	}

	// Compile every config section before acquiring anything, so a bad config
	// fails fast and identically whether or not -check-config was passed.
	if err := validateConfig(fileCfg, *transformsFile); err != nil {
		return err
	}
	if *checkConfig {
		printConfigSummary(fileCfg, log)
		return nil
	}
	if *testConfig != "" {
		// Like -check-config: run and exit without acquiring anything.
		return runConfigTests(fileCfg, *transformsFile, *testConfig, log)
	}

	// The logs.rules chain is shared by the tailer AND journald, so it is
	// compiled here rather than inside startLogs: journald must get it even
	// with -logs=false.
	var logRules *logline.LineFilter
	if fileCfg.Logs != nil {
		if logRules, err = logline.NewLineFilter(fileCfg.Logs.Rules); err != nil {
			return fmt.Errorf("logs.rules: %w", err)
		}
	}

	attrBuilders, err := buildAttrs(fileCfg.ResourceAttributes)
	if err != nil {
		return fmt.Errorf("resource attributes: %w", err)
	}

	// A single positions file, when configured, backs both the log tailer's
	// offsets and the journald cursor.
	var posStore *positions.Store
	if *positionsFile != "" {
		if posStore, err = positions.Open(*positionsFile); err != nil {
			return fmt.Errorf("positions file: %w", err)
		}
	}

	// Optional log-line attribute lifting, shared by the tailer and journald.
	var logAttrs *logattrs.Extractor
	if fileCfg.LogAttributes != nil {
		if logAttrs, err = logattrs.New(fileCfg.LogAttributes); err != nil {
			return fmt.Errorf("log attributes config: %w", err)
		}
	}

	// Optional PII scrubbing, shared by every log path (tailer, journald,
	// ingest). Compiled once; fail-fast on bad patterns.
	var scrub *logscrub.Scrubber
	if fileCfg.LogScrubbing != nil {
		if scrub, err = logscrub.New(*fileCfg.LogScrubbing); err != nil {
			return fmt.Errorf("log scrubbing config: %w", err)
		}
		log.Info("log scrubbing enabled", "patterns", len(fileCfg.LogScrubbing.Builtin)+len(fileCfg.LogScrubbing.Rules))
	}

	// The metadata client's HTTP timeout must exceed the server-side wait —
	// including the ingest lookups' own wait, which may be longer.
	meta := metaclient.New(*metadataURL, max(*metadataWait, *ingestWait)+10*time.Second)
	// The client is dependency-free by design; feed its outcomes to our metrics.
	meta.Observe = func(outcome string) { obs.MetadataRequests.WithLabelValues(outcome).Inc() }
	if *scrapeAuthToken != "" {
		// /v1/scrape-auth returns Secret VALUES and is the one authenticated
		// endpoint. Read through a cache so the file is not hit per scrape, and
		// re-read so a rotated Secret is picked up without a restart.
		reader := newTokenFile(*scrapeAuthToken, log)
		if _, err := reader.read(); err != nil {
			return fmt.Errorf("reading -scrape-auth-token-file: %w", err)
		}
		meta.SetScrapeAuthToken(reader.get)
	}

	// The Prometheus scrape target for this process's own metrics, on its own
	// port (see -metrics-listen). With the OTLP self-metrics push disabled
	// (-self-metrics-interval=0) the kubescrape_* metrics ride the scrape
	// instead — one knob selects the modality, so the two paths never
	// double-deliver.
	stopMetrics := obs.ServeMetrics(ctx, *metricsListen, *selfMetricsIntv <= 0, log)
	defer stopMetrics()
	stopPprof := obs.ServePprof(ctx, *pprofListen, log)
	defer stopPprof()

	ready := newReadiness()
	if *nodeRefresh > 0 {
		// Reaching the metadata service is what separates a working new agent
		// from one that will attribute nothing; with refresh disabled the agent
		// never calls it, so there is nothing to gate on.
		ready.require(gateMetadata)
	}
	nodeInfo := startNodeInfo(ctx, meta, *nodeName, *nodeRefresh, log, ready)

	// The pod THIS process runs in, for the resource attributes of the metrics
	// it generates about itself, re-read on -self-attributes-refresh so a
	// relabelled pod or namespace reaches them. Skipped outright when nothing
	// self-describing is exported — an agent that generates no such metrics has
	// no reason to poll the service about itself — and when the refresh is 0,
	// which disables the lookup (the gauge is registered exactly when the
	// lookup RUNS, so a published 0 always means unresolved, never "off").
	var selfPod func() *kubemeta.Pod
	if *selfAttrsOn && selfDescribing() && *selfAttrsRefresh > 0 {
		selfPod = selfmeta.StartPod(ctx, selfResolve(meta), *selfAttrsRefresh, log)
		obs.RegisterSelfMetadata(func() bool { return selfPod() != nil })
	}

	var metricFilters *promscrape.MetricFilters
	var splitters []*promscrape.Splitter
	if fileCfg.Metrics != nil {
		if metricFilters, err = promscrape.NewMetricFilters(fileCfg.Metrics.Pipelines); err != nil {
			return fmt.Errorf("metrics config: %w", err)
		}
		if splitters, err = promscrape.NewSplitters(fileCfg.Metrics.Splitters); err != nil {
			return fmt.Errorf("metrics config: %w", err)
		}
	}

	baseExport := otlpexport.Config{
		Endpoint:           *otlpEndpoint,
		Protocol:           *otlpProtocol,
		Compression:        *otlpCompression,
		CompressionLevel:   *otlpCompressionLevel,
		Insecure:           *otlpInsecure,
		InsecureSkipVerify: *otlpSkipTLS,
		CAFile:             *otlpCAFile,
		BearerTokenFile:    *otlpBearer,
		Timeout:            *otlpTimeout,
		RetryAttempts:      *otlpRetries,
		RetryBackoff:       *otlpBackoff,
		MaxSendBytes:       *otlpMaxSendBytes,
	}
	// The flag base plus the config's export section: per-signal destinations
	// ride the existing per-signal spools, and the default chain gains static
	// headers / an mTLS client certificate.
	exporter, err := otlpexport.BuildExporter(baseExport, fileCfg.Export)
	if err != nil {
		return fmt.Errorf("creating OTLP exporter: %w", err)
	}
	defer func() { _ = exporter.Close() }()

	var wg sync.WaitGroup

	// Every consumer exports through `out`. With -buffer-dir set it is a
	// disk-backed buffer (separate spools for logs and metrics): a collector
	// outage spools to disk (bounded per signal) instead of pinning the tailer
	// to old file offsets or dropping scraped metrics. Otherwise it is the raw
	// client.
	var out otlpexport.Exporter = exporter
	// Set when the disk buffer is enabled: the shutdown pass that empties the
	// spools after every producer has stopped (Buffered.Run exits on cancel).
	var finalDrain func(context.Context)
	if *bufferDir != "" {
		logBuf, err := otlpexport.OpenBuffer(filepath.Join(*bufferDir, "logs"), int64(*bufferMax))
		if err != nil {
			return fmt.Errorf("log buffer: %w", err)
		}
		defer func() {
			if err := logBuf.Close(); err != nil {
				log.Warn("closing the log buffer", "error", err)
			}
		}()
		metricBuf, err := otlpexport.OpenBuffer(filepath.Join(*bufferDir, "metrics"), int64(*bufferMax))
		if err != nil {
			return fmt.Errorf("metric buffer: %w", err)
		}
		defer func() {
			if err := metricBuf.Close(); err != nil {
				log.Warn("closing the metric buffer", "error", err)
			}
		}()
		buffered := otlpexport.NewBuffered(exporter, logBuf, metricBuf, *otlpBackoff, log)
		wg.Add(1)
		go func() {
			defer wg.Done()
			buffered.Run(ctx)
		}()
		out = buffered
		finalDrain = buffered.FinalDrain
		// Make a filling buffer visible BEFORE it starts refusing writes: every
		// other buffer metric only moves once data is already being dropped.
		obs.RegisterBufferStats(buffered.Stats)
		log.Info("disk buffer enabled", "dir", *bufferDir, "max-bytes-per-signal", *bufferMax)
	}

	// Routing sits between transforms and the default delivery chain:
	// producers → transform → router → {default buffered chain | route
	// clients}. Route clients inherit the main endpoint/TLS/compression
	// settings unless the route overrides the endpoint; per-route
	// destinations are direct (unbuffered) — the default keeps the full
	// durability chain.
	// Captured before the router so the agent's own metrics can keep the
	// default (buffered) chain — see selfSink below.
	preRoute := out
	routed := false
	if fileCfg.Routing != nil && len(fileCfg.Routing.Routes) > 0 {
		var dests []route.Destination
		for i, rt := range fileCfg.Routing.Routes {
			if rt.Name == "" || len(rt.Namespaces) == 0 {
				return fmt.Errorf("routing route %d: name and namespaces are required", i)
			}
			// A malformed glob makes path.Match return ErrBadPattern for EVERY
			// namespace, which the matcher reads as "no match": the route never
			// fires and its tenant's telemetry goes silently to the default
			// destination — indistinguishable from "no traffic yet", since the
			// route's counter simply stays at zero. Fail startup instead.
			for _, pat := range rt.Namespaces {
				if _, err := path.Match(pat, ""); err != nil {
					return fmt.Errorf("routing route %q: invalid namespace pattern %q: %w", rt.Name, pat, err)
				}
			}
			// Route clients inherit the flag base PLUS the export section's
			// base additions (headers, client cert); the route's own headers
			// win per key.
			rcfg := fileCfg.Export.ApplyBase(baseExport)
			if len(rt.Headers) > 0 {
				merged := make(map[string]string, len(rcfg.Headers)+len(rt.Headers))
				for k, v := range rcfg.Headers {
					merged[k] = v
				}
				for k, v := range rt.Headers {
					merged[k] = v
				}
				rcfg.Headers = merged
			}
			if rt.Endpoint != "" {
				rcfg.Endpoint = rt.Endpoint
			}
			rc, err := otlpexport.New(rcfg)
			if err != nil {
				return fmt.Errorf("routing route %q: %w", rt.Name, err)
			}
			defer func() { _ = rc.Close() }()
			dests = append(dests, route.Destination{Name: rt.Name, Namespaces: rt.Namespaces, Exporter: rc})
		}
		out = route.New(out, dests)
		routed = true
		log.Info("routing enabled", "routes", len(dests))
	}

	// Transforms wrap the producer-facing exporter ABOVE the disk buffer:
	// producers → transform → buffer → client, so spooled bytes are final
	// and a reload never re-interprets a durable backlog. Compile fails
	// startup; reloads compile-then-commit (a broken edit keeps the last
	// good program).
	var transforms *transform.Wrapper
	if *transformsFile != "" {
		prog, err := transform.CompileFile(*transformsFile)
		if err != nil {
			return fmt.Errorf("transforms: %w", err)
		}
		traceNext, _ := out.(transform.TracesExporter)
		transforms = transform.Wrap(out, traceNext, prog)
		out = transforms
		wg.Add(1)
		go func() {
			defer wg.Done()
			transform.Reload(ctx, transforms, *transformsFile, 0, log)
		}()
		log.Info("transforms enabled", "file", *transformsFile, "hash", prog.Hash)
	}

	// Registered AFTER the exporter/spool Close defers (LIFO): an early `return
	// err` below must stop and drain every started goroutine BEFORE their
	// exporter and spools are closed under them. The normal path's inline
	// wg.Wait makes this a no-op there.
	defer func() {
		stop()
		wg.Wait()
	}()

	// The sink for the metrics the agent generates ABOUT ITSELF: this pod's own
	// Kubernetes attributes filled in where the agent's identity left a key
	// unset, over the chain selfSink picks.
	selfOut := selfmeta.Wrap(selfSink(out, preRoute, routed, transforms), selfPod,
		selfBuild(attrBuilders.Self, nodeInfo))

	var selfRes pcommon.Resource
	if *selfMetricsIntv > 0 {
		selfRes = agentSelfResource(*nodeName)
		wg.Add(1)
		go func() {
			defer wg.Done()
			obs.Registry.Run(ctx, selfOut, *selfMetricsIntv, selfRes, log)
		}()
		log.Info("self-metrics export started", "interval", *selfMetricsIntv)
	}

	// Optional metrics derived from log lines; only these configured metrics are
	// exported (over the shared OTLP exporter), on their own interval.
	var logMetrics *metrics.DynamicMetricSet
	if fileCfg.LogMetrics != nil && len(fileCfg.LogMetrics.Metrics) > 0 {
		opts := []metrics.Option{metrics.WithLogger(log), metrics.WithNamePrefix(*logsMetricsPrefix)}
		if logMetrics, err = metrics.NewDynamicMetricSet(fileCfg.LogMetrics.Metrics, opts...); err != nil {
			return fmt.Errorf("logs metrics config: %w", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			logMetrics.Run(ctx, out, *logsMetricsEvery, *logsMetricsBytes)
		}()
		log.Info("log-derived metrics started", "metrics", logMetrics.Count, "interval", *logsMetricsEvery)
	}

	// A fatal pipeline failure (currently only the ingest listener) is stored
	// here and returned after shutdown so the agent exits non-zero; wg.Wait
	// orders the write before the read.
	var fatalErr error

	p := &pipelines{
		ctx:          ctx,
		wg:           &wg,
		stop:         stop,
		log:          log,
		out:          out,
		selfOut:      selfOut,
		meta:         meta,
		nodeInfo:     nodeInfo,
		attrBuilders: attrBuilders,
		fileCfg:      fileCfg,
		posStore:     posStore,
		ready:        ready,
		logAttrs:     logAttrs,
		scrub:        scrub,
		transforms:   transforms,
		logMetrics:   logMetrics,
		journalRules: logRules,
		ingestMode:   ingestMode,
		filters:      metricFilters,
		splitters:    splitters,
		fatalErr:     &fatalErr,
	}
	tl, err := p.startLogs()
	if err != nil {
		return err
	}
	p.startJournald()
	if err := p.startIngest(); err != nil {
		return err
	}
	if err := p.startEvents(); err != nil {
		return err
	}
	if err := p.startAzure(); err != nil {
		return err
	}
	sc := p.startScraper()
	p.startDebugServer(tl, sc)

	<-ctx.Done()
	log.Info("shutting down")
	wg.Wait()
	if logMetrics != nil {
		// The tailer's final flush (inside wg.Wait) fed the set; export the
		// last window before the deferred exporter/buffer close.
		fctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := logMetrics.Export(fctx, out, *logsMetricsBytes); err != nil {
			log.Warn("final log-metrics export failed", "error", err)
		}
	}
	if p.spanMetricsGen != nil {
		// Generator.Run does its final export when ctx is cancelled, but the
		// ingest server's GracefulStop completes in-flight RPCs AFTER that,
		// and every trace they forward passes through the tap, bumping the
		// cumulative series. Those spans ship; without this their RED metrics
		// would not.
		fctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := p.spanMetricsGen.Export(fctx, p.selfOut, p.spanMetricsRes); err != nil {
			log.Warn("final span-metrics export failed", "error", err)
		}
		cancel()
	}
	if *selfMetricsIntv > 0 {
		// Registry.Run's own final export raced the final flushes inside
		// wg.Wait; counters they bumped (last batches, shutdown drops) would
		// otherwise die unexported. One more export now that everything is done.
		obs.Registry.FinalExport(selfOut, selfRes, log)
	}
	if finalDrain != nil {
		// Everything above only reached the SPOOL: Buffered.Run stopped when
		// ctx was cancelled. Empty it now, before the deferred exporter and
		// spool Closes, or this window waits for the next start of this pod on
		// this node — and is lost outright if the pod never comes back or the
		// buffer dir is not persistent. Bounded: a dead collector must not
		// outlive the pod's termination grace.
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		finalDrain(dctx)
		dcancel()
	}
	return fatalErr
}

// startLogs starts the container/plain-file log tailer. The returned Tailer
// (nil when -logs is off) is exposed on /debug/tailer.
func (p *pipelines) startLogs() (*tailer.Tailer, error) {
	if !*logsOn {
		return nil, nil
	}
	var err error
	var logSources []tailer.Source
	logRules := p.journalRules // the same compiled logs.rules chain
	if p.fileCfg.Logs != nil {
		if logSources, err = tailer.ValidateSources(p.fileCfg.Logs.Sources); err != nil {
			return nil, fmt.Errorf("logs config: %w", err)
		}
	}
	tl := tailer.New(tailer.Config{
		Dir:               *logDir,
		Sources:           logSources,
		Positions:         p.posStore,
		LogAttrs:          p.logAttrs,
		Scrub:             p.scrub,
		LogMetrics:        p.logMetrics,
		Watch:             *logsWatch,
		PollInterval:      *logsPoll,
		FingerprintBytes:  *logsFingerprint,
		FlushInterval:     *logsFlush,
		BatchSize:         *logsBatch,
		MaxEntryBytes:     *maxEntryBytes,
		RateLimit:         *logsRateLimit,
		RateBurst:         *logsRateBurst,
		RateDrop:          *logsRateDrop,
		UnknownFiles:      *logsUnknownFiles,
		IdleClose:         *logsIdleClose,
		Rules:             logRules,
		Multiline:         *multilineOn,
		MultilineTimeout:  *multilineWait,
		Enrich:            *enrichOn,
		FileAttributes:    *logsFileAttrs,
		ExcludeNamespaces: splitList(*excludeNs),
		Attrs:             p.attrBuilders.Logs,
		NodeInfo:          p.nodeInfo,
		MetadataWait:      *metadataWait,
		Metadata:          p.meta,
		Exporter:          p.out,
		Logger:            p.log,
	})
	p.spawn(func() {
		tl.Run(p.ctx)
	})
	if *positionsFile == "" {
		p.log.Warn("no -positions-file: offsets are not persisted (a restart re-reads per -logs-unknown-files; journald starts at the tail)")
	}
	p.log.Info("log tailer started", "dir", *logDir, "positions", *positionsFile)
	return tl, nil
}

// startJournald starts the systemd journal reader.
func (p *pipelines) startJournald() {
	if !*journaldOn {
		return
	}
	jr := journald.New(journald.Config{
		Dir:           *journaldDir,
		Units:         splitList(*journaldUnits),
		Positions:     p.posStore,
		BatchSize:     *journaldBatch,
		MaxBatchBytes: *journaldBytes,
		FlushInterval: *journaldFlush,
		MaxEntryBytes: *maxEntryBytes,
		Enrich:        *enrichOn,
		LogAttrs:      p.logAttrs,
		Scrub:         p.scrub,
		Rules:         p.journalRules,
		LogMetrics:    p.logMetrics,
		Attrs:         p.attrBuilders.Journal,
		NodeInfo:      p.nodeInfo,
		Exporter:      p.out,
		Logger:        p.log,
	})
	p.spawn(func() {
		jr.Run(p.ctx)
	})
	p.log.Info("journald reader started", "dir", *journaldDir, "units", *journaldUnits, "positions", *positionsFile)
}

// startEvents starts the cluster-singleton Kubernetes events reader under a
// leader election, so exactly one replica watches (N watchers would emit N
// copies of every event).
func (p *pipelines) startEvents() error {
	if !*eventsOn {
		return nil
	}
	cfg, err := kubeConfig(*kubeconfig)
	if err != nil {
		return fmt.Errorf("events: building the kubernetes client config: %w", err)
	}
	cfg.UserAgent = "kubescrape-agent"
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("events: creating the kubernetes client: %w", err)
	}
	ns := *eventsLeaseNS
	if ns == "" {
		ns = leader.Namespace()
	}
	if ns == "" {
		return fmt.Errorf("events: no namespace for the lease and position ConfigMap; set -events-lease-namespace or $POD_NAMESPACE (downward API)")
	}
	reader := events.New(events.Config{
		Client:          client,
		Positions:       &events.ConfigMapStore{Client: client, Namespace: ns, Name: *eventsConfigMap},
		StartMode:       *eventsStart,
		Namespace:       *eventsNamespace,
		BatchSize:       *eventsBatch,
		FlushInterval:   *eventsFlush,
		PersistInterval: *eventsPersist,
		Meta:            p.meta,
		Enrich:          *enrichOn,
		Scrub:           p.scrub,
		LogAttrs:        p.logAttrs,
		Rules:           p.journalRules, // the same logs.rules chain
		LogMetrics:      p.logMetrics,
		Attrs:           p.attrBuilders.Ingest,
		Exporter:        p.out,
		Logger:          p.log,
	})
	p.spawn(func() {
		// The election goroutine must be inside the WaitGroup: ReleaseOnCancel
		// only hands the lease back if Run returns before the process exits.
		err := leader.Run(p.ctx, leader.Config{
			Client:    client,
			Namespace: ns,
			Name:      *eventsLease,
			OnStarted: reader.Run,
			OnLeading: func(leading bool) {
				if leading {
					obs.Leader.Set(1)
				} else {
					obs.Leader.Set(0)
				}
			},
			Log: p.log,
		})
		if err != nil {
			p.log.Error("leader election failed; shutting down", "error", err)
			*p.fatalErr = fmt.Errorf("events leader election: %w", err)
			p.stop()
		}
	})
	p.log.Info("kubernetes events enabled", "lease", *eventsLease, "namespace", ns,
		"positionConfigMap", *eventsConfigMap, "start", *eventsStart)
	return nil
}

// gateAzure is satisfied by the first successful Event Hubs poll (the group
// is joined and the namespace reachable).
const gateAzure = "azure-eventhub"

// startAzure starts the Azure diagnostics consumer. Cluster-scoped like
// -events (run it in the same singleton Deployment), but with NO leader
// election: the Kafka consumer group is its coordination, so replicas > 1
// simply share partitions.
func (p *pipelines) startAzure() error {
	if !*azureOn {
		return nil
	}
	kafka := azurediag.KafkaConfig{
		Namespace:            *azureNamespace,
		Group:                *azureGroup,
		Start:                *azureStart,
		ConnectionStringFile: *azureConnFile,
		ClientID:             *azureClientID,
		TenantID:             *azureTenantID,
	}
	if *azureTopics != "" {
		for _, t := range strings.Split(*azureTopics, ",") {
			if t = strings.TrimSpace(t); t != "" {
				kafka.Topics = append(kafka.Topics, t)
			}
		}
	}
	if err := kafka.Resolve(); err != nil {
		return fmt.Errorf("azure diagnostics: %w", err)
	}
	p.ready.require(gateAzure)
	reader := azurediag.New(azurediag.Config{
		Kafka:        kafka,
		MetricPrefix: *azurePrefix,
		Enrich:       *enrichOn,
		Scrub:        p.scrub,
		LogAttrs:     p.logAttrs,
		Rules:        p.journalRules, // the same logs.rules chain
		LogMetrics:   p.logMetrics,
		Attrs:        p.attrBuilders.Ingest,
		Exporter:     p.out,
		Logger:       p.log,
		Ready:        func() { p.ready.done(gateAzure) },
	})
	p.spawn(func() { reader.Run(p.ctx) })
	p.log.Info("azure diagnostics enabled", "brokers", kafka.Brokers,
		"topics", kafka.Topics, "group", kafka.Group, "start", kafka.Start)
	return nil
}

// kubeConfig prefers an explicit kubeconfig, then in-cluster config, then the
// default loading rules (mirrors the metadata service).
func kubeConfig(path string) (*rest.Config, error) {
	if path == "" {
		if cfg, err := rest.InClusterConfig(); err == nil {
			return cfg, nil
		}
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = path
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
}

// startIngest starts the OTLP ingest receiver plus its optional trace
// sampler and span-metrics tap. A fatal listener failure is reported through
// p.fatalErr and p.stop so the agent exits non-zero.
func (p *pipelines) startIngest() error {
	if !*ingestOn {
		if *spanMetrics {
			p.log.Warn("-ingest-span-metrics ignored: the OTLP ingest receiver is disabled (-ingest=false)")
		}
		return nil
	}
	enr := otlpingest.NewEnricher(otlpingest.Config{
		ContainerIDKeys: splitList(*ingestCidKeys),
		PodUIDKeys:      splitList(*ingestUIDKeys),
		Wait:            *ingestWait,
		MetricsMode:     p.ingestMode,
		EnrichLines:     *enrichOn,
		Scrub:           p.scrub,
		PeerIPFallback:  *ingestPeerIP,
		Attrs:           p.attrBuilders.Ingest,
		Meta:            p.meta,
		Logger:          p.log,
	})
	var ingestOut otlpingest.Exporter = p.out
	var ingestTraceOut otlpingest.TracesExporter
	if *ingestTraces {
		// Both Client and Buffered export traces (Buffered passes them
		// through unbuffered).
		te, ok := p.out.(otlpingest.TracesExporter)
		if !ok {
			return fmt.Errorf("exporter does not support traces")
		}
		ingestTraceOut = te
	}
	// The sampler sits at the very bottom of the trace path — below the
	// span-metrics tap, so RED metrics are derived from 100% of spans while
	// only the sampled subset ships; decisions are per-trace-ID consistent,
	// so a sender's retry re-samples identically.
	if cfg := p.fileCfg.TraceSampling; cfg != nil && cfg.Enabled() {
		if err := cfg.Validate(); err != nil {
			return err
		}
		if ingestTraceOut == nil {
			// Symmetric with the -ingest-span-metrics warnings: a configured
			// section that silently does nothing is indistinguishable from one
			// that is working.
			p.log.Warn("traceSampling configured but ignored: the traces pipeline is off (-ingest and -ingest-traces)")
		} else {
			ingestTraceOut = tracesample.New(*cfg, ingestTraceOut)
			p.log.Info("trace sampling enabled", "probability", cfg.Probability,
				"maxSpansPerSecond", cfg.MaxSpansPerSecond, "keepSlowerThan", cfg.KeepSlowerThan)
		}
	}
	// The span-metrics tap wraps the RAW trace exporter: it forwards FIRST
	// and aggregates only on success (a transient failure surfaces retryable
	// to the sender, whose re-push must not double-count). Consume runs on
	// the concurrent ingest handler goroutines; the generator's series map is
	// mutex-guarded for exactly that.
	if *spanMetrics && ingestTraceOut != nil {
		var smCfg spanmetrics.Config
		if p.fileCfg.TraceMetrics != nil {
			smCfg = *p.fileCfg.TraceMetrics
		}
		gen := spanmetrics.New(smCfg)
		ingestTraceOut = gen.Tap(ingestTraceOut)
		res := agentSelfResource(*nodeName)
		p.spanMetricsGen, p.spanMetricsRes = gen, res
		p.spawn(func() {
			gen.Run(p.ctx, p.selfOut, *spanMetricsIv, res, p.log)
		})
		p.log.Info("span metrics from traces enabled", "interval", *spanMetricsIv)
	} else if *spanMetrics {
		p.log.Warn("-ingest-span-metrics ignored: traces are disabled (-ingest-traces=false)")
	}
	srv := otlpingest.NewServer(otlpingest.ServerConfig{
		GRPCAddr: *ingestGRPC,
		HTTPAddr: *ingestHTTP,
		Enricher: enr,
		Exporter: ingestOut,
		Traces:   ingestTraceOut,
		Logger:   p.log,
	})
	p.spawn(func() {
		if err := srv.Run(p.ctx); err != nil {
			// A dead ingest listener (e.g. the port already bound) must not
			// leave the agent looking healthy while apps push into a void:
			// shut the agent down and exit non-zero so the failure is
			// visible (CrashLoop).
			p.log.Error("otlp ingest server failed; shutting down", "error", err)
			*p.fatalErr = fmt.Errorf("otlp ingest server: %w", err)
			p.stop()
		}
	})
	p.log.Info("otlp ingest started", "grpc", *ingestGRPC, "http", *ingestHTTP, "metricsMode", *ingestMetrics)
	return nil
}

// startScraper starts the Prometheus scraper (annotation/ServiceMonitor
// targets and/or kubelet cadvisor+node scrapes). The returned Scraper (nil
// when scraping is off) is exposed on /debug/targets.
func (p *pipelines) startScraper() *promscrape.Scraper {
	kubeletScrapes := *kubeletEndpoint != "" && (*cadvisorOn || *nodeOn)
	var sc0 *promscrape.Scraper
	if *metricsOn || kubeletScrapes {
		sc := promscrape.New(promscrape.Config{
			Node:           *nodeName,
			Interval:       *scrapeInterval,
			Timeout:        *scrapeTimeout,
			Concurrency:    *scrapeConcurrency,
			BatchPoints:    *metricsBatch,
			BatchBytes:     *metricsBatchBytes,
			MaxSamples:     *maxSamples,
			Exemplars:      *exemplars,
			HealthMetrics:  *healthMetrics,
			DisableTargets: !*metricsOn,
			Kubelet: promscrape.KubeletConfig{
				Endpoint:       *kubeletEndpoint,
				Cadvisor:       *cadvisorOn,
				DisableRollups: !*rollupsOn,
				NodeMetrics:    *nodeOn,
				TokenFile:      *kubeletToken,
				InsecureTLS:    *kubeletInsecure,
				Meta:           p.meta,
			},
			Attrs:            p.attrBuilders,
			NodeInfo:         p.nodeInfo,
			Filters:          p.filters,
			Splitters:        p.splitters,
			Logger:           p.log,
			Targets:          p.meta,
			Auth:             p.meta,
			NativeHistograms: *nativeHists,
			Exporter:         p.out,
			StartTime:        time.Now(),
		})
		p.spawn(func() {
			sc.Run(p.ctx)
		})
		p.log.Info("prometheus scraper started", "node", *nodeName, "interval", *scrapeInterval,
			"targets", *metricsOn, "cadvisor", kubeletScrapes && *cadvisorOn, "nodeMetrics", kubeletScrapes && *nodeOn)
		sc0 = sc
	}
	return sc0
}

// startDebugServer serves /healthz, /readyz and the /debug endpoints on
// -listen, shutting down on ctx cancel.
func (p *pipelines) startDebugServer(tl *tailer.Tailer, sc *promscrape.Scraper) {
	if *listen == "" {
		return
	}
	mux := http.NewServeMux()
	ok := func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
	mux.HandleFunc("GET /healthz", ok)
	// Readiness is NOT liveness: a rolling update advances on this, so it
	// reports whether the agent can actually do its job. The pending gates are
	// in the body, so a stuck rollout is diagnosable from the probe alone.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if pending := p.ready.pending(); len(pending) > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprintf(w, "not ready: %s\n", strings.Join(pending, ", "))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	if tl != nil {
		// Per-file tail positions and lag (refreshed ~10s), largest lag first.
		mux.HandleFunc("GET /debug/tailer", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			enc := json.NewEncoder(w)
			enc.SetIndent("", "  ")
			_ = enc.Encode(tl.Status())
		})
	}
	if sc != nil {
		// The last scrape cycle's per-target outcomes, failures first: which
		// targets were discovered, which are down and why.
		mux.HandleFunc("GET /debug/targets", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			enc := json.NewEncoder(w)
			enc.SetIndent("", "  ")
			_ = enc.Encode(sc.Status())
		})
	}
	if p.transforms != nil {
		// The active transform program's content hash: which nodes have
		// converged after a reload.
		mux.HandleFunc("GET /debug/transforms", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"hash": p.transforms.Active().Hash})
		})
	}
	// Every handler here answers from an in-memory snapshot in
	// milliseconds, so tight timeouts are safe: ReadHeaderTimeout kills
	// Slowloris header trickling, Read/WriteTimeout bound trickled bodies
	// and stuck response writes, IdleTimeout reaps parked keep-alives.
	srv := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			p.log.Error("health/metrics server failed", "error", err)
		}
	}()
	go func() {
		<-p.ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	p.log.Info("health/metrics server started", "addr", *listen)
}

// startNodeInfo provides the node's labels/annotations for attribute
// templates, refreshed in the background from the metadata service. The name
// is known without the lookup, so the provider never yields nil; a refresh of
// 0 disables the lookup and leaves it at the bare name.
//
// It shares selfmeta.Poll with the self-pod lookup (same shape: resolve in the
// background, retry until the first success, then refresh, keep the last good
// value on a failure). The retries are why the readiness gate below clears
// seconds after the metadata service becomes reachable rather than up to a
// -node-metadata-refresh later — a rolling update advances on that gate.
func startNodeInfo(ctx context.Context, meta *metaclient.Client, nodeName string, refresh time.Duration, log *slog.Logger, ready *readiness) func() *attrs.NodeInfo {
	resolve := func(ctx context.Context) (*attrs.NodeInfo, error) {
		md, err := meta.Node(ctx, nodeName)
		if err != nil {
			return nil, err
		}
		return &attrs.NodeInfo{Name: nodeName, Labels: md.Labels, Annotations: md.Annotations}, nil
	}
	return selfmeta.Poll(ctx, resolve, selfmeta.PollConfig[attrs.NodeInfo]{
		Refresh: refresh,
		Initial: &attrs.NodeInfo{Name: nodeName},
		// The agent can reach the metadata service, so it can attribute what
		// it collects: the readiness gate a rolling update waits on.
		OnFirst: func(*attrs.NodeInfo) { ready.done(gateMetadata) },
		Log:     log,
	})
}

// buildAttrs assembles the per-pipeline resource-attribute builders from the
// config file and the flags; flag statics override config statics.
// selfSink picks the export chain for the metrics the agent generates about
// ITSELF: `out` normally, and the PRE-ROUTING chain when routing is enabled.
//
// The router fans out by the k8s.namespace.name on the resource, and these
// resources only acquired one when self-attributes started stamping it — so a
// route globbing the agent's own namespace would silently move the fleet's own
// health signal off the durable buffered chain onto an unbuffered per-tenant
// destination, and would do it only from the moment the lookup resolved.
// Transforms still apply: the fork shares the reloaded program, so the two
// chains can never run different scripts.
func selfSink(out, preRoute otlpexport.Exporter, routed bool, transforms *transform.Wrapper) selfmeta.Exporter {
	if !routed {
		return out
	}
	if transforms != nil {
		return transforms.Fork(preRoute, nil)
	}
	return preRoute
}

// selfDescribing reports whether this process exports metrics ABOUT ITSELF, so
// there is a resource worth resolving its own pod for. Self-metrics and span
// metrics are the two producers that carry agentSelfResource; everything else
// describes some other object.
func selfDescribing() bool {
	return *selfMetricsIntv > 0 || (*spanMetrics && *ingestOn && *ingestTraces)
}

// buildAttrs compiles the resource-attribute builders from the config's
// resourceAttributes section. The former -resource-attrs-static/-enable/
// -disable flags are gone: static duplicated resourceAttributes.static
// verbatim (it was merged into the very same field), and enable/disable were
// the only attribute knobs NOT in the config section, so they could not vary
// with the rest of it and their comma-separated form could not express a
// pattern containing a comma.
func buildAttrs(cfg *attrs.Config) (*attrs.Builders, error) {
	var enable, disable []string
	if cfg != nil {
		enable, disable = cfg.Enable, cfg.Disable
	}
	filter, err := attrs.NewFilterFromLists(enable, disable)
	if err != nil {
		return nil, err
	}
	return attrs.NewBuilders(cfg, filter)
}

// newLogger builds the slog logger from the -log-level/-log-format flags.
func newLogger(level, format string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("log level %q: %w", level, err)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	switch format {
	case "text":
		return slog.New(slog.NewTextHandler(os.Stderr, opts)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stderr, opts)), nil
	default:
		return nil, fmt.Errorf("log format %q (want text or json)", format)
	}
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
