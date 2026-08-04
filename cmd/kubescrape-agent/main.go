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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/events"
	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpingest"
	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"github.com/JohanLindvall/kubescrape/internal/agent/promscrape"
	"github.com/JohanLindvall/kubescrape/internal/agent/route"
	"github.com/JohanLindvall/kubescrape/internal/agent/servicegraph"
	"github.com/JohanLindvall/kubescrape/internal/agent/spanmetrics"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailbuffer"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailer"
	"github.com/JohanLindvall/kubescrape/internal/agent/transform"
	"github.com/JohanLindvall/kubescrape/internal/bearer"
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
	//
	// Keyed on actually BEING the singleton — the per-node pipelines all off —
	// not merely on the flag: -events added to the DaemonSet's extraArgs (a
	// supported way to run it, and the chart's own escape hatch) flipped every
	// agent in the fleet from the stable node name to a pod name that changes
	// on every restart, resetting each node's whole cumulative history.
	if singletonRole() || shardRole() {
		a.PutStr("service.instance.id", selfPodName())
	}
	attrs.Identity(res)
	return res
}

// shardRole reports whether this process is a service-graph SHARD rather than
// a node agent: the pairing role on with every per-node pipeline off, which is
// what charts/kubescrape/templates/servicegraph.yaml renders.
//
// Its instance is its POD for the singleton's reason and one more: the tier is
// a StatefulSet of N pods that may well be scheduled onto nodes that already
// run the DaemonSet, so the node name is not even unique among the processes
// exporting under service.name=kubescrape-agent — two or three of them would
// interleave counters on one (job, instance) and flap target_info between
// identities. A StatefulSet pod name is stable across restarts, so unlike a
// Deployment's this costs no cumulative history.
func shardRole() bool {
	if !*serviceGraphOn {
		return false
	}
	return !*logsOn && !*metricsOn && !*cadvisorOn && !*nodeOn && !*journaldOn && !*ingestOn
}

// singletonRole reports whether this process is the cluster-singleton
// deployment (the events / Azure-diagnostics reader) rather than a node agent:
// a cluster-scoped pipeline is on and every per-node one is off, which is
// exactly what charts/kubescrape/templates/events.yaml renders.
func singletonRole() bool {
	if !*eventsOn && !*azureOn {
		return false
	}
	return !*logsOn && !*metricsOn && !*cadvisorOn && !*nodeOn && !*journaldOn && !*ingestOn
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
	bufferDir         = flag.String("buffer-dir", "", "directory for a disk-backed export buffer (logs, metrics, and tail-sampled traces on the -service-graph tier); a collector outage spools here instead of pinning the tailer to old offsets or dropping metrics (empty disables)")
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
	// LOGS AND METRICS ONLY: traces are received by the -service-graph tier,
	// which is the only place that can hold a whole trace (see startServiceGraph).
	ingestOn      = flag.Bool("ingest", false, "receive pushed OTLP logs and metrics and enrich them with k8s attributes before forwarding. Traces go to the -service-graph tier instead: pairing an edge and (later) sampling a trace need every span of that trace in one process, which a per-node receiver can never have")
	ingestGRPC    = flag.String("ingest-grpc-endpoint", ":4317", "listen address for pushed OTLP/gRPC (empty disables)")
	ingestHTTP    = flag.String("ingest-http-endpoint", ":4318", "listen address for pushed OTLP/HTTP protobuf on /v1/logs and /v1/metrics (empty disables)")
	ingestWait    = flag.Duration("ingest-metadata-wait", 0, "how long an ingest metadata lookup may block for not-yet-known objects")
	ingestMetrics = flag.String("ingest-metrics-mode", "auto", "how pushed metrics resolve their object: resource (id on the resource), datapoint (id on each point, split into per-object resources), or auto")
	ingestCidKeys = flag.String("ingest-container-id-keys", "container.id,k8s.container.id", "comma-separated attribute keys inspected for a container id")
	ingestUIDKeys = flag.String("ingest-pod-uid-keys", "k8s.pod.uid", "comma-separated attribute keys inspected for a pod uid")
	spanMetrics   = flag.Bool("ingest-span-metrics", false, "derive RED (calls + duration histogram) metrics from received spans, dimensioned by service.name/span.name/span.kind/status.code; exported over OTLP (tune via the traceMetrics config section). Traces are received by the -service-graph tier, so this belongs on that workload")
	spanMetricsIv = flag.Duration("ingest-span-metrics-interval", time.Minute, "export interval for span metrics")
	ingestPeerIP  = flag.Bool("ingest-peer-ip-fallback", false, "attribute pushed telemetry whose resource carries no container id / pod uid to the pod owning the connection's SOURCE address (hostNetwork senders never resolve). Only correct where that address still names the sender: a proxy, a mesh sidecar that terminates, or any NAT hop replaces it, and on the -service-graph tier a source address belonging to the tier's own workload is refused and counted (kubescrape_ingest_resources_total{outcome=\"peer_ip_rejected\"}) rather than attributed")
	// The shed is the only defence the receiver has against senders it does
	// not authenticate, and it interacts directly with -otlp-timeout: a
	// collector taking the full timeout to answer holds every slot for that
	// long, so a node with many pushers needs a higher bound and a node with
	// a slow collector needs the pressure surfaced rather than buffered.
	// Hard-coded, it was tunable only by rebuilding.
	ingestMaxInFlight = flag.Int("ingest-max-in-flight", 0, "bound on concurrently-processed pushes across both ingest transports; over it senders get a retryable refusal (429 / ResourceExhausted with RetryInfo). 0 uses the built-in default (32)")

	// The trace tier (-service-graph). Opt-in, off by default, and its own
	// StatefulSet with every per-node pipeline off: it receives the cluster's
	// OTLP traces, enriches them, re-shards them by trace id so one process
	// holds a whole trace, and from there pairs edges, derives RED metrics,
	// samples and exports. It costs a workload, one internal hop per span and a
	// new metric family — none of which an operator should pay for silently.
	serviceGraphOn         = flag.Bool("service-graph", false, "run the TRACE TIER: receive application OTLP traces, enrich them, re-shard each span by trace id onto the tier's ring, and on the owning shard pair each request's client and server halves into Grafana-Tempo-compatible edge metrics. Deploy it as its own StatefulSet (stable per-pod DNS names are what the ring addresses) with -logs=false -metrics=false -cadvisor=false -node-metrics=false -ingest=false, NOT in the DaemonSet: a request's two halves are emitted by pods on two different nodes, so per-node pairing cannot complete an edge. Tuned by the config's serviceGraph section; REQUIRES -service-graph-token-file")
	serviceGraphListen     = flag.String("service-graph-listen", ":4319", "listen address for the tier's INTERNAL OTLP/gRPC receiver: spans re-sharded by a sibling shard, behind the shared bearer token. Deliberately not the application ports below — an internal hop addressed to those would re-enrich and re-shard on every pass (empty disables)")
	serviceGraphHTTPListen = flag.String("service-graph-http-listen", "", "listen address for the tier's internal OTLP/HTTP protobuf receiver on /v1/traces (empty disables). Only needed with serviceGraphShards.protocol: http; the default internal hop is gRPC")
	serviceGraphToken      = flag.String("service-graph-token-file", "", "shared bearer token file for the tier's INTERNAL hop: the receiver accepts it (and refuses to start without it — that listener takes spans from every pod in the cluster and must not be reachable unauthenticated), the sending shard presents it. Re-read periodically, with the previous value accepted for a grace window, so rotating the Secret needs no restart and no lockstep flip. It does NOT gate the application-facing listeners, which are open by design")
	serviceGraphIv         = flag.Duration("service-graph-interval", time.Minute, "export interval for the tier's service-graph edge metrics")
	serviceGraphIngest     = flag.Bool("service-graph-ingest", true, "accept application OTLP traces on the tier (the addresses below). Off leaves only the internal receiver, which is a tier nothing can push to")
	serviceGraphIngestGRPC = flag.String("service-graph-ingest-grpc", ":4317", "listen address for application OTLP/gRPC traces on the tier (empty disables). UNAUTHENTICATED by design: every instrumented pod in the cluster is a sender, and requiring a credential from each of them is not a bargain most fleets can make. Restrict it with a NetworkPolicy if the pod network is not trusted")
	serviceGraphIngestHTTP = flag.String("service-graph-ingest-http", ":4318", "listen address for application OTLP/HTTP protobuf traces on the tier, /v1/traces (empty disables)")
	serviceGraphShards     = flag.Int("service-graph-shards", 0, "number of shards in the tier (0 or 1 = no internal hop, everything is owned locally). It MUST equal the StatefulSet's replica count and be identical on every shard: the count defines the ring, and two shards disagreeing about it route a request's two halves to two different owners, where the edge silently never forms")
	serviceGraphEndpoint   = flag.String("service-graph-endpoint", "", "the tier's governing HEADLESS Service, <statefulset>.<namespace>.svc:<port>; each shard's stable per-pod address <sts>-<ordinal>.<service>.<ns>.svc:<port> is derived from it for the internal hop. Never a ClusterIP: a load-balanced destination round-robins, which is exactly what the re-shard exists to undo. The config's serviceGraphShards section is the richer form (explicit endpoints, TLS, tokensPerShard) and WINS field by field where both are set")
	serviceGraphSelf       = flag.String("service-graph-shard-name", os.Getenv("POD_NAME"), "this shard's own name in the ring (default $POD_NAME, which for a StatefulSet pod is <sts>-<ordinal>). Spans this shard already owns are then handled in-process instead of being sent over the network to itself; a name that is not in the ring still works but doubles the tier's internal traffic, and is warned about at startup")
)

// pipelines bundles what the per-pipeline start functions share: the
// lifecycle primitives (ctx/wg/stop), the common sinks and sources, and the
// parsed config. All flag reads stay in the start functions themselves.
type pipelines struct {
	// No ctx field: the process lifetime is a PARAMETER of every start
	// function below, not a property of this bundle. stop stays because it is
	// state — the one handle a pipeline uses to end the process.
	wg   *sync.WaitGroup
	stop context.CancelFunc
	log  *slog.Logger
	out  otlpexport.Exporter
	// selfOut is `out` for metrics the agent generates about ITSELF: it fills
	// in this pod's own Kubernetes resource attributes (see selfattrs.go).
	selfOut selfmeta.Exporter
	// selfPod is the pod THIS process runs in, or nil until the lookup lands
	// (and nil for good with -self-attributes off). The trace tier compares it
	// against a peer-IP attribution to refuse one that resolved to its own
	// workload — see peerIsOurOwnWorkload.
	selfPod      func() *kubemeta.Pod
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
	// The service-graph shard's pairing processor and edge registry, published
	// by startServiceGraph for the same reason: run() sweeps and exports one
	// last time once the receiver has stopped.
	serviceGraphProc *servicegraph.Processor
	serviceGraphReg  *servicegraph.Registry
	serviceGraphRes  pcommon.Resource
	// sgResharder is the tier's internal hop (nil on a single-shard tier);
	// run() closes its per-shard clients.
	sgResharder *servicegraph.Resharder
	// tailBuffer holds spans whose trace has not been decided yet (nil unless
	// tailSampling is configured). run() flushes it once the receivers have
	// stopped: those spans were acked to their senders and nothing else holds
	// them, so the graceful path is what keeps the loss to a hard kill.
	tailBuffer *tailbuffer.Buffer
	ingestMode otlpingest.MetricsMode
	filters    *promscrape.MetricFilters
	splitters  []*promscrape.Splitter
	// fatalErr receives a pipeline's fatal failure (currently the ingest
	// listener and events leader election). ATOMIC: shutdown joins the
	// producers on a BUDGET (waitFor), not an unbounded wg.Wait, so a
	// straggler writing past the deadline would race run()'s read — a plain
	// variable's happens-before died with the budget. First writer wins; the
	// agent exits non-zero on whichever failure came first.
	fatalErr *atomic.Pointer[error]
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
	// The -azure-* flag surface, from the tagged file pair: a build without the
	// `azure` tag does not link the package that defines what the values mean
	// (see buildtags.go).
	if err := validateAzureFlags(); err != nil {
		return err
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
	// The optional pipelines ride build tags (buildtags.go), and the Makefile —
	// not the constraint — carries the default that compiles both in. So the
	// binary itself has to say which one it is: a bare `go build` produces an
	// agent with neither, and "the flag is set and nothing happens" must not be
	// a thing anyone has to discover.
	log.Info("kubescrape-agent starting", "version", obs.BuildVersion(), "built", obs.BuildTime(),
		"optionalPipelines", builtPipelines())

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
	// Legal combinations that do not mean what they read like. Emitted here, so
	// -check-config and a real start report the same list.
	logConfigWarnings(fileCfg, log)
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

	var scrapeAuthTok func() string
	if *scrapeAuthToken != "" {
		// /v1/scrape-auth returns Secret VALUES and is the one authenticated
		// endpoint. Read through a cache so the file is not hit per scrape, and
		// re-read so a rotated Secret is picked up without a restart.
		reader := bearer.NewFile(*scrapeAuthToken, log)
		// The initial read is fatal HERE and nowhere else in the client half: a
		// configured -scrape-auth-token-file that cannot be read is an operator
		// error worth failing on, while a re-read that fails mid-rotation keeps
		// serving the last good value (bearer.File).
		if _, err := reader.Read(); err != nil {
			return fmt.Errorf("reading -scrape-auth-token-file: %w", err)
		}
		scrapeAuthTok = reader.Get
	}
	// The metadata client's HTTP timeout must exceed the server-side wait —
	// including the ingest lookups' own wait, which may be longer.
	meta := metaclient.New(metaclient.Config{
		Base:    *metadataURL,
		Timeout: max(*metadataWait, *ingestWait) + 10*time.Second,
		// The client is dependency-free by design; feed its outcomes to our
		// metrics.
		Observe:         func(outcome string) { obs.MetadataRequests.WithLabelValues(outcome).Inc() },
		ScrapeAuthToken: scrapeAuthTok,
	})

	// The Prometheus scrape target for this process's own metrics, on its own
	// port (see -metrics-listen). With the OTLP self-metrics push disabled
	// (-self-metrics-interval=0) the kubescrape_* metrics ride the scrape
	// instead — one knob selects the modality, so the two paths never
	// double-deliver.
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
		// Its OWN client, deliberately without the Observe hook. This lookup
		// retries on the refresh period forever when it cannot resolve — a
		// hostNetwork pod, a NAT hop, an address family status.podIP does not
		// carry — and errors are never cached, so through the shared client it
		// added a permanent per-node not_found floor to
		// kubescrape_metadata_requests_total, burying the container-attribution
		// failures the alert on that metric exists to catch. Its outcomes are
		// counted by kubescrape_self_metadata_lookups_total instead, which is
		// what that counter was added for.
		selfMeta := metaclient.New(metaclient.Config{
			Base:    *metadataURL,
			Timeout: max(*metadataWait, *ingestWait) + 10*time.Second,
		})
		selfPod = selfmeta.StartPod(ctx, selfResolve(selfMeta), *selfAttrsRefresh, log)
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

	baseExport := baseExportConfig()
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
		// A THIRD spool, opened only where something marks its payloads as
		// owned (otlpexport/owned.go): the tail sampler on the trace tier. A
		// forwarded trace must stay pass-through — its sender holds it and
		// retries — so on every other workload this stays nil rather than
		// preallocating a segment file that never takes a record.
		var traceBuf *otlpexport.Buffer
		if *serviceGraphOn && fileCfg.TailSampling.Enabled() { // nil-receiver safe
			traceBuf, err = otlpexport.OpenBuffer(filepath.Join(*bufferDir, "traces"), int64(*bufferMax))
			if err != nil {
				return fmt.Errorf("trace buffer: %w", err)
			}
			defer func() {
				if err := traceBuf.Close(); err != nil {
					log.Warn("closing the trace buffer", "error", err)
				}
			}()
		}
		buffered := otlpexport.NewBuffered(exporter, logBuf, metricBuf, traceBuf, *otlpBackoff, log)
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
		log.Info("disk buffer enabled", "dir", *bufferDir, "max-bytes-per-signal", *bufferMax,
			"traces", traceBuf != nil)
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
			// The SAME derivation -check-config validates (routeExportConfig),
			// so a config the dry run accepts is a config that starts.
			rcfg, err := routeExportConfig(fileCfg.Export, rt)
			if err != nil {
				return err
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
		// The refused-observation counters belong to THIS set (they used to be
		// process globals); publish them now that one exists.
		obs.RegisterLogMetricsDrops(logMetrics)
		wg.Add(1)
		go func() {
			defer wg.Done()
			logMetrics.Run(ctx, out, *logsMetricsEvery, *logsMetricsBytes)
		}()
		log.Info("log-derived metrics started", "metrics", logMetrics.Count, "interval", *logsMetricsEvery)
	}

	// A fatal pipeline failure (the ingest listener, events leader election)
	// is stored here and returned after shutdown so the agent exits non-zero.
	// Atomic because the shutdown join is BUDGETED: see pipelines.fatalErr.
	var fatalErr atomic.Pointer[error]

	p := &pipelines{
		wg:           &wg,
		stop:         stop,
		log:          log,
		out:          out,
		selfOut:      selfOut,
		selfPod:      selfPod,
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
	tl, err := p.startLogs(ctx)
	if err != nil {
		return err
	}
	if err := p.startJournald(ctx); err != nil {
		return err
	}
	if err := p.startIngest(ctx); err != nil {
		return err
	}
	// The sibling-shard clients are unrelated to the collector exporter;
	// registered here (LIFO, so they close before it) once startServiceGraph has
	// built them.
	defer func() { _ = p.sgResharder.Close() }() // nil-receiver safe
	if err := p.startServiceGraph(ctx); err != nil {
		return err
	}
	if err := p.startEvents(ctx); err != nil {
		return err
	}
	if err := p.startAzure(ctx); err != nil {
		return err
	}
	sc := p.startScraper(ctx)
	p.startDebugServer(ctx, tl, sc)

	<-ctx.Done()
	log.Info("shutting down")
	// BOUNDED. Everything that salvages in-memory state runs after this: the
	// final log-metrics window (DynamicMetricSet.Run deliberately does not
	// export on cancel, so this is its only chance), the final span-metrics
	// export, the self-metrics FinalExport and the disk-buffer drain. A
	// producer stuck retrying against a dead collector would otherwise hold
	// the whole sequence past the kubelet's grace period and lose all of it to
	// SIGKILL. Producers that miss the deadline lose nothing they own: log
	// offsets, the journal cursor and the events position all re-read.
	if !waitFor(&wg, shutdownDrain) {
		log.Warn("producers did not stop within the shutdown budget; continuing with the final exports",
			"budget", shutdownDrain)
	}
	if p.tailBuffer != nil {
		// The one shutdown step that salvages ACKED data rather than a last
		// aggregation window: the tail-sampling buffer holds spans whose senders
		// were told they had landed, and nothing else holds a copy. The receivers
		// have stopped (wg above), so nothing is arriving; decide everything now
		// and let the keeps reach the exporter (and, with -buffer-dir, the final
		// drain below). Budgeted like the rest — a dead collector must not outlive
		// the pod's termination grace, and what it costs is counted as lost.
		fctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		p.tailBuffer.Flush(fctx)
		cancel()
	}
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
	if p.serviceGraphReg != nil {
		// Same argument as the span-metrics export above — Registry.Run's own
		// final export raced the receiver's GracefulStop, and every forward it
		// completed afterwards moved the cumulative edge counters — plus one
		// sweep first: the sweeper goroutine has stopped, and a half-edge whose
		// wait elapsed during the shutdown is an edge (a virtual-node one, for
		// the uninstrumented dependencies that are most of an interesting
		// graph). Half-edges NOT yet due are lost with the process, by design:
		// the pairing state is in-memory and worth no more than the wait window
		// that bounds it.
		if p.serviceGraphProc != nil {
			p.serviceGraphProc.Sweep()
		}
		fctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := p.serviceGraphReg.Export(fctx, p.selfOut, p.serviceGraphRes); err != nil {
			log.Warn("final service-graph export failed", "error", err)
		}
		cancel()
	}
	if *selfMetricsIntv > 0 {
		// Registry.Run's own final export raced the final flushes inside
		// wg.Wait; counters they bumped (last batches, shutdown drops) would
		// otherwise die unexported. One more export now that everything is done.
		// Budgeted here, like every other final export above: ctx is cancelled
		// by this point, and a dead collector must not outlive the pod's grace.
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), metrics.FinalExportTimeout)
		obs.Registry.FinalExport(fctx, selfOut, selfRes, log)
		cancel()
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
	if ferr := fatalErr.Load(); ferr != nil {
		return *ferr
	}
	return nil
}

// startLogs starts the container/plain-file log tailer. The returned Tailer
// (nil when -logs is off) is exposed on /debug/tailer.
func (p *pipelines) startLogs(ctx context.Context) (*tailer.Tailer, error) {
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
		tl.Run(ctx)
	})
	if *positionsFile == "" {
		p.log.Warn("no -positions-file: offsets are not persisted (a restart re-reads per -logs-unknown-files; journald starts at the tail)")
	}
	p.log.Info("log tailer started", "dir", *logDir, "positions", *positionsFile)
	return tl, nil
}

// startJournald and startAzure live in build-tag-gated file pairs
// (journald_enabled.go / journald_disabled.go, azure_enabled.go /
// azure_disabled.go): each pipeline is compiled in by the POSITIVE tag of its
// name, which the Makefile's TAGS sets by default. See buildtags.go.

// startEvents starts the cluster-singleton Kubernetes events reader under a
// leader election, so exactly one replica watches (N watchers would emit N
// copies of every event).
func (p *pipelines) startEvents(ctx context.Context) error {
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
		err := leader.Run(ctx, leader.Config{
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
			ferr := fmt.Errorf("events leader election: %w", err)
			p.fatalErr.CompareAndSwap(nil, &ferr) // first fatal wins
			p.stop()
		}
	})
	p.log.Info("kubernetes events enabled", "lease", *eventsLease, "namespace", ns,
		"positionConfigMap", *eventsConfigMap, "start", *eventsStart)
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

// startIngest starts the node-local OTLP ingest receiver: LOGS AND METRICS.
// A fatal listener failure is reported through p.fatalErr and p.stop so the
// agent exits non-zero.
//
// Traces are deliberately not here. Both things worth doing to a trace — pairing
// its two halves into a service-graph edge, and (in time) deciding whether to
// keep the whole trace — need every span of that trace in one process, and a
// per-node receiver holds an arbitrary subset of them by construction. So the
// trace receiver lives on the -service-graph tier, which re-shards by trace id
// until one process does hold the whole thing (startServiceGraph).
func (p *pipelines) startIngest(ctx context.Context) error {
	if !*ingestOn {
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
		NodeInfo:        p.nodeInfo,
		Meta:            p.meta,
		Logger:          p.log,
	})
	// Traces: nil, so neither the gRPC trace service nor POST /v1/traces is
	// served here. A sender pointed at the agent for traces gets Unimplemented /
	// 404 — a loud, immediate error naming the wrong destination — rather than an
	// ack for spans that could never have become an edge.
	srv := otlpingest.NewServer(otlpingest.ServerConfig{
		GRPCAddr:    *ingestGRPC,
		HTTPAddr:    *ingestHTTP,
		MaxInFlight: *ingestMaxInFlight,
		Enricher:    enr,
		Exporter:    p.out,
		Logger:      p.log,
	})
	p.spawn(func() {
		if err := srv.Run(ctx); err != nil {
			// A dead ingest listener (e.g. the port already bound) must not
			// leave the agent looking healthy while apps push into a void:
			// shut the agent down and exit non-zero so the failure is
			// visible (CrashLoop).
			p.log.Error("otlp ingest server failed; shutting down", "error", err)
			ferr := fmt.Errorf("otlp ingest server: %w", err)
			p.fatalErr.CompareAndSwap(nil, &ferr) // first fatal wins
			p.stop()
		}
	})
	p.log.Info("otlp ingest started", "grpc", *ingestGRPC, "http", *ingestHTTP, "metricsMode", *ingestMetrics)
	return nil
}

// startScraper starts the Prometheus scraper (annotation/ServiceMonitor
// targets and/or kubelet cadvisor+node scrapes). The returned Scraper (nil
// when scraping is off) is exposed on /debug/targets.
func (p *pipelines) startScraper(ctx context.Context) *promscrape.Scraper {
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
			sc.Run(ctx)
		})
		p.log.Info("prometheus scraper started", "node", *nodeName, "interval", *scrapeInterval,
			"targets", *metricsOn, "cadvisor", kubeletScrapes && *cadvisorOn, "nodeMetrics", kubeletScrapes && *nodeOn)
		sc0 = sc
	}
	return sc0
}

// startDebugServer serves /healthz, /readyz and the /debug endpoints on
// -listen, shutting down on ctx cancel.
func (p *pipelines) startDebugServer(ctx context.Context, tl *tailer.Tailer, sc *promscrape.Scraper) {
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
		<-ctx.Done()
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
// shutdownDrain bounds the wait for the producers to stop. It plus the
// tailer's own budget and the final exports has to fit inside the pod's
// terminationGracePeriodSeconds, which the manifests set explicitly.
const shutdownDrain = 15 * time.Second

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

// baseExportConfig is the exporter configuration the OTLP flags describe. It
// is a function so -check-config can validate it without building anything:
// the dry run returns long before the exporter is assembled, so every one of
// these flags used to be unchecked by a run whose whole purpose is catching a
// bad ConfigMap before it becomes a fleet-wide CrashLoop.
func baseExportConfig() otlpexport.Config {
	return otlpexport.Config{
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
}

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
// there is a resource worth resolving its own pod for. Self-metrics, span
// metrics and the service-graph shard's edge metrics are the producers that
// carry agentSelfResource; everything else describes some other object.
//
// The shard counts even though an EDGE describes two other services: the
// series are emitted under this process's identity (the two services are
// data-point labels), so the shard pod's own attributes are what tells an
// operator which shard produced them.
func selfDescribing() bool {
	return *selfMetricsIntv > 0 || *serviceGraphOn
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
