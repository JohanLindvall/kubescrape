package main

// Every command-line flag the agent registers (plus the blocks shared with the
// metadata service through internal/cli). docs/FLAGS.md is generated from this
// set (flagsdoc_test.go).

import (
	"flag"
	"os"
	"strings"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/cgroupstats"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpingest"
	"github.com/JohanLindvall/kubescrape/internal/cli"
	"github.com/JohanLindvall/kubescrape/internal/selfmeta"
)

// The agent's flag surface. Package-level so the per-pipeline start
// functions can read them directly; main parses.
var (
	configFile = flag.String("config", "", "unified YAML config file; sections: "+configSections()+" (docs/CONFIGURATION.md)")
	nodeName   = flag.String("node-name", os.Getenv("NODE_NAME"), "name of the node this agent runs on (default $NODE_NAME)")
	listen     = flag.String("listen", ":8081", "HTTP listen address for /healthz, /readyz, the /debug homepage and the debug surfaces it links — /debug/tailer, /debug/targets, /debug/transforms and the live OTLP stream /debug/otlp (+ /debug/otlp/ui). Reachable from every pod in the cluster, so the data-bearing three are gated: see -debug-token-file. Empty disables all of it, /readyz included")
	// The data-bearing half of that port — /debug/otlp, its UI and
	// /debug/tailer — is not open: see debugauth.go for why a DaemonSet's tap
	// is a different exposure from a collector's.
	debugToken = flag.String("debug-token-file", "", "bearer token file gating the DATA-BEARING debug surfaces on -listen (/debug/otlp, /debug/otlp/ui, /debug/tailer), re-read periodically with the previous value accepted for a grace window so rotating the Secret needs no restart. WITHOUT it those three are served ONLY to a local connection — `kubectl port-forward` (the kubelet dials 127.0.0.1 inside the pod, so port-forward IS the loopback address), a container in this same pod, or, on an agent deliberately put on hostNetwork, the node itself — because /debug/otlp streams verbatim every log record, metric and span this process exports and the port is reachable from every pod in the cluster. A local connection must ALSO carry a loopback Host header (localhost/127.0.0.1/::1), which every direct client sends and a DNS-rebound browser page aimed at a port-forward does not. Set this to read them from anywhere else with `Authorization: Bearer <token>`. /healthz, /readyz, /debug, /debug/targets and /debug/transforms are never gated (probes, and state the metadata service already serves unauthenticated)")

	// The process-observability block (metrics/pprof listeners, self-metrics
	// cadence, logger) is registered through internal/cli, SHARED with the
	// metadata service: one registration, so defaults and help text cannot
	// drift between the binaries again. The two parameters are the hints that
	// genuinely differ per binary.
	obsFlags        = cli.RegisterObsFlags(flag.CommandLine, "agent", "the debug/health surface")
	metricsListen   = obsFlags.MetricsListen
	pprofListen     = obsFlags.PprofListen
	selfMetricsIntv = obsFlags.SelfMetricsInterval
	logLevel        = obsFlags.LogLevel

	metadataURL     = flag.String("metadata-endpoint", "http://kubescrape.monitoring", "base URL of the kubescrape metadata service")
	metadataWait    = flag.Duration("metadata-wait", 5*time.Second, "how long the metadata service may block waiting for a new container")
	scrapeAuthToken = flag.String("scrape-auth-token-file", "", "bearer token file for the metadata service's /v1/scrape-auth endpoint (re-read periodically); required when the service runs -scrape-auth-secrets")

	// The shared -otlp-* registration (internal/cli); the retry and
	// split-size knobs below stay agent-only, and only the endpoint help is
	// this binary's own.
	otlpFlags        = cli.RegisterOTLPFlags(flag.CommandLine, "OTLP endpoint: host:port for grpc, base URL for http")
	otlpEndpoint     = otlpFlags.Endpoint
	otlpProtocol     = otlpFlags.Protocol
	otlpInsecure     = otlpFlags.Insecure
	otlpSkipTLS      = otlpFlags.InsecureSkipVerify
	otlpCAFile       = otlpFlags.CAFile
	otlpBearer       = otlpFlags.BearerTokenFile
	otlpRetries      = flag.Int("otlp-retry-attempts", 3, "tries per metrics export (logs retry via the tailer's rewind)")
	otlpBackoff      = flag.Duration("otlp-retry-backoff", time.Second, "initial backoff between metric export retries, doubled per attempt up to 30s")
	otlpMaxSendBytes = flag.Int("otlp-max-send-bytes", 0, "cap on one exported payload's encoded protobuf size; a larger payload is split into parts before sending (0 = default ~3.75 MiB, under the 4 MiB gRPC limit; negative disables)")

	transformsFile = flag.String("transforms-file", "", "Starlark transforms file applied to exported logs/metrics/traces at the exporter seam; hot-reloaded on change (mount its ConfigMap as a directory, not subPath). Empty disables")

	nativeHists = flag.Bool("scrape-native-histograms", false, "offer the Prometheus protobuf exposition to scrape targets and convert native histograms to OTLP exponential histograms")
	checkConfig = flag.Bool("check-config", false, "validate -config and -transforms-file (every section compiled: templates, regexes, selectors, globs) plus the flags, print a summary and exit — no listeners, log files, positions file, spools or network. For CI and pre-rollout checks: a DaemonSet's bad ConfigMap otherwise surfaces as a fleet-wide CrashLoop")
	testConfig  = flag.String("test-config", "", "run the YAML test cases in this file through the compiled log pipeline (scrub → logAttributes → enrich → logMetrics → logs.rules → transforms) and exit non-zero on failure — CI proof of what a rule/scrub/transform edit does to sample lines, with nothing acquired (like -check-config)")

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
	excludeNs         = flag.String("logs-exclude-namespaces", "", "comma-separated namespaces whose container logs are not tailed (exact names, not globs; a log source's namespaces/excludeNamespaces take globs)")
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
	eventsBatch     = flag.Int("events-batch-size", 512, "flush events after this many, clamped to the retained-batch cap: the startup backlog walk blocks the reader goroutine and services no ticker, so a value above the cap would make the count trigger unreachable and shed the whole backlog")
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
	azureNamespace = flag.String("azure-eventhub-namespace", "", "comma-separated Event Hubs namespace hosts (myns.servicebus.windows.net), each consumed by its own client; derived from the connection strings' Endpoint when -azure-eventhub-connection-string-file is set, where at most one may be given as an override")
	azureTopics    = flag.String("azure-eventhub-topics", "", "comma-separated event hubs to consume; empty consumes the hub named by an entity-scoped connection string's EntityPath, else every hub matching ^insights-.* (the names diagnostic settings create by default)")
	azureGroup     = flag.String("azure-eventhub-group", "$Default", "Kafka consumer group; its committed offsets ARE the resume position, shared across restarts and replicas")
	azureConnFile  = flag.String("azure-eventhub-connection-string-file", "", "comma-separated files, each holding one Event Hubs connection string, namespace- or entity-scoped (SASL PLAIN; re-read per connection, so rotation needs no restart). One CLIENT per file — a connection authenticates with exactly one credential, so entity-scoped strings need one each. Empty authenticates with managed identity (OAUTHBEARER via AKS workload identity when its env is present, else IMDS)")
	azureClientID  = flag.String("azure-client-id", "", "user-assigned managed identity / workload identity client id (default $AZURE_CLIENT_ID)")
	azureTenantID  = flag.String("azure-tenant-id", "", "Microsoft Entra tenant for workload identity (default $AZURE_TENANT_ID)")
	azureStart     = flag.String("azure-start", "end", "where a consumer group with NO committed offsets starts: end (skip the backlog) or start (replay everything the hubs retain)")
	azurePrefix    = flag.String("azure-metric-prefix", "azure.", "prefix for converted Azure metric names (<prefix><metricname>.<aggregation>)")

	scrapeInterval    = flag.Duration("scrape-interval", 30*time.Second, "Prometheus scrape interval")
	scrapeTimeout     = flag.Duration("scrape-timeout", 15*time.Second, "per-target scrape timeout")
	scrapeConcurrency = flag.Int("scrape-concurrency", 4, "concurrent target scrapes")
	metricsBatch      = flag.Int("metrics-batch-size", 10000, "export metrics in chunks of this many data points")
	metricsBatchBytes = flag.Int("metrics-batch-bytes", 3<<20, "also flush a metrics chunk once its estimated encoded size reaches this many bytes (0 = the 3 MiB default; NEGATIVE disables the byte bound, leaving only -metrics-batch-size). The collector's gRPC receive limit applies to the DECOMPRESSED message (4 MiB by default), and a label-rich target can exceed it well before the point limit — every export of that target would then fail")
	maxSamples        = flag.Int("scrape-max-samples", 0, "abort a single scrape beyond this many samples (0 = unlimited)")
	exemplars         = flag.Bool("scrape-exemplars", false, "negotiate OpenMetrics and attach exemplars to counter and histogram data points")
	healthMetrics     = flag.Bool("scrape-health-metrics", true, "export synthetic up/scrape_duration_seconds/scrape_samples_scraped gauges per target")

	kubeletEndpoint = flag.String("kubelet-endpoint", "", "kubelet base URL, e.g. https://$(NODE_IP):10250 (empty disables the cadvisor, node-metrics and stats-summary scrapes)")
	kubeletToken    = flag.String("kubelet-token-file", "/var/run/secrets/kubernetes.io/serviceaccount/token", "bearer token file for the kubelet (re-read at most once a minute; a failed re-read keeps the last good token)")
	kubeletInsecure = flag.Bool("kubelet-insecure-tls", true, "skip TLS verification for the kubelet (its serving certificate is typically self-signed)")

	nodeRefresh      = flag.Duration("node-metadata-refresh", time.Minute, "refresh interval for the node's labels/annotations used in attribute templates (0 disables the lookup)")
	selfAttrsOn      = flag.Bool("self-attributes", true, "add THIS pod's Kubernetes resource attributes (namespace, pod, uid, owners, labels, plus the resourceAttributes section's static/template attributes for the `self` pipeline) to the metrics the agent generates about itself — its self-metrics, span metrics and service-graph edge metrics. Resolved from the metadata service's GET /v1/self, which attributes the request by its source address. Attributes the agent already set (service.name, service.instance.id, ...) are never overwritten; a caller the service cannot attribute to a live pod (hostNetwork, an address-rewriting hop) simply gets none. kubescrape_self_metadata_resolved reports whether it resolved")
	selfAttrsRefresh = flag.Duration("self-attributes-refresh", selfmeta.DefaultRefresh, "how often to re-read this pod's own metadata, so an edited pod or namespace label reaches the metrics it stamps (0 disables the lookup entirely, as -node-metadata-refresh=0 does for the node's). Cheap by construction: GET /v1/self carries `private, max-age` + ETag, so the client serves a fresh entry locally and revalidates a stale one as a conditional GET — a 304 whenever nothing changed. Retries before the first success start at 5s and back off to this")

	// Pipeline toggles.
	logsOn     = flag.Bool("logs", true, "tail container logs")
	metricsOn  = flag.Bool("metrics", true, "scrape annotation-discovered pod/service targets")
	cadvisorOn = flag.Bool("cadvisor", true, "scrape <kubelet-endpoint>/metrics/cadvisor (per-container metrics)")
	rollupsOn  = flag.Bool("cadvisor-rollups", true, "include cadvisor rollup series: cgroups above pod level and pod-level rows of container-scoped families")
	nodeOn     = flag.Bool("node-metrics", true, "scrape <kubelet-endpoint>/metrics (kubelet/node metrics)")

	// The third kubelet scrape, and the only one whose payload is JSON. Off by
	// default because of RBAC rather than cost: the kubelet authorizes /stats/*
	// against the nodes/stats subresource, which the agent's ClusterRole did
	// not hold until this feature shipped. On by default, a binary that rolled
	// ahead of its RBAC — the normal order for deploy/*.yaml, a hand-managed
	// ClusterRole, and any GitOps setup that applies RBAC separately — would
	// 403 on every node in the fleet, every scrape interval, forever. Same
	// reasoning as -cgroup-stats, which needs a host mount the operator has to
	// grant: a pipeline whose prerequisite is outside the binary starts off.
	//
	// The help below enumerates what this endpoint has that /metrics/cadvisor
	// does not AND what it merely restates, because the two lists are both
	// non-empty and only the first is a reason to turn the scrape on. cadvisor
	// does carry filesystem families (container_fs_usage_bytes and its
	// _limit_bytes/_inodes_free/_inodes_total siblings, keyed by device); what
	// it has no concept of is the kubelet's ephemeral-storage accounting, a
	// volume of any kind, an inodes-USED number, and which node filesystem is
	// nodefs, imagefs or containerfs.
	summaryOn = flag.Bool("kubelet-summary", false, "scrape <kubelet-endpoint>/stats/summary, the kubelet's JSON stats report, as per-pod, per-container, per-volume and per-node filesystem, ephemeral-storage and process gauges. WHAT ONLY THIS ENDPOINT HAS: per-pod ephemeral-storage USAGE (its containers' writable layers plus their logs plus their on-disk emptyDirs — the quantity the eviction manager and limits[\"ephemeral-storage\"] are measured against, reported by neither cadvisor nor kube-state-metrics), per-container LOG bytes, every volume the kubelet can measure attributed to the POD that mounts it (emptyDir, configMap, secret and the projected token included), and inodes-USED at every level, which cadvisor has no metric for anywhere. WHAT OVERLAPS, because /metrics/cadvisor is not silent about filesystems — it carries container_fs_usage_bytes, container_fs_limit_bytes, container_fs_inodes_free and container_fs_inodes_total, keyed by DEVICE: the eighteen k8s.node.{filesystem,imagefs,containerfs}.* restate cadvisor's root-cgroup (id=\"/\") rows, adding the nodefs/imagefs/containerfs ROLE that the eviction thresholds are written against and that a device name cannot give you; and on a PVC-backed volume the six k8s.volume.* restate kubelet_volume_stats_* from the -node-metrics scrape, adding the pod attribution those lack (they are labelled by namespace and PVC only). WHAT LOOKS LIKE AN OVERLAP AND IS NOT, measured on a live node: k8s.pod.process.count is the kubelet's own per-pod figure, not a pre-summed container_processes (one pod read 0 on its pod-cgroup row, 1 on its sandbox row and its own count on the app container), and it survives -cadvisor-rollups=false; k8s.container.ephemeral_storage.usage{fs.type=rootfs} has no cadvisor counterpart on a modern node, where container_fs_usage_bytes is emitted only for id=\"/\", keyed by device. cpu, memory, network and swap are left to the cadvisor scrape entirely. Each statistic lands on the resource for the object it DESCRIBES, built by the same code a cadvisor row goes through, so the series join cadvisor's for the same container.id; a volume's stats ride its pod's resource with the volume named on the data point. OFF by default because of RBAC, not cost: the kubelet authorizes /stats/* against the nodes/stats subresource while /metrics and /metrics/cadvisor go through nodes/metrics, so a binary that rolls ahead of its ClusterRole 403s on every node in the fleet. Grant the agent's ClusterRole a rule with apiGroups [\"\"], resources [\"nodes/stats\"] and verbs [\"get\"] — the shipped manifests and the chart do, unconditionally — before enabling this. Measured on a synthetic 110-pod node with two containers and two measured volumes each: 2550 data points per scrape, of which 1320 (just over half) are volume series at six per measured volume, 880 container, 330 pod and 20 node. The lever is a drop rule under the config's metrics.pipelines.summary, which sees the data-point attributes (k8s.volume.name, fs.type) as labels; this flag is all or nothing")

	// The high-frequency cgroup sampler. Off by default: it needs a host mount
	// the other pipelines do not, and it adds a metric family per container.
	cgroupStatsOn = flag.Bool("cgroup-stats", false, "sample container cgroups directly every -cgroup-stats-interval and export the DISTRIBUTION (stddev/max/min/mean plus the sample count, for the CPU rate and the memory working set) of each -scrape-interval window. The cadvisor scrape reports one average per window, so a container spiking to 4 cores for 2s inside a 60s window reads as ~0.13 cores; this recovers that at ten gauges per container instead of 60x the raw series. Requires the host's /sys/fs/cgroup mounted read-only (the shipped DaemonSet and the chart do so behind this flag) and cgroup v2 — a v1 node logs an error naming the version and this flag, disables this pipeline alone and keeps the others running, rather than misreading v1's nanosecond CPU counters or CrashLooping the node's log shipping over a metric. Only containers the metadata service can place are exported (a series with no service.name joins nothing), which is also what keeps each pod's sandbox cgroup out. Measured against 200 real cgroup v2 scopes, sampler-on vs sampler-off in separate processes, eight pairs: 0.48% of one core (0.43-0.51%) and +5.5 MiB RSS (+4.7 to +6.3) — the sampler's own cost, excluding the metadata lookups the -cadvisor pipeline's one-minute cache already pays for. The memory is the per-window export (pdata build, proto marshal, gzip) rather than retention — the sampler holds ~1.5 KiB per container — and it is a floor: a 300s window reads +7.2 MiB with the same retained heap")
	cgroupStatsIv = flag.Duration("cgroup-stats-interval", cgroupstats.DefaultInterval, "sampling period for -cgroup-stats. Shorter catches shorter bursts and costs three cgroup file reads per container per period; it must be well below -scrape-interval, which is the window the distribution describes, and at least 100ms")
	// The blind spot this flag exists for is stated in the help, because it is
	// not visible anywhere else: a container that starts and exits between two
	// passes is never sampled and leaves nothing behind to count.
	cgroupDiscoverIv = flag.Duration("cgroup-stats-discover-interval", cgroupstats.DefaultDiscoverInterval, "how often -cgroup-stats re-reads the container set from the cgroup hierarchy. Discovery is the ONLY way into the sampled set, so a container that starts and exits between two passes is never sampled and leaves NO trace that it existed — measured at the 15s default (6000 trials per lifetime, the container's start a continuous random phase), the share of containers with at least one exported window is 0% at a 2s lifetime, 20% at 5s, 53% at 10s, 87% at 15s and 100% from 17s up — one discovery period plus the two sampling periods a distribution needs (cadvisor's housekeeping has the same blind spot for the same reason). Lower it to catch init containers, CronJob pods and crashloops, at the price of a directory walk plus one metadata lookup for every cgroup that has not resolved yet — which for the first three minutes of a pod's life includes its sandbox cgroup, one per pod, permanently unresolvable. One CORNER of the loss is countable: kubescrape_cgroup_windows_dropped_total{reason=\"too_short\"} counts a container the sampler had descriptors open on that vanished before two readings, which is 13% of containers at every lifetime up to 15s — two sampling periods out of each discovery period, real evidence that a shorter interval would recover something, and a weak lower bound on how much")
	cgroupRoot       = flag.String("cgroup-stats-root", "", "cgroup v2 mount point for -cgroup-stats (empty autodetects /sys/fs/cgroup). Only the MOUNT POINT: the layout beneath it is discovered, so kind's kubelet.slice nesting, a stock systemd node's kubepods.slice and a cgroupfs-driver node's kubepods all work unconfigured")

	// OTLP ingest (apps push telemetry to the local agent for enrichment).
	// LOGS AND METRICS ONLY: traces are received by the -service-graph tier,
	// which is the only place that can hold a whole trace (see startServiceGraph).
	ingestOn      = flag.Bool("ingest", false, "receive pushed OTLP logs and metrics and enrich them with k8s attributes before forwarding. Traces go to the -service-graph tier instead: pairing an edge and (later) sampling a trace need every span of that trace in one process, which a per-node receiver can never have")
	ingestGRPC    = flag.String("ingest-grpc-endpoint", ":4317", "listen address for pushed OTLP/gRPC (empty disables)")
	ingestHTTP    = flag.String("ingest-http-endpoint", ":4318", "listen address for pushed OTLP/HTTP protobuf on /v1/logs and /v1/metrics (empty disables)")
	ingestWait    = flag.Duration("ingest-metadata-wait", 0, "how long an ingest metadata lookup may block for not-yet-known objects")
	ingestMetrics = flag.String("ingest-metrics-mode", "auto", "how pushed metrics resolve their object: resource (id on the resource), datapoint (id on each point, split into per-object resources), or auto")
	// Defaults BUILT from the enricher's own (otlpingest.Default*Keys), so the
	// flag and the package cannot state them differently.
	ingestCidKeys = flag.String("ingest-container-id-keys", strings.Join(otlpingest.DefaultContainerIDKeys, ","), "comma-separated attribute keys inspected for a container id")
	ingestUIDKeys = flag.String("ingest-pod-uid-keys", strings.Join(otlpingest.DefaultPodUIDKeys, ","), "comma-separated attribute keys inspected for a pod uid")
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
	// The per-message gRPC cap is a real memory grant on an unauthenticated
	// listener (the tap reserves exactly this much per push), so raising it is
	// an operator's deliberate trade — but it must BE an operator's: senders
	// migrating from a collector whose max_recv_msg_size was raised (a common
	// Alloy tweak) otherwise hit a rebuild-only wall. Applies to the agent's
	// -ingest listeners and the trace tier's application ports alike.
	ingestGRPCMaxRecv = flag.Int("ingest-grpc-max-recv-bytes", 0, "cap on one decoded OTLP/gRPC message on the ingest listeners (and the trace tier's application ports); an over-cap push is refused, not truncated. 0 uses gRPC's own default (4 MiB); the OTLP/HTTP body cap stays 16 MiB")

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
