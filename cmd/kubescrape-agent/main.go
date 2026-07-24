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
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/journald"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpbatch"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpingest"
	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"github.com/JohanLindvall/kubescrape/internal/agent/promscrape"
	"github.com/JohanLindvall/kubescrape/internal/agent/spanmetrics"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailer"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
	"github.com/JohanLindvall/kubescrape/pkg/spool"
	"go.opentelemetry.io/collector/pdata/pcommon"
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
	a.PutStr("k8s.node.name", node)
	attrs.Identity(res)
	return res
}

// The agent's flag surface. Package-level so the per-pipeline start
// functions can read them directly; main parses.
var (
	configFile           = flag.String("config", "", "unified YAML config file with resourceAttributes, logs, logAttributes, logMetrics, metrics and traceMetrics sections")
	nodeName             = flag.String("node-name", os.Getenv("NODE_NAME"), "name of the node this agent runs on (default $NODE_NAME)")
	listen               = flag.String("listen", ":8081", "HTTP listen address for /healthz, /readyz, /debug/tailer and /debug/targets (empty disables)")
	selfMetricsIntv      = flag.Duration("self-metrics-interval", time.Minute, "export the agent's own metrics over OTLP at this interval (0 disables)")
	metadataURL          = flag.String("metadata-endpoint", "http://kubescrape.monitoring", "base URL of the kubescrape metadata service")
	metadataWait         = flag.Duration("metadata-wait", 5*time.Second, "how long the metadata service may block waiting for a new container")
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

	logLevel  = flag.String("log-level", "info", "log level: debug, info, warn, error")
	logFormat = flag.String("log-format", "text", "log format: text or json")

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
	logsEnrich        = flag.Bool("logs-enrich", true, "parse per-line metadata (timestamp, severity, trace/span IDs, exception details) into the OTLP record fields via github.com/JohanLindvall/enrich")
	logsFileAttrs     = flag.Bool("logs-file-attributes", false, "stamp log.file.name and log.file.position (byte offset) on every log record, for each file source")
	bufferDir         = flag.String("buffer-dir", "", "directory for a disk-backed export buffer (logs and metrics); a collector outage spools here instead of pinning the tailer to old offsets or dropping metrics (empty disables)")
	bufferMax         = flag.Int("buffer-max-bytes", 1<<30, "per-signal cap on the undelivered on-disk buffer; producers back-pressure (the tailer rewinds) when full")
	logsMetricsEvery  = flag.Duration("logs-metrics-interval", 30*time.Second, "export interval for log-derived metrics")
	logsMetricsBytes  = flag.Int("logs-metrics-max-bytes", 3<<20, "export log-derived metrics in chunks below this many bytes (0 = one payload)")
	logsMetricsPrefix = flag.String("logs-metrics-name-prefix", "", "prefix prepended to every log-derived metric name")
	logsWatch         = flag.Bool("logs-watch", true, "use file events (fsnotify) to trigger reads and discovery; polling remains the fallback")
	logsPoll          = flag.Duration("logs-poll-interval", 500*time.Millisecond, "fallback sweep interval for the log tailer")
	logsFingerprint   = flag.Int("logs-fingerprint-bytes", 1024, "file-head hash length used with the inode as file identity (negative = inode only)")

	journaldOn     = flag.Bool("journald", false, "read the systemd journal natively via libsystemd/sdjournal (the image must provide libsystemd)")
	journaldDir    = flag.String("journald-dir", "", "read a specific journal directory; empty opens the default system journal")
	journaldUnits  = flag.String("journald-units", "", "comma-separated systemd units to read (empty reads everything)")
	journaldEnrich = flag.Bool("journald-enrich", true, "parse per-message metadata into the OTLP record fields (as -logs-enrich); an explicit level in the message wins over the journal priority")
	journaldBatch  = flag.Int("journald-batch-size", 1024, "flush journal entries after this many")
	journaldBytes  = flag.Int("journald-max-batch-bytes", 1<<20, "flush journal entries before a batch's summed message bytes exceed this")
	journaldFlush  = flag.Duration("journald-flush-interval", 2*time.Second, "flush journal entries at least this often")

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

	attrsEnable  = flag.String("resource-attrs-enable", "", "comma-separated anchored regexes; only matching resource attributes are exported (empty enables all)")
	attrsDisable = flag.String("resource-attrs-disable", "", "comma-separated anchored regexes; matching resource attributes are dropped (empty disables none)")
	attrsStatic  = flag.String("resource-attrs-static", "", "comma-separated key=value attributes added to every exported resource")
	nodeRefresh  = flag.Duration("node-metadata-refresh", time.Minute, "refresh interval for the node's labels/annotations used in attribute templates (0 disables the lookup)")

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
	ingestEnrich  = flag.Bool("ingest-logs-enrich", true, "parse pushed log-record bodies for timestamp/severity/trace as -logs-enrich does, filling only fields the sender left unset")
	ingestTraces  = flag.Bool("ingest-traces", true, "accept pushed traces (gRPC + /v1/traces), enrich their resources and pass them through")
	spanMetrics   = flag.Bool("ingest-span-metrics", false, "derive RED (calls + duration histogram) metrics from ingested spans, dimensioned by service.name/span.name/span.kind/status.code; exported over OTLP (tune via the traceMetrics config section)")
	spanMetricsIv = flag.Duration("ingest-span-metrics-interval", time.Minute, "export interval for span metrics")
	ingestPeerIP  = flag.Bool("ingest-peer-ip-fallback", false, "attribute pushed telemetry whose resource carries no container id / pod uid to the pod owning the connection's peer IP (hostNetwork senders never resolve)")
	ingestBatch   = flag.Int("ingest-batch-items", 0, "coalesce pushed payloads per signal to this many items (log records / data points / spans) before forwarding; 0 forwards each request as received")
	ingestBatchTO = flag.Duration("ingest-batch-timeout", 200*time.Millisecond, "max time a partial ingest batch waits before flushing")
	ingestBatchB  = flag.Int("ingest-batch-bytes", 3<<20, "flush a coalescing ingest batch before its encoded size would exceed this many bytes (keeps merged payloads under the collector's 4 MiB gRPC recv default)")
)

// pipelines bundles what the per-pipeline start functions share: the
// lifecycle primitives (ctx/wg/stop), the common sinks and sources, and the
// parsed config. All flag reads stay in the start functions themselves.
type pipelines struct {
	ctx          context.Context
	wg           *sync.WaitGroup
	stop         context.CancelFunc
	log          *slog.Logger
	out          otlpexport.Exporter
	meta         *metaclient.Client
	nodeInfo     func() *attrs.NodeInfo
	attrBuilders *attrs.Builders
	fileCfg      agentConfig
	posStore     *positions.Store
	logAttrs     *logattrs.Extractor
	logMetrics   *metrics.DynamicMetricSet
	ingestMode   otlpingest.MetricsMode
	filters      *promscrape.MetricFilters
	splitters    []*promscrape.Splitter
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log, err := newLogger(*logLevel, *logFormat)
	if err != nil {
		return err
	}
	slog.SetDefault(log)

	// All YAML config lives in one file; each section is optional.
	var fileCfg agentConfig
	if *configFile != "" {
		c, err := loadAgentConfig(*configFile)
		if err != nil {
			return fmt.Errorf("config: %w", err)
		}
		fileCfg = *c
	}

	attrBuilders, err := buildAttrs(fileCfg.ResourceAttributes, *attrsStatic, *attrsEnable, *attrsDisable)
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

	// The metadata client's HTTP timeout must exceed the server-side wait —
	// including the ingest lookups' own wait, which may be longer.
	meta := metaclient.New(*metadataURL, max(*metadataWait, *ingestWait)+10*time.Second)
	// The client is dependency-free by design; feed its outcomes to our metrics.
	meta.Observe = func(outcome string) { obs.MetadataRequests.WithLabelValues(outcome).Inc() }

	nodeInfo := startNodeInfo(ctx, meta, *nodeName, *nodeRefresh, log)

	var metricFilters *promscrape.MetricFilters
	var splitters []*promscrape.Splitter
	if fileCfg.Metrics != nil {
		if metricFilters, err = promscrape.NewMetricFilters(&promscrape.FilterConfig{Pipelines: fileCfg.Metrics.Pipelines}); err != nil {
			return fmt.Errorf("metrics config: %w", err)
		}
		if splitters, err = promscrape.NewSplitters(fileCfg.Metrics.Splitters); err != nil {
			return fmt.Errorf("metrics config: %w", err)
		}
	}

	exporter, err := otlpexport.New(otlpexport.Config{
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
	})
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
	if *bufferDir != "" {
		logSpool, err := spool.Open(filepath.Join(*bufferDir, "logs"), spool.Options{MaxBytes: int64(*bufferMax)})
		if err != nil {
			return fmt.Errorf("log buffer: %w", err)
		}
		defer func() { _ = logSpool.Close() }()
		metricSpool, err := spool.Open(filepath.Join(*bufferDir, "metrics"), spool.Options{MaxBytes: int64(*bufferMax)})
		if err != nil {
			return fmt.Errorf("metric buffer: %w", err)
		}
		defer func() { _ = metricSpool.Close() }()
		buffered := otlpexport.NewBuffered(exporter, logSpool, metricSpool, *otlpBackoff, log)
		wg.Add(1)
		go func() {
			defer wg.Done()
			buffered.Run(ctx)
		}()
		out = buffered
		log.Info("disk buffer enabled", "dir", *bufferDir, "max-bytes-per-signal", *bufferMax)
	}

	// Registered AFTER the exporter/spool Close defers (LIFO): an early `return
	// err` below must stop and drain every started goroutine BEFORE their
	// exporter and spools are closed under them. The normal path's inline
	// wg.Wait makes this a no-op there.
	defer func() {
		stop()
		wg.Wait()
	}()

	var selfRes pcommon.Resource
	if *selfMetricsIntv > 0 {
		selfRes = agentSelfResource(*nodeName)
		wg.Add(1)
		go func() {
			defer wg.Done()
			obs.Registry.Run(ctx, out, *selfMetricsIntv, selfRes, log)
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
		meta:         meta,
		nodeInfo:     nodeInfo,
		attrBuilders: attrBuilders,
		fileCfg:      fileCfg,
		posStore:     posStore,
		logAttrs:     logAttrs,
		logMetrics:   logMetrics,
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
	if *selfMetricsIntv > 0 {
		// Registry.Run's own final export raced the final flushes inside
		// wg.Wait; counters they bumped (last batches, shutdown drops) would
		// otherwise die unexported. One more export now that everything is done.
		obs.Registry.FinalExport(out, selfRes, log)
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
	var logRules *logline.LineFilter
	if p.fileCfg.Logs != nil {
		if logSources, err = tailer.ValidateSources(p.fileCfg.Logs.Sources); err != nil {
			return nil, fmt.Errorf("logs config: %w", err)
		}
		if logRules, err = logline.NewLineFilter(p.fileCfg.Logs.Rules); err != nil {
			return nil, fmt.Errorf("logs config: %w", err)
		}
	}
	tl := tailer.New(tailer.Config{
		Dir:               *logDir,
		Sources:           logSources,
		Positions:         p.posStore,
		LogAttrs:          p.logAttrs,
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
		Enrich:            *logsEnrich,
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
		Enrich:        *journaldEnrich,
		LogAttrs:      p.logAttrs,
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

// startIngest starts the OTLP ingest receiver plus its optional batcher and
// span-metrics tap. A fatal listener failure is reported through p.fatalErr
// and p.stop so the agent exits non-zero.
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
		EnrichLines:     *ingestEnrich,
		PeerIPFallback:  *ingestPeerIP,
		Attrs:           p.attrBuilders.Ingest,
		NodeInfo:        p.nodeInfo,
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
	// The span-metrics tap wraps the RAW trace exporter, BELOW the batcher:
	// the tap aggregates only after a successful forward (a retried batch
	// must not double-count), and under the batcher it runs on the batcher's
	// own goroutine against a payload nothing else mutates — wrapping above
	// the batcher would race Consume against the batcher's merge.
	if *spanMetrics && ingestTraceOut != nil {
		var smCfg spanmetrics.Config
		if p.fileCfg.TraceMetrics != nil {
			smCfg = *p.fileCfg.TraceMetrics
		}
		gen := spanmetrics.New(smCfg)
		ingestTraceOut = gen.Tap(ingestTraceOut)
		res := agentSelfResource(*nodeName)
		p.spawn(func() {
			gen.Run(p.ctx, p.out, *spanMetricsIv, res, p.log)
		})
		p.log.Info("span metrics from traces enabled", "interval", *spanMetricsIv)
	} else if *spanMetrics {
		p.log.Warn("-ingest-span-metrics ignored: traces are disabled (-ingest-traces=false)")
	}
	// The batcher stops on its own context, cancelled only after the
	// ingest server has fully returned: a GracefulStop completing in-flight
	// RPCs may still enqueue (and ack) payloads, which the batcher's final
	// drain must see.
	batchStop := func() {}
	if *ingestBatch > 0 {
		batcher := otlpbatch.NewBatcher(ingestOut, ingestTraceOut,
			otlpbatch.BatchConfig{Items: *ingestBatch, MaxBatchBytes: *ingestBatchB, Timeout: *ingestBatchTO}, p.log)
		batchCtx, cancel := context.WithCancel(context.Background())
		batchStop = cancel
		p.spawn(func() {
			batcher.Run(batchCtx)
		})
		ingestOut = batcher
		if ingestTraceOut != nil {
			ingestTraceOut = batcher
		}
		p.log.Info("otlp ingest batching enabled", "items", *ingestBatch, "maxBytes", *ingestBatchB, "timeout", *ingestBatchTO)
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
		defer batchStop() // server fully stopped: no more enqueues
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
			Attrs:     p.attrBuilders,
			NodeInfo:  p.nodeInfo,
			Filters:   p.filters,
			Splitters: p.splitters,
			Logger:    p.log,
			Targets:   p.meta,
			Exporter:  p.out,
			StartTime: time.Now(),
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

// startDebugServer serves /healthz, /readyz, the Go-runtime /metrics and
// /debug/tailer on -listen, shutting down on ctx cancel.
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
	mux.HandleFunc("GET /readyz", ok)
	mux.Handle("GET /metrics", obs.RuntimeHandler())
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
// templates, refreshed in the background from the metadata service.
func startNodeInfo(ctx context.Context, meta *metaclient.Client, nodeName string, refresh time.Duration, log *slog.Logger) func() *attrs.NodeInfo {
	var current atomic.Pointer[attrs.NodeInfo]
	current.Store(&attrs.NodeInfo{Name: nodeName})
	if refresh > 0 {
		fetch := func() {
			fctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			md, err := meta.Node(fctx, nodeName)
			if err != nil {
				log.Debug("fetching node metadata", "node", nodeName, "error", err)
				return
			}
			current.Store(&attrs.NodeInfo{Name: nodeName, Labels: md.Labels, Annotations: md.Annotations})
		}
		go func() {
			fetch()
			ticker := time.NewTicker(refresh)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					fetch()
				}
			}
		}()
	}
	return current.Load
}

// buildAttrs assembles the per-pipeline resource-attribute builders from the
// config file and the flags; flag statics override config statics.
func buildAttrs(cfg *attrs.Config, static, enable, disable string) (*attrs.Builders, error) {
	filter, err := attrs.NewFilter(enable, disable)
	if err != nil {
		return nil, err
	}
	flagStatic, err := attrs.ParseStatic(static)
	if err != nil {
		return nil, err
	}
	if flagStatic != nil {
		if cfg == nil {
			cfg = &attrs.Config{}
		}
		if cfg.Static == nil {
			cfg.Static = map[string]string{}
		}
		for k, v := range flagStatic {
			cfg.Static[k] = v
		}
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
