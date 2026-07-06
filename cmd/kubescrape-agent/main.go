// Command kubescrape-agent runs on every node (DaemonSet). It tails
// containerd container logs and scrapes the node's Prometheus targets
// (discovered through the kubescrape metadata service), exporting both as
// OTLP over gRPC to an OpenTelemetry collector, enriched with Kubernetes
// resource attributes from the metadata service.
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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/journald"
	"github.com/JohanLindvall/kubescrape/internal/agent/metaclient"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/promscrape"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailer"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

func main() {
	if err := run(); err != nil {
		slog.Error("kubescrape-agent failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		nodeName     = flag.String("node-name", os.Getenv("NODE_NAME"), "name of the node this agent runs on (default $NODE_NAME)")
		listen       = flag.String("listen", ":8081", "HTTP listen address for /healthz, /readyz and /metrics (empty disables)")
		metadataURL  = flag.String("metadata-endpoint", "http://kubescrape.monitoring", "base URL of the kubescrape metadata service")
		metadataWait = flag.Duration("metadata-wait", 5*time.Second, "how long the metadata service may block waiting for a new container")
		otlpEndpoint = flag.String("otlp-endpoint", "otel-collector.monitoring:4317", "OTLP endpoint: host:port for grpc, base URL for http")
		otlpProtocol = flag.String("otlp-protocol", "grpc", "OTLP transport: grpc or http")
		otlpInsecure = flag.Bool("otlp-insecure", true, "use a plaintext gRPC connection (for http, use an http:// endpoint)")
		otlpSkipTLS  = flag.Bool("otlp-tls-insecure-skip-verify", false, "skip TLS certificate verification towards the collector")
		otlpCAFile   = flag.String("otlp-tls-ca-file", "", "PEM CA bundle for verifying the collector")
		otlpBearer   = flag.String("otlp-bearer-token-file", "", "file with a bearer token sent on every export (re-read periodically)")
		otlpTimeout  = flag.Duration("otlp-timeout", 15*time.Second, "per-export-attempt timeout")
		otlpRetries  = flag.Int("otlp-retry-attempts", 3, "tries per metrics export (logs retry via the tailer's rewind)")
		otlpBackoff  = flag.Duration("otlp-retry-backoff", time.Second, "initial backoff between metric export retries, doubled per attempt")

		logLevel  = flag.String("log-level", "info", "log level: debug, info, warn, error")
		logFormat = flag.String("log-format", "text", "log format: text or json")

		logDir          = flag.String("log-dir", "/var/log/containers", "directory of containerd log symlinks")
		checkpointFile  = flag.String("checkpoint-file", "", "file persisting log read offsets across restarts (empty disables)")
		logsBatch       = flag.Int("logs-batch-size", 1024, "flush logs after this many entries")
		logsFlush       = flag.Duration("logs-flush-interval", 2*time.Second, "flush logs at least this often")
		maxEntryBytes   = flag.Int("logs-max-entry-bytes", 1<<20, "truncate assembled log entries beyond this size")
		multilineOn     = flag.Bool("logs-multiline", true, "join application-level multi-line entries (stack traces, ...)")
		multilineWait   = flag.Duration("logs-multiline-timeout", time.Second, "flush incomplete multi-line groups after this long")
		excludeNs       = flag.String("logs-exclude-namespaces", "", "comma-separated namespaces whose container logs are not tailed")
		logsEnrich      = flag.Bool("logs-enrich", true, "parse per-line metadata (timestamp, severity, trace/span IDs, exception details) into the OTLP record fields via github.com/JohanLindvall/enrich")
		logsWatch       = flag.Bool("logs-watch", true, "use file events (fsnotify) to trigger reads and discovery; polling remains the fallback")
		logsPoll        = flag.Duration("logs-poll-interval", 500*time.Millisecond, "fallback sweep interval for the log tailer")
		logsFingerprint = flag.Int("logs-fingerprint-bytes", 1024, "file-head hash length used with the inode as file identity (negative = inode only)")

		journaldOn     = flag.Bool("journald", false, "tail the systemd journal via journalctl (the binary must exist in the image)")
		journaldPath   = flag.String("journald-path", "journalctl", "journalctl binary")
		journaldDir    = flag.String("journald-dir", "", "journal directory (journalctl -D); empty uses the system default")
		journaldUnits  = flag.String("journald-units", "", "comma-separated systemd units to read (empty reads everything)")
		journaldCursor = flag.String("journald-cursor-file", "", "file persisting the journal cursor across restarts (empty disables; every start then begins at the tail)")
		journaldEnrich = flag.Bool("journald-enrich", true, "parse per-message metadata into the OTLP record fields (as -logs-enrich); an explicit level in the message wins over the journal priority")
		journaldBatch  = flag.Int("journald-batch-size", 1024, "flush journal entries after this many")
		journaldFlush  = flag.Duration("journald-flush-interval", 2*time.Second, "flush journal entries at least this often")

		scrapeInterval    = flag.Duration("scrape-interval", 30*time.Second, "Prometheus scrape interval")
		scrapeTimeout     = flag.Duration("scrape-timeout", 15*time.Second, "per-target scrape timeout")
		scrapeConcurrency = flag.Int("scrape-concurrency", 4, "concurrent target scrapes")
		metricsBatch      = flag.Int("metrics-batch-size", 10000, "export metrics in chunks of this many data points")
		maxSamples        = flag.Int("scrape-max-samples", 0, "abort a single scrape beyond this many samples (0 = unlimited)")
		exemplars         = flag.Bool("scrape-exemplars", false, "negotiate OpenMetrics and attach exemplars to counter and histogram data points")
		healthMetrics     = flag.Bool("scrape-health-metrics", true, "export synthetic up/scrape_duration_seconds/scrape_samples_scraped gauges per target")
		metricsConfig     = flag.String("metrics-config", "", "YAML file with per-pipeline keep/drop rules for scraped series and target splitters (empty keeps all)")

		kubeletEndpoint = flag.String("kubelet-endpoint", "", "kubelet base URL, e.g. https://$(NODE_IP):10250 (empty disables the cadvisor and node-metrics scrapes)")
		kubeletToken    = flag.String("kubelet-token-file", "/var/run/secrets/kubernetes.io/serviceaccount/token", "bearer token file for the kubelet (re-read per scrape)")
		kubeletInsecure = flag.Bool("kubelet-insecure-tls", true, "skip TLS verification for the kubelet (its serving certificate is typically self-signed)")

		attrsEnable  = flag.String("resource-attrs-enable", "", "comma-separated anchored regexes; only matching resource attributes are exported (empty enables all)")
		attrsDisable = flag.String("resource-attrs-disable", "", "comma-separated anchored regexes; matching resource attributes are dropped (empty disables none)")
		attrsStatic  = flag.String("resource-attrs-static", "", "comma-separated key=value attributes added to every exported resource")
		attrsConfig  = flag.String("resource-attrs-config", "", "YAML file declaring resource attribute building (defaults, static, template attributes, per-pipeline overrides)")
		nodeRefresh  = flag.Duration("node-metadata-refresh", time.Minute, "refresh interval for the node's labels/annotations used in attribute templates (0 disables the lookup)")

		// Pipeline toggles.
		logsOn     = flag.Bool("logs", true, "tail container logs")
		metricsOn  = flag.Bool("metrics", true, "scrape annotation-discovered pod/service targets")
		cadvisorOn = flag.Bool("cadvisor", true, "scrape <kubelet-endpoint>/metrics/cadvisor (per-container metrics)")
		rollupsOn  = flag.Bool("cadvisor-rollups", true, "include cadvisor rollup series: cgroups above pod level and pod-level rows of container-scoped families")
		nodeOn     = flag.Bool("node-metrics", true, "scrape <kubelet-endpoint>/metrics (kubelet/node metrics)")
	)
	flag.Parse()

	if *nodeName == "" {
		return fmt.Errorf("node name is required (set -node-name or $NODE_NAME)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log, err := newLogger(*logLevel, *logFormat)
	if err != nil {
		return err
	}
	slog.SetDefault(log)

	attrBuilders, err := buildAttrs(*attrsConfig, *attrsStatic, *attrsEnable, *attrsDisable)
	if err != nil {
		return fmt.Errorf("resource attributes: %w", err)
	}

	// The metadata client's HTTP timeout must exceed the server-side wait.
	meta := metaclient.New(*metadataURL, *metadataWait+10*time.Second)

	nodeInfo := startNodeInfo(ctx, meta, *nodeName, *nodeRefresh, log)

	var metricFilters *promscrape.MetricFilters
	var splitters []*promscrape.Splitter
	if *metricsConfig != "" {
		if metricFilters, splitters, err = promscrape.LoadMetricsConfig(*metricsConfig); err != nil {
			return fmt.Errorf("metrics config: %w", err)
		}
	}

	exporter, err := otlpexport.New(otlpexport.Config{
		Endpoint:           *otlpEndpoint,
		Protocol:           *otlpProtocol,
		Insecure:           *otlpInsecure,
		InsecureSkipVerify: *otlpSkipTLS,
		CAFile:             *otlpCAFile,
		BearerTokenFile:    *otlpBearer,
		Timeout:            *otlpTimeout,
		RetryAttempts:      *otlpRetries,
		RetryBackoff:       *otlpBackoff,
	})
	if err != nil {
		return fmt.Errorf("creating OTLP exporter: %w", err)
	}
	defer func() { _ = exporter.Close() }()

	var wg sync.WaitGroup

	if *logsOn {
		tl := tailer.New(tailer.Config{
			Dir:               *logDir,
			CheckpointFile:    *checkpointFile,
			Watch:             *logsWatch,
			PollInterval:      *logsPoll,
			FingerprintBytes:  *logsFingerprint,
			FlushInterval:     *logsFlush,
			BatchSize:         *logsBatch,
			MaxEntryBytes:     *maxEntryBytes,
			Multiline:         *multilineOn,
			MultilineTimeout:  *multilineWait,
			Enrich:            *logsEnrich,
			ExcludeNamespaces: splitList(*excludeNs),
			Attrs:             attrBuilders.Logs,
			NodeInfo:          nodeInfo,
			MetadataWait:      *metadataWait,
			Metadata:          meta,
			Exporter:          exporter,
			Logger:            log,
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			tl.Run(ctx)
		}()
		log.Info("log tailer started", "dir", *logDir, "checkpoint", *checkpointFile)
	}

	if *journaldOn {
		jr := journald.New(journald.Config{
			Path:          *journaldPath,
			Dir:           *journaldDir,
			Units:         splitList(*journaldUnits),
			CursorFile:    *journaldCursor,
			BatchSize:     *journaldBatch,
			FlushInterval: *journaldFlush,
			MaxEntryBytes: *maxEntryBytes,
			Enrich:        *journaldEnrich,
			Attrs:         attrBuilders.Journal,
			NodeInfo:      nodeInfo,
			Exporter:      exporter,
			Logger:        log,
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			jr.Run(ctx)
		}()
		log.Info("journald reader started", "path", *journaldPath, "units", *journaldUnits, "cursor", *journaldCursor)
	}

	kubeletScrapes := *kubeletEndpoint != "" && (*cadvisorOn || *nodeOn)
	if *metricsOn || kubeletScrapes {
		sc := promscrape.New(promscrape.Config{
			Node:           *nodeName,
			Interval:       *scrapeInterval,
			Timeout:        *scrapeTimeout,
			Concurrency:    *scrapeConcurrency,
			BatchPoints:    *metricsBatch,
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
				Meta:           meta,
			},
			Attrs:     attrBuilders,
			NodeInfo:  nodeInfo,
			Filters:   metricFilters,
			Splitters: splitters,
			Logger:    log,
			Targets:   meta,
			Exporter:  exporter,
			StartTime: time.Now(),
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			sc.Run(ctx)
		}()
		log.Info("prometheus scraper started", "node", *nodeName, "interval", *scrapeInterval,
			"targets", *metricsOn, "cadvisor", kubeletScrapes && *cadvisorOn, "nodeMetrics", kubeletScrapes && *nodeOn)
	}

	if *listen != "" {
		mux := http.NewServeMux()
		ok := func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		}
		mux.HandleFunc("GET /healthz", ok)
		mux.HandleFunc("GET /readyz", ok)
		mux.Handle("GET /metrics", obs.Handler())
		srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("health/metrics server failed", "error", err)
			}
		}()
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
		}()
		log.Info("health/metrics server started", "addr", *listen)
	}

	<-ctx.Done()
	log.Info("shutting down")
	wg.Wait()
	return nil
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
func buildAttrs(configPath, static, enable, disable string) (*attrs.Builders, error) {
	filter, err := attrs.NewFilter(enable, disable)
	if err != nil {
		return nil, err
	}
	var cfg *attrs.Config
	if configPath != "" {
		if cfg, err = attrs.LoadConfig(configPath); err != nil {
			return nil, err
		}
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
