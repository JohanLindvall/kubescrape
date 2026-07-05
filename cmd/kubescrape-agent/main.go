// Command kubescrape-agent runs on every node (DaemonSet). It tails
// containerd container logs and scrapes the node's Prometheus targets
// (discovered through the kubescrape metadata service), exporting both as
// OTLP over gRPC to an OpenTelemetry collector, enriched with Kubernetes
// resource attributes from the metadata service.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/metaclient"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/promscrape"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailer"
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
		metadataURL  = flag.String("metadata-endpoint", "http://kubescrape.monitoring", "base URL of the kubescrape metadata service")
		metadataWait = flag.Duration("metadata-wait", 5*time.Second, "how long the metadata service may block waiting for a new container")
		otlpEndpoint = flag.String("otlp-endpoint", "otel-collector.monitoring:4317", "OTLP gRPC endpoint of the OpenTelemetry collector")
		otlpInsecure = flag.Bool("otlp-insecure", true, "use a plaintext OTLP connection")
		otlpTimeout  = flag.Duration("otlp-timeout", 15*time.Second, "per-export timeout")

		logDir         = flag.String("log-dir", "/var/log/containers", "directory of containerd log symlinks")
		checkpointFile = flag.String("checkpoint-file", "", "file persisting log read offsets across restarts (empty disables)")
		logsBatch      = flag.Int("logs-batch-size", 1024, "flush logs after this many entries")
		logsFlush      = flag.Duration("logs-flush-interval", 2*time.Second, "flush logs at least this often")
		maxEntryBytes  = flag.Int("logs-max-entry-bytes", 1<<20, "truncate assembled log entries beyond this size")
		multilineOn    = flag.Bool("logs-multiline", true, "join application-level multi-line entries (stack traces, ...)")
		multilineWait  = flag.Duration("logs-multiline-timeout", time.Second, "flush incomplete multi-line groups after this long")
		excludeNs      = flag.String("logs-exclude-namespaces", "", "comma-separated namespaces whose container logs are not tailed")

		scrapeInterval    = flag.Duration("scrape-interval", 30*time.Second, "Prometheus scrape interval")
		scrapeTimeout     = flag.Duration("scrape-timeout", 15*time.Second, "per-target scrape timeout")
		scrapeConcurrency = flag.Int("scrape-concurrency", 4, "concurrent target scrapes")
		metricsBatch      = flag.Int("metrics-batch-size", 10000, "export metrics in chunks of this many data points")
		maxSamples        = flag.Int("scrape-max-samples", 0, "abort a single scrape beyond this many samples (0 = unlimited)")
		exemplars         = flag.Bool("scrape-exemplars", false, "negotiate OpenMetrics and attach exemplars to counter and histogram data points")

		kubeletEndpoint = flag.String("kubelet-endpoint", "", "kubelet base URL, e.g. https://$(NODE_IP):10250 (empty disables the cadvisor and node-metrics scrapes)")
		kubeletToken    = flag.String("kubelet-token-file", "/var/run/secrets/kubernetes.io/serviceaccount/token", "bearer token file for the kubelet (re-read per scrape)")
		kubeletInsecure = flag.Bool("kubelet-insecure-tls", true, "skip TLS verification for the kubelet (its serving certificate is typically self-signed)")

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

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(log)

	// The metadata client's HTTP timeout must exceed the server-side wait.
	meta := metaclient.New(*metadataURL, *metadataWait+10*time.Second)

	exporter, err := otlpexport.New(*otlpEndpoint, *otlpInsecure, *otlpTimeout)
	if err != nil {
		return fmt.Errorf("creating OTLP exporter: %w", err)
	}
	defer func() { _ = exporter.Close() }()

	var wg sync.WaitGroup

	if *logsOn {
		tl := tailer.New(tailer.Config{
			Dir:               *logDir,
			CheckpointFile:    *checkpointFile,
			FlushInterval:     *logsFlush,
			BatchSize:         *logsBatch,
			MaxEntryBytes:     *maxEntryBytes,
			Multiline:         *multilineOn,
			MultilineTimeout:  *multilineWait,
			ExcludeNamespaces: splitList(*excludeNs),
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

	<-ctx.Done()
	log.Info("shutting down")
	wg.Wait()
	return nil
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
