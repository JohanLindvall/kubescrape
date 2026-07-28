package main

import (
	"fmt"
	"log/slog"
	"os"
	"path"
	"sort"
	"strings"
	"sync"

	"sigs.k8s.io/yaml"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/promscrape"
	"github.com/JohanLindvall/kubescrape/internal/agent/route"
	"github.com/JohanLindvall/kubescrape/internal/agent/spanmetrics"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailer"
	"github.com/JohanLindvall/kubescrape/internal/agent/tracesample"
	"github.com/JohanLindvall/kubescrape/internal/agent/transform"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

// agentConfig is the single -config YAML file. Each section mirrors the shape
// of the standalone config file it replaces, so migrating means nesting the
// former file under its section key.
type agentConfig struct {
	// ResourceAttributes builds exported resource attributes (defaults, static,
	// template attributes, per-pipeline overrides).
	ResourceAttributes *attrs.Config `json:"resourceAttributes,omitempty"`
	// Logs declares the tailer's log sources (include/exclude globs, containerd
	// vs plain, per-source attributes/encoding/compression).
	Logs *tailer.SourcesConfig `json:"logs,omitempty"`
	// LogAttributes lifts JSON/logfmt keys out of log lines onto records as
	// resource/scope/log attributes.
	LogAttributes *logattrs.Config `json:"logAttributes,omitempty"`
	// LogMetrics declares metrics derived from log lines.
	LogMetrics *metrics.DynamicConfig `json:"logMetrics,omitempty"`
	// Metrics holds per-pipeline keep/drop rules for scraped series and target
	// splitters.
	Metrics *promscrape.MetricsConfig `json:"metrics,omitempty"`
	// TraceMetrics tunes the RED metrics derived from ingested trace spans
	// (histogram buckets, extra dimensions, cardinality cap). Aggregation is
	// gated by -ingest-span-metrics; this section only tunes it.
	TraceMetrics *spanmetrics.Config `json:"traceMetrics,omitempty"`
	// Routing fans exported payloads out by namespace to extra destinations
	// or tenants (headers); unmatched resources use the default chain.
	Routing *route.Config `json:"routing,omitempty"`
	// LogScrubbing redacts sensitive values (built-in + user patterns) from
	// log bodies in the tailer, journald and OTLP-ingest paths, before any
	// enrichment copies from them.
	LogScrubbing *logscrub.Config `json:"logScrubbing,omitempty"`
	// TraceSampling drops ingested spans before forwarding: consistent
	// trace-ID probabilistic sampling with keep-errors/keep-slow guard rails
	// and a spans/second cap. Span metrics still see 100% of spans (the
	// sampler sits below the spanmetrics tap).
	TraceSampling *tracesample.Config `json:"traceSampling,omitempty"`
	// Export overlays per-signal OTLP destinations (endpoint/protocol/headers/
	// auth/TLS per signal) and default-chain additions (static headers, an mTLS
	// client certificate) onto the -otlp-* flag base — what makes collectorless
	// delivery to Mimir/Loki/Tempo's distinct OTLP endpoints expressible, with
	// the disk buffer intact per signal.
	Export *otlpexport.ExportConfig `json:"export,omitempty"`
}

// loadAgentConfig reads and strictly parses the unified config file.
func loadAgentConfig(path string) (*agentConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg agentConfig
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &cfg, nil
}

// validateConfig compiles every section of the unified config (and the
// separate transforms file) without acquiring a single resource: no listeners,
// no log files, no positions file, no spools, no network.
//
// run() always calls it before touching anything, so a bad config fails fast;
// -check-config makes run() stop right after it. Keeping ONE function means the
// dry-run cannot drift from what a real start accepts — adding a config surface
// means adding it here as well as to agentConfig.
func validateConfig(cfg agentConfig, transformsFile string) error {
	if _, err := buildAttrs(cfg.ResourceAttributes); err != nil {
		return fmt.Errorf("resourceAttributes: %w", err)
	}
	if _, err := logattrs.New(cfg.LogAttributes); err != nil {
		return fmt.Errorf("logAttributes: %w", err)
	}
	if cfg.LogScrubbing != nil {
		if _, err := logscrub.New(*cfg.LogScrubbing); err != nil {
			return fmt.Errorf("logScrubbing: %w", err)
		}
	}
	if cfg.LogMetrics != nil && len(cfg.LogMetrics.Metrics) > 0 {
		// The SAME options a real start uses: the name prefix participates in
		// validation (an empty rule name is legal only because the prefix makes
		// the result non-empty), so validating without it would reject configs
		// that actually run.
		opts := []metrics.Option{metrics.WithNamePrefix(*logsMetricsPrefix)}
		if _, err := metrics.NewDynamicMetricSet(cfg.LogMetrics.Metrics, opts...); err != nil {
			return fmt.Errorf("logMetrics: %w", err)
		}
	}
	if cfg.Metrics != nil {
		if _, err := promscrape.NewMetricFilters(cfg.Metrics.Pipelines); err != nil {
			return fmt.Errorf("metrics.pipelines: %w", err)
		}
		if _, err := promscrape.NewSplitters(cfg.Metrics.Splitters); err != nil {
			return fmt.Errorf("metrics.splitters: %w", err)
		}
	}
	if cfg.Logs != nil {
		if _, err := tailer.ValidateSources(cfg.Logs.Sources); err != nil {
			return fmt.Errorf("logs.sources: %w", err)
		}
		if _, err := logline.NewLineFilter(cfg.Logs.Rules); err != nil {
			return fmt.Errorf("logs.rules: %w", err)
		}
	}
	if cfg.TraceMetrics != nil {
		if err := cfg.TraceMetrics.Validate(); err != nil {
			return fmt.Errorf("traceMetrics: %w", err)
		}
	}
	if cfg.TraceSampling != nil {
		if err := cfg.TraceSampling.Validate(); err != nil {
			return fmt.Errorf("traceSampling: %w", err)
		}
	}
	// Shape-only (no filesystem, no network): file errors surface at the real
	// start, where the clients are built.
	if err := cfg.Export.Validate(); err != nil {
		return fmt.Errorf("export: %w", err)
	}
	if cfg.Routing != nil {
		for i, rt := range cfg.Routing.Routes {
			if rt.Name == "" || len(rt.Namespaces) == 0 {
				return fmt.Errorf("routing route %d: name and namespaces are required", i)
			}
			for _, pat := range rt.Namespaces {
				if _, err := path.Match(pat, ""); err != nil {
					return fmt.Errorf("routing route %q: invalid namespace pattern %q: %w", rt.Name, pat, err)
				}
			}
		}
	}
	if transformsFile != "" {
		if _, err := transform.CompileFile(transformsFile); err != nil {
			return fmt.Errorf("transforms: %w", err)
		}
	}
	return nil
}

// printConfigSummary reports what a real start would enable, so -check-config
// answers "is this valid?" and "is this what I meant?" in one run.
func printConfigSummary(cfg agentConfig, log *slog.Logger) {
	on := func(b bool) string {
		if b {
			return "on"
		}
		return "off"
	}
	sections := []string{}
	add := func(name string, present bool) {
		if present {
			sections = append(sections, name)
		}
	}
	add("resourceAttributes", cfg.ResourceAttributes != nil)
	add("logs", cfg.Logs != nil)
	add("logAttributes", cfg.LogAttributes != nil)
	add("logMetrics", cfg.LogMetrics != nil)
	add("logScrubbing", cfg.LogScrubbing != nil)
	add("metrics", cfg.Metrics != nil)
	add("traceMetrics", cfg.TraceMetrics != nil)
	add("traceSampling", cfg.TraceSampling != nil)
	add("routing", cfg.Routing != nil)
	add("export", cfg.Export != nil)
	if len(sections) == 0 {
		sections = append(sections, "(none)")
	}

	log.Info("config is valid",
		"sections", strings.Join(sections, ","),
		"pipelines", fmt.Sprintf("logs=%s metrics=%s cadvisor=%s node=%s journald=%s ingest=%s events=%s",
			on(*logsOn), on(*metricsOn), on(*cadvisorOn), on(*nodeOn), on(*journaldOn), on(*ingestOn), on(*eventsOn)),
		"otlp-endpoint", *otlpEndpoint,
		"otlp-protocol", *otlpProtocol,
		"buffer-dir", *bufferDir,
		"positions-file", *positionsFile,
		"transforms-file", *transformsFile,
		"enrich", *enrichOn,
	)
	if cfg.LogMetrics != nil {
		log.Info("logMetrics", "rules", len(cfg.LogMetrics.Metrics))
	}
	if cfg.Routing != nil {
		for _, rt := range cfg.Routing.Routes {
			log.Info("routing route", "name", rt.Name, "namespaces", strings.Join(rt.Namespaces, ","), "endpoint", rt.Endpoint)
		}
	}
}

// readiness tracks the startup gates /readyz reports on.
//
// A DaemonSet rolling update advances only when the new pod reports ready, so
// this endpoint decides whether a bad rollout stops at the first node or
// marches across the fleet. It previously returned the same static "ok" as
// /healthz — the agent was "ready" the instant the mux was built, even if it
// could not reach the metadata service and could therefore attribute nothing.
//
// Gates are registered at startup and satisfied as each becomes true; /readyz
// is 200 only when none are pending, and reports the pending ones so the
// failure is diagnosable from the probe alone.
// gateMetadata is satisfied by the first successful node-metadata fetch.
const gateMetadata = "metadata-service"

type readiness struct {
	mu    sync.Mutex
	gates map[string]bool
}

func newReadiness() *readiness { return &readiness{gates: map[string]bool{}} }

// require registers a gate that must be satisfied before the agent is ready.
func (r *readiness) require(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.gates[name]; !ok {
		r.gates[name] = false
	}
}

// done marks a gate satisfied. Safe to call repeatedly and from any goroutine.
func (r *readiness) done(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gates[name] = true
}

// pending returns the unsatisfied gates, sorted for a stable probe body.
func (r *readiness) pending() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for name, ok := range r.gates {
		if !ok {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}
