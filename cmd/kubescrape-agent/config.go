package main

import (
	"fmt"
	"os"

	"sigs.k8s.io/yaml"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/promscrape"
	"github.com/JohanLindvall/kubescrape/internal/agent/spanmetrics"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailer"
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
