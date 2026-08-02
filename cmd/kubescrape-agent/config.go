package main

import (
	"fmt"
	"log/slog"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/promscrape"
	"github.com/JohanLindvall/kubescrape/internal/agent/route"
	"github.com/JohanLindvall/kubescrape/internal/agent/servicegraph"
	"github.com/JohanLindvall/kubescrape/internal/agent/spanmetrics"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailbuffer"
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
	// TraceMetrics tunes the RED metrics derived from trace spans (histogram
	// buckets, extra dimensions, cardinality cap). Aggregation runs on the
	// service-graph tier, gated by -ingest-span-metrics; this section only
	// tunes it.
	TraceMetrics *spanmetrics.Config `json:"traceMetrics,omitempty"`
	// Routing fans exported payloads out by namespace to extra destinations
	// or tenants (headers); unmatched resources use the default chain.
	Routing *route.Config `json:"routing,omitempty"`
	// LogScrubbing redacts sensitive values (built-in + user patterns) from
	// log bodies in the tailer, journald and OTLP-ingest paths, before any
	// enrichment copies from them.
	LogScrubbing *logscrub.Config `json:"logScrubbing,omitempty"`
	// ServiceGraph tunes edge pairing on the trace tier (-service-graph): the
	// wait window, the store and series caps, the latency buckets and the extra
	// dimensions. It is read only by that role.
	ServiceGraph *servicegraph.Config `json:"serviceGraph,omitempty"`
	// ServiceGraphShards tells a tier pod about the tier: which shards exist,
	// which one it is, and how to reach the others for the internal re-shard
	// hop. The flags (-service-graph-shards / -service-graph-endpoint /
	// -service-graph-shard-name / -service-graph-token-file) express the shape
	// the chart renders; this section is the richer form — explicit endpoints
	// for a tier outside Kubernetes, TLS material, headers, tokensPerShard — and
	// wins field by field where both are set (see serviceGraphShardConfig).
	ServiceGraphShards *servicegraph.ReshardConfig `json:"serviceGraphShards,omitempty"`
	// TraceSampling drops spans before export: consistent trace-ID
	// probabilistic sampling with keep-errors/keep-slow guard rails and a
	// spans/second cap. It runs on the trace tier, below the spanmetrics tap, so
	// RED metrics still see 100% of spans.
	TraceSampling *tracesample.Config `json:"traceSampling,omitempty"`
	// TailSampling decides each trace AS A WHOLE, after buffering its spans for
	// a decision window: a policy list (errors, latency, attributes, rate) plus
	// the buffer's memory bounds. It runs on the trace tier, below traceSampling
	// and below both taps, so the graph and the RED metrics still see 100% of
	// spans. Off unless it has policies — and note that buffered spans are ACKED
	// before they are decided, which is the one place in this agent where a hard
	// kill loses data (agent/tailbuffer's package doc).
	TailSampling *tailbuffer.Config `json:"tailSampling,omitempty"`
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
	// The OTLP transport flags. Shape-only (no dial, no file reads), but they
	// are the ones that abort a real start: a bad protocol, compression,
	// compression level or scheme-less endpoint, or TLS material on a
	// plaintext connection.
	//
	// Only when the default chain is actually BUILT, and against the base the
	// export section merges into. BuildExporter skips the default entirely
	// once all three signals are overridden (persignal.go) — the collectorless
	// case, where the flag endpoint still points at the stock collector
	// address nothing dials — so validating it unconditionally CrashLooped
	// the whole DaemonSet on a config a real start accepts, from the check
	// whose purpose is preventing exactly that.
	if e := cfg.Export; e == nil || e.Logs == nil || e.Metrics == nil || e.Traces == nil {
		if err := cfg.Export.ApplyBase(baseExportConfig()).Validate(); err != nil {
			return fmt.Errorf("otlp flags: %w", err)
		}
	}
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
	// Shape-only, and it validates the policy list by COMPILING it (regexes,
	// budgets, durations), so -check-config accepts exactly what a start does.
	if err := cfg.TailSampling.Validate(); err != nil { // nil-receiver safe
		return fmt.Errorf("tailSampling: %w", err)
	}
	// Both service-graph sections are shape-only (no DNS, no filesystem, no
	// namespace resolution), so the dry run runs exactly what a start does.
	if err := cfg.ServiceGraph.Validate(); err != nil { // nil-receiver safe
		return fmt.Errorf("serviceGraph: %w", err)
	}
	// The shard's receiver takes forwarded spans from every pod in the cluster,
	// so it is refused unauthenticated — HERE rather than at the listener, so
	// -check-config catches it before the StatefulSet CrashLoops. (The chart
	// renders -service-graph with no token flag when no Secret is configured,
	// precisely so this refusal is what an operator sees.) Shape only: whether
	// the file is readable and non-empty is checked at the real start, where
	// it is equally fatal.
	if *serviceGraphOn && strings.TrimSpace(*serviceGraphToken) == "" {
		return fmt.Errorf("-service-graph requires -service-graph-token-file: the shard's span receiver is reachable from every pod in the cluster and must not be unauthenticated")
	}
	// A shard with no listener at all receives nothing, pairs nothing, and
	// reports READY forever (the gate is satisfied by the receiver binding, and
	// with neither address there is nothing to bind). sgReceiver.Run refuses it,
	// but only at the real start — so -check-config used to pass a config that
	// CrashLoops the StatefulSet, from the check whose whole purpose is catching
	// exactly that. Same wording as the runtime refusal.
	if *serviceGraphOn && *serviceGraphListen == "" && *serviceGraphHTTPListen == "" {
		return fmt.Errorf("-service-graph is set but -service-graph-listen (and -service-graph-http-listen) are empty: the shard would receive nothing")
	}
	// The SAME merge of flags and section a real start uses, so the dry run
	// cannot accept a shard set the start rejects (the flags participate: the
	// chart configures this feature entirely through them).
	shards, err := serviceGraphShardConfig(cfg.ServiceGraphShards)
	if err != nil {
		return err
	}
	if err := shards.Validate(); err != nil { // its messages already name the section
		return err
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
			// Through the SAME derivation a real start uses, then validated
			// like any other destination. The dry run used to check the name,
			// the namespaces and the patterns and stop — so a scheme-less
			// route endpoint, or TLS material inherited onto a plaintext
			// route, passed -check-config and CrashLooped the agent on start,
			// which is the one outcome this check exists to prevent.
			rcfg, err := routeExportConfig(cfg.Export, rt)
			if err != nil {
				return err
			}
			if err := rcfg.Validate(); err != nil {
				return fmt.Errorf("routing route %q: %w", rt.Name, err)
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

// configWarnings reports combinations that are LEGAL but do something other than
// what they read like. They are warnings rather than errors, and each one has to
// justify being a warning rather than a refusal: an error is right when the
// config can only be a mistake, and wrong when it is a supported arrangement
// with a sharp edge.
//
// Emitted by -check-config and by every real start, from the same list, so a dry
// run says exactly what a start would.
func configWarnings(cfg agentConfig) []string {
	var out []string

	// traceSampling (per-SPAN) above tailSampling (per-TRACE). The two nest
	// correctly for the PROBABILITY — both hash the trace id the same way, so a
	// tail probabilistic policy at 50% keeps exactly the traces a head
	// probability of 0.5 already passed — and maxSpansPerSecond is an overload
	// valve that only truncates when the shard is over budget. The GUARD RAILS
	// are the problem: they are decided per span, so they rescue the error (or
	// slow) spans of traces the probability dropped, and hand the tail sampler a
	// trace that is only its error spans. It judges that fragment as if it were
	// the trace — latency reads a lower bound, an inverted attribute exclusion
	// can miss the span that would have vetoed — and can then EXPORT it, which
	// is a trace that never existed rather than merely an incomplete one.
	//
	// Not a refusal, for one concrete reason: keepErrors DEFAULTS to true, so
	// refusing would reject `traceSampling: {probability: 0.1}` next to any
	// tailSampling section — the most natural composition there is — and would
	// make the same effective config legal or illegal depending on whether the
	// operator spelled the default out. The degradation is also well-defined and
	// documented (agent/tailsample on partial traces), which is the line: a
	// sharp edge gets named, an impossibility gets refused.
	if *serviceGraphOn && cfg.TailSampling.Enabled() && cfg.TraceSampling != nil && cfg.TraceSampling.Enabled() {
		var rails []string
		if cfg.TraceSampling.KeepErrors == nil || *cfg.TraceSampling.KeepErrors {
			rails = append(rails, "keepErrors")
			if cfg.TraceSampling.KeepErrors == nil {
				rails[len(rails)-1] = "keepErrors (defaulted on)"
			}
		}
		if d, err := time.ParseDuration(cfg.TraceSampling.KeepSlowerThan); err == nil && d > 0 {
			rails = append(rails, "keepSlowerThan")
		}
		if len(rails) > 0 {
			out = append(out, fmt.Sprintf(
				"traceSampling %s runs ABOVE tailSampling and decides PER SPAN: it rescues individual spans of traces the probability dropped, so the tail sampler is handed trace fragments and may export a trace that never existed. "+
					"Set traceSampling.keepErrors: false (and drop keepSlowerThan), and express the same intent as tail policies — statusCode: [ERROR] and latency — which judge whole traces. "+
					"traceSampling.probability is safe below a tail sampler (the two nest: a tail probabilistic policy at the same fraction keeps exactly what the head kept) and so is maxSpansPerSecond, which is an overload valve.",
				strings.Join(rails, " and ")))
		}
	}
	return out
}

// logConfigWarnings emits configWarnings.
func logConfigWarnings(cfg agentConfig, log *slog.Logger) {
	for _, w := range configWarnings(cfg) {
		log.Warn(w)
	}
}

// routeExportConfig derives one route destination's client config: the flag
// base, plus the export section's base additions (headers, client cert), plus
// the route's own endpoint and headers (which win per key).
//
// ONE derivation, shared by validateConfig and the real start, for the same
// reason validateConfig itself is shared — a dry run that builds something
// else proves nothing about what will start.
//
// A route with no endpoint of its own inherits the flag base, and that is an
// ERROR when the base is not a destination this deployment uses: with all
// three signals overridden in export:, BuildExporter never constructs the
// default chain, so the flag endpoint is whatever it happened to default to
// (the stock otel-collector.monitoring address). Inheriting it silently sent
// a tenant's telemetry to a collector nobody configured.
func routeExportConfig(exp *otlpexport.ExportConfig, rt route.Route) (otlpexport.Config, error) {
	rcfg := exp.ApplyBase(baseExportConfig())
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
		return rcfg, nil
	}
	if baseEndpointUnused(exp) {
		return otlpexport.Config{}, fmt.Errorf("routing route %q: no endpoint, and the flag base is not a destination here (export: gives every signal its own endpoint, so nothing dials -otlp-endpoint) — give the route its own endpoint", rt.Name)
	}
	return rcfg, nil
}

// baseEndpointUnused reports whether NOTHING dials the flag base endpoint:
// every signal is overridden AND every override names its own endpoint.
//
// Struct presence is not the test. An override that sets only headers (or a
// bearer file, or TLS) inherits the base ENDPOINT through merged(), so the
// base is still the address that signal reaches — and rejecting an
// endpoint-less route there would fail a config the exporter builds happily,
// which is the CrashLoop the shared derivation exists to prevent.
func baseEndpointUnused(exp *otlpexport.ExportConfig) bool {
	if exp == nil {
		return false
	}
	for _, o := range []*otlpexport.ExportOverride{exp.Logs, exp.Metrics, exp.Traces} {
		if o == nil || o.Endpoint == "" {
			return false
		}
	}
	return true
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
	add("tailSampling", cfg.TailSampling != nil)
	add("serviceGraph", cfg.ServiceGraph != nil)
	add("serviceGraphShards", cfg.ServiceGraphShards != nil)
	add("routing", cfg.Routing != nil)
	add("export", cfg.Export != nil)
	if len(sections) == 0 {
		sections = append(sections, "(none)")
	}

	log.Info("config is valid",
		"sections", strings.Join(sections, ","),
		"pipelines", fmt.Sprintf("logs=%s metrics=%s cadvisor=%s node=%s journald=%s ingest=%s events=%s azure=%s serviceGraph=%s",
			on(*logsOn), on(*metricsOn), on(*cadvisorOn), on(*nodeOn), on(*journaldOn), on(*ingestOn), on(*eventsOn), on(*azureOn), on(*serviceGraphOn)),
		"otlp-endpoint", *otlpEndpoint,
		"otlp-protocol", *otlpProtocol,
		"buffer-dir", *bufferDir,
		"positions-file", *positionsFile,
		"transforms-file", *transformsFile,
		"enrich", *enrichOn,
		"self-attributes", *selfAttrsOn,
	)
	if cfg.LogMetrics != nil {
		log.Info("logMetrics", "rules", len(cfg.LogMetrics.Metrics))
	}
	if cfg.Routing != nil {
		for _, rt := range cfg.Routing.Routes {
			log.Info("routing route", "name", rt.Name, "namespaces", strings.Join(rt.Namespaces, ","), "endpoint", rt.Endpoint)
		}
	}
	// The MERGED shard set, not the section: the chart configures this feature
	// through flags alone, so printing the section would report "(none)" for
	// the deployment the dry run most needs to describe. The shard count and
	// the tier's name are the two things an operator gets wrong (a count that
	// does not match the StatefulSet leaves traces unpaired, silently), so they
	// are what the summary names.
	if shards, err := serviceGraphShardConfig(cfg.ServiceGraphShards); err == nil && shards.Enabled() {
		log.Info("service-graph forwarding", "shards", shards.Replicas, "statefulSet", shards.StatefulSet,
			"namespace", shards.Namespace, "port", shards.Port, "endpoints", strings.Join(shards.Endpoints, ","))
	}
	if *serviceGraphOn {
		log.Info("service-graph shard role", "listen", *serviceGraphListen, "httpListen", *serviceGraphHTTPListen,
			"interval", *serviceGraphIv, "tokenFile", *serviceGraphToken)
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
