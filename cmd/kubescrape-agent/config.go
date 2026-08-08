package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"

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
	"github.com/JohanLindvall/kubescrape/internal/config"
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

// sectionNames is the JSON name of each agentConfig field, by field index ("
// for a field that is not a section). ONE walk of the struct, so the -config
// flag's help and the -check-config summary cannot disagree about what a
// section is — both enumerated them by hand before, and the help had already
// drifted three sections behind the type it describes.
func sectionNames() []string {
	t := reflect.TypeOf(agentConfig{})
	names := make([]string, t.NumField())
	for i := range t.NumField() {
		if name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ","); name != "-" {
			names[i] = name
		}
	}
	return names
}

// configSections lists the section keys agentConfig accepts, in declaration
// order.
func configSections() string {
	var present []string
	for _, name := range sectionNames() {
		if name != "" {
			present = append(present, name)
		}
	}
	return strings.Join(present, ", ")
}

// presentSections lists the sections cfg actually carries, in declaration
// order. Every section is a pointer, so "present" is "not nil".
func presentSections(cfg agentConfig) []string {
	v := reflect.ValueOf(cfg)
	var present []string
	for i, name := range sectionNames() {
		if name != "" && !v.Field(i).IsZero() {
			present = append(present, name)
		}
	}
	return present
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

// flagWasSet reports whether the operator TYPED this flag, from flag.Visit's
// record of what was Set — never from a comparison against the flag's default.
// A default is the binary's own choice and must never trip a refusal, and a
// default that changes later would otherwise silently start refusing configs
// nobody edited; a value deliberately typed as the default is still a request.
//
// A var only for the tests: flag.Set is the one way into that record and there
// is no way out of it, so a test typing a flag would leave every later test in
// the package looking like an operator typed it. The real command line is
// covered end-to-end by the -check-config child process instead.
var flagWasSet = func(name string) bool {
	set := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			set = true
		}
	})
	return set
}

// checkFlagValues refuses flag values whose only workable meaning is not the
// one the flag reads like.
//
// Both are also NORMALISED by their consumers — promscrape.New defaults a
// non-positive Timeout, tailer.New floors the effective burst at 1 — and that
// stays: it is the LIBRARY guarantee, giving a zero value arriving
// programmatically a defined, non-destructive meaning. This is the other
// question, and the answer differs: what an OPERATOR TYPED. `-scrape-timeout=0`
// is typed to mean "no timeout" and silently becomes 15s; a bucket of half a
// token is typed as a small burst and silently becomes 1. Neither operator gets
// what they asked for, and neither finds out from a fleet they are mid-rollout
// on. Refusing is this repo's pattern for operator nonsense (the excluded-
// pipeline errors above), and it puts the discovery in -check-config.
//
// EXPLICITLY TYPED ONLY, so the normalisation still covers everything else.
// Neither refusal depends on whether the pipeline that reads the value is
// enabled: a flag belongs to one workload's command line (unlike a -config
// section, which one ConfigMap shares with three), and making the same typed
// value legal or illegal according to an unrelated toggle is the trap the
// composition warning below is careful not to fall into.
func checkFlagValues() error {
	// The value is the whole per-request context budget (Scraper.targetTimeout
	// → context.WithTimeout), so a non-positive one expires before the request
	// is written: every annotation-discovered target and both kubelet scrapes
	// fail instantly with "context deadline exceeded", which is total metric
	// loss on every node carrying the flag.
	if flagWasSet("scrape-timeout") && *scrapeTimeout <= 0 {
		return fmt.Errorf("-scrape-timeout=%s does not mean 'no timeout': the value IS each request's context budget, so a non-positive one expires before the request goes out and every target plus both kubelet scrapes fail with 'context deadline exceeded'. "+
			"Pass a positive duration (the default is 15s); this flag has no spelling for 'unbounded'", *scrapeTimeout)
	}
	// A line costs one WHOLE token and the bucket never holds more than the
	// burst, so a bucket below 1 can never grant. Zero is exempt because it is
	// the documented sentinel for "derive it as 2x -logs-rate-limit" — the
	// chart passes it verbatim on every deployment that enables rate limiting,
	// so reading it as a bucket size would refuse the stock config.
	if flagWasSet("logs-rate-burst") && *logsRateBurst != 0 && *logsRateBurst < 1 {
		return fmt.Errorf("-logs-rate-burst=%g is a bucket that can never admit a line: a line spends one WHOLE token and the bucket never holds more than the burst, so pause mode stops reading every log file on this node forever (with -logs-rate-drop, every line is discarded instead). "+
			"Pass a burst of 1 or more, or -logs-rate-burst=0 to derive it as 2x -logs-rate-limit", *logsRateBurst)
	}
	return nil
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
	// A pipeline this binary was not built with (buildtags.go). FIRST, because
	// no amount of valid config makes an absent pipeline run, and because
	// -check-config is where that has to surface: the alternative is a rollout
	// where the flag is accepted, nothing is collected, and the only clue is a
	// missing signal.
	if err := checkExcludedPipelines(); err != nil {
		return err
	}
	// Flag VALUES that can only be a mistake. Beside the pipeline check because
	// both are about the command line rather than a section, and here rather
	// than at the consumer because -check-config is the only place a fleet-wide
	// flag mistake surfaces before the rollout does.
	if err := checkFlagValues(); err != nil {
		return err
	}
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
	// The shape AND every merged per-signal destination — exactly the Configs
	// BuildExporter hands to otlpexport.New. Validating only the shape and the
	// base left every check that exists ONLY after the merge out of the dry
	// run (TLS material on a plaintext gRPC destination; the http:// scheme
	// requirement when the protocol is inherited from -otlp-protocol rather
	// than declared on the override), so `-check-config` exited 0 in CI and the
	// same ConfigMap then CrashLooped the fleet at `creating OTLP exporter` —
	// produced by the check whose whole purpose is preventing that.
	if err := cfg.Export.ValidateAgainst(baseExportConfig()); err != nil {
		return err
	}
	if _, err := buildAttrs(cfg.ResourceAttributes); err != nil {
		return fmt.Errorf("resourceAttributes: %w", err)
	}
	if _, err := compileLogAttrs(cfg.LogAttributes); err != nil {
		return fmt.Errorf("logAttributes: %w", err)
	}
	if _, err := compileScrub(cfg.LogScrubbing); err != nil {
		return fmt.Errorf("logScrubbing: %w", err)
	}
	if _, err := compileLogMetrics(cfg.LogMetrics); err != nil {
		return fmt.Errorf("logMetrics: %w", err)
	}
	if cfg.Metrics != nil {
		if _, err := promscrape.NewMetricFilters(cfg.Metrics.Pipelines); err != nil {
			return fmt.Errorf("metrics.pipelines: %w", err)
		}
		if _, err := promscrape.NewSplitters(cfg.Metrics.Splitters); err != nil {
			return fmt.Errorf("metrics.splitters: %w", err)
		}
	}
	if _, err := compileSources(cfg.Logs); err != nil {
		return fmt.Errorf("logs.sources: %w", err)
	}
	if _, err := compileLogRules(cfg.Logs); err != nil {
		return fmt.Errorf("logs.rules: %w", err)
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
	// exactly that. The SAME const as the runtime refusal, so the wording
	// cannot drift.
	if *serviceGraphOn && *serviceGraphListen == "" && *serviceGraphHTTPListen == "" {
		return errors.New(msgShardNoListener)
	}
	// The SAME merge of flags and section a real start uses, so the dry run
	// cannot accept a shard set the start rejects (the flags participate: the
	// chart configures this feature entirely through them).
	//
	// Only ON THE TIER, like every other tier-only section. Off it the flags
	// are empty, so a section supplying one half of the template (replicas
	// without statefulSet, or the reverse) tripped the half-filled-template
	// refusal on a workload that never reads the section at all — and one
	// ConfigMap is shared by the DaemonSet, the events/Azure singleton and the
	// tier, so a shard set that is valid where it is READ made every other
	// workload exit 1 at startup. startServiceGraph warns where it is ignored.
	if *serviceGraphOn {
		shards, err := serviceGraphShardConfig(cfg.ServiceGraphShards)
		if err != nil {
			return err
		}
		if err := shards.Validate(); err != nil { // its messages already name the section
			return err
		}
		// ReshardConfig.Validate is shape-only by contract and cannot see the
		// flags, so the ring's transport and port are checked against this
		// shard's own listeners here — the one place that knows both.
		if err := shardRingReachesThisShard(shards); err != nil {
			return err
		}
	}
	if cfg.Routing != nil {
		for i, rt := range cfg.Routing.Routes {
			// Validated like any other destination — the dry run used to check
			// the name, the namespaces and the patterns and stop, so a
			// scheme-less route endpoint, or TLS material inherited onto a
			// plaintext route, passed -check-config and CrashLooped the agent
			// on start, which is the one outcome this check exists to prevent.
			rcfg, err := validateRoute(cfg.Export, i, rt)
			if err != nil {
				return err
			}
			if err := rcfg.Validate(); err != nil {
				return fmt.Errorf("routing route %q: %w", rt.Name, err)
			}
		}
	}
	prog, err := compileTransforms(transformsFile)
	if err != nil {
		return fmt.Errorf("transforms: %w", err)
	}
	// Cross-file check: a `type: script` tail-sampling policy is only
	// satisfiable when the transforms file defines a sample: section.
	// Validate() itself uses a placeholder (the injection has not happened at
	// check time), so without this a config that CrashLoops the StatefulSet
	// would pass -check-config.
	if cfg.TailSampling.Enabled() && cfg.TailSampling.UsesScript() && !prog.HasSample() {
		return fmt.Errorf("tailSampling: a `type: script` policy requires -transforms-file with a sample: section defining decide(trace)")
	}
	return nil
}

// --- per-section compile helpers ---
//
// ONE home per section for "which arguments participate in its validity":
// validateConfig, run() and the -test-config harness all compile a section
// through the same helper, so an option that participates in validation (the
// log-metrics name prefix) or a new refusal cannot land in one path and not
// the others. validateConfig passes no extras, and every helper acquires
// NOTHING (no listeners, no log files, no network; compileTransforms reads
// only the file the flag names, which the dry run always did) — run() supplies
// loggers and classifiers through the extras.

// compileScrub compiles the logScrubbing section; nil = no scrubbing.
func compileScrub(cfg *logscrub.Config) (*logscrub.Scrubber, error) {
	if cfg == nil {
		return nil, nil
	}
	return logscrub.New(*cfg)
}

// compileLogAttrs compiles the logAttributes section (logattrs.New already
// returns nil for an absent or empty section; the helper exists so this
// section reads like the others and keeps one compile path).
func compileLogAttrs(cfg *logattrs.Config) (*logattrs.Extractor, error) {
	return logattrs.New(cfg)
}

// compileLogRules compiles the logs.rules chain; nil when the logs section is
// absent. Shared by the tailer AND journald (same section, same semantics),
// which is why run() compiles it outside startLogs — journald must get it even
// with -logs=false.
func compileLogRules(logs *tailer.SourcesConfig) (*logline.LineFilter, error) {
	if logs == nil {
		return nil, nil
	}
	return logline.NewLineFilter(logs.Rules)
}

// compileSources validates the logs.sources list; nil when the section is
// absent (the tailer then uses the default containerd source over -log-dir).
func compileSources(logs *tailer.SourcesConfig) ([]tailer.Source, error) {
	if logs == nil {
		return nil, nil
	}
	return tailer.ValidateSources(logs.Sources)
}

// compileLogMetrics compiles the logMetrics section into a set; nil when the
// section is absent or empty. The name PREFIX is applied here because it
// participates in validation — an empty rule name is legal only because the
// prefix makes the result non-empty — so validating without it would reject
// configs that actually run; extras carry what only a real start wants (the
// logger, the permanent-rejection classifier).
func compileLogMetrics(cfg *metrics.DynamicConfig, extra ...metrics.Option) (*metrics.DynamicMetricSet, error) {
	if cfg == nil || len(cfg.Metrics) == 0 {
		return nil, nil
	}
	opts := append([]metrics.Option{metrics.WithNamePrefix(*logsMetricsPrefix)}, extra...)
	return metrics.NewDynamicMetricSet(cfg.Metrics, opts...)
}

// compileTransforms compiles the -transforms-file; "" = none.
func compileTransforms(file string) (*transform.Program, error) {
	if file == "" {
		return nil, nil
	}
	return transform.CompileFile(file)
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

	// No offset persistence at all, for a pipeline that has offsets. The
	// consequence differs per pipeline and neither is visible from the flag:
	// the tailer re-reads per -logs-unknown-files, while journald has NO cursor
	// file of its own and so seeks to the journal TAIL on every restart, losing
	// whatever was written while the process was down. This lived inside
	// startLogs, behind the -logs toggle — so the journald half, the only place
	// that consequence is written down, could never be said to the
	// journald-only agent it describes.
	//
	// Named rather than refused: running without persistence is a supported
	// arrangement (the flag's own help says empty disables it), just one whose
	// cost is invisible until a restart.
	if *positionsFile == "" && (*logsOn || *journaldOn) {
		out = append(out, "no -positions-file: offsets are not persisted — a -logs restart re-reads per -logs-unknown-files, and -journald resumes at the journal TAIL, losing every entry written while the process was down (journald has no cursor file of its own)")
	}

	// A DERIVED token bucket below one whole token. The value the operator
	// typed is -logs-rate-limit, and it is delivered EXACTLY — the floor lifts
	// the bucket to 1 and leaves the refill accruing at the requested rate — so
	// nothing they asked for is discarded, which is what separates this from
	// the typed sub-1 burst checkFlagValues refuses. Refusing here would also
	// outlaw a legitimate throttle (0.4 lines/s is one line every 2.5s on a
	// chatty file) and would hang its legality on the 2x derivation constant:
	// change that to 3x and the same typed rate flips from refused to accepted,
	// which is not a property of anything the operator wrote. So it is named
	// rather than refused — silent normalisation is the other half of the trap.
	if *logsRateLimit > 0 && *logsRateBurst <= 0 {
		if burst := 2 * *logsRateLimit; burst < 1 {
			out = append(out, fmt.Sprintf(
				"-logs-rate-limit=%g derives a token bucket of %g (-logs-rate-burst=0 means 2x the rate), below the one whole token a line costs: the tailer raises the bucket to 1 — the refill rate stays %g/s — because a bucket that cannot hold a token pauses every file forever, or with -logs-rate-drop discards every line. Set -logs-rate-burst explicitly to choose the bucket.",
				*logsRateLimit, burst, *logsRateLimit))
		}
	}

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
		// The sampler's OWN parse (config.Duration through SlowerThan), not a
		// re-parse: the warning asks whether the guard rail is armed, and it
		// must read the field exactly as the code that arms it does.
		if d, err := cfg.TraceSampling.SlowerThan(); err == nil && d > 0 {
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

	// Peer-IP attribution on the trace tier with the self-metadata lookup turned
	// off. The veto that keeps a rewritten source address from labelling an
	// application's spans with a kubescrape pod (peerIsOurOwnWorkload) reads the
	// pod THIS process resolved for -self-attributes; with the lookup never run
	// it has no pod, and its documented answer for "we do not know yet" — false,
	// do not veto — becomes the answer for the process LIFETIME.
	//
	// A warning rather than a refusal: the fallback still attributes correctly on
	// a direct hop (the arrangement it is meant for), and -self-attributes is a
	// legitimate thing to turn off. What must not happen silently is the failure
	// mode, because it is invisible: the misattribution renders perfectly, and
	// kubescrape_ingest_resources_total{outcome="peer_ip_rejected"} — documented
	// as THE signal that peer-IP attribution cannot work on a path — stays flat
	// whether the veto found nothing or was never able to look.
	if *serviceGraphOn && *ingestPeerIP && (!*selfAttrsOn || *selfAttrsRefresh <= 0) {
		off := "-self-attributes=false"
		if *selfAttrsOn {
			off = fmt.Sprintf("-self-attributes-refresh=%s (0 disables the lookup)", *selfAttrsRefresh)
		}
		out = append(out, fmt.Sprintf(
			"-ingest-peer-ip-fallback on the trace tier needs this process's own pod to veto an attribution that resolved to the tier's OWN workload, and %s never resolves it: a proxy, mesh or misaddressed hop then labels application spans with a kubescrape pod's identity — on every span, and with peer_ip_rejected flat, so it is indistinguishable from success. "+
				"Leave -self-attributes on with a positive -self-attributes-refresh, or drop -ingest-peer-ip-fallback and have senders carry k8s.pod.uid / container.id.", off))
	}

	// A logAttributes rule lifting a LINE value into a resolved-identity
	// resource attribute hands whatever writes the log line control of that
	// key — and k8s.namespace.name is what routing keys tenancy on, so a pod
	// printing a crafted line could steer its records onto another tenant's
	// destination. The pod-annotation path REFUSES these keys outright
	// (attrs.ReservedIdentity, tailer/podconfig.go); here the config is the
	// OPERATOR's own, so a deliberate lift stays legal — but it is the same
	// boundary crossed from the other side, and it must be named, not silent.
	if cfg.LogAttributes != nil {
		for _, r := range cfg.LogAttributes.Rules {
			if r.Target != logattrs.TargetResource {
				continue
			}
			attr := r.Attribute
			if attr == "" {
				attr = r.Key
			}
			if attrs.ReservedIdentity(attr) {
				out = append(out, fmt.Sprintf(
					"logAttributes rule %q lifts a log-line value into %q, a RESOLVED-IDENTITY resource attribute: whatever writes the line controls it (k8s.namespace.name keys tenancy routing; service.instance.id and k8s.pod.* forge series identity). The pod-annotation path refuses these keys; lift into a differently-named attribute unless the workload is genuinely authoritative for this one.",
					r.Key, attr))
			}
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

// validateRoute checks one routing route's shape — name and namespaces
// present, every namespace pattern parseable — and derives its export config
// through the shared derivation. ONE function called by BOTH validateConfig
// and run()'s route loop, so a new refusal cannot land in one and not the
// other; i is the route's index, used only when it has no name to report.
//
// The pattern check fails startup because a malformed glob reads as silent
// no-match at runtime — the route never fires and its tenant's telemetry goes
// to the default destination, indistinguishable from "no traffic yet"
// (config.Glob carries the full rationale).
func validateRoute(exp *otlpexport.ExportConfig, i int, rt route.Route) (otlpexport.Config, error) {
	if rt.Name == "" || len(rt.Namespaces) == 0 {
		return otlpexport.Config{}, fmt.Errorf("routing route %d: name and namespaces are required", i)
	}
	for _, pat := range rt.Namespaces {
		if err := config.Glob(pat); err != nil {
			return otlpexport.Config{}, fmt.Errorf("routing route %q: invalid namespace pattern %q: %w", rt.Name, pat, err)
		}
	}
	// The SAME derivation on both paths, so a config the dry run accepts is a
	// config that starts.
	return routeExportConfig(exp, rt)
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
		// A route naming its OWN endpoint does not inherit the default chain's
		// credentials. Those authenticate this deployment to ITS collector, and
		// this is a different host — usually a different tenant's, sometimes a
		// different organization's. Carrying the base BearerTokenFile and mTLS
		// client certificate over presented them to whatever address the route
		// named, which is a credential disclosure with no way to opt out: there
		// was no per-route field to override them with. So the destination is
		// rebuilt on otlpexport.Config.TransportOnly — the one spelling of the
		// transport-vs-destination partition, shared with the reshard hop's
		// client derivation — and every destination field is taken from the
		// route or left unset.
		out := rcfg.TransportOnly()
		out.Endpoint = rt.Endpoint
		// Two deliberate carryovers from the merged base, bit-identical to the
		// hand-rolled strip this replaced: the MERGED base headers still ride to
		// an own-endpoint route (a possible header leak, noted by review —
		// changing it is a product decision, not this refactor), and so does
		// -otlp-insecure-skip-verify, which a route has no field of its own for.
		out.Headers = rcfg.Headers
		out.InsecureSkipVerify = rcfg.InsecureSkipVerify
		out.BearerTokenFile = rt.BearerTokenFile
		out.ClientCertFile = rt.ClientCertFile
		out.ClientKeyFile = rt.ClientKeyFile
		out.CAFile = rt.CAFile
		out.Insecure = rt.Insecure
		return out, nil
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
	// DERIVED, not enumerated: a section added to agentConfig updates the -config
	// help through the same walk, and this summary is what answers "is this what
	// I meant?" — a section missing from a hand-written list reads as "not
	// configured", which is the one wrong answer it can give.
	sections := presentSections(cfg)
	if len(sections) == 0 {
		sections = append(sections, "(none)")
	}

	log.Info("config is valid",
		"sections", strings.Join(sections, ","),
		// Which binary this is, not just whether the config parses: the
		// optional pipelines are build-tag-gated (buildtags.go).
		"optionalPipelines", builtPipelines(),
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

// gate registers a gate and returns the func that satisfies it, so a
// require/done pair cannot name two different gates. The returned func has
// done's semantics: idempotent, safe from any goroutine.
func (r *readiness) gate(name string) func() {
	r.require(name)
	return func() { r.done(name) }
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
