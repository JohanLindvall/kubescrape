package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/cgroupstats"
	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpingest"
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

// sectionNames is the JSON name of each agentConfig field, by field index ("
// for a field that is not a section). ONE walk of the struct, so the -config
// flag's help and the -check-config summary cannot disagree about what a
// section is — both enumerated them by hand before, and the help had already
// drifted three sections behind the type it describes.
func sectionNames() []string {
	t := reflect.TypeFor[agentConfig]()
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
// Most are also NORMALISED by their consumers — promscrape.New defaults a
// non-positive Timeout, tailer.New floors the effective burst at 1, the
// periodic loops substitute or skip a non-positive period — and that stays:
// it is the LIBRARY guarantee, giving a zero value arriving programmatically a
// defined, non-destructive meaning. This is the other question, and the answer
// differs: what an OPERATOR TYPED. `-scrape-timeout=0` is typed to mean "no
// timeout" and silently becomes 15s; a bucket of half a token is typed as a
// small burst and silently becomes 1. Neither operator gets what they asked
// for, and neither finds out from a fleet they are mid-rollout on. Refusing is
// this repo's pattern for operator nonsense (the excluded-pipeline errors,
// checkExcludedPipelines in buildtags.go), and it puts the discovery in
// -check-config.
//
// EXPLICITLY TYPED ONLY, so the normalisation still covers everything else.
// No refusal depends on whether the pipeline that reads the value is enabled:
// a flag belongs to one workload's command line (unlike a -config section,
// which one ConfigMap shares with three), and making the same typed value
// legal or illegal according to an unrelated toggle is the trap the
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
	// The sampler's ticker period. A non-positive one is not "as fast as
	// possible" and not "off" — time.NewTicker PANICS on it, which would take
	// the whole agent down at the point the pipeline starts rather than at the
	// point the flag was typed. cgroupstats.New defaults a non-positive value
	// (the library guarantee, for a Config arriving programmatically); this is
	// the other question, what an operator TYPED, and the answer is the same as
	// -scrape-timeout's: they did not get what they asked for and would find
	// out mid-rollout.
	if flagWasSet("cgroup-stats-interval") && *cgroupStatsIv <= 0 {
		return fmt.Errorf("-cgroup-stats-interval=%s is not a sampling period: this flag has no spelling for 'as fast as possible' or for 'off' (-cgroup-stats=false is off). "+
			"Pass a positive duration well below -scrape-interval, which is the window the distribution describes (the default is %s)", *cgroupStatsIv, cgroupstats.DefaultInterval)
	}
	// The floor, and it only exists in the direction that BURNS THE NODE. One
	// sweep is three cgroup reads per container, so the period is what divides
	// that cost: at the floor a 200-container node already issues 6000 reads a
	// second, and a tenth of it would be a busy loop on the process that also
	// tails every log file here — bought for no signal, since a burst shorter
	// than 100ms cannot be attributed to anything anyway. Refused rather than
	// clamped because the operator typed it; cgroupstats.New clamps the same
	// value arriving programmatically.
	if flagWasSet("cgroup-stats-interval") && *cgroupStatsIv > 0 && *cgroupStatsIv < cgroupstats.MinInterval {
		return fmt.Errorf("-cgroup-stats-interval=%s is below the %s floor: one sweep is three cgroup file reads per container, so this asks the node agent for %.0f reads a second per 100 containers and buys no burst resolution that survives a %s export window. "+
			"Pass %s or more (the default is %s)",
			*cgroupStatsIv, cgroupstats.MinInterval, 300*float64(time.Second)/float64(*cgroupStatsIv),
			*scrapeInterval, cgroupstats.MinInterval, cgroupstats.DefaultInterval)
	}
	// The discovery ticker's period, and the same two refusals for the same two
	// reasons: time.NewTicker PANICS on a non-positive one (at the point the
	// pipeline starts, not the point the flag was typed), and below the floor a
	// pass re-offers every unresolved cgroup to the metadata service — which on
	// a fresh node is every pod's sandbox, one per pod, for maxUnresolvedAge.
	if flagWasSet("cgroup-stats-discover-interval") && *cgroupDiscoverIv <= 0 {
		return fmt.Errorf("-cgroup-stats-discover-interval=%s is not a discovery cadence: this flag has no spelling for 'never re-scan' or for 'off' (-cgroup-stats=false is off), and discovery is the ONLY way a container enters the sampled set. "+
			"Pass a positive duration (the default is %s)", *cgroupDiscoverIv, cgroupstats.DefaultDiscoverInterval)
	}
	if flagWasSet("cgroup-stats-discover-interval") && *cgroupDiscoverIv > 0 && *cgroupDiscoverIv < cgroupstats.MinDiscoverInterval {
		return fmt.Errorf("-cgroup-stats-discover-interval=%s is below the %s floor: every pass walks the cgroup hierarchy AND asks the metadata service about each cgroup that has not resolved yet, which on a fresh node is one per pod (the sandbox cgroup, which never resolves) for the first few minutes. "+
			"Pass %s or more (the default is %s)",
			*cgroupDiscoverIv, cgroupstats.MinDiscoverInterval, cgroupstats.MinDiscoverInterval, cgroupstats.DefaultDiscoverInterval)
	}
	// The trace tier's two export periods, refused for the cgroup sampler's
	// reason: a non-positive one is neither "off" nor "as fast as possible".
	// cumagg.Store.Run cannot hand it to time.NewTicker (which panics) and
	// substitutes a minute instead — while the startup lines and -check-config
	// print the value that was typed, so the process describes an interval it
	// does not use.
	if flagWasSet("ingest-span-metrics-interval") && *spanMetricsIv <= 0 {
		return fmt.Errorf("-ingest-span-metrics-interval=%s is not an export period: this flag has no spelling for 'as fast as possible' or for 'off' (-ingest-span-metrics=false is off). "+
			"Pass a positive duration (the default is 1m)", *spanMetricsIv)
	}
	if flagWasSet("service-graph-interval") && *serviceGraphIv <= 0 {
		return fmt.Errorf("-service-graph-interval=%s is not an export period: this flag has no spelling for 'as fast as possible' or for 'off' (-service-graph=false is off). "+
			"Pass a positive duration (the default is 1m)", *serviceGraphIv)
	}
	// The scrape period, and the one typed value that meant something
	// DIFFERENT to each of its three readers: Scraper.Run warns once and
	// never scrapes (annotation targets and all three kubelet scrapes alike),
	// the cgroup sampler — whose export window is this flag — substitutes 30s
	// and keeps exporting, and the cgroup parity warning skips itself. None of
	// those is "off" for the pipelines the operator meant, and the effective-
	// limits line prints the 0 throughout.
	if flagWasSet("scrape-interval") && *scrapeInterval <= 0 {
		return fmt.Errorf("-scrape-interval=%s is not a scrape period: a non-positive one stops the whole scrape loop — every annotation-discovered target and all three kubelet scrapes — while -cgroup-stats keeps exporting on a 30s window of its own; this flag has no spelling for 'off' "+
			"(-metrics=false -cadvisor=false -node-metrics=false -kubelet-summary=false is off). Pass a positive duration (the default is 30s)", *scrapeInterval)
	}
	// The log-derived metrics' export period. metrics.DynamicMetricSet.Run
	// reads a non-positive one as "never export" — every line is still matched
	// and observed at full per-line cost, and only the shutdown flush ever
	// sends — and says so only from inside a real start, so -check-config
	// signed off on a section that would deliver nothing for the process
	// lifetime.
	if flagWasSet("logs-metrics-interval") && *logsMetricsEvery <= 0 {
		return fmt.Errorf("-logs-metrics-interval=%s is not an export period: a non-positive one still matches and observes every log line and never exports the result (only the final flush at shutdown sends); this flag has no spelling for 'off' "+
			"(removing the logMetrics section is off). Pass a positive duration (the default is 30s)", *logsMetricsEvery)
	}
	// A relative cgroup root would be resolved against the process' working
	// directory, which in a distroless container is "/" — so it would silently
	// almost-work, finding nothing, which is precisely the outcome this
	// pipeline is built to never produce quietly.
	if flagWasSet("cgroup-stats-root") && *cgroupRoot != "" && !filepath.IsAbs(*cgroupRoot) {
		return fmt.Errorf("-cgroup-stats-root=%q must be an absolute path (empty autodetects %s): a relative one resolves against the container's working directory and would find no cgroups while reporting no error",
			*cgroupRoot, cgroupstats.DefaultRoot)
	}
	// Two ingest bounds whose ONLY documented spelling of "use the built-in
	// default" is 0. otlpingest.NewServer normalises a non-positive value to the
	// default, so a typed negative runs at 32 / 4 MiB in silence — the
	// effective-limits line prints the resolved bound, so it would say 32
	// where the operator typed -1 meaning "no bound".
	//
	// The refusal is worth spelling out because THIS BINARY establishes the
	// opposite convention one flag away: -otlp-max-send-bytes documents
	// "negative disables", and sgMaxRecvBytes reads it that way. An operator
	// raising a collector's max_recv_msg_size and typing -1 here to mean "no
	// cap" gets the 4 MiB default and a ResourceExhausted on every larger push,
	// while believing the cap is off. There is no "unbounded" here to offer
	// them: the bound is what keeps an unauthenticated listener from being an
	// OOM the process cannot defend against.
	if flagWasSet("ingest-max-in-flight") && *ingestMaxInFlight < 0 {
		return fmt.Errorf("-ingest-max-in-flight=%d is not 'unbounded': a negative value is normalised to the built-in default (32), so the concurrency bound runs at the default while the flag reads as unbounded. "+
			"Pass a positive bound, or 0 for the default; this flag has no spelling for 'no bound' — it is what keeps an unauthenticated listener from being an OOM the process cannot defend against (the neighbouring -otlp-max-send-bytes is the flag where a negative disables)",
			*ingestMaxInFlight)
	}
	if flagWasSet("ingest-grpc-max-recv-bytes") && *ingestGRPCMaxRecv < 0 {
		return fmt.Errorf("-ingest-grpc-max-recv-bytes=%d is not 'no cap': a negative value is normalised to gRPC's own default (4 MiB), and every larger push is then refused with ResourceExhausted while the cap looks disabled. "+
			"Pass a positive byte count, or 0 for the default; unlike -otlp-max-send-bytes, this flag has no spelling for 'unbounded'",
			*ingestGRPCMaxRecv)
	}
	return nil
}

// checkFlagChoices refuses a flag value outside its closed set, or a flag
// combination that names nothing to do — whether typed or not, unlike
// checkFlagValues: no default spells any of these, so the "a default must never
// trip a refusal" rule has nothing to protect.
//
// They used to be checked in run()'s prologue, which -check-config reached too
// but validate_test could not; here they sit with every other command-line
// refusal, in the one function both paths call.
func checkFlagChoices() error {
	switch otlpingest.MetricsMode(*ingestMetrics) {
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
		return errors.New("-ingest is set but both -ingest-grpc-endpoint and -ingest-http-endpoint are empty")
	}
	// From the tagged file pair: a build without the `events` tag does not link
	// the package that defines what -events-start means (see buildtags.go).
	if err := validateEventsFlags(); err != nil {
		return err
	}
	// The -azure-* flag surface, from the tagged file pair: a build without the
	// `azure` tag does not link the package that defines what the values mean
	// (see buildtags.go).
	return validateAzureFlags()
}

// validateConfig compiles every section of the unified config (and the
// separate transforms file) without acquiring a single resource: no listeners,
// no log files, no positions file, no spools, no network — and discards what it
// compiled. It is compileConfig's dry-run face: -check-config and the tests
// ask it for a verdict, while run() calls compileConfig itself and consumes
// the result.
func validateConfig(cfg agentConfig, transformsFile string) error {
	_, err := compileConfig(cfg, transformsFile)
	return err
}

// compiledConfig is everything compileConfig compiled that a real start goes
// on to USE, so no section is compiled twice. It used to be: validateConfig
// compiled every section and threw the results away, and run() and the start
// functions compiled them again behind error branches validation had already
// made unreachable (with their own drifted wording), re-parsed the kubelet
// endpoint behind a should-not-happen fallback, and read the hot-reloadable
// transforms file a second time — so the program a start ran was not
// guaranteed to be the one that had just been validated.
//
// Every field is nil (or empty) when its section is absent, exactly as the
// per-section helper returns it.
type compiledConfig struct {
	attrs      *attrs.Builders
	logAttrs   *logattrs.Extractor
	scrub      *logscrub.Scrubber
	logMetrics *metrics.DynamicMetricSet
	// logRules is the logs.rules chain, shared by every log producer.
	logRules *logline.LineFilter
	// logSources is the validated logs.sources list; nil means the tailer's
	// default containerd source over -log-dir.
	logSources    []tailer.Source
	metricFilters *promscrape.MetricFilters
	splitters     []*promscrape.Splitter
	// kubeletBase is -kubelet-endpoint normalised into a base URL net/http
	// will accept (kubeletBase); "" when the flag is empty.
	kubeletBase string
	// routes holds one exporter config per routing.routes entry, index for
	// index (validateRoutes); nil without a routing section.
	routes []otlpexport.Config
	// transforms is the compiled -transforms-file; nil without the flag.
	transforms *transform.Program
}

// compileConfig compiles every section of the unified config and the
// transforms file, refusing what a start would refuse, and returns what it
// compiled. run() always calls it before touching anything, so a bad config
// fails fast; -check-config makes run() stop right after it. ONE function for
// the verdict and the start means the dry run cannot drift from what a real
// start accepts — adding a config surface means adding it here as well as to
// agentConfig.
//
// extra reaches only the logMetrics set, and carries what only a real start
// wants (its logger, its permanent-rejection classifier); neither affects
// whether the section compiles, so validateConfig passes none.
func compileConfig(cfg agentConfig, transformsFile string, extra ...metrics.Option) (*compiledConfig, error) {
	var cc compiledConfig
	// A pipeline this binary was not built with (buildtags.go). FIRST, because
	// no amount of valid config makes an absent pipeline run, and because
	// -check-config is where that has to surface: the alternative is a rollout
	// where the flag is accepted, nothing is collected, and the only clue is a
	// missing signal.
	if err := checkExcludedPipelines(); err != nil {
		return nil, err
	}
	// Flag VALUES that can only be a mistake. Beside the pipeline check because
	// both are about the command line rather than a section, and here rather
	// than at the consumer because -check-config is the only place a fleet-wide
	// flag mistake surfaces before the rollout does.
	if err := checkFlagValues(); err != nil {
		return nil, err
	}
	if err := checkFlagChoices(); err != nil {
		return nil, err
	}
	// The kubelet base URL, PARSED — nothing used to parse it at all, so a
	// value no request can be built from (the commonest being an IPv6 host the
	// chart cannot bracket for us, see kubeletBase) passed -check-config and
	// then failed every kubelet scrape on every node for the process lifetime.
	// NORMALISED here, once, and handed to startScraper: this is the one place
	// the flag is read.
	kb, err := kubeletBase(*kubeletEndpoint)
	if err != nil {
		return nil, err
	}
	cc.kubeletBase = kb
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
		return nil, err
	}
	if cc.attrs, err = buildAttrs(cfg.ResourceAttributes); err != nil {
		return nil, fmt.Errorf("resourceAttributes: %w", err)
	}
	if cc.logAttrs, err = compileLogAttrs(cfg.LogAttributes); err != nil {
		return nil, fmt.Errorf("logAttributes: %w", err)
	}
	if cc.scrub, err = compileScrub(cfg.LogScrubbing); err != nil {
		return nil, fmt.Errorf("logScrubbing: %w", err)
	}
	if cc.logMetrics, err = compileLogMetrics(cfg.LogMetrics, extra...); err != nil {
		return nil, fmt.Errorf("logMetrics: %w", err)
	}
	if cfg.Metrics != nil {
		if cc.metricFilters, err = promscrape.NewMetricFilters(cfg.Metrics.Pipelines); err != nil {
			return nil, fmt.Errorf("metrics.pipelines: %w", err)
		}
		if cc.splitters, err = promscrape.NewSplitters(cfg.Metrics.Splitters); err != nil {
			return nil, fmt.Errorf("metrics.splitters: %w", err)
		}
	}
	if cc.logSources, err = compileSources(cfg.Logs); err != nil {
		return nil, fmt.Errorf("logs.sources: %w", err)
	}
	if cc.logRules, err = compileLogRules(cfg.Logs); err != nil {
		return nil, fmt.Errorf("logs.rules: %w", err)
	}
	if cfg.TraceMetrics != nil {
		if err := cfg.TraceMetrics.Validate(); err != nil {
			return nil, fmt.Errorf("traceMetrics: %w", err)
		}
	}
	if cfg.TraceSampling != nil {
		if err := cfg.TraceSampling.Validate(); err != nil {
			return nil, fmt.Errorf("traceSampling: %w", err)
		}
	}
	// Shape-only, and it validates the policy list by COMPILING it (regexes,
	// budgets, durations), so -check-config accepts exactly what a start does.
	if err := cfg.TailSampling.Validate(); err != nil { // nil-receiver safe
		return nil, fmt.Errorf("tailSampling: %w", err)
	}
	// Both service-graph sections are shape-only (no DNS, no filesystem, no
	// namespace resolution), so the dry run runs exactly what a start does.
	if err := cfg.ServiceGraph.Validate(); err != nil { // nil-receiver safe
		return nil, fmt.Errorf("serviceGraph: %w", err)
	}
	// The shard's receiver takes forwarded spans from every pod in the cluster,
	// so it is refused unauthenticated — HERE rather than at the listener, so
	// -check-config catches it before the StatefulSet CrashLoops. (The chart
	// renders -service-graph with no token flag when no Secret is configured,
	// precisely so this refusal is what an operator sees.) Shape only: whether
	// the file is readable and non-empty is checked at the real start, where
	// it is equally fatal.
	if *serviceGraphOn && strings.TrimSpace(*serviceGraphToken) == "" {
		return nil, errors.New("-service-graph requires -service-graph-token-file: the shard's span receiver is reachable from every pod in the cluster and must not be unauthenticated")
	}
	// A shard with no listener at all receives nothing, pairs nothing, and
	// reports READY forever (the gate is satisfied by the receiver binding, and
	// with neither address there is nothing to bind). sgReceiver.Run refuses it,
	// but only at the real start — so -check-config used to pass a config that
	// CrashLoops the StatefulSet, from the check whose whole purpose is catching
	// exactly that. The SAME const as the runtime refusal, so the wording
	// cannot drift.
	if *serviceGraphOn && *serviceGraphListen == "" && *serviceGraphHTTPListen == "" {
		return nil, errors.New(msgShardNoListener)
	}
	// Two of this process's listeners on one address. Fatal at the real start
	// (the second bind loses), and the chart renders three of the tier's four
	// from values, so it is a one-value mistake — checked here for the same
	// reason as the refusal above.
	//
	// UNCONDITIONAL, not under *serviceGraphOn: the collision is not a tier
	// property. -ingest and -service-graph-ingest default to the same
	// :4317/:4318, so combining them on one process is a CrashLoop the dry run
	// used to sign off on, and -pprof-listen typed onto -metrics-listen's :9090
	// needs no feature flag at all.
	if err := listenersDistinct(); err != nil {
		return nil, err
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
	// workload exit 1 at startup. configWarnings says where it is ignored.
	if *serviceGraphOn {
		shards, err := serviceGraphShardConfig(cfg.ServiceGraphShards)
		if err != nil {
			return nil, err
		}
		// ValidateAgainst, not Validate: the section's own shape AND the
		// per-shard exporter configs NewResharder derives from it over the same
		// flag base a start uses. Checking only the section left every rule that
		// exists after the derivation out of the dry run — TLS material beside a
		// plaintext gRPC hop, a scheme-less explicit endpoint under
		// protocol: http — so -check-config exited 0 and the same ConfigMap then
		// aborted every pod of the tier's StatefulSet at startup.
		if err := shards.ValidateAgainst(baseExportConfig()); err != nil { // its messages already name the section
			return nil, err
		}
		// ReshardConfig.Validate is shape-only by contract and cannot see the
		// flags, so the ring's transport and port are checked against this
		// shard's own listeners here — the one place that knows both.
		if err := shardRingReachesThisShard(shards); err != nil {
			return nil, err
		}
	}
	if cfg.Routing != nil {
		if cc.routes, err = validateRoutes(cfg.Export, cfg.Routing.Routes); err != nil {
			return nil, err
		}
	}
	if cc.transforms, err = compileTransforms(transformsFile); err != nil {
		return nil, fmt.Errorf("transforms: %w", err)
	}
	// Cross-file check: a `type: script` tail-sampling policy is only
	// satisfiable when the transforms file defines a sample: section.
	// Validate() itself uses a placeholder (the injection has not happened at
	// check time), so without this a config that CrashLoops the StatefulSet
	// would pass -check-config.
	//
	// ON THE TIER ONLY, like every other tier-only refusal in this function
	// (the token, the listeners, the shard set) and for the same reason the
	// serviceGraphShards block above records: ONE ConfigMap is shared by the
	// DaemonSet, the events/Azure singleton and the tier, and the chart renders
	// -transforms-file on none of them, so a policy that is valid exactly where
	// it is READ made every other workload exit 1 at startup — the singleton
	// unrepairably, since events.yaml exposes no extraVolumes to mount a
	// transforms file with. Off the tier tailSampling is not read at all, and
	// the config summary's tier-only-sections line (tierOnlySections) is
	// already the right report.
	if *serviceGraphOn && cfg.TailSampling.Enabled() && cfg.TailSampling.UsesScript() && !cc.transforms.HasSample() {
		return nil, errors.New("tailSampling: a `type: script` policy requires -transforms-file with a sample: section defining decide(trace)")
	}
	return &cc, nil
}

// --- per-section compile helpers ---
//
// ONE home per section for "which arguments participate in its validity":
// compileConfig (and through it validateConfig and run()) and the -test-config
// harness's per-case log-metrics set all compile a section through the same
// helper, so an option that participates in validation (the log-metrics name
// prefix) or a new refusal cannot land in one path and not the others. Every
// helper acquires NOTHING (no listeners, no log files, no network;
// compileTransforms reads only the file the flag names, which the dry run
// always did) — run() supplies loggers and classifiers through the extras.

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
// which is why it is compiled by compileConfig rather than inside startLogs —
// journald must get it even with -logs=false.
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
