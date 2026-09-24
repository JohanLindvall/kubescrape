// Command kubescrape-agent is one binary deployed as three workloads, selected
// by its pipeline flags (agentRole names which one a process is):
//
//   - the per-node DaemonSet: tails container (and plain and journal) logs,
//     scrapes the node's Prometheus targets (discovered through the kubescrape
//     metadata service) and the kubelet, and receives OTLP logs and metrics
//     pushed by the node's pods (-ingest);
//   - the cluster singleton Deployment: Kubernetes events (-events) and Azure
//     diagnostics (-azure-diagnostics);
//   - the trace tier StatefulSet (-service-graph): receives the cluster's OTLP
//     traces, derives service-graph and span metrics, and samples.
//
// Everything is enriched with Kubernetes resource attributes from the metadata
// service and exported as OTLP, over gRPC or HTTP, to an OpenTelemetry
// collector.
package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/cgroupstats"
	"github.com/JohanLindvall/kubescrape/internal/agent/debugtap"
	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
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
	"github.com/JohanLindvall/kubescrape/internal/cli"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/selfmeta"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
)

func main() {
	// The process logger cannot exist until -log-level is parsed, and several
	// refusals happen before that (a missing -node-name, an unknown level).
	// Without this they went out through slog's stdlib default, which is not
	// logfmt — so the ONE line that says why the pod will not start was the one
	// line an operator's log pipeline could not parse. Replaced by the leveled
	// logger a few statements into run(). A flag-PARSE error never reaches it:
	// flag.Parse prints its own message and usage and exits 2 itself.
	slog.SetDefault(slog.New(cli.NewLogfmtHandler(os.Stderr, slog.LevelInfo)))
	if err := run(); err != nil {
		slog.Error("kubescrape-agent failed", "error", err)
		os.Exit(1)
	}
}

// agentServiceName is the service.name every metric this process generates
// about ITSELF carries, whichever of its three workloads it is deployed as.
const agentServiceName = "kubescrape-agent"

// agentSelfResource is the agent's own OTLP resource identity, shared by its
// self-metrics and span-metrics exporters (a described service is carried as a
// data-point dimension, not on this resource). It is also what the startup
// summary's "effective identity" line READS (printConfigSummary), so the line
// reports the identity this function stamps rather than a second derivation of
// it — which is why it must stay free of lookups: -check-config calls it too.
func agentSelfResource(node string) pcommon.Resource {
	res := pcommon.NewResource()
	a := res.Attributes()
	a.PutStr("service.name", agentServiceName)
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
	// Keyed on actually BEING the singleton — perNodePipelinesOff, which
	// carries the war story — not merely on the flag.
	//
	// Only when there is a pod name to use: selfPodName() is "" for a hostNetwork
	// pod with no $POD_NAME (or on a hostname error). Putting that empty string
	// would make attrs.Identity treat the key as ALREADY SET and skip its
	// fallback, pinning the process onto (job, instance="") — merged with every
	// other empty-instance process, which is strictly worse than the
	// node/hostname fallback Identity derives when the key is left absent.
	if singletonRole() || shardRole() {
		if inst := selfInstanceName(); inst != "" {
			a.PutStr("service.instance.id", inst)
		}
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
	return perNodePipelinesOff()
}

// perNodePipelinesOff reports that every per-node pipeline is off — the shape
// the cluster-scoped roles (the events/Azure singleton, the trace tier) are
// deployed in, and what the role helpers key their instance identity on rather
// than their flags alone. The distinction is load-bearing: -events added to
// the DaemonSet's extraArgs (a supported way to run it, and the chart's own
// escape hatch) flipped every agent in the fleet from the stable node name to
// a pod name that changes on every restart, resetting each node's whole
// cumulative history.
func perNodePipelinesOff() bool {
	return !*logsOn && !*metricsOn && !*cadvisorOn && !*nodeOn && !*summaryOn && !*journaldOn && !*ingestOn && !*cgroupStatsOn
}

// singletonRole reports whether this process is the cluster-singleton
// deployment (the events / Azure-diagnostics reader) rather than a node agent:
// a cluster-scoped pipeline is on and every per-node one is off, which is
// exactly what charts/kubescrape/templates/events.yaml renders.
func singletonRole() bool {
	if !*eventsOn && !*azureOn {
		return false
	}
	return perNodePipelinesOff()
}

// pipelines bundles what the per-pipeline start functions share: the
// lifecycle primitives (wg/stop), the common sinks and sources, the parsed
// config and what compileConfig compiled from it. Flag reads stay in the start
// functions themselves, except where compileConfig already derived the value
// (the normalised -kubelet-endpoint) — a start must use what was validated.
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
	// debugTap serves the on-demand GET /debug/otlp stream (and its /ui) —
	// always present, costing one atomic load per export while unused.
	debugTap   *debugtap.Tap
	logMetrics *metrics.DynamicMetricSet
	// logRules is the compiled logs.rules chain, shared by every log producer —
	// the tailer, journald, the -ingest receiver, the -events reader and the
	// Azure consumer — so one section selects identically however a line
	// arrived. Compiled by compileConfig, not startLogs, because journald
	// needs it with -logs=false.
	logRules *logline.LineFilter
	// logSources is the validated logs.sources list (compileConfig); nil
	// means the tailer's default containerd source over -log-dir.
	logSources []tailer.Source
	// cgroupSampler is published by startCgroupStats so run() can ship the last
	// sampling window after the sampler has joined. Sampler.Run deliberately
	// does NOT export on cancel: the budget belongs to the shutdown sequence's
	// shared deadline, not to a constant inside the package.
	cgroupSampler *cgroupstats.Sampler
	// spanMetricsGen is published by buildOwnerChain (servicegraph.go) so run()
	// can export the last aggregation window after every producer has joined.
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
	// kubeletBase is -kubelet-endpoint normalised by compileConfig
	// (kubeletBase); "" leaves the kubelet scrapes unscheduled.
	kubeletBase string
	// fatalErr receives a pipeline's fatal failure (the listeners and the
	// events election; see p.fatal's callers). ATOMIC: shutdown joins the
	// producers on a BUDGET (cli.WaitFor), not an unbounded wg.Wait, so a
	// straggler writing past the deadline would race run()'s read — a plain
	// variable's happens-before died with the budget. First writer wins; the
	// agent exits non-zero on whichever failure came first.
	fatalErr *atomic.Pointer[error]
}

// spawn runs fn on the shared WaitGroup.
func (p *pipelines) spawn(fn func()) { p.wg.Go(fn) }

// fatal reports a pipeline's fatal failure and shuts the agent down. It is the
// ONE spelling of the sequence (four call sites used to hand-roll it): the
// first failure wins — fatalErr is read after a BUDGETED shutdown join, so the
// swap must be atomic and never overwrite an earlier writer — and stop is
// ALWAYS called, so the process exits non-zero instead of looking healthy
// while a listener or election is dead. what names the pipeline in both the
// log line and the returned error.
func (p *pipelines) fatal(what string, err error) {
	p.log.Error(what+" failed; shutting down", "error", err)
	ferr := fmt.Errorf("%s: %w", what, err)
	p.fatalErr.CompareAndSwap(nil, &ferr) // first fatal wins
	p.stop()
}

// shutdownTotal bounds the WHOLE shutdown sequence and shutdownStep any single
// step of it. The total is set under the chart's terminationGracePeriodSeconds
// (60s) with room for the kubelet's own overhead: the point is that the summed
// per-step budgets can no longer exceed the grace and get SIGKILLed mid-drain.
const (
	shutdownTotal = 45 * time.Second
	shutdownStep  = 10 * time.Second
)

func run() error {
	flag.Parse()

	if *nodeName == "" {
		return errors.New("node name is required (set -node-name or $NODE_NAME)")
	}

	// stop is also what a pipeline's fatal failure calls (pipelines.fatal);
	// SIGTERM stays handled through the shutdown that follows either way.
	ctx, stop, releaseSignals := cli.ShutdownContext()
	defer releaseSignals() // registered FIRST, so it runs LAST: see cli.ShutdownContext
	defer stop()

	// The process logger, and every other logger in the process routed into
	// it: client-go's klog (the singleton roles -events and its leader election
	// are pure client-go, so its lease churn and watch errors would otherwise
	// go out as glog lines) and grpc-go's grpclog (the OTLP exporter's client,
	// the ingest listeners and the trace tier's three — i.e. every line that
	// says why nothing is reaching the collector). Both unconditional: a binary
	// that logs two formats depending on a flag is worse than either.
	log, err := cli.SetupLogging(*logLevel)
	if err != nil {
		return err
	}
	// First line of every run: without a build identity a panic trace, a
	// metric anomaly or a half-finished rollout cannot be tied to a commit.
	// The optional pipelines ride build tags (buildtags.go), and the Makefile —
	// not the constraint — carries the default that compiles both in. So the
	// binary itself has to say which one it is: a bare `go build` produces an
	// agent with neither, and "the flag is set and nothing happens" must not be
	// a thing anyone has to discover.
	log.Info("kubescrape-agent starting", "version", obs.BuildVersion(), "built", obs.BuildTime(),
		"optionalPipelines", builtPipelines())
	// Bound the Go heap goal by this container's memory limit, before anything
	// large is allocated. All three workloads this binary runs as (the
	// DaemonSet, the events singleton, the trace tier) ship WITH a memory
	// limit, and each has a measured burst shape that the GC would otherwise
	// size against nothing at all.
	cli.SetMemoryLimit(log)

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
	// fails fast and identically whether or not -check-config was passed — and
	// ONCE: everything below consumes what this compiled (compiledConfig). The
	// extras reach only the log-metrics set and change nothing about whether it
	// compiles, so the dry run is still exactly validateConfig's verdict.
	cc, err := compileConfig(fileCfg, *transformsFile,
		// The permanent classifier is injected (the set cannot import
		// otlpexport — obs sits between the packages): a definitively
		// rejected chunk is dropped counted rather than re-offered forever.
		metrics.WithLogger(log), metrics.WithPermanentClassifier(otlpexport.IsPermanent))
	if err != nil {
		return err
	}
	// The effective configuration, then the legal-but-surprising combinations —
	// what this process WILL do, and what about that is worth a second look.
	// BOTH are emitted here, by -check-config and by every real start alike, so
	// a dry run and a rollout can never describe different agents (the
	// discipline validateConfig already holds for the refusals).
	printConfigSummary(fileCfg, log)
	logConfigWarnings(fileCfg, cc.transforms, log)
	if *checkConfig {
		// The verdict, on its own line: everything above is a description, and
		// a dry run's exit status is not visible in a CI log's scrollback.
		log.Info("config is valid")
		return nil
	}
	if *testConfig != "" {
		// Like -check-config: run and exit without acquiring anything.
		return runConfigTests(fileCfg, cc, *testConfig, log)
	}

	// A single positions file, when configured, backs both the log tailer's
	// offsets and the journald cursor.
	var posStore *positions.Store
	if *positionsFile != "" {
		if posStore, err = positions.Open(*positionsFile); err != nil {
			return fmt.Errorf("positions file: %w", err)
		}
	}

	// Optional PII scrubbing, shared by every log path (tailer, journald,
	// events, Azure diagnostics, ingest), compiled by compileConfig.
	if fileCfg.LogScrubbing != nil {
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
	// ONE connection pool to the metadata service for every client this
	// process builds. The self-pod lookup keeps a client of its own (see
	// startSelfPod: a separate Observe hook and cache), but not a TRANSPORT of
	// its own: its 1m refresh sits inside both the client's 90s and the
	// service's 120s idle timeouts, so a second transport held one extra
	// keep-alive connection — and its server goroutine — open on the singleton
	// per agent, forever.
	metaTransport := metaclient.NewTransport()
	meta := metaclient.New(metaclient.Config{
		Base:    *metadataURL,
		Timeout: metaTimeout(),
		// The client is dependency-free by design; feed its outcomes to our
		// metrics.
		Observe:         func(outcome string) { obs.MetadataRequests.WithLabelValues(outcome).Inc() },
		ScrapeAuthToken: scrapeAuthTok,
		Transport:       metaTransport,
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
	// The metric half of /readyz: a gate per subsystem, published from the
	// moment it is required. The probe body already names the pending gates,
	// but only to whoever curls the pod — and the pod nobody can reach is
	// precisely the one holding a rolling update. Registered before any gate
	// exists: the family renders whatever is registered at export time, and an
	// agent with no gates at all exports nothing rather than a fake 1.
	obs.RegisterReadiness(ready.states)
	var metaReady func()
	if *nodeRefresh > 0 {
		// Reaching the metadata service is what separates a working new agent
		// from one that will attribute nothing; with refresh disabled the agent
		// never calls it, so there is nothing to gate on.
		metaReady = ready.gate(gateMetadata)
	}
	nodeInfo := startNodeInfo(ctx, meta, *nodeName, *nodeRefresh, log, metaReady)

	// The pod THIS process runs in, for the resource attributes of the metrics
	// it generates about itself (startSelfPod has the three cases).
	selfPod := startSelfPod(ctx, metaTransport, log)

	baseExport := baseExportConfig()
	// The flag base plus the config's export section: per-signal destinations
	// ride the existing per-signal spools, and the default chain gains static
	// headers / an mTLS client certificate.
	exporter, err := otlpexport.BuildExporter(baseExport, fileCfg.Export)
	if err != nil {
		return fmt.Errorf("creating OTLP exporter: %w", err)
	}
	defer func() {
		// Swallowed until now. A failing Close is the last thing this process
		// can say about its connection to the collector, and it is exactly the
		// moment (a shutdown that has already blown its budget) where an
		// operator is reading the log to find out what was lost.
		if err := exporter.Close(); err != nil {
			log.Warn("closing the OTLP exporter", "error", err)
		}
	}()

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
	// Buffered.Run, held until the stop-and-drain defer below is registered.
	var startBuffered func(context.Context)
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
		// STARTED below, past the stop-and-drain defer, not here. Nothing
		// exports between the two points — the route clients and the transform
		// program are only being BUILT — and starting the drain above that
		// defer is what made the invariant it asserts false: an early return
		// from either of those closed the spools and the exporter under a live
		// drain goroutine, with `defer stop()` (registered far higher up)
		// cancelling its context only afterwards.
		startBuffered = buffered.Run
		out = buffered
		finalDrain = buffered.FinalDrain
		// Make a filling buffer visible BEFORE it starts refusing writes: every
		// other buffer metric only moves once data is already being dropped.
		obs.RegisterBufferStats(buffered.Stats)
		log.Info("disk buffer enabled", "dir", *bufferDir, "maxBytesPerSignal", *bufferMax,
			"traces", traceBuf != nil)
	}

	// Routing sits between transforms and the default delivery chain:
	// producers → transform → router → {default buffered chain | route
	// clients}. An endpoint-less route inherits the whole merged base; a
	// route naming its OWN endpoint keeps only the transport settings, the
	// merged headers and (unless it sets its own `insecure`) the base's
	// plaintext-ness — never the base credentials nor its skip-verify trust
	// decision (routeExportConfig). Per-route destinations are direct (unbuffered) —
	// the default keeps the full durability chain.
	// Captured before the router so the agent's own metrics can keep the
	// default (buffered) chain — see selfSink below.
	//
	// BOTH chains terminate in a Router, even with no destinations, because the
	// Router is the only thing that strips route.ScriptMarker — the reserved
	// attribute a transform script's route() stamps. Left on, it ships to the
	// collector as a resource attribute and changes the stream identity
	// (target_info) of everything the script touched. So a config with no
	// routing: section, and the self chain (which forks below the router by
	// design), used to export the marker verbatim. A destination-less Router
	// costs one attrs.Get per resource: split() returns nil for an unmarked
	// payload and the export takes the uncopied fast path, so the marker-free
	// case stays allocation-free, and a stamped one takes match()'s no-match
	// arm — throttled warn, default chain, marker removed — which is exactly
	// the documented unknown-route behaviour.
	preRoute := route.New(out, nil)
	var dests []route.Destination
	if fileCfg.Routing != nil && len(fileCfg.Routing.Routes) > 0 {
		for i, rt := range fileCfg.Routing.Routes {
			// cc.routes is the SAME derivation -check-config ran
			// (validateRoutes), index for index, so a config the dry run
			// accepts is a config that starts. Named, so the client's own
			// health lines say WHICH route's collector is failing; the router
			// then leaves it to narrate itself rather than saying everything
			// twice (route.New).
			rc, err := otlpexport.New(cc.routes[i], otlpexport.WithReport(nil, "a routing destination", "route", rt.Name))
			if err != nil {
				return fmt.Errorf("routing route %q: %w", rt.Name, err)
			}
			defer func() {
				if err := rc.Close(); err != nil {
					log.Warn("closing a routing destination", "error", err, "route", rt.Name)
				}
			}()
			dests = append(dests, route.Destination{Name: rt.Name, Namespaces: rt.Namespaces, Exporter: rc})
		}
		log.Info("routing enabled", "routes", len(dests))
	}
	out = route.New(out, dests)

	// The on-demand debug stream (GET /debug/otlp + /debug/otlp/ui): between
	// the transforms and the router, so it shows payloads exactly as they
	// will ship — post-transform, routed destinations included. One atomic
	// load per export while nobody is attached. The self-metrics chain
	// (preRoute, captured above) deliberately bypasses it along with the
	// router.
	debugTap := debugtap.New(out)
	out = debugTap

	// Transforms wrap the producer-facing exporter ABOVE the disk buffer:
	// producers → transform → buffer → client, so spooled bytes are final
	// and a reload never re-interprets a durable backlog. Compile fails
	// startup (compileConfig — the program wrapped here is the one it
	// validated, not a second read of a file that may have changed since);
	// reloads compile-then-commit (a broken edit keeps the last good program).
	var transforms *transform.Wrapper
	if prog := cc.transforms; prog != nil {
		traceNext, _ := out.(transform.TracesExporter)
		transforms = transform.Wrap(out, traceNext, prog)
		out = transforms
		// The watcher is started below the stop-and-drain defer too, for the
		// reason given at the buffer's Run.
		log.Info("transforms enabled", "path", *transformsFile, "hash", prog.Hash)
	}

	// Registered AFTER the exporter/spool Close defers (LIFO): an early `return
	// err` below must stop and drain every started goroutine BEFORE their
	// exporter and spools are closed under them.
	//
	// NOTHING THE PRODUCER WG JOINS IS STARTED ABOVE THIS POINT — nothing that
	// exports or touches a spool — and that is what makes the sentence above
	// true rather than aspirational. (The metrics and pprof listeners and the
	// node-info and self-pod pollers ARE started above it: they use neither
	// the exporter nor a spool, and they stop on ctx.
	// TestNoGoroutineIsStartedBeforeTheProducerDrainIsRegistered pins the
	// producer half.) The disk buffer's drain and the transform watcher used
	// to be spawned where they are built, which is above
	// the route-client and transform-compile early returns, so those returns
	// ran the Close defers under two live goroutines and cancelled their context
	// only afterwards (`defer stop()` is registered far higher, so LIFO runs it
	// last). Both are started a few lines below instead; neither exports
	// anything in between.
	//
	// BOUNDED, and on the normal shutdown path CLAMPED TO THE SHARED DEADLINE
	// (shutdownBy, anchored once ctx is cancelled below). This is NOT a no-op on
	// that path when a producer is wedged: the inline join in the shutdown
	// sequence times out and CONTINUES to the final exports rather than finishing
	// the wg, so a fresh shutdownDrain here would stack on top of the inline join
	// and the steps — shutdownDrain + shutdownTotal = 15s + 45s = 60s, EXACTLY
	// the terminationGracePeriodSeconds, SIGKILLed mid-close with nothing spared
	// for the exporter/spool Closes below or the kubelet's own overhead. Clamping
	// to what is left before shutdownBy (shutdownBudget) keeps the whole sequence
	// inside shutdownTotal. On an early return shutdownBy is still zero and the
	// full shutdownDrain is right — no steps ran, so there is nothing to fit
	// under. Missing the deadline costs
	// nothing a producer owns — log offsets, the journal cursor and the events
	// position are all re-read on the next start.
	var shutdownBy time.Time // anchored when the shutdown sequence begins (below)
	// The sibling-shard clients (startServiceGraph) belong with the exporter and
	// spool Closes, not above the drain: registered HERE, LIFO runs them AFTER it,
	// so a started goroutine never has its client closed under it. p is nil until
	// it is built below — an early return before that has nothing to close.
	var p *pipelines
	defer func() {
		if p != nil {
			_ = p.sgResharder.Close() // nil-receiver safe
		}
	}()
	defer func() {
		stop()
		budget := shutdownBudget(shutdownDrain, shutdownBy)
		if !cli.WaitFor(&wg, budget) {
			log.Warn("producers did not stop within the shutdown budget; closing anyway",
				"budget", budget)
		}
		// The backstop for an EARLY return: shutdownBy is set exactly when the
		// shutdown sequence begins, and that sequence always flushes the tail
		// buffer, so a zero one means run() returned an error after the trace
		// tier's receivers had started acking — spans no sender still holds.
		// The debug guard and startResharder are acquired before any listener
		// serves, but startEvents, startAzure and startCgroupStats still run
		// after the tier's receivers are up and can each return an error (a
		// kubeconfig, an unreadable connection-string file, an unusable
		// -cgroup-stats-root) — that, and any start added later, is this case.
		// Before the exporter and spool Closes (registered earlier, so run
		// later), and flushed into the spool when there is one.
		if shutdownBy.IsZero() && p != nil && p.tailBuffer != nil {
			fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownStep)
			p.tailBuffer.Flush(fctx)
			cancel()
		}
	}()

	// The two goroutines the sections above deliberately did not start. Below
	// the drain defer, so every started goroutine is joined before the exporter
	// and the spools it uses are closed.
	if startBuffered != nil {
		wg.Go(func() { startBuffered(ctx) })
	}
	if transforms != nil {
		wg.Go(func() { transform.Reload(ctx, transforms, *transformsFile, 0, log) })
	}

	// The sink for the metrics the agent generates ABOUT ITSELF: this pod's own
	// Kubernetes attributes filled in where the agent's identity left a key
	// unset, over the chain selfSink picks.
	selfOut := selfmeta.Wrap(selfSink(preRoute, transforms), selfPod,
		selfBuild(cc.attrs.Self, nodeInfo))

	var selfRes pcommon.Resource
	if *selfMetricsIntv > 0 {
		selfRes = agentSelfResource(*nodeName)
		wg.Go(func() {
			// Handoff: the Registry renders fresh pdata per export and never
			// re-offers a failed payload, but internal/metrics cannot import
			// the transform package (transform → obs → metrics), so the mark
			// rides in from here. Consumed: a failed export re-arms the series
			// and the next one renders new points, never these.
			obs.Registry.Run(transform.Consumed(ctx), selfOut, *selfMetricsIntv, selfRes, log)
		})
		log.Info("self-metrics export started", "interval", *selfMetricsIntv)
	}

	// Optional metrics derived from log lines; only these configured metrics are
	// exported (over the shared OTLP exporter), on their own interval.
	logMetrics := cc.logMetrics
	if transforms != nil && logMetrics != nil {
		// The emit_metric bridge: scripts observe into DECLARED logMetrics
		// series. Guarded on the typed value — a nil *DynamicMetricSet boxed
		// into the interface would defeat the builtin's own nil check.
		transforms.SetMetricEmitter(logMetrics)
	}
	if logMetrics != nil {
		// The refused-observation counters belong to THIS set (they used to be
		// process globals); publish them now that one exists.
		obs.RegisterLogMetricsDrops(logMetrics)
		wg.Go(func() {
			// Handoff for the transform seam: the set renders fresh pdata per
			// export, and its failed-chunk retention keeps raw SAMPLES that the
			// next export re-renders (metrics/export.go retain) — never the
			// pdata. Marked here because internal/metrics cannot import the
			// transform package (transform → obs → metrics). A plain Handoff,
			// NOT Consumed: the retained samples come back as the SAME points,
			// so a failed chunk's script drops are counted on its delivery.
			logMetrics.Run(transform.Handoff(ctx), out, *logsMetricsEvery, *logsMetricsBytes)
		})
		log.Info("log-derived metrics started", "metrics", logMetrics.Count, "interval", *logsMetricsEvery)
	}

	// A fatal pipeline failure (the listeners and the events election; see
	// p.fatal's callers) is stored here and returned after shutdown so the
	// agent exits non-zero.
	// Atomic because the shutdown join is BUDGETED: see pipelines.fatalErr.
	var fatalErr atomic.Pointer[error]

	p = &pipelines{
		wg:           &wg,
		stop:         stop,
		log:          log,
		out:          out,
		selfOut:      selfOut,
		selfPod:      selfPod,
		meta:         meta,
		nodeInfo:     nodeInfo,
		attrBuilders: cc.attrs,
		fileCfg:      fileCfg,
		posStore:     posStore,
		ready:        ready,
		logAttrs:     cc.logAttrs,
		scrub:        cc.scrub,
		transforms:   transforms,
		debugTap:     debugTap,
		logMetrics:   logMetrics,
		logRules:     cc.logRules,
		logSources:   cc.logSources,
		ingestMode:   otlpingest.MetricsMode(*ingestMetrics), // checkFlagChoices vetted it
		filters:      cc.metricFilters,
		splitters:    cc.splitters,
		kubeletBase:  cc.kubeletBase,
		fatalErr:     &fatalErr,
	}
	// ACQUIRE BEFORE SERVING. The debug token's read is FATAL, and it used to
	// happen in startDebugServer — the LAST start below, after the ingest
	// listeners and the trace tier's receivers were already acking pushes. The
	// tier's tail buffer acks BEFORE it decides, so an unreadable token file
	// returned out of run() with spans a sender had been told had landed still
	// buffered (the drain defer's flush below is the backstop, not the plan).
	var guard *debugGuard
	if *listen != "" {
		if guard, err = newDebugGuard(ctx, *debugToken, log); err != nil {
			return fmt.Errorf("-debug-token-file: %w", err)
		}
	}
	tl := p.startLogs(ctx)
	if err := p.startJournald(ctx); err != nil {
		return err
	}
	if err := p.startIngest(ctx); err != nil {
		return err
	}
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
	if err := p.startCgroupStats(ctx, sc); err != nil {
		return err
	}
	p.startDebugServer(ctx, guard, tl, sc)

	// Every gate is registered by now, so the watchdog can report the whole set:
	// one Info line when the agent becomes ready, and a repeating Warn naming
	// the gates that will not clear. A DaemonSet rollout stopped at the first
	// node is otherwise a process that logged "started" for every pipeline and
	// then went quiet.
	p.spawn(func() { ready.watch(ctx, log) })

	<-ctx.Done()
	log.Info("shutting down")
	shutdownStart := time.Now()
	// One DEADLINE for the whole shutdown sequence, rather than a fixed budget
	// per step. Summed literals could exceed the pod's termination grace on a
	// fully-configured trace tier — each step is individually reasonable and
	// the total is not — after which the kubelet SIGKILLs mid-drain and the
	// steps that had not run yet lose their data anyway. Sharing a deadline
	// means a slow step spends the budget the later ones would have had, and
	// nothing overruns.
	//
	// Anchored BEFORE the producer join below, which is part of the sequence and
	// can itself spend shutdownDrain. Anchoring it after made the real worst
	// case shutdownDrain + shutdownTotal = 15s + 45s = 60s, i.e. EXACTLY the
	// terminationGracePeriodSeconds every shipped manifest sets, leaving nothing
	// for the deferred exporter/spool closes or for the kubelet's own overhead —
	// so the last step in the sequence, the disk-buffer drain, was the one
	// SIGKILLed. Now the whole thing fits in shutdownTotal with the grace period
	// to spare, which is what the constant's comment always claimed.
	shutdownBy = time.Now().Add(shutdownTotal)
	// BOUNDED. Everything that salvages in-memory state runs after this: the
	// final log-metrics window (DynamicMetricSet.Run deliberately does not
	// export on cancel, so this is its only chance), the final span-metrics
	// export, the self-metrics FinalExport and the disk-buffer drain. A
	// producer stuck retrying against a dead collector would otherwise hold
	// the whole sequence past the kubelet's grace period and lose all of it to
	// SIGKILL. Producers that miss the deadline lose nothing they own: log
	// offsets, the journal cursor and the events position all re-read.
	if drain := shutdownBudget(shutdownDrain, shutdownBy); !cli.WaitFor(&wg, drain) {
		log.Warn("producers did not stop within the shutdown budget; continuing with the final exports",
			"budget", drain)
	}
	deadlineWarned := false
	stepBudget := func() time.Duration {
		budget := shutdownBudget(shutdownStep, shutdownBy)
		// A step reached with nothing left does not fail loudly — it gets an
		// already-dead context and returns instantly — so a blown deadline is
		// otherwise indistinguishable from a fast, clean shutdown. It costs
		// real data here (the last log-metrics window, the last span-metrics
		// window, the tail-sampling flush, the disk-buffer drain), so it gets a
		// line. Once, not per step: the operator needs the fact, not six copies.
		if budget == 0 && !deadlineWarned {
			deadlineWarned = true
			log.Warn("shutdown deadline exceeded; the remaining final exports get no budget and their windows are lost",
				"budget", shutdownTotal)
		}
		return budget
	}
	// step runs ONE shutdown step on its own context and cancels that context
	// the moment the step returns: the one spelling of a step (six sites used to
	// hand-roll the context and its cancel, one of them as a defer that held its
	// timer until run() returned). The context is DETACHED — every final flush
	// below must outlive the cancellation that triggered it — but via
	// WithoutCancel, never a bare context.Background(), which silently strips
	// whatever the caller put on the context. otlpexport.Own's durability
	// marker rides there, and the tail-sampling flush depends on it reaching
	// the buffer; WithoutCancel is harmless where no marker exists and correct
	// where one does. Its budget is the shared deadline's (stepBudget), capped
	// further at limit — shutdownStep for every step but the one with a
	// tighter bound of its own.
	step := func(limit time.Duration, fn func(context.Context)) {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), min(limit, stepBudget()))
		defer cancel()
		fn(sctx)
	}
	if p.tailBuffer != nil {
		// The one shutdown step that salvages ACKED data rather than a last
		// aggregation window: the tail-sampling buffer holds spans whose senders
		// were told they had landed, and nothing else holds a copy. The receivers
		// have been ASKED to stop (wg above), which is weaker than stopped: the
		// join budget can expire while gRPC's GracefulStop still waits on a slow
		// RPC, and http.Server.Shutdown never interrupts an active handler — so a
		// straggler can still push after this Flush. Flush latches the buffer
		// against exactly that: a post-Flush push is decided immediately on the
		// spans present and its keeps ride out on its own ack (tailbuffer's
		// shutdown bullet), so nothing silently re-fills a buffer nobody will
		// flush again. Decide everything now and let the keeps reach the exporter
		// (and, with -buffer-dir, the final drain below). Budgeted like the rest —
		// a dead collector must not outlive the pod's termination grace, and what
		// it costs is counted as lost.
		step(shutdownStep, p.tailBuffer.Flush)
	}
	if logMetrics != nil {
		// The tailer's final flush (inside wg.Wait) fed the set; export the
		// last window before the deferred exporter/buffer close.
		step(shutdownStep, func(sctx context.Context) {
			if err := logMetrics.Export(transform.Handoff(sctx), out, *logsMetricsBytes); err != nil {
				log.Warn("final log-metrics export failed", "error", err)
			}
		})
	}
	if p.cgroupSampler != nil {
		// The last sampling window — up to a whole -scrape-interval of burst
		// data, including the final seconds of anything that vanished just
		// before shutdown, which is exactly the OOM-killed container this
		// pipeline exists for. Sampler.Run exports nothing on cancel by design:
		// the budget is this sequence's shared deadline, not a constant the
		// package would have to guess (the same correction internal/metrics'
		// FinalExport already carries).
		step(shutdownStep, func(sctx context.Context) {
			if err := p.cgroupSampler.FinalExport(transform.Consumed(sctx), p.out); err != nil {
				log.Warn("final cgroup-stats export failed", "error", err)
			}
		})
	}
	if p.spanMetricsGen != nil {
		// Generator.Run does its final export when ctx is cancelled, but the
		// ingest server's GracefulStop completes in-flight RPCs AFTER that,
		// and every trace they forward passes through the tap, bumping the
		// cumulative series. Those spans ship; without this their RED metrics
		// would not.
		step(shutdownStep, func(sctx context.Context) {
			if err := p.spanMetricsGen.Export(sctx, p.selfOut, p.spanMetricsRes); err != nil {
				log.Warn("final span-metrics export failed", "error", err)
			}
		})
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
			// SweepAll is the name the shutdown path spells this by; it and
			// Sweep are one function now, since the ticker's pass had the same
			// bug and got the same fix. So this is no longer a choice between
			// two behaviours — what it asks for is the behaviour BOTH have: a
			// drain of everything due, in bounded lock holds. A pass capped at
			// one hold silently discarded the remainder on a busy tier, and
			// those are edges the shutdown path claims to emit.
			p.serviceGraphProc.SweepAll()
		}
		step(shutdownStep, func(sctx context.Context) {
			if err := p.serviceGraphReg.Export(sctx, p.selfOut, p.serviceGraphRes); err != nil {
				log.Warn("final service-graph export failed", "error", err)
			}
		})
	}
	if *selfMetricsIntv > 0 {
		// Registry.Run's own final export raced the final flushes inside
		// wg.Wait; counters they bumped (last batches, shutdown drops) would
		// otherwise die unexported. One more export now that everything is done.
		// Budgeted here, like every other final export above: ctx is cancelled
		// by this point, and a dead collector must not outlive the pod's grace.
		step(metrics.FinalExportTimeout, func(sctx context.Context) {
			obs.Registry.FinalExport(transform.Consumed(sctx), selfOut, selfRes, log)
		})
	}
	if finalDrain != nil {
		// Everything above only reached the SPOOL: Buffered.Run stopped when
		// ctx was cancelled. Empty it now, before the deferred exporter and
		// spool Closes, or this window waits for the next start of this pod on
		// this node — and is lost outright if the pod never comes back or the
		// buffer dir is not persistent. Bounded: a dead collector must not
		// outlive the pod's termination grace.
		step(shutdownStep, finalDrain)
	}
	// The one line that says how the shutdown FIT: an operator sizing
	// terminationGracePeriodSeconds, or reading a pod that was SIGKILLed, needs
	// the elapsed time against the budget, and the deadline warning above
	// fires only once it has already been blown.
	log.Info("shutdown complete", "elapsed", time.Since(shutdownStart).Round(time.Millisecond),
		"budget", shutdownTotal, "deadlineExceeded", deadlineWarned)
	if ferr := fatalErr.Load(); ferr != nil {
		return *ferr
	}
	return nil
}

// startLogs starts the container/plain-file log tailer. The returned Tailer
// (nil when -logs is off) is exposed on /debug/tailer.
func (p *pipelines) startLogs(ctx context.Context) *tailer.Tailer {
	if !*logsOn {
		return nil
	}
	cfg := tailer.Config{
		Dir:               *logDir,
		Sources:           p.logSources,
		Positions:         p.posStore,
		Chain:             p.logChain(),
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
		Multiline:         *multilineOn,
		MultilineTimeout:  *multilineWait,
		FileAttributes:    *logsFileAttrs,
		ExcludeNamespaces: cli.SplitList(*excludeNs),
		Attrs:             p.attrBuilders.Logs,
		NodeInfo:          p.nodeInfo,
		MetadataWait:      *metadataWait,
		Metadata:          p.meta,
		Exporter:          p.out,
		Logger:            p.log,
	}
	if p.transforms != nil {
		// The tailer transforms each batch ITSELF — once, in place, before its
		// retry loop — and exports through the chain BELOW the transform layer:
		// its retries re-send the SAME payload, which through the wrapper paid a
		// deep copy plus a script run per attempt. Only the tailer takes this
		// shape; journald/events/azurediag keep the wrapped `out` (their
		// logchain.Pending re-offers the same payload, which needs the copy),
		// and the self chain still sees the shared reloaded program via Fork.
		cfg.Transform = p.transforms.TransformLogs
		cfg.Exporter = p.transforms.Inner()
		// The parse: hook, consulted only for sources flagged parseScript
		// (the adapter keeps the tailer free of a transform dependency).
		w := p.transforms
		cfg.ParseLine = func(line string) (tailer.ParsedLine, bool) {
			parsed, ok := w.ParseLine(line)
			if !ok {
				return tailer.ParsedLine{}, false
			}
			return tailer.ParsedLine{
				Body:         parsed.Body,
				HasBody:      parsed.HasBody,
				SeverityText: parsed.SeverityText,
				TimeUnixNano: parsed.TimeUnixNano,
			}, true
		}
	}
	tl := tailer.New(cfg)
	p.spawn(func() {
		tl.Run(ctx)
	})
	// The no-persistence warning is NOT here: it also describes journald, which
	// runs with -logs=false, so behind this function's toggle it could never
	// reach the deployment it was written for. It is a configWarnings entry now
	// (configwarn.go), which -check-config reports too.
	p.log.Info("log tailer started", "dir", *logDir, "positionsFile", *positionsFile)
	return tl
}

// startJournald and startAzure live in build-tag-gated file pairs
// (journald_enabled.go / journald_disabled.go, azure_enabled.go /
// azure_disabled.go): each pipeline is compiled in by the POSITIVE tag of its
// name, which the Makefile's TAGS sets by default. See buildtags.go.

// gateIngest is satisfied when the ingest listeners are BOUND. Apps on the node
// push into them, so a rolling update that advanced before they bound moved
// across the fleet while every node's receiver was still a void.
const gateIngest = "otlp-ingest"

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
	ecfg := p.enricherBase()
	// The DaemonSet's deltas: metrics mode applies to a signal this receiver
	// serves, and NodeInfo is the agent's own node — correct here because the
	// agent only ever receives from pods on it (the tier's construction leaves
	// it nil, for the reason recorded there).
	ecfg.MetricsMode = p.ingestMode
	ecfg.NodeInfo = p.nodeInfo
	// Traces: nil, so neither the gRPC trace service nor POST /v1/traces is
	// served here. A sender pointed at the agent for traces gets Unimplemented /
	// 404 — a loud, immediate error naming the wrong destination — rather than an
	// ack for spans that could never have become an edge.
	scfg := otlpingest.ServerConfig{
		GRPCAddr: *ingestGRPC,
		HTTPAddr: *ingestHTTP,
		Exporter: p.out,
		// The operator's cost levers reach pushed logs too: the same compiled
		// logs.rules chain and the same logMetrics set the tailer, journald,
		// events and Azure producers run — one config, one behavior, however
		// the line arrived.
		//
		// logAttributes rides along for the same reason, and it is the one that
		// makes the other two agree with the tailer: a rule that RENAMES a line
		// key (`attribute:` != `key:`, the documented canonical use) is what a
		// rule or a metric label then selects on, so without the extractor here
		// an allowlist ruleset silently DISCARDED pushed records the tailer
		// keeps. The receiver applies the `target: log` half only — see
		// ServerConfig.LogAttrs for why the resource and scope halves must not
		// be written onto a grouping the sender owns. The scrubber and the
		// line enrichment are the same chain's first steps (the producers'
		// logchain.Config.Scrub/Enrich), so they sit here beside it.
		Scrub:       p.scrub,
		EnrichLines: *enrichOn,
		Rules:       p.logRules,
		LogAttrs:    p.logAttrs,
		LogMetrics:  p.logMetrics,
	}
	// The gate only where something binds: with neither address configured the
	// listeners are a no-op and Ready never fires, so registering it anyway
	// would hold /readyz down for the process lifetime.
	if *ingestGRPC != "" || *ingestHTTP != "" {
		scfg.Ready = p.ready.gate(gateIngest)
	}
	srv := p.newAppIngestServer(ecfg, scfg)
	p.spawn(func() {
		if err := srv.Run(ctx); err != nil {
			// A dead ingest listener (e.g. the port already bound) must not
			// leave the agent looking healthy while apps push into a void:
			// shut the agent down and exit non-zero so the failure is
			// visible (CrashLoop).
			p.fatal("otlp ingest server", err)
		}
	})
	p.log.Info("otlp ingest started", "ingestGRPC", *ingestGRPC, "ingestHTTP", *ingestHTTP, "metricsMode", *ingestMetrics)
	return nil
}

// ingestReservedAttrs is the strip list every APPLICATION-FACING receiver
// wires — the DaemonSet's ingest listeners (startIngest) and the trace tier's
// application ports (startServiceGraphIngest); the tier's internal receiver is
// kubescrape-to-kubescrape and deliberately not on it.
//
// It has two halves, and only the first is about kubescrape's own PLUMBING.
// Both consumers of those keys are presence-only and cannot tell kubescrape's
// own mark from a sender's: the router honors route.ScriptMarker on a RESOURCE
// before its namespace globs, so a wire-supplied copy selects any configured
// route and that route's tenant headers, and the transform prune deletes any
// ELEMENT carrying transform.DropMarker whenever a script for its signal is
// active, counting it as an operator-intended drop. Those lists are minimal
// and true — the router never reads its marker off an element, so ScriptMarker
// is NOT in Element.
//
// The second half is the sender's IDENTITY CLAIM (Enricher.SenderIdentityStrip,
// which owns the argument): on a listener with no credentials, a pod declaring
// someone else's k8s.namespace.name has its records routed to that tenant's
// endpoint under that tenant's headers, and the k8s.pod.*/k8s.node.name/
// container.* siblings are the keys a resolved lookup overwrites anyway. It is
// asked of the ENRICHER rather than spelled here because the strip runs BEFORE
// enrichment, so it must exclude this receiver's own lookup keys — stripping
// those would resolve nothing at all, which is the trap in "strip every
// identity key".
//
// It rides ReservedAttrs.Identity rather than being appended to Resource,
// because the two are not the same event: a plumbing marker on the wire is a
// key only kubescrape has a reason to set, while these are what every
// conformant SDK ships — appending them to Resource gave a healthy cluster a
// climbing kubescrape_ingest_reserved_stripped_total and a throttled Warn per
// key accusing honest senders of shipping kubescrape's plumbing.
//
// One derivation, for enricherBase's reason: spelling it twice is how the two
// receivers drift.
func ingestReservedAttrs(enr *otlpingest.Enricher) otlpingest.ReservedAttrs {
	return otlpingest.ReservedAttrs{
		Resource: []string{route.ScriptMarker},
		Element:  []string{transform.DropMarker},
		Identity: enr.SenderIdentityStrip(),
	}
}

// newAppIngestServer builds an APPLICATION-FACING OTLP receiver — the
// DaemonSet's -ingest listeners (startIngest) and the trace tier's application
// ports (startServiceGraphIngest) — from the caller's enricher config and its
// ServerConfig deltas (addresses, what it serves, the log chain, Ready), and
// fills in the admission base the two SHARE. One construction, for
// enricherBase's reason one level down: the two receivers were spelled out
// field by field and each field is a place for them to drift apart.
func (p *pipelines) newAppIngestServer(ecfg otlpingest.Config, scfg otlpingest.ServerConfig) *otlpingest.Server {
	enr := otlpingest.NewEnricher(ecfg)
	scfg.Enricher = enr
	// The admission knobs, the same on both: trace pushes are the LARGEST
	// payloads a fleet sends, so the raised message cap matters on the tier's
	// ports first.
	scfg.MaxInFlight = *ingestMaxInFlight
	scfg.MaxRecvBytes = *ingestGRPCMaxRecv
	// Wire-supplied copies of kubescrape's plumbing keys — and of the
	// resolved-identity keys this receiver derives itself — die at receipt
	// (ingestReservedAttrs): the router and the transform prune cannot tell
	// them from kubescrape's own, and neither can routing tell a forged
	// namespace from a resolved one. Both ports are first receipt; the tier's
	// INTERNAL receiver (sgReceiver) is not built here and deliberately does
	// not strip — what arrives there was sanitized when an application pushed
	// it, and re-stripping would delete the identity the entry shard resolved.
	scfg.ReservedAttrs = ingestReservedAttrs(enr)
	if p.transforms != nil {
		// The ingest: admission hook (per resource, pre-enrichment; hot reload
		// adds/removes it without a restart — AdmitResource resolves the active
		// program per call and admits when no hook exists). Its contract covers
		// all three signals, and trace pushes arrive on the tier's ports.
		scfg.Admit = p.transforms.AdmitResource
	}
	scfg.Logger = p.log
	return otlpingest.NewServer(scfg)
}

// logChain is the per-record log chain configuration (scrub → lift → enrich →
// log-metrics → rules) the tailer, journald, events and Azure producers share.
// ONE construction, so a lever added to logchain.Config reaches all four rather
// than whichever start function remembered it.
func (p *pipelines) logChain() logchain.Config {
	return logchain.Config{
		Scrub:      p.scrub,
		LogAttrs:   p.logAttrs,
		Enrich:     *enrichOn,
		LogMetrics: p.logMetrics,
		Rules:      p.logRules,
	}
}

// enricherBase is the flag-derived subset of the ingest enricher's config that
// the DaemonSet's receiver (startIngest) and the trace tier's application
// listeners (startServiceGraphIngest) SHARE. Each caller applies its own
// deltas on top — the DaemonSet its metrics mode and node info, the
// tier its own-workload peer veto (NodeInfo staying nil there, for the reason
// recorded at that call site). One derivation, because spelling the base twice
// is how the two constructions drift apart field by field.
func (p *pipelines) enricherBase() otlpingest.Config {
	return otlpingest.Config{
		ContainerIDKeys: cli.SplitList(*ingestCidKeys),
		PodUIDKeys:      cli.SplitList(*ingestUIDKeys),
		Wait:            *ingestWait,
		PeerIPFallback:  *ingestPeerIP,
		Attrs:           p.attrBuilders.Ingest,
		Meta:            p.meta,
		Logger:          p.log,
	}
}

// startScraper starts the Prometheus scraper (annotation/ServiceMonitor
// targets and/or the kubelet's cadvisor, node-metrics and stats-summary
// scrapes). The returned Scraper (nil when scraping is off) is exposed on
// /debug/targets.
func (p *pipelines) startScraper(ctx context.Context) *promscrape.Scraper {
	// The endpoint gates all three kubelet scrapes: with it empty they are not
	// disabled, they are never scheduled. configWarnings names that, because
	// nothing else can — no counter moves for a scrape that never ran.
	//
	// NORMALISED, by compileConfig (kubeletBase): an IPv6 host arrives bare
	// from `https://$(NODE_IP):10250` (status.hostIP, which the chart and the
	// shipped manifests both use, and which cannot be pre-bracketed without
	// breaking every IPv4 cluster) and every request built from it would be
	// refused by net/url before it went out. The flag is read there and only
	// there, so the value that passed -check-config is the value scraped.
	kubeletEP := p.kubeletBase
	kubeletScrapes := kubeletEP != "" && (*cadvisorOn || *nodeOn || *summaryOn)
	var sc0 *promscrape.Scraper
	if *metricsOn || kubeletScrapes {
		var targetHook func([]kubemeta.ScrapeTarget) []kubemeta.ScrapeTarget
		if p.transforms != nil {
			// The targets: hook — per fetched target, once per cycle.
			targetHook = p.transforms.TransformTargets
		}
		sc := promscrape.New(promscrape.Config{
			Node:           *nodeName,
			TargetHook:     targetHook,
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
				Endpoint:       kubeletEP,
				Cadvisor:       *cadvisorOn,
				DisableRollups: !*rollupsOn,
				NodeMetrics:    *nodeOn,
				Summary:        *summaryOn,
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
			"targets", *metricsOn, "cadvisor", kubeletScrapes && *cadvisorOn, "nodeMetrics", kubeletScrapes && *nodeOn,
			"summary", kubeletScrapes && *summaryOn)
		sc0 = sc
	}
	return sc0
}

// startCgroupStats starts the high-frequency cgroup sampler (-cgroup-stats).
//
// It takes the Scraper because the sampler must build its resources through the
// SAME code the cadvisor scrape does — see promscrape.FillContainerResource for
// why a second implementation would be worse than useless. sc is nil when every
// scrape pipeline is off, and the sampler is still perfectly valid then (it is
// the only container CPU/memory signal on such an agent), so a Scraper is
// constructed for its resolver alone: promscrape.New starts no goroutines,
// binds nothing and dials nothing until Run, so what is built here is the
// metadata cache and the attribute builder and nothing else.
func (p *pipelines) startCgroupStats(ctx context.Context, sc *promscrape.Scraper) error {
	if !*cgroupStatsOn {
		return nil
	}
	resolver := sc
	if resolver == nil {
		resolver = promscrape.New(promscrape.Config{
			Node:     *nodeName,
			Attrs:    p.attrBuilders,
			NodeInfo: p.nodeInfo,
			Logger:   p.log,
			Kubelet:  promscrape.KubeletConfig{Meta: p.meta},
		})
	}
	s, err := cgroupstats.New(cgroupstats.Config{
		Root:             *cgroupRoot,
		Interval:         *cgroupStatsIv,
		DiscoverInterval: *cgroupDiscoverIv,
		Resolver:         resolver,
		Logger:           p.log,
	})
	switch {
	case errors.Is(err, cgroupstats.ErrUnsupportedNode):
		// A property of the NODE, not of anything the operator typed: this node
		// runs cgroup v1 (at any root — including the chart's default explicit
		// /host/sys/fs/cgroup), or exposes no cgroup hierarchy at all at the
		// default root, and no amount of waiting changes it. DEGRADE — one
		// pipeline off, every other one running.
		//
		// It used to be fatal, and that made enabling one flag on a MIXED FLEET
		// take the LOG pipeline down on every v1 node: the DaemonSet pod
		// CrashLoops, so the node stops shipping logs, for a metric. And
		// -check-config cannot catch it in advance, because the cgroup version
		// is a property of the node the pod lands on. An explicit
		// -cgroup-stats-root with no usable cgroup hierarchy behind it stays
		// fatal below: that one is an operator error, identical on every node.
		p.log.Error("cgroup stats are not available on this node; the pipeline is disabled and every other pipeline keeps running",
			"error", err, "flag", "-cgroup-stats", "root", cmp.Or(*cgroupRoot, cgroupstats.DefaultRoot))
		return nil
	case err != nil:
		return fmt.Errorf("cgroup stats: %w", err)
	}
	// Registered only now, so a published 0 means "running and finding
	// nothing" rather than "off" (obs.RegisterCgroupStats).
	obs.RegisterCgroupStats(s.Containers, s.Unresolved)
	p.cgroupSampler = s
	p.spawn(func() {
		// Handoff for the transform seam: every export renders fresh pdata from
		// a window that is reset as it is rendered, and a failed payload is
		// never re-offered, so a script may run in place instead of paying a
		// deep copy. Marked here rather than inside the package for the reason
		// internal/metrics is (the mark belongs to the call site that knows the
		// retry policy). Consumed: the windows a failed export rendered are
		// gone, so its script drops are counted then or never.
		s.Run(transform.Consumed(ctx), p.out, *scrapeInterval)
	})
	// The EFFECTIVE periods, not the flag values: New clamps a sub-floor one,
	// and the line that says what this pipeline is doing must not report what
	// was asked for instead.
	p.log.Info("cgroup sampler started", "root", s.Root(), "interval", s.Interval(),
		"discoverInterval", s.DiscoverInterval(), "window", *scrapeInterval, "cgroups", s.Discovered())
	return nil
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
func startNodeInfo(ctx context.Context, meta *metaclient.Client, nodeName string, refresh time.Duration, log *slog.Logger, onReady func()) func() *attrs.NodeInfo {
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
		// it collects: the readiness gate a rolling update waits on (onReady is
		// readiness.gate's done func; nil when the lookup is disabled and run()
		// registered no gate).
		OnFirst: func(*attrs.NodeInfo) {
			if onReady != nil {
				onReady()
			}
		},
		Log: log,
	})
}

// shutdownDrain bounds the producer join. On the shutdown path it is clamped
// to the shared shutdownTotal deadline (shutdownBudget), so it spends that
// budget rather than extending the sequence — shutdownTotal alone is what has to fit inside the pod's
// terminationGracePeriodSeconds, which the manifests set explicitly. On an
// early return from run(), before shutdownBy is anchored, it applies in full.
const shutdownDrain = 15 * time.Second

// shutdownBudget is what one wait or step of the shutdown sequence may spend:
// limit, clamped to what is left before by — the shared shutdownTotal deadline —
// and never below zero, because a sequence that has spent the deadline has
// nothing left to give, and a negative budget is not a wait but a number in a
// log line that reads as one. A zero by means the deadline is not anchored yet
// (run() returned early, before the sequence began), where limit applies in
// full. The ONE spelling of the clamp: the producer join, the drain defer and
// every step used to hand-roll it, and one copy went negative.
func shutdownBudget(limit time.Duration, by time.Time) time.Duration {
	if by.IsZero() {
		return limit
	}
	return max(0, min(limit, time.Until(by)))
}

// metaTimeout is every metadata client's HTTP timeout: it must exceed the
// LONGEST server-side wait a lookup can ask for — the container wait AND the
// ingest lookups' own wait, which may be longer — plus headroom for the
// response itself. One derivation, because the shared client and the self
// lookup's private client (see run) must agree on it.
func metaTimeout() time.Duration {
	return max(*metadataWait, *ingestWait) + 10*time.Second
}

// baseExportConfig is the exporter configuration the OTLP flags describe. It
// is a function so -check-config can validate it without building anything:
// the dry run returns long before the exporter is assembled, so every one of
// these flags used to be unchecked by a run whose whole purpose is catching a
// bad ConfigMap before it becomes a fleet-wide CrashLoop.
func baseExportConfig() otlpexport.Config {
	cfg := otlpexport.ConfigFromFlags(otlpFlags)
	cfg.RetryAttempts = *otlpRetries
	cfg.RetryBackoff = *otlpBackoff
	cfg.MaxSendBytes = *otlpMaxSendBytes
	return cfg
}

// selfSink picks the export chain for the metrics the agent generates about
// ITSELF: the PRE-ROUTING chain (preRoute, captured above the debug tap and
// the router), forked through the shared transform program when transforms
// are on.
//
// The bypass is UNCONDITIONAL. The router fans out by the k8s.namespace.name
// on the resource, and these resources only acquired one when self-attributes
// started stamping it — so a route globbing the agent's own namespace would
// silently move the fleet's own health signal off the durable buffered chain
// onto an unbuffered per-tenant destination, and would do it only from the
// moment the lookup resolved. The debug tap is bypassed with the router: it
// sits between the transforms and the router, and gating this pick on whether
// any route was configured made /debug/otlp show the agent's own metrics
// exactly when no routing section existed and silently omit them once a route
// was added — a config-dependent difference in a debug surface, denied by
// every comment describing this chain. Transforms still apply: the fork
// shares the reloaded program, so the two chains can never run different
// scripts.
func selfSink(preRoute otlpexport.Exporter, transforms *transform.Wrapper) selfmeta.Exporter {
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
