package main

// The startup summary: the "effective ..." Info lines every real start and
// -check-config emit from one function (printConfigSummary), so a dry run and a
// rollout cannot describe different agents.

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpingest"
	"github.com/JohanLindvall/kubescrape/internal/cli"
)

// printConfigSummary is the EFFECTIVE CONFIGURATION dump: what this process
// will do, where it will send it, what it will listen on, who it thinks it is,
// and the knobs most likely to be wrong.
//
// It is emitted by every real start AND by -check-config, from one function, so
// the dry run and the start cannot describe different agents — the same
// discipline validateConfig already holds for the refusals. On a first live run
// this is the one thing an operator can grep to answer "is it even configured
// the way I think?" before any pipeline has produced a byte.
//
// A few lines rather than one: an operator greps a message ("effective
// destinations") and reads the pairs under it, and a single 40-pair line is
// unreadable in a terminal and in Loki alike. Every line is logfmt, every value
// is a flag's EFFECTIVE value, and no line carries a credential — only the
// PATHS credentials are read from (see internal/cli's "never log a secret").
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

	// -cgroup-stats is the one pipeline a NODE can refuse: on a cgroup v1 host
	// (or one with no /sys/fs/cgroup mounted into the pod) cgroupstats.New
	// reports ErrUnsupportedNode and the agent disables this pipeline alone,
	// keeping every other one running. This summary reads FLAGS and probes
	// nothing — deliberately, and the node's cgroup version is not knowable
	// from a dry run anyway, which is the whole reason that classification
	// exists — so "on" would be this line claiming a pipeline runs where it
	// may never start. It reports the REQUEST instead; what actually happened
	// is the startup log's "cgroup sampler started" or "cgroup stats are not
	// available on this node", which is emitted where the answer is known.
	cgroupStats := "off"
	if *cgroupStatsOn {
		cgroupStats = "requested"
	}

	log.Info("effective configuration",
		// Which of the three shapes this process is deployed as. It is derived
		// from the pipeline toggles rather than from one flag (see shardRole),
		// and it is the first thing to check when the metrics of two workloads
		// collide: the role decides service.instance.id.
		"role", agentRole(),
		"sections", strings.Join(sections, ","),
		// Which binary this is, not just whether the config parses: the
		// optional pipelines are build-tag-gated (buildtags.go).
		"optionalPipelines", builtPipelines(),
		"pipelines", fmt.Sprintf("logs=%s metrics=%s cadvisor=%s cgroupStats=%s node=%s summary=%s journald=%s ingest=%s events=%s azure=%s serviceGraph=%s",
			on(*logsOn), on(*metricsOn), on(*cadvisorOn), cgroupStats, on(*nodeOn), on(*summaryOn), on(*journaldOn), on(*ingestOn), on(*eventsOn), on(*azureOn), on(*serviceGraphOn)),
		"positionsFile", *positionsFile,
		"transformsFile", *transformsFile,
		"enrich", *enrichOn,
		"selfAttributes", *selfAttrsOn,
		"logLevel", *logLevel,
	)

	// Everything this process will TALK to. An endpoint typo is the single most
	// common first-run failure and it is otherwise only visible as an export
	// error per interval, long after startup — and the per-signal overrides are
	// worse than that, because a healthy default endpoint makes the wrong one
	// look like a collector problem. Credentials appear as PATHS only.
	dest := append([]any{"metadataEndpoint", *metadataURL}, otlpFlags.SummaryAttrs()...)
	dest = append(dest,
		"kubeletEndpoint", *kubeletEndpoint,
		"bufferDir", *bufferDir,
		"bufferMaxBytes", *bufferMax,
	)
	for _, s := range cfg.Export.Overrides() {
		if s.Override != nil && s.Override.Endpoint != "" {
			// otlpLogsEndpoint, otlpMetricsEndpoint, otlpTracesEndpoint.
			dest = append(dest, "otlp"+strings.ToUpper(s.Name[:1])+s.Name[1:]+"Endpoint", s.Override.Endpoint)
		}
	}
	log.Info("effective destinations", dest...)

	// Every socket this process will bind. An address already in use is a
	// startup failure that names itself, but a listener that is simply EMPTY —
	// and therefore never bound — is silent, and "-metrics-listen=\"\" so there
	// are no metrics" is a question nobody thinks to ask. Derived from
	// processListeners, the list the collision check refuses on, so the two
	// cannot disagree about what this process binds.
	var listeners []any
	for _, l := range processListeners() {
		listeners = append(listeners, l.key, l.addr)
		if l.flag == "-listen" {
			// WHO may read the data-bearing debug surfaces on that port — the
			// live OTLP stream is this node's whole telemetry feed, so "who can
			// read it" belongs on the same line as "what is bound", and
			// -check-config must answer it before a rollout rather than after.
			listeners = append(listeners, "debugAccess", debugAccessMode())
		}
	}
	log.Info("effective listeners", listeners...)

	// Who this process says it is on every series it produces about itself.
	// service.instance.id is role-dependent, and getting it wrong makes two
	// workloads interleave counters on one (job, instance) — a failure that
	// renders perfectly and is wrong everywhere.
	//
	// READ from the resource agentSelfResource stamps, not re-derived beside
	// it: a second copy of the role rule and of attrs.Identity's fallback is
	// exactly what could report one instance while the metrics carried
	// another. It acquires nothing (an env var or the ServiceAccount
	// namespace file, and the flags), so the dry run builds it too.
	self := agentSelfResource(*nodeName).Attributes()
	selfAttr := func(k string) string {
		if v, ok := self.Get(k); ok {
			return v.AsString()
		}
		return "" // absent, e.g. no namespace known and no instance derivable
	}
	log.Info("effective identity",
		"node", *nodeName,
		"namespace", selfAttr("k8s.namespace.name"),
		"serviceName", selfAttr("service.name"),
		"instance", selfAttr("service.instance.id"),
		"selfAttributesRefresh", *selfAttrsRefresh,
		"selfMetricsInterval", *selfMetricsIntv,
	)

	// The knobs whose wrong value is expensive and quiet: a cadence, a cap or
	// an exclusion. Not every flag — docs/FLAGS.md is the full list — but the
	// ones a first rollout gets wrong.
	limits := []any{
		"scrapeInterval", *scrapeInterval,
		"scrapeTimeout", *scrapeTimeout,
		"scrapeConcurrency", *scrapeConcurrency,
		"metadataWait", *metadataWait,
		"logsExcludeNamespaces", strings.Join(cli.SplitList(*excludeNs), ","),
		"logsUnknownFiles", *logsUnknownFiles,
		"logsBatchSize", *logsBatch,
		"logsFlushInterval", *logsFlush,
		"logsMaxEntryBytes", *maxEntryBytes,
		"logsRateLimit", *logsRateLimit,
		"logsMetricsInterval", *logsMetricsEvery,
	}
	// The ingest admission bounds govern BOTH receivers that take application
	// pushes — the DaemonSet's -ingest listeners and the trace tier's
	// application ports — and they are printed RESOLVED, through the functions
	// otlpingest.NewServer itself resolves them with: the flags' stock value is
	// 0 ("the built-in default"), and ingestMaxInFlight=0 beside a shed running
	// at 32 reads as "unbounded".
	if *ingestOn || (*serviceGraphOn && tierIngestOn()) {
		limits = append(limits,
			"ingestMaxInFlight", otlpingest.EffectiveMaxInFlight(*ingestMaxInFlight),
			"ingestGRPCMaxRecvBytes", otlpingest.EffectiveMaxRecvBytes(*ingestGRPCMaxRecv),
			"ingestMetadataWait", *ingestWait)
	}
	log.Info("effective limits", limits...)

	if cfg.LogMetrics != nil {
		log.Info("logMetrics", "rules", len(cfg.LogMetrics.Metrics))
	}
	if cfg.Routing != nil {
		for _, rt := range cfg.Routing.Routes {
			log.Info("routing route", "route", rt.Name, "namespaces", strings.Join(rt.Namespaces, ","), "endpoint", rt.Endpoint)
		}
	}
	// The MERGED shard set, not the section: the chart configures this feature
	// through flags alone, so printing the section would report "(none)" for
	// the deployment the dry run most needs to describe. The shard count and
	// the tier's name are the two things an operator gets wrong (a count that
	// does not match the StatefulSet leaves traces unpaired, silently), so they
	// are what the summary names.
	//
	// ON THE TIER ONLY: nothing else forwards traces (applications push to the
	// tier), so off it a serviceGraphShards section in the shared ConfigMap is
	// inert — and the tier-only-sections line below says so. Printing
	// "forwarding" there contradicted it in the same dry run.
	if *serviceGraphOn {
		if shards, err := serviceGraphShardConfig(cfg.ServiceGraphShards); err == nil && shards.Enabled() {
			log.Info("service-graph forwarding", "shards", shards.Replicas, "statefulSet", shards.StatefulSet,
				"namespace", shards.Namespace, "port", shards.Port, "endpoints", strings.Join(shards.Endpoints, ","))
		}
		log.Info("service-graph shard role", "listen", *serviceGraphListen, "httpListen", *serviceGraphHTTPListen,
			"interval", *serviceGraphIv, "tokenFile", *serviceGraphToken)
	}
	if inert := tierOnlySections(cfg); len(inert) > 0 {
		log.Info("tier-only config sections present; only the trace tier (-service-graph) reads them, so they are inert on this workload",
			"sections", strings.Join(inert, ","), "role", agentRole())
	}
}

// agentRole names the deployment shape this process is in, the way
// agentSelfResource decides it: the two cluster-scoped roles are keyed on every
// per-node pipeline being off, never on the flag alone.
func agentRole() string {
	switch {
	case shardRole():
		return "trace-tier-shard"
	case singletonRole():
		return "cluster-singleton"
	default:
		return "node-agent"
	}
}
