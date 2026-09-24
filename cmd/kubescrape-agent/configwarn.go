package main

// The legal-but-surprising half of config validation: the warnings -check-config
// and every real start emit alike (logConfigWarnings), beside the refusals
// validateConfig raises.

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/cgroupstats"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/route"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailer"
	"github.com/JohanLindvall/kubescrape/internal/agent/transform"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

// plainSourcePodSelectionWarnings names the three pod-selection keys on a source
// that has no pods.
//
// `namespaces`, `excludeNamespaces` and `selector` all select by POD identity:
// the first two are read from the CRI FILENAME at discovery and the third from
// the pod's labels once metadata resolves, and neither exists for a plain file —
// so on a plain source all three do nothing at all. Silently: every matched file
// is collected, -check-config stays green, and the operator's evidence that the
// filter works is the absence of an error.
//
// NAMED, not refused, and the reason is not the usual "it might be deliberate".
// A refusal here is strictly worse than the inert key it reports: one -config is
// shared by every workload of this chart, and only ENABLING a pipeline a binary
// lacks may fail startup — no section belongs to one pipeline, precisely so a
// shared ConfigMap stays decodable by all of them. Refusing aborts the
// events/Azure singleton and the trace tier, neither of which ever tails a file,
// over a key that is inert in their config by construction; and on the DaemonSet
// that does read it, it turns a running fleet into a CrashLoop at the next
// rollout for a mistake that costs egress, not correctness. The keys stay inert
// either way — this makes the operator's evidence a line of output instead of a
// silence.
//
// It is deliberately ungated: the fault is in the config TEXT, so every workload
// reading that ConfigMap reports the same list, and one `-check-config` in CI
// speaks for all of them.
func plainSourcePodSelectionWarnings(logs *tailer.SourcesConfig) []string {
	if logs == nil {
		return nil
	}
	var out []string
	for i, s := range logs.Sources {
		if s.Containerd {
			continue
		}
		var keys []string
		if len(s.Namespaces) > 0 {
			keys = append(keys, "namespaces")
		}
		if len(s.ExcludeNamespaces) > 0 {
			keys = append(keys, "excludeNamespaces")
		}
		if len(s.Selector) > 0 {
			keys = append(keys, "selector")
		}
		if len(keys) > 0 {
			out = append(out, fmt.Sprintf(
				"logs.sources[%d] (%q) is a plain (non-containerd) source and %s selects pods, which its files do not have — the namespace filters read the CRI FILENAME at discovery and the selector reads pod labels at resolve time, so the key is IGNORED and every matched file is collected. "+
					"Set containerd: true if these are container logs, or narrow the source with include/exclude globs (or logs.rules, which costs the read first).",
				i, s.Name, strings.Join(keys, " and ")))
		}
	}
	return out
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

	// Pod-selection keys on a source that has no pods; see the function for why
	// this is a warning and not the refusal it was first written as.
	out = append(out, plainSourcePodSelectionWarnings(cfg.Logs)...)

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

	// A kubelet scrape asked for with no kubelet to scrape. startScraper gates
	// all three of them on -kubelet-endpoint being non-empty, so the pipeline is
	// not disabled, not failing and not retrying: it is never SCHEDULED. That is
	// the quietest failure a pipeline has — nothing is attempted, so no scrape
	// counter moves, no error is logged, and /debug/targets carries no row for
	// them (it lists the outcomes of scrapes that RAN); the only evidence is
	// metrics that never arrive, which reads exactly like a collector or a query
	// problem. -cadvisor and -node-metrics DEFAULT to on, so the commonest way
	// in is typing nothing at all.
	//
	// The flag VALUES, never whether they were typed: the chart renders
	// -cadvisor= and -node-metrics= unconditionally, so keying on explicitness
	// would make one rendered ConfigMap warn and an identical hand-written one
	// stay silent — the same "does the same effective config behave differently
	// depending on whether a default was spelled out" trap the guard-rail
	// warning below refuses.
	//
	// Named rather than refused: a logs-only agent that leaves the metric
	// toggles at their defaults is a legitimate deployment, and refusing to
	// start it would take a node's log shipping down over a metric it never
	// asked for.
	if *kubeletEndpoint == "" {
		var asked []string
		if *cadvisorOn {
			asked = append(asked, "-cadvisor")
		}
		if *nodeOn {
			asked = append(asked, "-node-metrics")
		}
		if *summaryOn {
			asked = append(asked, "-kubelet-summary")
		}
		if len(asked) > 0 {
			out = append(out, fmt.Sprintf(
				"-kubelet-endpoint is empty, so the kubelet scrapes that depend on it are never scheduled: %s. Nothing is attempted and nothing fails — no scrape counter moves and no error is logged — so the only symptom is the missing metrics. "+
					"Set -kubelet-endpoint=https://$(NODE_IP):10250 (the shipped manifests and the chart do), or turn those flags off so the startup log describes what is actually collected.",
				strings.Join(asked, ", ")))
		}
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

	// A tier-only FLAG on a workload that is not the tier. It is on this
	// workload's own command line, so it is a mistake here and is warned about;
	// the tier-only SECTIONS are a different case (tierOnlySections) — they sit
	// in the ConfigMap every workload mounts BY DESIGN, so they are reported
	// once, at Info, by printConfigSummary.
	//
	// HERE rather than in startServiceGraph, where it used to live: that
	// function is reached only by a real start, so -check-config printed
	// `config is valid` with no hint, and the promise above this function is
	// that a dry run says exactly what a start would.
	if !*serviceGraphOn && *spanMetrics {
		out = append(out, "-ingest-span-metrics ignored: span metrics are derived from received traces, and traces are received by the trace tier (-service-graph), which this process is not")
	}
	// The mirror image, ON the tier: the traceMetrics section is read only by
	// the span-metrics generator, which buildOwnerChain builds only under
	// -ingest-span-metrics. A tier configured with the section and without the
	// flag derives no RED metrics at all and says nothing — off the tier the
	// section is merely inert in the shared ConfigMap (tierOnlySections), but
	// here it is on the one workload that would read it. Warned, not refused:
	// turning span metrics off is legitimate, and the ConfigMap may carry the
	// tuning for a later enable.
	if *serviceGraphOn && !*spanMetrics && cfg.TraceMetrics != nil {
		out = append(out, "traceMetrics configured but inert: this is the trace tier, but -ingest-span-metrics is off, so no span metrics are derived and the section tunes nothing. Pass -ingest-span-metrics to derive them, or drop the section")
	}

	// Two flag-only conditions that used to be warned about from the start
	// path alone (startDebugServer, startServiceGraphIngest), so the dry run
	// described a different agent than the one that started.
	//
	// -listen empty is legal and quietly expensive: /readyz goes with it, so a
	// rolling update has nothing to gate on and advances across the fleet
	// whatever the agent's state — and every diagnostic surface an incident
	// needs (/debug/tailer, /debug/targets, /debug/otlp) is gone with it.
	if *listen == "" {
		out = append(out, "-listen is empty: no /healthz, no /readyz and no /debug surfaces are served, so a rolling update cannot gate on this agent's readiness")
	}
	// A tier that serves no application port receives only what its siblings
	// re-shard to it — legal (a dedicated owner shard), but indistinguishable
	// from a misconfigured entry point that nothing can push to.
	if *serviceGraphOn && !tierIngestOn() {
		out = append(out, "the trace tier accepts no application pushes (-service-graph-ingest=false, or both -service-graph-ingest-grpc and -service-graph-ingest-http are empty); it will only receive spans re-sharded by sibling shards")
	}

	// A configured dimension the tier's aggregators will DROP (an empty entry,
	// a repeat, or — for traceMetrics — a label every point already carries).
	// Dropping is right; the constructors used to be the only thing that said
	// so, and they run on a real start only, so -check-config printed `config
	// is valid` for a list the start then shortened with WARN lines. The same
	// gap the tier-only warnings above closed, for the same reason. Gated
	// exactly as the constructors are reached (startServiceGraph builds the
	// processor whenever -service-graph is on; buildOwnerChain builds the
	// generator only under -ingest-span-metrics), and emitted ONLY here — the
	// constructors log the drops at Debug, or a start would say each twice.
	if *serviceGraphOn {
		out = append(out, cfg.ServiceGraph.DimensionWarnings()...) // nil-receiver safe
		if *spanMetrics && cfg.TraceMetrics != nil {
			out = append(out, cfg.TraceMetrics.DimensionWarnings()...)
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
	//
	// Gated on the probability actually DROPPING traces, not on the section
	// being Enabled(): a section that is only a maxSpansPerSecond cap keeps
	// every trace (probability 0 and 1 are both keep-all), so there is no
	// dropped trace for a guard rail to rescue fragments of, and naming
	// keepErrors there told the operator to change a field that has no effect.
	// (Validate refuses a probability outside [0,1].)
	if *serviceGraphOn && cfg.TailSampling.Enabled() && cfg.TraceSampling != nil &&
		cfg.TraceSampling.Probability > 0 && cfg.TraceSampling.Probability < 1 {
		var rails []string
		if cfg.TraceSampling.KeepsErrors() {
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

	// A traceSampling section on the tier that samples NOTHING. The trap is
	// `probability: 0`: it reads as "ship no traces" and does the exact
	// opposite, because Probability is a plain float64 — 0 is indistinguishable
	// from an unset field, so Enabled() is false, buildOwnerChain never wires the
	// sampler, no "trace sampling enabled" line is logged, and New would map 0 to
	// keep-all anyway. Nothing else says so either: the configured-but-ignored
	// warnings only fire OFF the tier, and no counter moves for a sampler that
	// does not exist, so the only symptom is the egress bill.
	//
	// Named rather than refused, and for once the reason is not "it might be
	// deliberate": 0 CANNOT be told from unset, so refusing it would refuse a
	// section that merely spells out its defaults. Refusing is what
	// tracesample.Validate already does for the values that are unambiguously
	// wrong (a negative, or the 50-for-50% typo). `probability: 1` is left silent
	// — it is an honest, explicit "keep everything".
	if *serviceGraphOn && cfg.TraceSampling != nil && !cfg.TraceSampling.Enabled() && cfg.TraceSampling.Probability != 1 {
		out = append(out, fmt.Sprintf(
			"traceSampling is configured but samples nothing: probability=%v keeps EVERY trace (only a fraction strictly BETWEEN 0 and 1 samples — 0.1 is a tenth; 0 is indistinguishable from an unset field and means keep-all, not drop-all) and maxSpansPerSecond=%v is uncapped, so the section is inert and 100%% of the cluster's spans are shipped. "+
				"There is no value here that drops everything — stop the senders, or express the intent as tailSampling policies.",
			cfg.TraceSampling.Probability, cfg.TraceSampling.MaxSpansPerSecond))
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
			attr := r.Attribute
			if attr == "" {
				attr = r.Key
			}
			// The default, spelled the way pkg/logattrs spells it, because
			// which marker bites depends on this value and `log` is what an
			// omitted `target:` means.
			tgt := r.Target
			if tgt == "" {
				tgt = logattrs.TargetLog
			}
			// Identity is a RESOURCE concern and nothing else: routing keys on
			// the resource's k8s.namespace.name, and series identity is the
			// resource's. A record- or scope-target lift of one of these keys
			// forges nothing — rule keys and log-metric labels resolve these
			// keys resource-first too (logchain.Resolver), so a record-level
			// copy cannot shadow the resolved value there either.
			if tgt == logattrs.TargetResource && attrs.ReservedIdentity(attr) {
				out = append(out, fmt.Sprintf(
					"logAttributes rule %q lifts a log-line value into %q, a RESOLVED-IDENTITY resource attribute: whatever writes the line controls it (k8s.namespace.name keys tenancy routing; service.instance.id and k8s.pod.* forge series identity). The pod-annotation path refuses these keys; lift into a differently-named attribute unless the workload is genuinely authoritative for this one.",
					r.Key, attr))
			}
			// The plumbing markers are the SHARPER case, and the two are NOT
			// honoured in the same place — which is the same Resource/Element
			// split ingestReservedAttrs wires on the receivers. The router
			// reads route.ScriptMarker off a RESOURCE and nowhere else; the
			// transform engine's post-script prune reads transform.DropMarker
			// off the ELEMENT, which for logs is the log RECORD — logattrs'
			// DEFAULT target, and the one this loop used to skip entirely.
			//
			// So the old single `target: resource` gate was inverted for the
			// drop marker: it stayed silent on the placement that deletes
			// records, and on the placement where nothing reads the marker it
			// emitted a warning ASSERTING that deletion. Each message now names
			// the consequence that exists for THIS rule's target.
			//
			// The gate stays attrs.ReservedPlumbing (the predicate attrs
			// documents as shared with the pod-annotation surface, so a marker
			// added there is still reported here) — but a NEW marker lands on
			// the inert arm until its honoured-here case is added above it.
			var why string
			switch {
			case attr == route.ScriptMarker && tgt == logattrs.TargetResource:
				why = "the router honours the route marker on a resource BEFORE its namespace globs, so whatever writes the line chooses the destination — and that route's tenant headers"
			case attr == transform.DropMarker && tgt == logattrs.TargetLog:
				why = "with any logs: transform program active, the engine's post-script prune DELETES every record carrying the drop marker and counts it into kubescrape_transform_dropped_total{signal=\"logs\"} as an operator-intended drop, so a line's own content decides whether its record ships"
			case attrs.ReservedPlumbing(attr):
				why = fmt.Sprintf("nothing reads this marker off a %s, so the rule is inert today — but it is kubescrape's own control-plane key, one changed `target:` away from the surface that does honour it", tgt)
			}
			if why != "" {
				out = append(out, fmt.Sprintf(
					"logAttributes rule %q lifts a log-line value into %q, which is kubescrape's OWN plumbing, not a describable attribute: %s. The pod-annotation path and the ingest receivers both refuse this key; lift into a differently-named attribute.",
					r.Key, attr, why))
			}
		}
	}

	// A routing route that names its OWN endpoint does not inherit the flag
	// base's collector credentials (routeExportConfig), and says so the way the
	// export section's per-signal overrides do (otlpexport's
	// DroppedBaseCredentials): a route silently losing a token, a CA or the
	// skip-verify it relied on surfaces only as a transient export failure
	// against a destination the operator believes is configured. Warn, not
	// refuse — dropping them IS the correct destination, and the route has a
	// field for every one of them.
	if cfg.Routing != nil {
		base := baseExportConfig()
		for _, rt := range cfg.Routing.Routes {
			o := rt.ExportOverride()
			if dropped := otlpexport.DroppedBaseCredentials(&o, base); len(dropped) > 0 {
				out = append(out, fmt.Sprintf(
					"routing route %q names its own endpoint %q, so the flag base's collector credentials are NOT presented to it (%s): they authenticate this deployment to ITS collector, and this is a different host. "+
						"Set bearerTokenFile / caFile / insecureSkipVerify on the route itself if this destination needs them.",
					rt.Name, rt.Endpoint, strings.Join(dropped, ", ")))
			}
		}
	}

	// A sampling period that is not comfortably SHORTER than the export window
	// buys nothing: the window is -scrape-interval, a CPU rate needs two
	// readings inside one window, and at parity there is at most one reading —
	// so the CPU gauges would be absent most windows and the memory ones would
	// report a one-sample "distribution" whose stddev is 0 and whose max and min
	// are the same number the cadvisor scrape already publishes. Named rather
	// than refused: the value is legal, it just quietly undoes the reason the
	// pipeline was enabled, and the threshold (a quarter of the window, i.e.
	// four-plus samples) is a judgement rather than a correctness boundary.
	if *cgroupStatsOn && *scrapeInterval > 0 && *cgroupStatsIv*4 > *scrapeInterval {
		out = append(out, fmt.Sprintf(
			"-cgroup-stats-interval=%s against a -scrape-interval=%s export window yields at most %d samples per window: the CPU gauges need TWO readings to derive one rate and are omitted below that, and a one- or two-sample window's stddev/max/min is not a distribution — it is the last one re-stated (which the exported container_cpu_usage_samples / container_memory_working_set_bytes_samples then report as 0 or 1), i.e. the average the cadvisor scrape already publishes, under ten new names. "+
				"Sample at a small fraction of the window (the default 1s against 30s is 30 samples) or turn -cgroup-stats off.",
			*cgroupStatsIv, *scrapeInterval, *scrapeInterval / *cgroupStatsIv))
	}
	// The other end of the same flag, the one that costs the NODE rather than
	// the signal. Above the floor checkFlagValues refuses, so this is legal —
	// but three reads per container per period is a cost an operator should
	// have chosen deliberately, and a burst finer than this window is not
	// attributable to anything anyway.
	if *cgroupStatsOn && *cgroupStatsIv > 0 && *cgroupStatsIv < costlyCgroupInterval {
		out = append(out, fmt.Sprintf(
			"-cgroup-stats-interval=%s asks this node agent for %.0f cgroup file reads a second per 100 containers — on the process that also tails every log file on the node. "+
				"The default %s already resolves a burst far shorter than any export window; go below %s only for a measured reason.",
			*cgroupStatsIv, 300*float64(time.Second)/float64(*cgroupStatsIv),
			cgroupstats.DefaultInterval, costlyCgroupInterval))
	}
	// A fast discovery cadence buys short-lived containers and is paid for by
	// the METADATA SERVICE rather than by this node: every pass re-offers every
	// cgroup that has not resolved, and on a node whose pods are still starting
	// that is one lookup per pod per pass — the sandbox cgroup never resolves,
	// by construction, and only stops being asked about after its grace period.
	// Legal above the floor, but it is a fleet-wide cost an operator should
	// have chosen rather than discovered on the service's request graph.
	if *cgroupStatsOn && *cgroupDiscoverIv > 0 && *cgroupDiscoverIv < costlyCgroupDiscoverInterval {
		out = append(out, fmt.Sprintf(
			"-cgroup-stats-discover-interval=%s re-walks the cgroup hierarchy and re-offers every unresolved cgroup to the metadata service that often; on a 110-pod node that is ~%.0f lookups a second while pods are starting, since each pod's sandbox cgroup is permanently unresolvable. "+
				"It does buy shorter-lived containers (the default %s misses most containers living under ~10s, and there is no counter for the ones it never sees) — go below %s deliberately, and watch kubescrape_cgroup_unresolved_total.",
			*cgroupDiscoverIv, 110*float64(time.Second)/float64(*cgroupDiscoverIv),
			cgroupstats.DefaultDiscoverInterval, costlyCgroupDiscoverInterval))
	}
	return out
}

// costlyCgroupInterval is where -cgroup-stats-interval stops being free and
// starts being a choice. It is not a boundary — cgroupstats.MinInterval is —
// which is why it warns rather than refuses.
const costlyCgroupInterval = 500 * time.Millisecond

// costlyCgroupDiscoverInterval is the same threshold for the discovery
// cadence, and it is far larger than its sampling sibling because the two spend
// different budgets: a sweep costs this node three preads per container, while
// a discovery pass costs the METADATA SERVICE one lookup per unresolved cgroup
// — a fleet-wide cost, multiplied by every node.
const costlyCgroupDiscoverInterval = 5 * time.Second

// tierOnlySections names the config sections present in cfg that only the
// trace tier (-service-graph) reads, when this process is NOT the tier —
// empty on the tier itself.
//
// A configured section that silently does nothing is indistinguishable from
// one that is working, so they are reported. But NOT as warnings, which is how
// they used to be reported: the chart documents putting serviceGraph,
// tailSampling and traceSampling into the one agent ConfigMap the DaemonSet,
// the events/Azure singleton and the tier all mount, so a correct deployment
// logged up to four WARN lines per node per start — a fleet-wide stream of
// warnings about nothing, teaching operators to skip WARN. One Info line
// beside the effective configuration (printConfigSummary, so -check-config
// prints it too) is the report; a hand-written deployment that forgot
// -service-graph on its intended tier reads it there, next to role=node-agent.
func tierOnlySections(cfg agentConfig) []string {
	if *serviceGraphOn {
		return nil
	}
	var out []string
	for _, sec := range tierOnly {
		if sec.present(cfg) {
			out = append(out, sec.name)
		}
	}
	return out
}

// tierOnly is every config section only the trace tier reads, with what
// "configured" means for it. ONE list, and TestEveryConfigSectionIsClassified
// holds it against sectionNames(): traceMetrics was the fifth tier-only
// section and the one this report forgot, so a new section now has to be
// classified before the package's tests pass.
var tierOnly = []struct {
	name    string
	present func(agentConfig) bool
}{
	{"serviceGraph", func(c agentConfig) bool { return c.ServiceGraph != nil }},
	{"serviceGraphShards", func(c agentConfig) bool { return c.ServiceGraphShards != nil }},
	{"traceSampling", func(c agentConfig) bool { return c.TraceSampling != nil && c.TraceSampling.Enabled() }},
	{"tailSampling", func(c agentConfig) bool { return c.TailSampling.Enabled() }}, // nil-receiver safe
	// Span metrics are derived on the tier (buildOwnerChain) and nowhere else;
	// this section only tunes them.
	{"traceMetrics", func(c agentConfig) bool { return c.TraceMetrics != nil }},
}

// parseScriptWarnings names every plain log source that opts into the
// transforms file's parse: hook (parseScript: true) when the program this
// process starts with defines none — no -transforms-file, or a file with no
// parse: section. startLogs wires the hook only when a program exists and the
// wrapper answers "not parsed" when it has no parse function, so every line of
// such a source ships UNPARSED with nothing logged, nothing counted and
// -check-config printing `config is valid`. The sibling `type: script` tail
// policy is cross-checked against HasSample by validateConfig; this is the same
// question for the per-source opt-in.
//
// A warning, not a refusal: the section hot-reloads (a parse: added later takes
// effect without a restart, and one removed later is not something a start can
// refuse anyway), and the logs section lives in the ConfigMap every workload
// mounts while the chart renders -transforms-file on none of them (it arrives
// via extraArgs). Gated on -logs, since a workload that tails nothing never
// reads the sources.
func parseScriptWarnings(cfg agentConfig, hasParse bool) []string {
	if !*logsOn || hasParse || cfg.Logs == nil {
		return nil
	}
	var out []string
	for i, s := range cfg.Logs.Sources {
		if !s.ParseScript {
			continue
		}
		out = append(out, fmt.Sprintf(
			"logs.sources[%d] (%q) sets parseScript but no parse: hook is loaded (no -transforms-file, or the file defines no parse: section), so every line of this source ships unparsed. "+
				"Add a parse: section defining parse(line) to the transforms file, or drop parseScript from the source.",
			i, s.Name))
	}
	return out
}

// logConfigWarnings emits configWarnings, plus the reports that need the
// compiled transforms program (prog, nil without -transforms-file) — handed
// in from compileConfig rather than compiled a second time.
func logConfigWarnings(cfg agentConfig, prog *transform.Program, log *slog.Logger) {
	for _, w := range configWarnings(cfg) {
		log.Warn(w)
	}
	for _, w := range parseScriptWarnings(cfg, prog.HasParse()) {
		log.Warn(w)
	}
}
