package main

// The trace tier's wiring (-service-graph).
//
// One workload, two listeners, and the difference between them is the whole
// design:
//
//   - The APPLICATION listeners (-service-graph-ingest-grpc / -http, 4317/4318)
//     take OTLP traces pushed by instrumented pods. Unauthenticated, because
//     every pod in the cluster is a sender. A payload arriving here is enriched
//     with Kubernetes metadata — this is the ONE place the connection's source
//     address still names the sender — and then re-sharded by trace id.
//   - The INTERNAL listener (-service-graph-listen, 4319) takes spans a sibling
//     shard re-sharded to us. Authenticated with the shared bearer token,
//     because it is reachable from every pod too and what it accepts is treated
//     as final: already enriched, already routed. It is TERMINAL — nothing
//     arriving here is enriched again or re-sharded again.
//
// Both funnel into one owner chain: pair the edge, derive the RED metrics, head
// sample, export. That chain runs exactly once per span, on the shard that owns
// its trace.
//
// Everything about pairing, ring placement, re-sharding and the emitted series
// lives in internal/agent/servicegraph; this file is the seam between that
// package and the process — flags, listeners, readiness, shutdown.
//
// Why this is the agent binary with a flag rather than its own: the tier needs
// the same exporter, the same self-metrics identity, the same enricher and the
// same config file, exactly as the events/Azure singleton does. A second binary
// would duplicate all of it to save one flag.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpingest"
	"github.com/JohanLindvall/kubescrape/internal/agent/servicegraph"
	"github.com/JohanLindvall/kubescrape/internal/agent/spanmetrics"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailbuffer"
	"github.com/JohanLindvall/kubescrape/internal/agent/tracesample"
	"github.com/JohanLindvall/kubescrape/internal/bearer"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// gateServiceGraph is satisfied once the tier's INTERNAL receiver is BOUND —
// not once it has received something.
//
// A shard that has been sent nothing yet is working; gating on the first payload
// would leave a freshly-scaled tier permanently not-ready, and a StatefulSet
// rollout advances on this probe. Bound is the honest claim: the port answers,
// and a sibling's hop will land.
const gateServiceGraph = "service-graph-receiver"

// gateServiceGraphIngest is the same claim for the application-facing listeners.
// It is separate because they can fail independently (a port already bound is
// the usual way), and a rollout that advanced while nothing could push traces to
// the new pod would march that state across the tier.
const gateServiceGraphIngest = "service-graph-ingest"

// startServiceGraph starts the trace tier: the two receivers, the pairing
// processor and sweeper, the span-metrics generator, the sampler and the edge
// metric export loop. Off unless -service-graph.
func (p *pipelines) startServiceGraph(ctx context.Context) error {
	if !*serviceGraphOn {
		// The "configured but ignored" reports for the tier-only sections
		// and -ingest-span-metrics used to live here. They are emitted from the
		// config summary (tierOnlySections, Info) and configWarnings (the flag,
		// Warn) now: a start reached them and -check-config did not, and the
		// dry run is where an operator asks whether a section of the ConfigMap
		// shared by the DaemonSet, the singleton and the tier is being applied.
		return nil
	}
	var cfg servicegraph.Config
	if p.fileCfg.ServiceGraph != nil {
		cfg = *p.fileCfg.ServiceGraph
	}

	// The internal receiver accepts spans from anything that can reach the pod,
	// so the hop is authenticated — validateConfig already refused an empty
	// -service-graph-token-file (fatal there so -check-config catches it too);
	// the READ is fatal here for the metadata service's reason, which is
	// bearer.NewRotating's contract: an unreadable or empty token file must stop
	// the process, never open the listener with nothing to check against. Same
	// package, same re-read (about once a second —
	// bearer.DefaultRefreshInterval — on use, plus Rotating.Run's ticker below)
	// and same rotation grace as the metadata service's /v1/scrape-auth — one
	// auth model in this repo rather than two.
	tok, err := bearer.NewRotating(*serviceGraphToken, p.log)
	if err != nil {
		return fmt.Errorf("-service-graph-token-file: %w", err)
	}
	// The clock-driven half of that parity, which this call site did NOT have:
	// Tokens() re-reads only when called, so on a listener between pushes a
	// rotation went unnoticed until the next request, which armed the revoked
	// token's grace window at THAT moment — accepting it far past the five
	// minutes the model documents, indefinitely while the listener stayed
	// quiet. The comment above claimed the parity; only Run delivers it. It is
	// also the ONLY thing that moves the set the gRPC auth tap reads
	// (sgReceiver.cached): the tap never refreshes, so without Run a rotation
	// would reach the interceptor and the HTTP arm and never the tap.
	go tok.Run(ctx)

	reg := servicegraph.NewRegistry(cfg, p.log)
	proc := servicegraph.NewProcessor(cfg, reg, p.log)
	// One snapshot serves all four gauges of an export: the hook memoises per
	// evaluation pass (metrics.PerPass), so the pairing mutex is taken once per
	// export and the final export after SweepAll reads the store afresh.
	obs.RegisterServiceGraphStats(func() obs.ServiceGraphStat {
		st := proc.Stats()
		return obs.ServiceGraphStat{
			Pending:     st.Items,
			Completed:   st.Completed,
			VirtualNode: st.VirtualNode,
			Unkeyable:   st.Unkeyable,
		}
	})

	// The tier's own resource identity, like the span-metrics generator's: the
	// edge's two services are DATA-POINT labels (client/server), never the
	// emitting process's identity — this workload describes other objects, it is
	// not one of them.
	res := agentSelfResource(*nodeName)
	p.serviceGraphProc, p.serviceGraphReg, p.serviceGraphRes = proc, reg, res
	p.spawn(func() { reg.Run(ctx, p.selfOut, *serviceGraphIv, res, p.log) })
	p.spawn(func() { sweepServiceGraph(ctx, proc) })

	owner, err := p.buildOwnerChain(ctx, proc)
	if err != nil {
		return err
	}
	// ACQUIRE BEFORE SERVING. The resharder is the last thing here that can
	// fail, and it used to be built inside startServiceGraphIngest — AFTER the
	// internal receiver below was already accepting sibling pushes into the
	// owner chain, whose tail buffer acks BEFORE it decides. A failure there
	// returned out of run() without ever reaching the shutdown sequence's
	// Flush, so spans a sibling had been told had landed were dropped with no
	// counter moving.
	var resharder *servicegraph.Resharder
	if tierIngestOn() {
		if resharder, err = p.startResharder(); err != nil {
			return err
		}
	}

	rcv := &sgReceiver{
		grpcAddr: *serviceGraphListen,
		httpAddr: *serviceGraphHTTPListen,
		tokens:   tok.Tokens,
		cached:   tok.Cached,
		consume:  ownerReceive(owner),
		ready:    p.ready.gate(gateServiceGraph),
		log:      p.log,
	}
	p.spawn(func() {
		if err := rcv.Run(ctx); err != nil {
			// Fatal like the ingest listener: a shard whose internal receiver is
			// dead accepts no re-sharded spans, and it would otherwise sit there
			// looking healthy while its siblings' pushes fail.
			p.fatal("service-graph receiver", err)
		}
	})
	p.startServiceGraphIngest(ctx, owner, resharder)
	p.log.Info("trace tier started", "serviceGraphInternalGRPC", *serviceGraphListen, "serviceGraphInternalHTTP", *serviceGraphHTTPListen,
		"wait", proc.Wait(), "interval", *serviceGraphIv)
	return nil
}

// ownerReceive turns the owner chain into the internal receiver's consume
// callback: strip the transport marker, then run the chain.
//
// Stripping FIRST and unconditionally is the point. The marker exists to let the
// application listener refuse a hop addressed to the wrong port; past that it is
// kubescrape's internal plumbing, and letting it ride to the collector would put
// a kubescrape.* resource attribute on every application span in the cluster.
func ownerReceive(owner servicegraph.TracesExporter) func(context.Context, ptrace.Traces) error {
	return func(ctx context.Context, td ptrace.Traces) error {
		servicegraph.StripForwarded(td)
		return owner.ExportTraces(ctx, td)
	}
}

// buildOwnerChain assembles what happens to a span once it is on the shard that
// OWNS its trace, from the bottom up:
//
//	pair the edge -> derive RED metrics -> head sample -> tail sample -> export
//
// The two taps forward to their inner exporter FIRST and act only on success, as
// spanmetrics has always done, and here that is load-bearing rather than tidy: a
// failed export propagates all the way back to the pushing application, whose
// retry re-pushes the identical batch. Counting before the export would inflate
// both the graph and the RED metrics by one copy per back-pressure window.
//
// Sampling sits BELOW both, non-negotiably. An edge is one request and the graph
// counts requests; RED metrics are the request rate. A sampled chain would still
// pair correctly (the sampler keeps whole traces — the decision is per trace id)
// and would simply report 10% of the traffic as if that were the traffic, on
// series whose entire purpose is saying how much there is.
//
// The two samplers are ordered head-then-tail, and they NEST rather than
// compound: both hash the trace id with the same unsalted hash against the same
// threshold arithmetic, so a tail policy at 50% keeps exactly the traces a head
// probability of 0.5 already passed (agent/tailbuffer's package doc, and the
// cross-package tests in both). Head first is also the cheap order — a trace the
// head drops is never buffered for five seconds — with one caveat worth knowing:
// the head sampler's guard rails are per SPAN, so keepErrors delivers a
// fragment of a trace to a layer that judges whole traces.
func (p *pipelines) buildOwnerChain(ctx context.Context, proc *servicegraph.Processor) (servicegraph.TracesExporter, error) {
	// Both Client and Buffered export traces. Buffered passes a plain forwarded
	// trace through unbuffered — the pushing sender owns the retry, and
	// spooling would ack a sender that then stops holding it — but SPOOLS a
	// payload the tail sampler marks otlpexport.Own, whose senders were acked
	// when their spans were buffered and hold nothing (otlpexport/owned.go).
	out, ok := p.out.(servicegraph.TracesExporter)
	if !ok {
		return nil, errors.New("exporter does not support traces")
	}
	chain := out
	if cfg := p.fileCfg.TailSampling; cfg.Enabled() { // nil-receiver safe
		if p.transforms != nil {
			// The `type: script` policy body (the transforms file's sample:
			// section). nil when the section is absent — the policy compiler
			// then refuses `type: script` at config time, naming the fix.
			cfg.Script = p.transforms.SampleDecider()
		}
		tb, err := tailbuffer.New(*cfg, chain, p.log)
		if err != nil {
			return nil, fmt.Errorf("tailSampling: %w", err)
		}
		p.tailBuffer = tb
		obs.RegisterTailSamplingStats(func() obs.TailSamplingStat {
			st := tb.Stats()
			return obs.TailSamplingStat{Traces: st.Traces, Spans: st.Spans}
		})
		p.spawn(func() { tb.Run(ctx) })
		chain = tb
		// Loud, once, because this is the one pipeline in the agent that acks a
		// payload it has not delivered: an operator reading the startup log
		// should not have to find that out from the package doc.
		p.log.Info("tail sampling enabled", "policies", len(cfg.Policies), "decisionWait", tb.Wait(),
			"note", "buffered spans are acked to their senders before they are decided; a hard kill of this pod loses them (kubescrape_tail_sampling_buffered_spans)")
	}
	var sampler *tracesample.Sampler
	if cfg := p.fileCfg.TraceSampling; cfg != nil && cfg.Enabled() {
		// Validated by compileConfig before anything started (and New still
		// warns rather than refuses on a value that somehow got past it).
		sampler = tracesample.New(*cfg, chain)
		chain = sampler
		// Both aggregators above sample nothing (they count every request)
		// but their exemplars are LINKS to traces, and a link to a trace this
		// sampler drops resolves to nothing. The Registry was built before
		// this sampler existed, hence a setter; the generator is built below.
		if p.serviceGraphReg != nil {
			p.serviceGraphReg.SetExemplarKeep(sampler.TraceKept)
		}
		p.log.Info("trace sampling enabled", "probability", cfg.Probability,
			"maxSpansPerSecond", cfg.MaxSpansPerSecond, "keepSlowerThan", cfg.KeepSlowerThan)
	}
	if *spanMetrics {
		var smCfg spanmetrics.Config
		if p.fileCfg.TraceMetrics != nil {
			smCfg = *p.fileCfg.TraceMetrics
		}
		if sampler != nil {
			smCfg.ExemplarKeep = sampler.SpanKept
		}
		gen := spanmetrics.New(smCfg)
		chain = gen.Tap(chain)
		smRes := agentSelfResource(*nodeName)
		p.spanMetricsGen, p.spanMetricsRes = gen, smRes
		p.spawn(func() { gen.Run(ctx, p.selfOut, *spanMetricsIv, smRes, p.log) })
		p.log.Info("span metrics from traces enabled", "interval", *spanMetricsIv)
	}
	return proc.Tap(chain), nil
}

// --- the application-facing listeners ---

// tierIngestOn reports whether the trace tier serves the application-facing
// ports at all: -service-graph-ingest, with at least one of its listeners set.
func tierIngestOn() bool {
	return *serviceGraphIngest && (*serviceGraphIngestGRPC != "" || *serviceGraphIngestHTTP != "")
}

// startResharder builds the application ports' resharder (nil for a
// single-shard tier) and publishes its counters. Split out of
// startServiceGraphIngest so startServiceGraph can build it — the one step that
// can fail — BEFORE any receiver is serving.
func (p *pipelines) startResharder() (*servicegraph.Resharder, error) {
	resharder, err := serviceGraphResharder(p.fileCfg.ServiceGraphShards, p.log)
	if err != nil {
		return nil, fmt.Errorf("service-graph shards: %w", err)
	}
	p.sgResharder = resharder
	obs.RegisterServiceGraphResharder(func() obs.ServiceGraphReshardStat {
		st := resharder.Stats() // nil-receiver safe
		return obs.ServiceGraphReshardStat{
			SpansForwarded: st.SpansForwarded,
			SpansLocal:     st.SpansLocal,
			SpansUnkeyed:   st.SpansUnkeyed,
			SendsFailed:    st.SendsFailed,
			LoopsBlocked:   st.LoopsBlocked,
		}
	})
	if resharder != nil {
		shards := resharder.Ring().Shards()
		p.log.Info("trace re-sharding enabled", "shards", len(shards), "self", *serviceGraphSelf, "ring", strings.Join(shards, ","))
	} else {
		p.log.Info("trace re-sharding is off: a single-shard tier owns every trace locally")
	}
	return resharder, nil
}

// startServiceGraphIngest starts the tier's OTLP trace receiver for
// applications: enrich, re-shard (resharder, built by startResharder before any
// receiver serves; nil for a single-shard tier), and hand this shard's own
// share to the owner chain. Nothing here can fail: whatever could is acquired
// by the caller first.
func (p *pipelines) startServiceGraphIngest(ctx context.Context, owner servicegraph.TracesExporter, resharder *servicegraph.Resharder) {
	if !tierIngestOn() {
		// configWarnings says so, for -check-config and every start alike.
		return
	}
	ecfg := p.enricherBase()
	// The tier's one delta on the shared base: veto a peer-IP attribution that
	// resolves to our own workload (a sibling shard's hop, a proxy on the tier).
	ecfg.PeerReject = p.peerIsOurOwnWorkload
	// NodeInfo is deliberately left NIL here (the shared base does not set it),
	// unlike the DaemonSet's ingest server. There, `.Node` in a
	// resourceAttributes template is the node the sending pod runs on, because
	// the agent only ever receives from its own node. This tier receives from
	// the WHOLE CLUSTER, so the shard's node is not a property of the spans it
	// is enriching — a template reading `.Node` stamped this shard's node
	// labels onto every application span from every node, which renders
	// perfectly and is wrong on every one. The same argument the events reader
	// records for leaving actx.Node nil: a described object's node is the
	// object's property, never the reader's.
	//
	// The admission base (in-flight and message caps, the reserved strip, the
	// ingest: hook) is newAppIngestServer's, shared with the DaemonSet's
	// receiver. The strip matters here at least as much: these ports take
	// pushes from every pod in the cluster, and a span resource declaring
	// another tenant's k8s.namespace.name would route the whole trace there.
	scfg := otlpingest.ServerConfig{
		GRPCAddr: *serviceGraphIngestGRPC,
		HTTPAddr: *serviceGraphIngestHTTP,
		// Exporter nil: this listener serves TRACES only. Logs and metrics belong
		// on the node-local DaemonSet, where the sender is a pod on the same node
		// and the payload crosses no network to be attributed.
		//
		// Which is why the log-chain fields the DaemonSet's receiver wires
		// (Rules, LogAttrs, LogMetrics) are absent here rather than merely
		// forgotten: with no logs service registered, applyLogChain is
		// unreachable on this listener, and carrying the config would advertise
		// a seam that cannot run.
		Traces: &sgEntry{resharder: resharder, owner: owner},
		// The loop guard belongs on the RECEIVE path, above enrichment: a payload
		// that is going to be refused must not spend a metadata lookup per
		// resource, nor move the ingest counters, on its way to the refusal — and
		// its peer address is a sibling shard, so what enrichment would deduce
		// from it is wrong anyway.
		RejectTraces: func(_ context.Context, td ptrace.Traces) error {
			return refuseForwarded(resharder, td)
		},
		Ready: p.ready.gate(gateServiceGraphIngest),
	}
	srv := p.newAppIngestServer(ecfg, scfg)
	p.spawn(func() {
		if err := srv.Run(ctx); err != nil {
			p.fatal("service-graph trace ingest", err)
		}
	})
	p.log.Info("trace ingest listening", "serviceGraphIngestGRPC", *serviceGraphIngestGRPC, "serviceGraphIngestHTTP", *serviceGraphIngestHTTP,
		"peerIPFallback", *ingestPeerIP)
}

// msgForwardedToAppPort is the loop guard's refusal, spelled once for the
// receive-path hook and sgEntry's own second line alike.
const msgForwardedToAppPort = "this payload carries " + servicegraph.ForwardedMarker +
	": it was re-sharded by another shard and addressed to the tier's APPLICATION port instead of its internal receiver (-service-graph-listen). Point serviceGraphShards at the internal port"

// refuseForwarded refuses a payload that already carries the tier's re-shard
// marker — an internal hop addressed to the application port — and counts the
// spans it turned away. An unmarked payload returns nil, which is every
// application push.
//
// PERMANENT (InvalidArgument, which otlpexport.IsPermanent classifies as
// do-not-retry) is what turns that misconfiguration into a bounded, counted
// failure instead of an amplification loop: accepting the payload would
// re-shard it and send it round again on every hop, and a RETRYABLE refusal
// would have the sending shard re-push it forever, which is the same loop at a
// slower rate.
//
// r is nil on a single-shard tier; CountLoopBlocked is nil-receiver safe.
func refuseForwarded(r *servicegraph.Resharder, td ptrace.Traces) error {
	if !servicegraph.IsForwarded(td) {
		return nil
	}
	r.CountLoopBlocked(td.SpanCount())
	return status.Error(codes.InvalidArgument, msgForwardedToAppPort)
}

// sgEntry is the terminal exporter of the APPLICATION listener: it runs after
// otlpingest.Server has enriched the payload, re-shards it, and runs the owner
// chain over whatever this shard owns.
//
// Ordering is fixed by what each step needs. The loop guard runs on the RECEIVE
// path, above enrichment (otlpingest.ServerConfig.RejectTraces), because a
// refused payload must cost nothing. Enrichment happens above this too, inside
// the server, because it needs the connection's source address, which only
// exists on the hop the application itself opened. Re-sharding happens after
// enrichment because the payload that crosses to a sibling must be the finished
// one — the sibling has no way to attribute it. And the owner chain happens
// after both because it is the thing that must run exactly once per span, on one
// shard.
type sgEntry struct {
	resharder *servicegraph.Resharder // nil on a single-shard tier
	owner     servicegraph.TracesExporter
}

func (e *sgEntry) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	// The loop guard's second line, and the reason it is worth a second one:
	// this exporter is reachable only through a receiver, but WHICH receiver —
	// and whether that receiver was given the hook — is the caller's wiring, and
	// a wiring that forgot it would leave the marker as no defence at all. It
	// costs one attribute probe per push and never double-counts: with the hook
	// in place the refusal returns before this exporter is reached.
	if err := refuseForwarded(e.resharder, td); err != nil {
		return err
	}
	local, err := e.resharder.Reshard(ctx, td) // nil-receiver safe: everything stays local
	if err != nil {
		return err
	}
	if local.SpanCount() == 0 {
		return nil
	}
	return e.owner.ExportTraces(ctx, local)
}

// peerIsOurOwnWorkload vetoes a peer-IP attribution that resolved to a pod of
// THIS process's own workload.
//
// The peer address is only the sender's on the hop the sender opened. Everything
// that can rewrite it in flight — a mesh sidecar that terminates the connection,
// an ingress or proxy in front of the tier, an internal hop addressed to the
// wrong port — leaves an address belonging to some infrastructure pod, and on
// this tier the infrastructure pod is usually one of US. Attributing an
// application's traces to a kubescrape shard is the worst available outcome:
// every span in the cluster labelled with the same wrong pod, service.name and
// namespace, rendering perfectly, alerting on nothing.
//
// Identity comes from the self-metadata lookup the process already runs for
// -self-attributes, so there is no new dependency.
//
// It returns false when we do not know our own pod yet. That is the honest
// answer during the first seconds after start: the check exists to prevent a
// confident lie, and inventing one from a lookup that has not landed would be
// the same mistake in the other direction.
func (p *pipelines) peerIsOurOwnWorkload(pod *kubemeta.Pod) bool {
	if p.selfPod == nil {
		return false
	}
	return sameWorkload(p.selfPod(), pod)
}

// sameWorkload reports whether two pods are the same pod or two replicas of one
// workload.
//
// The WORKLOAD comparison is what catches the sibling case: shard 3's address is
// not shard 0's pod, but it is the same StatefulSet, and no application ever is.
// It walks to the TOP of each owner chain (pod -> ReplicaSet -> Deployment) and
// compares uids, so a ReplicaSet rollout does not make two generations of one
// Deployment look like different workloads.
func sameWorkload(a, b *kubemeta.Pod) bool {
	if a == nil || b == nil {
		return false
	}
	if a.UID != "" && a.UID == b.UID {
		return true
	}
	if a.Namespace != b.Namespace || a.Namespace == "" {
		return false
	}
	ao, bo := topOwner(a), topOwner(b)
	return ao != nil && bo != nil && ao.UID != "" && ao.UID == bo.UID
}

// topOwner is the last link of a pod's ownership chain — the workload object
// (Deployment, StatefulSet, DaemonSet, CronJob), which is what the metadata
// service resolves the chain up to.
func topOwner(p *kubemeta.Pod) *kubemeta.Owner {
	if len(p.Owners) == 0 {
		return nil
	}
	return &p.Owners[len(p.Owners)-1]
}

// sweepServiceGraph runs the pairing store's expiry on a ticker.
//
// Consume already expires incrementally on the way in, which covers a shard
// that is being pushed spans. This loop covers the other half of the day: a
// shard that goes quiet still holds half-edges whose partners will never
// arrive, and a client half that could become a VIRTUAL-NODE edge only reaches
// the graph when something retires it. Without the ticker those edges would
// appear on the next busy batch — or, on a tier that quiesces at night, not
// until morning.
func sweepServiceGraph(ctx context.Context, proc *servicegraph.Processor) {
	t := time.NewTicker(sweepInterval(proc.Wait()))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			proc.Sweep()
		}
	}
}

// sweepInterval sizes the expiry cadence from the pairing window ITSELF (the
// processor reports the effective one, so a configured wait and the sweep that
// enforces it cannot drift). Half the window bounds an edge's promotion delay
// at 1.5x wait — far inside any sane export interval — while keeping the
// mutex-holding pass rare. The clamps stop a pathological config from either
// spinning (a millisecond wait) or letting a quiet shard sit on promotable
// half-edges for minutes (an hour-long wait).
func sweepInterval(wait time.Duration) time.Duration {
	return min(max(wait/2, time.Second), 30*time.Second)
}

// --- the tier's INTERNAL receiver ---

// --- the internal hop's configuration ---

// serviceGraphResharder builds the tier's internal resharder from the flags and
// the config section, or (nil, nil) when there is nothing to re-shard (a
// single-shard tier) — so the caller can wire it unconditionally.
func serviceGraphResharder(sec *servicegraph.ReshardConfig, log *slog.Logger) (*servicegraph.Resharder, error) {
	cfg, err := serviceGraphShardConfig(sec)
	if err != nil {
		return nil, err
	}
	return servicegraph.NewResharder(cfg, baseExportConfig(), log)
}

// serviceGraphShardConfig merges the flag surface into the config's
// serviceGraphShards section.
//
// PRECEDENCE: the config section wins, field by field; the flags fill in what it
// leaves unset. The chart renders the flags and nothing else, so they have to
// work alone — but an operator who reaches for the section is asking for
// something the flags cannot express (explicit endpoints outside Kubernetes, a
// TLS'd hop, tokensPerShard), and having the flags override it would make the
// richer form unusable in exactly the deployment that renders them.
//
// It touches nothing: no DNS, no filesystem, no namespace resolution (that
// happens in shardTargets at start), so -check-config runs it as-is.
func serviceGraphShardConfig(sec *servicegraph.ReshardConfig) (servicegraph.ReshardConfig, error) {
	var cfg servicegraph.ReshardConfig
	if sec != nil {
		cfg = *sec
	}
	// The template form only: a section naming explicit endpoints has said
	// where the shards are, and layering a derived template over it would
	// address two different shard sets from one config.
	if *serviceGraphEndpoint != "" && cfg.StatefulSet == "" && len(cfg.Endpoints) == 0 {
		name, ns, port, err := parseShardEndpoint(*serviceGraphEndpoint)
		if err != nil {
			return cfg, err
		}
		cfg.StatefulSet = name
		// Guarded like Namespace and Port below, and for the same reason: the
		// endpoint's first label feeds the Service only as the CONVENTION's
		// default (the governing Service carries the StatefulSet's name).
		// Overwriting a section that named a differently-named headless Service
		// discarded the one field that decides the per-pod DNS the ring dials,
		// so every remote share addressed a name nothing publishes — silently,
		// since the merge reports no error and -check-config stays green.
		if cfg.Service == "" {
			cfg.Service = name
		}
		if cfg.Namespace == "" {
			cfg.Namespace = ns
		}
		if cfg.Port == 0 {
			cfg.Port = port
		}
	}
	if cfg.Replicas == 0 {
		cfg.Replicas = *serviceGraphShards
	}
	if cfg.Self == "" {
		cfg.Self = strings.TrimSpace(*serviceGraphSelf)
	}
	if cfg.BearerTokenFile == "" {
		// The SAME flag as the internal listener's credential: one Secret, one
		// token, so a shard cannot be given a token its siblings do not accept by
		// a config that looks complete.
		cfg.BearerTokenFile = *serviceGraphToken
	}
	// Half a template is the one "disabled" state worth refusing: it reads as
	// configured and re-shards nothing, which is indistinguishable from a
	// deliberately single-shard tier. ReshardConfig.Validate refuses the same
	// shapes, but in the section's spelling — these messages name the FLAG the
	// operator actually set.
	switch {
	case *serviceGraphShards > 1 && cfg.StatefulSet == "" && len(cfg.Endpoints) == 0:
		return cfg, fmt.Errorf("-service-graph-shards=%d has nothing to address: set -service-graph-endpoint to the tier's governing headless Service, or name the shards in the config's serviceGraphShards section", *serviceGraphShards)
	case *serviceGraphEndpoint != "" && cfg.Replicas <= 0:
		return cfg, fmt.Errorf("-service-graph-endpoint %q has no shard count: set -service-graph-shards to the StatefulSet's replica count (it is part of the ring's definition, so every shard must be given the same number)", *serviceGraphEndpoint)
	}
	return cfg, nil
}

// shardRingReachesThisShard cross-checks the ring's TRANSPORT and PORT against
// the receivers this shard actually binds.
//
// The two surfaces are configured independently — the ring's protocol and port
// from the serviceGraphShards section (or -service-graph-endpoint), the
// listeners from -service-graph-listen / -service-graph-http-listen — and a
// TEMPLATE ring is symmetric: it addresses THIS pod on exactly the protocol and
// port it addresses every sibling on. So `protocol: http` beside an empty
// -service-graph-http-listen means every sibling POSTs /v1/traces at a
// gRPC-only listener; the hop is synchronous and failable, so every application
// push is refused for the tier's lifetime while both readiness gates stay green
// (they gate on BINDING, and the gRPC listener binds). Same for a port the ring
// dials that nothing serves. Refused HERE, like msgShardNoListener, so
// -check-config catches it instead of the StatefulSet rolling out healthy and
// forwarding nothing.
//
// Two deliberate exemptions, both "this pod need not be in the ring it dials":
// explicit `endpoints` name the shard set outright (NewResharder only WARNS
// when self is not among them), and a single-shard template has no internal hop
// at all.
func shardRingReachesThisShard(cfg servicegraph.ReshardConfig) error {
	if len(cfg.Endpoints) > 0 || cfg.StatefulSet == "" || cfg.Replicas <= 1 {
		return nil
	}
	proto, listen, flagName := "gRPC", *serviceGraphListen, "-service-graph-listen"
	if cfg.Protocol == "http" {
		proto, listen, flagName = "OTLP/HTTP", *serviceGraphHTTPListen, "-service-graph-http-listen"
	}
	if strings.TrimSpace(listen) == "" {
		return fmt.Errorf("serviceGraphShards addresses the ring over %s but %s is empty: a sibling's forward would reach a receiver this shard does not serve", proto, flagName)
	}
	port := cfg.Port
	if port == 0 {
		port = servicegraph.DefaultShardPort
	}
	_, p, err := net.SplitHostPort(listen)
	if err != nil {
		return fmt.Errorf("%s %q is not host:port: %w", flagName, listen, err)
	}
	if lp, err := strconv.Atoi(p); err != nil || lp != port {
		return fmt.Errorf("serviceGraphShards addresses each shard on port %d but %s binds %q: a sibling's forward would reach nothing", port, flagName, listen)
	}
	return nil
}

// parseShardEndpoint reads the shard tier's GOVERNING HEADLESS SERVICE address
// — `<statefulset>.<namespace>.svc[.cluster.local][:port]`, which is what the
// chart renders — into the template ReshardConfig expands to each shard's
// stable per-pod name, `<sts>-<ordinal>.<service>.<ns>.svc:<port>`.
//
// The Service is named but never DIALLED: a load-balanced destination
// round-robins, which sends a trace's client half to one shard and its server
// half to another — precisely the failure the ring exists to prevent. Only its
// name is taken, and only because the StatefulSet's pods are named after it.
//
// The first host label feeds BOTH StatefulSet and Service: the chart (and the
// convention) gives the governing Service the StatefulSet's name, and the
// per-pod DNS name needs both halves. The second label is the namespace
// (absent = the agent's own, resolved at start). Anything after that is the
// cluster domain and is dropped — shardTargets re-renders the `.svc` suffix
// itself, and a name three dots deep still resolves through the pod's search
// list.
func parseShardEndpoint(ep string) (name, namespace string, port int, err error) {
	ep = strings.TrimSpace(ep)
	if ep == "" {
		return "", "", 0, errors.New("-service-graph-endpoint is empty")
	}
	if strings.Contains(ep, "//") {
		// A URL would end up inside a DNS name and fail at connect time, far
		// from the config that caused it. The scheme is the section's
		// `protocol`, never part of the address.
		return "", "", 0, fmt.Errorf("-service-graph-endpoint %q is a URL: give host:port (the transport is serviceGraphShards.protocol)", ep)
	}
	host := ep
	if h, p, e := net.SplitHostPort(ep); e == nil {
		host = h
		if port, err = strconv.Atoi(p); err != nil || port <= 0 || port > 65535 {
			return "", "", 0, fmt.Errorf("-service-graph-endpoint %q: invalid port %q", ep, p)
		}
	}
	labels := strings.Split(strings.TrimSuffix(host, "."), ".")
	if labels[0] == "" {
		return "", "", 0, fmt.Errorf("-service-graph-endpoint %q names no host", ep)
	}
	name = labels[0]
	if len(labels) > 1 {
		namespace = labels[1]
	}
	return name, namespace, port, nil
}
