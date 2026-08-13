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
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpingest"
	"github.com/JohanLindvall/kubescrape/internal/agent/servicegraph"
	"github.com/JohanLindvall/kubescrape/internal/agent/spanmetrics"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailbuffer"
	"github.com/JohanLindvall/kubescrape/internal/agent/tracesample"
	"github.com/JohanLindvall/kubescrape/internal/bearer"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/otlpsplit"
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
		// A configured section that silently does nothing is indistinguishable
		// from one that is working, so each of them says so once.
		if p.fileCfg.ServiceGraph != nil {
			p.log.Warn("serviceGraph configured but ignored: this process is not the trace tier (-service-graph=false)")
		}
		if cfg := p.fileCfg.TraceSampling; cfg != nil && cfg.Enabled() {
			p.log.Warn("traceSampling configured but ignored: traces are received by the trace tier (-service-graph), and this process is not it")
		}
		if p.fileCfg.ServiceGraphShards != nil {
			p.log.Warn("serviceGraphShards configured but ignored: the shard ring is read only by the trace tier (-service-graph), and this process is not it")
		}
		if p.fileCfg.TailSampling.Enabled() { // nil-receiver safe
			p.log.Warn("tailSampling configured but ignored: a trace can only be judged where all of its spans are, which is the trace tier (-service-graph), and this process is not it")
		}
		if *spanMetrics {
			p.log.Warn("-ingest-span-metrics ignored: span metrics are derived from received traces, and traces are received by the trace tier (-service-graph), which this process is not")
		}
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
	// package, same per-minute re-read and same rotation grace as the metadata
	// service's /v1/scrape-auth — one auth model in this repo rather than two.
	tok, err := bearer.NewRotating(*serviceGraphToken, p.log)
	if err != nil {
		return fmt.Errorf("-service-graph-token-file: %w", err)
	}
	// The clock-driven half of that parity, which this call site did NOT have:
	// Tokens() re-reads only when called, so on a listener between pushes a
	// rotation went unnoticed until the next request, which armed the revoked
	// token's grace window at THAT moment — accepting it far past the five
	// minutes the model documents, indefinitely while the listener stayed
	// quiet. The comment above claimed the parity; only Run delivers it.
	go tok.Run(ctx)

	proc := servicegraph.NewProcessor(cfg, p.log)
	reg := servicegraph.NewRegistry(cfg)
	// Before the first Consume, as the package requires: the sink is read on
	// the pairing path under the store's mutex.
	proc.SetSink(reg)
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

	rcv := &sgReceiver{
		grpcAddr: *serviceGraphListen,
		httpAddr: *serviceGraphHTTPListen,
		tokens:   tok.Tokens,
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
	if err := p.startServiceGraphIngest(ctx, owner); err != nil {
		return err
	}
	p.log.Info("trace tier started", "internalGRPC", *serviceGraphListen, "internalHTTP", *serviceGraphHTTPListen,
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
		return nil, fmt.Errorf("exporter does not support traces")
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
	if cfg := p.fileCfg.TraceSampling; cfg != nil && cfg.Enabled() {
		if err := cfg.Validate(); err != nil {
			return nil, err
		}
		chain = tracesample.New(*cfg, chain)
		p.log.Info("trace sampling enabled", "probability", cfg.Probability,
			"maxSpansPerSecond", cfg.MaxSpansPerSecond, "keepSlowerThan", cfg.KeepSlowerThan)
	}
	if *spanMetrics {
		var smCfg spanmetrics.Config
		if p.fileCfg.TraceMetrics != nil {
			smCfg = *p.fileCfg.TraceMetrics
		}
		gen := spanmetrics.New(smCfg)
		chain = gen.Tap(chain)
		smRes := agentSelfResource(*nodeName)
		p.spanMetricsGen, p.spanMetricsRes = gen, smRes
		p.spawn(func() { gen.Run(ctx, p.selfOut, *spanMetricsIv, smRes, p.log) })
		p.log.Info("span metrics from traces enabled", "interval", *spanMetricsIv)
	}
	return &sgPairTap{proc: proc, inner: chain}, nil
}

// sgPairTap feeds the pairing store after a successful export. Consume runs on
// the concurrent receiver goroutines; the pairing store is mutex-guarded for
// exactly that.
type sgPairTap struct {
	proc  *servicegraph.Processor
	inner servicegraph.TracesExporter
}

func (t *sgPairTap) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	if err := t.inner.ExportTraces(ctx, td); err != nil {
		return err
	}
	t.proc.Consume(td)
	return nil
}

// --- the application-facing listeners ---

// startServiceGraphIngest starts the tier's OTLP trace receiver for
// applications: enrich, re-shard, and hand this shard's own share to the owner
// chain.
func (p *pipelines) startServiceGraphIngest(ctx context.Context, owner servicegraph.TracesExporter) error {
	if !*serviceGraphIngest || (*serviceGraphIngestGRPC == "" && *serviceGraphIngestHTTP == "") {
		p.log.Warn("the trace tier accepts no application pushes (-service-graph-ingest=false, or both -service-graph-ingest-grpc and -service-graph-ingest-http are empty); it will only receive spans re-sharded by sibling shards")
		return nil
	}
	resharder, err := serviceGraphResharder(p.fileCfg.ServiceGraphShards, p.log)
	if err != nil {
		return fmt.Errorf("service-graph shards: %w", err)
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
	enr := otlpingest.NewEnricher(ecfg)
	scfg := otlpingest.ServerConfig{
		GRPCAddr: *serviceGraphIngestGRPC,
		HTTPAddr: *serviceGraphIngestHTTP,
		// The application ports share the DaemonSet receiver's admission
		// knobs: trace pushes are the LARGEST payloads a fleet sends, so the
		// raised message cap matters here first.
		MaxInFlight:  *ingestMaxInFlight,
		MaxRecvBytes: *ingestGRPCMaxRecv,
		Enricher:     enr,
		// The application ports are first receipt for traces, so the same
		// reserved-plumbing strip as the DaemonSet's receiver applies; the
		// INTERNAL receiver (sgReceiver) deliberately does not — what arrives
		// there was sanitized when an application pushed it.
		ReservedAttrs: ingestReservedAttrs(),
		// Exporter nil: this listener serves TRACES only. Logs and metrics belong
		// on the node-local DaemonSet, where the sender is a pod on the same node
		// and the payload crosses no network to be attributed.
		Traces: &sgEntry{resharder: resharder, owner: owner},
		// The loop guard belongs on the RECEIVE path, above enrichment: a payload
		// that is going to be refused must not spend a metadata lookup per
		// resource, nor move the ingest counters, on its way to the refusal — and
		// its peer address is a sibling shard, so what enrichment would deduce
		// from it is wrong anyway.
		RejectTraces: func(_ context.Context, td ptrace.Traces) error {
			return refuseForwarded(resharder, td)
		},
		Ready:  p.ready.gate(gateServiceGraphIngest),
		Logger: p.log,
	}
	if p.transforms != nil {
		// The ingest: admission hook (per resource, pre-enrichment; hot
		// reload adds/removes it without a restart — AdmitResource resolves
		// the active program per call and admits when no hook exists). The
		// same wiring as the DaemonSet's receiver (startIngest): the hook's
		// contract covers all three signals, and trace pushes arrive HERE.
		scfg.Admit = p.transforms.AdmitResource
	}
	srv := otlpingest.NewServer(scfg)
	p.spawn(func() {
		if err := srv.Run(ctx); err != nil {
			p.fatal("service-graph trace ingest", err)
		}
	})
	p.log.Info("trace ingest listening", "grpc", *serviceGraphIngestGRPC, "http", *serviceGraphIngestHTTP,
		"peerIPFallback", *ingestPeerIP)
	return nil
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
	d := wait / 2
	if d < time.Second {
		return time.Second
	}
	if d > 30*time.Second {
		return 30 * time.Second
	}
	return d
}

// --- the tier's INTERNAL receiver ---

// sgReceiver is the tier's internal intake: a TRACES-ONLY OTLP receiver, gRPC on
// -service-graph-listen plus optional OTLP/HTTP protobuf on
// -service-graph-http-listen, both behind the shared bearer token.
//
// It is the port that says "this payload is final". What arrives here has
// already been enriched and routed by the shard that received it from an
// application, so this path never enriches (the peer is a sibling shard, not the
// sender) and never re-shards (we are the owner). Both are structural: there is
// no Enricher and no Resharder in this path at all.
//
// # Why not internal/agent/otlpingest
//
// That receiver is what the APPLICATION listeners use, and this one is its
// opposite on the two axes that matter. It is unauthenticated, because its
// senders are every instrumented pod in the cluster; this one must
// authenticate, because what it accepts skips enrichment and routing and a
// forged payload would be exported unattributed. And it enriches by peer
// address, which is exactly the thing that is meaningless here. Reusing it
// would have meant bolting an auth mode and a skip-enrichment mode onto the
// ingest path so neither could be changed without re-reasoning about the other.
//
// The SERVER is separate; the HTTP request seam is NOT. Reading a body, mapping
// a read failure to a status and mapping a forward failure to one are the same
// decisions on both ports and are shared (otlpingest.BodyReader,
// WriteBodyError, GRPCForwardStatus, HTTPForwardStatus) — this file's copies of
// them had already drifted: an over-cap gzip answered 400 "malformed" here and
// 413 there, and 400 tells a kubescrape sender to DROP the batch.
//
// No in-flight shed here, for the reason the application listener needs one:
// that one holds a slot for as long as the whole owner chain takes (a
// re-shard hop plus the collector's ack, up to -otlp-timeout), from senders it
// cannot identify. This one is authenticated and shorter — it runs the owner
// chain, whose one blocking step is the collector export — and the gRPC message
// cap below bounds what a single request can allocate.
type sgReceiver struct {
	grpcAddr string
	httpAddr string
	// tokens is the accepted set, re-read and rotation-aware.
	tokens func() []string
	// consume runs the owner chain (strip the marker, pair, RED metrics, sample,
	// export). Safe to call from the concurrent handler goroutines. It RETURNS AN
	// ERROR, and that error becomes the sending shard's — which becomes the
	// application's, whose retry is the only thing standing between a failed
	// export and a lost span.
	consume func(context.Context, ptrace.Traces) error
	ready   func()
	log     *slog.Logger

	// body reads one OTLP/HTTP body under sgMaxRecvBytes. Lazily built by Run
	// so a zero-value sgReceiver (tests construct one directly) still works.
	body *otlpingest.BodyReader

	// warnGate throttles the rejected-push log; see warnUnauthorized.
	warnGate logdedupe.Throttle
}

// sgAuthRealm is sent on 401s so a client can tell "wrong credentials" from
// "wrong URL".
const sgAuthRealm = `Bearer realm="kubescrape service-graph"`

// sgUnauthorizedMsg is the internal receiver's refusal, spelled once for the
// gRPC tap, the gRPC interceptor and the HTTP handler alike: three sites
// re-typing it is how one drifts into naming a different flag.
const sgUnauthorizedMsg = "missing or invalid bearer token (-service-graph-token-file)"

// msgShardNoListener is the "shard would receive nothing" refusal, spelled
// ONCE: validateConfig raises it so -check-config catches the config before
// the StatefulSet CrashLoops, and sgReceiver.Run raises it again at the real
// start. One const is what keeps the two paths' wording from drifting.
const msgShardNoListener = "-service-graph is set but -service-graph-listen (and -service-graph-http-listen) are empty: the shard would receive nothing"

// sgMaxRecvFloor is the receive cap when the sender's split size is unknown or
// small: the historical 4 MiB, matching a collector's own default. Derived
// with the sender's default split cap as a lower bound so a future raise of
// otlpsplit.DefaultMaxBytes can never silently out-size the internal hop's
// receive floor — today DefaultMaxBytes is 3.75 MiB, so the value is
// unchanged at 4 MiB.
const sgMaxRecvFloor = max(4<<20, otlpsplit.DefaultMaxBytes)

// sgMaxRecvBytes caps one decoded payload on the internal hop.
//
// It must be at least what the SENDING shard will produce, and the sender is
// another kubescrape splitting at -otlp-max-send-bytes. That flag is the
// operator's, tuned for the COLLECTOR's receive limit — so pinning this at a
// constant meant raising it for a collector that accepts 8 MiB silently made
// every over-4-MiB shard-to-shard payload fail, breaking the ring for exactly
// the large traces the raise was for. Derived from the same flag instead, with
// the floor as a lower bound so a small or unset value cannot shrink it.
//
// A NEGATIVE flag disables splitting outright, so "what the sender will
// produce" stops being a split cap and becomes the whole enriched payload —
// bounded by no constant this side can name (the application ports cap what
// enters the ring per push, but enrichment grows a payload by its resource
// count). The receive cap is therefore disabled WITH the splitting: they are
// one decision, and the flag spells it. Reading the negative form as "use the
// floor" instead sent unsplit shares into a sibling capped at 4 MiB — every
// large trace deterministically rejected on the ring, a rejection
// sgForwardStatus hands the application as retryable, so its SDK re-pushed an
// undeliverable payload until the retry budget dropped the spans, with only
// SendsFailed moving.
func sgMaxRecvBytes() int {
	n := *otlpMaxSendBytes
	if n < 0 {
		// grpc-go's own ceiling; the internal hop's BodyReader shares it (Run).
		return math.MaxInt32
	}
	if n > sgMaxRecvFloor {
		return n
	}
	return sgMaxRecvFloor
}

// sgWarnEvery throttles the rejected-push warning: a fleet pointed at the
// wrong token would otherwise write one line per forwarded batch — thousands a
// second — burying the diagnosis in its own symptom.
const sgWarnEvery = 30 * time.Second

// Run serves until ctx is cancelled. A runtime listener failure propagates to
// the caller (fatal there); a cancelled shutdown returns nil.
//
// The bind-then-ready-then-serve-then-drain skeleton is otlpingest.Listeners —
// the SERVERS stay this receiver's own (the auth tap and interceptor on gRPC,
// the bearer check in the HTTP handler, no in-flight shed — see the type doc),
// only the run shape is shared. This file's hand-rolled copy of that shape is
// how the keepalive policy below drifted in the first place.
func (r *sgReceiver) Run(ctx context.Context) error {
	if r.body == nil {
		r.body = otlpingest.NewBodyReader(int64(sgMaxRecvBytes()))
	}
	if r.grpcAddr == "" && r.httpAddr == "" {
		// A shard with no listener pairs nothing, and would report ready and
		// idle forever. Refuse instead — indistinguishable-from-working is the
		// failure mode this whole feature's counters exist to avoid. (Refused
		// HERE, because Listeners.Run treats nothing-configured as a no-op.)
		return errors.New(msgShardNoListener)
	}

	l := otlpingest.Listeners{Name: "service-graph internal", Logger: r.log, Ready: r.ready}
	if r.grpcAddr != "" {
		l.GRPCAddr = r.grpcAddr
		l.GRPC = grpc.NewServer(
			// Reap connections a peer opened and abandoned, and bound a
			// socket's AGE (otlpingest.KeepaliveOption, the policy every
			// kubescrape OTLP receiver shares). The MaxConnectionAge/AgeGrace
			// half is a deliberate behavior change with this adoption: this
			// port used to set only MaxConnectionIdle — a documented drift, an
			// authenticated peer's abandoned stream had no age bound, since a
			// connection carrying an open stream is never idle.
			otlpingest.KeepaliveOption(),
			grpc.MaxRecvMsgSize(sgMaxRecvBytes()),
			// Authenticate on the HEADERS frame, BEFORE grpc-go reads the
			// message. A UnaryInterceptor runs only after recvAndDecompress has
			// pulled the whole thing into memory, so an unauthenticated peer
			// could make this process allocate sgMaxRecvBytes per stream and be
			// refused afterwards — the credential bought nothing it was there
			// to buy. tap.Info carries the request headers (grpc/tap/tap.go),
			// which is exactly what the check needs, and the ingest server
			// already uses a tap for its byte budget for the same reason.
			grpc.InTapHandle(r.authTap),
			// A cap on concurrent streams PER CONNECTION. grpc-go's default is
			// math.MaxUint32, so without it one authenticated connection could
			// hold unbounded concurrent decodes; this receiver has no in-flight
			// semaphore of its own (its senders are sibling shards, not
			// arbitrary applications).
			grpc.MaxConcurrentStreams(sgMaxConcurrentStreams),
			grpc.UnaryInterceptor(r.authUnary),
		)
		ptraceotlp.RegisterGRPCServer(l.GRPC, &sgTraces{r: r})
	}
	if r.httpAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("POST /v1/traces", r.handleHTTPTraces)
		// The shared push-server shape: Slowloris header bound, trickled-body
		// bound, keep-alive reaping, and deliberately no WriteTimeout (its
		// clock would race a slow but legal upload).
		l.HTTP = otlpingest.NewPushHTTPServer(r.httpAddr, mux)
	}
	// Graceful on shutdown: in-flight forwards are already-paid-for spans, and
	// pairing them costs microseconds.
	return l.Run(ctx)
}

// sgMaxConcurrentStreams bounds concurrent RPCs per connection on the internal
// hop. The senders are sibling shards issuing one synchronous forward per push,
// so this is far above the working set; it exists so a single connection cannot
// pin an unbounded number of in-flight sgMaxRecvBytes decodes.
const sgMaxConcurrentStreams = 64

// authorized reports whether the metadata carries an accepted bearer token.
//
// gRPC lower-cases metadata keys and otlpexport sends `authorization: Bearer
// <token>` (otlpexport.grpcAuth), which is the same header its HTTP arm sets —
// one credential, two transports.
func (r *sgReceiver) authorized(md metadata.MD) bool {
	tokens := r.tokens()
	for _, v := range md.Get("authorization") {
		if bearer.Authorized(v, tokens) {
			return true
		}
	}
	return false
}

// authTap rejects an unauthenticated push on the HEADERS frame, before grpc-go
// reads (and allocates) the message. It runs in the transport's I/O goroutine
// with its mutex held, so it must not block: a token read and a constant-time
// compare, both of which internal/bearer already does without I/O (the file is
// re-read on its own schedule).
// The returned context BECOMES the stream's context (http2Server.operateHeaders
// assigns it), so the success path must hand back the one it was given —
// returning nil leaves the stream with no context at all.
func (r *sgReceiver) authTap(ctx context.Context, info *tap.Info) (context.Context, error) {
	if info != nil && r.authorized(info.Header) {
		return ctx, nil
	}
	r.warnUnauthorized("grpc")
	return nil, status.Error(codes.Unauthenticated, sgUnauthorizedMsg)
}

// authUnary re-checks the token after decode. The tap above is what actually
// keeps an unauthenticated peer from spending memory; this stays as the second
// line, so a future grpc-go that stopped running taps (they are marked
// experimental) could not silently open the listener.
func (r *sgReceiver) authUnary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if r.authorized(md) {
		return handler(ctx, req)
	}
	r.warnUnauthorized("grpc")
	return nil, status.Error(codes.Unauthenticated, sgUnauthorizedMsg)
}

// warnUnauthorized logs a rejected push at most once per sgWarnEvery. Silence
// would be worse than noise here: a token mismatch after a botched rotation
// produces no other symptom on either side — the agents' forwards "fail" into
// a counter, and the graph is simply empty.
func (r *sgReceiver) warnUnauthorized(transport string) {
	if !r.warnGate.Allow(sgWarnEvery) {
		return
	}
	r.log.Warn("rejected a service-graph push with a missing or invalid bearer token; the agents' -service-graph-token-file must match this shard's",
		"transport", transport)
}

// sgTraces is the gRPC trace service. Only traces are registered: logs and
// metrics on this port would be an unhandled method, which is the honest
// answer — this listener exists to pair spans.
type sgTraces struct {
	ptraceotlp.UnimplementedGRPCServer
	r *sgReceiver
}

func (g *sgTraces) Export(ctx context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	// The ack is honest only if the whole owner chain succeeded: the sending
	// shard holds no copy after we answer, and its own sender is the only thing
	// that can produce these spans again. An error travels back to the
	// application, whose retry re-pushes the identical batch — which is safe
	// because the taps count only after a successful export.
	if err := g.r.consume(ctx, req.Traces()); err != nil {
		return ptraceotlp.ExportResponse{}, sgForwardStatus(err)
	}
	return ptraceotlp.NewExportResponse(), nil
}

// sgForwardStatus maps an owner-chain failure onto a gRPC status the sending
// shard's exporter classifies correctly. A bare error surfaces as codes.Unknown,
// which otlpexport reads as NON-permanent — fine — but a genuinely permanent
// upstream rejection has to stay permanent, or the sending shard's application
// retries a payload nothing will ever accept.
//
// It is the ingest receiver's classification verbatim, so it IS it now: the two
// receivers must answer the same way, or the same collector failure reads as
// retryable on one port and permanent on the other.
func sgForwardStatus(err error) error { return otlpingest.GRPCForwardStatus(err) }

func (r *sgReceiver) handleHTTPTraces(w http.ResponseWriter, req *http.Request) {
	if !bearer.Authorized(req.Header.Get("Authorization"), r.tokens()) {
		r.warnUnauthorized("http")
		w.Header().Set("WWW-Authenticate", sgAuthRealm)
		w.Header().Set("Cache-Control", "no-store")
		http.Error(w, sgUnauthorizedMsg, http.StatusUnauthorized)
		return
	}
	// otlpingest owns the body reader for BOTH receivers. This one used to
	// have its own copy, and the fix that makes an over-cap GZIP report 413
	// instead of 400 "malformed" landed only in the other — on the one hop
	// whose sender is another kubescrape, whose exporter reads 400 as PERMANENT
	// and drops the batch. The CAP is the parameter (4 MiB here, 16 MiB for
	// application pushes); the byte budget is deliberately absent, as is the
	// in-flight semaphore — see the type doc. So is the door COUNTER: this
	// process serves the unauthenticated application ports too, and
	// kubescrape_ingest_body_rejected_total means "an application push was
	// refused at a listener nothing authenticates" (otlpingest.NewBodyReader).
	body, charged, err := r.body.Read(req)
	if err != nil {
		otlpingest.WriteBodyError(w, err)
		return
	}
	defer r.body.Release(charged)
	er := ptraceotlp.NewExportRequest()
	if err := er.UnmarshalProto(body); err != nil {
		http.Error(w, "malformed OTLP traces payload", http.StatusBadRequest)
		return
	}
	if err := r.consume(req.Context(), er.Traces()); err != nil {
		// The HTTP counterpart of sgForwardStatus: a permanent upstream rejection
		// is 400 (do not retry this batch), everything else 503 (retryable). The
		// sending shard's exporter reads both correctly.
		http.Error(w, err.Error(), otlpingest.HTTPForwardStatus(err))
		return
	}
	otlpingest.WriteProto(w, ptraceotlp.NewExportResponse())
}

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

// tierListener is one address the trace tier binds, with the flag that named it.
type tierListener struct {
	flag string
	addr string
}

// tierListeners are the addresses this process will bind for the trace tier, in
// the order the flags are documented. The application ports are listed only when
// they are actually served (-service-graph-ingest): a dry run that refused a
// collision with a listener the start never binds would be stricter than the
// start, which CrashLoops just as hard as being laxer.
func tierListeners() []tierListener {
	out := []tierListener{
		{"-service-graph-listen", *serviceGraphListen},
		{"-service-graph-http-listen", *serviceGraphHTTPListen},
	}
	if *serviceGraphIngest {
		out = append(out,
			tierListener{"-service-graph-ingest-grpc", *serviceGraphIngestGRPC},
			tierListener{"-service-graph-ingest-http", *serviceGraphIngestHTTP})
	}
	return out
}

// serviceGraphListenersDistinct refuses two of the tier's listeners configured
// on one address.
//
// The tier binds up to four, from four independent flags — and the chart renders
// three of them from values, so `serviceGraph.port: 4317` (warned against in
// values.yaml prose, enforced nowhere) puts the INTERNAL receiver on the
// application gRPC port. The two servers start concurrently, so whichever binds
// second dies with `address already in use` and takes the process with it; which
// one that is varies between restarts. Loud, but only at the real start —
// refused here, beside the ring cross-check, because this is the other place
// that knows more than one listener exists.
func serviceGraphListenersDistinct() error {
	ls := tierListeners()
	for i := range ls {
		for _, other := range ls[i+1:] {
			if sameListenAddr(ls[i].addr, other.addr) {
				return fmt.Errorf("%s and %s are both %q: the tier binds them concurrently, so whichever loses the race fails with `address already in use` and takes the process down. The internal hop and the application ports must be different ports — an internal hop addressed to an application port would also re-enrich and re-shard on every pass",
					ls[i].flag, other.flag, ls[i].addr)
			}
		}
	}
	return nil
}

// sameListenAddr reports whether two listen addresses would contend for one
// socket. Empty disables a listener, so it collides with nothing; an address
// that is not host:port is left to fail at bind, where the error names it.
//
// Hosts must match or one must be a WILDCARD: two different loopback or pod
// addresses on one port are legitimate, while 0.0.0.0 (or "", or ::) covers
// every address on that port and so contends with all of them.
func sameListenAddr(a, b string) bool {
	ha, pa, ok := splitListen(a)
	if !ok {
		return false
	}
	hb, pb, ok := splitListen(b)
	if !ok || pa != pb {
		return false
	}
	return ha == hb || wildcardHost(ha) || wildcardHost(hb)
}

// splitListen splits a listen address into host and normalised port.
func splitListen(addr string) (host, port string, ok bool) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", "", false
	}
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		return "", "", false
	}
	// ":04317" and ":4317" are one port; a NAMED port (":http") is compared as
	// written, which is exact for the equality this is used for.
	if n, err := strconv.Atoi(p); err == nil {
		p = strconv.Itoa(n)
	}
	return h, p, true
}

// wildcardHost reports whether a listen host covers every local address.
// net.SplitHostPort has already stripped the brackets from "[::]:4319", so the
// bracketed spelling never reaches here.
func wildcardHost(h string) bool {
	switch h {
	case "", "0.0.0.0", "::":
		return true
	}
	return false
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
