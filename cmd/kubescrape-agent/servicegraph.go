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
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
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

	p.ready.require(gateServiceGraph)
	rcv := &sgReceiver{
		grpcAddr: *serviceGraphListen,
		httpAddr: *serviceGraphHTTPListen,
		tokens:   tok.Tokens,
		consume:  ownerReceive(owner),
		ready:    func() { p.ready.done(gateServiceGraph) },
		log:      p.log,
	}
	p.spawn(func() {
		if err := rcv.Run(ctx); err != nil {
			// Fatal like the ingest listener: a shard whose internal receiver is
			// dead accepts no re-sharded spans, and it would otherwise sit there
			// looking healthy while its siblings' pushes fail.
			p.log.Error("service-graph receiver failed; shutting down", "error", err)
			ferr := fmt.Errorf("service-graph receiver: %w", err)
			p.fatalErr.CompareAndSwap(nil, &ferr) // first fatal wins
			p.stop()
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

	enr := otlpingest.NewEnricher(otlpingest.Config{
		ContainerIDKeys: splitList(*ingestCidKeys),
		PodUIDKeys:      splitList(*ingestUIDKeys),
		Wait:            *ingestWait,
		EnrichLines:     *enrichOn,
		PeerIPFallback:  *ingestPeerIP,
		PeerReject:      p.peerIsOurOwnWorkload,
		Attrs:           p.attrBuilders.Ingest,
		NodeInfo:        p.nodeInfo,
		Meta:            p.meta,
		Logger:          p.log,
	})
	p.ready.require(gateServiceGraphIngest)
	srv := otlpingest.NewServer(otlpingest.ServerConfig{
		GRPCAddr:    *serviceGraphIngestGRPC,
		HTTPAddr:    *serviceGraphIngestHTTP,
		MaxInFlight: *ingestMaxInFlight,
		Enricher:    enr,
		// Exporter nil: this listener serves TRACES only. Logs and metrics belong
		// on the node-local DaemonSet, where the sender is a pod on the same node
		// and the payload crosses no network to be attributed.
		Traces: &sgEntry{resharder: resharder, owner: owner},
		Ready:  func() { p.ready.done(gateServiceGraphIngest) },
		Logger: p.log,
	})
	p.spawn(func() {
		if err := srv.Run(ctx); err != nil {
			p.log.Error("service-graph trace ingest failed; shutting down", "error", err)
			ferr := fmt.Errorf("service-graph trace ingest: %w", err)
			p.fatalErr.CompareAndSwap(nil, &ferr) // first fatal wins
			p.stop()
		}
	})
	p.log.Info("trace ingest listening", "grpc", *serviceGraphIngestGRPC, "http", *serviceGraphIngestHTTP,
		"peerIPFallback", *ingestPeerIP)
	return nil
}

// sgEntry is the terminal exporter of the APPLICATION listener: it runs after
// otlpingest.Server has enriched the payload, refuses anything that has already
// been through the tier, re-shards the rest, and runs the owner chain over
// whatever this shard owns.
//
// Ordering is fixed by what each step needs. Enrichment happens above (inside
// the server) because it needs the connection's source address, which only
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
	if servicegraph.IsForwarded(td) {
		// An internal hop addressed to the application port. Refusing it
		// PERMANENTLY (InvalidArgument, which otlpexport.IsPermanent classifies as
		// do-not-retry) is what turns that misconfiguration into a bounded,
		// counted failure instead of an amplification loop: enriching this
		// payload would attribute it to the sending SHARD, and re-sharding it
		// would send it round again, on every hop, forever.
		e.resharder.CountLoopBlocked(td.SpanCount())
		return status.Error(codes.InvalidArgument,
			"this payload carries "+servicegraph.ForwardedMarker+": it was re-sharded by another shard and addressed to the tier's APPLICATION port instead of its internal receiver (-service-graph-listen). Point serviceGraphShards at the internal port")
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

	// lastWarn throttles the rejected-push log; see warn.
	lastWarn atomic.Int64
}

// sgAuthRealm is sent on 401s so a client can tell "wrong credentials" from
// "wrong URL".
const sgAuthRealm = `Bearer realm="kubescrape service-graph"`

// sgMaxRecvBytes caps one decoded payload. The agents' own
// -otlp-max-send-bytes splits at ~3.75 MiB, under this, so a payload that
// trips it is a misconfigured sender rather than a big one — and this is the
// only hard bound on what a single push can allocate here.
const sgMaxRecvBytes = 4 << 20

// sgWarnEvery throttles the rejected-push warning: a fleet pointed at the
// wrong token would otherwise write one line per forwarded batch — thousands a
// second — burying the diagnosis in its own symptom.
const sgWarnEvery = 30 * time.Second

// Run serves until ctx is cancelled. A runtime listener failure propagates to
// the caller (fatal there); a cancelled shutdown returns nil.
func (r *sgReceiver) Run(ctx context.Context) error {
	if r.body == nil {
		r.body = otlpingest.NewBodyReader(sgMaxRecvBytes)
	}
	if r.grpcAddr == "" && r.httpAddr == "" {
		// A shard with no listener pairs nothing, and would report ready and
		// idle forever. Refuse instead — indistinguishable-from-working is the
		// failure mode this whole feature's counters exist to avoid.
		return errors.New("-service-graph is set but -service-graph-listen (and -service-graph-http-listen) are empty: the shard would receive nothing")
	}

	var grpcSrv *grpc.Server
	var httpSrv *http.Server
	errc := make(chan error, 2)

	if r.grpcAddr != "" {
		lis, err := net.Listen("tcp", r.grpcAddr)
		if err != nil {
			return fmt.Errorf("service-graph gRPC listen %s: %w", r.grpcAddr, err)
		}
		grpcSrv = grpc.NewServer(
			// Reap connections an agent opened and abandoned (default gRPC
			// keeps them forever), as the ingest server does.
			grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionIdle: 120 * time.Second}),
			grpc.MaxRecvMsgSize(sgMaxRecvBytes),
			grpc.UnaryInterceptor(r.authUnary),
		)
		ptraceotlp.RegisterGRPCServer(grpcSrv, &sgTraces{r: r})
		go func() { errc <- grpcSrv.Serve(lis) }()
		r.log.Info("service-graph gRPC receiver listening", "addr", r.grpcAddr)
	}

	if r.httpAddr != "" {
		// Listened explicitly rather than through ListenAndServe so the BIND
		// failure is known before the readiness gate below clears: a probe that
		// goes green on a port nobody bound is worse than no probe.
		lis, err := net.Listen("tcp", r.httpAddr)
		if err != nil {
			if grpcSrv != nil {
				grpcSrv.Stop()
			}
			return fmt.Errorf("service-graph HTTP listen %s: %w", r.httpAddr, err)
		}
		mux := http.NewServeMux()
		mux.HandleFunc("POST /v1/traces", r.handleHTTPTraces)
		// Timeouts as on the ingest receiver: ReadHeaderTimeout kills Slowloris
		// header trickling, ReadTimeout bounds a trickled body, IdleTimeout
		// reaps parked keep-alives. WriteTimeout is omitted — the responses are
		// a few bytes and its clock would race a slow but legal upload.
		httpSrv = &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       60 * time.Second,
			IdleTimeout:       120 * time.Second,
		}
		go func() {
			if err := httpSrv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errc <- err
				return
			}
			errc <- nil
		}()
		r.log.Info("service-graph HTTP receiver listening", "addr", r.httpAddr)
	}

	if r.ready != nil {
		r.ready()
	}

	var runErr error
	select {
	case <-ctx.Done():
	case err := <-errc:
		if err != nil {
			r.log.Error("service-graph receiver listener failed", "error", err)
			runErr = fmt.Errorf("listener: %w", err)
		}
	}
	if grpcSrv != nil {
		// Graceful: in-flight forwards are already-paid-for spans, and pairing
		// them costs microseconds.
		grpcSrv.GracefulStop()
	}
	if httpSrv != nil {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(sctx)
	}
	return runErr
}

// authUnary rejects a push that does not carry an accepted bearer token.
//
// gRPC lower-cases metadata keys and otlpexport sends `authorization: Bearer
// <token>` (otlpexport.grpcAuth), which is the same header its HTTP arm sets —
// one credential, two transports.
func (r *sgReceiver) authUnary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	tokens := r.tokens()
	for _, v := range md.Get("authorization") {
		if bearer.Authorized(v, tokens) {
			return handler(ctx, req)
		}
	}
	r.warnUnauthorized("grpc")
	return nil, status.Error(codes.Unauthenticated, "missing or invalid bearer token (-service-graph-token-file)")
}

// warnUnauthorized logs a rejected push at most once per sgWarnEvery. Silence
// would be worse than noise here: a token mismatch after a botched rotation
// produces no other symptom on either side — the agents' forwards "fail" into
// a counter, and the graph is simply empty.
func (r *sgReceiver) warnUnauthorized(transport string) {
	now := time.Now().UnixNano()
	last := r.lastWarn.Load()
	if now-last < int64(sgWarnEvery) || !r.lastWarn.CompareAndSwap(last, now) {
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
		http.Error(w, "missing or invalid bearer token (-service-graph-token-file)", http.StatusUnauthorized)
		return
	}
	// otlpingest owns the body reader for BOTH receivers. This one used to
	// have its own copy, and the fix that makes an over-cap GZIP report 413
	// instead of 400 "malformed" landed only in the other — on the one hop
	// whose sender is another kubescrape, whose exporter reads 400 as PERMANENT
	// and drops the batch. The CAP is the parameter (4 MiB here, 16 MiB for
	// application pushes); the byte budget is deliberately absent, as is the
	// in-flight semaphore — see the type doc.
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
		cfg.StatefulSet, cfg.Service = name, name
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
