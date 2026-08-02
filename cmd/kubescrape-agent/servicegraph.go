package main

// The service-graph feature's wiring, both halves of it.
//
//   - The SHARD role (-service-graph): a receiver for spans other agents
//     forwarded, the pairing processor they feed, and the edge-metric registry
//     that exports what pairs.
//   - The AGENT half (-service-graph-shards / -service-graph-endpoint, or the
//     config's serviceGraphShards section): the ring, and the tap that ships a
//     trimmed copy of each ingested span to the shard owning its trace.
//
// Everything about pairing, ring placement, trimming and the emitted series
// lives in internal/agent/servicegraph; this file is only the seam between
// that package and the process — flags, listeners, readiness, shutdown.
//
// Why two roles in one binary: the shard tier IS the agent binary with every
// per-node pipeline off (charts/kubescrape/templates/servicegraph.yaml), the
// same way the events/Azure singleton is. It needs the same exporter, the same
// self-metrics identity and the same config file, and a second binary would
// duplicate all of it to save one flag.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/gzip"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/JohanLindvall/kubescrape/internal/agent/servicegraph"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// gateServiceGraph is satisfied once the shard's receiver is BOUND — not once
// it has received something.
//
// A shard that has been forwarded nothing yet is working; gating on the first
// payload would leave a freshly-scaled tier permanently not-ready in a cluster
// whose agents are still rolling out, and a StatefulSet rollout advances on
// this probe. Bound is the honest claim: the port answers, and an agent's
// forward will land.
const gateServiceGraph = "service-graph-receiver"

// --- the shard role ---

// startServiceGraph starts the pairing shard: receiver, sweeper and the edge
// metric export loop. Off unless -service-graph.
func (p *pipelines) startServiceGraph() error {
	if !*serviceGraphOn {
		if p.fileCfg.ServiceGraph != nil {
			// Symmetric with the traceSampling/-ingest-span-metrics warnings: a
			// configured section that silently does nothing is indistinguishable
			// from one that is working.
			p.log.Warn("serviceGraph configured but ignored: the pairing role is off (-service-graph=false); this section belongs on the shard tier")
		}
		return nil
	}
	var cfg servicegraph.Config
	if p.fileCfg.ServiceGraph != nil {
		cfg = *p.fileCfg.ServiceGraph
	}

	// The receiver accepts spans from every agent in the cluster, so the hop is
	// authenticated — validateConfig already refused an empty -service-graph-
	// token-file (fatal there so -check-config catches it too); the READ is
	// fatal here for the metadata service's reason: an unreadable or empty
	// token file must stop the process, never open the listener with nothing to
	// check against.
	tok, err := newRotatingToken(*serviceGraphToken, p.log)
	if err != nil {
		return fmt.Errorf("-service-graph-token-file: %w", err)
	}

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

	// The shard's own resource identity, like the span-metrics generator's: the
	// edge's two services are DATA-POINT labels (client/server), never the
	// emitting process's identity — a shard describes other objects, it is not
	// one of them.
	res := agentSelfResource(*nodeName)
	p.serviceGraphProc, p.serviceGraphReg, p.serviceGraphRes = proc, reg, res
	p.spawn(func() { reg.Run(p.ctx, p.selfOut, *serviceGraphIv, res, p.log) })
	p.spawn(func() { sweepServiceGraph(p.ctx, proc) })

	p.ready.require(gateServiceGraph)
	rcv := &sgReceiver{
		grpcAddr: *serviceGraphListen,
		httpAddr: *serviceGraphHTTPListen,
		tokens:   tok.tokens,
		consume:  proc.Consume,
		ready:    func() { p.ready.done(gateServiceGraph) },
		log:      p.log,
	}
	p.spawn(func() {
		if err := rcv.Run(p.ctx); err != nil {
			// Fatal like the ingest listener: a shard whose receiver is dead
			// pairs nothing, and it would otherwise sit there looking healthy
			// while the whole cluster's graph is empty.
			p.log.Error("service-graph receiver failed; shutting down", "error", err)
			ferr := fmt.Errorf("service-graph receiver: %w", err)
			p.fatalErr.CompareAndSwap(nil, &ferr) // first fatal wins
			p.stop()
		}
	})
	p.log.Info("service-graph shard started", "grpc", *serviceGraphListen, "http", *serviceGraphHTTPListen,
		"wait", proc.Wait(), "interval", *serviceGraphIv)
	return nil
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

// --- the shard's span receiver ---

// sgReceiver is the shard's intake: a TRACES-ONLY OTLP receiver, gRPC on
// -service-graph-listen plus optional OTLP/HTTP protobuf on
// -service-graph-http-listen, both behind the shared bearer token.
//
// # Why not internal/agent/otlpingest
//
// That receiver is the -ingest feature, and the shard is its opposite on every
// axis that matters. It requires an Enricher and a logs+metrics Exporter and
// registers all three OTLP services; the shard runs with -ingest=false, accepts
// only traces and forwards nothing anywhere. It is deliberately
// UNAUTHENTICATED because it is node-local — its only defence is the in-flight
// shed — while this listener is reachable from every pod in the cluster and
// must authenticate. Reusing it would have meant bolting an auth mode and a
// consume-instead-of-forward mode onto the ingest path for a listener that
// shares none of its concerns (and neither role could then be changed without
// re-reasoning about the other). What is actually worth sharing — the pdata
// OTLP services, the body handling, the token-file contract — is shared by
// following the same shapes, not by widening an existing type.
//
// No in-flight shed here, for the same reason otlpingest needs one: that
// receiver holds a slot for as long as the COLLECTOR takes to ack a forwarded
// payload (up to -otlp-timeout), from senders it cannot identify. This one is
// authenticated and terminal — Consume is an in-memory upsert bounded by
// maxItems and returns immediately — so a request occupies memory for its own
// decode and nothing longer. The gRPC message cap below bounds that.
type sgReceiver struct {
	grpcAddr string
	httpAddr string
	// tokens is the accepted set, re-read and rotation-aware.
	tokens func() []string
	// consume is servicegraph.Processor.Consume: safe to call from the
	// concurrent handler goroutines (the pairing store is mutex-guarded).
	consume func(ptrace.Traces)
	ready   func()
	log     *slog.Logger

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
		if authorizedBearer(v, tokens) {
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
	// Consume is terminal and cannot fail: pairing either stores the span,
	// completes an edge or counts a refusal (obs.ServiceGraphStoreFull). The
	// ack is therefore honest — nothing downstream can still reject it, and a
	// sender retry would double-count the edge.
	g.r.consume(req.Traces())
	return ptraceotlp.NewExportResponse(), nil
}

func (r *sgReceiver) handleHTTPTraces(w http.ResponseWriter, req *http.Request) {
	if !authorizedBearer(req.Header.Get("Authorization"), r.tokens()) {
		r.warnUnauthorized("http")
		w.Header().Set("WWW-Authenticate", sgAuthRealm)
		w.Header().Set("Cache-Control", "no-store")
		http.Error(w, "missing or invalid bearer token (-service-graph-token-file)", http.StatusUnauthorized)
		return
	}
	body, err := sgReadBody(req)
	if err != nil {
		http.Error(w, err.Error(), sgBodyStatus(err))
		return
	}
	er := ptraceotlp.NewExportRequest()
	if err := er.UnmarshalProto(body); err != nil {
		http.Error(w, "malformed OTLP traces payload", http.StatusBadRequest)
		return
	}
	r.consume(er.Traces())
	b, err := ptraceotlp.NewExportResponse().MarshalProto()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	_, _ = w.Write(b)
}

var (
	// errSGBodyTooLarge maps to 413: truncating silently would ack a payload
	// whose tail was dropped.
	errSGBodyTooLarge = fmt.Errorf("request body exceeds %d bytes", sgMaxRecvBytes)
	// errSGUnsupportedType maps to 415 (wrong media type, not a bad request).
	errSGUnsupportedType = errors.New("unsupported Content-Type")
)

func sgBodyStatus(err error) int {
	switch {
	case errors.Is(err, errSGBodyTooLarge):
		return http.StatusRequestEntityTooLarge
	case errors.Is(err, errSGUnsupportedType):
		return http.StatusUnsupportedMediaType
	}
	return http.StatusBadRequest
}

// sgReadBody reads one OTLP/HTTP protobuf body, gzip included (otlpexport
// compresses by default), under the same cap the gRPC arm enforces — the two
// transports must not offer different limits for the same payload.
func sgReadBody(req *http.Request) ([]byte, error) {
	if ct := req.Header.Get("Content-Type"); ct != "" {
		// Parameterized types ("application/x-protobuf; charset=...") are fine;
		// only the media type itself must match.
		if mt, _, err := mime.ParseMediaType(ct); err != nil || mt != "application/x-protobuf" {
			return nil, fmt.Errorf("%w %q (want application/x-protobuf)", errSGUnsupportedType, ct)
		}
	}
	var src io.Reader = req.Body
	switch enc := req.Header.Get("Content-Encoding"); enc {
	case "", "identity":
	case "gzip":
		zr, err := gzip.NewReader(io.LimitReader(req.Body, sgMaxRecvBytes+1))
		if err != nil {
			return nil, fmt.Errorf("gzip body: %w", err)
		}
		defer func() { _ = zr.Close() }()
		src = zr
	default:
		return nil, fmt.Errorf("unsupported Content-Encoding %q (want gzip or identity)", enc)
	}
	// The cap applies to the DECOMPRESSED size too (zip-bomb guard); one byte
	// past it distinguishes at-cap from over-cap.
	body, err := io.ReadAll(io.LimitReader(src, sgMaxRecvBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > sgMaxRecvBytes {
		return nil, errSGBodyTooLarge
	}
	return body, nil
}

// --- the agent half: forwarding to the shards ---

// serviceGraphForwarder builds the shard forwarder from the flags and the
// config section, or (nil, nil) when the feature is off — so the caller can
// wire it unconditionally.
func serviceGraphForwarder(sec *servicegraph.ForwardConfig, log *slog.Logger) (*servicegraph.Forwarder, error) {
	cfg, err := serviceGraphShardConfig(sec)
	if err != nil {
		return nil, err
	}
	return servicegraph.NewForwarder(cfg, baseExportConfig(), log)
}

// serviceGraphShardConfig merges the flag surface into the config's
// serviceGraphShards section.
//
// PRECEDENCE: the config section wins, field by field; the flags fill in what
// it leaves unset. The chart renders the flags and nothing else, so they have
// to work alone — but an operator who reaches for the section is asking for
// something the flags cannot express (explicit endpoints outside Kubernetes, a
// TLS'd hop, tokensPerShard), and having the flags override it would make the
// richer form unusable in exactly the deployment that renders them.
//
// It touches nothing: no DNS, no filesystem, no namespace resolution (that
// happens in shardTargets at start), so -check-config runs it as-is.
func serviceGraphShardConfig(sec *servicegraph.ForwardConfig) (servicegraph.ForwardConfig, error) {
	var cfg servicegraph.ForwardConfig
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
	if cfg.BearerTokenFile == "" {
		// The SAME flag as the shard's listener credential: one Secret, mounted
		// on both workloads, so the two sides cannot be given different tokens
		// by a config that looks complete.
		cfg.BearerTokenFile = *serviceGraphToken
	}
	// Half a template is the one "disabled" state worth refusing: it reads as
	// configured and forwards nothing, which is indistinguishable from the
	// feature being off. ForwardConfig.Validate refuses the same shapes, but in
	// the section's spelling — these messages name the FLAG the operator
	// actually set.
	switch {
	case *serviceGraphShards > 0 && cfg.StatefulSet == "" && len(cfg.Endpoints) == 0:
		return cfg, fmt.Errorf("-service-graph-shards=%d has nothing to address: set -service-graph-endpoint to the shard tier's governing headless Service, or name the shards in the config's serviceGraphShards section", *serviceGraphShards)
	case *serviceGraphEndpoint != "" && cfg.Replicas <= 0:
		return cfg, fmt.Errorf("-service-graph-endpoint %q has no shard count: set -service-graph-shards to the StatefulSet's replica count (it is part of the ring's definition, so every agent must be given the same number)", *serviceGraphEndpoint)
	}
	return cfg, nil
}

// parseShardEndpoint reads the shard tier's GOVERNING HEADLESS SERVICE address
// — `<statefulset>.<namespace>.svc[.cluster.local][:port]`, which is what the
// chart renders — into the template ForwardConfig expands to each shard's
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
