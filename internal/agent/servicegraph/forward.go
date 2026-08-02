package servicegraph

// The AGENT side of the service graph: trim each ingested batch down to what
// pairing needs, hash every span onto the shard ring by trace id, and ship
// each group to its owner. Nothing here pairs anything — the agent has half
// the data by construction (see the package doc's Topology section); it is a
// distributor.
//
// The shard tier runs the SAME binary with -service-graph and no shards
// configured, so it never constructs a Forwarder and cannot re-forward. See
// "Loop prevention" below for why that structural fact is still backed by a
// marker attribute.

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/selfmeta"
)

// TracesExporter forwards traces onward. Structurally identical to the ingest
// server's and spanmetrics' own interfaces, so a Tap satisfies all three and
// the taps stack.
type TracesExporter interface {
	ExportTraces(ctx context.Context, td ptrace.Traces) error
}

// ForwardedMarker is stamped as a boolean resource attribute on every payload
// a Forwarder emits, and a Forwarder REFUSES to forward a payload that already
// carries it.
//
// # Loop prevention
//
// The primary defence is structural: forwarding needs a *Forwarder, a
// Forwarder needs a shard list, and a shard is configured with -service-graph
// (the pairing side) and NO shard list. A shard therefore has nothing to
// forward with — not a check that can fail, an object that does not exist.
//
// This marker is the second line, and it earns its one attribute per resource
// because the failure it prevents is unbounded amplification rather than a
// wrong number: a shard tier misconfigured to point at itself (or at the
// agents' own ingest — an easy copy-paste, both are OTLP receivers on 4317)
// would multiply every span by the fan-out on every hop until the cluster's
// network is the incident. A drop-if-marked check turns that from an outage
// into a counter (ForwardStats.LoopsBlocked) that says exactly what is wrong. It also
// covers the case the structural argument cannot: an operator who enables BOTH
// -service-graph and -service-graph-shards on one deployment, which is a
// legal-looking config that reads like "shard AND forward".
//
// The pairing side is expected to drop this attribute from anything it
// derives; it is a transport detail, not a property of the described service.
const ForwardedMarker = "kubescrape.service_graph.forwarded"

// ForwardedDimensions and ForwardedPeerAttributes declare, on every forwarded
// resource, WHICH attribute keys this agent kept — the effective
// serviceGraphShards.dimensions and .peerAttributes, canonically encoded (see
// encodeAttrList).
//
// # Why the agent has to say
//
// The two sides' lists must agree. Trim keeps the configured keys and drops
// everything else, so a dimension the shard reads but the agent does not
// forward can only ever render as an empty label, and a peer attribute it does
// not forward disables the virtual node that was the only thing naming an
// uninstrumented dependency. Both failures produce a graph that looks fine and
// is wrong, which is worse than one that is missing.
//
// Nothing else can notice. The shard cannot read the agents' config and the
// agents cannot read the shard's; the chart renders both from one values block,
// but the chart is not the only way either binary is run (deploy/*.yaml, a
// hand-written ConfigMap, a partially-rolled upgrade). So the claim rides on
// the payload that already crosses between them, and the shard compares it
// against its own (see mismatchWatch).
//
// One attribute pair per RESOURCE, not per span, and both strings are built
// once at construction: the cost is two map writes on a path that already
// copies a resource's attributes.
const (
	ForwardedDimensions     = "kubescrape.service_graph.dimensions"
	ForwardedPeerAttributes = "kubescrape.service_graph.peer_attributes"
)

// DefaultShardPort is the port a shard target takes when the config names none.
//
// It MUST equal the shard receiver's own default listen address
// (cmd/kubescrape-agent's -service-graph-listen, :4319), and the two are pinned
// against each other by a test there. It was 4317 — the AGENTS' OWN ingest port
// — so a config that relied on the default addressed a port the shard pods do
// not serve at all (they run with -ingest=false), and every forward failed into
// a counter while the graph stayed empty. On any deployment that DID run ingest
// on those pods it would have been worse: spans looping back through the
// fan-out, which is the failure ForwardedMarker exists to stop.
const DefaultShardPort = 4319

const defaultShardProtocol = "grpc"

const (
	// forwardWarnEvery throttles the forwarder's warnings — a failed shard send
	// and a shed batch alike, which share one throttle because they share one
	// cause. Both are per INGEST RPC, so a shard that is down would otherwise
	// write one line per pushed batch — thousands a second on a busy node,
	// drowning the log the operator needs to diagnose it. The counters are the
	// real signal; the line exists to name the shard and the error now and then.
	forwardWarnEvery = 30 * time.Second
)

// keepResourceAttrs are the resource attributes a trimmed payload carries: the
// service identity that IS the graph's node, and the k8s identity the shard
// needs to say WHICH pod of that service — everything else on an enriched
// resource (labels, annotations, owner chains, the operator's static
// attributes) is the agent's local knowledge, already attached to the spans
// the collector received on the normal path, and repeating it on the graph hop
// buys nothing an edge can use.
var keepResourceAttrs = map[string]bool{
	"service.name":        true,
	"service.namespace":   true,
	"service.instance.id": true,
	"k8s.namespace.name":  true,
	"k8s.pod.name":        true,
	"k8s.pod.uid":         true,
	"k8s.container.name":  true,
	"k8s.node.name":       true,
}

// keepSpanAttrs are the span attributes kept regardless of configuration: the
// ones that name the far side of a call or classify it. The configured
// Dimensions and PeerAttributes are unioned in on top, and with the DEFAULT
// peer list this set is redundant — it is a floor under the one config mistake
// this design makes easy. The agent's PeerAttributes must match the shard's
// virtualNodePeerAttributes, and an operator who tunes the shard's list and
// forgets the agent's would otherwise trim away exactly the attributes the
// shard is looking for: every database and messaging call would silently
// render as a plain service-to-service edge, or as no edge at all where a
// virtual node was the only thing naming the far side. That disagreement is
// now DETECTED — the agent declares its effective lists on every forwarded
// resource (ForwardedDimensions) and the shard compares them with its own —
// but detection is after the fact, and these keys stop the most damaging half
// of the mistake from happening at all. A wrong graph is worse than a missing
// one, and a handful of attribute keys are a cheap floor.
//
// The floor MUST cover every key the pairing side classifies on, which is why
// the post-semconv-1.30 database spellings are here beside Tempo's older pair:
// the shard's namesDatabase reads all four (processor.go, databaseAttrs), and an
// SDK on today's conventions emits ONLY db.system.name/db.namespace. Keeping
// just the old pair made that support dead code in the one topology that exists
// — the agent trims, the shard reads — so every modern database client rendered
// as a plain service-to-service call. mismatchWatch cannot notice: it compares
// the two sides' CONFIGURED lists, and the floor is in neither.
var keepSpanAttrs = map[string]bool{
	"messaging.system": true,
	"db.system":        true,
	"db.name":          true,
	"db.system.name":   true, // semconv 1.30 rename of db.system
	"db.namespace":     true, // semconv 1.30 rename of db.name
	"peer.service":     true,
}

// ForwardConfig is the agent config's `serviceGraphShards` section: where the
// shard tier is, and how to reach it.
//
// # Why a StatefulSet template rather than a list of endpoints
//
// Both are offered, but the template is the intended form and the one the
// chart renders. The ring is only useful if EVERY agent computes the identical
// one: two agents disagreeing about the shard set send the two halves of a
// request to two different shards, and the edge simply never forms — silently,
// with both agents reporting success. A template makes disagreement take a
// visible form (a different replica count) instead of an invisible one (a
// list with one entry misspelled, reordered — reordering is harmless, the ring
// sorts — or stale by one entry). It is also the only form that matches how
// the tier actually scales: `kubectl scale` changes one number, and the shard
// names are then derivable rather than transcribable.
//
// Endpoints exists as the escape hatch: a shard tier outside Kubernetes, a
// test, or an operator who wants to name pods explicitly. When set it wins,
// and the shard NAME is the endpoint string itself (so the ring is stable
// against reordering but not against renaming a host).
type ForwardConfig struct {
	// StatefulSet is the shard tier's StatefulSet name. Its pods are the
	// shards, named <statefulSet>-0 .. <statefulSet>-<replicas-1> — the
	// ordinal identity that makes the ring's tokens derivable (see Ring).
	StatefulSet string `json:"statefulSet,omitempty"`
	// Replicas is the shard count. It must match the StatefulSet's spec: a
	// larger value addresses pods that do not exist (their traces are never
	// paired), a smaller one leaves the tail shards idle. Both are visible in
	// ForwardStats/logs rather than silent.
	Replicas int `json:"replicas,omitempty"`
	// Service is the governing headless Service. Empty defaults to
	// StatefulSet, the usual convention.
	Service string `json:"service,omitempty"`
	// Namespace holds the shard tier. Empty resolves to the agent's own
	// namespace ($POD_NAMESPACE or the ServiceAccount projection).
	Namespace string `json:"namespace,omitempty"`
	// Port is the shards' OTLP port (default DefaultShardPort, which is the
	// shard receiver's own default listen port).
	Port int `json:"port,omitempty"`
	// Endpoints names the shards explicitly, bypassing the template. With
	// protocol: http these must carry their own http:// or https:// scheme —
	// the template form derives one (see httpTLS), an explicit endpoint is
	// taken verbatim.
	Endpoints []string `json:"endpoints,omitempty"`

	// TokensPerShard is the ring's virtual tokens per shard (0 =
	// DefaultTokensPerShard). It MUST be identical across agents — it is part
	// of the ring's definition, not a local tuning knob.
	TokensPerShard int `json:"tokensPerShard,omitempty"`

	// BearerTokenFile authenticates to the shard receiver. The shard tier is
	// cluster-reachable OTLP, so the hop is authenticated exactly like the
	// metadata service's /v1/scrape-auth: a shared token in a mounted file,
	// re-read periodically (otlpexport.Config.BearerTokenFile re-reads once a
	// minute, which is the repo's existing rotation contract — no new auth
	// scheme, no lockstep restart when the Secret rotates).
	BearerTokenFile string `json:"bearerTokenFile,omitempty"`
	// Protocol is "grpc" (default) or "http".
	Protocol string `json:"protocol,omitempty"`
	// Insecure sends gRPC in plaintext. nil defaults to "plaintext unless TLS is
	// asked for" — a caFile or insecureSkipVerify: pod-to-pod inside the
	// cluster, where the bearer token is the authenticator. Set caFile (or run
	// the hop through a mesh) when the pod network is not trusted — otherwise
	// the token crosses it in the clear. With protocol: http this decides the
	// derived endpoint's SCHEME instead (see httpTLS).
	Insecure *bool `json:"insecure,omitempty"`
	// InsecureSkipVerify disables certificate verification on the shard hop —
	// and, like CAFile, implies TLS: without that the http protocol would take
	// the plaintext scheme and never handshake at all.
	InsecureSkipVerify *bool `json:"insecureSkipVerify,omitempty"`
	// CAFile verifies the shard receiver's certificate.
	CAFile string `json:"caFile,omitempty"`
	// Headers are static headers on every shard send.
	Headers map[string]string `json:"headers,omitempty"`

	// Dimensions must MATCH the shard's serviceGraph.dimensions: a dimension
	// the agent trims away is a label the shard can only ever render empty.
	// Configured here rather than read from the shard because there is no
	// config channel between the two, and guessing would mean shipping every
	// attribute — the thing Trim exists to avoid. A DISAGREEMENT is detected
	// though: both lists ride on every forwarded resource and the shard
	// compares them (ForwardedDimensions, mismatch.go).
	Dimensions []string `json:"dimensions,omitempty"`
	// PeerAttributes must likewise match the shard's
	// serviceGraph.virtualNodePeerAttributes. nil takes Tempo's defaults.
	PeerAttributes []string `json:"peerAttributes,omitempty"`
}

// Enabled reports whether the agent should forward at all.
func (c *ForwardConfig) Enabled() bool {
	if c == nil {
		return false
	}
	return len(c.Endpoints) > 0 || (c.StatefulSet != "" && c.Replicas > 0)
}

// Validate checks the section's SHAPE only — no filesystem, no DNS, no
// namespace resolution — so -check-config stays a pure dry run.
func (c *ForwardConfig) Validate() error {
	if c == nil || !c.Enabled() {
		// A half-filled template is the one "disabled" state worth refusing:
		// `statefulSet` with no `replicas` reads as configured and forwards
		// nothing, which is indistinguishable from the feature being off.
		if c != nil && c.StatefulSet != "" && c.Replicas <= 0 {
			return fmt.Errorf("serviceGraphShards.statefulSet is set but replicas is %d", c.Replicas)
		}
		if c != nil && c.Replicas > 0 && c.StatefulSet == "" && len(c.Endpoints) == 0 {
			return fmt.Errorf("serviceGraphShards.replicas is set but statefulSet is empty")
		}
		return nil
	}
	switch c.Protocol {
	case "", "grpc", "http":
	default:
		return fmt.Errorf("serviceGraphShards.protocol %q (want grpc or http)", c.Protocol)
	}
	if c.Port < 0 || c.Port > 65535 {
		return fmt.Errorf("serviceGraphShards.port %d out of range", c.Port)
	}
	if c.TokensPerShard < 0 {
		return fmt.Errorf("serviceGraphShards.tokensPerShard must not be negative")
	}
	for i, e := range c.Endpoints {
		if strings.TrimSpace(e) == "" {
			return fmt.Errorf("serviceGraphShards.endpoints[%d] is empty", i)
		}
	}
	return nil
}

// shardTargets resolves the configured shard set to (name, endpoint) pairs.
// The NAME is what the ring hashes, so it must be stable across agents and
// across restarts: the pod's ordinal identity for the template form, the
// endpoint string itself for the explicit form.
func (c *ForwardConfig) shardTargets() ([]shardTarget, error) {
	if len(c.Endpoints) > 0 {
		out := make([]shardTarget, 0, len(c.Endpoints))
		for _, e := range c.Endpoints {
			e = strings.TrimSpace(e)
			out = append(out, shardTarget{name: e, endpoint: e})
		}
		return out, nil
	}
	ns := c.Namespace
	if ns == "" {
		ns = selfmeta.Namespace()
	}
	if ns == "" {
		return nil, fmt.Errorf("serviceGraphShards.namespace is empty and the agent's own namespace could not be resolved ($POD_NAMESPACE or the ServiceAccount projection)")
	}
	svc := c.Service
	if svc == "" {
		svc = c.StatefulSet
	}
	port := c.Port
	if port == 0 {
		port = DefaultShardPort
	}
	out := make([]shardTarget, 0, c.Replicas)
	for i := 0; i < c.Replicas; i++ {
		// The StatefulSet's per-pod stable DNS. Deliberately NOT the headless
		// Service name: that round-robins, which would send a trace's two
		// halves to two different shards — the exact failure the ring exists
		// to prevent.
		name := fmt.Sprintf("%s-%d", c.StatefulSet, i)
		host := fmt.Sprintf("%s.%s.%s.svc:%d", name, svc, ns, port)
		if c.protocol() == "http" {
			scheme := "http://"
			if c.httpTLS() {
				scheme = "https://"
			}
			host = scheme + host
		}
		out = append(out, shardTarget{name: name, endpoint: host})
	}
	return out, nil
}

func (c *ForwardConfig) protocol() string {
	if c.Protocol == "" {
		return defaultShardProtocol
	}
	return c.Protocol
}

// httpTLS decides the SCHEME of a template-derived http-protocol endpoint.
//
// otlpexport ignores Config.Insecure for HTTP — there the scheme IS the
// decision — so the TLS intent expressed in this section has to be translated
// into one. All three ways of expressing it count: an explicit `insecure`
// (which wins, so `insecure: true` beside a caFile produces http:// and
// otlpexport then refuses the contradiction loudly rather than quietly
// handshaking), a caFile, and `insecureSkipVerify` — the last being the case
// that used to fall through: an operator pointing `protocol: http` at a shard
// with a self-signed certificate and no CA got PLAINTEXT, silently, with the
// bearer token on the wire in the clear.
//
// Explicit `endpoints` are exempt: there the operator writes the scheme.
func (c *ForwardConfig) httpTLS() bool {
	if c.Insecure != nil {
		return !*c.Insecure
	}
	return c.CAFile != "" || (c.InsecureSkipVerify != nil && *c.InsecureSkipVerify)
}

type shardTarget struct {
	name     string // the ring key
	endpoint string
}

// clientConfig builds one shard's exporter config from the flag-built base.
//
// It inherits the base's TRANSPORT tuning (timeout, compression, send-size
// cap) but never its CREDENTIALS or destination: bearer token, headers, TLS
// material and endpoint are all destination-scoped, and inheriting them would
// present the collector's token to the shard tier — a credential leak across a
// trust boundary caused by leaving a field empty. Everything the shard hop
// authenticates with is named in ForwardConfig, explicitly.
func (c *ForwardConfig) clientConfig(t shardTarget, base otlpexport.Config) otlpexport.Config {
	// The same TLS intent the http scheme is derived from, so the two transports
	// cannot disagree (TLS material plus plaintext is refused by otlpexport.New).
	insecure := !c.httpTLS()
	skip := false
	if c.InsecureSkipVerify != nil {
		skip = *c.InsecureSkipVerify
	}
	return otlpexport.Config{
		Endpoint:           t.endpoint,
		Protocol:           c.protocol(),
		Insecure:           insecure,
		InsecureSkipVerify: skip,
		CAFile:             c.CAFile,
		Headers:            c.Headers,
		BearerTokenFile:    c.BearerTokenFile,
		Compression:        base.Compression,
		CompressionLevel:   base.CompressionLevel,
		Timeout:            base.Timeout,
		// One attempt, never a retry. A duplicate delivery would double-count
		// the edge (the shard aggregates cumulatively and dedupes nothing), and
		// a retry occupies the shard's single worker for a payload the graph is
		// explicitly allowed to lose — holding up every batch queued behind it
		// for one that a healthy shard would already have taken. Traces exports
		// do not retry in otlpexport anyway; setting it makes that a decision
		// rather than a coincidence.
		RetryAttempts: 1,
		MaxSendBytes:  base.MaxSendBytes,
	}
}

// Forwarder routes trimmed spans to the shard owning their trace.
type Forwarder struct {
	ring    *Ring
	clients map[string]TracesExporter
	closers []func() error
	trim    *trimmer
	log     *slog.Logger

	// queues holds one bounded queue and one worker goroutine per shard; see
	// Forward for why the fan-out is asynchronous and shardQueue for the bounds.
	queues    map[string]*shardQueue
	workers   sync.WaitGroup
	stop      chan struct{} // closed by Close: workers exit
	closing   atomic.Bool   // set by Close: enqueue refuses
	closeOnce sync.Once     // Close is idempotent (a defer plus an explicit call)
	queued    atomic.Int64  // items enqueued and not yet delivered

	lastWarn atomic.Int64
	counters forwardCounters
}

type forwardCounters struct {
	spansForwarded atomic.Uint64
	spansSkipped   atomic.Uint64
	spansLost      atomic.Uint64
	spansQueueFull atomic.Uint64
	sendsFailed    atomic.Uint64
	loopsBlocked   atomic.Uint64
}

// Queue bounds. Two of them, because one item's size is not knowable in
// advance: an ingest push may be one span or a few thousand.
const (
	// forwardQueueItems caps a shard's pending payload COUNT. It bounds the
	// channel itself; the span budget below is what actually bounds memory.
	forwardQueueItems = 64
	// forwardQueueSpans caps a shard's pending SPANS. Trimmed spans are ~107
	// bytes on the wire and a few hundred in pdata, so 8k is single-digit MiB
	// per shard — with the usual handful of shards, a bound an operator can hold
	// in their head next to the ingest receiver's own 4 MiB message cap. It is
	// deliberately small: this queue is a shock absorber for a shard that is
	// briefly slow, NOT a buffer for one that is down. Durability for a failed
	// destination is -buffer-dir's job, and the graph does not want it — a
	// replayed forward would double-count an edge (the shard aggregates
	// cumulatively and dedupes nothing).
	forwardQueueSpans = 8192
)

// forwardDrainTimeout bounds Close: what is queued gets this long to land, and
// is then abandoned. Bounded because Close runs inside the agent's own shutdown
// budget, and a shard tier that is down (the reason the queue is deep at all)
// must not hold the process past the kubelet's grace period. A var only so the
// shutdown tests need not spend two real seconds proving the bound exists.
var forwardDrainTimeout = 2 * time.Second

// shardQueue is one shard's pending work: a bounded channel plus the span
// budget that bounds the memory it can hold. PER SHARD rather than shared, so a
// single black-holing shard cannot consume the whole budget and starve the
// healthy ones — its arc of the ring degrades, the rest of the graph does not.
type shardQueue struct {
	ch    chan ptrace.Traces
	spans atomic.Int64
}

// reserve claims budget for n spans, or reports that the queue is full. A batch
// larger than the whole budget is admitted when the queue is EMPTY: refusing it
// outright would wedge that shard forever the moment one oversized push arrived.
func (q *shardQueue) reserve(n int64) bool {
	for {
		cur := q.spans.Load()
		if cur > 0 && cur+n > forwardQueueSpans {
			return false
		}
		if q.spans.CompareAndSwap(cur, cur+n) {
			return true
		}
	}
}

func (q *shardQueue) release(n int64) { q.spans.Add(-n) }

// ForwardStats is a snapshot of the forwarder's counters. The wire-level
// outcome of each shard send is already counted by otlpexport
// (kubescrape_exports_total{signal="traces"}); these are the numbers only the
// forwarder knows. They are exposed as a struct rather than registered here
// because every kubescrape metric is declared in internal/obs/obs.go, from
// which docs/METRICS.md is generated — the wiring seam publishes them there,
// the way obs.RegisterBufferStats publishes the disk buffer's.
type ForwardStats struct {
	// SpansForwarded is spans handed to a shard client (delivered or not).
	SpansForwarded uint64
	// SpansSkipped is spans that can never form an edge: INTERNAL/UNSPECIFIED
	// kinds, and spans with no trace id. Expected to dominate — that is the
	// saving, not a problem.
	SpansSkipped uint64
	// SpansLost is spans in a shard send that failed. They are gone: the graph
	// is best-effort and the caller's export already succeeded.
	SpansLost uint64
	// SpansQueueFull is spans dropped WITHOUT being sent, because the owning
	// shard's forward queue was full. Distinct from SpansLost, which is a send
	// the collector refused: this one means the shard is not draining (down,
	// black-holing, or slower than this node's span rate) and the fan-out is
	// shedding rather than blocking the ingest handler. See Forward.
	SpansQueueFull uint64
	// SendsFailed is failed shard sends.
	SendsFailed uint64
	// LoopsBlocked is payloads refused because they already carried
	// ForwardedMarker — always zero in a correct deployment.
	LoopsBlocked uint64
}

// NewForwarder builds a Forwarder from cfg over the flag-built base exporter
// config. It returns (nil, nil) when the section is disabled, so a caller can
// wire it unconditionally.
func NewForwarder(cfg ForwardConfig, base otlpexport.Config, log *slog.Logger) (*Forwarder, error) {
	if !cfg.Enabled() {
		return nil, nil
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	targets, err := cfg.shardTargets()
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}
	f := &Forwarder{
		clients: make(map[string]TracesExporter, len(targets)),
		trim:    newTrimmer(cfg.Dimensions, cfg.PeerAttributes),
		log:     log,
	}
	names := make([]string, 0, len(targets))
	for _, t := range targets {
		if _, dup := f.clients[t.name]; dup {
			continue // a repeated endpoint is one shard, not two
		}
		c, err := otlpexport.New(cfg.clientConfig(t, base))
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("service-graph shard %s (%s): %w", t.name, t.endpoint, err)
		}
		f.clients[t.name] = c
		f.closers = append(f.closers, c.Close)
		names = append(names, t.name)
	}
	f.ring = NewRing(names, cfg.TokensPerShard)
	f.startWorkers()
	return f, nil
}

// NewForwarderWithClients builds a Forwarder over caller-supplied per-shard
// exporters, keyed by shard name. It is the seam the tests use instead of real
// OTLP clients — and the only way to construct a Forwarder without opening a
// connection.
func NewForwarderWithClients(clients map[string]TracesExporter, tokensPerShard int, dims, peers []string, log *slog.Logger) *Forwarder {
	if log == nil {
		log = slog.Default()
	}
	names := make([]string, 0, len(clients))
	for name := range clients {
		names = append(names, name)
	}
	f := &Forwarder{
		ring:    NewRing(names, tokensPerShard),
		clients: clients,
		trim:    newTrimmer(dims, peers),
		log:     log,
	}
	f.startWorkers()
	return f
}

// startWorkers gives every shard its queue and its draining goroutine. Called
// once, from both constructors: a Forwarder is useless without them, and
// starting them lazily on the first Forward would need a lock on the hot path.
func (f *Forwarder) startWorkers() {
	f.stop = make(chan struct{})
	f.queues = make(map[string]*shardQueue, len(f.clients))
	for name := range f.clients {
		q := &shardQueue{ch: make(chan ptrace.Traces, forwardQueueItems)}
		f.queues[name] = q
		f.workers.Add(1)
		go f.work(name, q)
	}
}

// Ring exposes the shard ring (for a startup log line, or a /debug handler).
func (f *Forwarder) Ring() *Ring { return f.ring }

// Stats snapshots the counters.
func (f *Forwarder) Stats() ForwardStats {
	return ForwardStats{
		SpansForwarded: f.counters.spansForwarded.Load(),
		SpansSkipped:   f.counters.spansSkipped.Load(),
		SpansLost:      f.counters.spansLost.Load(),
		SpansQueueFull: f.counters.spansQueueFull.Load(),
		SendsFailed:    f.counters.sendsFailed.Load(),
		LoopsBlocked:   f.counters.loopsBlocked.Load(),
	}
}

// Close stops the fan-out and shuts every shard client down.
//
// BOUNDED, in the agent's own style: enqueueing stops immediately, whatever is
// already queued gets forwardDrainTimeout to land, and then the workers are
// told to stop whether or not it did. A shard tier that is down is exactly the
// state in which the queue is deep, so an unbounded drain here would hold the
// process past the kubelet's grace period and lose the shutdown's other final
// exports to SIGKILL — for edges that are best-effort by design.
func (f *Forwarder) Close() error {
	f.closeOnce.Do(func() {
		if f.stop == nil {
			return // built but never started: NewForwarder's own failure path
		}
		f.closing.Store(true) // enqueue refuses from here on
		if !f.awaitIdle(forwardDrainTimeout) {
			f.log.Warn("service-graph forwards were still queued at shutdown; abandoning them",
				"budget", forwardDrainTimeout, "pending", f.queued.Load())
		}
		close(f.stop)
		// The workers exit between items, but one already INSIDE a send only
		// returns when the exporter's own Timeout does — which may be far longer
		// than the budget above. So this wait is bounded too: past it we fall
		// through and close the clients, which aborts the in-flight send and
		// lets the worker exit on its own. Nothing leaks either way; the total
		// is at most two drain windows.
		done := make(chan struct{})
		go func() { defer close(done); f.workers.Wait() }()
		select {
		case <-done:
		case <-time.After(forwardDrainTimeout):
			f.log.Warn("a service-graph shard send was still in flight at shutdown; closing under it",
				"budget", forwardDrainTimeout)
		}
	})
	var first error
	for _, c := range f.closers {
		if err := c(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Tap returns a TracesExporter that forwards each batch to inner and then
// distributes its edge-forming spans to the shards. It mirrors
// spanmetrics.Generator.Tap deliberately: same seam, same ordering rule, so
// the two stack in either order without either one changing what the sender
// sees.
func (f *Forwarder) Tap(inner TracesExporter) TracesExporter {
	if f == nil {
		return inner // the feature is off: no wrapper, no cost
	}
	return &forwardTap{f: f, inner: inner}
}

type forwardTap struct {
	f     *Forwarder
	inner TracesExporter
}

func (t *forwardTap) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	// Forward the ORIGINAL payload first and act only on success, exactly as
	// spanmetrics' tap does. Two reasons, and the second is the one specific to
	// this tap:
	//
	//   - A failed inner export is retried by the sender, and a graph hop that
	//     already happened would then be repeated: the shard's edge counters
	//     are cumulative and dedupe nothing, so every back-pressure window
	//     would inflate the graph.
	//   - The graph must never cost a span. Doing the shard fan-out first would
	//     put its latency (and, if it could fail the export, its availability)
	//     in front of the payload that actually matters.
	//
	// The payload handed to inner is the caller's, untouched: Trim builds a
	// copy and nothing here mutates td.
	if err := t.inner.ExportTraces(ctx, td); err != nil {
		return err
	}
	t.f.Forward(ctx, td)
	return nil
}

// Forward trims td, hashes each span onto the ring and HANDS each shard's share
// to that shard's queue. It never returns an error and — since the queue hand-off
// is non-blocking — never blocks: losing an edge must never cost a span, and by
// this point the caller's export has already succeeded, so there is nothing left
// to fail. Failures are counted (ForwardStats.SendsFailed / SpansLost /
// SpansQueueFull) and logged, throttled.
//
// # Why the send is asynchronous
//
// This runs inside the OTLP ingest handler, which holds one of only
// -ingest-max-in-flight slots (32 by default). A synchronous fan-out — what
// this used to be — parks that slot for as long as a shard takes to answer, up
// to the exporter's whole timeout. A shard that black-holes traffic is the
// EXPECTED failure, not an exotic one: the chart's headless Service sets
// publishNotReadyAddresses: true (the ring needs stable names before a pod is
// ready), so DNS resolves to shards that are still starting, and a rolling
// update of the tier points every agent at a pod that accepts the connection
// and answers nothing. The whole node's pushed LOGS and METRICS would then shed
// with 429 because the GRAPH's destination is slow — the exact opposite of this
// package's stated contract, that losing an edge must never cost a span.
//
// It is safe to be asynchronous because the graph is best-effort by
// construction: the payload here is a trimmed COPY (Trim never aliases or
// mutates the caller's), the shard's edge counters are cumulative and
// at-least-once-tolerant, and nothing downstream waits on the result — the
// caller's own export already succeeded and its ack does not depend on this.
//
// What it costs: the queue is bounded, so a shard that stops draining sheds
// spans (counted, SpansQueueFull) instead of applying back-pressure — which is
// what we want, an edge is worth less than an ingest slot — and a payload still
// in a queue when the process exits is lost if Close's drain window elapses
// first. Both are edges missing from the graph, which is what every other bound
// in this feature also trades away, and both are counted rather than silent.
func (f *Forwarder) Forward(_ context.Context, td ptrace.Traces) {
	if f == nil || len(f.clients) == 0 {
		return
	}
	// The trim itself stays on the caller's goroutine: it is pure CPU, it is
	// what makes the queued payload small (~107 bytes/span), and doing it here
	// means the queue holds trimmed copies rather than references into the
	// sender's whole batch.
	groups := f.trim.split(td, f.ring, &f.counters)
	for name, g := range groups {
		f.enqueue(name, g)
	}
}

// enqueue hands one shard's share to its worker, or drops it. Never blocks.
//
// The context is deliberately NOT the caller's: an ingest RPC's context is
// cancelled the moment the handler returns, which is now BEFORE the send runs,
// so passing it down would cancel every forward. The send's own bound is the
// exporter's Timeout (inherited from the flag-built base in clientConfig).
func (f *Forwarder) enqueue(shard string, td ptrace.Traces) {
	n := int64(td.SpanCount())
	q, ok := f.queues[shard]
	if !ok { // unreachable: the ring is built from the client map's keys
		f.counters.spansLost.Add(uint64(n))
		return
	}
	if f.closing.Load() || !q.reserve(n) {
		f.dropQueued(shard, n, "the shard's forward queue is full")
		return
	}
	f.queued.Add(1)
	select {
	case q.ch <- td:
	default:
		// The item bound rather than the span bound: release the reservation and
		// account for it identically.
		q.release(n)
		f.queued.Add(-1)
		f.dropQueued(shard, n, "the shard's forward queue is full")
	}
}

func (f *Forwarder) dropQueued(shard string, n int64, why string) {
	f.counters.spansQueueFull.Add(uint64(n))
	f.warn("dropping spans bound for a service-graph shard: "+why,
		"shard", shard, "spans", n)
}

// work drains one shard's queue. One goroutine per shard, started at
// construction and stopped by Close — never one per batch, which is what makes
// the memory bound above a bound rather than a hope.
func (f *Forwarder) work(shard string, q *shardQueue) {
	defer f.workers.Done()
	for {
		select {
		case <-f.stop:
			return
		case td := <-q.ch:
			f.sendOne(shard, q, td)
		}
	}
}

func (f *Forwarder) sendOne(shard string, q *shardQueue, td ptrace.Traces) {
	n := uint64(td.SpanCount())
	defer func() {
		q.release(int64(n))
		f.queued.Add(-1)
	}()
	client, ok := f.clients[shard]
	if !ok { // unreachable: the ring is built from the client map's keys
		f.counters.spansLost.Add(n)
		return
	}
	f.counters.spansForwarded.Add(n)
	// A detached context (see enqueue): the exporter applies its own Timeout.
	if err := client.ExportTraces(context.Background(), td); err != nil {
		f.counters.sendsFailed.Add(1)
		f.counters.spansLost.Add(n)
		f.warn("forwarding spans to a service-graph shard failed", "shard", shard, "spans", n, "error", err)
	}
}

// awaitIdle waits for every queued payload to be delivered (or fail), up to d.
// It is the drain in Close and the synchronisation point the tests use; nothing
// on the hot path calls it.
func (f *Forwarder) awaitIdle(d time.Duration) bool {
	deadline := time.Now().Add(d)
	for f.queued.Load() > 0 {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
	return true
}

// warn logs at most once per forwardWarnEvery across all shards.
func (f *Forwarder) warn(msg string, args ...any) {
	now := time.Now().UnixNano()
	last := f.lastWarn.Load()
	if now-last < int64(forwardWarnEvery) || !f.lastWarn.CompareAndSwap(last, now) {
		return
	}
	f.log.Warn(msg, args...)
}

// --- trimming ---

// trimmer holds the attribute allow-lists a trim applies.
type trimmer struct {
	res  map[string]bool
	span map[string]bool
	// dimsClaim and peersClaim are the encoded lists stamped on every forwarded
	// resource (see ForwardedDimensions), built once here so the per-resource
	// cost is a map write rather than a sort and a join.
	dimsClaim, peersClaim string
}

func newTrimmer(dims, peers []string) *trimmer {
	if peers == nil {
		peers = DefaultPeerAttributes()
	}
	t := &trimmer{
		res:        make(map[string]bool, len(keepResourceAttrs)+len(dims)),
		span:       make(map[string]bool, len(keepSpanAttrs)+len(dims)+len(peers)),
		dimsClaim:  encodeAttrList(dims),
		peersClaim: encodeAttrList(peers),
	}
	for k := range keepResourceAttrs {
		t.res[k] = true
	}
	for k := range keepSpanAttrs {
		t.span[k] = true
	}
	// A dimension resolves span-then-resource (Tempo's rule), so it must
	// survive on BOTH: keeping it only on the span would blank every dimension
	// carried by a resource — the common case for k8s/deployment metadata.
	for _, d := range dims {
		t.res[d] = true
		t.span[d] = true
	}
	for _, p := range peers {
		t.span[p] = true
	}
	return t
}

// Trim returns a COPY of td carrying only what edge pairing needs: the
// edge-forming spans' ids, kind, status code and timestamps, the resource's
// service and k8s identity, and the attributes that name a peer or decide a
// connection type. Everything else — span names, events, links, log-heavy
// attributes, scope identity, the whole enriched resource — is dropped.
//
// This is the feature's wire cost, paid per span on the node's network, so it
// is worth knowing what it buys. Measured by TestTrimByteCost on realistic
// instrumented HTTP spans (15 span attributes, 2 events with a stack trace, a
// status message, over a 15-attribute enriched resource): 1002 bytes/span
// becomes 107 bytes/span, an 89% cut. Dropping INTERNAL spans then removes 3
// of every 5 spans outright, so a batch shrinks by 96% end to end — the graph
// hop costs a few percent of what the span itself already costs to ship to the
// collector.
//
// It never mutates td.
func Trim(td ptrace.Traces) ptrace.Traces {
	out := defaultTrimmer.split(td, nil, nil)
	if g, ok := out[""]; ok {
		return g
	}
	return ptrace.NewTraces()
}

var defaultTrimmer = newTrimmer(nil, nil)

// split trims td and groups the result by owning shard. A nil ring puts
// everything under the "" key (what Trim wants); st may be nil.
func (t *trimmer) split(td ptrace.Traces, ring *Ring, st *forwardCounters) map[string]ptrace.Traces {
	out := make(map[string]ptrace.Traces, 4)
	// cur maps a shard to the ScopeSpans it is currently filling for the
	// resource being walked. Reset per resource, so each shard gets one
	// ResourceSpans per source resource — the grouping the collector expects,
	// and the reason a resource's attributes are copied once rather than per
	// span.
	cur := make(map[string]ptrace.ScopeSpans, 4)
	// Skips are tallied locally and added once: split runs on every ingest RPC
	// and an atomic add per dropped INTERNAL span would be the hottest line in
	// the feature.
	var skipped uint64

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		if _, marked := rs.Resource().Attributes().Get(ForwardedMarker); marked {
			// Already a forwarded payload: refuse rather than amplify. See
			// ForwardedMarker.
			if st != nil {
				st.loopsBlocked.Add(1)
				st.spansSkipped.Add(uint64(resourceSpanCount(rs)))
			}
			continue
		}
		clear(cur)
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				// INTERNAL and UNSPECIFIED spans can never be half of an edge —
				// an edge is a call BETWEEN processes — and they are the bulk of
				// an instrumented service's volume. Dropping them here is most
				// of what makes the hop affordable.
				if !edgeKind(span.Kind()) {
					skipped++
					continue
				}
				tid := span.TraceID()
				if tid.IsEmpty() {
					// Unpairable by construction (there is no trace to pair
					// within) AND a hot spot: every zero id hashes to the same
					// token, so forwarding them would pile the whole cluster's
					// malformed spans onto one shard.
					skipped++
					continue
				}
				shard := ""
				if ring != nil {
					if shard = ring.Owner(tid[:]); shard == "" {
						skipped++ // empty ring; the caller forwards nothing
						continue
					}
				}
				ss, ok := cur[shard]
				if !ok {
					g, ok := out[shard]
					if !ok {
						g = ptrace.NewTraces()
						out[shard] = g
					}
					nrs := g.ResourceSpans().AppendEmpty()
					t.copyResource(rs.Resource(), nrs.Resource())
					// One anonymous ScopeSpans per resource: the scope names the
					// instrumentation library, which no edge depends on, and
					// preserving it would multiply the resource-level framing by
					// the number of libraries in the process.
					ss = nrs.ScopeSpans().AppendEmpty()
					cur[shard] = ss
				}
				t.copySpan(span, ss.Spans().AppendEmpty())
			}
		}
	}
	if st != nil && skipped > 0 {
		st.spansSkipped.Add(skipped)
	}
	return out
}

// copyResource copies the allow-listed resource attributes and stamps the
// loop marker.
func (t *trimmer) copyResource(src pcommon.Resource, dst pcommon.Resource) {
	attrs := dst.Attributes()
	// Size the map once instead of letting it double its way up: the allow-list
	// length is a tight upper bound, and this runs per resource per shard.
	attrs.EnsureCapacity(min(len(t.res), src.Attributes().Len()) + 3)
	src.Attributes().Range(func(k string, v pcommon.Value) bool {
		if t.res[k] {
			v.CopyTo(attrs.PutEmpty(k))
		}
		return true
	})
	attrs.PutBool(ForwardedMarker, true)
	// What this agent kept, so the shard can tell whether it matches what it
	// reads — see ForwardedDimensions.
	attrs.PutStr(ForwardedDimensions, t.dimsClaim)
	attrs.PutStr(ForwardedPeerAttributes, t.peersClaim)
}

// copySpan copies exactly the fields pairing reads.
func (t *trimmer) copySpan(src ptrace.Span, dst ptrace.Span) {
	dst.SetTraceID(src.TraceID())
	// The span id and the parent span id ARE the pairing: a client span's span
	// id is its server span's parent span id.
	dst.SetSpanID(src.SpanID())
	dst.SetParentSpanID(src.ParentSpanID())
	dst.SetKind(src.Kind())
	dst.SetStartTimestamp(src.StartTimestamp())
	dst.SetEndTimestamp(src.EndTimestamp())
	// The code only; the message is free text of unbounded size and an edge
	// records failed-or-not, never why.
	dst.Status().SetCode(src.Status().Code())
	srcAttrs := src.Attributes()
	attrs := dst.Attributes()
	attrs.EnsureCapacity(min(len(t.span), srcAttrs.Len()))
	srcAttrs.Range(func(k string, v pcommon.Value) bool {
		if t.span[k] {
			v.CopyTo(attrs.PutEmpty(k))
		}
		return true
	})
}

// edgeKind reports whether a span can be half of an edge.
func edgeKind(k ptrace.SpanKind) bool {
	switch k {
	case ptrace.SpanKindClient, ptrace.SpanKindServer, ptrace.SpanKindProducer, ptrace.SpanKindConsumer:
		return true
	}
	return false
}

func resourceSpanCount(rs ptrace.ResourceSpans) int {
	n := 0
	sss := rs.ScopeSpans()
	for i := 0; i < sss.Len(); i++ {
		n += sss.At(i).Spans().Len()
	}
	return n
}
