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

const (
	defaultShardPort     = 4317
	defaultShardProtocol = "grpc"
	// forwardWarnEvery throttles the "a shard send failed" log. The failure is
	// per INGEST RPC, so a shard that is down would otherwise write one line
	// per pushed batch — thousands a second on a busy node, drowning the log
	// the operator needs to diagnose it. The counters are the real signal; the
	// line exists to name the endpoint and the error once in a while.
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
// virtualNodePeerAttributes (there is no channel between them), and an
// operator who tunes the shard's list and forgets the agent's would otherwise
// trim away exactly the attributes the shard is looking for: every database
// and messaging call would silently render as a plain service-to-service edge,
// or as no edge at all where a virtual node was the only thing naming the far
// side. A wrong graph is worse than a missing one, and four attribute keys are
// a cheap floor.
var keepSpanAttrs = map[string]bool{
	"messaging.system": true,
	"db.system":        true,
	"db.name":          true,
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
	// Port is the shards' OTLP port (default 4317).
	Port int `json:"port,omitempty"`
	// Endpoints names the shards explicitly, bypassing the template.
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
	// Insecure sends gRPC in plaintext. nil defaults to "plaintext unless a CA
	// is configured": pod-to-pod inside the cluster, where the bearer token is
	// the authenticator. Set caFile (or run the hop through a mesh) when the
	// pod network is not trusted — otherwise the token crosses it in the clear.
	Insecure *bool `json:"insecure,omitempty"`
	// InsecureSkipVerify disables certificate verification on the shard hop.
	InsecureSkipVerify *bool `json:"insecureSkipVerify,omitempty"`
	// CAFile verifies the shard receiver's certificate.
	CAFile string `json:"caFile,omitempty"`
	// Headers are static headers on every shard send.
	Headers map[string]string `json:"headers,omitempty"`

	// Dimensions must MATCH the shard's serviceGraph.dimensions: a dimension
	// the agent trims away is a label the shard can only ever render empty.
	// Kept here (rather than read from the shard) because the agent has no
	// channel to the shard's config, and guessing would mean shipping every
	// attribute — the thing Trim exists to avoid.
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
		port = defaultShardPort
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
			if c.CAFile != "" {
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
	insecure := c.CAFile == "" // TLS material plus plaintext is refused by otlpexport.New
	if c.Insecure != nil {
		insecure = *c.Insecure
	}
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
		// One attempt, never a retry: a retried graph hop costs latency on the
		// ingest handler for a payload the graph is explicitly allowed to lose,
		// and a duplicate delivery would double-count the edge (the shard
		// aggregates, it does not dedupe). Traces exports do not retry in
		// otlpexport anyway; setting it makes that a decision rather than a
		// coincidence.
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

	lastWarn atomic.Int64
	counters forwardCounters
}

type forwardCounters struct {
	spansForwarded atomic.Uint64
	spansSkipped   atomic.Uint64
	spansLost      atomic.Uint64
	sendsFailed    atomic.Uint64
	loopsBlocked   atomic.Uint64
}

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
	return &Forwarder{
		ring:    NewRing(names, tokensPerShard),
		clients: clients,
		trim:    newTrimmer(dims, peers),
		log:     log,
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
		SendsFailed:    f.counters.sendsFailed.Load(),
		LoopsBlocked:   f.counters.loopsBlocked.Load(),
	}
}

// Close shuts every shard client down.
func (f *Forwarder) Close() error {
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

// Forward trims td and sends each shard's share. It never returns an error:
// losing an edge must never cost a span, and by this point the caller's export
// has already succeeded, so there is nothing left to fail. Failures are
// counted (ForwardStats.SendsFailed / SpansLost) and logged, throttled.
func (f *Forwarder) Forward(ctx context.Context, td ptrace.Traces) {
	if f == nil || len(f.clients) == 0 {
		return
	}
	groups := f.trim.split(td, f.ring, &f.counters)
	if len(groups) == 0 {
		return
	}
	if len(groups) == 1 {
		for name, g := range groups {
			f.sendOne(ctx, name, g)
		}
		return
	}
	// Fan out concurrently. The bound is the shard count (single digits), and
	// the alternative — sequential sends — would add every shard's round trip
	// to the ingest handler's latency in series, which is what a sender
	// notices. Nothing is shared between the goroutines: each group is its own
	// payload and each counter is atomic.
	var wg sync.WaitGroup
	for name, g := range groups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.sendOne(ctx, name, g)
		}()
	}
	wg.Wait()
}

func (f *Forwarder) sendOne(ctx context.Context, shard string, td ptrace.Traces) {
	n := uint64(td.SpanCount())
	client, ok := f.clients[shard]
	if !ok { // unreachable: the ring is built from the client map's keys
		f.counters.spansLost.Add(n)
		return
	}
	f.counters.spansForwarded.Add(n)
	if err := client.ExportTraces(ctx, td); err != nil {
		f.counters.sendsFailed.Add(1)
		f.counters.spansLost.Add(n)
		f.warn("forwarding spans to a service-graph shard failed", "shard", shard, "spans", n, "error", err)
	}
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
}

func newTrimmer(dims, peers []string) *trimmer {
	if peers == nil {
		peers = DefaultPeerAttributes()
	}
	t := &trimmer{
		res:  make(map[string]bool, len(keepResourceAttrs)+len(dims)),
		span: make(map[string]bool, len(keepSpanAttrs)+len(dims)+len(peers)),
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
	attrs.EnsureCapacity(min(len(t.res), src.Attributes().Len()) + 1)
	src.Attributes().Range(func(k string, v pcommon.Value) bool {
		if t.res[k] {
			v.CopyTo(attrs.PutEmpty(k))
		}
		return true
	})
	attrs.PutBool(ForwardedMarker, true)
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
