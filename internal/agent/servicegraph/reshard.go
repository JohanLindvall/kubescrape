package servicegraph

// Re-sharding: the tier's own internal hop.
//
// Applications push their traces to the shard tier's Service, which
// round-robins — so a trace's client half and its server half routinely land on
// two DIFFERENT shards, which is exactly the split that makes pairing
// impossible. The shard that RECEIVES a push therefore hashes every span by
// trace id onto the ring (ring.go) and forwards each span to the shard that
// OWNS its trace; the owner then holds every span of that trace and can pair
// it, derive its span metrics and export it.
//
// The hop is SYNCHRONOUS and FAILABLE, which is the one thing to understand
// about this file. The entry shard holds the ONLY copy of a pushed span, so a
// dropped hop is a lost span, not a lost edge: there is no shedding here, no
// queue and no best-effort. A failed send fails the application's push and the
// sender's own retry is the recovery — the same contract every other receive
// path in this repo honours.
//
// Nothing is trimmed either: the payload that crosses is the payload the
// collector will get, so it keeps its resource, its scope identity and every
// span field.

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"

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

// ForwardedMarker is stamped as a boolean resource attribute on every payload a
// Resharder sends to another shard, and it means exactly one thing: THIS
// PAYLOAD HAS ALREADY BEEN THROUGH THE TIER'S ENTRY PATH. It has been enriched
// and it has been sharded, and it must be neither again.
//
// # Loop prevention
//
// The primary defence is the PORT a payload arrives on, not an attribute.
// Application pushes land on the tier's ingest listeners
// (-service-graph-ingest-grpc / -http) and are always re-sharded exactly once;
// internal hops land on the authenticated receiver (-service-graph-listen) and
// are never re-sharded at all — the internal path has no Resharder in it.
// Loop-freedom is a property of the topology (one directed edge, ingest port ->
// internal port, and no edge out of the internal port), not a runtime check that
// could be wrong.
//
// A port cannot be forged by a payload, which is why the decision hangs on one.
// The marker is the second line, and it earns its one attribute per resource
// because of a specific, easy misconfiguration: pointing the reshard endpoint at
// the tier's own INGEST port instead of its internal one — two OTLP receivers on
// one workload, an obvious copy-paste. Without the marker that mistake is
// unbounded amplification: every span re-enters the entry path, is re-enriched
// against a peer that is now a kubescrape pod, and is re-sharded again, on every
// hop, until the cluster's network is the incident. With it, the ingest path
// REFUSES a marked payload outright — permanently, so the sender does not retry
// it — and ReshardStats.LoopsBlocked says exactly what is wrong.
//
// The owner STRIPS it before anything else runs (StripForwarded): it is a
// transport detail of this tier, and application telemetry must not reach the
// collector carrying kubescrape's internal plumbing as a resource attribute.
const ForwardedMarker = "kubescrape.service_graph.forwarded"

// DefaultShardPort is the port a shard target takes when the config names none.
//
// It MUST equal the internal receiver's own default listen address
// (cmd/kubescrape-agent's -service-graph-listen, :4319), and the two are pinned
// against each other by a test there. It is deliberately NOT 4317/4318 — those
// are the tier's APPLICATION-facing ingest ports, and a reshard hop addressed
// there is the amplification loop ForwardedMarker exists to stop.
const DefaultShardPort = 4319

const defaultShardProtocol = "grpc"

// reshardWarnEvery throttles the failed-hop warning. The failure is already
// reported to the sender (the push fails) and counted; the log line exists to
// name the shard and the error now and then, not once per rejected batch.
const reshardWarnEvery = 30 * time.Second

// ReshardConfig is the `serviceGraphShards` section: which shards exist, which
// one this process is, and how to reach the others.
//
// # Why a StatefulSet template rather than a list of endpoints
//
// Both are offered, but the template is the intended form and the one the chart
// renders. The ring is only useful if EVERY shard computes the identical one:
// two shards disagreeing about the shard set send the two halves of a request to
// two different owners, and the edge simply never forms — silently, with both
// sides reporting success. A template makes disagreement take a visible form (a
// different replica count) instead of an invisible one (a list with one entry
// misspelled or stale by one). It is also the only form that matches how the
// tier scales: the replica count is one number, and the shard names are then
// derivable rather than transcribable.
//
// Endpoints exists as the escape hatch: a shard tier outside Kubernetes, a test,
// or an operator who wants to name pods explicitly. When set it wins, and the
// shard NAME is the endpoint string itself.
type ReshardConfig struct {
	// Self is THIS shard's name in the ring — the pod's own name for the
	// template form ($POD_NAME, which is <statefulSet>-<ordinal>), or the
	// matching entry for the explicit form.
	//
	// It is what makes the common case free: a span this shard already owns is
	// handled in-process, with no second network hop and no second decode. A
	// Self naming nothing in the ring is not an error — every share is then
	// remote, including the one this pod owns, so it sends to itself through the
	// internal receiver and everything still works — but it doubles the tier's
	// internal traffic, so it is warned about loudly at startup.
	Self string `json:"self,omitempty"`

	// StatefulSet is the tier's StatefulSet name. Its pods are the shards, named
	// <statefulSet>-0 .. <statefulSet>-<replicas-1> — the ordinal identity that
	// makes the ring's tokens derivable (see Ring).
	StatefulSet string `json:"statefulSet,omitempty"`
	// Replicas is the shard count. It must match the StatefulSet's spec: a
	// larger value addresses pods that do not exist (pushes whose traces hash
	// there fail), a smaller one leaves the tail shards idle.
	Replicas int `json:"replicas,omitempty"`
	// Service is the governing headless Service. Empty defaults to StatefulSet,
	// the usual convention.
	Service string `json:"service,omitempty"`
	// Namespace holds the shard tier. Empty resolves to this pod's own namespace
	// ($POD_NAMESPACE or the ServiceAccount projection).
	Namespace string `json:"namespace,omitempty"`
	// Port is the shards' INTERNAL receiver port (default DefaultShardPort).
	Port int `json:"port,omitempty"`
	// Endpoints names the shards explicitly, bypassing the template. With
	// protocol: http these must carry their own http:// or https:// scheme.
	Endpoints []string `json:"endpoints,omitempty"`

	// TokensPerShard is the ring's virtual tokens per shard (0 =
	// DefaultTokensPerShard). It MUST be identical across shards — it is part of
	// the ring's definition, not a local tuning knob.
	TokensPerShard int `json:"tokensPerShard,omitempty"`

	// BearerTokenFile authenticates the internal hop. The receiver is
	// cluster-reachable OTLP, so the hop is authenticated exactly like the
	// metadata service's /v1/scrape-auth: a shared token in a mounted file,
	// re-read once a minute (otlpexport.Config.BearerTokenFile), which is the
	// repo's existing rotation contract.
	BearerTokenFile string `json:"bearerTokenFile,omitempty"`
	// Protocol is "grpc" (default) or "http".
	Protocol string `json:"protocol,omitempty"`
	// Insecure sends gRPC in plaintext. nil defaults to "plaintext unless TLS is
	// asked for" — a caFile or insecureSkipVerify: pod-to-pod inside the
	// cluster, where the bearer token is the authenticator. With protocol: http
	// this decides the derived endpoint's SCHEME instead (see httpTLS).
	Insecure *bool `json:"insecure,omitempty"`
	// InsecureSkipVerify disables certificate verification on the hop — and,
	// like CAFile, implies TLS.
	InsecureSkipVerify *bool `json:"insecureSkipVerify,omitempty"`
	// CAFile verifies the receiver's certificate.
	CAFile string `json:"caFile,omitempty"`
	// Headers are static headers on every internal send.
	Headers map[string]string `json:"headers,omitempty"`
}

// Enabled reports whether re-sharding is configured at all. A single-shard tier
// is legitimately DISABLED here: with one shard every trace is already local, so
// there is nothing to hash and nothing to send.
func (c *ReshardConfig) Enabled() bool {
	if c == nil {
		return false
	}
	return len(c.Endpoints) > 0 || (c.StatefulSet != "" && c.Replicas > 0)
}

// Validate checks the section's SHAPE only — no filesystem, no DNS, no namespace
// resolution — so -check-config stays a pure dry run.
func (c *ReshardConfig) Validate() error {
	if c == nil || !c.Enabled() {
		// A half-filled template is the one "disabled" state worth refusing: it
		// reads as configured and shards nothing, which is indistinguishable
		// from a deliberately single-shard tier.
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

// shardTargets resolves the configured shard set to (name, endpoint) pairs. The
// NAME is what the ring hashes, so it must be stable across shards and across
// restarts: the pod's ordinal identity for the template form, the endpoint
// string itself for the explicit form.
func (c *ReshardConfig) shardTargets() ([]shardTarget, error) {
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
		return nil, fmt.Errorf("serviceGraphShards.namespace is empty and this pod's own namespace could not be resolved ($POD_NAMESPACE or the ServiceAccount projection)")
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
		// Service name: that round-robins, which is the very thing this hop
		// exists to undo.
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

func (c *ReshardConfig) protocol() string {
	if c.Protocol == "" {
		return defaultShardProtocol
	}
	return c.Protocol
}

// httpTLS decides the SCHEME of a template-derived http-protocol endpoint.
//
// otlpexport ignores Config.Insecure for HTTP — there the scheme IS the
// decision — so the TLS intent expressed in this section has to be translated
// into one. All three ways of expressing it count: an explicit `insecure` (which
// wins, so `insecure: true` beside a caFile produces http:// and otlpexport then
// refuses the contradiction loudly rather than quietly handshaking), a caFile,
// and `insecureSkipVerify`.
//
// Explicit `endpoints` are exempt: there the operator writes the scheme.
func (c *ReshardConfig) httpTLS() bool {
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
// It inherits the base's TRANSPORT tuning (otlpexport.Config.TransportOnly:
// timeout, compression, send-size cap) but never its CREDENTIALS or
// destination: bearer token, headers, TLS material and endpoint are all
// destination-scoped, and inheriting them would present the collector's token
// to a sibling shard — a credential leak across a trust boundary caused by
// leaving a field empty. TransportOnly owns that partition (and
// otlpexport.TestConfigFieldsAreClassified forces every new Config field onto
// one side of it); this function only fills in the destination and pins the
// retry shape.
func (c *ReshardConfig) clientConfig(t shardTarget, base otlpexport.Config) otlpexport.Config {
	// The same TLS intent the http scheme is derived from, so the two transports
	// cannot disagree (TLS material plus plaintext is refused by otlpexport.New).
	insecure := !c.httpTLS()
	skip := false
	if c.InsecureSkipVerify != nil {
		skip = *c.InsecureSkipVerify
	}
	cfg := base.TransportOnly()
	cfg.Endpoint = t.endpoint
	cfg.Protocol = c.protocol()
	cfg.Insecure = insecure
	cfg.InsecureSkipVerify = skip
	cfg.CAFile = c.CAFile
	cfg.Headers = c.Headers
	cfg.BearerTokenFile = c.BearerTokenFile
	// One attempt, never a retry, and that is a correctness rule rather than
	// a preference: the APPLICATION owns the retry. Retrying here would hold
	// the entry shard's in-flight slot for a multiple of the OTLP timeout
	// while the sender — which still holds the payload — waits, and a hop
	// that half-succeeded and then succeeded on the retry would double-count
	// the owner's cumulative edge and RED counters. Fail fast, tell the
	// sender, let it re-push. (The backoff is zeroed with it: with one attempt
	// there is nothing to back off between, and inheriting the base's value
	// would make the derived config claim a retry shape it does not have.)
	cfg.RetryAttempts = 1
	cfg.RetryBackoff = 0
	return cfg
}

// Resharder routes spans to the shard owning their trace.
//
// It holds no queue and starts no goroutine: Reshard runs entirely on the
// calling handler's goroutine, so the bound on concurrent internal hops is the
// entry listener's in-flight semaphore (-ingest-max-in-flight) and the memory
// bound is that semaphore times one payload. That is deliberate — see the file
// header on why the hop cannot shed.
type Resharder struct {
	ring    *Ring
	self    string
	clients map[string]TracesExporter
	closers []func() error
	log     *slog.Logger

	warnGate logdedupe.Throttle
	counters reshardCounters
}

type reshardCounters struct {
	spansForwarded atomic.Uint64
	spansLocal     atomic.Uint64
	spansUnkeyed   atomic.Uint64
	sendsFailed    atomic.Uint64
	loopsBlocked   atomic.Uint64
}

// disabledCounters is the counter block of the NIL Resharder — the (nil, nil)
// NewResharder returns for a disabled section, which is not an edge case but the
// single-shard tier: `-service-graph` with no serviceGraphShards, the simplest
// way to run the tier and the one every chart-less deployment runs. Every method
// here is nil-receiver safe because of it, and one that answered a nil receiver
// by dropping its tally froze all five kubescrape_service_graph_* series at zero
// for exactly that topology.
//
// Package-level is exact rather than a compromise: "no resharder" is ONE thing
// per process (the wiring builds at most one), so unlike instance state moved
// out of globals elsewhere in this repo there is nothing here for two owners to
// merge.
var disabledCounters reshardCounters

// tally is the block to bump. See disabledCounters for why nil has one.
func (r *Resharder) tally() *reshardCounters {
	if r == nil {
		return &disabledCounters
	}
	return &r.counters
}

// ReshardStats is a snapshot of the resharder's counters. The wire-level outcome
// of each hop is already counted by otlpexport
// (kubescrape_export_requests_total{signal="traces"}); these are the numbers only
// the resharder knows. They are exposed as a struct rather than registered here
// because every kubescrape metric is declared in internal/obs/obs.go, from which
// docs/METRICS.md is generated.
type ReshardStats struct {
	// SpansForwarded is spans handed to ANOTHER shard and accepted by it.
	SpansForwarded uint64
	// SpansLocal is spans this shard already owned, kept in-process.
	SpansLocal uint64
	// SpansUnkeyed is spans with no trace id. They cannot be hashed, so they are
	// kept locally — they can never pair, and piling every zero id onto the one
	// shard that owns the zero token would be a hot spot for no gain.
	SpansUnkeyed uint64
	// SendsFailed is failed internal sends (one per shard per batch). The
	// application's push fails with them.
	SendsFailed uint64
	// LoopsBlocked is SPANS refused off application pushes carrying
	// ForwardedMarker (CountLoopBlocked tallies the push's span count) —
	// always zero in a correct deployment.
	LoopsBlocked uint64
}

// NewResharder builds a Resharder from cfg over the flag-built base exporter
// config. It returns (nil, nil) when the section is disabled (a single-shard
// tier), so a caller can wire it unconditionally — a nil *Resharder keeps every
// span local, which is exactly right for one shard.
func NewResharder(cfg ReshardConfig, base otlpexport.Config, log *slog.Logger) (*Resharder, error) {
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
	r := &Resharder{
		self:    cfg.Self,
		clients: make(map[string]TracesExporter, len(targets)),
		log:     log,
	}
	names := make([]string, 0, len(targets))
	seen := make(map[string]bool, len(targets))
	for _, t := range targets {
		if seen[t.name] {
			continue // a repeated endpoint is one shard, not two
		}
		seen[t.name] = true
		names = append(names, t.name)
		if t.name == cfg.Self {
			// No client for ourselves: our share never leaves the process. This
			// is also what makes a one-shard tier cost nothing.
			continue
		}
		c, err := otlpexport.New(cfg.clientConfig(t, base))
		if err != nil {
			_ = r.Close()
			return nil, fmt.Errorf("service-graph shard %s (%s): %w", t.name, t.endpoint, err)
		}
		r.clients[t.name] = c
		r.closers = append(r.closers, c.Close)
	}
	r.ring = NewRing(names, cfg.TokensPerShard)
	if cfg.Self == "" || !seen[cfg.Self] {
		// Not fatal — every share simply becomes remote and this pod sends its
		// own traces to itself through the internal receiver, which is correct,
		// just twice the traffic. Loud, because nothing else would ever say so.
		log.Warn("this shard's own name is not in the service-graph ring, so every span takes an extra network hop (including the ones this pod already owns); set serviceGraphShards.self (or $POD_NAME) to one of the ring's names",
			"self", cfg.Self, "ring", strings.Join(names, ","))
	}
	return r, nil
}

// NewResharderWithClients builds a Resharder over caller-supplied per-shard
// exporters, keyed by shard name. It is the seam the tests use instead of real
// OTLP clients — and the only way to construct one without opening a connection.
// self names this shard; it joins the ring even though it has no client entry.
func NewResharderWithClients(self string, clients map[string]TracesExporter, tokensPerShard int, log *slog.Logger) *Resharder {
	if log == nil {
		log = slog.Default()
	}
	names := make([]string, 0, len(clients)+1)
	for name := range clients {
		names = append(names, name)
	}
	if self != "" {
		names = append(names, self)
	}
	return &Resharder{
		ring:    NewRing(names, tokensPerShard),
		self:    self,
		clients: clients,
		log:     log,
	}
}

// Ring exposes the shard ring (for a startup log line, or a /debug handler).
func (r *Resharder) Ring() *Ring { return r.ring }

// Stats snapshots the counters.
func (r *Resharder) Stats() ReshardStats {
	c := r.tally()
	return ReshardStats{
		SpansForwarded: c.spansForwarded.Load(),
		SpansLocal:     c.spansLocal.Load(),
		SpansUnkeyed:   c.spansUnkeyed.Load(),
		SendsFailed:    c.sendsFailed.Load(),
		LoopsBlocked:   c.loopsBlocked.Load(),
	}
}

// CountLoopBlocked records a refused application push that carried
// ForwardedMarker. The refusal belongs to the receive path and the counter lives
// here, because this is where the marker's whole story is.
//
// Where the refusal sits: ABOVE enrichment, on the hook the ingest server
// consults before it attributes a trace payload
// (otlpingest.ServerConfig.RejectTraces, wired by cmd/kubescrape-agent). That
// ordering is the point — a payload this counter records is one nothing should
// be spent on: no metadata lookup per resource, no ingest counter
// (obs.Ingested{enriched|unresolved|peer_ip_rejected}) moved by traffic that is
// refused, and no attribution deduced from a peer address that belongs to the
// sending SHARD rather than to any application. The refusal is PERMANENT, so the
// payload is neither re-sharded nor retried, and one hop is where it ends.
func (r *Resharder) CountLoopBlocked(spans int) {
	if spans <= 0 {
		return
	}
	r.tally().loopsBlocked.Add(uint64(spans))
}

// Close shuts every shard client down. There is nothing to drain: the hop is
// synchronous, so anything in flight is a handler that has not returned yet, and
// the receiving server's own graceful stop is what waits for it.
func (r *Resharder) Close() error {
	if r == nil {
		return nil
	}
	var first error
	for _, c := range r.closers {
		if err := c(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Reshard sends every span in td to the shard owning its trace and returns the
// share THIS shard owns, for local pairing, span metrics and export. The
// returned Traces holds no spans when this shard owns none.
//
// It TAKES OWNERSHIP of td: the single-owner fast path returns (or forwards) the
// caller's own payload rather than copying it, and stamps the loop marker on it
// when that owner is remote. Callers hand over a freshly-decoded request and use
// only the return value.
//
// # Failure
//
// An error means at least one owner did not accept its share, and the caller
// must fail the application's push so the sender re-pushes. Shares sent BEFORE
// the failing one have already landed; the sender's retry re-splits
// deterministically (the ring is a pure function of the trace id) and those
// owners see them a second time. That is at-least-once — the same trade
// agent/route makes for tenancy fan-out — and it is why the owner's taps count
// only after a SUCCESSFUL export: a re-delivered batch that was never counted
// costs nothing.
//
// Remote shares go first, deliberately: the most common correlated failure is
// the collector being unreachable, which fails every owner including this one,
// and returning on the first remote error costs less than discovering it after
// the local chain has run.
func (r *Resharder) Reshard(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	if r == nil || len(r.clients) == 0 {
		// Single-shard tier (or every shard is us): everything is already local.
		// The unkeyable tally still has to happen — neither counting pass runs on
		// this path, and a metric whose help text promises "a moving rate means an
		// SDK is emitting malformed spans" must not be frozen at zero on the
		// commonest topology there is.
		r.countUnkeyed(td)
		r.countLocal(td.SpanCount())
		return td, nil
	}
	local, remote := r.split(td)
	for name, g := range remote {
		client, ok := r.clients[name]
		if !ok { // unreachable: the ring is built from the client map's keys plus self
			return ptrace.NewTraces(), fmt.Errorf("service-graph shard %q has no client", name)
		}
		n := uint64(g.SpanCount())
		markForwarded(g)
		if err := client.ExportTraces(ctx, g); err != nil {
			r.counters.sendsFailed.Add(1)
			r.warn("re-sharding spans to a service-graph shard failed; the push is refused so the sender retries",
				"shard", name, "spans", n, "error", err)
			return ptrace.NewTraces(), fmt.Errorf("service-graph shard %s: %w", name, err)
		}
		r.counters.spansForwarded.Add(n)
	}
	r.countLocal(local.SpanCount())
	return local, nil
}

func (r *Resharder) countLocal(n int) {
	if n <= 0 {
		return
	}
	r.tally().spansLocal.Add(uint64(n))
}

// countUnkeyed tallies the spans of td that carry no trace id — the single-shard
// path's version of what singleOwner and owner do for the sharded one, and the
// only walk that path makes (one id check per span, no copy, no hash).
//
// The walk is UNCONDITIONAL and pays for it: this path goes from O(1) to
// O(spans), ~1.9 ns/span (BenchmarkReshardSingleOwner, 4.7 ns -> 169 ns on a
// 20-span batch, 0 allocs). That is 0.19% of one core at a million spans a
// second, a rate one shard cannot reach — decoding those spans costs orders of
// magnitude more. Gating it on whether anything reads the counter would buy the
// walk back and make a zero mean two things, "no malformed spans" and "nobody
// asked", which is the ambiguity an SDK-health signal must not have.
func (r *Resharder) countUnkeyed(td ptrace.Traces) {
	var unkeyed uint64
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				if spans.At(k).TraceID().IsEmpty() {
					unkeyed++
				}
			}
		}
	}
	if unkeyed > 0 {
		r.tally().spansUnkeyed.Add(unkeyed)
	}
}

// split partitions td by owning shard. The single-owner case — a one-shard tier,
// a small batch, or any batch whose traces happen to hash to one place — returns
// the caller's payload untouched, which is the difference between this hop
// costing a copy of every span and costing nothing.
func (r *Resharder) split(td ptrace.Traces) (local ptrace.Traces, remote map[string]ptrace.Traces) {
	if only, ok := r.singleOwner(td); ok {
		if only == r.self {
			return td, nil
		}
		return ptrace.NewTraces(), map[string]ptrace.Traces{only: td}
	}
	local = ptrace.NewTraces()
	remote = make(map[string]ptrace.Traces, 4)
	// res maps an owner to the ResourceSpans it is filling for the resource being
	// walked, cur to the ScopeSpans within it for the scope being walked. Reset
	// per resource and per scope respectively, so each owner gets one
	// ResourceSpans per source resource and one ScopeSpans per source scope — the
	// grouping the collector expects, and what keeps instrumentation-scope
	// identity intact on a payload that is now the one the collector receives.
	res := make(map[string]ptrace.ResourceSpans, 4)
	cur := make(map[string]ptrace.ScopeSpans, 4)

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		clear(res)
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j)
			clear(cur)
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				owner := r.owner(span)
				dst, ok := cur[owner]
				if !ok {
					nrs, ok := res[owner]
					if !ok {
						g := local
						if owner != r.self {
							if g, ok = remote[owner]; !ok {
								g = ptrace.NewTraces()
								remote[owner] = g
							}
						}
						nrs = g.ResourceSpans().AppendEmpty()
						rs.Resource().CopyTo(nrs.Resource())
						nrs.SetSchemaUrl(rs.SchemaUrl())
						res[owner] = nrs
					}
					dst = nrs.ScopeSpans().AppendEmpty()
					ss.Scope().CopyTo(dst.Scope())
					dst.SetSchemaUrl(ss.SchemaUrl())
					cur[owner] = dst
				}
				span.CopyTo(dst.Spans().AppendEmpty())
			}
		}
	}
	return local, remote
}

// singleOwner reports the one shard owning every span in td, if there is one. It
// walks the spans without allocating; the split path re-walks them, which is the
// price of never copying a payload that did not need it.
//
// The unkeyable tally is local and is only published when this pass WINS (a
// single owner, so the split pass never runs). Bailing out early publishes
// nothing and leaves the counting to split's own owner(), so a span is tallied
// exactly once however many times the two passes look at it.
func (r *Resharder) singleOwner(td ptrace.Traces) (string, bool) {
	first := ""
	seen := false
	var unkeyed uint64
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				if span.TraceID().IsEmpty() {
					unkeyed++
				}
				o := r.ownerOf(span)
				if !seen {
					first, seen = o, true
					continue
				}
				if o != first {
					return "", false
				}
			}
		}
	}
	if unkeyed > 0 {
		r.counters.spansUnkeyed.Add(unkeyed)
	}
	if !seen {
		// No spans at all: treat it as ours, so an empty push is acked without a
		// hop.
		return r.self, true
	}
	return first, true
}

// owner is ownerOf plus the unkeyable counter, for the SPLIT pass (see
// singleOwner on why exactly one of the two passes counts).
func (r *Resharder) owner(span ptrace.Span) string {
	if span.TraceID().IsEmpty() {
		r.counters.spansUnkeyed.Add(1)
	}
	return r.ownerOf(span)
}

// ownerOf is the shard owning span's trace, or this shard for a span that cannot
// be hashed.
func (r *Resharder) ownerOf(span ptrace.Span) string {
	tid := span.TraceID()
	if tid.IsEmpty() {
		// Unkeyable: it can never pair (there is no trace to pair within), and
		// every zero id hashes to the same token, so forwarding them would pile
		// the whole cluster's malformed spans onto one shard. Kept here, where
		// the Service's round-robin has already spread them.
		return r.self
	}
	if o := r.ring.Owner(tid[:]); o != "" {
		return o
	}
	return r.self // empty ring
}

// warn logs at most once per reshardWarnEvery across all shards.
func (r *Resharder) warn(msg string, args ...any) {
	if !r.warnGate.Allow(reshardWarnEvery) {
		return
	}
	r.log.Warn(msg, args...)
}

// --- the loop marker ---

// markForwarded stamps ForwardedMarker on every resource of a payload about to
// cross to another shard.
func markForwarded(td ptrace.Traces) {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rss.At(i).Resource().Attributes().PutBool(ForwardedMarker, true)
	}
}

// IsForwarded reports whether any resource in td carries ForwardedMarker. The
// tier's APPLICATION-facing listener refuses such a payload: no SDK sets this
// key, so its presence there means an internal hop was addressed to the wrong
// port, which without the refusal amplifies without bound (see ForwardedMarker).
func IsForwarded(td ptrace.Traces) bool {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		if _, ok := rss.At(i).Resource().Attributes().Get(ForwardedMarker); ok {
			return true
		}
	}
	return false
}

// StripForwarded removes ForwardedMarker from every resource. The owner calls it
// on arrival: the marker is this tier's transport detail, and an application's
// spans must not reach the collector carrying kubescrape's internal plumbing as
// a resource attribute (downstream it would become a target_info dimension).
func StripForwarded(td ptrace.Traces) {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rss.At(i).Resource().Attributes().Remove(ForwardedMarker)
	}
}
