// Package servicegraph derives service-graph EDGE metrics from ingested trace
// spans: for each request, one series describing the call from the CLIENT
// service to the SERVER service, with both sides' latency, the error count and
// the connection type.
//
// # Why this is not part of agent/spanmetrics
//
// spanmetrics aggregates every span INDEPENDENTLY, which is why its RED
// metrics are already correct cluster-wide: the spans partition across agents
// and cumulative counters sum. An edge is the opposite shape — it needs BOTH
// halves of one request, and the two halves are produced by two pods that
// usually run on different nodes, so the two agents that receive them never
// see each other's half. No amount of per-agent aggregation fixes that.
//
// # The Tempo model
//
// This follows Grafana Tempo's metrics-generator: spans are routed by a hash
// of the TRACE ID onto a ring, so every span of a trace reaches the same
// owner, and pairing is then a local in-memory operation. The hash is Tempo's
// (FNV-1 32-bit over the trace-id bytes, pkg/util.TokenFor), so the
// distribution matches; see ring.go for what is deliberately different (fixed,
// deterministically-derived tokens rather than gossip-registered random ones,
// because the shard set here is an ordinal StatefulSet rather than a
// memberlist cluster).
//
// The emitted metrics are wire-compatible with Tempo's, because Grafana's
// Service Graph view queries those exact names and labels — see metrics.go.
// Compatibility is the point: a bespoke series set would render in nothing.
//
// # Topology
//
// The DaemonSet agents do NOT pair among themselves. Every agent is sharded
// by node — the one key guaranteed to split a request's two halves — and an
// agent-to-agent mesh would need peer discovery the DaemonSet has no RBAC for,
// would rebuild its ring on every node drain (discarding in-flight pairing
// state cluster-wide), and would need every agent reachable from every other.
// Instead a small, separately-scaled tier owns the ring, exactly as Tempo
// splits distributors from metrics-generators: agents forward TRIMMED spans
// (see Trim) to the owning shard over the ordinary OTLP export path, and the
// shards run this package.
//
// Everything here is OPT-IN (-service-graph on the shard, -service-graph-shards
// on the agent): it costs a workload, a network hop per span and a new metric
// family, none of which an operator should pay for without asking.
package servicegraph

import (
	"fmt"
	"time"
)

// Defaults mirror Tempo's service-graphs processor, so an operator moving from
// Tempo gets the same behaviour without re-tuning. Tempo: wait 10s, max_items
// 10_000, buckets ExponentialBuckets(0.1, 2, 8).
const (
	DefaultWait            = 10 * time.Second
	DefaultMaxItems        = 10_000
	DefaultMaxCardinality  = 20_000
	DefaultStaleAfter      = 15 * time.Minute
	defaultBucketStart     = 0.1
	defaultBucketFactor    = 2
	defaultBucketCount     = 8
	maxDimensionValueBytes = 256
)

// Config is the `serviceGraph` section of the agent config file.
//
// Wait, MaxItems and MaxCardinality are the three bounds that decide what this
// costs and what it loses, so all three are configurable rather than baked in:
// Wait trades completeness against memory (a longer window pairs more requests
// but holds more half-edges), MaxItems bounds the pairing store, and
// MaxCardinality bounds the emitted series. Each has a counter that moves when
// it binds, so a too-small value is visible rather than silent.
type Config struct {
	// Wait is how long a half-edge waits for its partner before it expires.
	// An expired CLIENT half can still become an edge against a virtual node
	// (see VirtualNodePeerAttributes); an expired server half cannot, and is
	// counted as unpaired.
	Wait time.Duration `json:"wait,omitempty"`
	// MaxItems bounds the pairing store (half-edges awaiting a partner). Over
	// it, spans are dropped and counted — never silently.
	MaxItems int `json:"maxItems,omitempty"`
	// MaxCardinality bounds the number of distinct EDGE series. A new edge
	// over the cap is dropped and counted; existing edges keep reporting,
	// because these are cumulative series.
	MaxCardinality int `json:"maxCardinality,omitempty"`
	// StaleAfter evicts an edge series that has not been observed for this
	// long (0 disables). Without it the cardinality cap is a one-way latch:
	// one burst of short-lived services permanently blinds the graph.
	StaleAfter time.Duration `json:"staleAfter,omitempty"`
	// HistogramBuckets are the latency buckets, in SECONDS, for both the
	// client and server duration histograms. Empty uses Tempo's default.
	HistogramBuckets []float64 `json:"histogramBuckets,omitempty"`
	// Dimensions are extra span/resource attribute keys lifted onto the edge
	// series. Tempo's rule applies: a dimension resolves from whichever side
	// carried it, exposed as client_<dim> and server_<dim>.
	Dimensions []string `json:"dimensions,omitempty"`
	// VirtualNodePeerAttributes name the far side of an UNPAIRED client span,
	// in precedence order, so calls to uninstrumented dependencies (managed
	// databases, third-party APIs) still appear on the graph. Tempo's default
	// is peer.service, db.name, db.system. An empty list disables virtual
	// nodes; nil takes the default.
	VirtualNodePeerAttributes []string `json:"virtualNodePeerAttributes,omitempty"`
}

// Validate checks the section without acquiring anything, so -check-config can
// run it. It is shape-only by design (see cmd/kubescrape-agent/config.go).
func (c *Config) Validate() error {
	if c == nil {
		return nil
	}
	if c.Wait < 0 {
		return fmt.Errorf("wait must not be negative")
	}
	if c.MaxItems < 0 || c.MaxCardinality < 0 {
		return fmt.Errorf("maxItems and maxCardinality must not be negative")
	}
	if c.StaleAfter < 0 {
		return fmt.Errorf("staleAfter must not be negative")
	}
	var prev float64
	for i, b := range c.HistogramBuckets {
		if b <= 0 {
			return fmt.Errorf("histogramBuckets[%d] = %v (want > 0, in seconds)", i, b)
		}
		if i > 0 && b <= prev {
			return fmt.Errorf("histogramBuckets must be strictly increasing (%v after %v)", b, prev)
		}
		prev = b
	}
	return nil
}

// withDefaults returns the config with Tempo's defaults filled in.
func (c Config) withDefaults() Config {
	if c.Wait == 0 {
		c.Wait = DefaultWait
	}
	if c.MaxItems == 0 {
		c.MaxItems = DefaultMaxItems
	}
	if c.MaxCardinality == 0 {
		c.MaxCardinality = DefaultMaxCardinality
	}
	if c.StaleAfter == 0 {
		c.StaleAfter = DefaultStaleAfter
	}
	if len(c.HistogramBuckets) == 0 {
		c.HistogramBuckets = defaultBuckets()
	}
	if c.VirtualNodePeerAttributes == nil {
		c.VirtualNodePeerAttributes = DefaultPeerAttributes()
	}
	return c
}

// defaultBuckets is Tempo's prometheus.ExponentialBuckets(0.1, 2, 8), in
// seconds: 0.1 0.2 0.4 0.8 1.6 3.2 6.4 12.8.
func defaultBuckets() []float64 {
	out := make([]float64, defaultBucketCount)
	v := defaultBucketStart
	for i := range out {
		out[i] = v
		v *= defaultBucketFactor
	}
	return out
}

// DefaultPeerAttributes is Tempo's defaultPeerAttributes, in precedence order.
func DefaultPeerAttributes() []string {
	return []string{"peer.service", "db.name", "db.system"}
}

// ConnectionType classifies an edge. The values are Tempo's verbatim: they are
// LABEL VALUES that Grafana's Service Graph view matches on.
type ConnectionType string

const (
	// ConnectionUnknown is a plain service-to-service call.
	ConnectionUnknown ConnectionType = ""
	// ConnectionMessagingSystem is a producer/consumer pair.
	ConnectionMessagingSystem ConnectionType = "messaging_system"
	// ConnectionDatabase is a client span naming a database.
	ConnectionDatabase ConnectionType = "database"
	// ConnectionVirtualNode is an edge whose far side was never instrumented
	// and was named from a peer attribute instead.
	ConnectionVirtualNode ConnectionType = "virtual_node"
)

// Edge is one completed (or expired-and-promoted) call between two services.
// It is what the store hands the metric writer; nothing else crosses that
// seam.
type Edge struct {
	ClientService string
	ServerService string
	Connection    ConnectionType
	// ClientSeconds and ServerSeconds are each side's own measured duration.
	// They are reported as SEPARATE histograms rather than one: the two are
	// measured by different processes with unsynchronised clocks, and their
	// difference (network + queue time) is the operator's, not ours, to take.
	// A side that never arrived is zero and is not observed.
	ClientSeconds, ServerSeconds float64
	HaveClient, HaveServer       bool
	// Failed is true when either side reported an error status.
	Failed bool
	// Dimensions are the configured extra labels, already prefixed client_ /
	// server_ and truncated.
	Dimensions map[string]string
	// VirtualNode is "client" or "server" when one side was synthesized, else
	// empty. Tempo exposes this as the `virtual_node` dimension.
	VirtualNode string
}
