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
// A small, separately-scaled tier owns the ring and the whole trace pipeline.
// Applications push their OTLP traces to that tier's Service; the shard that
// receives a push enriches it with Kubernetes metadata (while the connection's
// peer address still names the sender — that is the one thing only the entry
// shard can do), then re-shards each span by trace id and hands it to the shard
// owning that trace (reshard.go). The owner holds every span of the trace, so it
// can pair edges here, derive per-span RED metrics, apply head sampling and
// export to the collector.
//
// Pairing cannot happen in the per-node DaemonSet at all: a request's client and
// server spans are emitted by two pods that usually run on two different nodes,
// so a node-local aggregator holds half of every edge by construction. That is
// exactly the opposite of agent/spanmetrics, where each span is aggregated
// INDEPENDENTLY and cumulative counters simply sum cluster-wide.
//
// The tier is OPT-IN (-service-graph): it costs a workload, and a cluster that
// does not push traces has no use for it.
package servicegraph

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/agent/cumagg"
	"github.com/JohanLindvall/kubescrape/internal/config"
)

// Defaults mirror Tempo's service-graphs processor, so an operator moving from
// Tempo gets the same behaviour without re-tuning. Tempo: wait 10s, max_items
// 10_000, buckets ExponentialBuckets(0.1, 2, 8).
const (
	DefaultWait           = 10 * time.Second
	DefaultMaxItems       = 10_000
	DefaultMaxCardinality = 20_000
	DefaultStaleAfter     = 15 * time.Minute
	defaultBucketStart    = 0.1
	defaultBucketFactor   = 2
	defaultBucketCount    = 8
	// maxDimensionValueBytes bounds one retained label value; see
	// cumagg.MaxLabelBytes for what it is protecting against.
	maxDimensionValueBytes = cumagg.MaxLabelBytes
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
	// Wait is how long a half-edge waits for its partner before it expires (a
	// Go duration such as "10s"; empty = 10s). An expired half carrying a peer
	// attribute — EITHER side — still becomes an edge against a virtual node
	// (see VirtualNodePeerAttributes): an expired client half names its CALLEE
	// (virtual_node="server"), and an expired server half names its CALLER
	// (virtual_node="client"), which is how uninstrumented external callers —
	// browsers, ingresses — get named on the graph. A half with no peer
	// attribute is counted as unpaired.
	//
	// A STRING, not a time.Duration, and this is not a style choice: the agent
	// config is decoded through sigs.k8s.io/yaml -> encoding/json, which accepts
	// only a raw nanosecond integer for a time.Duration, so the documented "10s"
	// spelling would fail to decode — and since the whole file is
	// UnmarshalStrict'ed, that one field rejects the ENTIRE config and neither
	// the DaemonSet nor the shard StatefulSet starts. agent/spanmetrics'
	// staleAfter and traceSampling's keepSlowerThan are strings for the same
	// reason.
	Wait string `json:"wait,omitempty"`
	// MaxItems bounds the pairing store (half-edges awaiting a partner). Over
	// it, spans are dropped and counted — never silently.
	MaxItems int `json:"maxItems,omitempty"`
	// MaxCardinality bounds the number of distinct EDGE series. A new edge
	// over the cap is dropped and counted; existing edges keep reporting,
	// because these are cumulative series.
	MaxCardinality int `json:"maxCardinality,omitempty"`
	// StaleAfter evicts an edge series that has not been observed for this long
	// (a Go duration such as "15m"; empty = 15m, "0" disables eviction and keeps
	// every series for the process' life). Without it the cardinality cap is a
	// one-way latch: one burst of short-lived services permanently blinds the
	// graph.
	//
	// A STRING for the same reason as Wait above.
	StaleAfter string `json:"staleAfter,omitempty"`
	// HistogramBuckets are the latency buckets, in SECONDS, for both the
	// client and server duration histograms. Empty uses Tempo's default.
	HistogramBuckets []float64 `json:"histogramBuckets,omitempty"`
	// Exemplars attaches a trace/span-id exemplar (one per latency bucket,
	// reset after each DELIVERED export) to both duration histograms. nil
	// defaults to true — spelled as agent/spanmetrics spells it, since the two
	// generators' exemplars have identical semantics and an operator turning
	// one off is usually turning both off for the same reason.
	//
	// On by default because an edge's exemplar is the only link from "this
	// call between two services is slow" to a trace showing WHY, and it costs
	// one data point per occupied bucket per edge per export.
	Exemplars *bool `json:"exemplars,omitempty"`
	// Dimensions are extra span/resource attribute keys lifted onto the edge
	// series. Tempo's rule applies: a dimension resolves from whichever side
	// carried it, exposed as client_<dim> and server_<dim>.
	//
	// Nothing has to agree with anything for these to work: the spans reaching
	// the pairing store are the ones the sender pushed, whole, so a key added
	// here starts resolving on the next export with no second list to keep in
	// step.
	Dimensions []string `json:"dimensions,omitempty"`
	// VirtualNodePeerAttributes name the far side of an UNPAIRED client span,
	// in precedence order, so calls to uninstrumented dependencies (managed
	// databases, third-party APIs) still appear on the graph. Tempo's default
	// is peer.service, db.name, db.system. An empty list disables virtual
	// nodes; nil takes the default.
	VirtualNodePeerAttributes []string `json:"virtualNodePeerAttributes,omitempty"`
}

// wait parses Wait through config.Duration — the one reader for optional
// duration strings. Empty takes the default; an explicit zero is REFUSED
// (config.Positive): a zero pairing window is not a window at all (every
// half-edge would expire before its partner could arrive), and silently
// substituting the default — the third zero-policy this field used to carry,
// beside "0 is legal" and Positive's refusal — meant `wait: "0"` read as 10s
// with no diagnostic. The default rides along on error so the constructor can
// fall back; reporting the value is Validate's job.
func (c Config) wait() (time.Duration, error) {
	d, err := config.Duration("serviceGraph.wait", c.Wait, DefaultWait,
		config.Positive("a zero pairing window would expire every half-edge before its partner arrives"))
	if err != nil {
		return DefaultWait, err
	}
	return d, nil
}

// staleAfter parses StaleAfter. Empty means the default; an explicit zero
// DISABLES eviction (cumagg's <= 0 branch) and keeps every series for the
// process' life; a negative value is an error. The reading is
// cumagg.ParseStaleAfter's, shared with agent/spanmetrics' identical field —
// that one used to clamp a negative to zero, which silently DISABLED eviction
// instead of refusing the config.
func (c Config) staleAfter() (time.Duration, error) {
	return cumagg.ParseStaleAfter("serviceGraph.staleAfter", c.StaleAfter, DefaultStaleAfter)
}

// Validate checks the section without acquiring anything, so -check-config can
// run it. It is shape-only by design (see cmd/kubescrape-agent/config.go).
func (c *Config) Validate() error {
	if c == nil {
		return nil
	}
	if _, err := c.wait(); err != nil {
		return err
	}
	if _, err := c.staleAfter(); err != nil {
		return err
	}
	if c.MaxItems < 0 || c.MaxCardinality < 0 {
		return errors.New("maxItems and maxCardinality must not be negative")
	}
	return cumagg.ValidateBuckets("histogramBuckets", c.HistogramBuckets)
}

// withDefaults returns the config with Tempo's defaults filled in. The two
// DURATION fields are deliberately left alone: they are strings, and wait() /
// staleAfter() are where their defaults (and their "0 disables" reading) live —
// filling them in here would have to re-render a duration as text and would
// give "0" two meanings depending on which side of this call it was read.
func (c Config) withDefaults() Config {
	if c.MaxItems == 0 {
		c.MaxItems = DefaultMaxItems
	}
	if c.MaxCardinality == 0 {
		c.MaxCardinality = DefaultMaxCardinality
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
//
// DELIBERATELY narrower than processor.go's databaseAttrs, which also carries
// the current semconv spellings (db.system.name, db.namespace). The two
// lists answer different questions — databaseAttrs classifies an edge's
// connection_type, this list names the VIRTUAL NODE minted for an unpaired
// client span — and this one is a wire contract: Tempo's verbatim default,
// which Grafana's Service Graph view and any dashboard ported from Tempo
// assume. The asymmetry's visible consequence: a client instrumented ONLY with
// current conventions classifies as `database` but never promotes to a virtual
// node — its unpaired spans expire nameless — unless the operator extends
// `serviceGraph.virtualNodePeerAttributes` (Config.VirtualNodePeerAttributes)
// with the current keys. Extending the DEFAULT is a product decision (it
// changes the `server` label existing dashboards select on), not a refactor.
//
// As of semconv v1.44.0 ALL THREE entries here are deprecated upstream:
// db.name -> db.namespace (v1.26.0), db.system -> db.system.name (v1.30.0),
// and peer.service -> service.peer.name (v1.39.0, alongside a new
// service.peer.namespace that has no analogue here). The db pair is only half
// the story, since databaseAttrs already classifies both spellings;
// peer.service is the one that widens the gap, being the GENERAL case — a
// callee named only via service.peer.name mints no virtual node at all, not
// even a misclassified one. Operators on current SDKs want:
//
//	serviceGraph:
//	  virtualNodePeerAttributes: [peer.service, service.peer.name, db.name, db.system, db.namespace, db.system.name]
//
// which is the list CONFIGURATION.md documents, rather than this one.
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
	//
	// An ORDERED slice rather than a map, for two reasons that pull the same
	// way. The metric layer keys a series on this sequence, and a map has no
	// sequence — it had to iterate the keys into a scratch and SORT them on
	// every completed request to get one. And a map is two allocations per
	// dimensioned edge, on the shard's per-request path. The store hands the
	// pairs in an order that is already a function of the SET (the client
	// half's dimensions in the processor's configured order, then the server
	// half's), so the sort disappears rather than moving.
	//
	// The slice is BORROWED: it aliases a buffer the pairing store recycles the
	// instant the sink returns (see edgeStore.retire and joinDims), so a sink
	// that keeps the Edge past its Record call must copy the pairs out. Every
	// production sink already does — Registry.Record copies what it needs into
	// the series' own label set the one time the series is admitted.
	Dimensions []EdgeDimension
	// VirtualNode is "client" or "server" when one side was synthesized, else
	// empty. Tempo exposes this as the `virtual_node` dimension.
	VirtualNode string

	// TraceID is the trace both halves belong to, and is what an exemplar on
	// the duration histograms points at.
	//
	// There is no "which side" to decide: the trace id IS half the pairing key
	// (see edgeKey), so the two halves carry the same one by construction —
	// they cannot pair otherwise. Whichever half arrived first therefore sets
	// it, which is also the only half a promoted virtual-node edge has. Trace
	// sampling cannot split them either: it is consistent per trace id, so both
	// halves are sampled or neither is, and an exemplar either resolves in
	// Tempo or the trace was never stored to begin with.
	//
	// Zero when the edge came from spans with no trace id — impossible today
	// (the processor refuses to key those), but an exemplar is skipped rather
	// than emitted as a dead link either way.
	TraceID pcommon.TraceID
	// ClientSpanID and ServerSpanID are each half's OWN span id, and each one
	// rides on ITS OWN histogram's exemplar: the client-latency exemplar points
	// at the span that measured the client latency, the server's at the server's.
	// Attaching one id to both would explain only half of what it is attached
	// to. Zero for a side that never arrived.
	ClientSpanID, ServerSpanID pcommon.SpanID
}

// EdgeDimension is one configured extra label: the name is already prefixed
// client_ / server_, and the value points into the OTLP payload the span
// arrived in unless it was over-long, in which case it is a truncated COPY
// (cumagg.Retain) — the store holds it for a whole Wait, and a slice of the
// payload would hold the payload's string with it.
type EdgeDimension struct{ Name, Value string }
