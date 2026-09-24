package obs

import (
	"github.com/JohanLindvall/kubescrape/internal/metrics"
)

// Trace tier (the -service-graph shard's head sampler and span metrics).
var (
	TraceSpansDropped = Registry.CounterVec("kubescrape_trace_spans_dropped_total",
		"Ingested spans dropped by the trace sampler (probability = the consistent trace-ID decision, rate = the spans/second cap).", "reason")
	SpanMetricsDropped = Registry.Counter("kubescrape_span_metrics_dropped_total",
		"Spans not aggregated into span metrics because the dimension-cardinality cap was reached.")
	SpanMetricsEvicted = Registry.Counter("kubescrape_span_metrics_evicted_total",
		"Span-metric series dropped at export because their dimensions went unobserved for traceMetrics.staleAfter (this is what frees cardinality-cap slots).")
)

// Service-graph edges (the -service-graph shard). Every one of these counts a
// place where the graph is INCOMPLETE, and each names its cause — a config
// bound for all but the unnamed-spans counter, because those bounds trade
// completeness for memory and an operator has to be able to see which one is
// binding: an edge missing from Grafana's Service Graph view looks exactly
// like a call that never happened.
var (
	ServiceGraphDropped = Registry.Counter("kubescrape_service_graph_dropped_total",
		"Edges not aggregated because the serviceGraph.maxCardinality series cap was reached (existing edges keep reporting; a new one is lost until eviction frees a slot).")
	ServiceGraphEvicted = Registry.Counter("kubescrape_service_graph_evicted_total",
		"Edge series dropped at export because they went unobserved for serviceGraph.staleAfter (this is what frees cardinality-cap slots).")
	// ServiceGraphStoreFull is the pairing store's back-pressure. It is bumped
	// per SPAN, not per edge: the span is never stored, so its partner will
	// later expire unpaired too, and both counters move for one lost request.
	ServiceGraphStoreFull = Registry.Counter("kubescrape_service_graph_store_full_total",
		"Spans dropped because the pairing store held serviceGraph.maxItems half-edges (the request cannot become an edge).")
	// ServiceGraphExpired is the OTHER shape of an incomplete graph, and a
	// non-zero baseline is normal: a client half whose server is uninstrumented
	// expires by design (and may then become a virtual-node edge). A RISING rate
	// means serviceGraph.wait is shorter than the real client-to-server span
	// delivery gap, or the two halves are landing on different shards.
	ServiceGraphExpired = Registry.Counter("kubescrape_service_graph_expired_total",
		"Half-edges that expired after serviceGraph.wait without their partner arriving.")
	// ServiceGraphUnnamed is the one counter in this block with no config bound
	// behind it: the incompleteness is the SENDER's. A resource without
	// service.name gives the graph no label value to name a node with, and
	// pairing its spans anyway minted one anonymous ""-labeled vertex that
	// every unattributable sender in the cluster shared, accreting edges to
	// real services (Tempo skips such resources too).
	//
	// It counts only the spans PAIRING WOULD HAVE USED — client, server,
	// producer, consumer — because the named arm counts nothing at all for an
	// INTERNAL or UNSPECIFIED span, and an ordinary batch is mostly those. The
	// two arms of one question have to be in one unit or the ratio against
	// _completed_total is inflated by a fraction that could never be an edge.
	// Two such spans make one request, so the ratio is still against half the
	// spans a completed edge takes.
	ServiceGraphUnnamed = Registry.Counter("kubescrape_service_graph_unnamed_spans_total",
		"Edge-capable spans (client/server/producer/consumer) skipped by the service-graph processor because their resource carries no service.name — nothing to name a graph node with. INTERNAL and UNSPECIFIED spans are not counted: pairing would have skipped them too. The shipped tier enriches at entry and derives service.name for every attributable sender, so a sustained rate means unattributable senders whose spans can mint no edge (they still forward to the collector; only the graph skips them); two counted spans are one lost request.")
)

// RegisterServiceGraphStats publishes the pairing store's own numbers on the
// shard (-service-graph): what pairing ACHIEVED, plus the one leading
// indicator the counters above cannot give.
//
// The counters above all move only once the graph has already lost something.
// The pending gauge is the backlog against serviceGraph.maxItems — "the store
// is at 80% and climbing" — the same reason RegisterBufferStats exists beside
// the disk buffer's drop counters. Completed is what makes the rest readable:
// an expiry rate means nothing without the pairing rate it is a fraction of.
//
// Published through a registration function because the values are owned by
// servicegraph's store, which obs cannot reach — that package imports obs, so
// the dependency runs one way only. ServiceGraphStat mirrors
// servicegraph.Stats for exactly that reason.
//
// Stats.Dropped and Stats.Unpaired are deliberately NOT published here: the
// first is already kubescrape_service_graph_store_full_total, and the second is
// kubescrape_service_graph_expired_total minus the virtual-node counter below
// (every expiry goes exactly one of those two ways). A second series carrying a
// number two others already determine is one more thing to keep consistent.
//
// stats is sampled ONCE per export or scrape and its one snapshot serves all
// four series (metrics.PerPass), like every struct-shaped hook here.
func RegisterServiceGraphStats(stats func() ServiceGraphStat) {
	stats = metrics.PerPass(Registry, stats)
	Registry.GaugeFunc("kubescrape_service_graph_pending_edges",
		"Half-edges currently awaiting their partner. serviceGraph.maxItems caps this; at the cap spans are refused (see kubescrape_service_graph_store_full_total), so the ratio against the cap is the leading indicator.",
		func() float64 { return float64(stats().Pending) })
	Registry.CounterFunc("kubescrape_service_graph_completed_total",
		"Edges completed by BOTH halves arriving within serviceGraph.wait. The denominator for the loss counters: an expiry or drop rate only means something against the rate of pairings that worked.",
		func() float64 { return float64(stats().Completed) })
	Registry.CounterFunc("kubescrape_service_graph_virtual_node_total",
		"Half-edges that expired unpaired but named their far side through serviceGraph.virtualNodePeerAttributes, and so still reached the graph (as a virtual-node edge). The remainder of kubescrape_service_graph_expired_total is the genuinely lost part — nothing named the missing side, so that request is on no edge at all.",
		func() float64 { return float64(stats().VirtualNode) })
	Registry.CounterFunc("kubescrape_service_graph_unkeyable_total",
		"Spans that could not be keyed for pairing: no trace id, or a client/producer span with no span id of its own. Never stored — every zero id shares one key space, so they would cross-pair unrelated requests into invented edges (a zero-id client span keys exactly where every ROOT SERVER span of its trace does). A moving rate means an SDK is emitting malformed spans.",
		func() float64 { return float64(stats().Unkeyable) })
}

// ServiceGraphStat is the pairing store's snapshot (servicegraph.Stats' shape).
type ServiceGraphStat struct {
	Pending     int
	Completed   uint64
	VirtualNode uint64
	Unkeyable   uint64
}

// RegisterServiceGraphResharder publishes the tier's INTERNAL hop: how a shard
// that received an application push distributed those spans to the shards that
// own their traces.
//
// These are the numbers only the resharder knows. The wire outcome of each hop
// lands in kubescrape_export_requests_total{signal="traces"} together with the
// ordinary collector exports, where a hop failure is indistinguishable from a
// collector failure — and the two mean very different things: one is a shard
// that cannot be reached, the other is a destination outside the cluster.
//
// A failed hop is NOT silent, unlike the best-effort side channel this replaced:
// the entry shard holds the only copy of a pushed span, so it refuses the
// application's push and the sender retries. SendsFailed therefore moves in step
// with the senders' own error rate.
func RegisterServiceGraphResharder(stats func() ServiceGraphReshardStat) {
	Registry.CounterFunc("kubescrape_service_graph_spans_forwarded_total",
		"Spans this shard handed to ANOTHER shard because that shard owns their trace, and which it accepted. Roughly (N-1)/N of everything pushed to this pod on an N-shard tier: the remainder is kubescrape_service_graph_spans_local_total. This is the tier's internal bandwidth.",
		func() float64 { return float64(stats().SpansForwarded) })
	Registry.CounterFunc("kubescrape_service_graph_spans_local_total",
		"Spans this shard already owned and kept in-process, taking no second hop. Its ratio against the forwarded count should track 1/N for an N-shard tier; a persistent skew means the ring is unbalanced or one shard is receiving most of the pushes.",
		func() float64 { return float64(stats().SpansLocal) })
	Registry.CounterFunc("kubescrape_service_graph_spans_unkeyed_total",
		"Pushed spans with no trace id. They cannot be hashed onto the ring, so they are kept locally and exported from here; they can never pair into an edge. A moving rate means an SDK is emitting malformed spans.",
		func() float64 { return float64(stats().SpansUnkeyed) })
	Registry.CounterFunc("kubescrape_service_graph_sends_failed_total",
		"Failed internal hops (one per owning shard per batch). Every one of them FAILS the application's push — the entry shard holds the only copy of those spans — so this moves together with the senders' export errors, and a rate that tracks one shard means that shard is down or its token is wrong.",
		func() float64 { return float64(stats().SendsFailed) })
	Registry.CounterFunc("kubescrape_service_graph_loops_blocked_total",
		"Spans in application pushes refused for carrying the internal forwarded marker: an internal hop addressed to the tier's APPLICATION port instead of its authenticated receiver, which without the refusal would re-enrich and re-shard every span on every hop until the network is the incident. Always zero in a correct deployment; anything else is a config error to fix now.",
		func() float64 { return float64(stats().LoopsBlocked) })
}

// ServiceGraphReshardStat is the resharder's snapshot
// (servicegraph.ReshardStats' shape; see RegisterServiceGraphStats on why obs
// declares its own).
type ServiceGraphReshardStat struct {
	SpansForwarded uint64
	SpansLocal     uint64
	SpansUnkeyed   uint64
	SendsFailed    uint64
	LoopsBlocked   uint64
}

// Tail sampling (the -service-graph shard's buffering layer). Two things need
// counting here that no other layer knows: WHICH rule decided a trace — the
// policy engine returns it precisely so the answer is not "some subset of the
// list" — and what the memory bounds cost when they bind, since every bound
// trades trace completeness for a smaller buffer.
var (
	TailSampleTraces = Registry.CounterVec("kubescrape_tail_sampling_traces_total",
		"Traces decided by the tail sampler, by verdict (keep, drop) and by the policy that decided. policy=\"none\" is the default drop — no policy had an opinion — which is what a policy list matching nothing looks like. Every configured policy gets its series at startup, so a policy that has never fired reads as zero rather than as absent. This counts DECISIONS: whether the kept trace then reached the collector is kubescrape_tail_sampling_spans_total.", "decision", "policy")
	TailSampleSpans = Registry.CounterVec("kubescrape_tail_sampling_spans_total",
		"Spans leaving the tail-sampling buffer, by fate: kept = the trace was sampled and the export was acked (with -buffer-dir, acked means SPOOLED — a decided keep is durable and a collector outage becomes a backlog); dropped = the trace was not sampled; lost = the final attempt to move the trace's spans out of this process failed and nothing here holds a copy any longer (the sender was acked when the spans were buffered). Under a routed or size-split export, earlier shares may already have been delivered or spooled, so lost is an UPPER BOUND on loss — it never under-counts. A moving lost rate is data loss, not back-pressure; without -buffer-dir a collector outage produces it directly, with one it means the spool itself refused the payload.", "outcome")
	TailSampleEarly = Registry.CounterVec("kubescrape_tail_sampling_early_decisions_total",
		"Traces decided BEFORE their decisionWait elapsed, by the bound that forced it (spans_per_trace, max_traces, max_spans) or shutdown (a graceful stop flushing the buffer, plus any straggler push decided on arrival after that flush). An early decision judges the spans present, so it degrades gracefully — a slow trace can be missed, a fast one is never invented — but a sustained rate against a bound means that bound is sized below the shard's span rate, or that the decision loop is stalled behind a slow export (it decides nothing while its own send is in flight, so the buffer fills); the early-decision warning's slowestDecisionExport tells the two apart. A trace whose window had already closed when a bound caught it is not counted.", "reason")
	TailSampleLate = Registry.CounterVec("kubescrape_tail_sampling_late_spans_total",
		"Spans that arrived after their trace was decided and followed the cached verdict, by outcome (kept = forwarded immediately, dropped). A large kept+dropped share means decisionWait is shorter than the spread of a trace's arrival.", "outcome")
	TailSampleCacheEvicted = Registry.Counter("kubescrape_tail_sampling_cache_evictions_total",
		"Verdicts evicted from the decision cache by its SIZE cap while still LIVE (evicting an entry already past its TTL is the cache reclaiming space, not a signal, and is not counted). A span arriving for an evicted trace starts a fresh window AND re-charges the rateLimiting and composite budgets — the cache remembers a trace was charged for as long as it holds the entry at all, so eviction is the only thing that still double-charges. Raise tailSampling.decisionCacheSize if this moves.")
)

// RegisterTailSamplingStats publishes what the tail-sampling buffer is holding.
//
// This is the one number the counters above cannot give and the one an operator
// most needs: buffered spans are ACKED but not durable (see agent/tailbuffer's
// package doc), so this gauge is exactly what a hard kill of the shard would
// lose at that instant. It is also the leading indicator for the memory bounds,
// the same reason RegisterBufferStats exists beside the disk buffer's drop
// counters — the early-decision counter only moves once a bound has ALREADY
// bound. stats is sampled once per export or scrape (metrics.PerPass): it takes
// the buffer's mutex, and both gauges must describe one instant.
func RegisterTailSamplingStats(stats func() TailSamplingStat) {
	stats = metrics.PerPass(Registry, stats)
	Registry.GaugeFunc("kubescrape_tail_sampling_buffered_traces",
		"Traces currently assembling in the tail-sampling buffer, awaiting their decision. tailSampling.maxTraces caps it; at the cap the oldest is decided early (kubescrape_tail_sampling_early_decisions_total).",
		func() float64 { return float64(stats().Traces) })
	Registry.GaugeFunc("kubescrape_tail_sampling_buffered_spans",
		"Spans currently held in the tail-sampling buffer. These are acked to their senders but not yet decided and not durable anywhere (a DECIDED keep is spooled when -buffer-dir is set; an undecided one is not): this is what a hard kill of this pod would lose. tailSampling.maxSpans caps it, and is itself checked against the pod's memory limit at startup — the likeliest hard kill here is the OOM this buffer causes.",
		func() float64 { return float64(stats().Spans) })
}

// TailSamplingStat is the buffer's occupancy (tailbuffer.Stats' shape; see
// RegisterServiceGraphStats on why obs declares its own).
type TailSamplingStat struct {
	Traces int
	Spans  int
}
