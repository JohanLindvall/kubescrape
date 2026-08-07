package otlpingest

// The logs.rules / logMetrics / line-enrichment half of the ingest LOG path.
//
// The four log PRODUCERS run the shared chain (internal/agent/logchain):
// scrub → lift → enrich → log-metrics → rules. Ingest scrubs in EnrichLogs
// (before anything reads the body) and runs the REST of the chain here, over
// ONE bounded text rendering of each body — enrichment, metric observation
// and the rules all read the same view, in the chain's order, so the same
// config selects identically however the line arrived. The chain's two
// standing subtleties hold: METRICS OBSERVE EVERY RECORD (before the rules)
// and RULES RUN AFTER ENRICHMENT (__severity__ selects on the enriched
// severity, falling back to the OTLP severity-number band for the
// SDK-legal number-only shape — logchain.RecordSeverity).
//
// This is deliberately NOT logchain.Chain: Chain BUILDS records into a
// producer's group, while ingested records already exist in the sender's own
// grouping — the move here is observe-and-filter-in-place. What is reused is
// everything that decides semantics: the Resolver, RecordSeverity, the
// LineFilter, DynamicMetricSet.Bind and Prune.
//
// A dropped record is still ACKED to the sender: it was delivered, the
// operator chose to drop it (obs.LogRulesDropped, the producers' counter).
//
// UNAUTHENTICATED-SENDER BOUNDS (each measured in the adversarial review,
// each counted into obs.IngestChainSkipped — the records themselves are
// always still forwarded):
//
//   - maxChainBodyBytes: a structured body's AsString rendering costs 14-18x
//     its wire bytes in transient heap (a 16 MiB map body is ~230 MB live,
//     ~40 KiB on the wire after gzip), and rule regexes cost 40-70 ms/MiB of
//     text. Bodies whose text view exceeds the bound skip line-derived
//     processing — the tailer never feeds lines past -logs-max-entry-bytes
//     either — while attribute/severity-keyed rules still apply.
//   - maxObservedResourceAttrs: the metric store retains a serialization of
//     the WHOLE resource per admitted series, so sender-chosen attribute
//     width is sender-chosen retained heap (measured: 4000-attr resources
//     filling a 10k-series cap retain ~2.2 GB for maxAge, bought with
//     ~4.6 MiB of pushes, with the serialization CPU spent inside the mutex
//     the tailer's flush also takes). Real SDK resources carry a few dozen
//     attributes at most.
//   - maxObservedResources: bounds how many resources of ONE push are
//     observed into the metric set. It slows — it cannot stop — a sender
//     minting distinct resources to latch maxCardinality (the cap counts
//     series and holds them for maxAge); the honest boundary here is
//     visibility, and the store's own DroppedCapped counter plus this one
//     are the alert surface.
import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
	"github.com/JohanLindvall/kubescrape/internal/agent/logenrich"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

const (
	maxChainBodyBytes        = 1 << 20
	maxObservedResourceAttrs = 64
	maxObservedResources     = 256
)

// applyLogChain enriches, observes and filters an ingested payload in place,
// reporting whether anything is left to forward — false means the push is
// acked without a send, exactly as a producer commits an all-dropped batch.
func (s *Server) applyLogChain(ld plog.Logs) bool {
	enrich := s.cfg.Enricher.LinesEnabled()
	if !enrich && s.cfg.Rules == nil && s.cfg.LogMetrics == nil {
		return ld.ResourceLogs().Len() > 0
	}
	var resolver *logchain.Resolver
	if s.cfg.Rules != nil || s.cfg.LogMetrics != nil {
		resolver = logchain.New()
	}
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		rattrs := rl.Resource().Attributes()
		observe := s.cfg.LogMetrics != nil
		if observe && i >= maxObservedResources {
			obs.IngestChainSkipped.WithLabelValues("resources_capped").Inc()
			observe = false
		}
		if observe && rattrs.Len() > maxObservedResourceAttrs {
			obs.IngestChainSkipped.WithLabelValues("resource_too_wide").Inc()
			observe = false
		}
		if observe {
			// The store's order-independent sum-fold hash needs KEY-UNIQUE
			// attributes to be an identity (see dedupeResourceKeys); only
			// ingested resources can violate that.
			dedupeResourceKeys(rattrs)
		}
		// Bound once per resource: Bind hashes the attribute set, and a push
		// groups many records under few resources.
		var bm metrics.BoundResource
		bound := false
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			sls.At(j).LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
				body, ok := chainBody(lr)
				if !ok {
					obs.IngestChainSkipped.WithLabelValues("body_too_large").Inc()
				}
				if enrich && ok {
					logenrich.ApplyBodyText(lr, body)
				}
				if observe {
					if !bound {
						bm = s.cfg.LogMetrics.Bind(rattrs)
						bound = true
					}
					// Severity empty, as in logchain.Chain.Emit: __severity__
					// is a RULE key, and a label resolver must not invent one.
					// A capped body observes as "": attribute-keyed labels
					// and values still resolve; the record still counts.
					resolver.Set(lr.Attributes(), rattrs, "")
					bm.Add(resolver.ValueFn(), resolver.LabelFn(), body)
				}
				if s.cfg.Rules == nil {
					return false
				}
				resolver.Set(lr.Attributes(), rattrs, logchain.RecordSeverity(lr))
				if s.cfg.Rules.Keep(resolver.RuleFn(), body) {
					return false
				}
				obs.LogRulesDropped.Inc()
				return true
			})
		}
	}
	logchain.Prune(ld)
	return ld.ResourceLogs().Len() > 0
}

// chainBody renders the ONE text view of a body that enrichment, log-metrics
// and the rules all share. A string body is used as-is; a structured body
// (map/slice — an SDK's legal shape) renders through AsString, which is the
// text the tailer would have seen for the equivalent logged line. false means
// the body exceeds maxChainBodyBytes and line-derived processing is skipped —
// the size of a STRUCTURED body is estimated by a materialization-free walk,
// because rendering first and then measuring is the exact amplification the
// bound exists to prevent.
func chainBody(lr plog.LogRecord) (string, bool) {
	b := lr.Body()
	if b.Type() == pcommon.ValueTypeStr {
		s := b.Str()
		if len(s) > maxChainBodyBytes {
			return "", false
		}
		return s, true
	}
	rem := maxChainBodyBytes
	if renderedSizeOver(b, &rem, 0) {
		return "", false
	}
	return b.AsString(), true
}

// renderedSizeOver estimates the AsString rendering size of a value tree,
// aborting as soon as the budget is spent. Depth-bounded like scrubValue:
// bodies come from unauthenticated senders.
func renderedSizeOver(v pcommon.Value, rem *int, depth int) bool {
	if depth > maxBodyScrubDepth {
		*rem -= 16
		return *rem < 0
	}
	switch v.Type() {
	case pcommon.ValueTypeStr:
		*rem -= len(v.Str()) + 2
	case pcommon.ValueTypeBytes:
		*rem -= v.Bytes().Len()*4/3 + 2
	case pcommon.ValueTypeMap:
		over := false
		v.Map().Range(func(k string, mv pcommon.Value) bool {
			*rem -= len(k) + 4
			over = renderedSizeOver(mv, rem, depth+1)
			return !over
		})
		if over {
			return true
		}
	case pcommon.ValueTypeSlice:
		sl := v.Slice()
		for i := 0; i < sl.Len(); i++ {
			*rem -= 2
			if renderedSizeOver(sl.At(i), rem, depth+1) {
				return true
			}
		}
	default:
		*rem -= 24 // numbers, bools, empty
	}
	return *rem < 0
}

// dedupeResourceKeys rewrites a resource whose attributes repeat a key. OTLP
// encodes attributes as a repeated KeyValue and pdata does not dedupe on
// decode; the metric store's order-independent SUM-fold hashes every entry
// while its rendered identity applies last-wins, so {k=p, k=q} and
// {k=q, k=p} hash identical (two senders' series MERGE) while {k=v, k=v}
// hashes distinct from {k=v} yet renders the same (byte-identical duplicate
// points in one exported payload — invalid for a Prometheus-shaped backend).
// Every agent-built resource comes from a map and cannot repeat a key, so
// the fix lives at this boundary instead of taxing every producer's hot
// path. Last-wins, which is what any map-shaped consumer of the forwarded
// payload reads anyway; the rewrite is visible only as attribute order.
func dedupeResourceKeys(m pcommon.Map) {
	var seen [maxObservedResourceAttrs]string
	n, dup := 0, false
	m.Range(func(k string, _ pcommon.Value) bool {
		for i := 0; i < n; i++ {
			if seen[i] == k {
				dup = true
				return false
			}
		}
		if n < len(seen) {
			seen[n] = k
			n++
		}
		return true
	})
	if !dup {
		return
	}
	// AsRaw builds a Go map (later entries overwrite — last-wins), FromRaw
	// rebuilds the attribute list from it. Boxing the tree is acceptable on
	// this path: it only runs for a payload that actually repeated a key.
	_ = m.FromRaw(m.AsRaw())
}

// admitLogs/admitMetrics/admitTraces apply the operator's ingest admission
// hook (ServerConfig.Admit — the transforms file's ingest: section) per
// pushed RESOURCE, before enrichment: a rejected resource is removed and
// counted, the push is still acked, and an emptied payload acks without a
// send. Before enrichment deliberately — a rejected sender must not spend a
// metadata lookup per resource on its way out (the same argument as
// RejectTraces).
func (s *Server) admitLogs(ld plog.Logs) {
	if s.cfg.Admit == nil {
		return
	}
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		if s.cfg.Admit(rl.Resource().Attributes()) {
			return false
		}
		obs.IngestAdmissionRejected.Inc()
		return true
	})
}

func (s *Server) admitMetrics(md pmetric.Metrics) {
	if s.cfg.Admit == nil {
		return
	}
	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		if s.cfg.Admit(rm.Resource().Attributes()) {
			return false
		}
		obs.IngestAdmissionRejected.Inc()
		return true
	})
}

func (s *Server) admitTraces(td ptrace.Traces) {
	if s.cfg.Admit == nil {
		return
	}
	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		if s.cfg.Admit(rs.Resource().Attributes()) {
			return false
		}
		obs.IngestAdmissionRejected.Inc()
		return true
	})
}
