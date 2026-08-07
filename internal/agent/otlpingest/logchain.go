package otlpingest

// The logs.rules / logMetrics half of the ingest LOG path.
//
// The four log PRODUCERS run the shared chain (internal/agent/logchain):
// scrub → lift → enrich → log-metrics → rules. Ingest already had the first
// half — the Enricher scrubs structured bodies and applies the non-overwrite
// body enrichment — but not the second, so an operator's cost levers stopped
// at the pushed-OTLP boundary: a metrics.json-style logMetrics rule could
// count a tailed error line but not the same line pushed by an SDK, and
// logs.rules could not drop it. applyLogChain closes that, with the chain's
// two standing subtleties intact: METRICS OBSERVE EVERY RECORD (before the
// rules — a metric counting errors must not fall to zero because a rule
// stopped shipping the lines) and RULES RUN AFTER ENRICHMENT (__severity__
// selects on the enriched severity).
//
// This is deliberately NOT logchain.Chain: Chain BUILDS records into a
// producer's group, while ingested records already exist in the sender's own
// grouping — the move here is observe-and-filter-in-place. What is reused is
// everything that decides semantics: the Resolver (record attrs → resource,
// __severity__), LowerSeverity, the LineFilter, DynamicMetricSet.Bind and
// Prune — so the same config selects identically however the line arrived.
//
// A dropped record is still ACKED to the sender: it was delivered, the
// operator chose to drop it (obs.LogRulesDropped, the producers' counter).
// The handlers run concurrently under the in-flight bound; the metric set's
// store is internally locked (journald and the tailer already share one
// across goroutines) and the Resolver is per call.

import (
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// applyLogChain observes and filters an enriched ingested payload in place,
// reporting whether anything is left to forward — false means the push is
// acked without a send, exactly as a producer commits an all-dropped batch.
func (s *Server) applyLogChain(ld plog.Logs) bool {
	if s.cfg.Rules == nil && s.cfg.LogMetrics == nil {
		return ld.ResourceLogs().Len() > 0
	}
	resolver := logchain.New()
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		rattrs := rl.Resource().Attributes()
		// Bound once per resource: Bind hashes the attribute set, and a push
		// groups many records under few resources.
		var bm metrics.BoundResource
		bound := false
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			sls.At(j).LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
				body := lr.Body().AsString()
				if s.cfg.LogMetrics != nil {
					if !bound {
						bm = s.cfg.LogMetrics.Bind(rattrs)
						bound = true
					}
					// Severity empty, as in logchain.Chain.Emit: __severity__
					// is a RULE key, and a label resolver must not invent one.
					resolver.Set(lr.Attributes(), rattrs, "")
					bm.Add(resolver.ValueFn(), resolver.LabelFn(), body)
				}
				if s.cfg.Rules == nil {
					return false
				}
				resolver.Set(lr.Attributes(), rattrs, logchain.LowerSeverity(lr.SeverityText()))
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
