package otlpingest

// The logs.rules / logMetrics / line-enrichment half of the ingest LOG path.
//
// The four log PRODUCERS run the shared chain (internal/agent/logchain):
// scrub → lift → enrich → log-metrics → rules. Ingest scrubs in EnrichLogs
// (before anything reads the body) and runs the REST of the chain here, over
// ONE bounded text rendering of each body — the lift, enrichment, metric
// observation and the rules all read the same view, in the chain's order, so
// the same config selects identically however the line arrived. The LIFT is
// the newest of those and was missing for a while: without it a logAttributes
// rule that renamed a key made one logs.rules config drop a tailed line and
// ship the identical pushed one — and, under an allowlist ruleset, discard the
// pushed one the tailer keeps. The chain's two
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
// operator chose to drop it (obs.LogRulesDropped, the producers' counter) —
// and the tally is applied when the push is ACKED, not when the chain runs,
// because a NACKed push is retransmitted and re-chained (chainCommit). The
// log-METRIC observation is the one side effect that stays per receive
// ATTEMPT; chainCommit says why, and it is the only such place in this repo.
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
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
	"github.com/JohanLindvall/kubescrape/internal/agent/logenrich"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

const (
	maxChainBodyBytes        = 1 << 20
	maxObservedResourceAttrs = 64
	maxObservedResources     = 256
)

// chainCommit holds the side effects of one push's chain that may only be
// applied once the receiver knows the payload was DELIVERED.
//
// The receiver forwards after the chain runs, and a transient forward failure
// is answered retryably (503 / Unavailable) precisely so the sender resends the
// identical bytes — which re-runs the whole chain. Counting inside the chain
// therefore multiplied kubescrape_log_rules_dropped_total by the number of
// delivery attempts an outage spanned, for records that were never delivered
// once: measured 5 drops counted for 1 dropped record over a 5-attempt SDK
// retry policy. The metric's own registered description says the opposite
// ("Counted ONCE PER RECORD, not once per attempt") and names -ingest among the
// paths, and the repo already holds this line elsewhere — journald holds its
// batch rather than rebuilding it, spanmetrics' tap "forwards FIRST and
// aggregates only on success".
//
// So the drop tally is staged here and committed by the caller after the
// forward returns nil (or after an all-dropped payload is acked without a send,
// which is a delivery as far as the sender is concerned). What is NOT staged is
// the log-METRIC observation: the observations are lazy — the value and each
// label are resolved per rule, inside the metric store, off the record's
// attributes and the line — so deferring them means either snapshotting a
// resolved label set per record per rule (an allocation per record on the
// receive path) or re-reading the record after the export, which is not the
// same record: the forward is marked transform.Handoff and a script may have
// mutated the payload in place, and a rules-dropped record is not in it at all.
// Log-derived metrics on the INGEST path are therefore observed once per
// RECEIVE ATTEMPT, not once per delivery — the one place in this repo where
// that is true, and the ingest bullet in the docs says so.
type chainCommit struct{ dropped int64 }

// commit applies the staged tallies. Called once the push is acked.
func (c chainCommit) commit() {
	if c.dropped > 0 {
		obs.LogRulesDropped.Add(float64(c.dropped))
	}
}

// applyLogChain enriches, observes and filters an ingested payload in place. It
// reports whether anything is left to forward — false means the push is acked
// without a send, exactly as a producer commits an all-dropped batch — plus the
// side effects the caller commits once the payload is delivered (chainCommit).
func (s *Server) applyLogChain(ld plog.Logs) (chainCommit, bool) {
	var cc chainCommit
	enrich := s.cfg.Enricher.LinesEnabled()
	if !enrich && s.cfg.Rules == nil && s.cfg.LogMetrics == nil && s.cfg.LogAttrs == nil {
		return cc, ld.ResourceLogs().Len() > 0
	}
	var resolver *logchain.Resolver
	if s.cfg.Rules != nil || s.cfg.LogMetrics != nil {
		resolver = logchain.New()
	}
	// Per-push tallies for the one line the skips get (noteChainSkipped). They
	// are ints on the stack, incremented beside the counters; the reporting
	// happens once, after the loop, because these bounds bind for whole
	// PUSHES — a per-resource or per-record line would be a flood a sender
	// chooses the volume of.
	var skipped chainSkips
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		rattrs := rl.Resource().Attributes()
		observe := s.cfg.LogMetrics != nil
		if observe && i >= maxObservedResources {
			obs.IngestChainSkipped.WithLabelValues(chainSkipResources).Inc()
			skipped.resources++
			observe = false
		}
		if observe && rattrs.Len() > maxObservedResourceAttrs {
			obs.IngestChainSkipped.WithLabelValues(chainSkipTooWide).Inc()
			skipped.tooWide++
			observe = false
		}
		if resolver != nil {
			// Not for the store's identity — that fold handles a repeated key
			// itself now — but for the resolver reading this resource (Get is
			// FIRST-wins, while the store renders last-wins) and for the
			// payload forwarded on. Independent of `observe`: a rules-only
			// config (no LogMetrics), or a resource past the observe cap/width,
			// still reads the resource through the resolver and still forwards
			// it, so gating the dedupe on the metric-observation eligibility
			// let a drop rule evaluate against the wrong value. See
			// dedupeResourceKeys.
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
					obs.IngestChainSkipped.WithLabelValues(chainSkipBody).Inc()
					skipped.bodies++
				}
				// LIFT, before enrichment and before anything selects on the
				// record: the producers' chain is scrub -> lift -> enrich ->
				// log-metrics -> rules, and this step was missing here, so a
				// logAttributes rule that RENAMES a key (`attribute:` !=
				// `key:`, the documented canonical use) made the same
				// logs.rules / logMetrics config select differently for a
				// pushed line than for the identical tailed one — an allowlist
				// ruleset silently DISCARDED pushed records the tailer keeps.
				var lifted logattrs.Result
				if s.cfg.LogAttrs != nil && ok {
					lifted = s.cfg.LogAttrs.Extract(body)
					logattrs.Put(lr.Attributes(), lifted.Log)
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
					// Set clears the lifted set, so this follows every Set.
					// The line's RESOURCE-target attributes rank between the
					// record's and the resource's, exactly as in Chain.Emit.
					resolver.SetLifted(lifted.Resource)
					bm.Add(resolver.ValueFn(), resolver.LabelFn(), body)
				}
				if s.cfg.Rules == nil {
					return false
				}
				resolver.Set(lr.Attributes(), rattrs, logchain.RecordSeverity(lr))
				resolver.SetLifted(lifted.Resource)
				if s.cfg.Rules.Keep(resolver.RuleFn(), body) {
					return false
				}
				// Staged, not counted: see chainCommit.
				cc.dropped++
				return true
			})
		}
	}
	logchain.Prune(ld)
	s.noteChainSkipped(skipped)
	return cc, ld.ResourceLogs().Len() > 0
}

// The reason label of obs.IngestChainSkipped, and the keys the skip warning
// throttles on. Named constants because the counter and the line must agree:
// an operator reading the metric's reason and grepping for it has to find the
// line that explains it.
const (
	chainSkipResources = "resources_capped"
	chainSkipTooWide   = "resource_too_wide"
	chainSkipBody      = "body_too_large"
)

// chainSkips tallies one push's line-derived-processing skips.
type chainSkips struct {
	resources int
	tooWide   int
	bodies    int
}

// chainSkipWarnEvery paces the skip warning per reason. Each bound is a
// property of how the SENDER batches, so it binds on every push until the
// sender changes.
const chainSkipWarnEvery = time.Minute

// noteChainSkipped narrates the abuse bounds that silently degrade a pushed
// payload. The data is still forwarded, which is exactly why this is easy to
// miss: nothing is dropped, nothing 429s, the sender sees success — but the
// records skipped here were NOT observed into logMetrics and NOT evaluated
// against logs.rules, so a metric silently under-counts and a drop rule
// silently fails to fire, for pushed lines only. The counter carries the rate;
// this says which bound and how far past it the sender is.
func (s *Server) noteChainSkipped(sk chainSkips) {
	if sk.resources == 0 && sk.tooWide == 0 && sk.bodies == 0 {
		return
	}
	warn := func(reason, msg string, args ...any) {
		if allow, _ := s.chainSkipWarns.Allow(reason); !allow {
			return
		}
		s.log.Warn(msg, append([]any{"reason", reason}, args...)...)
	}
	if sk.resources > 0 {
		warn(chainSkipResources,
			"ingest: a push carried more resources than log-derived metrics observe, so the remainder was "+
				"forwarded WITHOUT being observed or rule-evaluated; have the sender batch fewer resources",
			"resources", sk.resources, "maxResources", maxObservedResources)
	}
	if sk.tooWide > 0 {
		warn(chainSkipTooWide,
			"ingest: a pushed resource declares more attributes than log-derived metrics will retain, so its "+
				"records were forwarded WITHOUT being observed or rule-evaluated (the store retains a "+
				"serialization of the whole resource per series, so its width is retained heap)",
			"resources", sk.tooWide, "maxAttributes", maxObservedResourceAttrs)
	}
	if sk.bodies > 0 {
		warn(chainSkipBody,
			"ingest: pushed log bodies exceeded the size line-derived processing will render, so they were "+
				"forwarded WITHOUT enrichment, log-metric observation or rule evaluation",
			"records", sk.bodies, "maxBytes", maxChainBodyBytes)
	}
}

// chainBody renders the ONE text view of a body that enrichment, log-metrics
// and the rules all share. A string body is used as-is; a structured body
// (map/slice — an SDK's legal shape) renders through AsString, which is the
// text the tailer would have seen for the equivalent logged line. false means
// the body exceeds maxChainBodyBytes — or nests past the depth the estimate
// walks, which is the same thing since an unmeasured subtree cannot be shown
// to fit — and line-derived processing is skipped. The size of a STRUCTURED
// body is estimated by a materialization-free walk, because rendering first
// and then measuring is the exact amplification the bound exists to prevent.
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

// escapedLen is what a string COSTS once AsString renders it as JSON, which is
// what the estimate has to charge: the raw length is not an upper bound on the
// rendered one, and charging len() let a body estimate just under the budget
// and materialise up to 6x past it — on an unauthenticated path, and against a
// guard whose whole purpose is to prevent exactly that amplification.
//
// It MODELS encoding/json (reached through pcommon.Value.AsString ->
// marshalJSONNoHTMLEscape) rather than measuring it, because measuring means
// rendering, and AsString renders by boxing the whole tree through AsRaw first
// — which is the amplification, not a way to avoid it. So the model is pinned
// to the renderer by test instead (TestEscapedLenNeverUnderchargesTheRenderer),
// measured per 100-byte input on Go 1.26.5 / pdata v1.65:
//
//	plain, <, DEL, é, emoji      1.0x   (nothing escapes; HTML escaping is OFF)
//	" and \                      2.0x   -> \" \\
//	\n \r \t                      2.0x   -> the two-byte escapes
//	other bytes < 0x20           6.0x   -> \u00XX
//	INVALID UTF-8, per byte      6.0x   -> the literal \ufffd escape
//	U+2028 / U+2029 (3 bytes)    2.0x   -> \u2028 (JS line terminators)
//
// The last two were missing, and they are the ones an attacker reaches for: a
// megabyte of 0xFF estimated at 1x and rendered at 6x, i.e. the same 6.00x
// all-NUL case this comment already claimed was fixed. The whole point of the
// function is that it may never UNDER-charge; over-charging (which it no longer
// does for \n/\r/\t) only refuses line-derived processing early, and that is
// counted and still forwards the record.
func escapedLen(s string) int {
	n := len(s)
	for i := 0; i < len(s); {
		if c := s[i]; c < utf8.RuneSelf {
			switch {
			case c == '"' || c == '\\', c == '\n', c == '\r', c == '\t':
				n++ // two-byte escape
			case c < 0x20:
				n += 5 // -> \u00XX
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == utf8.RuneError && size == 1:
			n += 5 // one invalid byte -> the six-byte \ufffd escape
		case r == '\u2028' || r == '\u2029':
			n += 3 // three bytes in, the six-byte \u2028 escape out
		}
		i += size
	}
	return n
}

// renderedSizeOver estimates the AsString rendering size of a value tree,
// aborting as soon as the budget is spent. Depth-bounded like scrubValue:
// bodies come from unauthenticated senders.
func renderedSizeOver(v pcommon.Value, rem *int, depth int) bool {
	if depth > maxBodyScrubDepth {
		// UNMEASURABLE IS OVER BUDGET, never under. Charging a flat cost for an
		// unwalked subtree made the bound unenforceable: ~130 wire bytes of
		// nesting put the whole 16 MiB body below the cut-off, the estimate came
		// back tiny and AsString then materialized it in full — the 14-18x
		// amplification the bound exists to prevent, with body_too_large flat
		// while it happened. It is also the depth scrubValue stops at, so a
		// subtree below it is UNSCRUBBED and must not reach the text view that
		// enrichment, log-metrics (whose labels can lift the raw line) and the
		// rules all read. The cost is that an honest body nested deeper than
		// this skips line-derived processing — counted, and still forwarded.
		*rem = -1
		return true
	}
	switch v.Type() {
	case pcommon.ValueTypeStr:
		*rem -= escapedLen(v.Str()) + 2
	case pcommon.ValueTypeBytes:
		// base64 is 4 bytes per 3, ROUNDED UP with padding: the truncating
		// 4/3 undercharged every value by up to 3 bytes, which is a 2x
		// undercharge for a payload of many tiny byte arrays. Same invariant
		// as escapedLen's — this estimate may never come in low. The +4 is the
		// two quotes, floored at what an EMPTY bytes value actually renders as
		// (AsRaw hands encoding/json a nil slice, which is `null`, not `""`).
		*rem -= (v.Bytes().Len()+2)/3*4 + 4
	case pcommon.ValueTypeMap:
		*rem -= 2 // the braces themselves: an EMPTY map still renders {}
		over := false
		v.Map().Range(func(k string, mv pcommon.Value) bool {
			// A KEY is a JSON string too, and charging len() for it moved the
			// exact undercharge escapedLen exists to prevent into the one
			// position that skipped it: a key of NULs or invalid UTF-8 renders
			// 6x what it was charged. The +4 is the framing ("k": plus a
			// separator).
			*rem -= escapedLen(k) + 4
			over = renderedSizeOver(mv, rem, depth+1)
			return !over
		})
		if over {
			return true
		}
	case pcommon.ValueTypeSlice:
		*rem -= 2 // the brackets, as above
		sl := v.Slice()
		for i := 0; i < sl.Len(); i++ {
			*rem -= 2
			if renderedSizeOver(sl.At(i), rem, depth+1) {
				return true
			}
		}
	default:
		// Numbers, bools, empty. 26 rather than the obvious 24: encoding/json
		// renders a float64 in [1e-6, 1e-5) through its 'f' branch rather than
		// 'e', and a NEGATIVE one there is 25 characters —
		// -0.0000012345678901234567. 24 undercharged that shape by a byte per
		// entry, which a map of such values turns into a ~3% breach of a bound
		// whose whole contract (escapedLen, above) is that it may never
		// UNDER-charge. The two spare bytes are slack, not arithmetic: an
		// int64 is at most 20, a bool 5, empty 4 ("null"), and over-charging
		// only refuses line-derived processing marginally earlier — which is
		// counted, and still forwards the record.
		*rem -= 26
	}
	return *rem < 0
}

// dedupeResourceKeys rewrites a resource whose attributes repeat a key. OTLP
// encodes attributes as a repeated KeyValue and pdata does not dedupe on
// decode, and nothing downstream of here agrees about what such a resource
// MEANS: a Go-map-shaped consumer takes the last entry, pcommon.Map.Get takes
// the first, and this chain reads the resource both ways.
//
// It is NO LONGER what keeps the metric store's identity honest — the store's
// fold proves key-uniqueness itself now (metrics.resourceAccum), so an
// undeduped resource keys as the identity it renders whether or not this ran.
// It stays for the two readers the store cannot speak for:
//
//   - logchain.Resolver resolves a metric LABEL or a rule key off the resource
//     with Get, i.e. FIRST-wins, while the store renders the resource last-wins.
//     Undeduped, a label lifted from the resource would disagree with the
//     resource the series carries.
//   - the payload FORWARDED to the collector, which is passed through
//     untouched and would hand the same ambiguity to whatever receives it.
//
// Every agent-built resource comes from a map and cannot repeat a key, so this
// stays at the boundary rather than taxing every producer's hot path.
// Last-wins, which is what any map-shaped consumer reads anyway and what the
// store's identity applies; the rewrite is visible only as attribute order.
func dedupeResourceKeys(m pcommon.Map) {
	if m.Len() < 2 {
		return
	}
	dup := false
	if m.Len() <= maxObservedResourceAttrs {
		// Fast, allocation-free path for the common case: a fixed scratch, no
		// map. Sound because every key is compared against every earlier one.
		var seen [maxObservedResourceAttrs]string
		n := 0
		m.Range(func(k string, _ pcommon.Value) bool {
			for i := 0; i < n; i++ {
				if seen[i] == k {
					dup = true
					return false
				}
			}
			seen[n] = k
			n++
			return true
		})
	} else {
		// A resource wider than the scratch (rare) falls back to a map so a
		// duplicate among keys past position 64 is still detected — the old
		// fixed-scratch scan silently stopped tracking there.
		seen := make(map[string]struct{}, m.Len())
		m.Range(func(k string, _ pcommon.Value) bool {
			if _, ok := seen[k]; ok {
				dup = true
				return false
			}
			seen[k] = struct{}{}
			return true
		})
	}
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
