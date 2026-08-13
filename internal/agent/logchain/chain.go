package logchain

// The per-record chain itself.
//
// Four producers — the tailer, journald, the Kubernetes-events reader and the
// Azure-diagnostics reader — ran the identical sequence over every record:
//
//	scrub -> lift line attributes -> stamp -> enrich -> log-metrics -> rules
//
// in that order, with the same three subtleties each time (metrics observe
// EVERY record, rules run AFTER enrichment so they see the enriched severity,
// and a dropped record still advances the producer's offsets). Two of the four
// carried comments saying "the tailer does the same". They had already drifted:
// per-pod annotation rules and the lifted-attribute ranking (SetLifted) existed
// only in the tailer's copy, so the identical logAttributes + logs.rules config
// selected differently depending on which pipeline carried the line — which is
// the exact class of bug this package was created to end for the key resolver.
//
// What stays with the producer, because the four are genuinely four things:
// building the RESOURCE (a file, a systemd unit, an event's involved object, an
// ARM resource), grouping records into it, and the offset/cursor/position
// bookkeeping that decides when a batch may be committed.
//
// Allocation discipline: the tailer's flush path is bench-pinned, so nothing
// here allocates per record. The producer is an INTERFACE rather than a pair of
// closures (a closure per record is two allocations), the resolver's closures
// stay bound for the whole flush, and the bound-metric cache is keyed by a type
// parameter rather than `any` so the tailer's *file key is not boxed.

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/logenrich"
	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

// Config is the part of a producer's configuration the chain runs. Every field
// is optional; a zero Config emits records unchanged.
type Config struct {
	// Scrub redacts secrets from the BODY, and runs first: everything
	// downstream copies from the body — logattrs lifts fields out of it,
	// enrich slices exception.stacktrace out of it, log-metrics extract label
	// values from it — so a secret must not survive into any of them.
	//
	// nil is legitimate: two producers (journald, the events reader) scrub
	// where they build their batch entry, before the record exists.
	Scrub *logscrub.Scrubber
	// LogAttrs lifts configured keys off the line. The RESOURCE and SCOPE
	// halves of the result drive the producer's grouping, so Line hands them
	// back rather than applying them here.
	LogAttrs *logattrs.Extractor
	// Enrich parses metadata out of the line (timestamp, severity, trace ids,
	// exception fields) onto the record.
	Enrich bool
	// LogMetrics observes EVERY record, kept or dropped: a metric counting
	// errors must not fall to zero because a rule stopped shipping the lines.
	// Once per RECORD, though, not once per export attempt — see Input.Observed.
	// Per record rather than per delivery is the exact claim: a record withheld by
	// a commit clamp and then rewound is DELIVERED twice and observed once.
	LogMetrics *metrics.DynamicMetricSet
	// Rules is the global ordered keep/drop/sample chain, evaluated last.
	Rules *logline.LineFilter
}

// Producer is the half of record building that stays with the pipeline.
type Producer interface {
	// Dest returns the record slice a KEPT record lands in. It is called at
	// most ONCE per Emit, and only when the record is kept — so a producer
	// that groups lazily (the tailer, whose metric/rule resolution uses the
	// FILE's resource rather than the group's) never materialises a
	// ResourceLogs for a record the rules drop. Producers whose resolution
	// needs the group's own resource create it before Emit and prune the empty
	// groups afterwards; the resulting payload is the same.
	Dest() plog.LogRecordSlice
	// Stamp fills in everything the producer knows about the record:
	// timestamps, body, severity and its own record attributes. Called before
	// the line attributes, enrichment, metrics and rules.
	Stamp(plog.LogRecord)
}

// Input is one record's varying half.
//
// K is the key of the bound-metric cache: a resource-identifying value the
// producer already has (the tailer's *file, everyone else's group key). It is a
// type parameter rather than `any` because boxing a string key into an
// interface allocates, once per record, on a path that is pinned at zero.
type Input[K comparable] struct {
	// Body is the line, already scrubbed (Line does that).
	Body string
	// Lifted is Line's extraction result. The Log half goes on the record
	// here; the Resource half also ranks between record and resource
	// attributes for metric labels and rule keys (see Resolver.SetLifted).
	Lifted logattrs.Result
	// Resource is what metric labels and rule keys resolve against after the
	// record's own attributes, and what log-metrics bind to.
	Resource pcommon.Map
	// BoundKey identifies Resource for the bind cache (Bind hashes the
	// resource, so it is paid once per group per flush rather than per record).
	BoundKey K
	// PodRules are this record's own rules, evaluated BEFORE the global chain:
	// a pod drop is final, a pod keep still passes the global rules. Only the
	// tailer has them (pod annotation config); nil everywhere else.
	PodRules *logline.LineFilter
	// Observed says an earlier pass over the same bytes already applied this
	// record's observations, so this pass must not repeat them. The record is
	// still built and the rules still RUN — their verdict decides whether it
	// ships, and it is free to differ from the earlier pass's (a hot-reloaded
	// rule set, or a `sample` rule, whose verdict is a counter rather than a
	// function of the bytes); only the counting is suppressed, with the
	// consequences for the drop tally spelled out where Emit counts it.
	//
	// It exists because delivery is at-least-once and observation is not.
	// Producers that rebuild a failed batch from source — the tailer rewinds
	// its files and re-reads them next sweep — would otherwise multiply every
	// user-configured counter and histogram by the number of attempts an outage
	// spanned, biasing cumulative series upward for good and spiking rate()
	// during exactly the outage the operator is reading them to diagnose.
	// (journald takes the other route: it retries the same batch in place, so
	// nothing is rebuilt and nothing sets this.) The producer owns the proof —
	// for the tailer, the exact identity (segment-qualified byte range plus a
	// hash of the joined body) of every entry a rewind can bring back.
	//
	// WHAT IS GATED is every counter whose unit is a RECORD, wherever the
	// increment physically lives:
	//
	//   - Config.LogMetrics.Add, the operator's own counters/histograms. These
	//     are the ones that matter most: they are cumulative, someone alerts on
	//     them, and nothing downstream can undo a double count.
	//   - obs.LogRulesDropped, the tally of records the keep/drop chain refused.
	//   - obs.LogEnriched and obs.LogEnrichTimeRejected, a package away in
	//     logenrich — reached through ApplyUncounted rather than Apply, which
	//     is why Emit branches on this field around one call.
	//
	// The enrichment pair is worth the extra entry point because it is read
	// AGAINST a delivery count: sum(kubescrape_log_enriched_total) is the
	// decomposition of kubescrape_log_entries_total by parse format, and the two
	// agree to the record whenever nothing is failing. Counting passes in the
	// numerator and deliveries in the denominator turns "what fraction of my
	// lines are JSON" into a multiple of itself during exactly the outage
	// someone is reading it to diagnose — 32,245 against 277 entries, 116x,
	// measured over a three-minute collector outage — and skews the format MIX
	// too, since the extra passes sample the rewinding batch, not the stream.
	//
	// NOT gated, and the rule rather than the list is what to remember —
	// anything counted where this field cannot reach still multiplies by the
	// number of passes a rewind spans:
	//
	//   - kubescrape_log_scrubbed_total, bumped inside logscrub.Scrub from
	//     Chain.Line — 6 for 3 delivered records across one rewind. Redaction
	//     has to precede grouping, so it happens in Line, which takes a body
	//     rather than an Input and so never sees this field. Same shape as the
	//     enrichment pair and the same fix: carry the flag into Line and give
	//     logscrub an uncounted entry point beside the counting one.
	//   - the producer's READ side, before a record exists and so before this
	//     chain can be told anything: for the tailer,
	//     kubescrape_log_rate_limited_total (both label values, in consume),
	//     kubescrape_log_bytes_total and kubescrape_log_oversized_dropped_total.
	//     These count passes over BYTES, which is honestly what they measure —
	//     a rewind really does re-read them — so there is nothing here to fix.
	//
	// Under-claiming is safe (a re-observation, i.e. the behaviour before this
	// existed); over-claiming loses observations outright, so a producer must
	// only set it where the bytes provably went through the chain before.
	Observed bool
}

// Chain holds the per-flush state: the key resolver with its closures bound
// once, the one-record scratch slice, and the bound log-metric handles.
type Chain[K comparable] struct {
	cfg      Config
	resolver *Resolver
	scratch  plog.LogRecordSlice
	bound    map[K]metrics.BoundResource
	// rules is true when SOME record in this batch may be dropped, which is
	// what decides whether records are built in the scratch slice at all.
	rules bool
}

// NewChain prepares one flush's chain state.
//
// perRecordRules says that some record in this batch may carry its OWN rule set
// (Input.PodRules). It is a constructor argument rather than a per-record
// discovery because the scratch slice and the resolver must exist before the
// first record, and answering it costs the caller one pass over its files. It
// is a PROMISE, not a hard precondition: a record arriving with PodRules
// despite perRecordRules=false upgrades the chain lazily in Emit (one
// allocation, on that unpinned path only) — the promise is kept by an ordering
// argument a package away (the tailer's anyPodRules pass over t.files), and a
// drifted caller must degrade to a small cost, not to a nil-slice panic on the
// sweep goroutine that serves every log file on the node.
func NewChain[K comparable](cfg Config, perRecordRules bool) *Chain[K] {
	c := &Chain[K]{cfg: cfg, rules: cfg.Rules != nil || perRecordRules}
	if c.cfg.LogMetrics != nil || c.rules {
		c.resolver = New()
	}
	if c.cfg.LogMetrics != nil {
		c.bound = make(map[K]metrics.BoundResource, 4)
	}
	if c.rules {
		// With rules active, records are built in a one-record scratch slice
		// and only MOVED into the batch when kept, so a drop never materialises
		// a record in the payload. Without rules they are built in place.
		c.scratch = plog.NewLogRecordSlice()
	}
	return c
}

// Line runs the two steps that must precede GROUPING: scrubbing the body and
// extracting the line's configured attributes. The caller needs the result's
// Resource and Scope halves to pick or build the group a record belongs to, so
// they cannot happen inside Emit.
func (c *Chain[K]) Line(body string) (string, logattrs.Result) {
	if c.cfg.Scrub != nil {
		body = c.cfg.Scrub.Scrub(body)
	}
	var extracted logattrs.Result
	if c.cfg.LogAttrs != nil {
		extracted = c.cfg.LogAttrs.Extract(body)
	}
	return body, extracted
}

// Emit builds one record and reports whether it was KEPT. A false return means
// the rules dropped it: the producer must still advance its offsets/cursor, and
// the drop is already counted (obs.LogRulesDropped — unless Input.Observed says
// an earlier pass over the same bytes already counted it).
func (c *Chain[K]) Emit(p Producer, in Input[K]) bool {
	scratched := c.cfg.Rules != nil || in.PodRules != nil
	if scratched && !c.rules {
		// The perRecordRules promise was violated (see NewChain): upgrade
		// instead of hitting the zero-value scratch slice.
		c.rules = true
		c.scratch = plog.NewLogRecordSlice()
		if c.resolver == nil {
			c.resolver = New()
		}
	}
	var lr plog.LogRecord
	if scratched {
		lr = c.scratch.AppendEmpty()
	} else {
		lr = p.Dest().AppendEmpty()
	}
	p.Stamp(lr)
	logattrs.Put(lr.Attributes(), in.Lifted.Log)
	if c.cfg.Enrich {
		if in.Observed {
			// Same enrichment, no counters: obs.LogEnriched and
			// obs.LogEnrichTimeRejected are per-RECORD outcomes, and this
			// record's were tallied by the pass a rewind brought it back from.
			logenrich.ApplyUncounted(lr, in.Body)
		} else {
			logenrich.Apply(lr, in.Body)
		}
	}
	if c.cfg.LogMetrics != nil && !in.Observed {
		// Metric label/value keys resolve against the record's attributes
		// (line-derived + enriched) first, then this line's lifted resource
		// attributes, then the resource; the resource itself becomes the
		// metric's OTLP resource (hashed once per group via Bind).
		//
		// The severity is deliberately left as it was: __severity__ is a RULE
		// key only, and a resolver used for labels must not invent one.
		bm, ok := c.bound[in.BoundKey]
		if !ok {
			bm = c.cfg.LogMetrics.Bind(in.Resource)
			c.bound[in.BoundKey] = bm
		}
		c.resolver.Set(lr.Attributes(), in.Resource, c.resolver.Severity)
		c.resolver.SetLifted(in.Lifted.Resource)
		bm.Add(c.resolver.ValueFn(), c.resolver.LabelFn(), in.Body)
	}
	if !scratched {
		return true
	}
	// Rules run AFTER enrichment, so __severity__ selects on the ENRICHED
	// severity rather than whatever the producer stamped.
	c.resolver.Set(lr.Attributes(), in.Resource, RecordSeverity(lr))
	// This line's lifted resource attributes rank between the record's and the
	// resource's. Without it the same logAttributes + rules config selected
	// differently depending on which pipeline carried the line — the tailer was
	// the only producer that did this.
	c.resolver.SetLifted(in.Lifted.Resource)
	keep := in.PodRules == nil || in.PodRules.Keep(c.resolver.RuleFn(), in.Body)
	if keep && c.cfg.Rules != nil {
		keep = c.cfg.Rules.Keep(c.resolver.RuleFn(), in.Body)
	}
	if keep {
		c.scratch.MoveAndAppendTo(p.Dest())
		return true
	}
	c.scratch.RemoveIf(func(plog.LogRecord) bool { return true })
	// Counted for the pass that FIRST put these bytes through the chain, never
	// for a re-read of them. So a record whose VERDICT differs between the two
	// passes is attributed to the wrong one: kept on pass one and dropped on
	// pass two is not counted at all, dropped on pass one and kept on pass two
	// is counted although it shipped.
	//
	// TWO things flip a verdict, and only one of them is rare. A hot-reloaded
	// rule set landing inside a rewind window is the rare one. The other is a
	// `sample` keep rule, which needs no reload and skews on EVERY rewind: a
	// sample verdict is not a function of the bytes at all but of a per-rule
	// atomic counter (internal/logline: `(picked.Add(1)-1)%every == 0`), which
	// the rewound pass already advanced, so the re-read keeps a DIFFERENT
	// subset — and every drop in it is suppressed here. Feeding the same three
	// matching lines through a `sample: 0.5` rule twice keeps lines 1 and 3,
	// then line 2.
	//
	// It is still the right direction, because this error does not grow with
	// the outage while the alternative does. Over one batch of N matching
	// lines a sample rule keeps floor(N/every) or ceil(N/every) of them
	// whichever pass runs, so what is counted (the first pass's drops) and what
	// actually shipped (the last pass's) differ by the phase alone — one record
	// in the example above, and never by more than that batch however many
	// attempts the outage spans, because only the first pass counts at all.
	// Counting every pass instead multiplies both this tally and the operator's
	// own log metrics by the number of attempts: wrong for every record, and
	// worst for exactly the config that drops the most of them.
	if !in.Observed {
		obs.LogRulesDropped.Inc()
	}
	return false
}

// Prune drops the ResourceLogs and ScopeLogs a batch left empty. Producers that
// build their group BEFORE the record (because metric and rule resolution needs
// the group's own resource) end up with record-less groups when the rules drop
// everything in one; the payload must not carry them.
func Prune(ld plog.Logs) {
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool { return sl.LogRecords().Len() == 0 })
		return rl.ScopeLogs().Len() == 0
	})
}
