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
// the drop is already counted (obs.LogRulesDropped).
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
		logenrich.Apply(lr, in.Body)
	}
	if c.cfg.LogMetrics != nil {
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
	c.resolver.Set(lr.Attributes(), in.Resource, LowerSeverity(lr.SeverityText()))
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
	obs.LogRulesDropped.Inc()
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
