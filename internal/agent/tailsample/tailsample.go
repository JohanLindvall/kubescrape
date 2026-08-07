// Package tailsample is the POLICY ENGINE half of tail sampling: the pure
// decision logic that looks at an ASSEMBLED trace and says keep or drop, and
// which rule said so. It holds no traces, opens no sockets, knows nothing about
// how spans of one trace find each other. Whoever assembles traces (a buffering
// layer, a shard, a collector tier) calls Decide once per trace and acts on the
// verdict — that seam is deliberately the only one, so the two halves can be
// built, tested and reasoned about apart.
//
// # Vocabulary
//
// The policy set and its semantics are the OpenTelemetry Collector's
// tailsamplingprocessor: the same policy types, the same bodies, the same
// meaning for every field. That vocabulary is the de-facto standard an operator
// already knows, so the concepts transfer unchanged.
//
// The SYNTAX is this repo's. Field names and policy type values are camelCase
// (`stringAttribute`, `enabledRegexMatching`, `maxTotalSpansPerSecond`), the way
// every other section of the agent config is spelled, so a Collector policy list
// is TRANSCRIBED rather than pasted: the keys map one for one
// (`string_attribute` -> `stringAttribute`, `threshold_ms: 500` ->
// `threshold: "500ms"`), and a snake_case key left behind is a strict-decode
// failure rather than a silently ignored field.
//
// Durations are STRINGS parsed with time.ParseDuration, never time.Duration
// fields: the agent config decodes through sigs.k8s.io/yaml -> encoding/json,
// which accepts only a raw nanosecond integer for a time.Duration, so the
// documented "500ms" spelling would reject the ENTIRE file (it is
// UnmarshalStrict'ed) and the DaemonSet would not start. servicegraph.Config.Wait,
// spanmetrics.Config.StaleAfter and tracesample.Config.KeepSlowerThan are
// strings for exactly this reason.
//
// # Aggregation: first match wins
//
// Policies are evaluated in configured order and the first one with an OPINION
// ends the evaluation:
//
//   - a policy that MATCHES samples the trace, and is named in the Decision;
//   - an INVERTED policy that matches vetoes the trace (dropped), and is named;
//   - a policy that does not match ABSTAINS, and the next policy is consulted;
//   - no policy with an opinion means not sampled, with an empty Decision.Policy.
//
// The Collector instead evaluates every policy and ORs the results (any Sampled
// wins), with one exception that behaves like a global veto: an inverted policy
// whose match fails returns InvertSampled, which samples the trace outright,
// and whose match succeeds returns InvertNotSampled, which drops it no matter
// what any other policy said. Two deliberate differences follow.
//
// First, ordering is meaningful here and is not in the Collector. That is the
// whole point: an OR has no attribution, and operators need to know WHICH rule
// kept a trace (it is a metric label, see Decision.Policy). Under an OR the
// answer is "some subset of them"; under first-match-wins it is exactly one,
// and the answer is stable because the list order is the operator's own
// statement of precedence.
//
// Second, an inverted policy here VETOES on a match and ABSTAINS otherwise,
// rather than sampling everything it does not match. The Collector's reading
// makes an exclusion rule silently enable 100% sampling for everything else —
// a well-known footgun of that processor — and under first-match-wins it would
// additionally make every policy after it unreachable. Writing exclusions first
// and sampling rules after is then the natural arrangement. An operator who
// genuinely wants the Collector's reading gets it by appending a final
// `- type: alwaysSample` policy, which is one line and says what it means.
//
// A policy never fails the trace. Everything that can fail — parsing, regex
// compilation, rate arithmetic — happens once in New, so by the time Decide
// runs there is no error left to swallow; a policy that cannot form an opinion
// (a missing attribute, a wrongly-typed value, a trace with no timestamps)
// abstains and the next policy decides. That is what the Collector's error
// handling amounts to in the end (it logs and treats the policy as
// not-sampled), minus the per-trace error path.
//
// # The evaluator sees the trace it is GIVEN
//
// Every policy but two is a statement about the SET OF SPANS PRESENT, so its
// answer is only as good as assembly. This is a property of tail sampling, not
// of this implementation, and it is stated at the API boundary rather than
// assumed away:
//
//   - latency reads earliest-start-to-latest-end over the spans present, so an
//     incomplete trace yields a LOWER BOUND on the real duration: slow traces
//     can be missed, fast ones are never invented (adding spans only widens the
//     interval). Clock skew between the hosts that produced the extreme spans
//     is inside that number.
//   - statusCode, string/numeric/booleanAttribute are "any span matches", so
//     they are monotone in the span set: an incomplete trace UNDER-matches. The
//     inverse also holds and is the uncomfortable half — an inverted
//     stringAttribute can fail to see the span that would have vetoed, so an
//     exclusion leaks.
//   - rateLimiting and composite charge the SPANS PRESENT, so a partial trace
//     under-charges the budget. A trace decided twice (late spans, a
//     re-assembly, a restart) would charge twice, which is what Trace.Charged
//     exists to prevent — an assembler that knows it has decided this trace
//     before sets it, and the second decision checks the budget instead of
//     spending it.
//   - probabilistic and alwaysSample are the two that do not care: they are a
//     function of the trace id alone.
//
// # Bounds, concurrency, allocations
//
// Everything data-driven is bounded by construction: the only per-policy state
// is a fixed-size regex-result cache (stringAttribute) and a token bucket
// (rateLimiting, composite), neither of which grows with trace or attribute
// cardinality. There is no decision cache, no per-trace scratch and no map
// keyed by anything the wire controls.
//
// Decide is safe for concurrent use — buckets and caches are mutex-guarded,
// everything else is immutable after New — and is allocation-free for every
// policy type (see bench_test.go).
//
// # No metrics here
//
// Unlike agent/tracesample, this package imports internal/obs not at all. What
// an operator wants counted is not decisions but their COST — spans and bytes
// kept and dropped, per policy — and the decision engine is the one layer that
// does not know either: it is handed a view of a trace, not the payload that
// will be forwarded or discarded. So Decision carries the policy name (bounded
// by the config, safe as a label) and the layer that acts on the verdict owns
// the counter, the way otlpexport rather than pkg/otlpsplit counts sends.
// Evaluator.Names exists so that layer can create the label values up front.
package tailsample

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/config"
)

// Policy type names: the Collector's vocabulary in this repo's camelCase (see
// the package doc). A `type:` value is also the key its body lives under, so
// these constants are the json tags on PolicyConfig too — one spelling per
// policy, and no way for the two to drift.
const (
	TypeAlwaysSample     = "alwaysSample"
	TypeLatency          = "latency"
	TypeStatusCode       = "statusCode"
	TypeStringAttribute  = "stringAttribute"
	TypeNumericAttribute = "numericAttribute"
	TypeBooleanAttribute = "booleanAttribute"
	TypeProbabilistic    = "probabilistic"
	TypeRateLimiting     = "rateLimiting"
	TypeAnd              = "and"
	TypeComposite        = "composite"
	// TypeScript is the injected Starlark policy: the transforms file's
	// sample: section (decide(trace) -> True/False/None). Not a Collector
	// type; it exists so bespoke keep-logic (cross-span invariants,
	// tenant-aware sampling) does not grow this DSL forever.
	TypeScript = "script"
)

// Config is the policy list. The assembly layer's own section (how long to wait
// for a trace, how many to hold) wraps or embeds this one: the split is the
// same one the package doc describes, so neither half has to know the other's
// knobs.
type Config struct {
	// Policies are evaluated in order, first match wins. An empty list is not
	// an error — it means the feature is off (see Enabled), so a chart that
	// renders the section unconditionally does not fail startup — but New
	// refuses it, because an evaluator that can never sample anything is a
	// caller bug rather than a config state.
	Policies []PolicyConfig `json:"policies"`

	// Script is the `type: script` policy body, INJECTED by the wiring from
	// the transforms file's sample: section — never decoded from YAML (this
	// package stays free of Starlark; the engine hands it a closure). It
	// returns (sample, abstain); errors inside the script surface as abstain
	// there, so a broken hot-reloaded script degrades to the next policy in
	// the list rather than to a verdict. nil + a `type: script` policy is a
	// config error naming the missing section.
	Script func(Trace) (sample, abstain bool) `json:"-"`
}

// Enabled reports whether any policy is configured.
func (c *Config) Enabled() bool { return c != nil && len(c.Policies) > 0 }

// Validate reports a malformed policy list. It is shape-only and acquires
// nothing (no filesystem, no network, no clock), so -check-config can run it.
//
// It validates by COMPILING and discarding, which is the only way the dry-run
// cannot drift from the real start: there is one type switch, one set of bounds
// checks and one regex compiler, and both paths go through them.
func (c *Config) Validate() error {
	if !c.Enabled() {
		return nil
	}
	var alloc bucketAlloc
	// Shape validation must not depend on the INJECTED script body (Validate
	// runs in -check-config, where the wiring has not happened): a
	// placeholder satisfies the type: script compile, and the cross-check
	// that the transforms file actually defines a sample: section lives in
	// the agent's validateConfig, beside the transforms compile (UsesScript).
	alloc.script = func(Trace) (bool, bool) { return false, true }
	_, _, err := compilePolicies(c.Policies, false, &alloc)
	return err
}

// UsesScript reports whether any policy — top level or nested inside
// and/composite — is `type: script`, so config validation can require the
// transforms file's sample: section.
func (c *Config) UsesScript() bool {
	if c == nil {
		return false
	}
	var uses func(pcs []PolicyConfig) bool
	uses = func(pcs []PolicyConfig) bool {
		for _, pc := range pcs {
			if pc.Type == TypeScript {
				return true
			}
			if pc.And != nil && uses(pc.And.SubPolicies) {
				return true
			}
			if pc.Composite != nil && uses(pc.Composite.SubPolicies) {
				return true
			}
		}
		return false
	}
	return uses(c.Policies)
}

// PolicyConfig is one entry of the list. The body lives under a key named after
// the type, the way the Collector nests it — spelled in this repo's camelCase,
// so a Collector policy is transcribed key for key rather than pasted.
type PolicyConfig struct {
	// Name identifies the policy in Decision.Policy — i.e. in a metric label
	// and in whatever the operator reads to answer "why was this trace kept".
	// Required and unique within its list; bounded by the config, which is what
	// makes it safe as a label.
	Name string `json:"name"`
	// Type selects the body below. The body matching Type must be present, and
	// any OTHER body present is an error rather than dead config: a body that
	// does not match the type is always a half-finished copy-paste.
	Type string `json:"type"`

	Latency          *LatencyConfig          `json:"latency,omitempty"`
	StatusCode       *StatusCodeConfig       `json:"statusCode,omitempty"`
	StringAttribute  *StringAttributeConfig  `json:"stringAttribute,omitempty"`
	NumericAttribute *NumericAttributeConfig `json:"numericAttribute,omitempty"`
	BooleanAttribute *BooleanAttributeConfig `json:"booleanAttribute,omitempty"`
	Probabilistic    *ProbabilisticConfig    `json:"probabilistic,omitempty"`
	RateLimiting     *RateLimitingConfig     `json:"rateLimiting,omitempty"`
	And              *AndConfig              `json:"and,omitempty"`
	Composite        *CompositeConfig        `json:"composite,omitempty"`
}

// LatencyConfig keeps traces whose duration falls in [threshold, upper].
type LatencyConfig struct {
	// Threshold is the inclusive LOWER bound, a Go duration such as "500ms".
	// Empty is zero, i.e. no lower bound at all (every trace carrying a usable
	// timestamp qualifies).
	//
	// A STRING, not a time.Duration, and not the Collector's integer
	// threshold_ms either: the agent config decodes through sigs.k8s.io/yaml ->
	// encoding/json, which accepts only a raw nanosecond integer for a
	// time.Duration, so "500ms" would fail to decode — and since the whole file
	// is UnmarshalStrict'ed, that one field rejects the ENTIRE config and the
	// DaemonSet does not start. See the package doc.
	Threshold string `json:"threshold,omitempty"`
	// Upper is the inclusive UPPER bound ("30s"); empty — or an explicit zero —
	// means unbounded. A window (rather than "anything slower than X") is what
	// lets several latency policies sample different bands at different rates
	// without the slow band's rule being shadowed by the merely-slow one above
	// it in the list. The Collector calls this upper_threshold_ms.
	//
	// A STRING for the same reason as Threshold above.
	Upper string `json:"upper,omitempty"`
}

// threshold parses Threshold; empty is zero (no lower bound).
func (c *LatencyConfig) threshold() (time.Duration, error) {
	return config.Duration("latency.threshold", c.Threshold, 0, config.ZeroDisables())
}

// upper parses Upper; empty — and an explicit zero — is unbounded, which
// compileLatency spells as the type's maximum.
func (c *LatencyConfig) upper() (time.Duration, error) {
	return config.Duration("latency.upper", c.Upper, 0, config.ZeroDisables())
}

// StatusCodeConfig keeps traces where ANY span carries one of the listed codes.
type StatusCodeConfig struct {
	// StatusCodes are ERROR, OK and/or UNSET (case-insensitive). These are the
	// OTLP status names, not config keys, so they stay upper-case. Note that OK
	// is not "no error": an SDK that never calls SetStatus leaves UNSET, so
	// [OK] and [ERROR] together do not cover a typical trace.
	StatusCodes []string `json:"statusCodes"`
}

// StringAttributeConfig matches a string attribute on any span of the trace.
type StringAttributeConfig struct {
	// Key is looked up on the SPAN first and on its RESOURCE second, per span.
	// The span wins because it is the more specific statement: a resource
	// attribute is a default every span of that process carries, and a span
	// that overrides it is saying something about itself. (The Collector ORs
	// the two without a precedence, which is indistinguishable from this except
	// when both carry the key with different values — where "the process said
	// X" should not outvote "this span said Y".) Scope attributes are not
	// consulted; nothing puts routing-grade attributes there.
	Key string `json:"key"`
	// Values are matched exactly, or as anchored-free regexes when
	// EnabledRegexMatching is set. Only STRING-typed attribute values are
	// considered: rendering an int or a bool to text to compare it would be an
	// allocation per span on the decision path and a coercion the operator did
	// not ask for — numericAttribute and booleanAttribute exist for those.
	Values []string `json:"values"`
	// EnabledRegexMatching switches Values from exact to regexp (RE2,
	// unanchored, i.e. a substring match unless the pattern anchors itself).
	EnabledRegexMatching bool `json:"enabledRegexMatching,omitempty"`
	// CacheMaxSize bounds the regex-result cache (attribute value -> matched),
	// which only exists when EnabledRegexMatching is set. 0 takes the default;
	// there is deliberately no setting for "unbounded", because the keys are
	// attribute values chosen by whoever is instrumented, i.e. by the wire.
	CacheMaxSize int `json:"cacheMaxSize,omitempty"`
	// InvertMatch turns the policy into an exclusion: a match VETOES the trace
	// and ends the evaluation; no match abstains and the next policy decides.
	// See the package doc for why this is not the Collector's reading.
	InvertMatch bool `json:"invertMatch,omitempty"`
}

// NumericAttributeConfig matches an integer (or float) attribute in a range.
type NumericAttributeConfig struct {
	// Key resolves span-then-resource, as StringAttributeConfig.Key does.
	Key string `json:"key"`
	// MinValue and MaxValue are INCLUSIVE bounds; either may be omitted for an
	// open side. Pointers rather than plain ints so that omitting max does not
	// silently mean "max 0" — the shape that makes a numeric policy match
	// nothing at all while looking configured.
	MinValue *int64 `json:"minValue,omitempty"`
	MaxValue *int64 `json:"maxValue,omitempty"`
}

// BooleanAttributeConfig matches a bool attribute.
type BooleanAttributeConfig struct {
	// Key resolves span-then-resource, as StringAttributeConfig.Key does.
	Key string `json:"key"`
	// Value is required — a plain bool would make an omitted value mean false,
	// which is a policy the operator never wrote.
	Value *bool `json:"value"`
}

// ProbabilisticConfig keeps a consistent fraction of traces by trace id.
type ProbabilisticConfig struct {
	// SamplingPercentage is a PERCENTAGE: 50 is half, 0.5 is half a percent.
	// The Collector's unit, worth reading twice next to
	// traceSampling.probability in the same config file, which is a FRACTION.
	// Neither typo can be detected — both values are legal in the other's units
	// — so they are only ever a doc problem.
	SamplingPercentage float64 `json:"samplingPercentage"`
}

// RateLimitingConfig admits traces while a spans/second budget lasts.
type RateLimitingConfig struct {
	// SpansPerSecond is the sustained budget. A trace is charged its whole span
	// count and admitted all-or-nothing: half a trace is worth less than none.
	SpansPerSecond float64 `json:"spansPerSecond"`
}

// AndConfig requires every sub-policy to sample.
type AndConfig struct {
	// SubPolicies must all match for the AND to sample. A veto from any of them
	// propagates (the trace is dropped, the AND is named): an exclusion nested
	// in an AND is still an exclusion.
	//
	// Sub-policies must be LEAF types — no and/composite nesting — which is the
	// Collector's constraint too and bounds evaluation depth at two.
	SubPolicies []PolicyConfig `json:"andSubPolicy"`
}

// CompositeConfig runs ordered sub-policies against per-policy rate budgets.
type CompositeConfig struct {
	// MaxTotalSpansPerSecond is the budget shared out by RateAllocation.
	// Required and positive: zero would allocate nothing to anybody and the
	// whole composite would silently never sample.
	MaxTotalSpansPerSecond float64 `json:"maxTotalSpansPerSecond"`
	// PolicyOrder is the evaluation order. Empty means declaration order;
	// non-empty must be a permutation of the sub-policy names, so a typo or a
	// forgotten policy is an error rather than a policy that never runs.
	PolicyOrder []string `json:"policyOrder,omitempty"`
	// SubPolicies are the leaf policies (no and/composite nesting).
	SubPolicies []PolicyConfig `json:"compositeSubPolicy"`
	// RateAllocation splits MaxTotalSpansPerSecond. Sub-policies with no entry
	// share the leftover percentage evenly; if there is no leftover to share,
	// that is an error rather than a zero-budget policy.
	RateAllocation []RateAllocationConfig `json:"rateAllocation,omitempty"`
}

// RateAllocationConfig gives one composite sub-policy a share of the budget.
type RateAllocationConfig struct {
	Policy  string  `json:"policy"`
	Percent float64 `json:"percent"`
}

// Span is one span of an assembled trace together with the resource attributes
// it arrived under.
//
// Both fields are pdata HANDLES — a pointer and a state pointer each — so the
// assembly layer builds this view by appending what it already holds, with no
// copy and no conversion. That is the contract this type exists to make cheap:
// whatever the buffer stores, handing it to the evaluator must not cost a
// serialization pass.
type Span struct {
	// Span is the span itself. Read-only here: the evaluator never mutates a
	// span, so a buffer may hand out spans it still owns.
	Span ptrace.Span
	// Resource is the ResourceSpans attributes the span arrived under —
	// rs.Resource().Attributes(), not a copy. A zero (never-initialised) Map is
	// tolerated and reads as empty, because an assembler that drops resources
	// on some path should lose attribute matching, not panic in the sampler.
	Resource pcommon.Map
}

// Trace is the assembled view of one trace: its id and the spans that have been
// collected for it SO FAR. The evaluator neither retains nor mutates either
// field, so the caller may reuse the slice across decisions.
//
// "So far" is the load-bearing part — see the package doc: every policy except
// probabilistic and alwaysSample answers a question about this span set, and
// the answer changes if the set does. The caller decides when a trace is
// complete enough to judge; this package cannot know.
type Trace struct {
	// TraceID drives the probabilistic policy and must be set (the assembler
	// keys by it anyway). A zero id is not rejected but hashes like any other
	// value, so EVERY zero-id trace decides identically — visibly all-or-
	// nothing rather than silently re-rolled per trace.
	TraceID pcommon.TraceID
	// Spans may be empty: a trace with no spans abstains from every span-based
	// policy and can still be sampled by probabilistic or alwaysSample.
	Spans []Span

	// Charged says WHICH rate buckets this trace has ALREADY been charged to
	// by an earlier decision, so the two policies that spend one (rateLimiting,
	// composite) must CHECK a bucket whose bit is set instead of spending it
	// again — and must still SPEND a bucket whose bit is not. It is the earlier
	// decision's Decision.Charged and nothing else — "we decided this trace
	// before" is a different fact, and a decision the budget REFUSED spent
	// nothing to protect. Per BUCKET rather than one bit for all of them,
	// because with several buckets (a composite's shares, multiple rateLimiting
	// policies) a single bit let a re-decision Peek a bucket the trace never
	// paid — admitting its spans free against a budget some OTHER bucket's
	// spend had set the bit for.
	//
	// It exists because assembly is not a single event. An assembler that
	// remembers a trace was decided but no longer remembers the verdict — a
	// straggler arriving past the assembler's verdict TTL is the canonical case
	// — re-decides it, and without this the same trace pays the spans/second
	// budget twice, so late spans quietly shrink the budget available to
	// genuinely new traces.
	//
	// It changes nothing else: the same policy list is evaluated in the same
	// order, the same policy is named in the Decision, and every other policy is
	// a pure function of the trace and answers identically either way. What it
	// cannot do is guarantee the SAME verdict as the first decision — the bucket
	// may have emptied since — which is inherent to a bounded memory of past
	// decisions, not something this flag makes worse.
	Charged ChargedMask
}

// ChargedMask records which rate buckets an evaluation SPENT, one bit per
// bucket in compile order (bucketAlloc). Buckets past the mask's width share
// the last bit — they degrade to single-bit behaviour among themselves, which
// no realistic policy list reaches. The zero mask means "spent nothing".
type ChargedMask uint64

// Decision is the verdict plus its author.
type Decision struct {
	// Sampled is the verdict: keep the trace.
	Sampled bool
	// Policy names the policy that decided — the one that matched, or the
	// inverted one that vetoed. Empty when no policy had an opinion (the
	// default drop).
	//
	// This is meant to be a metric label ({policy, sampled}), which is why it
	// is a config-supplied name and never anything derived from a span: its
	// cardinality is the length of the policy list. A composite reports
	// "<composite>/<sub-policy>", precomputed at New so naming the author costs
	// no allocation.
	Policy string
	// Charged reports WHICH rate buckets this evaluation actually SPENT —
	// what an assembler must feed back as Trace.Charged if it re-decides this
	// trace, and nothing else.
	//
	// "It was decided before" is NOT the same fact, and using it is a hole: a
	// rateLimiting policy that REFUSED the trace deducted nothing, so a
	// re-decision told it was charged would peek a budget the trace never paid
	// and admit its spans free — every fresh window, for as long as the
	// assembler remembers the id. Note also that a decision may charge without
	// being the one that sampled (a rateLimiting sub-policy inside an `and`
	// charges before a later sub-policy abstains), which is why this is not
	// derivable from Policy or Sampled.
	Charged ChargedMask
}

// Evaluator applies a compiled policy list. Safe for concurrent use.
type Evaluator struct {
	policies []namedPolicy

	// needClock is set when some policy holds a token bucket. Without one there
	// is nothing time-dependent in the whole evaluator, and reading the clock
	// per trace would be pure overhead — so Decide reads it only if asked, and
	// then exactly ONCE per trace, so every bucket in one decision sees the
	// same instant.
	needClock bool
	now       func() time.Time // injectable for tests
}

// New compiles cfg. Every fallible thing happens here: regexes are compiled,
// status codes parsed, budgets divided, names resolved. Decide cannot fail
// afterwards.
func New(cfg Config) (*Evaluator, error) {
	if len(cfg.Policies) == 0 {
		return nil, errors.New("tail sampling: no policies configured (an evaluator with no policies drops every trace)")
	}
	var alloc bucketAlloc
	alloc.script = cfg.Script
	ps, needClock, err := compilePolicies(cfg.Policies, false, &alloc)
	if err != nil {
		return nil, err
	}
	return &Evaluator{policies: ps, needClock: needClock, now: time.Now}, nil
}

// Decide evaluates the policy list against t and returns the verdict and the
// policy that produced it. First match wins, in configured order; a policy with
// no opinion abstains and the next one decides; nothing errors (see the package
// doc for both semantics and why they are what they are).
//
// The result is a function of t and — for rateLimiting and composite only — of
// the budget already spent. Deciding the same trace twice may therefore answer
// differently; every other policy is idempotent. It does not charge twice as
// long as the caller sets Trace.Charged on the re-decision from the first
// decision's Decision.Charged: remembering that a trace was decided is not the
// same fact as remembering that the decision BILLED.
//
// What Decide CANNOT tell you is whether t is the whole trace. It answers about
// the spans it was handed.
func (e *Evaluator) Decide(t Trace) Decision {
	var now time.Time
	if e.needClock {
		now = e.now()
	}
	// spent accumulates across the WHOLE list, not just the deciding policy: a
	// policy that charged and then abstained (a rateLimiting sub-policy inside an
	// `and` whose later conjunct did not match) moved its bucket all the same.
	spent := ChargedMask(0)
	for i := range e.policies {
		p := &e.policies[i]
		v, sub, s := p.p.eval(t, now)
		spent |= s
		if v == verdictAbstain {
			continue
		}
		name := p.name
		if sub != "" { // composite: the sub-policy that actually decided
			name = sub
		}
		return Decision{Sampled: v == verdictSample, Policy: name, Charged: spent}
	}
	return Decision{Charged: spent}
}

// Names returns the policy names, in order, including the qualified
// "<composite>/<sub>" names a Decision can carry. It exists so a caller can
// pre-create the label values of its decision counter at startup: a counter
// whose series only appear once a policy first fires makes "policy X kept
// nothing" indistinguishable from "policy X does not exist".
func (e *Evaluator) Names() []string {
	out := make([]string, 0, len(e.policies))
	for i := range e.policies {
		out = append(out, e.policies[i].names()...)
	}
	return out
}

// traceDuration is earliest start to latest end across the spans PRESENT, and
// the second return reports whether any span had a usable start at all.
//
// Spans with no start timestamp cannot bound an interval and are skipped; a
// span whose end precedes its start (unfinished, or a clock that stepped) is
// treated as instantaneous rather than dragging the interval backwards. Both
// are one-line decisions with the same justification: a malformed span must
// never make a trace look SLOWER than it was, because that is the direction
// that costs money.
func traceDuration(t Trace) (time.Duration, bool) {
	var minStart, maxEnd pcommon.Timestamp
	found := false
	for i := range t.Spans {
		sp := t.Spans[i].Span
		start := sp.StartTimestamp()
		if start == 0 {
			continue
		}
		end := sp.EndTimestamp()
		if end < start {
			end = start
		}
		if !found || start < minStart {
			minStart = start
		}
		if !found || end > maxEnd {
			maxEnd = end
		}
		found = true
	}
	if !found {
		return 0, false
	}
	return time.Duration(maxEnd - minStart), true
}

// lookup resolves key for one span: its own attributes first, its resource's
// second. See StringAttributeConfig.Key for why that order.
func lookup(s Span, key string) (pcommon.Value, bool) {
	if v, ok := s.Span.Attributes().Get(key); ok {
		return v, true
	}
	if s.Resource == (pcommon.Map{}) {
		// Zero-value Map: pdata says a zero-initialised instance is not valid
		// for use and panics on access. An assembler with no resource for a
		// span should lose attribute matching, not the process.
		return pcommon.Value{}, false
	}
	return s.Resource.Get(key)
}

// errPolicy formats a config error that names the offending policy, so a
// -check-config failure points at a line rather than at a feature.
//
// Two properties have to hold at once, and each has already been lost once:
//
//   - The cause must stay WRAPPED. Rendering the args with an inner Sprintf
//     (the original) flattens them to text first, which makes %w structurally
//     impossible and silently degrades every caller to %v.
//   - go vet must still CHECK the format at all ~37 call sites. Concatenating
//     the prefix onto the format (%s: "+format, the fix for the first defect)
//     makes the format non-constant at the fmt.Errorf call, and the printf
//     analyser then gives up on the whole function — verbs stopped being
//     checked everywhere, silently.
//
// Formatting in two steps satisfies both. The inner call forwards `format` and
// `args...` VERBATIM, which is the shape vet's printf analyser recognises as a
// printf wrapper (it will not infer one through a concatenation or a rebuilt
// arg slice), so every call site is checked against `format` again; the outer
// %w keeps the inner error — including whatever the caller's own %w wrapped —
// reachable by errors.Is/errors.As. The rendered text is byte-identical to the
// concatenating version: "<where>: <formatted>".
func errPolicy(where, format string, args ...any) error {
	return fmt.Errorf("%s: %w", where, fmt.Errorf(format, args...))
}
