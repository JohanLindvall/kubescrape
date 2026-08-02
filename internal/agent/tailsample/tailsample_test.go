package tailsample

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"sigs.k8s.io/yaml"

	"github.com/JohanLindvall/kubescrape/internal/agent/tracesample"
)

// documentedYAML is the policy list as an operator writes it: the Collector's
// policy vocabulary in this repo's camelCase, with durations as parseable
// strings. Every policy type appears.
const documentedYAML = `
policies:
  - name: exclude-health-checks
    type: stringAttribute
    stringAttribute:
      key: http.route
      values: ["^/healthz$", "^/readyz$"]
      enabledRegexMatching: true
      cacheMaxSize: 500
      invertMatch: true
  - name: errors
    type: statusCode
    statusCode:
      statusCodes: [ERROR]
  - name: slow
    type: latency
    latency:
      threshold: 500ms
      upper: 30s
  - name: server-errors
    type: numericAttribute
    numericAttribute:
      key: http.status_code
      minValue: 500
      maxValue: 599
  - name: debug-flagged
    type: booleanAttribute
    booleanAttribute:
      key: sampling.debug
      value: true
  - name: slow-for-acme
    type: and
    and:
      andSubPolicy:
        - name: tenant
          type: stringAttribute
          stringAttribute:
            key: tenant
            values: [acme]
        - name: slowish
          type: latency
          latency:
            threshold: 100ms
  - name: budgeted
    type: composite
    composite:
      maxTotalSpansPerSecond: 500
      policyOrder: [errors, everything]
      compositeSubPolicy:
        - name: errors
          type: statusCode
          statusCode:
            statusCodes: [ERROR]
        - name: everything
          type: alwaysSample
      rateAllocation:
        - policy: errors
          percent: 60
  - name: baseline
    type: probabilistic
    probabilistic:
      samplingPercentage: 5
  - name: ceiling
    type: rateLimiting
    rateLimiting:
      spansPerSecond: 1000
`

// The test the whole config surface is worth: the agent config decodes through
// sigs.k8s.io/yaml (YAML -> JSON -> encoding/json) and is UnmarshalStrict'ed, so
// a field this package spells differently from its documentation does not
// degrade — it rejects the ENTIRE agent config and the DaemonSet does not start.
// That is the bug class that shipped last time (a time.Duration field against a
// documented "10s"), and it is why the duration fields here are STRINGS: the
// `threshold: 500ms` below has to survive the round trip.
func TestConfigDecodesFromDocumentedYAML(t *testing.T) {
	t.Parallel()
	var cfg Config
	if err := yaml.UnmarshalStrict([]byte(documentedYAML), &cfg); err != nil {
		t.Fatalf("the documented policy YAML does not decode: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate rejected the documented config: %v", err)
	}
	if !cfg.Enabled() {
		t.Fatal("Enabled = false for a nine-policy list")
	}
	if got, want := len(cfg.Policies), 9; got != want {
		t.Fatalf("decoded %d policies, want %d", got, want)
	}
	// Spot-check the shapes that a rename or a retyping would silently break.
	if got := cfg.Policies[0].StringAttribute; got == nil || !got.InvertMatch ||
		!got.EnabledRegexMatching || got.CacheMaxSize != 500 || len(got.Values) != 2 {
		t.Fatalf("stringAttribute decoded as %+v", got)
	}
	// The durations decode as the STRINGS they were written as, and parse to
	// what they read like. A time.Duration field would have failed the
	// UnmarshalStrict above outright.
	lat := cfg.Policies[2].Latency
	if lat == nil || lat.Threshold != "500ms" || lat.Upper != "30s" {
		t.Fatalf("latency decoded as %+v", lat)
	}
	if d, err := lat.threshold(); err != nil || d != 500*time.Millisecond {
		t.Fatalf("latency.threshold parsed as (%v, %v), want 500ms", d, err)
	}
	if d, err := lat.upper(); err != nil || d != 30*time.Second {
		t.Fatalf("latency.upper parsed as (%v, %v), want 30s", d, err)
	}
	if got := cfg.Policies[3].NumericAttribute; got == nil || got.MinValue == nil || *got.MinValue != 500 {
		t.Fatalf("numericAttribute decoded as %+v", got)
	}
	if got := cfg.Policies[4].BooleanAttribute; got == nil || got.Value == nil || !*got.Value {
		t.Fatalf("booleanAttribute decoded as %+v", got)
	}
	if got := cfg.Policies[6].Composite; got == nil || got.MaxTotalSpansPerSecond != 500 ||
		len(got.SubPolicies) != 2 || len(got.RateAllocation) != 1 {
		t.Fatalf("composite decoded as %+v", got)
	}

	// And it compiles: -check-config validating a config that New then refuses
	// would be worse than no dry run at all.
	e, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := len(e.policies); got != 9 {
		t.Fatalf("compiled %d policies, want 9", got)
	}
}

// A Collector policy is transcribed, not pasted, and a snake_case key left
// behind must SAY so: strict decoding rejects it rather than ignoring it. Same
// for a mis-cased body key and a mis-cased type value — a policy that decodes
// but never fires is the failure mode this pins shut.
func TestUnknownPolicyFieldIsRejected(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		yaml string
	}{
		{"snake_case body field", `
policies:
  - name: slow
    type: latency
    latency:
      threshold_ms: 500
`},
		{"snake_case body key", `
policies:
  - name: attr
    type: stringAttribute
    string_attribute:
      key: k
      values: [v]
`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg Config
			if err := yaml.UnmarshalStrict([]byte(tc.yaml), &cfg); err == nil {
				t.Fatal("decoded silently; a typo'd policy would be a policy that never fires")
			}
		})
	}

	// A mis-cased TYPE value decodes (it is just a string) but must not
	// compile: the type table is the second half of the same guarantee.
	t.Run("snake_case type value", func(t *testing.T) {
		var cfg Config
		if err := yaml.UnmarshalStrict([]byte(`
policies:
  - name: all
    type: always_sample
`), &cfg); err != nil {
			t.Fatalf("decode: %v", err)
		}
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate accepted the Collector's always_sample spelling")
		}
		for _, want := range []string{"unknown type", "always_sample", TypeAlwaysSample} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not mention %q", err, want)
			}
		}
	})
}

func TestValidateRejectsBadShapes(t *testing.T) {
	t.Parallel()
	strAttr := func(cfg StringAttributeConfig) PolicyConfig {
		p := pol("attr", TypeStringAttribute)
		p.StringAttribute = &cfg
		return p
	}
	for _, tc := range []struct {
		name     string
		policies []PolicyConfig
		want     []string
	}{
		{"unknown type", []PolicyConfig{pol("p", "latencyy")}, []string{"unknown type", "latencyy"}},
		{"no type", []PolicyConfig{{Name: "p"}}, []string{"type is required"}},
		{"no name", []PolicyConfig{{Type: TypeAlwaysSample}}, []string{"name is required"}},
		{"duplicate name", []PolicyConfig{pol("p", TypeAlwaysSample), pol("p", TypeAlwaysSample)},
			[]string{"duplicate policy name", `"p"`}},
		{"missing body", []PolicyConfig{pol("p", TypeLatency)}, []string{"no \"latency\" body"}},
		{"body for another type", []PolicyConfig{func() PolicyConfig {
			p := pol("p", TypeAlwaysSample)
			p.Latency = &LatencyConfig{Threshold: "1ms"}
			return p
		}()}, []string{"a \"latency\" body is also set"}},

		// The duration fields are strings, so an unparseable one is a config
		// error that has to name the field AND the value — the operator's typo
		// is in neither the field list nor the policy name.
		{"unparseable threshold", []PolicyConfig{latCfg("500", "")},
			[]string{"latency.threshold", `"500"`, "missing unit"}},
		{"unparseable upper", []PolicyConfig{latCfg("500ms", "30 seconds")},
			[]string{"latency.upper", `"30 seconds"`}},
		{"negative latency", []PolicyConfig{latCfg("-1ms", "")},
			[]string{"latency.threshold", `"-1ms"`, "negative"}},
		{"negative upper", []PolicyConfig{latCfg("", "-1s")},
			[]string{"latency.upper", `"-1s"`, "negative"}},
		{"empty latency window", []PolicyConfig{latCfg("100ms", "50ms")},
			[]string{"latency.upper", `"50ms"`, "below threshold", `"100ms"`}},

		{"unknown status code", []PolicyConfig{func() PolicyConfig {
			p := pol("p", TypeStatusCode)
			p.StatusCode = &StatusCodeConfig{StatusCodes: []string{"FAILED"}}
			return p
		}()}, []string{"unknown status code", "FAILED"}},
		{"no status codes", []PolicyConfig{func() PolicyConfig {
			p := pol("p", TypeStatusCode)
			p.StatusCode = &StatusCodeConfig{}
			return p
		}()}, []string{"statusCodes is empty"}},

		{"no attribute key", []PolicyConfig{strAttr(StringAttributeConfig{Values: []string{"v"}})},
			[]string{"key is required"}},
		{"no attribute values", []PolicyConfig{strAttr(StringAttributeConfig{Key: "k"})},
			[]string{"values is empty"}},
		{"bad regex", []PolicyConfig{strAttr(StringAttributeConfig{Key: "k", Values: []string{"("}, EnabledRegexMatching: true})},
			[]string{"values[0]", "error parsing regexp"}},
		{"cache without regex", []PolicyConfig{strAttr(StringAttributeConfig{Key: "k", Values: []string{"v"}, CacheMaxSize: 10})},
			[]string{"cacheMaxSize", "enabledRegexMatching"}},
		{"negative cache", []PolicyConfig{strAttr(StringAttributeConfig{Key: "k", Values: []string{"v"}, EnabledRegexMatching: true, CacheMaxSize: -1})},
			[]string{"cacheMaxSize", "negative"}},

		{"numeric with no bounds", []PolicyConfig{func() PolicyConfig {
			p := pol("p", TypeNumericAttribute)
			p.NumericAttribute = &NumericAttributeConfig{Key: "k"}
			return p
		}()}, []string{"minValue and/or maxValue"}},
		{"numeric inverted bounds", []PolicyConfig{func() PolicyConfig {
			p := pol("p", TypeNumericAttribute)
			p.NumericAttribute = &NumericAttributeConfig{Key: "k", MinValue: i64p(10), MaxValue: i64p(1)}
			return p
		}()}, []string{"minValue", "above maxValue"}},

		{"boolean without a value", []PolicyConfig{func() PolicyConfig {
			p := pol("p", TypeBooleanAttribute)
			p.BooleanAttribute = &BooleanAttributeConfig{Key: "k"}
			return p
		}()}, []string{"value is required"}},

		{"percentage out of range", []PolicyConfig{probPol(150)}, []string{"samplingPercentage", "[0,100]"}},
		{"negative percentage", []PolicyConfig{probPol(-1)}, []string{"samplingPercentage"}},
		{"zero rate", []PolicyConfig{ratePol(0)}, []string{"spansPerSecond", "> 0"}},

		{"empty and", []PolicyConfig{func() PolicyConfig {
			p := pol("p", TypeAnd)
			p.And = &AndConfig{}
			return p
		}()}, []string{"andSubPolicy is empty"}},
		{"nested and", []PolicyConfig{func() PolicyConfig {
			inner := pol("inner", TypeAnd)
			inner.And = &AndConfig{SubPolicies: []PolicyConfig{pol("x", TypeAlwaysSample)}}
			p := pol("p", TypeAnd)
			p.And = &AndConfig{SubPolicies: []PolicyConfig{inner}}
			return p
		}()}, []string{"may not be nested", "leaf types"}},

		{"composite with no budget", []PolicyConfig{compositeCfg(0, nil, nil, pol("a", TypeAlwaysSample))},
			[]string{"maxTotalSpansPerSecond", "> 0"}},
		{"composite with no sub-policies", []PolicyConfig{compositeCfg(10, nil, nil)},
			[]string{"compositeSubPolicy is empty"}},
		{"policyOrder names a stranger", []PolicyConfig{compositeCfg(10, []string{"a", "ghost"}, nil,
			pol("a", TypeAlwaysSample), pol("b", TypeAlwaysSample))},
			[]string{"policyOrder", "ghost"}},
		{"policyOrder is partial", []PolicyConfig{compositeCfg(10, []string{"a"}, nil,
			pol("a", TypeAlwaysSample), pol("b", TypeAlwaysSample))},
			[]string{"policyOrder omits", "b"}},
		{"rateAllocation names a stranger", []PolicyConfig{compositeCfg(10, nil,
			[]RateAllocationConfig{{Policy: "ghost", Percent: 50}}, pol("a", TypeAlwaysSample))},
			[]string{"rateAllocation", "ghost"}},
		{"rateAllocation over 100%", []PolicyConfig{compositeCfg(10, nil,
			[]RateAllocationConfig{{Policy: "a", Percent: 60}, {Policy: "b", Percent: 60}},
			pol("a", TypeAlwaysSample), pol("b", TypeAlwaysSample))},
			[]string{"rateAllocation sums to"}},
		{"rateAllocation starves the rest", []PolicyConfig{compositeCfg(10, nil,
			[]RateAllocationConfig{{Policy: "a", Percent: 100}},
			pol("a", TypeAlwaysSample), pol("b", TypeAlwaysSample))},
			[]string{"leaves 0%", "could never sample"}},
		{"composite inside composite", []PolicyConfig{compositeCfg(10, nil, nil,
			compositeCfg(5, nil, nil, pol("x", TypeAlwaysSample)))},
			[]string{"may not be nested", "leaf types"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Policies: tc.policies}
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("Validate accepted %+v", tc.policies)
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not mention %q", err, w)
				}
			}
			// Validate and New must agree: a dry run that passes what the real
			// start refuses is worse than no dry run.
			if _, nerr := New(cfg); nerr == nil {
				t.Errorf("New accepted what Validate rejected")
			}
		})
	}
}

// An absent or empty section is not an error — the feature is simply off — but
// building an evaluator out of it is a caller bug, because such an evaluator
// drops every trace it is handed.
func TestEmptyConfigIsDisabledNotInvalid(t *testing.T) {
	t.Parallel()
	var nilCfg *Config
	if err := nilCfg.Validate(); err != nil {
		t.Fatalf("a nil section must validate: %v", err)
	}
	if nilCfg.Enabled() {
		t.Fatal("a nil section must not report Enabled")
	}
	empty := Config{}
	if err := empty.Validate(); err != nil {
		t.Fatalf("an empty policy list must validate: %v", err)
	}
	if empty.Enabled() {
		t.Fatal("an empty policy list must not report Enabled")
	}
	if _, err := New(empty); err == nil {
		t.Fatal("New must refuse a policy-less evaluator")
	}
}

// First match wins, in configured order, and the Decision names the policy that
// won — that name is the metric label an operator answers "why was this trace
// kept" with, so it has to be the one rule, not a set.
func TestFirstMatchWinsInOrder(t *testing.T) {
	t.Parallel()
	errs := pol("errors", TypeStatusCode)
	errs.StatusCode = &StatusCodeConfig{StatusCodes: []string{"ERROR"}}
	slow := pol("slow", TypeLatency)
	slow.Latency = &LatencyConfig{Threshold: "100ms"}

	slowError := mkTrace(1, nil, spanDef{start: 0, end: 5000, status: ptrace.StatusCodeError})

	if got := mustNew(t, errs, slow).Decide(slowError); got.Policy != "errors" {
		t.Fatalf("Decide = %+v, want the FIRST matching policy (errors)", got)
	}
	if got := mustNew(t, slow, errs).Decide(slowError); got.Policy != "slow" {
		t.Fatalf("Decide = %+v, want the FIRST matching policy (slow)", got)
	}
	// A policy that does not match abstains and the next one decides.
	fast := mkTrace(2, nil, spanDef{start: 0, end: 1, status: ptrace.StatusCodeError})
	if got := mustNew(t, slow, errs).Decide(fast); !got.Sampled || got.Policy != "errors" {
		t.Fatalf("Decide = %+v, want errors after slow abstained", got)
	}
	// Nothing matches: dropped, with no policy to blame.
	plain := mkTrace(3, nil, spanDef{start: 0, end: 1})
	if got := mustNew(t, slow, errs).Decide(plain); got != (Decision{}) {
		t.Fatalf("Decide = %+v, want the default drop with an empty Policy", got)
	}
}

// The inverted-policy contract, which is where this package deliberately parts
// from the Collector: a match VETOES and ends the evaluation; no match ABSTAINS
// and the later policies still decide. The Collector would sample everything the
// rule does not name, which makes an exclusion silently switch on 100% sampling.
func TestInvertedPolicyAbstainsAndLetsLaterPoliciesDecide(t *testing.T) {
	t.Parallel()
	excl := pol("exclude-healthz", TypeStringAttribute)
	excl.StringAttribute = &StringAttributeConfig{Key: "http.route", Values: []string{"/healthz"}, InvertMatch: true}
	errs := pol("errors", TypeStatusCode)
	errs.StatusCode = &StatusCodeConfig{StatusCodes: []string{"ERROR"}}
	e := mustNew(t, excl, errs)

	health := mkTrace(1, nil, spanDef{start: 0, end: 1, status: ptrace.StatusCodeError,
		attrs: map[string]any{"http.route": "/healthz"}})
	if got := e.Decide(health); got.Sampled || got.Policy != "exclude-healthz" {
		t.Fatalf("Decide = %+v, want a veto by exclude-healthz even though the trace errored", got)
	}
	checkout := mkTrace(2, nil, spanDef{start: 0, end: 1, status: ptrace.StatusCodeError,
		attrs: map[string]any{"http.route": "/checkout"}})
	if got := e.Decide(checkout); !got.Sampled || got.Policy != "errors" {
		t.Fatalf("Decide = %+v, want errors: a non-matching exclusion must abstain, not sample", got)
	}
	// The Collector's reading is one line away, and says what it means.
	all := mustNew(t, excl, errs, pol("everything-else", TypeAlwaysSample))
	plain := mkTrace(3, nil, spanDef{start: 0, end: 1, attrs: map[string]any{"http.route": "/checkout"}})
	if got := all.Decide(plain); !got.Sampled || got.Policy != "everything-else" {
		t.Fatalf("Decide = %+v, want everything-else", got)
	}
}

// A policy earlier in the list that samples must not spend a later
// rateLimiting policy's budget — that is what makes "keep every error, then
// rate-limit the rest" mean what it reads like.
func TestEarlierMatchDoesNotSpendTheRateBudget(t *testing.T) {
	t.Parallel()
	errs := pol("errors", TypeStatusCode)
	errs.StatusCode = &StatusCodeConfig{StatusCodes: []string{"ERROR"}}
	e := mustNew(t, errs, ratePol(4))
	now := time.Unix(0, 0)
	fixedClock(e, &now)

	errTrace := mkTrace(1, nil, spanDef{start: 0, end: 1, status: ptrace.StatusCodeError})
	for i := 0; i < 100; i++ {
		if got := e.Decide(errTrace); !got.Sampled || got.Policy != "errors" {
			t.Fatalf("iteration %d: %+v, want errors", i, got)
		}
	}
	plain := mkTrace(2, nil, spanDef{start: 0, end: 1})
	kept := 0
	for i := 0; i < 100; i++ {
		if e.Decide(plain).Sampled {
			kept++
		}
	}
	if kept != 4 {
		t.Fatalf("rate policy admitted %d traces, want its untouched 4-span burst", kept)
	}
}

// Names lists every string a Decision can carry, so a caller can create its
// decision counter's label values up front: a series that only appears once a
// policy first fires makes "kept nothing" indistinguishable from "does not
// exist".
func TestNamesCoversEveryDecisionLabel(t *testing.T) {
	t.Parallel()
	errs := pol("errors", TypeStatusCode)
	errs.StatusCode = &StatusCodeConfig{StatusCodes: []string{"ERROR"}}
	e := mustNew(t,
		errs,
		compositeCfg(10, nil, nil, pol("a", TypeAlwaysSample), pol("b", TypeAlwaysSample)),
	)
	got := strings.Join(e.Names(), ",")
	if want := "errors,composite/a,composite/b"; got != want {
		t.Fatalf("Names = %q, want %q", got, want)
	}
}

// An assembler that has no resource for a span must lose attribute matching,
// not the process: pdata panics on a zero-initialised Map.
func TestZeroResourceMapIsTolerated(t *testing.T) {
	t.Parallel()
	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.Attributes().PutStr("k", "v")
	tr := Trace{TraceID: traceID(1), Spans: []Span{{Span: sp}}} // Resource left zero

	match := pol("match", TypeStringAttribute)
	match.StringAttribute = &StringAttributeConfig{Key: "k", Values: []string{"v"}}
	miss := pol("miss", TypeStringAttribute)
	miss.StringAttribute = &StringAttributeConfig{Key: "only.on.resource", Values: []string{"v"}}

	if got := mustNew(t, match).Decide(tr); !got.Sampled {
		t.Fatalf("span attribute did not match with a zero resource: %+v", got)
	}
	if got := mustNew(t, miss).Decide(tr); got.Sampled {
		t.Fatalf("Decide = %+v, want no match", got)
	}
}

// Decide runs on whatever goroutines the assembly layer decides makes traces,
// so the two pieces of mutable state (regex caches, token buckets) have to hold
// up. Run with -race; the budget assertion is the non-race half.
func TestDecideIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()
	attr := pol("attr", TypeStringAttribute)
	attr.StringAttribute = &StringAttributeConfig{
		Key: "http.route", Values: []string{"^/checkout"}, EnabledRegexMatching: true, CacheMaxSize: 8,
	}
	e := mustNew(t, attr, ratePol(50))
	now := time.Unix(0, 0)
	fixedClock(e, &now)

	const workers, each = 8, 200
	var wg sync.WaitGroup
	var mu sync.Mutex
	byPolicy := map[string]int{}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				// Distinct route values keep the bounded regex cache churning.
				tr := mkTrace(byte(w), nil, spanDef{attrs: map[string]any{
					"http.route": "/orders/" + string(rune('a'+i%26)),
				}})
				d := e.Decide(tr)
				mu.Lock()
				byPolicy[d.Policy]++
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	if got := byPolicy["rate"]; got != 50 {
		t.Fatalf("rate policy admitted %d traces concurrently, want exactly its 50-span burst", got)
	}
	if got := byPolicy["attr"]; got != 0 {
		t.Fatalf("attr matched %d traces, want 0 (no route starts with /checkout)", got)
	}
}

// The probabilistic policy shares agent/tracesample's hash and threshold
// arithmetic, and this pins the consequence the package doc claims: a tail
// policy at 50% keeps EXACTLY the traces a head sampler at probability 0.5
// already kept, so the two stages nest instead of compounding into a quarter.
// If either side ever changes its hash, salts it, or rounds its threshold
// differently, this fails.
func TestProbabilisticNestsWithTheHeadSampler(t *testing.T) {
	t.Parallel()
	const n = 2000
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	for i := uint64(0); i < n; i++ {
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(traceID(i))
		sp.SetStartTimestamp(ts(0))
		sp.SetEndTimestamp(ts(1))
	}
	cap := &headCapture{}
	head := tracesample.New(tracesample.Config{Probability: 0.5}, cap)
	if err := head.ExportTraces(context.Background(), td); err != nil {
		t.Fatalf("head sampler: %v", err)
	}
	headKept := map[pcommon.TraceID]bool{}
	for _, batch := range cap.batches {
		rss := batch.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			sss := rss.At(i).ScopeSpans()
			for j := 0; j < sss.Len(); j++ {
				spans := sss.At(j).Spans()
				for k := 0; k < spans.Len(); k++ {
					headKept[spans.At(k).TraceID()] = true
				}
			}
		}
	}
	if len(headKept) == 0 || len(headKept) == n {
		t.Fatalf("the head sampler kept %d of %d; this test would prove nothing", len(headKept), n)
	}

	tail := mustNew(t, probPol(50))
	for i := uint64(0); i < n; i++ {
		id := traceID(i)
		if got, want := tail.Decide(Trace{TraceID: id}).Sampled, headKept[id]; got != want {
			t.Fatalf("trace %d: tail kept=%v, head kept=%v — the two stages no longer nest", i, got, want)
		}
	}
}

type headCapture struct{ batches []ptrace.Traces }

func (c *headCapture) ExportTraces(_ context.Context, td ptrace.Traces) error {
	c.batches = append(c.batches, td)
	return nil
}
