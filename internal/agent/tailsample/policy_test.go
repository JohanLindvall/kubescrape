package tailsample

import (
	"encoding/binary"
	"errors"
	"regexp/syntax"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// --- helpers ----------------------------------------------------------------

// tsBase is a fixed wall clock for span timestamps; every offset below is
// milliseconds from it. Nothing here reads the real clock (the evaluator's is
// injectable, see fixedClock), so the tests are not timing-based.
const tsBase int64 = 1_760_000_000_000

// unset marks a timestamp the span does not carry.
const unset int64 = -1

func ts(ms int64) pcommon.Timestamp {
	if ms == unset {
		return 0
	}
	return pcommon.Timestamp(uint64(tsBase+ms) * uint64(time.Millisecond))
}

type spanDef struct {
	start, end int64 // ms offsets from tsBase, or unset
	status     ptrace.StatusCode
	attrs      map[string]any
}

// mkTrace assembles a trace the way the buffering layer is expected to: pdata
// handles into a payload it already holds, no copies. All spans share one
// resource, which is the common case and the one where span-vs-resource
// attribute precedence is decided.
func mkTrace(id byte, res map[string]any, defs ...spanDef) Trace {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	putAttrs(rs.Resource().Attributes(), res)
	ss := rs.ScopeSpans().AppendEmpty()
	var tid pcommon.TraceID
	tid[15] = id
	t := Trace{TraceID: tid, Spans: make([]Span, 0, len(defs))}
	for _, d := range defs {
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(tid)
		sp.SetStartTimestamp(ts(d.start))
		sp.SetEndTimestamp(ts(d.end))
		sp.Status().SetCode(d.status)
		putAttrs(sp.Attributes(), d.attrs)
		t.Spans = append(t.Spans, Span{Span: sp, Resource: rs.Resource().Attributes()})
	}
	return t
}

func putAttrs(m pcommon.Map, kv map[string]any) {
	for k, v := range kv {
		switch t := v.(type) {
		case string:
			m.PutStr(k, t)
		case int:
			m.PutInt(k, int64(t))
		case int64:
			m.PutInt(k, t)
		case float64:
			m.PutDouble(k, t)
		case bool:
			m.PutBool(k, t)
		default:
			panic("unsupported attribute type in test helper")
		}
	}
}

// traceID spreads n over the whole id so the hash sees varied input.
func traceID(n uint64) pcommon.TraceID {
	var id pcommon.TraceID
	binary.BigEndian.PutUint64(id[0:8], n*0x9E3779B97F4A7C15)
	binary.BigEndian.PutUint64(id[8:16], n)
	return id
}

func mustNew(t testing.TB, policies ...PolicyConfig) *Evaluator {
	t.Helper()
	e, err := New(Config{Policies: policies})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return e
}

// fixedClock pins the evaluator's clock; the caller advances it by assignment.
func fixedClock(e *Evaluator, now *time.Time) { e.now = func() time.Time { return *now } }

func boolp(b bool) *bool  { return &b }
func i64p(i int64) *int64 { return &i }
func pol(name, typ string) PolicyConfig {
	return PolicyConfig{Name: name, Type: typ}
}

// latCfg builds a latency policy from the two duration STRINGS the config
// carries (empty = omitted), so the compile-time parse is exercised by every
// latency test rather than by the decode test alone.
func latCfg(threshold, upper string) PolicyConfig {
	p := pol("slow", TypeLatency)
	p.Latency = &LatencyConfig{Threshold: threshold, Upper: upper}
	return p
}

// --- alwaysSample -----------------------------------------------------------

func TestAlwaysSample(t *testing.T) {
	t.Parallel()
	e := mustNew(t, pol("all", TypeAlwaysSample))
	// Including a trace with no spans at all: alwaysSample and probabilistic
	// are the two policies that do not depend on the assembled span set.
	for _, tr := range []Trace{mkTrace(1, nil, spanDef{start: 0, end: 1}), {TraceID: traceID(7)}} {
		if got := e.Decide(tr); !got.Sampled || got.Policy != "all" {
			t.Fatalf("Decide = %+v, want sampled by \"all\"", got)
		}
	}
}

// --- latency ----------------------------------------------------------------

func TestLatencyPolicy(t *testing.T) {
	t.Parallel()
	lat := latCfg
	for _, tc := range []struct {
		name string
		cfg  PolicyConfig
		tr   Trace
		want bool
	}{
		{"below threshold", lat("500ms", ""), mkTrace(1, nil, spanDef{start: 0, end: 499}), false},
		{"exactly at threshold is kept", lat("500ms", ""), mkTrace(1, nil, spanDef{start: 0, end: 500}), true},
		{"above threshold", lat("500ms", ""), mkTrace(1, nil, spanDef{start: 0, end: 5000}), true},
		{"exactly at upper is kept", lat("500ms", "1s"), mkTrace(1, nil, spanDef{start: 0, end: 1000}), true},
		{"above upper is left to a later policy", lat("500ms", "1s"), mkTrace(1, nil, spanDef{start: 0, end: 1001}), false},
		// Any spelling time.ParseDuration accepts is the same window: the
		// duration is parsed, not read as a number in a fixed unit.
		{"a compound duration reads the same", lat("1s500ms", ""), mkTrace(1, nil, spanDef{start: 0, end: 1500}), true},
		{"sub-millisecond precision survives", lat("500us", ""), mkTrace(1, nil, spanDef{start: 0, end: 1}), true},
		// The trace's duration is earliest start to latest end ACROSS SPANS:
		// no single span here is slow, the trace is.
		{"spans the whole trace, not one span", lat("500ms", ""),
			mkTrace(1, nil,
				spanDef{start: 0, end: 10},
				spanDef{start: 300, end: 310},
				spanDef{start: 590, end: 600}), true},
		// A child that outlives its parent still widens the interval — the
		// definition is deliberately not "the root span's duration", which an
		// incomplete assembly may not even contain.
		{"latest end wins even without a root", lat("500ms", ""),
			mkTrace(1, nil, spanDef{start: 100, end: 700}), true},
		{"no timestamps abstains rather than counting as zero", lat("", ""),
			mkTrace(1, nil, spanDef{start: unset, end: unset}), false},
		{"a start with no end is instantaneous", lat("1ms", ""),
			mkTrace(1, nil, spanDef{start: 100, end: unset}), false},
		{"an end before its start never lengthens the trace", lat("50ms", ""),
			mkTrace(1, nil, spanDef{start: 100, end: 40}, spanDef{start: 100, end: 120}), false},
		// An omitted threshold is no lower bound, and an omitted (or explicitly
		// zero) upper is unbounded — the reading the old integer 0 carried.
		{"an omitted threshold keeps anything with a timestamp", lat("", ""),
			mkTrace(1, nil, spanDef{start: 100, end: 100}), true},
		{"an explicit zero upper is unbounded, not an empty window", lat("500ms", "0s"),
			mkTrace(1, nil, spanDef{start: 0, end: 100_000}), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := mustNew(t, tc.cfg).Decide(tc.tr)
			if got.Sampled != tc.want {
				t.Fatalf("Sampled = %v, want %v", got.Sampled, tc.want)
			}
			if got.Sampled && got.Policy != "slow" {
				t.Fatalf("Policy = %q, want \"slow\"", got.Policy)
			}
		})
	}
}

// --- statusCode -------------------------------------------------------------

func TestStatusCodePolicy(t *testing.T) {
	t.Parallel()
	sc := func(codes ...string) PolicyConfig {
		p := pol("status", TypeStatusCode)
		p.StatusCode = &StatusCodeConfig{StatusCodes: codes}
		return p
	}
	errSpan := spanDef{start: 0, end: 1, status: ptrace.StatusCodeError}
	okSpan := spanDef{start: 0, end: 1, status: ptrace.StatusCodeOk}
	unsetSpan := spanDef{start: 0, end: 1}

	for _, tc := range []struct {
		name string
		cfg  PolicyConfig
		tr   Trace
		want bool
	}{
		{"any span matching is enough", sc("ERROR"), mkTrace(1, nil, okSpan, unsetSpan, errSpan), true},
		{"no span matching", sc("ERROR"), mkTrace(1, nil, okSpan, unsetSpan), false},
		{"OK is not 'no error'", sc("OK"), mkTrace(1, nil, unsetSpan), false},
		{"UNSET is a code like any other", sc("UNSET"), mkTrace(1, nil, unsetSpan), true},
		{"several codes", sc("ERROR", "OK"), mkTrace(1, nil, okSpan), true},
		{"case insensitive", sc("error"), mkTrace(1, nil, errSpan), true},
		{"empty trace", sc("ERROR"), Trace{TraceID: traceID(1)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := mustNew(t, tc.cfg).Decide(tc.tr); got.Sampled != tc.want {
				t.Fatalf("Sampled = %v, want %v", got.Sampled, tc.want)
			}
		})
	}
}

// --- stringAttribute --------------------------------------------------------

func TestStringAttributePolicy(t *testing.T) {
	t.Parallel()
	sa := func(cfg StringAttributeConfig) PolicyConfig {
		p := pol("attr", TypeStringAttribute)
		p.StringAttribute = &cfg
		return p
	}
	for _, tc := range []struct {
		name string
		cfg  PolicyConfig
		tr   Trace
		want Decision
	}{
		{"span attribute matches",
			sa(StringAttributeConfig{Key: "http.route", Values: []string{"/checkout"}}),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"http.route": "/checkout"}}),
			Decision{Sampled: true, Policy: "attr"}},
		{"resource attribute matches when no span carries the key",
			sa(StringAttributeConfig{Key: "service.name", Values: []string{"checkout"}}),
			mkTrace(1, map[string]any{"service.name": "checkout"}, spanDef{}),
			Decision{Sampled: true, Policy: "attr"}},
		// Precedence: the span is the more specific statement, so when both
		// carry the key the resource's value is NOT consulted.
		{"span overrides resource",
			sa(StringAttributeConfig{Key: "tenant", Values: []string{"acme"}}),
			mkTrace(1, map[string]any{"tenant": "acme"}, spanDef{attrs: map[string]any{"tenant": "globex"}}),
			Decision{}},
		{"span overrides resource, the other way",
			sa(StringAttributeConfig{Key: "tenant", Values: []string{"globex"}}),
			mkTrace(1, map[string]any{"tenant": "acme"}, spanDef{attrs: map[string]any{"tenant": "globex"}}),
			Decision{Sampled: true, Policy: "attr"}},
		{"any span matching is enough",
			sa(StringAttributeConfig{Key: "k", Values: []string{"v"}}),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"k": "other"}}, spanDef{attrs: map[string]any{"k": "v"}}),
			Decision{Sampled: true, Policy: "attr"}},
		{"missing key",
			sa(StringAttributeConfig{Key: "k", Values: []string{"v"}}),
			mkTrace(1, nil, spanDef{}), Decision{}},
		// No coercion: an int attribute is not rendered to text to be matched.
		{"a non-string value never matches",
			sa(StringAttributeConfig{Key: "k", Values: []string{"7"}}),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"k": 7}}), Decision{}},
		{"exact matching is exact, not a prefix",
			sa(StringAttributeConfig{Key: "k", Values: []string{"/api"}}),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"k": "/api/v1"}}), Decision{}},
		{"regex matching is unanchored",
			sa(StringAttributeConfig{Key: "k", Values: []string{"api"}, EnabledRegexMatching: true}),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"k": "/api/v1"}}),
			Decision{Sampled: true, Policy: "attr"}},
		{"regex can anchor itself",
			sa(StringAttributeConfig{Key: "k", Values: []string{"^/api$"}, EnabledRegexMatching: true}),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"k": "/api/v1"}}), Decision{}},
		{"one of several regexes",
			sa(StringAttributeConfig{Key: "k", Values: []string{"^/healthz$", "^/metrics$"}, EnabledRegexMatching: true}),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"k": "/metrics"}}),
			Decision{Sampled: true, Policy: "attr"}},
		// Inverted: a match is a VETO (dropped, and the policy is named), which
		// is what makes it usable as an exclusion ahead of the sampling rules.
		{"invertMatch vetoes on a match",
			sa(StringAttributeConfig{Key: "http.route", Values: []string{"/healthz"}, InvertMatch: true}),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"http.route": "/healthz"}}),
			Decision{Sampled: false, Policy: "attr"}},
		// ... and no match ABSTAINS rather than sampling everything else (the
		// Collector's reading); the fall-through is pinned in
		// TestInvertedPolicyAbstainsAndLetsLaterPoliciesDecide.
		{"invertMatch abstains when nothing matches",
			sa(StringAttributeConfig{Key: "http.route", Values: []string{"/healthz"}, InvertMatch: true}),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"http.route": "/checkout"}}),
			Decision{}},
		{"invertMatch with the key absent abstains",
			sa(StringAttributeConfig{Key: "http.route", Values: []string{"/healthz"}, InvertMatch: true}),
			mkTrace(1, nil, spanDef{}), Decision{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := mustNew(t, tc.cfg).Decide(tc.tr); got != tc.want {
				t.Fatalf("Decide = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// The regex cache is keyed by attribute VALUES, i.e. by the wire. It must be
// bounded, and it must keep answering correctly while it is being bounded —
// this repo has fixed two unbounded-map bugs this week.
func TestStringAttributeRegexCacheIsBounded(t *testing.T) {
	t.Parallel()
	p := pol("attr", TypeStringAttribute)
	p.StringAttribute = &StringAttributeConfig{
		Key: "k", Values: []string{"^/api/"}, EnabledRegexMatching: true, CacheMaxSize: 16,
	}
	e := mustNew(t, p)
	sp := e.policies[0].p.(*stringAttrPolicy)

	for i := 0; i < 1000; i++ {
		val := "/api/" + strings.Repeat("x", i%7) + string(rune('a'+i%26)) + string(rune('0'+i%10))
		if !sp.match(val) {
			t.Fatalf("%q should match ^/api/", val)
		}
		if sp.match("/other/" + val) { // anchored pattern, and a cached miss
			t.Fatalf("%q must not match ^/api/", "/other/"+val)
		}
		sp.cache.mu.Lock()
		n := len(sp.cache.m)
		sp.cache.mu.Unlock()
		if n > 16 {
			t.Fatalf("cache holds %d entries, cap is 16", n)
		}
	}
	// A value longer than the per-key budget is matched but never stored: the
	// count bound alone does not bound bytes.
	long := "/api/" + strings.Repeat("y", maxRegexCacheKeyBytes+1)
	if !sp.match(long) {
		t.Fatal("a long value must still match")
	}
	sp.cache.mu.Lock()
	_, cached := sp.cache.m[long]
	sp.cache.mu.Unlock()
	if cached {
		t.Fatalf("a %d-byte key was cached; the cache is bounded in entries only", len(long))
	}
}

// --- numericAttribute -------------------------------------------------------

func TestNumericAttributePolicy(t *testing.T) {
	t.Parallel()
	na := func(cfg NumericAttributeConfig) PolicyConfig {
		p := pol("num", TypeNumericAttribute)
		p.NumericAttribute = &cfg
		return p
	}
	for _, tc := range []struct {
		name string
		cfg  PolicyConfig
		tr   Trace
		want bool
	}{
		{"in range", na(NumericAttributeConfig{Key: "http.status_code", MinValue: i64p(500), MaxValue: i64p(599)}),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"http.status_code": 503}}), true},
		{"min is inclusive", na(NumericAttributeConfig{Key: "k", MinValue: i64p(500), MaxValue: i64p(599)}),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"k": 500}}), true},
		{"max is inclusive", na(NumericAttributeConfig{Key: "k", MinValue: i64p(500), MaxValue: i64p(599)}),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"k": 599}}), true},
		{"just below", na(NumericAttributeConfig{Key: "k", MinValue: i64p(500), MaxValue: i64p(599)}),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"k": 499}}), false},
		{"just above", na(NumericAttributeConfig{Key: "k", MinValue: i64p(500), MaxValue: i64p(599)}),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"k": 600}}), false},
		{"min only leaves the top open", na(NumericAttributeConfig{Key: "k", MinValue: i64p(500)}),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"k": 1 << 40}}), true},
		{"max only leaves the bottom open", na(NumericAttributeConfig{Key: "k", MaxValue: i64p(0)}),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"k": -5}}), true},
		{"a double is compared, not ignored", na(NumericAttributeConfig{Key: "k", MinValue: i64p(1), MaxValue: i64p(2)}),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"k": 1.5}}), true},
		{"a double outside the range", na(NumericAttributeConfig{Key: "k", MinValue: i64p(1), MaxValue: i64p(2)}),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"k": 2.5}}), false},
		{"a string value is not parsed", na(NumericAttributeConfig{Key: "k", MinValue: i64p(1), MaxValue: i64p(2)}),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"k": "1"}}), false},
		{"resource attributes count too", na(NumericAttributeConfig{Key: "k", MinValue: i64p(1)}),
			mkTrace(1, map[string]any{"k": 7}, spanDef{}), true},
		{"missing key", na(NumericAttributeConfig{Key: "k", MinValue: i64p(1)}),
			mkTrace(1, nil, spanDef{}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := mustNew(t, tc.cfg).Decide(tc.tr); got.Sampled != tc.want {
				t.Fatalf("Sampled = %v, want %v", got.Sampled, tc.want)
			}
		})
	}
}

// --- booleanAttribute -------------------------------------------------------

func TestBooleanAttributePolicy(t *testing.T) {
	t.Parallel()
	ba := func(key string, want bool) PolicyConfig {
		p := pol("bool", TypeBooleanAttribute)
		p.BooleanAttribute = &BooleanAttributeConfig{Key: key, Value: boolp(want)}
		return p
	}
	for _, tc := range []struct {
		name string
		cfg  PolicyConfig
		tr   Trace
		want bool
	}{
		{"true matches true", ba("sampled.hint", true),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"sampled.hint": true}}), true},
		{"true does not match false", ba("k", true),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"k": false}}), false},
		{"false matches false", ba("k", false),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"k": false}}), true},
		{"a string 'true' is not a bool", ba("k", true),
			mkTrace(1, nil, spanDef{attrs: map[string]any{"k": "true"}}), false},
		{"resource attributes count too", ba("k", true),
			mkTrace(1, map[string]any{"k": true}, spanDef{}), true},
		{"missing key", ba("k", false), mkTrace(1, nil, spanDef{}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := mustNew(t, tc.cfg).Decide(tc.tr); got.Sampled != tc.want {
				t.Fatalf("Sampled = %v, want %v", got.Sampled, tc.want)
			}
		})
	}
}

// --- probabilistic ----------------------------------------------------------

func probPol(pct float64) PolicyConfig {
	p := pol("prob", TypeProbabilistic)
	p.Probabilistic = &ProbabilisticConfig{SamplingPercentage: pct}
	return p
}

// The keep rate must track the configured percentage, and — the property the
// whole design rests on — the SAME trace id must decide identically in a
// different evaluator, which is what makes a decision taken on one node (or
// after a restart, or on a re-assembled trace) agree with every other.
func TestProbabilisticIsProportionalAndConsistent(t *testing.T) {
	t.Parallel()
	const n = 100_000
	for _, pct := range []float64{0, 1, 10, 50, 99, 100} {
		a := mustNew(t, probPol(pct))
		b := mustNew(t, probPol(pct))
		kept := 0
		for i := uint64(0); i < n; i++ {
			tr := Trace{TraceID: traceID(i)}
			da, db := a.Decide(tr), b.Decide(tr)
			if da != db {
				t.Fatalf("id %d decided %+v in one evaluator and %+v in another", i, da, db)
			}
			if da.Sampled {
				kept++
			}
		}
		want := pct / 100 * n
		// A three-sigma binomial band, floored so the 0%/100% ends are exact.
		sigma := 3 * (0.5 * float64(n) / 100) // ~1.5% of n, comfortably wide
		if pct == 0 || pct == 100 {
			sigma = 0
		}
		if float64(kept) < want-sigma || float64(kept) > want+sigma {
			t.Errorf("at %v%%: kept %d of %d, want %v +/- %v", pct, kept, n, want, sigma)
		}
	}
}

// Sampling is a function of the trace ID ONLY: the spans present, their count
// and their contents cannot change the answer. That is what lets a partially
// assembled trace be judged the same as a complete one.
func TestProbabilisticIgnoresTheSpanSet(t *testing.T) {
	t.Parallel()
	e := mustNew(t, probPol(50))
	for i := uint64(0); i < 1000; i++ {
		id := traceID(i)
		bare := Trace{TraceID: id}
		full := mkTrace(1, map[string]any{"service.name": "x"},
			spanDef{start: 0, end: 100, status: ptrace.StatusCodeError},
			spanDef{start: 5, end: 90})
		full.TraceID = id
		if a, b := e.Decide(bare), e.Decide(full); a != b {
			t.Fatalf("id %d: bare %+v vs assembled %+v", i, a, b)
		}
	}
}

// --- rateLimiting -----------------------------------------------------------

func ratePol(sps float64) PolicyConfig {
	p := pol("rate", TypeRateLimiting)
	p.RateLimiting = &RateLimitingConfig{SpansPerSecond: sps}
	return p
}

func TestRateLimitingBoundsAndRefills(t *testing.T) {
	t.Parallel()
	e := mustNew(t, ratePol(10))
	now := time.Unix(0, 0)
	fixedClock(e, &now)

	tr := mkTrace(1, nil, spanDef{start: 0, end: 1}, spanDef{start: 0, end: 1}) // 2 spans
	kept := 0
	for i := 0; i < 20; i++ { // 40 spans offered in one instant against a 10-span burst
		if e.Decide(tr).Sampled {
			kept++
		}
	}
	if kept != 5 {
		t.Fatalf("kept %d traces (%d spans) against a 10-span burst, want 5", kept, kept*2)
	}
	now = now.Add(500 * time.Millisecond) // refills 5 tokens
	kept = 0
	for i := 0; i < 20; i++ {
		if e.Decide(tr).Sampled {
			kept++
		}
	}
	if kept != 2 {
		t.Fatalf("kept %d traces after a half-second refill of 5 spans, want 2", kept)
	}
}

// A trace with more spans than the whole budget must not shut the policy
// forever: it is admitted from a full bucket and the overspend is carried as
// debt, so the long-run rate is still what was configured.
func TestRateLimitingAdmitsOversizedTracesAndCarriesTheDebt(t *testing.T) {
	t.Parallel()
	e := mustNew(t, ratePol(10))
	now := time.Unix(0, 0)
	fixedClock(e, &now)

	defs := make([]spanDef, 50)
	big := mkTrace(1, nil, defs...)
	if !e.Decide(big).Sampled {
		t.Fatal("a 50-span trace against a 10 spans/second budget was refused from a full bucket; that policy would never sample this workload again")
	}
	// 40 spans of debt: nothing is admitted until it is paid off.
	small := mkTrace(2, nil, spanDef{})
	for s := 1; s <= 4; s++ {
		now = now.Add(time.Second)
		if e.Decide(small).Sampled {
			t.Fatalf("admitted at t+%ds while %d spans of debt were outstanding", s, 40-10*s)
		}
	}
	now = now.Add(time.Second)
	if !e.Decide(small).Sampled {
		t.Fatal("still refusing after the debt was paid off")
	}
}

// --- and --------------------------------------------------------------------

func TestAndPolicy(t *testing.T) {
	t.Parallel()
	and := func(subs ...PolicyConfig) PolicyConfig {
		p := pol("and", TypeAnd)
		p.And = &AndConfig{SubPolicies: subs}
		return p
	}
	errStatus := func(name string) PolicyConfig {
		p := pol(name, TypeStatusCode)
		p.StatusCode = &StatusCodeConfig{StatusCodes: []string{"ERROR"}}
		return p
	}
	tenant := func(name, value string, invert bool) PolicyConfig {
		p := pol(name, TypeStringAttribute)
		p.StringAttribute = &StringAttributeConfig{Key: "tenant", Values: []string{value}, InvertMatch: invert}
		return p
	}

	errAcme := mkTrace(1, map[string]any{"tenant": "acme"}, spanDef{start: 0, end: 1, status: ptrace.StatusCodeError})
	okAcme := mkTrace(2, map[string]any{"tenant": "acme"}, spanDef{start: 0, end: 1})
	errGlobex := mkTrace(3, map[string]any{"tenant": "globex"}, spanDef{start: 0, end: 1, status: ptrace.StatusCodeError})

	for _, tc := range []struct {
		name string
		cfg  PolicyConfig
		tr   Trace
		want Decision
	}{
		{"both match", and(errStatus("errors"), tenant("acme", "acme", false)), errAcme,
			Decision{Sampled: true, Policy: "and"}},
		{"one fails", and(errStatus("errors"), tenant("acme", "acme", false)), okAcme, Decision{}},
		{"the other fails", and(errStatus("errors"), tenant("acme", "acme", false)), errGlobex, Decision{}},
		// A veto inside an AND is still a veto: it ends the evaluation rather
		// than merely failing the conjunction and letting a later policy in the
		// outer list resurrect the trace (pinned by the ordering test below).
		{"a nested veto propagates", and(errStatus("errors"), tenant("not-globex", "globex", true)), errGlobex,
			Decision{Sampled: false, Policy: "and"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := mustNew(t, tc.cfg).Decide(tc.tr); got != tc.want {
				t.Fatalf("Decide = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// A veto from inside an AND must stop the whole list, not just the AND.
func TestAndVetoStopsTheOuterList(t *testing.T) {
	t.Parallel()
	nested := pol("exclude-globex", TypeStringAttribute)
	nested.StringAttribute = &StringAttributeConfig{Key: "tenant", Values: []string{"globex"}, InvertMatch: true}
	andCfg := pol("and", TypeAnd)
	andCfg.And = &AndConfig{SubPolicies: []PolicyConfig{nested}}

	e := mustNew(t, andCfg, pol("catch-all", TypeAlwaysSample))
	tr := mkTrace(1, map[string]any{"tenant": "globex"}, spanDef{start: 0, end: 1})
	if got := e.Decide(tr); got.Sampled || got.Policy != "and" {
		t.Fatalf("Decide = %+v, want a veto from \"and\" (alwaysSample must not run)", got)
	}
}

// --- composite --------------------------------------------------------------

func compositeCfg(total float64, order []string, alloc []RateAllocationConfig, subs ...PolicyConfig) PolicyConfig {
	p := pol("composite", TypeComposite)
	p.Composite = &CompositeConfig{
		MaxTotalSpansPerSecond: total,
		PolicyOrder:            order,
		SubPolicies:            subs,
		RateAllocation:         alloc,
	}
	return p
}

// The composite's contract: ordered sub-policies, each with its own slice of
// the budget, and a sub-policy that is over budget yields to the next one
// instead of blocking it.
func TestCompositeAllocatesPerSubPolicyBudgets(t *testing.T) {
	t.Parallel()
	errs := pol("errors", TypeStatusCode)
	errs.StatusCode = &StatusCodeConfig{StatusCodes: []string{"ERROR"}}
	rest := pol("rest", TypeAlwaysSample)

	e := mustNew(t, compositeCfg(10, nil, []RateAllocationConfig{{Policy: "errors", Percent: 50}}, errs, rest))
	now := time.Unix(0, 0)
	fixedClock(e, &now)

	// errors: 5 spans/s, rest: the leftover 50% = 5 spans/s. One-span traces.
	errTrace := mkTrace(1, nil, spanDef{start: 0, end: 1, status: ptrace.StatusCodeError})
	okTrace := mkTrace(2, nil, spanDef{start: 0, end: 1})

	byPolicy := map[string]int{}
	for i := 0; i < 20; i++ {
		if d := e.Decide(errTrace); d.Sampled {
			byPolicy[d.Policy]++
		}
	}
	if byPolicy["composite/errors"] != 5 {
		t.Fatalf("errors sub-policy admitted %d, want its 5-span allocation (%v)", byPolicy["composite/errors"], byPolicy)
	}
	// The error budget is spent, but the error traces still MATCH the first
	// sub-policy — they must fall through to "rest", which has its own budget.
	if byPolicy["composite/rest"] != 5 {
		t.Fatalf("over-budget error traces did not fall through to \"rest\" (%v)", byPolicy)
	}
	// Both budgets are now spent; nothing else fits until a refill.
	if d := e.Decide(okTrace); d.Sampled {
		t.Fatalf("admitted %+v with both allocations exhausted", d)
	}
	now = now.Add(time.Second)
	if d := e.Decide(okTrace); !d.Sampled || d.Policy != "composite/rest" {
		t.Fatalf("after a refill: %+v, want composite/rest", d)
	}
}

// policyOrder decides which sub-policy gets first refusal, and the Decision
// names the sub-policy rather than the composite — "why was this kept" has to
// resolve to a rule, and a composite is a container.
func TestCompositeOrderAndAttribution(t *testing.T) {
	t.Parallel()
	a := pol("a", TypeAlwaysSample)
	b := pol("b", TypeAlwaysSample)
	tr := mkTrace(1, nil, spanDef{start: 0, end: 1})

	for _, tc := range []struct {
		order []string
		want  string
	}{
		{nil, "composite/a"},
		{[]string{"a", "b"}, "composite/a"},
		{[]string{"b", "a"}, "composite/b"},
	} {
		e := mustNew(t, compositeCfg(100, tc.order, nil, a, b))
		now := time.Unix(0, 0)
		fixedClock(e, &now)
		if got := e.Decide(tr); !got.Sampled || got.Policy != tc.want {
			t.Fatalf("order %v: Decide = %+v, want %q", tc.order, got, tc.want)
		}
	}
}

func TestCompositeVetoPropagates(t *testing.T) {
	t.Parallel()
	excl := pol("exclude", TypeStringAttribute)
	excl.StringAttribute = &StringAttributeConfig{Key: "tenant", Values: []string{"globex"}, InvertMatch: true}
	all := pol("rest", TypeAlwaysSample)

	e := mustNew(t, compositeCfg(100, nil, nil, excl, all), pol("catch-all", TypeAlwaysSample))
	now := time.Unix(0, 0)
	fixedClock(e, &now)
	tr := mkTrace(1, map[string]any{"tenant": "globex"}, spanDef{start: 0, end: 1})
	if got := e.Decide(tr); got.Sampled || got.Policy != "composite/exclude" {
		t.Fatalf("Decide = %+v, want a veto named composite/exclude", got)
	}
}

// errPolicy used to render its args with an inner Sprintf before the outer
// Errorf saw them, which made %w structurally impossible: every call site that
// carried a real error had to spell it %v, and the leaf — a regexp syntax
// error, a duration parse error — was flattened to text at the package
// boundary. Nothing outside could inspect it, and no linter could see the
// problem because the %v was on a local helper.
//
// These assert the chain, not the message: a caller must be able to reach the
// wrapped error with errors.As/errors.Unwrap while the rendered text still
// names the offending policy.
func TestCompileErrorsWrapTheirCause(t *testing.T) {
	t.Parallel()
	t.Run("regexp", func(t *testing.T) {
		p := pol("route", TypeStringAttribute)
		p.StringAttribute = &StringAttributeConfig{
			Key: "http.route", Values: []string{"(unclosed"}, EnabledRegexMatching: true,
		}
		_, err := New(Config{Policies: []PolicyConfig{p}})
		if err == nil {
			t.Fatal("an unparseable regex compiled")
		}
		var se *syntax.Error
		if !errors.As(err, &se) {
			t.Fatalf("errors.As cannot reach regexp's *syntax.Error through the policy compiler: %v", err)
		}
		if !strings.Contains(err.Error(), `"route"`) {
			t.Errorf("the error no longer names the offending policy: %v", err)
		}
	})

	t.Run("duration", func(t *testing.T) {
		p := pol("slow", TypeLatency)
		p.Latency = &LatencyConfig{Threshold: "10 seconds"} // not a Go duration
		_, err := New(Config{Policies: []PolicyConfig{p}})
		if err == nil {
			t.Fatal("an unparseable duration compiled")
		}
		// errPolicy -> config.Duration -> time.ParseDuration. Unwrap to the
		// LEAF rather than counting hops: how many wrappers the helper adds is
		// its own business (it formats in two steps so go vet keeps checking
		// the call sites), while reaching the cause is the guarantee.
		if errors.Unwrap(err) == nil {
			t.Fatalf("errPolicy did not wrap its cause: %v", err)
		}
		leaf := err
		for next := errors.Unwrap(leaf); next != nil; next = errors.Unwrap(leaf) {
			leaf = next
		}
		_, want := time.ParseDuration("10 seconds")
		if leaf.Error() != want.Error() {
			t.Errorf("leaf = %v, want time.ParseDuration's %v", leaf, want)
		}
		if !strings.Contains(err.Error(), `"slow"`) {
			t.Errorf("the error no longer names the offending policy: %v", err)
		}
	})

	t.Run("status-code", func(t *testing.T) {
		p := pol("errors", TypeStatusCode)
		p.StatusCode = &StatusCodeConfig{StatusCodes: []string{"BOOM"}}
		_, err := New(Config{Policies: []PolicyConfig{p}})
		if err == nil {
			t.Fatal("an unknown status code compiled")
		}
		if errors.Unwrap(err) == nil {
			t.Fatalf("errPolicy did not wrap its cause: %v", err)
		}
	})
}
