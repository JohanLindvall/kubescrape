package tailsample

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// Decide sits on the assembly layer's decision path — once per trace, on
// whatever goroutine drains the buffer — so it must not allocate. Every
// benchmark here is expected to report 0 allocs/op; the numbers in the package
// history are ~10-60ns for a leaf policy.
//
// The two things that would break it are easy to add by accident: building a
// slice or a map per trace (there is no per-trace scratch anywhere, by design),
// and formatting a name (composite's qualified names are precomputed).

// benchTrace is a realistic assembled trace: a handful of spans over one
// resource, with the attributes a policy list actually looks at.
func benchTrace(spans int) Trace {
	defs := make([]spanDef, spans)
	for i := range defs {
		defs[i] = spanDef{
			start: int64(i), end: int64(100 + i*10),
			attrs: map[string]any{
				"http.route":       "/api/v1/orders",
				"http.status_code": 200,
				"sampling.debug":   false,
			},
		}
	}
	defs[spans-1].status = ptrace.StatusCodeError
	return mkTrace(1, map[string]any{"service.name": "checkout", "tenant": "acme"}, defs...)
}

func benchDecide(b *testing.B, tr Trace, policies ...PolicyConfig) {
	e := mustNew(b, policies...)
	e.Decide(tr) // warm anything cacheable (the regex cache)
	b.ReportAllocs()
	for b.Loop() {
		e.Decide(tr)
	}
}

func BenchmarkDecideLatency(b *testing.B) {
	p := pol("slow", TypeLatency)
	p.Latency = &LatencyConfig{Threshold: "100ms"}
	benchDecide(b, benchTrace(10), p)
}

func BenchmarkDecideStatusCode(b *testing.B) {
	p := pol("errors", TypeStatusCode)
	p.StatusCode = &StatusCodeConfig{StatusCodes: []string{"ERROR"}}
	benchDecide(b, benchTrace(10), p)
}

func BenchmarkDecideStringAttributeExact(b *testing.B) {
	p := pol("route", TypeStringAttribute)
	p.StringAttribute = &StringAttributeConfig{Key: "http.route", Values: []string{"/api/v1/orders"}}
	benchDecide(b, benchTrace(10), p)
}

// The regex path with a warm cache — the steady state for a route-shaped
// attribute, and the reason the cache exists at all.
func BenchmarkDecideStringAttributeRegexCached(b *testing.B) {
	p := pol("route", TypeStringAttribute)
	p.StringAttribute = &StringAttributeConfig{
		Key: "http.route", Values: []string{"^/api/v[0-9]+/orders$"}, EnabledRegexMatching: true,
	}
	benchDecide(b, benchTrace(10), p)
}

// The cold path: every value is a cache miss, so the regexes actually run.
func BenchmarkDecideStringAttributeRegexUncached(b *testing.B) {
	p := pol("route", TypeStringAttribute)
	p.StringAttribute = &StringAttributeConfig{
		Key: "http.route", Values: []string{"^/api/v[0-9]+/orders$"}, EnabledRegexMatching: true,
	}
	e := mustNew(b, p)
	sp := e.policies[0].p.(*stringAttrPolicy)
	tr := benchTrace(10)
	b.ReportAllocs()
	for b.Loop() {
		sp.cache.mu.Lock()
		clear(sp.cache.m)
		sp.cache.mu.Unlock()
		e.Decide(tr)
	}
}

func BenchmarkDecideNumericAttribute(b *testing.B) {
	p := pol("status", TypeNumericAttribute)
	p.NumericAttribute = &NumericAttributeConfig{Key: "http.status_code", MinValue: i64p(500), MaxValue: i64p(599)}
	benchDecide(b, benchTrace(10), p)
}

func BenchmarkDecideBooleanAttribute(b *testing.B) {
	p := pol("debug", TypeBooleanAttribute)
	p.BooleanAttribute = &BooleanAttributeConfig{Key: "sampling.debug", Value: boolp(true)}
	benchDecide(b, benchTrace(10), p)
}

func BenchmarkDecideProbabilistic(b *testing.B) {
	benchDecide(b, benchTrace(10), probPol(10))
}

// rateLimiting is the one leaf that reads the clock and takes a lock. The rate
// is set high enough that the bucket never binds, so this measures the admitted
// path rather than the refusal.
func BenchmarkDecideRateLimiting(b *testing.B) {
	benchDecide(b, benchTrace(10), ratePol(1e12))
}

func BenchmarkDecideAnd(b *testing.B) {
	route := pol("route", TypeStringAttribute)
	route.StringAttribute = &StringAttributeConfig{Key: "http.route", Values: []string{"/api/v1/orders"}}
	slow := pol("slow", TypeLatency)
	slow.Latency = &LatencyConfig{Threshold: "100ms"}
	p := pol("and", TypeAnd)
	p.And = &AndConfig{SubPolicies: []PolicyConfig{route, slow}}
	benchDecide(b, benchTrace(10), p)
}

func BenchmarkDecideComposite(b *testing.B) {
	errs := pol("errors", TypeStatusCode)
	errs.StatusCode = &StatusCodeConfig{StatusCodes: []string{"ERROR"}}
	benchDecide(b, benchTrace(10),
		compositeCfg(1e12, nil, []RateAllocationConfig{{Policy: "errors", Percent: 50}},
			errs, pol("rest", TypeAlwaysSample)))
}

// The worst case an operator actually writes: a list where nothing matches, so
// every policy runs and every span is walked by each of them. This is the
// number to watch — the per-trace cost is the LIST, not one policy.
func BenchmarkDecideNoMatch(b *testing.B) {
	excl := pol("exclude-healthz", TypeStringAttribute)
	excl.StringAttribute = &StringAttributeConfig{Key: "http.route", Values: []string{"/healthz"}, InvertMatch: true}
	errs := pol("errors", TypeStatusCode)
	errs.StatusCode = &StatusCodeConfig{StatusCodes: []string{"ERROR"}}
	slow := pol("slow", TypeLatency)
	slow.Latency = &LatencyConfig{Threshold: "10s"}
	num := pol("server-errors", TypeNumericAttribute)
	num.NumericAttribute = &NumericAttributeConfig{Key: "http.status_code", MinValue: i64p(500), MaxValue: i64p(599)}
	boolp := pol("debug", TypeBooleanAttribute)
	boolp.BooleanAttribute = &BooleanAttributeConfig{Key: "sampling.debug", Value: boolp2(true)}

	// A trace with no error span, so nothing in the list matches.
	defs := make([]spanDef, 10)
	for i := range defs {
		defs[i] = spanDef{start: int64(i), end: int64(10 + i), attrs: map[string]any{
			"http.route": "/api/v1/orders", "http.status_code": 200, "sampling.debug": false,
		}}
	}
	tr := mkTrace(1, map[string]any{"service.name": "checkout"}, defs...)
	benchDecide(b, tr, excl, errs, slow, num, boolp, probPol(0.0001))
}

// boolp2 exists because BenchmarkDecideNoMatch shadows the boolp helper with a
// policy of the same name; renaming the local would read worse than this.
func boolp2(b bool) *bool { return &b }

// One span versus many: every span-walking policy is linear in the assembled
// span count, which is the cost model the buffering layer needs when it decides
// how big a trace it is willing to hold.
func BenchmarkDecideManySpans(b *testing.B) {
	p := pol("errors", TypeStatusCode)
	p.StatusCode = &StatusCodeConfig{StatusCodes: []string{"OK"}} // never matches: full walk
	benchDecide(b, benchTrace(200), p)
}

// The benchmarks above REPORT the budget; a benchmark cannot fail a build, so
// this is what holds it. Decide runs once per assembled trace on the buffering
// layer's decision goroutine, and the package's whole shape — no per-trace
// scratch, precomputed composite names, a regex cache keyed by value — exists
// to keep it at zero. A slice, a map, a fmt call or a closure per trace would
// each cost one allocation and nothing else would notice.
func TestDecideAllocationBudget(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	excl := pol("exclude-healthz", TypeStringAttribute)
	excl.StringAttribute = &StringAttributeConfig{Key: "http.route", Values: []string{"/healthz"}, InvertMatch: true}
	errs := pol("errors", TypeStatusCode)
	errs.StatusCode = &StatusCodeConfig{StatusCodes: []string{"ERROR"}}
	slow := pol("slow", TypeLatency)
	slow.Latency = &LatencyConfig{Threshold: "10s"}
	num := pol("server-errors", TypeNumericAttribute)
	num.NumericAttribute = &NumericAttributeConfig{Key: "http.status_code", MinValue: i64p(500), MaxValue: i64p(599)}
	bl := pol("debug", TypeBooleanAttribute)
	bl.BooleanAttribute = &BooleanAttributeConfig{Key: "sampling.debug", Value: boolp2(true)}
	rx := pol("route-regex", TypeStringAttribute)
	rx.StringAttribute = &StringAttributeConfig{Key: "http.route", Values: []string{"/api/v[0-9]+/.*"}, EnabledRegexMatching: true}

	for _, tc := range []struct {
		name     string
		policies []PolicyConfig
		trace    Trace
	}{
		// The single-policy shapes: one leaf of each kind that walks spans.
		{"status-code", []PolicyConfig{errs}, benchTrace(10)},
		{"latency", []PolicyConfig{slow}, benchTrace(10)},
		{"string-attribute-regex", []PolicyConfig{rx}, benchTrace(10)},
		{"probabilistic", []PolicyConfig{probPol(0.5)}, benchTrace(10)},
		// The worst case an operator actually writes: a list where nothing
		// matches, so every policy runs and every span is walked by each.
		{"no-match-list", []PolicyConfig{excl, errs, slow, num, bl, probPol(0.0001)}, benchTrace(10)},
		// A deep trace: the per-span walk must not allocate per span either.
		{"many-spans", []PolicyConfig{errs}, benchTrace(200)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := mustNew(t, tc.policies...)
			tr := tc.trace
			e.Decide(tr) // warm the regex cache
			if allocs := testing.AllocsPerRun(200, func() { e.Decide(tr) }); allocs != 0 {
				t.Fatalf("Decide allocates %v times per trace, want 0", allocs)
			}
		})
	}
}
