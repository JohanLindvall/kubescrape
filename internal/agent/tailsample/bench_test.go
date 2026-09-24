package tailsample

import (
	"fmt"
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
	b.Helper()
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
	p.NumericAttribute = &NumericAttributeConfig{Key: "http.status_code", MinValue: new(int64(500)), MaxValue: new(int64(599))}
	benchDecide(b, benchTrace(10), p)
}

func BenchmarkDecideBooleanAttribute(b *testing.B) {
	p := pol("debug", TypeBooleanAttribute)
	p.BooleanAttribute = &BooleanAttributeConfig{Key: "sampling.debug", Value: new(true)}
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
	num.NumericAttribute = &NumericAttributeConfig{Key: "http.status_code", MinValue: new(int64(500)), MaxValue: new(int64(599))}
	boolp := pol("debug", TypeBooleanAttribute)
	boolp.BooleanAttribute = &BooleanAttributeConfig{Key: "sampling.debug", Value: new(true)}

	benchDecide(b, noMatchTrace(10), excl, errs, slow, num, boolp, probPol(0.0001))
}

// noMatchTrace is benchTrace WITHOUT the error span: nothing in the no-match
// list (BenchmarkDecideNoMatch, TestDecideAllocationBudget) matches it, so every
// policy runs and walks every span. benchTrace itself cannot serve — its last
// span is an ERROR, so the errors policy samples and the rest never run.
func noMatchTrace(spans int) Trace {
	defs := make([]spanDef, spans)
	for i := range defs {
		defs[i] = spanDef{start: int64(i), end: int64(10 + i), attrs: map[string]any{
			"http.route": "/api/v1/orders", "http.status_code": 200, "sampling.debug": false,
		}}
	}
	return mkTrace(1, map[string]any{"service.name": "checkout"}, defs...)
}

// resourceKeyedTrace is a trace whose policy key lives on the RESOURCE, the
// realistic shape of a tenancy or namespace policy: an enriched resource of 25
// attributes and spans of 6, none of which carries the key. A policy on such a
// key that matches nothing has to consult the resource for every span, which
// is the cost anySpanMatches' per-handle memo removes.
func resourceKeyedTrace(spans int) Trace {
	res := map[string]any{"k8s.namespace.name": "checkout", "service.name": "checkout"}
	for i := len(res); i < 25; i++ {
		res[fmt.Sprintf("k8s.pod.label.label-%02d", i)] = "value"
	}
	defs := make([]spanDef, spans)
	for i := range defs {
		defs[i] = spanDef{start: int64(i), end: int64(10 + i), attrs: map[string]any{
			"http.route": "/api/v1/orders", "http.method": "GET", "http.status_code": 200,
			"net.peer.name": "db", "thread.id": 7, "sampling.debug": false,
		}}
	}
	return mkTrace(1, res, defs...)
}

// resourceKeyedPolicy never matches resourceKeyedTrace.
func resourceKeyedPolicy() PolicyConfig {
	p := pol("tenant", TypeStringAttribute)
	p.StringAttribute = &StringAttributeConfig{Key: "k8s.namespace.name", Values: []string{"payments"}}
	return p
}

// A resource-keyed policy that matches nothing, over a deep trace.
func BenchmarkDecideResourceKeyedNoMatch(b *testing.B) {
	benchDecide(b, resourceKeyedTrace(200), resourceKeyedPolicy())
}

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
//
// Every BUILT-IN policy type has a row, and every row asserts the decision it
// measures: a row whose trace an EARLIER policy decides never runs the later
// ones, and the no-match row once measured nothing past its second policy for
// exactly that reason. (script is not here: it costs whatever its injected
// decide(trace) body costs.)
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
	num.NumericAttribute = &NumericAttributeConfig{Key: "http.status_code", MinValue: new(int64(500)), MaxValue: new(int64(599))}
	bl := pol("debug", TypeBooleanAttribute)
	bl.BooleanAttribute = &BooleanAttributeConfig{Key: "sampling.debug", Value: new(true)}
	rx := pol("route-regex", TypeStringAttribute)
	rx.StringAttribute = &StringAttributeConfig{Key: "http.route", Values: []string{"/api/v[0-9]+/.*"}, EnabledRegexMatching: true}
	route := pol("route", TypeStringAttribute)
	route.StringAttribute = &StringAttributeConfig{Key: "http.route", Values: []string{"/api/v1/orders"}}
	fast := pol("fast", TypeLatency)
	fast.Latency = &LatencyConfig{Threshold: "100ms"}
	and := pol("and", TypeAnd)
	and.And = &AndConfig{SubPolicies: []PolicyConfig{route, fast}}
	composite := compositeCfg(1e12, nil, []RateAllocationConfig{{Policy: "errors", Percent: 50}},
		errs, pol("rest", TypeAlwaysSample))

	none := Decision{} // the no-opinion default drop
	for _, tc := range []struct {
		name     string
		policies []PolicyConfig
		trace    Trace
		// charged re-decides: the trace carries the first decision's spend, so
		// the measured evaluations take the Peek arm (Trace.Charged).
		charged bool
		// want is the decision measured; nil for probabilistic, whose answer
		// is a hash of the trace id and not the point of the row.
		want *Decision
	}{
		// One leaf of each kind.
		{name: "always", policies: []PolicyConfig{pol("a", TypeAlwaysSample)}, trace: benchTrace(10),
			want: &Decision{Sampled: true, Policy: "a"}},
		{name: "status-code", policies: []PolicyConfig{errs}, trace: benchTrace(10),
			want: &Decision{Sampled: true, Policy: "errors"}},
		{name: "latency", policies: []PolicyConfig{slow}, trace: benchTrace(10), want: &none},
		{name: "string-attribute-regex", policies: []PolicyConfig{rx}, trace: benchTrace(10),
			want: &Decision{Sampled: true, Policy: "route-regex"}},
		{name: "numeric-attribute", policies: []PolicyConfig{num}, trace: benchTrace(10), want: &none},
		{name: "boolean-attribute", policies: []PolicyConfig{bl}, trace: benchTrace(10), want: &none},
		{name: "probabilistic", policies: []PolicyConfig{probPol(0.5)}, trace: benchTrace(10)},
		// The two that take a lock and read the clock, at a rate that never
		// binds — and again as a re-decision, which peeks instead of spending.
		{name: "rate-limiting", policies: []PolicyConfig{ratePol(1e12)}, trace: benchTrace(10),
			want: &Decision{Sampled: true, Policy: "rate"}},
		{name: "rate-limiting-charged", policies: []PolicyConfig{ratePol(1e12)}, trace: benchTrace(10), charged: true,
			want: &Decision{Sampled: true, Policy: "rate"}},
		{name: "composite", policies: []PolicyConfig{composite}, trace: benchTrace(10),
			want: &Decision{Sampled: true, Policy: "composite/errors"}},
		{name: "composite-charged", policies: []PolicyConfig{composite}, trace: benchTrace(10), charged: true,
			want: &Decision{Sampled: true, Policy: "composite/errors"}},
		{name: "and", policies: []PolicyConfig{and}, trace: benchTrace(10),
			want: &Decision{Sampled: true, Policy: "and"}},
		// The worst case an operator actually writes: a list where nothing
		// matches, so every policy runs and every span is walked by each.
		{name: "no-match-list", policies: []PolicyConfig{excl, errs, slow, num, bl, probPol(0.0001)},
			trace: noMatchTrace(10), want: &none},
		// A deep trace: the per-span walk must not allocate per span either —
		// a span-keyed policy, and a resource-keyed one over an enriched
		// resource.
		{name: "many-spans", policies: []PolicyConfig{errs}, trace: benchTrace(200),
			want: &Decision{Sampled: true, Policy: "errors"}},
		{name: "resource-keyed-no-match", policies: []PolicyConfig{resourceKeyedPolicy()}, trace: resourceKeyedTrace(200),
			want: &none},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := mustNew(t, tc.policies...)
			tr := tc.trace
			first := e.Decide(tr) // also warms the regex cache
			if tc.charged {
				if first.Charged == 0 {
					t.Fatalf("the first decision %+v spent nothing, so the re-decision below is not a charged one", first)
				}
				tr.Charged = first.Charged
			}
			if d := e.Decide(tr); tc.want != nil && (d.Sampled != tc.want.Sampled || d.Policy != tc.want.Policy) {
				t.Fatalf("Decide = %+v, want %+v: the row does not measure what it names", d, *tc.want)
			}
			if allocs := testing.AllocsPerRun(200, func() { e.Decide(tr) }); allocs != 0 {
				t.Fatalf("Decide allocates %v times per trace, want 0", allocs)
			}
		})
	}
}
