package metrics

import (
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// benchRules is a typical log-metrics config: a counted selector with masked
// labels, a line-content counter, and a windowed gauge over a JSON field.
func benchRules(b testing.TB) *DynamicMetricSet {
	b.Helper()
	set, err := newTestSet([]Dynamic{
		{
			Name:   "http_requests_total",
			Type:   CounterType,
			Value:  "1",
			Match:  []string{"level=info"},
			Labels: []string{"status=$http_status(_xx)", "method=$method"},
		},
		{
			Name:        "errors_total",
			Type:        CounterType,
			Value:       "1",
			MatchRegexp: []string{"level=(error|fatal)"},
		},
		{
			Name:   "latency_avg_ms",
			Type:   GaugeType,
			Action: "avg",
			Value:  "latency_ms",
			Match:  []string{"level=info"},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	return set
}

func benchResource() pcommon.Map {
	m := pcommon.NewMap()
	m.PutStr("k8s.namespace.name", "prod-payments")
	m.PutStr("k8s.pod.name", "payments-6f7b9c001")
	m.PutStr("k8s.container.name", "app")
	m.PutStr("k8s.node.name", "node-7")
	m.PutStr("service.name", "payments")
	m.PutStr("service.namespace", "prod-payments")
	m.PutStr("service.instance.id", "abc123def456")
	return m
}

// BenchmarkDynamicAddAttrs measures Add when the caller's lookup resolves the
// keys (record/resource attributes), the tailer's common case.
func BenchmarkDynamicAddAttrs(b *testing.B) {
	setTimeForTest(time.Unix(1_700_400_000, 0))
	defer testEpoch.Store(0)
	set := benchRules(b)
	res := benchResource()
	attrs := map[string]string{
		"level": "info", "http_status": "200", "method": "GET", "latency_ms": "42.5",
	}
	lookup := func(k string) string { return attrs[k] }
	values := func(k string) (float64, bool) {
		if k == "latency_ms" {
			return 42.5, true
		}
		return 0, false
	}
	line := `GET /api/v1/orders 200 42.5ms`
	b.ReportAllocs()
	for b.Loop() {
		set.Add(values, lookup, res, line)
	}
}

// BenchmarkDynamicAddJSONLine measures Add when keys resolve from the JSON
// line itself (the line-fields fallback path).
func BenchmarkDynamicAddJSONLine(b *testing.B) {
	setTimeForTest(time.Unix(1_700_400_100, 0))
	defer testEpoch.Store(0)
	set := benchRules(b)
	res := benchResource()
	line := `{"level":"info","http_status":200,"method":"GET","latency_ms":42.5,"msg":"handled request","path":"/api/v1/orders"}`
	b.ReportAllocs()
	for b.Loop() {
		set.Add(nil, nil, res, line)
	}
}

// BenchmarkDynamicAddLogfmtLine measures Add when keys resolve from a logfmt
// line (the other line-fields format; the JSON sibling above shares the
// KeyIndex/Fields machinery but not its per-key storage path).
func BenchmarkDynamicAddLogfmtLine(b *testing.B) {
	setTimeForTest(time.Unix(1_700_400_100, 0))
	defer testEpoch.Store(0)
	set := benchRules(b)
	res := benchResource()
	line := `level=info http_status=200 method=GET latency_ms=42.5 msg="handled request" path=/api/v1/orders`
	b.ReportAllocs()
	for b.Loop() {
		set.Add(nil, nil, res, line)
	}
}

// benchHistogramRules is a histogram log-metric with the default 14 bounds, so
// one matched line touches 15 bucket streams.
func benchHistogramRules(b testing.TB) *DynamicMetricSet {
	b.Helper()
	set, err := newTestSet([]Dynamic{{
		Name:   "request_duration_seconds",
		Type:   HistogramType,
		Value:  "latency_s",
		Match:  []string{"level=info"},
		Labels: []string{"method=$method", "status=$http_status"},
	}})
	if err != nil {
		b.Fatal(err)
	}
	return set
}

// BenchmarkDynamicAddHistogram measures a matched line against a histogram
// metric: observe walks every bucket stream, which is the hottest multiplier in
// the store (one line = len(buckets)+1 map probes at best).
func BenchmarkDynamicAddHistogram(b *testing.B) {
	setTimeForTest(time.Unix(1_700_400_400, 0))
	defer testEpoch.Store(0)
	set := benchHistogramRules(b)
	res := benchResource()
	attrs := map[string]string{"level": "info", "http_status": "200", "method": "GET"}
	lookup := func(k string) string { return attrs[k] }
	values := func(k string) (float64, bool) {
		if k == "latency_s" {
			return 0.42, true
		}
		return 0, false
	}
	line := `GET /api/v1/orders 200 0.42s`
	bound := set.Bind(res)
	b.ReportAllocs()
	for b.Loop() {
		bound.Add(values, lookup, line)
	}
}

// BenchmarkDynamicAddNoMatch measures the fast path: a line matching no rule.
func BenchmarkDynamicAddNoMatch(b *testing.B) {
	setTimeForTest(time.Unix(1_700_400_200, 0))
	defer testEpoch.Store(0)
	set := benchRules(b)
	res := benchResource()
	attrs := map[string]string{"level": "debug"}
	lookup := func(k string) string { return attrs[k] }
	line := `debug: cache refresh completed in 3ms`
	b.ReportAllocs()
	for b.Loop() {
		set.Add(nil, lookup, res, line)
	}
}

// BenchmarkExport measures rendering 100 series to OTLP.
func BenchmarkExport(b *testing.B) {
	setTimeForTest(time.Unix(1_700_400_300, 0))
	defer testEpoch.Store(0)
	set := benchRules(b)
	res := benchResource()
	for i := 0; i < 100; i++ {
		attrs := map[string]string{
			"level": "info", "http_status": fmt.Sprintf("%d", 200+i%5), "method": "GET",
		}
		set.Add(nil, func(k string) string { return attrs[k] }, res, "")
	}
	exp := &capExporter{}
	b.ReportAllocs()
	for b.Loop() {
		exp.md = nil
		if err := set.Export(b.Context(), exp, 0); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDynamicAddBound measures the path the tailer flush actually takes:
// the resource is hashed once per flush via Bind, and every line of that file
// reuses it. The unbound Add benchmarks re-hash the resource per line, which no
// production caller does, so they understate the share of the remaining work
// that the data-point label hashing represents.
func BenchmarkDynamicAddBound(b *testing.B) {
	setTimeForTest(time.Unix(1_700_400_300, 0))
	defer testEpoch.Store(0)
	set := benchRules(b)
	res := benchResource()
	attrs := map[string]string{
		"level": "info", "http_status": "200", "method": "GET", "latency_ms": "42.5",
	}
	lookup := func(k string) string { return attrs[k] }
	values := func(k string) (float64, bool) {
		if k == "latency_ms" {
			return 42.5, true
		}
		return 0, false
	}
	line := `GET /api/v1/orders 200 42.5ms`
	bound := set.Bind(res)
	b.ReportAllocs()
	for b.Loop() {
		bound.Add(values, lookup, line)
	}
}

// The benchmarks above REPORT the per-line budget; a benchmark cannot fail a
// build, so these are what hold it. Add runs on the tailer's and journald's
// per-line path for every log line on every node, and the whole pooled-context /
// bound-closure / precomputed-KeyIndex design exists to keep it near zero. A
// per-line closure, a map literal, a fmt call or an interface boxing in the
// observe path costs one allocation each and nothing else would notice.
func TestDynamicAddAllocationBudget(t *testing.T) {
	if raceEnabled {
		t.Skip("-race perturbs allocation counts")
	}
	// Bound once, outside every measured call, exactly as the tailer's flush
	// binds them once per flush. Building them inside the measured closure
	// would measure the test harness.
	infoAttrs := map[string]string{
		"level": "info", "http_status": "200", "method": "GET", "latency_ms": "42.5",
	}
	infoLookup := func(k string) string { return infoAttrs[k] }
	infoValues := func(k string) (float64, bool) {
		if k == "latency_ms" {
			return 42.5, true
		}
		return 0, false
	}
	debugAttrs := map[string]string{"level": "debug"}
	debugLookup := func(k string) string { return debugAttrs[k] }

	for _, tc := range []struct {
		name    string
		ceiling float64
		add     func(set *DynamicMetricSet, res pcommon.Map) func()
	}{
		// The tailer's shape: keys resolve from record/resource attributes
		// through the caller's bound closures. CLAUDE.md's "matched-line
		// ~1 alloc".
		{"attrs", 1.5, func(set *DynamicMetricSet, res pcommon.Map) func() {
			return func() { set.Add(infoValues, infoLookup, res, `GET /api/v1/orders 200 42.5ms`) }
		}},
		// The bound shape: the flush hashes the file's resource once and every
		// line of that file reuses it.
		{"bound", 1.5, func(set *DynamicMetricSet, res pcommon.Map) func() {
			bound := set.Bind(res)
			return func() { bound.Add(infoValues, infoLookup, `GET /api/v1/orders 200 42.5ms`) }
		}},
		// The line-fields fallback: keys parse straight off a logfmt line.
		{"logfmt", 1.5, func(set *DynamicMetricSet, res pcommon.Map) func() {
			return func() {
				set.Add(nil, nil, res, `level=info http_status=200 method=GET latency_ms=42.5 msg="handled request"`)
			}
		}},
		// The JSON fallback decodes through GetPaths into the reused Fields
		// buffer; it pays a little more than logfmt but must stay a small
		// constant, not a function of the line's field count.
		{"json", 3.5, func(set *DynamicMetricSet, res pcommon.Map) func() {
			return func() {
				set.Add(nil, nil, res, `{"level":"info","http_status":200,"method":"GET","latency_ms":42.5,"msg":"handled request"}`)
			}
		}},
		// The fast path: a line matching no rule must allocate NOTHING. It is
		// the overwhelmingly common case on a busy node.
		{"nomatch", 0, func(set *DynamicMetricSet, res pcommon.Map) func() {
			return func() { set.Add(nil, debugLookup, res, `debug: cache refresh completed in 3ms`) }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setTimeForTest(time.Unix(1_700_600_000, 0))
			defer testEpoch.Store(0)
			set := benchRules(t)
			add := tc.add(set, benchResource())
			add() // admit the series and warm every pool
			if allocs := testing.AllocsPerRun(200, add); allocs > tc.ceiling {
				t.Fatalf("Add allocates %v times per line, want <= %v", allocs, tc.ceiling)
			}
		})
	}
}

// A histogram observation walks every bucket stream — the hottest multiplier in
// the store. It must not allocate per bucket.
func TestDynamicAddHistogramAllocationBudget(t *testing.T) {
	if raceEnabled {
		t.Skip("-race perturbs allocation counts")
	}
	setTimeForTest(time.Unix(1_700_600_100, 0))
	defer testEpoch.Store(0)
	set, err := newTestSet([]Dynamic{{
		Name:   "request_duration_seconds",
		Type:   HistogramType,
		Value:  "latency_s",
		Match:  []string{"level=info"},
		Labels: []string{"method=$method", "status=$http_status"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	attrs := map[string]string{"level": "info", "http_status": "200", "method": "GET"}
	lookup := func(k string) string { return attrs[k] }
	values := func(k string) (float64, bool) {
		if k == "latency_s" {
			return 0.42, true
		}
		return 0, false
	}
	bound := set.Bind(benchResource())
	bound.Add(values, lookup, `GET /api/v1/orders 200 0.42s`)
	allocs := testing.AllocsPerRun(200, func() {
		bound.Add(values, lookup, `GET /api/v1/orders 200 0.42s`)
	})
	if allocs > 0.5 {
		t.Fatalf("a histogram observation allocates %v times, want ~0 (15 bucket streams "+
			"per line: anything per-bucket here is a per-line multiplier)", allocs)
	}
}
