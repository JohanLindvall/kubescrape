package metrics

import (
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// benchRules is a typical log-metrics config: a counted selector with masked
// labels, a line-content counter, and a windowed gauge over a JSON field.
func benchRules(b *testing.B) *DynamicMetricSet {
	b.Helper()
	set, err := NewDynamicMetricSet([]Dynamic{
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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.Add(nil, nil, res, line)
	}
}

// benchHistogramRules is a histogram log-metric with the default 14 bounds, so
// one matched line touches 15 bucket streams.
func benchHistogramRules(b *testing.B) *DynamicMetricSet {
	b.Helper()
	set, err := NewDynamicMetricSet([]Dynamic{{
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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bound.Add(values, lookup, line)
	}
}
