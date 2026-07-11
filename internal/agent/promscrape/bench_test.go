package promscrape

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// k8sScrapeBody synthesizes a typical Kubernetes workload exposition: repeated
// namespace/pod labels, counters, gauges and histograms with 12 buckets.
func k8sScrapeBody(series int) string {
	var sb strings.Builder
	sb.WriteString("# TYPE http_requests_total counter\n")
	for i := 0; i < series; i++ {
		fmt.Fprintf(&sb, "http_requests_total{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",container=\"app\",method=\"GET\",code=\"200\",path=\"/api/v1/orders\"} %d\n", i%40, i*7)
	}
	sb.WriteString("# TYPE process_resident_memory_bytes gauge\n")
	for i := 0; i < series/4; i++ {
		fmt.Fprintf(&sb, "process_resident_memory_bytes{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\"} %d\n", i%40, 100000000+i)
	}
	sb.WriteString("# TYPE http_request_duration_seconds histogram\n")
	bounds := []string{"0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "+Inf"}
	for i := 0; i < series/8; i++ {
		for bi, le := range bounds {
			fmt.Fprintf(&sb, "http_request_duration_seconds_bucket{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",handler=\"/api\",le=\"%s\"} %d\n", i%40, le, (bi+1)*10)
		}
		fmt.Fprintf(&sb, "http_request_duration_seconds_sum{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",handler=\"/api\"} 42.5\n", i%40)
		fmt.Fprintf(&sb, "http_request_duration_seconds_count{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",handler=\"/api\"} 120\n", i%40)
	}
	return sb.String()
}

// BenchmarkConvertScrape measures the full parse -> filter -> convert -> OTLP
// pipeline for a typical Kubernetes exposition.
func BenchmarkConvertScrape(b *testing.B) {
	input := k8sScrapeBody(4000)
	filter, err := newMetricFilter([]FilterRule{
		{Action: "keep", Metrics: "http_request_duration_seconds_bucket", Labels: map[string]string{"handler": "/api"}},
		{Action: "drop", Metrics: "(go_|promhttp_|process_start_).+"},
	})
	if err != nil {
		b.Fatal(err)
	}
	var points int
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bt := newBatcher(func(pcommon.Resource) {}, 1<<30, time.Unix(1, 0), time.Unix(2, 0))
		conv := newConverter(bt)
		fs := filter.session()
		p := NewParser(1<<20, false, false)
		_, err := p.Parse(strings.NewReader(input), func(s Sample) error {
			if !fs.Keep(s.Name, s.Labels) {
				return nil
			}
			conv.add(s)
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		conv.finish()
		points = bt.count()
	}
	if points == 0 {
		b.Fatal("no points")
	}
}
