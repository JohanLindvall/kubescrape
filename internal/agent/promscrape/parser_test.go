package promscrape

import (
	"math"
	"strings"
	"testing"
)

func parseAll(t *testing.T, input string) []Sample {
	t.Helper()
	p := NewParser(1 << 20)
	var out []Sample
	malformed, err := p.Parse(strings.NewReader(input), func(s Sample) error {
		cp := s
		cp.Labels = append([]Label(nil), s.Labels...)
		out = append(out, cp)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if malformed != 0 {
		t.Fatalf("%d malformed lines", malformed)
	}
	return out
}

func TestParseBasic(t *testing.T) {
	samples := parseAll(t, `
# HELP http_requests_total Total requests.
# TYPE http_requests_total counter
http_requests_total{method="get",code="200"} 1027 1395066363000
http_requests_total{method="post",code="200"} 3

# TYPE temperature gauge
temperature 36.5
untyped_metric{a="b"} 1
`)
	if len(samples) != 4 {
		t.Fatalf("got %d samples: %+v", len(samples), samples)
	}
	s := samples[0]
	if s.Name != "http_requests_total" || s.Kind != KindSum || s.Value != 1027 || s.TimestampMs != 1395066363000 {
		t.Fatalf("sample = %+v", s)
	}
	if len(s.Labels) != 2 || s.Labels[0] != (Label{"method", "get"}) || s.Labels[1] != (Label{"code", "200"}) {
		t.Fatalf("labels = %+v", s.Labels)
	}
	if samples[2].Kind != KindGauge || samples[2].Value != 36.5 {
		t.Fatalf("gauge = %+v", samples[2])
	}
	if samples[3].Kind != KindGauge {
		t.Fatalf("untyped should map to gauge: %+v", samples[3])
	}
}

func TestParseHistogramAndSummary(t *testing.T) {
	samples := parseAll(t, `
# TYPE http_duration histogram
http_duration_bucket{le="0.1"} 100
http_duration_bucket{le="+Inf"} 150
http_duration_sum 53.4
http_duration_count 150
# TYPE rpc_latency summary
rpc_latency{quantile="0.99"} 3.2
rpc_latency_sum 8000
rpc_latency_count 2000
`)
	kinds := map[string]SampleKind{}
	for _, s := range samples {
		key := s.Name
		if len(s.Labels) > 0 {
			key += "/" + s.Labels[0].Name
		}
		kinds[key] = s.Kind
	}
	expect := map[string]SampleKind{
		"http_duration_bucket/le": KindSum,
		"http_duration_sum":       KindSum,
		"http_duration_count":     KindSum,
		"rpc_latency/quantile":    KindGauge,
		"rpc_latency_sum":         KindSum,
		"rpc_latency_count":       KindSum,
	}
	for k, want := range expect {
		if kinds[k] != want {
			t.Errorf("%s: kind = %v, want %v", k, kinds[k], want)
		}
	}
}

func TestParseEscapesAndSpecials(t *testing.T) {
	samples := parseAll(t, `metric{path="C:\\temp\\x",msg="say \"hi\"\n"} +Inf
nan_metric NaN
neg{} -1.5e3
`)
	if len(samples) != 3 {
		t.Fatalf("got %d samples", len(samples))
	}
	if samples[0].Labels[0].Value != `C:\temp\x` {
		t.Errorf("backslash unescape: %q", samples[0].Labels[0].Value)
	}
	if samples[0].Labels[1].Value != "say \"hi\"\n" {
		t.Errorf("quote/newline unescape: %q", samples[0].Labels[1].Value)
	}
	if !math.IsInf(samples[0].Value, 1) {
		t.Errorf("value = %v, want +Inf", samples[0].Value)
	}
	if !math.IsNaN(samples[1].Value) {
		t.Errorf("value = %v, want NaN", samples[1].Value)
	}
	if samples[2].Value != -1500 {
		t.Errorf("value = %v", samples[2].Value)
	}
}

func TestParseMalformed(t *testing.T) {
	p := NewParser(1 << 20)
	var good int
	malformed, err := p.Parse(strings.NewReader(`ok_metric 1
{no_name} 1
bad_value x
missing_value
ok_metric2 2
`), func(s Sample) error { good++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if good != 2 || malformed != 3 {
		t.Fatalf("good=%d malformed=%d", good, malformed)
	}
}

func TestParseLongLineSkipped(t *testing.T) {
	p := NewParser(256)
	input := "short 1\nlong{x=\"" + strings.Repeat("a", 1000) + "\"} 2\nshort2 3\n"
	var names []string
	malformed, err := p.Parse(strings.NewReader(input), func(s Sample) error {
		names = append(names, s.Name)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if malformed != 1 || len(names) != 2 || names[0] != "short" || names[1] != "short2" {
		t.Fatalf("malformed=%d names=%v", malformed, names)
	}
}

func TestParseAbort(t *testing.T) {
	p := NewParser(1 << 20)
	count := 0
	_, err := p.Parse(strings.NewReader("a 1\nb 2\nc 3\n"), func(s Sample) error {
		count++
		if count == 2 {
			return ErrTooManySamples
		}
		return nil
	})
	if err != ErrTooManySamples || count != 2 {
		t.Fatalf("err=%v count=%d", err, count)
	}
}

func BenchmarkParseLargeScrape(b *testing.B) {
	var sb strings.Builder
	sb.WriteString("# TYPE bench_metric counter\n")
	for i := 0; i < 10_000; i++ {
		sb.WriteString("bench_metric_total{pod=\"pod-")
		sb.WriteString(strings.Repeat("x", 20))
		sb.WriteString("\",idx=\"")
		sb.WriteByte(byte('0' + i%10))
		sb.WriteString("\"} 12345.678\n")
	}
	input := sb.String()
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewParser(1 << 20)
		n := 0
		if _, err := p.Parse(strings.NewReader(input), func(s Sample) error { n++; return nil }); err != nil {
			b.Fatal(err)
		}
		if n != 10_000 {
			b.Fatalf("n=%d", n)
		}
	}
}
