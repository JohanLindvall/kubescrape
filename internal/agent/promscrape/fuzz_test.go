package promscrape

import (
	"bytes"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/promparse"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// fuzzSeedBodies are representative and adversarial exposition bodies shared
// by the parser and converter fuzz targets.
var fuzzSeedBodies = []string{
	// Representative classic exposition.
	"# HELP http_requests_total Total requests.\n" +
		"# TYPE http_requests_total counter\n" +
		"http_requests_total{code=\"200\",method=\"get\"} 1027 1395066363000\n" +
		"http_requests_total{code=\"400\",method=\"post\"} 3\n" +
		"# TYPE temp gauge\n" +
		"temp{host=\"a\"} -17.5\n",
	// Histogram + summary families.
	"# TYPE http_duration histogram\n" +
		"http_duration_bucket{le=\"0.1\"} 100\n" +
		"http_duration_bucket{le=\"0.5\"} 140\n" +
		"http_duration_bucket{le=\"+Inf\"} 144\n" +
		"http_duration_sum 53.4\n" +
		"http_duration_count 144\n" +
		"# TYPE rpc summary\n" +
		"rpc{quantile=\"0.5\"} 0.05\n" +
		"rpc{quantile=\"0.99\"} 0.9\n" +
		"rpc_sum 8000\n" +
		"rpc_count 100000\n",
	// OpenMetrics with exemplar and EOF.
	"# TYPE foo counter\n" +
		"foo_total 17.0 1520879607.789 # {trace_id=\"4bf92f3577b34da6a3ce929d0e0e4736\",span_id=\"00f067aa0ba902b7\"} 0.67 1520879607.789\n" +
		"# EOF\n",
	// Escapes, empty label block, NaN/Inf values, missing values.
	"a{b=\"c\\n\\\"d\\\\\"} 1\nempty{} 2\nnan NaN\ninf +Inf\nneg -Inf\nnoval\n",
	// TYPE redeclaration mid-exposition (converter order-key edge).
	"# TYPE x histogram\nx_bucket{le=\"1\"} 1\n# TYPE x summary\nx{quantile=\"0.5\"} 2\nx_count 3\n",
	// Buckets without le, summaries without quantile, decreasing cumulative
	// counts, duplicate le values.
	"# TYPE h histogram\nh_bucket 5\nh_bucket{le=\"2\"} 9\nh_bucket{le=\"2\"} 4\nh_bucket{le=\"1\"} 7\n# TYPE s summary\ns 3\n",
	// Malformed lines, control bytes, non-UTF8.
	"metric{a=\"unterminated\nm\x00etric 1\n\xff\xfe 2\nname{=\"v\"} 1\nname{a=} 1\nname{a=\"v\" 1\n",
	// Whitespace-heavy and comment-only.
	"   \n\t\n#\n# TYPE\n# TYPE t\n# TYPE t counter extra\n  m  1  \n",
	// Exemplar edge cases.
	"om_total 1 # {} 2\nom_total 1 #{a=\"b\"} 2 3 4\nom 2 1.5 # {a=\"b\"} 1\n",
	// Timestamp extremes.
	"m 1 9223372036854775807\nm 1 -9223372036854775808\nm 1 1e300\nm 1 0.0001\n",
}

// FuzzConverter pipes fuzzed parses through the converter and batcher
// (including mid-parse chunking via take) and requires that every produced
// pmetric.Metrics marshals cleanly.
func FuzzConverter(f *testing.F) {
	for _, body := range fuzzSeedBodies {
		f.Add([]byte(body), byte(0))
		f.Add([]byte(body), byte(3))
	}
	f.Fuzz(func(t *testing.T, data []byte, mode byte) {
		openMetrics := mode&1 != 0
		exemplars := mode&2 != 0
		limit := 1 + int(mode>>2) // small chunk limit exercises take() mid-parse

		marshaler := &pmetric.ProtoMarshaler{}
		checkTaken := func(md pmetric.Metrics) {
			if md.ResourceMetrics().Len() != 1 {
				t.Fatalf("batch has %d ResourceMetrics, want 1", md.ResourceMetrics().Len())
			}
			if _, err := marshaler.MarshalMetrics(md); err != nil {
				t.Fatalf("MarshalProto: %v", err)
			}
			checkHistogramsSumToCount(t, md)
		}

		b := newBatcher(func(res pcommon.Resource) {
			res.Attributes().PutStr("url.full", "http://fuzz.local/metrics")
		}, time.Unix(1e9, 0), time.Unix(1e9+60, 0))
		conv := newConverter(b, nil)
		pp := promparse.Get(promparse.Options{MaxLineBytes: 1 << 20, OpenMetrics: openMetrics, Exemplars: exemplars})
		_, err := pp.Parse(bytes.NewReader(data), func(s Sample) error {
			_ = conv.add(s)
			if b.count() >= limit {
				checkTaken(b.take())
			}
			return nil
		})
		promparse.Put(pp)
		if err != nil {
			t.Fatalf("parse returned error: %v", err)
		}
		_ = conv.finish()
		if conv.malformed < 0 {
			t.Fatalf("converter malformed count negative: %d", conv.malformed)
		}
		checkTaken(b.take())
	})
}

// checkHistogramsSumToCount asserts the one OTLP invariant a histogram point
// cannot express its way out of: sum(bucket_counts) == count. It is checked on
// every batch the converter fuzz target produces (seed corpus included, so a
// plain `go test` runs it) rather than only on hand-written bodies, because the
// inputs that break it are exactly the ones nobody writes by hand — a
// cumulative sequence that decreases, a bucket claiming more than _count, the
// same le spelled twice. A point that violates it is invalid OTLP, and a
// validating collector may answer by rejecting the whole chunk, which costs
// every other target batched with it.
//
// EXPONENTIAL points are checked too, and skipping them is how the invariant
// escaped once already: the native-histogram path rounds the sample count, the
// zero count and every float bucket independently, so a wire-consistent message
// converted to buckets carrying up to twice the observations their count
// declared. Their assertion is the ONE-DIRECTIONAL half — buckets may never
// claim MORE than count, while count may legitimately exceed them, since a NaN
// observation increments a Prometheus histogram's count without entering any
// bucket and that shape is passed through as the target reported it.
func checkHistogramsSumToCount(t *testing.T, md pmetric.Metrics) {
	t.Helper()
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				switch m.Type() {
				case pmetric.MetricTypeHistogram:
					dps := m.Histogram().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						var sum uint64
						for _, c := range dp.BucketCounts().AsRaw() {
							sum += c
						}
						if sum != dp.Count() {
							t.Fatalf("metric %q point %d: sum(bucket_counts) = %d, count = %d (bounds=%v counts=%v) — OTLP requires them equal",
								m.Name(), d, sum, dp.Count(), dp.ExplicitBounds().AsRaw(), dp.BucketCounts().AsRaw())
						}
					}
				case pmetric.MetricTypeExponentialHistogram:
					dps := m.ExponentialHistogram().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						sum := dp.ZeroCount()
						for _, c := range dp.Positive().BucketCounts().AsRaw() {
							sum += c
						}
						for _, c := range dp.Negative().BucketCounts().AsRaw() {
							sum += c
						}
						if sum > dp.Count() {
							t.Fatalf("metric %q point %d: zero+sum(bucket_counts) = %d > count = %d (pos=%v neg=%v) — the buckets claim observations the point's population does not have",
								m.Name(), d, sum, dp.Count(), dp.Positive().BucketCounts().AsRaw(), dp.Negative().BucketCounts().AsRaw())
						}
					}
				}
			}
		}
	}
}
