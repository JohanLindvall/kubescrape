package promscrape

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// point is one exported data point flattened for comparison.
type flatPoint struct {
	metric string
	attrs  string
	value  float64
	count  uint64
}

func scrapePoints(t *testing.T, body string) []flatPoint {
	t.Helper()
	exp := &captureExporter{}
	s := New(Config{Node: "n1", Interval: time.Minute, Exporter: exp, StartTime: time.Now()})
	cb := newBatcher(func(pcommon.Resource) {}, time.Now(), time.Now())
	if _, err := s.parseAndExport(context.Background(), strings.NewReader(body), false, false, cb, pipelineTargets, "t"); err != nil {
		t.Fatal(err)
	}
	var out []flatPoint
	for _, b := range exp.batches {
		rms := b.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)
					switch m.Type() {
					case pmetric.MetricTypeSum:
						for d := 0; d < m.Sum().DataPoints().Len(); d++ {
							p := m.Sum().DataPoints().At(d)
							out = append(out, flatPoint{m.Name(), attrString(p.Attributes()), p.DoubleValue(), 0})
						}
					case pmetric.MetricTypeGauge:
						for d := 0; d < m.Gauge().DataPoints().Len(); d++ {
							p := m.Gauge().DataPoints().At(d)
							out = append(out, flatPoint{m.Name(), attrString(p.Attributes()), p.DoubleValue(), 0})
						}
					case pmetric.MetricTypeHistogram:
						for d := 0; d < m.Histogram().DataPoints().Len(); d++ {
							p := m.Histogram().DataPoints().At(d)
							out = append(out, flatPoint{m.Name(), attrString(p.Attributes()), p.Sum(), p.Count()})
						}
					}
				}
			}
		}
	}
	return out
}

func attrString(m pcommon.Map) string {
	var sb strings.Builder
	for _, k := range sortedAttrKeys(m) {
		v, _ := m.Get(k)
		sb.WriteString(k + "=" + v.AsString() + ";")
	}
	return sb.String()
}

func sortedAttrKeys(m pcommon.Map) []string {
	keys := make([]string, 0, m.Len())
	for k := range m.All() {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

// Two DELIBERATE divergences from Prometheus, pinned here so they are a known
// cost rather than something a reviewer rediscovers as a bug — the shape
// TestKnownPartialRedactions uses in agent/logscrub. Both put two data points
// with ONE series identity in one payload, which is precisely what promparse
// refuses a duplicate label NAME to avoid, so the asymmetry needs saying out
// loud: promparse can refuse a duplicate it sees on ONE LINE for free, and
// neither of these can be seen without state that scales with the scrape.
//
//  1. A histogram/summary family whose component series are NOT CONTIGUOUS is
//     flushed twice, once per run of its lines. The converter holds
//     accumulators for the CURRENT family only — that constant-memory property
//     is the whole reason this parser exists instead of expfmt, which buffers
//     whole families and cannot survive the 100k-series requirement — so
//     rejoining the runs would mean holding every family of the exposition
//     until the end.
//  2. The same series appearing twice, literally or with its labels in another
//     order, becomes two points. Detecting it needs a per-scrape set of series
//     fingerprints (O(series) memory), or a labelKey fingerprint computed for
//     every number sample — a sort per sample on the path whose per-sample cost
//     the parser, the interning and the last-seen memos all exist to protect.
//
// Both triggers are targets breaking the exposition format's own MUST ("all
// lines for a given metric must be provided as one single group", no repeated
// series), which is why the trade is affordable: Prometheus tolerates the first
// silently too, and rejects the second with a scrape error. What a reader
// should take from this test is that kubescrape does NEITHER — it ships both
// points and moves no counter — and that changing that means paying one of the
// two costs above, deliberately.
func TestKnownDuplicateDataPointDivergences(t *testing.T) {
	t.Run("non-contiguous histogram family is flushed twice", func(t *testing.T) {
		got := scrapePoints(t, "# TYPE requests histogram\n"+
			"requests_bucket{route=\"/a\",le=\"1\"} 3\n"+
			"requests_bucket{route=\"/a\",le=\"+Inf\"} 5\n"+
			"# TYPE goroutines gauge\n"+
			"goroutines 12\n"+
			"requests_sum{route=\"/a\"} 4.2\n"+
			"requests_count{route=\"/a\"} 5\n")
		want := []flatPoint{
			{"requests", "route=/a;", 0, 5},   // the bucket run: no sum
			{"requests", "route=/a;", 4.2, 5}, // the sum/count run: no buckets
			{"goroutines", "", 12, 0},
		}
		assertPoints(t, got, want)
	})

	t.Run("a repeated series becomes two points", func(t *testing.T) {
		got := scrapePoints(t, "# TYPE http_requests counter\n"+
			"http_requests_total{code=\"200\"} 1\n"+
			"http_requests_total{code=\"200\"} 7\n"+
			"g{a=\"1\",b=\"2\"} 1\n"+
			"g{b=\"2\",a=\"1\"} 2\n")
		want := []flatPoint{
			{"http_requests_total", "code=200;", 1, 0},
			{"http_requests_total", "code=200;", 7, 0},
			{"g", "a=1;b=2;", 1, 0},
			{"g", "a=1;b=2;", 2, 0},
		}
		assertPoints(t, got, want)
	})
}

func assertPoints(t *testing.T, got, want []flatPoint) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d points, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("point %d = %+v, want %+v — if this changed on purpose, update the divergence note above with what it now costs",
				i, got[i], want[i])
		}
	}
}
