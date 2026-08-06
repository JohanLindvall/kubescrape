package promscrape

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// The converter's label fingerprint must be injective. The text format permits
// any byte but \, " and newline inside a quoted value, so a value carrying the
// delimiters could forge another series' key: the two histogram series merged
// into one data point, and the duplicate-le dedupe then destroyed one series'
// value outright.
func TestLabelKeyIsInjectiveAcrossDelimiterBytes(t *testing.T) {
	body := "# TYPE h histogram\n" +
		"h_bucket{a=\"1\",b=\"2\",le=\"1\"} 5\n" +
		"h_bucket{a=\"1\\u0001b\\u00002\",le=\"1\"} 99\n"
	// Real NUL/SOH bytes, not escapes — the parser passes them through.
	body = strings.ReplaceAll(body, "\\u0001", "\x01")
	body = strings.ReplaceAll(body, "\\u0000", "\x00")

	exp := &captureExporter{}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets:   staticTargets{testTarget(serve(t, body))},
		Exporter:  exp,
		StartTime: time.Now(),
	})
	s.cycle(context.Background())

	points := 0
	for _, b := range exp.batches {
		rms := b.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					points += ms.At(k).Histogram().DataPoints().Len()
				}
			}
		}
	}
	if points != 2 {
		t.Fatalf("got %d histogram data points, want 2: two distinct label sets merged into one series", points)
	}
}

// labelKey memoises the previous call's fingerprint so the 13 component samples
// of one histogram point cost one sort/fingerprint/map probe instead of 13.
// The memo must never merge distinct label sets nor split one, whatever order
// the exposition interleaves them in, and must not survive a family boundary
// (the accumulators it caches are recycled through the freelists there).
func TestLabelKeyMemoGroupsInterleavedSets(t *testing.T) {
	// Two label sets interleaved bucket-by-bucket, one of them with its labels
	// written in a DIFFERENT order on some rows (the fingerprint is
	// order-insensitive, so those rows must still land in the same point), then
	// a summary family reusing the very same label sets.
	body := "# TYPE h histogram\n" +
		"h_bucket{a=\"1\",b=\"x\",le=\"1\"} 1\n" +
		"h_bucket{a=\"2\",b=\"x\",le=\"1\"} 10\n" +
		"h_bucket{b=\"x\",a=\"1\",le=\"2\"} 2\n" +
		"h_bucket{a=\"2\",b=\"x\",le=\"2\"} 20\n" +
		"h_bucket{a=\"1\",b=\"x\",le=\"+Inf\"} 3\n" +
		"h_bucket{a=\"2\",b=\"x\",le=\"+Inf\"} 30\n" +
		"h_sum{a=\"1\",b=\"x\"} 0.5\n" +
		"h_count{a=\"1\",b=\"x\"} 3\n" +
		"h_sum{a=\"2\",b=\"x\"} 5\n" +
		"h_count{a=\"2\",b=\"x\"} 30\n" +
		"# TYPE s summary\n" +
		"s{a=\"1\",b=\"x\",quantile=\"0.5\"} 0.1\n" +
		"s{a=\"2\",b=\"x\",quantile=\"0.5\"} 0.2\n" +
		"s_sum{a=\"1\",b=\"x\"} 1\n" +
		"s_count{a=\"1\",b=\"x\"} 7\n" +
		"s_sum{a=\"2\",b=\"x\"} 2\n" +
		"s_count{a=\"2\",b=\"x\"} 9\n"

	exp := &captureExporter{}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets:   staticTargets{testTarget(serve(t, body))},
		Exporter:  exp,
		StartTime: time.Now(),
	})
	s.cycle(context.Background())

	// counts maps metric name -> label "a" value -> (buckets or quantiles, count).
	type got struct {
		parts int
		count uint64
	}
	seen := map[string]map[string]got{}
	for _, b := range exp.batches {
		rms := b.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)
					if seen[m.Name()] == nil {
						seen[m.Name()] = map[string]got{}
					}
					switch m.Type() {
					case pmetric.MetricTypeHistogram:
						dps := m.Histogram().DataPoints()
						for d := 0; d < dps.Len(); d++ {
							dp := dps.At(d)
							a, _ := dp.Attributes().Get("a")
							seen[m.Name()][a.Str()] = got{dp.ExplicitBounds().Len(), dp.Count()}
						}
					case pmetric.MetricTypeSummary:
						dps := m.Summary().DataPoints()
						for d := 0; d < dps.Len(); d++ {
							dp := dps.At(d)
							a, _ := dp.Attributes().Get("a")
							seen[m.Name()][a.Str()] = got{dp.QuantileValues().Len(), dp.Count()}
						}
					}
				}
			}
		}
	}

	want := map[string]map[string]got{
		"h": {"1": {2, 3}, "2": {2, 30}}, // le=1,2 explicit (+Inf is the overflow slot)
		"s": {"1": {1, 7}, "2": {1, 9}},
	}
	for name, byA := range want {
		if len(seen[name]) != len(byA) {
			t.Fatalf("%s: got %d data points %v, want %d — the memo merged or split a label set", name, len(seen[name]), seen[name], len(byA))
		}
		for a, w := range byA {
			if seen[name][a] != w {
				t.Errorf("%s{a=%q} = %+v, want %+v", name, a, seen[name][a], w)
			}
		}
	}
}

// A timestamp large enough to overflow the nanosecond conversion must not wrap
// into a bogus (often 1970s) time — and a NEGATIVE one (pre-epoch; the classic
// format's timestamp is a signed int64, so `foo 1 -1` parses cleanly) must not
// wrap through pcommon.Timestamp's unsigned model into a far-future stamp.
func TestOversizedTimestampDoesNotWrap(t *testing.T) {
	scrape := pcommon.Timestamp(1_700_000_000 * 1e9)
	for _, ms := range []int64{math.MaxInt64, math.MaxInt64 / 1000, 99999999999999999, math.MinInt64, -1, -1000} {
		got := pointTS(ms, scrape)
		if got != scrape {
			t.Errorf("pointTS(%d) = %d; an unrepresentable timestamp must fall back to the scrape time, not wrap", ms, got)
		}
	}
	// A normal timestamp still converts exactly.
	if got := pointTS(1_700_000_000_000, scrape); got != pcommon.Timestamp(1_700_000_000_000*int64(time.Millisecond)) {
		t.Errorf("pointTS lost a normal timestamp: %d", got)
	}
}
