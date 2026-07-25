package promscrape

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
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

// A timestamp large enough to overflow the nanosecond conversion must not wrap
// into a bogus (often 1970s) time.
func TestOversizedTimestampDoesNotWrap(t *testing.T) {
	scrape := pcommon.Timestamp(1_700_000_000 * 1e9)
	for _, ms := range []int64{math.MaxInt64, math.MaxInt64 / 1000, 99999999999999999, math.MinInt64} {
		got := pointTS(ms, scrape)
		if got != scrape {
			t.Errorf("pointTS(%d) = %d; an overflowing timestamp must fall back to the scrape time, not wrap", ms, got)
		}
	}
	// A normal timestamp still converts exactly.
	if got := pointTS(1_700_000_000_000, scrape); got != pcommon.Timestamp(1_700_000_000_000*int64(time.Millisecond)) {
		t.Errorf("pointTS lost a normal timestamp: %d", got)
	}
}
