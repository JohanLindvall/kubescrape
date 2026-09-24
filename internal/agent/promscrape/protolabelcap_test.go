package promscrape

import (
	"bytes"
	"context"
	"encoding/binary"
	"strconv"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// The protobuf front runs the same O(n²) duplicate-name scan as the text front,
// and it is the worse of the two: the whole message is resident before the
// first comparison, so no interleaved socket read gives the scrape deadline a
// place to land. Uncapped, 120k labels in one metric (1.38 MiB — well inside
// maxProtoMessageBytes) measured 80s against a 1s timeout, and cycle() waits
// for every scrape it starts.
func TestProtoLabelsAreCapped(t *testing.T) {
	mk := func(n int) *dto.Metric {
		m := &dto.Metric{Label: make([]*dto.LabelPair, 0, n)}
		for i := range n {
			m.Label = append(m.Label, &dto.LabelPair{
				Name:  new("l" + strconv.Itoa(i)),
				Value: new("v"),
			})
		}
		return m
	}

	if _, ok := protoLabels(mk(maxLabelsPerSample)); !ok {
		t.Errorf("a metric with exactly %d labels was refused; the cap must be inclusive", maxLabelsPerSample)
	}
	if _, ok := protoLabels(mk(maxLabelsPerSample + 1)); ok {
		t.Errorf("a metric with %d labels was accepted; it must be malformed past the cap", maxLabelsPerSample+1)
	}

	// And the refusal must be CHEAP — the whole point is that the quadratic
	// scan never runs. 200k labels is ~1.5x the size that measured 80s.
	start := time.Now()
	if _, ok := protoLabels(mk(200_000)); ok {
		t.Fatal("a 200k-label metric was accepted")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("refusing a 200k-label metric took %v; the cap is not short-circuiting the scan", d)
	}
}

// wideLabels is n label pairs with short distinct names (never `le` or
// `quantile`: "x" plus base-36) and empty values — the cheapest wire spelling
// of a metric sitting at the label cap.
func wideLabels(n int) []*dto.LabelPair {
	out := make([]*dto.LabelPair, n)
	for i := range out {
		out[i] = &dto.LabelPair{Name: new("x" + strconv.FormatInt(int64(i), 36)), Value: new("")}
	}
	return out
}

// The protobuf front must stop when the scrape's context does. The whole
// message is resident before the first row is converted, so no socket read
// gives the deadline a place to land, and the per-Metric label cap bounds one
// metric, never a message. Before the context checks each of these in-cap
// messages ran seconds to tens of seconds past its deadline and returned
// err=nil — while cycle() waited on it, stalling the node's whole cadence:
// one 4096-label histogram of ~280k buckets (35.8 s measured; ONE metric, so a
// per-Metric check alone does not reach it), ~100 gauges at the label cap
// (12.0 s), and one histogram whose buckets each carry a 4096-label exemplar
// (9.6 s). The first two must now stop within about one row of the deadline
// and be classified a timeout; the third no longer costs enough to reach it
// (the exemplar rune bound refuses each exemplar before either scan runs), and
// must simply finish inside it.
//
// The deadline is a whole second so the DECODE of each 4 MiB fixture fits
// inside it and the deadline lands in the conversion, which is the loop under
// test (a deadline that expired during the decode would be caught by the first
// per-Metric check and prove nothing about the per-row ones).
func TestProtoFrontStopsAtTheScrapeDeadline(t *testing.T) {
	cases := []struct {
		name        string
		wantTimeout bool
		fam         func() *dto.MetricFamily
	}{
		{"one wide histogram of many buckets", true, func() *dto.MetricFamily {
			h := &dto.Histogram{SampleCount: new(uint64(1)), SampleSum: new(1.0)}
			for i := range 280_000 {
				h.Bucket = append(h.Bucket, &dto.Bucket{UpperBound: new(float64(i)), CumulativeCount: new(uint64(1))})
			}
			return &dto.MetricFamily{Name: new("h"), Type: dto.MetricType_HISTOGRAM.Enum(),
				Metric: []*dto.Metric{{Label: wideLabels(maxLabelsPerSample), Histogram: h}}}
		}},
		{"many gauges at the label cap", true, func() *dto.MetricFamily {
			fam := &dto.MetricFamily{Name: new("g"), Type: dto.MetricType_GAUGE.Enum()}
			for i := range 100 {
				labels := wideLabels(maxLabelsPerSample)
				labels[0].Value = new(strconv.Itoa(i)) // distinct series
				fam.Metric = append(fam.Metric, &dto.Metric{Label: labels, Gauge: &dto.Gauge{Value: new(1.0)}})
			}
			return fam
		}},
		{"one histogram of wide exemplars", false, func() *dto.MetricFamily {
			h := &dto.Histogram{SampleCount: new(uint64(1)), SampleSum: new(1.0)}
			for i := range 100 {
				h.Bucket = append(h.Bucket, &dto.Bucket{UpperBound: new(float64(i)), CumulativeCount: new(uint64(1)),
					Exemplar: &dto.Exemplar{Value: new(1.0), Label: wideLabels(maxLabelsPerSample)}})
			}
			return &dto.MetricFamily{Name: new("e"), Type: dto.MetricType_HISTOGRAM.Enum(),
				Metric: []*dto.Metric{{Label: wideLabels(8), Histogram: h}}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := protoBody(t, tc.fam())
			// The shape must be one the size guards ADMIT, or this would be
			// testing a refusal instead of the deadline.
			n, k := binary.Uvarint(body)
			if n > maxProtoMessageBytes {
				t.Fatalf("fixture is %d bytes, over the %d-byte message cap", n, maxProtoMessageBytes)
			}
			if est := protoDecodedSize(body[k:], maxProtoDecodedBytes); est > maxProtoDecodedBytes {
				t.Fatalf("fixture estimates at %d decoded bytes, over the %d budget", est, maxProtoDecodedBytes)
			}
			s := New(Config{
				Node: "n1", Interval: time.Hour, Timeout: time.Hour,
				NativeHistograms: true, Exemplars: true, Targets: staticTargets{},
				Exporter: &captureExporter{}, StartTime: time.Now(),
			})
			const deadline = time.Second
			ctx, cancel := context.WithTimeout(context.Background(), deadline)
			defer cancel()
			cb := newBatcher(func(pcommon.Resource) {}, time.Now(), time.Now())
			start := time.Now()
			_, err := s.scrapeProto(ctx, bytes.NewReader(body), cb, nil, "t", "t")
			elapsed := time.Since(start)
			if elapsed > deadline+2*time.Second {
				t.Fatalf("the scrape returned %v after a %v deadline (err=%v); the front is not checking its context", elapsed, deadline, err)
			}
			switch reason := failureReason(err); {
			case tc.wantTimeout && reason != reasonTimeout:
				t.Fatalf("err = %v (reason %q) after %v, want a timeout: the deadline passed mid-message", err, reason, elapsed)
			case !tc.wantTimeout && err != nil && reason != reasonTimeout:
				t.Fatalf("err = %v (reason %q), want the scrape to finish (or, on a slow machine, time out)", err, reason)
			}
		})
	}
}
