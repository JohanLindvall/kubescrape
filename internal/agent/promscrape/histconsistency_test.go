package promscrape

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// scrapeHistogram runs one exposition through the production text front and
// returns the single histogram point it produced.
func scrapeHistogram(t *testing.T, body string) pmetric.HistogramDataPoint {
	t.Helper()
	exp := &captureExporter{}
	s := New(Config{Node: "n1", Interval: time.Minute, Exporter: exp, StartTime: time.Now()})
	cb := newBatcher(func(pcommon.Resource) {}, time.Now(), time.Now())
	if _, err := s.parseAndExport(context.Background(), strings.NewReader(body), false, false, cb, pipelineTargets, "t"); err != nil {
		t.Fatal(err)
	}
	var found []pmetric.HistogramDataPoint
	for _, b := range exp.batches {
		rms := b.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					if m := ms.At(k); m.Type() == pmetric.MetricTypeHistogram {
						for d := 0; d < m.Histogram().DataPoints().Len(); d++ {
							found = append(found, m.Histogram().DataPoints().At(d))
						}
					}
				}
			}
		}
	}
	if len(found) != 1 {
		t.Fatalf("got %d histogram points, want 1", len(found))
	}
	return found[0]
}

// A cumulative bucket sequence that contradicts itself, or contradicts the
// _count beside it, must still convert to a VALID OTLP point:
// sum(bucket_counts) == count. Both shapes below are what a hand-rolled
// exporter — or a client library reading its count and its buckets from
// separate atomics while an observation lands between them — actually serves,
// and both used to ship a point OTLP forbids, with nothing counted anywhere.
//
// The clamp is the conservative reading of a self-contradicting exposition, not
// a repair: the population _count declares bounds what any single bucket may
// claim, and a cumulative count is never allowed to go backwards.
func TestInconsistentHistogramStillEmitsAValidPoint(t *testing.T) {
	for _, tc := range []struct {
		name       string
		body       string
		wantBounds []float64
		wantCounts []uint64
		wantCount  uint64
	}{
		{
			// A bucket claiming 9 observations under a _count of 3. (The two le
			// spellings are one bound: labelFloat reads both as 1, and the dedupe
			// keeps the last.) Shipped counts=[9 0] against count=3.
			name: "bucket over the declared count",
			body: "# TYPE h histogram\n" +
				"h_bucket{le=\"1\"} 5\n" +
				"h_bucket{le=\"1.0\"} 9\n" +
				"h_bucket{le=\"+Inf\"} 3\n" +
				"h_count 3\n",
			wantBounds: []float64{1},
			wantCounts: []uint64{3, 0},
			wantCount:  3,
		},
		{
			// A cumulative count that DECREASES. Shipped counts=[10 0 5] = 15
			// against count=10: the zero-width clamp was taken against the
			// exposition's own previous cum, so the discarded 5 came back in the
			// overflow bucket.
			name: "decreasing cumulative counts",
			body: "# TYPE h2 histogram\n" +
				"h2_bucket{le=\"1\"} 10\n" +
				"h2_bucket{le=\"2\"} 5\n" +
				"h2_bucket{le=\"+Inf\"} 10\n" +
				"h2_count 10\n" +
				"h2_sum 3\n",
			wantBounds: []float64{1, 2},
			wantCounts: []uint64{10, 0, 0},
			wantCount:  10,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dp := scrapeHistogram(t, tc.body)
			var sum uint64
			for _, c := range dp.BucketCounts().AsRaw() {
				sum += c
			}
			if sum != dp.Count() {
				t.Fatalf("sum(bucket_counts) = %d, count = %d (bounds=%v counts=%v) — OTLP requires them equal",
					sum, dp.Count(), dp.ExplicitBounds().AsRaw(), dp.BucketCounts().AsRaw())
			}
			if got := dp.ExplicitBounds().AsRaw(); !slices.Equal(got, tc.wantBounds) {
				t.Fatalf("bounds = %v, want %v", got, tc.wantBounds)
			}
			if got := dp.BucketCounts().AsRaw(); !slices.Equal(got, tc.wantCounts) {
				t.Fatalf("bucket counts = %v, want %v", got, tc.wantCounts)
			}
			if dp.Count() != tc.wantCount {
				t.Fatalf("count = %d, want %d", dp.Count(), tc.wantCount)
			}
		})
	}
}

// The clamps must be invisible to a well-formed histogram: same bounds, same
// counts, same total as before they existed. Without this, "make it consistent"
// could be satisfied by quietly rewriting everyone's data.
func TestWellFormedHistogramIsUnchangedByTheClamps(t *testing.T) {
	dp := scrapeHistogram(t, "# TYPE d histogram\n"+
		"d_bucket{le=\"0.1\"} 100\n"+
		"d_bucket{le=\"0.5\"} 140\n"+
		"d_bucket{le=\"+Inf\"} 144\n"+
		"d_sum 53.4\n"+
		"d_count 144\n")
	if got := dp.ExplicitBounds().AsRaw(); !slices.Equal(got, []float64{0.1, 0.5}) {
		t.Fatalf("bounds = %v, want [0.1 0.5]", got)
	}
	if got := dp.BucketCounts().AsRaw(); !slices.Equal(got, []uint64{100, 40, 4}) {
		t.Fatalf("bucket counts = %v, want [100 40 4]", got)
	}
	if dp.Count() != 144 || dp.Sum() != 53.4 {
		t.Fatalf("count = %d sum = %v, want 144 and 53.4", dp.Count(), dp.Sum())
	}
}

// A histogram with no _count line at all takes its total from the last
// cumulative bucket, so the same identity has to hold there — that is the arm
// the fuzz corpus trips (a decreasing sequence with no _count), and it is the
// arm where "clamp to total" is clamping to a number the buckets themselves
// produced.
func TestHistogramWithoutCountLineIsConsistent(t *testing.T) {
	dp := scrapeHistogram(t, "# TYPE n histogram\n"+
		"n_bucket{le=\"1\"} 7\n"+
		"n_bucket{le=\"2\"} 4\n")
	var sum uint64
	for _, c := range dp.BucketCounts().AsRaw() {
		sum += c
	}
	if sum != dp.Count() {
		t.Fatalf("sum(bucket_counts) = %d, count = %d (counts=%v) — OTLP requires them equal",
			sum, dp.Count(), dp.BucketCounts().AsRaw())
	}
	if dp.Count() != 4 {
		t.Fatalf("count = %d, want 4 (the last cumulative bucket, since no _count was served)", dp.Count())
	}
}
