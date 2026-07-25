package promscrape

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// The byte estimate that gates a chunk flush must never fall below the encoded
// size: the collector rejects an over-limit message outright, so overshooting
// is TOTAL loss for that batch, not a partial one. Histogram buckets are the
// one term large enough to blow the budget on their own — OTLP encodes both
// explicit_bounds and bucket_counts as packed fixed64 (8 B each, and there is
// always one more count than bound), so a bucket costs 16. Charging 12 made a
// bucket-heavy family encode ~33% over its estimate and flush past the limit.
func TestBucketHeavyHistogramStaysUnderCollectorLimit(t *testing.T) {
	const (
		nBounds = 1200
		nSeries = 230
	)
	var body strings.Builder
	body.WriteString("# TYPE h histogram\n")
	for s := 0; s < nSeries; s++ {
		cum := 0
		for i := 0; i < nBounds; i++ {
			cum++
			_, _ = fmt.Fprintf(&body, "h_bucket{svc=\"s%d\",le=\"%d\"} %d\n", s, i, cum)
		}
		_, _ = fmt.Fprintf(&body, "h_bucket{svc=\"s%d\",le=\"+Inf\"} %d\n", s, cum)
		_, _ = fmt.Fprintf(&body, "h_sum{svc=\"s%d\"} 1\n", s)
		_, _ = fmt.Fprintf(&body, "h_count{svc=\"s%d\"} %d\n", s, cum)
	}

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 30 * time.Second,
		Targets:   staticTargets{testTarget(serve(t, body.String()))},
		Exporter:  exp,
		StartTime: time.Now(),
	}) // BatchPoints and BatchBytes both defaulted
	s.cycle(context.Background())

	if exp.points() != nSeries {
		t.Fatalf("got %d points, want %d (nothing may be lost to chunking)", exp.points(), nSeries)
	}
	var m pmetric.ProtoMarshaler
	for i, b := range exp.batches {
		if sz := m.MetricsSize(b); sz > grpcDefaultLimit {
			t.Errorf("batch %d is %d bytes, over the collector's %d-byte limit", i, sz, grpcDefaultLimit)
		}
	}
}
