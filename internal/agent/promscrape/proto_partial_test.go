package promscrape

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// An aborted scrape must still export what was converted before the abort —
// every kind here is cumulative, so a partial scrape costs only the missing
// series, while discarding it loses a whole scrape's worth silently. The text
// path already salvaged (TestPartialScrapeExportedOnSampleLimit); the protobuf
// path returned on error without running the converter's finish or exporting
// the pending chunk, so with -scrape-native-histograms enabled every point
// parsed before the abort was thrown away.
func TestProtoPartialScrapeExportedOnSampleLimit(t *testing.T) {
	var fams []*dto.MetricFamily
	for i := 0; i < 50; i++ {
		fams = append(fams, &dto.MetricFamily{
			Name: ptr(fmt.Sprintf("g%d", i)),
			Type: dto.MetricType_GAUGE.Enum(),
			Metric: []*dto.Metric{
				{Gauge: &dto.Gauge{Value: ptr(float64(i))}},
			},
		})
	}
	body := protoBody(t, fams...)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.google.protobuf; encoding=delimited")
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	exp := &captureExporter{}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		NativeHistograms: true, MaxSamples: 10, BatchPoints: 10_000,
		Targets:   staticTargets{testTarget(srv.URL)},
		Exporter:  exp,
		StartTime: time.Now(),
	})
	s.cycle(context.Background())

	// 10 samples pass the limit check; the 11th aborts. Those 10 must ship.
	if got := exp.points(); got != 10 {
		t.Fatalf("got %d exported points, want the 10 converted before the sample-limit abort", got)
	}
}

// Same contract for a body that dies mid-message: the families already parsed
// must still be exported.
func TestProtoPartialScrapeExportedOnTruncatedBody(t *testing.T) {
	var fams []*dto.MetricFamily
	for i := 0; i < 20; i++ {
		fams = append(fams, &dto.MetricFamily{
			Name: ptr(fmt.Sprintf("g%d", i)),
			Type: dto.MetricType_GAUGE.Enum(),
			Metric: []*dto.Metric{
				{Gauge: &dto.Gauge{Value: ptr(float64(i))}},
			},
		})
	}
	full := protoBody(t, fams...)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.google.protobuf; encoding=delimited")
		// Cut mid-message so the length prefix promises more than is delivered.
		_, _ = w.Write(full[:len(full)-6])
		w.(http.Flusher).Flush()
	}))
	defer srv.Close()

	exp := &captureExporter{}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		NativeHistograms: true, BatchPoints: 10_000,
		Targets:   staticTargets{testTarget(srv.URL)},
		Exporter:  exp,
		StartTime: time.Now(),
	})
	s.cycle(context.Background())

	if got := exp.points(); got == 0 {
		t.Fatal("a truncated protobuf body exported nothing; the families parsed before the cut were discarded")
	}
}
