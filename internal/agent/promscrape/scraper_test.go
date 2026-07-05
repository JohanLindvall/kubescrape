package promscrape

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
)

type captureExporter struct {
	mu      sync.Mutex
	batches []pmetric.Metrics
}

func (c *captureExporter) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batches = append(c.batches, md)
	return nil
}

func (c *captureExporter) points() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, b := range c.batches {
		n += b.DataPointCount()
	}
	return n
}

type staticTargets []kubemeta.ScrapeTarget

func (s staticTargets) NodeTargets(context.Context, string) ([]kubemeta.ScrapeTarget, error) {
	return s, nil
}

func testTarget(url string) kubemeta.ScrapeTarget {
	return kubemeta.ScrapeTarget{
		URL: url,
		Pod: kubemeta.Pod{
			Name: "pod1", Namespace: "ns1", UID: "uid1", NodeName: "node1",
			Owners: []kubemeta.Owner{{Kind: "Deployment", Name: "dep1"}},
		},
		Service: &kubemeta.Service{Name: "svc1", UID: "svc-uid"},
	}
}

func TestScrapeChunking(t *testing.T) {
	// 25 samples with a 10-point batch limit -> 3 exports.
	var body strings.Builder
	body.WriteString("# TYPE things counter\n")
	for i := 0; i < 25; i++ {
		fmt.Fprintf(&body, "things_total{i=\"%d\"} %d\n", i, i)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body.String()))
	}))
	defer srv.Close()

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		BatchPoints: 10, Targets: staticTargets{testTarget(srv.URL)}, Exporter: exp,
		StartTime: time.Now(),
	})
	s.cycle(context.Background())

	if len(exp.batches) != 3 {
		t.Fatalf("got %d batches, want 3", len(exp.batches))
	}
	if exp.points() != 25 {
		t.Fatalf("got %d points, want 25", exp.points())
	}

	// Resource attributes present on every batch.
	for i, b := range exp.batches {
		rm := b.ResourceMetrics().At(0)
		a := rm.Resource().Attributes()
		if v, _ := a.Get("k8s.pod.name"); v.Str() != "pod1" {
			t.Errorf("batch %d: k8s.pod.name = %q", i, v.Str())
		}
		if v, _ := a.Get("k8s.deployment.name"); v.Str() != "dep1" {
			t.Errorf("batch %d: k8s.deployment.name = %q", i, v.Str())
		}
		if v, _ := a.Get("k8s.service.name"); v.Str() != "svc1" {
			t.Errorf("batch %d: k8s.service.name = %q", i, v.Str())
		}
	}
	// Counters became monotonic cumulative sums.
	m := exp.batches[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	if m.Type() != pmetric.MetricTypeSum || !m.Sum().IsMonotonic() {
		t.Fatalf("metric type = %v", m.Type())
	}
}

func TestScrapeSampleLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for i := 0; i < 100; i++ {
			fmt.Fprintf(w, "m%d 1\n", i)
		}
	}))
	defer srv.Close()

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		MaxSamples: 10, BatchPoints: 1000,
		Targets: staticTargets{testTarget(srv.URL)}, Exporter: exp,
	})
	if err := s.scrapeTarget(context.Background(), testTarget(srv.URL)); err != ErrTooManySamples {
		t.Fatalf("err = %v, want ErrTooManySamples", err)
	}
}

func TestScrapeHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{testTarget(srv.URL)}, Exporter: exp,
	})
	if err := s.scrapeTarget(context.Background(), testTarget(srv.URL)); err == nil {
		t.Fatal("expected error for 503 response")
	}
	if len(exp.batches) != 0 {
		t.Fatalf("no batches expected, got %d", len(exp.batches))
	}
}
