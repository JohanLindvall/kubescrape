package promscrape

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A target that responds with a protobuf Content-Type must NOT be decoded
// unless the operator enabled -scrape-native-histograms: the proto decode
// materialises the whole message and is gzip-amplified, so an un-opted-in agent
// would OOM on a hostile target's chosen Content-Type. It must fail the scrape
// visibly instead. Regression for the proto reachability gate.
func TestProtoResponseRejectedWithoutNativeHistograms(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited")
		_, _ = w.Write([]byte{0x00}) // a byte the proto path would try to decode
	}))
	t.Cleanup(srv.Close)

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{testTarget(srv.URL)}, Exporter: exp,
		StartTime: time.Now(),
		// NativeHistograms deliberately left false (the default).
	})
	_, err := s.scrapeTarget(context.Background(), testTarget(srv.URL), s.cfg.Timeout)
	if err == nil {
		t.Fatal("a protobuf response was accepted without -scrape-native-histograms")
	}
	if !strings.Contains(err.Error(), "native histograms") {
		t.Errorf("error %q should explain the proto path is gated on native histograms", err)
	}
}
