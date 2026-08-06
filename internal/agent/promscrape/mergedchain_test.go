package promscrape

// Cross-package proof of the server's per-URL monitor merge: when two monitors
// resolve to one URL, the metadata service serves ONE target whose
// MetricRelabelings is the concatenation of both monitors' chains (see
// internal/scrape.MergeMonitorEndpoint). The agent applies that chain per
// sample exactly as it applies a single monitor's — a longer chain is still
// one compiled session — so a series EITHER monitor asked to drop is dropped
// from the one scrape. That union is the documented divergence from
// prometheus-operator's job-per-monitor model, and this pins the agent half
// of it without the server in the loop.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

func TestMergedRelabelChainDropsSamplesMatchingEitherMonitor(t *testing.T) {
	srv := serveBody(t, "platform_build_info 1\nsecret_ratio 2\nhttp_requests_total 7\n")
	exp := &captureExporter{}
	tgt := testTarget(srv.URL)
	// The winner's chain, then the loser's, in the server's merge order.
	tgt.MetricRelabelings = []kubemeta.RelabelRule{
		{Action: "drop", SourceLabels: []string{"__name__"}, Regex: "platform_.*"},
		{Action: "drop", SourceLabels: []string{"__name__"}, Regex: "secret_.*"},
	}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets:  staticTargets{tgt},
		Exporter: exp, StartTime: time.Now(),
	})
	s.cycle(context.Background())
	series := identitySeries(exp.batches)
	if len(series) != 1 {
		t.Fatalf("exported %d series, want only the one neither monitor dropped: %v", len(series), series)
	}
	if got := series[0]; !strings.Contains(got, "http_requests_total") {
		t.Fatalf("surviving series = %q, want http_requests_total", got)
	}
}
