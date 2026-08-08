package promscrape

import (
	"log/slog"
	"testing"
	"time"
)

// The parse table itself lives with the shared parser (internal/promdur);
// what stays here is the scraper's own edge rule around it.

// An overflowing duration reaches parseTargetDuration through the exact path a
// valid one does. The CRD admits `[0-9]+`, so a wrapped value passed the caller's
// `d <= 0` gate as a tiny cadence — a scrapeTimeout every scrape deadline-exceeds
// (total metric loss) with no warning. It must warn once and fall back like any
// other invalid value; the compound-sum overflow ("292y52w") takes the same path.
func TestOverflowDurationWarnsAndFallsBack(t *testing.T) {
	h := &countingHandler{}
	s := &Scraper{cfg: Config{Interval: time.Minute, Timeout: 30 * time.Second}, log: slog.New(h)}

	tgt := testTarget("http://10.0.0.1:9090/metrics")
	tgt.Source, tgt.Monitor = "servicemonitor", "monitoring/api"
	tgt.ScrapeTimeout = "18446744073710ms"
	if got := s.targetTimeout(tgt, time.Minute); got != 30*time.Second {
		t.Fatalf("timeout = %v, want the default 30s (overflow must fall back, not accept +448µs)", got)
	}

	tgt2 := testTarget("http://10.0.0.2:9090/metrics")
	tgt2.Source, tgt2.Monitor = "servicemonitor", "monitoring/api2"
	tgt2.Interval = "292y52w"
	if got := s.targetInterval(tgt2); got != time.Minute {
		t.Fatalf("interval = %v, want the default (compound-sum overflow must fall back)", got)
	}

	if h.n != 2 {
		t.Errorf("logged %d warnings, want 2 (one per overflowing field)", h.n)
	}
}
