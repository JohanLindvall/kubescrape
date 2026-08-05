package promscrape

import (
	"log/slog"
	"testing"
	"time"
)

// prometheus-operator's Duration pattern admits y/w/d, which time.ParseDuration
// rejects outright — so `interval: 1d` passed CRD validation, failed to parse
// here, and silently dropped the target back to the default cadence.
func TestParsePromDuration(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"30s", 30 * time.Second, false},
		{"1m", time.Minute, false},
		{"1m30s", 90 * time.Second, false},
		{"500ms", 500 * time.Millisecond, false},
		{"1h", time.Hour, false},
		{"1d", 24 * time.Hour, false},
		{"2d12h", 60 * time.Hour, false},
		{"1w", 7 * 24 * time.Hour, false},
		{"1y", 365 * 24 * time.Hour, false},
		{"0", 0, false},
		// Go-only forms still work through the fallback.
		{"1h30m", 90 * time.Minute, false},
		{"1.5h", 90 * time.Minute, false},
		{"garbage", 0, true},
		{"5", 0, true},
		// Large-but-valid must survive; overflow must be an error, not a wrap.
		{"290y", 290 * 365 * 24 * time.Hour, false}, // ~9.15e18 ns, under MaxInt64
		{"18446744073710ms", 0, true},               // digits fit int64; the *ms multiply wraps to +448µs
		{"292y52w", 0, true},                        // neither term overflows; their SUM does
	} {
		got, err := parsePromDuration(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("parsePromDuration(%q) err = %v, wantErr %v", tc.in, err, tc.wantErr)
			continue
		}
		if !tc.wantErr && got != tc.want {
			t.Errorf("parsePromDuration(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

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
