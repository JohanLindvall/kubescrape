package promdur

import (
	"testing"
	"time"
)

// The shared corpus: prometheus-operator's Duration pattern admits y/w/d,
// which time.ParseDuration rejects outright — so `interval: 1d` passed CRD
// validation and used to parse-fail in the agent, silently dropping the
// target back to the default cadence. Both consumers (internal/scrape's
// merge, internal/agent/promscrape's scheduler) now read through this one
// parser; their own tests cover their edge rules ("0" is not a usable
// interval, non-positive warns and falls back).
func TestParse(t *testing.T) {
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
		{"", 0, false}, // the CRD pattern matches empty; usability is the caller's rule
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
		got, err := Parse(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("Parse(%q) err = %v, wantErr %v", tc.in, err, tc.wantErr)
			continue
		}
		if !tc.wantErr && got != tc.want {
			t.Errorf("Parse(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
