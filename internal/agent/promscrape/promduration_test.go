package promscrape

import (
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
