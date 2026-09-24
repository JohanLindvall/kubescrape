package promdur

import (
	"math/big"
	"testing"
	"time"
)

// The shared corpus: prometheus-operator's Duration pattern admits y/w/d,
// which time.ParseDuration rejects outright — so `interval: 1d` passed CRD
// validation and used to parse-fail in the agent, silently dropping the
// target back to the default cadence. Both consumers (internal/scrape's
// merge, internal/agent/promscrape's scheduler) now read through this one
// parser and its Interval gate; their own tests cover their REACTIONS to an
// unusable value (the merge keeps the holder, the scraper warns and falls
// back).
var parseCases = []struct {
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
}

func TestParse(t *testing.T) {
	for _, tc := range parseCases {
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

// Interval is the usability rule both consumers apply: a value that parses to
// a positive duration is usable, and "0", an empty string, a negative Go
// duration, an overflow and garbage are not. Pinned here because the merge and
// the scraper must reach the same verdict on every value.
func TestIntervalIsUsableOnlyWhenPositive(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want time.Duration
		ok   bool
	}{
		{"30s", 30 * time.Second, true},
		{"1d", 24 * time.Hour, true},
		{"1ms", time.Millisecond, true},
		{"0", 0, false},
		{"0s", 0, false},
		{"", 0, false},
		{"-1s", 0, false},
		{"garbage", 0, false},
		{"292y52w", 0, false},
	} {
		got, ok := Interval(tc.in)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("Interval(%q) = %v, %v; want %v, %v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
	// And it is exactly Parse plus the gate, across the whole parse corpus.
	for _, tc := range parseCases {
		d, err := Parse(tc.in)
		got, ok := Interval(tc.in)
		if want := err == nil && d > 0; ok != want || (ok && got != d) {
			t.Errorf("Interval(%q) = %v, %v; Parse = %v, %v", tc.in, got, ok, d, err)
		}
	}
}

// FuzzParse holds the parser to an independent oracle. The input is
// TENANT-AUTHORED — a ServiceMonitor's interval/scrapeTimeout, admitted by the
// CRD's `[0-9]+` pattern at any magnitude — and the failure this package has
// already had once is a SILENT wrap: an overflowing value landing on a small
// positive duration that no caller's gate reads as invalid. So for every input
// on the CRD path the answer must be exactly the arbitrary-precision sum of
// n_i*unit_i when that fits, and an error when it does not; and any input at
// all must not panic.
func FuzzParse(f *testing.F) {
	for _, tc := range parseCases {
		f.Add(tc.in)
	}
	f.Add("99999999999999999999y")
	f.Add("1y1w1d1h1m1s1ms")
	f.Add("9223372036854775807ms")
	f.Fuzz(func(t *testing.T, s string) {
		got, err := Parse(s)
		m := promDurationRE.FindStringSubmatch(s)
		if s == "0" || m == nil {
			return // the "0" shortcut, or Go's parser: not this oracle's language
		}
		units := [...]int64{
			int64(365 * 24 * time.Hour), int64(7 * 24 * time.Hour), int64(24 * time.Hour),
			int64(time.Hour), int64(time.Minute), int64(time.Second), int64(time.Millisecond),
		}
		sum := new(big.Int)
		for i, u := range units {
			g := m[i+1]
			if g == "" {
				continue
			}
			n, ok := new(big.Int).SetString(g, 10)
			if !ok {
				t.Fatalf("the CRD pattern admitted a non-decimal group %q in %q", g, s)
			}
			sum.Add(sum, n.Mul(n, big.NewInt(u)))
		}
		if fits := sum.IsInt64(); fits != (err == nil) {
			t.Fatalf("Parse(%q) = %v, %v; the exact sum %s fits in int64: %v", s, got, err, sum, fits)
		}
		if err != nil {
			return
		}
		if int64(got) != sum.Int64() {
			t.Fatalf("Parse(%q) = %d, want the exact sum %s", s, int64(got), sum)
		}
		if got < 0 {
			t.Fatalf("Parse(%q) = %v: negative on the CRD path, which has no sign", s, got)
		}
	})
}
