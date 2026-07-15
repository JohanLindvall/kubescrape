package promparse

import (
	"math"
	"strings"
	"testing"
)

// TestOpenMetricsNonFiniteTimestampRejected is the regression test for a
// non-finite OpenMetrics timestamp (NaN/±Inf parse without error) becoming
// int64-min after the *1000 conversion and riding onto the data point. Such a
// line must be rejected as malformed, never emit a sample with a garbage
// timestamp.
func TestOpenMetricsNonFiniteTimestampRejected(t *testing.T) {
	parse := func(in string) (samples []Sample, malformed int) {
		p := New(Options{MaxLineBytes: 1 << 20, OpenMetrics: true})
		m, err := p.Parse(strings.NewReader(in), func(s Sample) error {
			samples = append(samples, s)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return samples, m
	}

	for _, in := range []string{"foo 1 NaN\n", "foo 1 +Inf\n", "foo 1 -Inf\n"} {
		samples, malformed := parse(in)
		for _, s := range samples {
			if s.TimestampMs == math.MinInt64 {
				t.Fatalf("input %q emitted a garbage int64-min timestamp instead of rejecting the line", in)
			}
		}
		if len(samples) != 0 || malformed != 1 {
			t.Fatalf("input %q: got %d samples / %d malformed, want 0 samples / 1 malformed", in, len(samples), malformed)
		}
	}
	// A finite fractional OM timestamp still parses (seconds → ms).
	got, malformed := parse("foo 1 1.5\n")
	if malformed != 0 || len(got) != 1 || got[0].TimestampMs != 1500 {
		t.Fatalf("finite OM timestamp mis-parsed: %+v (malformed=%d)", got, malformed)
	}
}
