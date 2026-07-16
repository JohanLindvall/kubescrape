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

// A finite-but-huge OpenMetrics timestamp must be rejected too: 1e300*1000
// overflows the int64 millisecond conversion to implementation-defined garbage.
func TestOpenMetricsHugeFiniteTimestampRejected(t *testing.T) {
	p := New(Options{MaxLineBytes: 1 << 20, OpenMetrics: true})
	var samples []Sample
	malformed, err := p.Parse(strings.NewReader("foo 1 1e300\nbar 1 -1e300\n"), func(s Sample) error {
		samples = append(samples, s)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 0 || malformed != 2 {
		t.Fatalf("got %d samples / %d malformed, want 0 / 2", len(samples), malformed)
	}
}

// A malformed # TYPE line (missing tokens or trailing garbage) is counted
// malformed rather than silently ignored.
func TestMalformedTypeLineCounted(t *testing.T) {
	p := New(Options{MaxLineBytes: 1 << 20})
	var samples int
	malformed, err := p.Parse(strings.NewReader("# TYPE foo\n# TYPE bar counter junk\n# TYPE ok counter\nok_total 1\n"), func(Sample) error {
		samples++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if malformed != 2 || samples != 1 {
		t.Fatalf("malformed = %d samples = %d, want 2 and 1", malformed, samples)
	}
}
