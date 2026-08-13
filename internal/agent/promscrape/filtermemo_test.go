package promscrape

import (
	"strings"
	"testing"
)

// retainedMemoBytes is what the session's memos actually hold alive, counted
// off the maps themselves rather than off the session's own accounting — a
// broken charge would agree with itself.
func retainedMemoBytes(s *filterSession) int {
	n := 0
	for k := range s.masks {
		n += len(k)
	}
	for k := range s.lblMatch {
		n += len(k.value)
	}
	return n
}

// The per-scrape memos are keyed by text the TARGET chooses, so their entry
// caps bound the wrong quantity: 100k series names of 16 KiB gzip to a ~2 MiB
// response and would retain 1.6 GB in the agent, the same amplification the
// parser's TYPE table had. This is the memo half of that bound.
//
// The second half of the test is the one that matters more: exhausting the memo
// must not change a single VERDICT. A memo is an optimization, and a bound that
// silently altered filtering would be a worse bug than the memory it saves.
func TestFilterSessionMemoIsBoundedByBytes(t *testing.T) {
	// Two rules so BOTH memos fill: the first exercises the name-mask memo, the
	// second (no `metrics`, so it is in every mask) makes every call resolve a
	// label value through the (matcher, value) memo.
	filter, err := newMetricFilter([]FilterRule{
		{Action: "keep", Metrics: "spared_.*"},
		{Action: "drop", Labels: map[string]string{"zone": "eu.*"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	s := filter.session()

	const (
		names   = 4000
		nameLen = 1000
	)
	for i := 0; i < names; i++ {
		name := strings.Repeat("n", nameLen-8) + pad8Scrape(i)
		value := strings.Repeat("v", nameLen-8) + pad8Scrape(i)
		if !s.Keep(name, []Label{{Name: "zone", Value: value}}) {
			t.Fatalf("series %d was dropped by a filter that only drops zone=eu.*", i)
		}
	}
	if retained := retainedMemoBytes(s); retained > maxMemoBytes {
		t.Fatalf("filter session memos retain %d bytes of target-chosen key text, want <= %d "+
			"(%d names + %d label values of %d bytes; the entry caps bound the COUNT, which is not a memory bound)",
			retained, maxMemoBytes, names, names, nameLen)
	}
	if len(s.masks) < 10 {
		t.Fatalf("only %d names memoized: the byte bound must stop the memo GROWING, not disable it", len(s.masks))
	}

	// Verdicts, after the budget is spent: identical to the unmemoized filter's.
	long := strings.Repeat("x", nameLen)
	for _, tc := range []struct {
		name   string
		labels []Label
		want   bool
	}{
		{"anything_" + long, []Label{{Name: "zone", Value: "eu-west"}}, false},
		{"anything_" + long, []Label{{Name: "zone", Value: "us-east"}}, true},
		{"spared_" + long, []Label{{Name: "zone", Value: "eu-west"}}, true}, // the name rule wins first
	} {
		if got := s.Keep(tc.name, tc.labels); got != tc.want {
			t.Fatalf("with the memo full, Keep(%.10s..., %v) = %v, want %v", tc.name, tc.labels, got, tc.want)
		}
		if got := filter.Keep(tc.name, tc.labels); got != tc.want {
			t.Fatalf("unmemoized Keep(%.10s..., %v) = %v, want %v", tc.name, tc.labels, got, tc.want)
		}
	}
}

// pad8Scrape renders a fixed-width suffix so every generated key is exactly the
// intended length.
func pad8Scrape(i int) string {
	const digits = "0123456789"
	var b [8]byte
	for j := 7; j >= 0; j-- {
		b[j] = digits[i%10]
		i /= 10
	}
	return string(b[:])
}
