package metrics

import (
	"testing"
	"time"

	"github.com/JohanLindvall/haste/xxh3"
)

// expirationSeconds' rounding is load-bearing and was guarded by nothing:
// replacing math.Ceil with a truncation passed the whole package. Whole seconds
// are the storage (the clock is coarse), so a maxAge under one second has to
// round UP — truncating to 0 does not mean "no expiry", it means idle > 0 at
// every export a tick later, which is the "expire on every export" outcome the
// function's own doc says it prevents.
func TestExpirationSecondsRoundsUp(t *testing.T) {
	for _, tc := range []struct {
		in   time.Duration
		want int64
	}{
		{0, 0},
		{time.Nanosecond, 1},
		{500 * time.Millisecond, 1},
		{time.Second, 1},
		{time.Second + time.Nanosecond, 2},
		{90 * time.Second, 90},
		{90*time.Second + time.Millisecond, 91},
	} {
		if got := expirationSeconds(tc.in); got != tc.want {
			t.Fatalf("expirationSeconds(%v) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// The consequence, end to end: `maxAge: "500ms"` is a legal rule
// (config.Positive only refuses non-positive), and under a truncating
// conversion its series is zeroed at every export, turning a CUMULATIVE counter
// into per-interval deltas — the exact outcome maxAge's own error text warns
// about, with nothing anywhere reporting it. compileRule compares the same
// stored value between rules sharing a name, so the rounding also decides a
// conflict verdict.
func TestSubSecondMaxAgeKeepsTheCounterCumulative(t *testing.T) {
	t0 := int64(1_700_600_000)
	setTimeForTest(time.Unix(t0, 0))
	defer testEpoch.Store(0)

	s := newTestSeries(seriesSpec{name: "c", kind: kindCounter, expiration: 500 * time.Millisecond})
	s.observe(labels{}.set("k", "v"), 1, xxh3.Uint128{}, emptyResource, nil)

	setTimeForTest(time.Unix(t0+1, 0))
	first := s.snapshot()
	if len(first) != 1 || first[0].value != 1 {
		t.Fatalf("first export = %+v, want one sample of value 1", first)
	}

	s.observe(labels{}.set("k", "v"), 1, xxh3.Uint128{}, emptyResource, nil)
	setTimeForTest(time.Unix(t0+2, 0))
	second := s.snapshot()
	if len(second) != 1 || second[0].value != 2 {
		t.Fatalf("second export = %+v, want one sample of value 2: a sub-second maxAge must round up to one second, "+
			"not to zero — at zero the idle reset fires at every export and the cumulative counter reports deltas", second)
	}
}
