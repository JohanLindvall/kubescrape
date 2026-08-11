package cgroupstats

import (
	"math"
	"testing"
)

// naiveStddev is the sum-of-squares form Welford replaces. It is here as the
// CONTROL for TestWelfordBeatsNaiveOnLargeMean: the claim in welford.go's doc
// is not "Welford is nicer", it is "the obvious form is wrong on exactly the
// series this package samples", and a claim like that is worth demonstrating
// rather than asserting.
func naiveStddev(xs []float64) float64 {
	var sum, sumsq float64
	for _, x := range xs {
		sum += x
		sumsq += x * x
	}
	n := float64(len(xs))
	mean := sum / n
	return math.Sqrt(sumsq/n - mean*mean)
}

func fold(xs ...float64) *window {
	var w window
	for _, x := range xs {
		w.add(x)
	}
	return &w
}

// TestWelfordAgainstHandComputedValues proves the maths on values whose
// population standard deviation can be written down.
func TestWelfordAgainstHandComputedValues(t *testing.T) {
	cases := []struct {
		name string
		in   []float64
		// want is the POPULATION standard deviation, sqrt(sum((x-mean)^2)/n).
		want float64
		mean float64
		min  float64
		max  float64
	}{
		{
			// mean 4; deviations -2,-1,0,1,2; sum of squares 10; 10/5 = 2.
			name: "2 4 4 4 5 5 7 9 style, reduced",
			in:   []float64{2, 3, 4, 5, 6},
			want: math.Sqrt(2),
			mean: 4, min: 2, max: 6,
		},
		{
			// The textbook set: mean 5, variance 4, stddev exactly 2.
			name: "population stddev is exactly 2",
			in:   []float64{2, 4, 4, 4, 5, 5, 7, 9},
			want: 2,
			mean: 5, min: 2, max: 9,
		},
		{
			// One observation is a legal window (a container discovered
			// mid-window has exactly one memory reading): zero spread, not NaN.
			name: "single sample",
			in:   []float64{1.25},
			want: 0,
			mean: 1.25, min: 1.25, max: 1.25,
		},
		{
			name: "constant series has zero spread",
			in:   []float64{3, 3, 3, 3},
			want: 0,
			mean: 3, min: 3, max: 3,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := fold(tc.in...)
			if got := w.stddev(); math.Abs(got-tc.want) > 1e-12 {
				t.Errorf("stddev = %v, want %v", got, tc.want)
			}
			if math.Abs(w.mean-tc.mean) > 1e-12 {
				t.Errorf("mean = %v, want %v", w.mean, tc.mean)
			}
			if w.min != tc.min || w.max != tc.max {
				t.Errorf("min/max = %v/%v, want %v/%v", w.min, w.max, tc.min, tc.max)
			}
			if int(w.n) != len(tc.in) {
				t.Errorf("n = %d, want %d", w.n, len(tc.in))
			}
		})
	}
}

// TestWelfordBeatsNaiveOnLargeMean is the numerical argument in welford.go made
// executable: a large steady mean with a small variance is what a well-behaved
// container looks like, and the sum-of-squares form loses it to cancellation.
func TestWelfordBeatsNaiveOnLargeMean(t *testing.T) {
	// A container pinned near 1e9 "cores" is not realistic; the magnitude is
	// what makes the failure reproducible in one test rather than statistically.
	// The same effect at a realistic 2.0 cores costs precision in the last
	// digits instead of all of them, which is exactly as wrong and much harder
	// to write an assertion about.
	const base = 1e9
	xs := make([]float64, 0, 8)
	for _, d := range []float64{-3, -2, -1, 0, 0, 1, 2, 3} {
		xs = append(xs, base+d)
	}
	// Deviations -3..3 summing to 0; sum of squares 28; population variance
	// 28/8 = 3.5.
	want := math.Sqrt(3.5)

	// 1e-6 rather than machine epsilon: at 1e9 the INPUTS are only
	// representable to ~1.2e-7, so the deviations are already quantised before
	// either algorithm sees them. What is being measured is the gap between
	// that irreducible floor and the naive form's error, which at this
	// magnitude is total — it returns 0.
	got := fold(xs...).stddev()
	if rel := math.Abs(got-want) / want; rel > 1e-6 {
		t.Fatalf("Welford stddev = %v, want %v (relative error %g)", got, want, rel)
	}

	naive := naiveStddev(xs)
	if math.IsNaN(naive) {
		t.Logf("the naive sum-of-squares form returned NaN (negative variance from cancellation), as documented")
		return
	}
	if rel := math.Abs(naive-want) / want; rel < 1e-3 {
		t.Fatalf("the naive form returned %v (relative error %g), which is accurate enough that this test no longer "+
			"demonstrates what welford.go claims — recheck the claim before relaxing the test", naive, rel)
	}
	t.Logf("naive = %v vs Welford = %v (want %v)", naive, got, want)
}

// TestWelfordMatchesNaiveWhenNaiveIsSafe keeps the two honest against each
// other on ordinary values, so the test above is measuring cancellation and not
// a bug in one of the implementations.
func TestWelfordMatchesNaiveWhenNaiveIsSafe(t *testing.T) {
	xs := []float64{0.1, 0.4, 3.2, 0.05, 1.9, 2.4, 0.0, 4.0}
	w := fold(xs...).stddev()
	n := naiveStddev(xs)
	if math.Abs(w-n) > 1e-9 {
		t.Fatalf("Welford %v vs naive %v on well-conditioned input", w, n)
	}
}

func TestWindowResetEmptiesEverything(t *testing.T) {
	w := fold(1, 2, 3)
	w.reset()
	if w.n != 0 || w.mean != 0 || w.m2 != 0 || w.min != 0 || w.max != 0 {
		t.Fatalf("reset left %+v", *w)
	}
	if got := w.stddev(); got != 0 {
		t.Fatalf("stddev of an empty window = %v, want 0", got)
	}
}

// A rounding residue in m2 must never reach math.Sqrt as a negative number: a
// NaN in one data point poisons every aggregation that touches it downstream.
func TestStddevNeverNaN(t *testing.T) {
	var w window
	w.n, w.m2 = 4, -1e-18
	if got := w.stddev(); math.IsNaN(got) || got != 0 {
		t.Fatalf("stddev with a tiny negative m2 = %v, want 0", got)
	}
}
