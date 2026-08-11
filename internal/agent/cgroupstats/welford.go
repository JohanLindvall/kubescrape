package cgroupstats

import "math"

// window accumulates one export window's samples of one signal: the count, the
// running mean and second moment (Welford), and the two extremes. It is reset
// whole at each export — see Sampler.snapshot for why the reset is not gated on
// a successful send.
//
// # Why Welford, explicitly
//
// The obvious implementation keeps sum and sum-of-squares and finishes with
// sqrt(sumsq/n - mean²). That expression subtracts two nearly equal large
// numbers, and the CPU case is exactly where it fails: a container sitting at a
// steady 2.0000 cores with millisecond jitter has a mean whose square is ~4 and
// a mean-of-squares that differs from it in the eighth significant digit.
// float64 carries about sixteen, so the difference is computed from the last
// eight bits of the mantissa — catastrophic cancellation. Measured, the naive
// form returns garbage (and routinely a small NEGATIVE variance, whose sqrt is
// NaN) on precisely the series an operator most wants to see: a large steady
// mean with a small variance, which is what a well-behaved container looks like
// right up to the moment it bursts. TestWelfordBeatsNaiveOnLargeMean pins it.
//
// Welford instead updates the mean and the second moment incrementally, so
// every quantity it holds is of the same order as the DEVIATIONS rather than of
// the values. It costs one extra multiply per sample, which at three preads per
// container per second is not a cost at all.
//
// This is the numerical half of the argument the package doc makes against
// internal/metrics/series.go's closed aggregation set — where the note "stddev
// alone needed Welford to dodge catastrophic cancellation" was a reason to keep
// stddev out of a general-purpose per-sample store. Here it is one accumulator
// for one signal on one bounded window, and the alternative is not a recording
// rule (the samples never reach the backend) but no signal at all.
type window struct {
	n    uint64
	mean float64
	m2   float64 // sum of squared deviations from the running mean
	min  float64
	max  float64
}

// add folds one observation in.
func (w *window) add(x float64) {
	w.n++
	if w.n == 1 {
		w.min, w.max = x, x
	} else {
		if x < w.min {
			w.min = x
		}
		if x > w.max {
			w.max = x
		}
	}
	delta := x - w.mean
	w.mean += delta / float64(w.n)
	// The second factor uses the UPDATED mean; that pairing is what makes the
	// increment exactly the deviation product and keeps m2 non-negative.
	w.m2 += delta * (x - w.mean)
}

// stddev is the POPULATION standard deviation of the window.
//
// Population (÷n), not sample (÷n-1): the window is not a sample drawn from a
// larger set that we are estimating: it IS the set. Every reading taken in the
// interval is in it, and the question the metric answers is "how much did this
// container's usage vary during this interval", not "what is the variance of
// the process it was drawn from". (Which of the two divisors a single
// observation would embarrass is moot: a window holding fewer than two samples
// of a signal is not published at all — it re-states the last distribution
// measured, see Sampler.finish — so neither the population 0 nor the sample
// NaN ever reaches a payload.)
func (w *window) stddev() float64 {
	if w.n == 0 {
		return 0
	}
	v := w.m2 / float64(w.n)
	if v <= 0 {
		// m2 is non-negative by construction; a rounding residue of -1e-18
		// would otherwise leave sqrt returning NaN, which serialises into the
		// payload and poisons every aggregation downstream.
		return 0
	}
	return math.Sqrt(v)
}

// reset empties the window for the next interval.
func (w *window) reset() { *w = window{} }
