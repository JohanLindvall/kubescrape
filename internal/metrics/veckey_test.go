package metrics

import "testing"

// vecKey's netstring encoding must keep aliasing tuples distinct: with a plain
// separator, ("x\x00y","z") and ("x","y\x00z") would collide. Only the
// single-label fast path may return the raw value.
func TestVecKeyMultiLabelCollisionProof(t *testing.T) {
	r := NewRegistry()
	v := r.CounterVec("test_veckey_total", "t", "a", "b")
	v.WithLabelValues("x\x00y", "z").Add(1)
	v.WithLabelValues("x", "y\x00z").Add(2)
	v.WithLabelValues("1:x", "").Add(4)
	v.WithLabelValues("", "1:x").Add(8)

	// Four distinct tuples → four independent counters.
	for _, tc := range []struct {
		vals []string
		want float64
	}{
		{[]string{"x\x00y", "z"}, 1},
		{[]string{"x", "y\x00z"}, 2},
		{[]string{"1:x", ""}, 4},
		{[]string{"", "1:x"}, 8},
	} {
		if got := v.WithLabelValues(tc.vals...).Value(); got != tc.want {
			t.Fatalf("tuple %q = %v, want %v (tuples aliased)", tc.vals, got, tc.want)
		}
	}
}

// Prometheus semantics: an empty label value is equivalent to the label being
// absent (labels.set drops empty values), so a short call and a padded call
// deliberately share one series — while values that merely CONTAIN the
// netstring syntax stay distinct.
func TestVecKeyEmptyValueEquivalence(t *testing.T) {
	r := NewRegistry()
	v := r.CounterVec("test_veckey_short_total", "t", "a", "b")
	v.WithLabelValues("1:x").Add(1)       // {a="1:x"} — 1 value, 2-label vec
	v.WithLabelValues("1:x", "").Add(100) // {a="1:x", b=""} ≡ {a="1:x"}
	v.WithLabelValues("x", "").Add(10)    // {a="x"} — distinct

	if got := v.WithLabelValues("1:x").Value(); got != 101 {
		t.Fatalf("{a=1:x} = %v, want 101 (empty-b call must merge, not fork)", got)
	}
	if got := v.WithLabelValues("x", "").Value(); got != 10 {
		t.Fatalf("{a=x} = %v, want 10 (must stay distinct from {a=1:x})", got)
	}
}
