package tracehash

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/JohanLindvall/haste/rapidhash"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// The two adopters' OLD threshold spellings, verbatim. Threshold must be
// bit-identical to both for every input they could pass — the nesting contract
// is an equality of uint64s, so "close" is not equal.
func TestThresholdMatchesBothOldSpellings(t *testing.T) {
	// tracesample.New: p in (0,1] after clamping; p == 1 was MaxUint64.
	for _, p := range []float64{1e-9, 0.0001, 0.1, 0.25, 1.0 / 3, 0.5, 0.75, 0.9999999999999999, 1} {
		var old uint64
		if p == 1 {
			old = math.MaxUint64
		} else {
			old = uint64(p * float64(math.MaxUint64))
		}
		if got := Threshold(p); got != old {
			t.Errorf("Threshold(%v) = %d, tracesample's old computation = %d", p, got, old)
		}
	}
	// tailsample.compileProbabilistic: pct in [0,100]; pct >= 100 was MaxUint64,
	// else uint64(pct/100*float64(math.MaxUint64)). The caller now passes
	// pct/100, so the division happens at the call site exactly as before.
	for _, pct := range []float64{0, 1e-7, 0.01, 10, 100.0 / 3, 50, 99, 99.99999999999999, 100} {
		var old uint64
		if pct >= 100 {
			old = math.MaxUint64
		} else {
			old = uint64(pct / 100 * float64(math.MaxUint64))
		}
		if got := Threshold(pct / 100); got != old {
			t.Errorf("Threshold(%v/100) = %d, tailsample's old computation = %d", pct, got, old)
		}
	}
}

func TestThresholdSaturates(t *testing.T) {
	for _, f := range []float64{1, 1.0000000000000002, 2, math.Inf(1)} {
		if got := Threshold(f); got != math.MaxUint64 {
			t.Errorf("Threshold(%v) = %d, want MaxUint64", f, got)
		}
	}
}

// The keep-all guard is the micro-drift this package closed: without it a trace
// whose hash is exactly MaxUint64 fails the strict comparison and a sampler
// configured to keep everything drops one trace in 2^64. That hash is the ONLY
// input on which the guard is observable, and no id a test can write is known
// to produce it — a Keep-level test with an arbitrary id passes with the guard
// deleted, which is what this one used to be. So the guard lives in keepHash,
// and this hands keepHash the hash itself.
func TestKeepHashKeepsEveryHashAtTheKeepAllThreshold(t *testing.T) {
	for _, h := range []uint64{0, 1, 1 << 63, math.MaxUint64 - 1, math.MaxUint64} {
		if !keepHash(h, math.MaxUint64) {
			t.Errorf("keepHash(%#x, MaxUint64) = false: the keep-all threshold dropped a trace", h)
		}
	}
	// Below the keep-all threshold it is the strict comparison — including at
	// MaxUint64-1, the largest threshold Threshold can return short of
	// saturating.
	for _, tc := range []struct {
		h, threshold uint64
		want         bool
	}{
		{0, 0, false}, // a zero threshold keeps nothing
		{math.MaxUint64, 0, false},
		{41, 42, true},
		{42, 42, false},
		{math.MaxUint64 - 2, math.MaxUint64 - 1, true},
		{math.MaxUint64 - 1, math.MaxUint64 - 1, false},
		{math.MaxUint64, math.MaxUint64 - 1, false},
	} {
		if got := keepHash(tc.h, tc.threshold); got != tc.want {
			t.Errorf("keepHash(%#x, %#x) = %v, want %v", tc.h, tc.threshold, got, tc.want)
		}
	}
}

// Keep at the two extreme thresholds. This CANNOT see the keep-all guard (see
// TestKeepHashKeepsEveryHashAtTheKeepAllThreshold); it pins Keep's own early
// return and the zero threshold, over ids spread across the hash space.
func TestKeepAtTheExtremeThresholds(t *testing.T) {
	for i := range 256 {
		var id pcommon.TraceID
		id[0], id[7], id[15] = byte(i), byte(i*13), byte(i^0xa5)
		if !Keep(id, math.MaxUint64) {
			t.Fatalf("Keep(%v, MaxUint64) = false; a keep-all threshold must keep every trace", id)
		}
		if Keep(id, 0) {
			t.Fatalf("Keep(%v, 0) = true; a zero threshold must keep nothing", id)
		}
	}
}

// Keep is the strict hash comparison for every non-saturated threshold — the
// other half of what both adopters spelled out.
func TestKeepIsTheHashComparison(t *testing.T) {
	thr := Threshold(0.5)
	for i := range byte(200) {
		var id pcommon.TraceID
		id[0], id[15] = i, i^0x5a
		if got, want := Keep(id, thr), rapidhash.Sum64(id[:]) < thr; got != want {
			t.Fatalf("Keep(%v) = %v, want %v (raw hash comparison)", id, got, want)
		}
	}
}

// Keep feeds the trace id to rapidhash as its two little-endian halves rather
// than as a slice, because that form takes them in registers on a path that
// runs once per span in BOTH samplers (Keep's doc carries the measurement). It
// is only a legitimate substitution because it hashes exactly the same bytes —
// this pins that, so the fast form can never quietly drift from the definition
// it shortcuts.
func TestUint128FormEqualsRawBytes(t *testing.T) {
	for i := range 5000 {
		var id pcommon.TraceID
		for j := range id {
			id[j] = byte(i*7 + j*31)
		}
		lo := binary.LittleEndian.Uint64(id[0:8])
		hi := binary.LittleEndian.Uint64(id[8:16])
		if got, want := rapidhash.Sum64Uint128(lo, hi), rapidhash.Sum64(id[:]); got != want {
			t.Fatalf("id %x: uint128 form = %x, raw-bytes form = %x", id, got, want)
		}
	}
}
