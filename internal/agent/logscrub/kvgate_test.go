package logscrub

import "testing"

// kvStart is the byte gate secretKVCandidate consults before it will look at a
// position at all, so a byte MISSING from it is a keyword the prefilter can
// never see — the prefilter goes narrower than its regex and the secret ships
// in clear. It is derived from kvDispatch at init; this asserts the derivation
// covers both ASCII cases of every dispatch byte and nothing else, which is
// what makes moving the case fold off the per-byte path safe.
func TestKvStartGateCoversEveryDispatchByte(t *testing.T) {
	for c := range 256 {
		lower := lowerASCII(byte(c))
		want := len(kvDispatch[lower]) > 0
		if got := kvStart[c]; got != want {
			t.Errorf("kvStart[%q] = %v, want %v (dispatch under %q)", byte(c), got, want, lower)
		}
	}
	// Spot-check the intent rather than only the mechanism: every keyword's
	// first byte, in both cases, must pass the gate.
	for _, parts := range kvKeywords {
		for _, w := range kvExpand(parts) {
			for _, c := range []byte{lowerASCII(w[0]), upperASCII(w[0])} {
				if !kvStart[c] {
					t.Errorf("the gate rejects %q, the first byte of keyword %q", c, w)
				}
			}
		}
	}
}
