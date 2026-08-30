package logscrub

import (
	"math/rand/v2"
	"strings"
	"testing"
)

// asciiIndexFold is a security primitive: it is the whole of the "bearer" and
// "basic-auth" prefilters, and a prefilter that is NARROWER than its regex
// ships the secret in clear. Its implementation is an IndexByte-driven scan
// with two cursors rather than the obvious walk, so it is pinned against the
// obvious walk — which is the DEFINITION — on every input these tests can
// reach.

// naiveIndexFold is the specification: the first offset at which lowerSub
// occurs, comparing ASCII-case-insensitively.
func naiveIndexFold(s, lowerSub string) int {
	n := len(lowerSub)
	if n == 0 {
		return 0
	}
	for i := 0; i+n <= len(s); i++ {
		k := 0
		for ; k < n; k++ {
			c := s[i+k]
			if 'A' <= c && c <= 'Z' {
				c += 'a' - 'A'
			}
			if c != lowerSub[k] {
				break
			}
		}
		if k == n {
			return i
		}
	}
	return -1
}

func TestAsciiIndexFoldMatchesTheNaiveWalk(t *testing.T) {
	needles := []string{"bearer", "basic", "b", "ab", "@k"}
	// Exhaustive over a small alphabet that contains both cases of the needles'
	// bytes, the case-adjacent punctuation ('[' is 'Z'+1, '`' is 'a'-1 — the
	// bytes a sloppy fold condition wrongly folds) and a filler.
	alphabet := []byte("bBaA@k[`x")
	var buf []byte
	var rec func(n int)
	rec = func(n int) {
		if n == 0 {
			s := string(buf)
			for _, needle := range needles {
				if got, want := asciiIndexFold(s, needle), naiveIndexFold(s, needle); got != want {
					t.Fatalf("asciiIndexFold(%q, %q) = %d, want %d", s, needle, got, want)
				}
			}
			return
		}
		for _, c := range alphabet {
			buf = append(buf, c)
			rec(n - 1)
			buf = buf[:len(buf)-1]
		}
	}
	for n := range 4 {
		rec(n)
	}
}

func FuzzAsciiIndexFold(f *testing.F) {
	f.Add("Authorization: BEARER abc", "bearer")
	f.Add("basicsomething", "basic")
	f.Add("BBBBBBBBBBBBBBBBb", "b")
	f.Add("", "bearer")
	f.Fuzz(func(t *testing.T, s, needle string) {
		lower := strings.ToLower(needle)
		if got, want := asciiIndexFold(s, lower), naiveIndexFold(s, lower); got != want {
			t.Fatalf("asciiIndexFold(%q, %q) = %d, want %d", s, lower, got, want)
		}
	})
}

// The cursor pair exists to keep the scan LINEAR on a line that is dense in
// one case of the needle's first byte and holds the other only at the very
// end. A per-candidate restart is quadratic there — 1 MiB of 'b' would be
// ~10^12 byte comparisons — so this asserts the answer on exactly that shape
// at a size that would not finish if the implementation regressed.
func TestAsciiIndexFoldIsLinearOnACaseDenseLine(t *testing.T) {
	s := strings.Repeat("b", 1<<20) + "Bearer x"
	if got, want := asciiIndexFold(s, "bearer"), 1<<20; got != want {
		t.Fatalf("index = %d, want %d", got, want)
	}
	// The mirror: dense uppercase, the lowercase needle byte only at the end.
	s = strings.Repeat("B", 1<<20) + "bearer x"
	if got, want := asciiIndexFold(s, "bearer"), 1<<20; got != want {
		t.Fatalf("index = %d, want %d", got, want)
	}
}

// A needle longer than the line, and an empty line, must not index out of
// range — the limit guard is what makes the cursor loop safe to dereference.
func TestAsciiIndexFoldShortInputs(t *testing.T) {
	for _, tc := range []struct{ s, sub string }{
		{"", "bearer"}, {"b", "bearer"}, {"bearе", "bearer"}, {"bearer", "bearer"}, {"", ""}, {"x", ""},
	} {
		if got, want := asciiIndexFold(tc.s, tc.sub), naiveIndexFold(tc.s, tc.sub); got != want {
			t.Fatalf("asciiIndexFold(%q, %q) = %d, want %d", tc.s, tc.sub, got, want)
		}
	}
}

// A random differential sweep over log-shaped bytes, so the exhaustive test's
// tiny alphabet is not the only evidence.
func TestAsciiIndexFoldRandomDifferential(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 11))
	alphabet := []byte("bBaAsSiIcCeErR ={}\"'@/0189")
	for range 20000 {
		n := r.IntN(40)
		buf := make([]byte, n)
		for i := range buf {
			buf[i] = alphabet[r.IntN(len(alphabet))]
		}
		s := string(buf)
		for _, needle := range []string{"bearer", "basic"} {
			if got, want := asciiIndexFold(s, needle), naiveIndexFold(s, needle); got != want {
				t.Fatalf("asciiIndexFold(%q, %q) = %d, want %d", s, needle, got, want)
			}
		}
	}
}
