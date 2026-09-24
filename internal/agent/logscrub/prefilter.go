package logscrub

// The zero-allocation prefilter primitives the built-in patterns gate their
// regexes with, and asciiFold — the REGEX side of the ASCII-only case folding
// these scans implement, kept beside them because the two must agree exactly:
// a prefilter narrower than its regex ships a secret unredacted.

import (
	"regexp"
	"strings"
)

// asciiFold renders a literal keyword as a regex matching exactly its ASCII
// case variants ("key" -> "[Kk][Ee][Yy]").
//
// The built-in patterns use this instead of (?i) because Go's (?i) folds via
// unicode.SimpleFold: `(?i)password` also matches `paſsword` (U+017F LATIN
// SMALL LETTER LONG S) and `(?i)token` matches `toKen` (U+212A KELVIN SIGN).
// The literal prefilters that gate those regexes fold ASCII only, so such a
// line was rejected by the prefilter, the pattern was skipped entirely, and
// the secret shipped unredacted — a security control quietly narrower than the
// regex it advertises. Constraining the regex to ASCII case makes the two
// exactly equal, which is what the prefilter optimisation requires to be safe.
func asciiFold(word string) string {
	var b strings.Builder
	for i := 0; i < len(word); i++ {
		c := word[i]
		switch {
		case c >= 'a' && c <= 'z':
			b.WriteByte('[')
			b.WriteByte(c - 'a' + 'A')
			b.WriteByte(c)
			b.WriteByte(']')
		case c >= 'A' && c <= 'Z':
			b.WriteByte('[')
			b.WriteByte(c)
			b.WriteByte(c - 'A' + 'a')
			b.WriteByte(']')
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	return b.String()
}

func containsFold(sub string) func(string) bool {
	lower := strings.ToLower(sub)
	return func(s string) bool {
		// A TRUE ASCII case-insensitive scan, matching the ASCII-only case
		// classes the guarded regexes use (see asciiFold): the prefilter must
		// be neither narrower NOR wider than its regex. The original
		// three-casing (lower/UPPER/Title) check let a mixed-case keyword
		// (`bEaReR`, `pAsSwOrD`) skip redaction — a real secret-leak gap.
		// Zero-alloc.
		return asciiIndexFold(s, lower) >= 0
	}
}

// asciiIndexFold returns the index of the first ASCII-case-insensitive
// occurrence of lowerSub (which must already be lowercase) in s, or -1. It
// allocates nothing.
//
// The candidate positions come from strings.IndexByte on the needle's first
// byte in BOTH cases, not from walking every offset: IndexByte is the
// architecture's vectorised scan (tens of bytes per cycle) where the walk was
// a fold and a compare per byte. This function was 46% of the whole no-match
// scrub path — the two literal prefilters "bearer" and "basic" run it on every
// exported record on the tailer's single sweep goroutine — and an ordinary log
// line contains almost no 'b' or 'B' to verify.
//
// The two cursors are what keep it LINEAR. A search that restarts both scans
// after every rejected candidate is quadratic on a line that is dense in one
// case and holds the other only near the end (a megabyte of 'b' followed by
// one 'B'), which is exactly the shape a hostile record takes — see
// BenchmarkScrubHostileLongLine for why that matters here. Each cursor instead
// resumes where its own previous IndexByte stopped, so each case is scanned
// across the line at most once in total.
func asciiIndexFold(s, lowerSub string) int {
	n := len(lowerSub)
	if n == 0 {
		return 0
	}
	limit := len(s) - n // the last index at which a match can begin
	if limit < 0 {
		return -1
	}
	lo := lowerSub[0]
	up := upperASCII(lo)
	nextLo := indexByteFrom(s, lo, 0)
	nextUp := -1
	if up != lo {
		nextUp = indexByteFrom(s, up, 0)
	}
	for {
		k := nextLo
		if k < 0 || (nextUp >= 0 && nextUp < k) {
			k = nextUp
		}
		if k < 0 || k > limit {
			return -1
		}
		if hasPrefixFold(s[k:], lowerSub) {
			return k
		}
		// Advance only the cursor(s) that produced this candidate.
		if nextLo == k {
			nextLo = indexByteFrom(s, lo, k+1)
		}
		if nextUp == k {
			nextUp = indexByteFrom(s, up, k+1)
		}
	}
}

// indexByteFrom is strings.IndexByte from an absolute offset, returning an
// absolute index.
func indexByteFrom(s string, c byte, from int) int {
	if from >= len(s) {
		return -1
	}
	if i := strings.IndexByte(s[from:], c); i >= 0 {
		return from + i
	}
	return -1
}

// digitRun reports a run of >= n digits, ignoring single spaces/dashes inside.
func digitRun(n int) func(string) bool {
	return func(s string) bool {
		run := 0
		for i := 0; i < len(s); i++ {
			c := s[i]
			switch {
			case c >= '0' && c <= '9':
				run++
				if run >= n {
					return true
				}
			case (c == ' ' || c == '-') && run > 0:
				// separator inside a group: allowed, does not reset
			default:
				run = 0
			}
		}
		return false
	}
}

// upperASCII is lowerASCII's inverse: asciiIndexFold's second cursor, and
// kvStart's raw-byte gate.
func upperASCII(c byte) byte {
	if 'a' <= c && c <= 'z' {
		c -= 'a' - 'A'
	}
	return c
}

func lowerASCII(c byte) byte {
	if 'A' <= c && c <= 'Z' {
		c += 'a' - 'A'
	}
	return c
}

// hasPrefixFold reports whether s starts with the (already lowercase) word,
// ASCII-case-insensitively — the same folding asciiFold gives the regexes.
func hasPrefixFold(s, lowerWord string) bool {
	if len(s) < len(lowerWord) {
		return false
	}
	for i := 0; i < len(lowerWord); i++ {
		if lowerASCII(s[i]) != lowerWord[i] {
			return false
		}
	}
	return true
}
