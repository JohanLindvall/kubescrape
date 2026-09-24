package kubemeta

import (
	"errors"
	"fmt"
	"regexp"
	"regexp/syntax"
	"strings"
	"unicode/utf8"
)

// CompileRelabelRegex compiles a relabel rule's regex with Prometheus
// semantics: FULLY ANCHORED, and the empty regex meaning Prometheus' default
// "(.*)".
//
// It is the one spelling of that wrap for the two doors that must agree on it:
// the metadata service validates a monitor's keep/drop rules at the parse door
// (internal/servicemonitors) and the agent compiles the same RelabelRule it is
// served (internal/agent/promscrape). The wrap is not cosmetic — "a)|(b" does
// not compile on its own and DOES compile as "^(?:a)|(b)$" — so a door that
// checked the bare regex would admit a rule the other door refuses, or the
// reverse.
//
// It refuses a regex over MaxRelabelRegexCost BEFORE compiling it (see
// RelabelRegexCost), with an error wrapping ErrRelabelRegexTooLarge: the
// parse door's validation is exactly RelabelRegexCost, so the two doors also
// agree on what is too large, and the agent never pays the compile the
// metadata service refused to serve.
func CompileRelabelRegex(regex string) (*regexp.Regexp, error) {
	if _, err := RelabelRegexCost(regex); err != nil {
		return nil, err
	}
	return regexp.Compile(anchoredRelabelRegex(regex))
}

func anchoredRelabelRegex(regex string) string {
	if regex == "" {
		regex = "(.*)"
	}
	return "^(?:" + regex + ")$"
}

// MaxRelabelRegexCost bounds what ONE relabel regex may cost, in the units
// RelabelRegexCost measures. It is twice what an 8 KiB metric-name allowlist —
// the legitimately-large shape, one `keep` with a long alternation — measures
// (~8k), so the bound binds on the shapes whose cost is NOT proportional to
// their text and on nothing an operator writes on purpose. At the bound a
// compile costs a few milliseconds and a few megabytes allocated; the refused
// shapes cost seconds and hundreds of megabytes (see RelabelRegexCost).
const MaxRelabelRegexCost = 16 << 10

// ErrRelabelRegexTooLarge is wrapped by the error RelabelRegexCost and
// CompileRelabelRegex return for a regex over MaxRelabelRegexCost, so a caller
// can tell "too large" (shrink it) from "not a regex" (fix it).
var ErrRelabelRegexTooLarge = errors.New("relabel regex is too large to compile")

// RelabelRegexCost reports what compiling and holding a relabel regex would
// cost, WITHOUT compiling it: the same anchored wrap and the same flags as
// CompileRelabelRegex, and — since a parse is where every regexp.Compile error
// comes from (syntax.Compile cannot fail) — the same verdict on whether it is
// a regex at all. A regex over MaxRelabelRegexCost returns an error wrapping
// ErrRelabelRegexTooLarge.
//
// The bound exists because a regex's cost is not proportional to its text, and
// its text is all a byte ceiling can see. Go's regexp spends it in three places,
// each measured on an 8 KiB regex (inside every byte ceiling a relabel chain
// has), and the unit — roughly one compiled instruction, or one rune-range
// step of the parser — charges each:
//
//   - COMPILE, through counted repetition: `a{1000}` repeated 1170 times
//     compiles to ~1.17M instructions — ~0.5-0.9 s, ~350 MB allocated and
//     ~52 MB RETAINED per compiled regex. Measured after the parse, by walking
//     the tree the way the compiler's own size check does (repetition
//     multiplies, nothing is expanded).
//   - PARSE, through case-folded class ranges: under (?i) every rune of a
//     class range between U+0041 and U+1E943 is folded one at a time, so the
//     17-byte `(?i)[B-\x{1E942}]` costs ~6.5 ms and 500 of them ~2.5 s — to
//     PARSE, before anything is compiled. Measured before the parse.
//   - PARSE, through Unicode classes: each \p or \P appends a whole Unicode
//     table (up to ~1300 ranges), and one class holding 2700 of them costs
//     ~250 ms and ~80 MB to parse. Also measured before the parse.
//
// So the text is scanned first — a single pass over the bytes that charges
// the two parse-side amplifiers without doing their work — and a regex whose
// scan is already over the bound is refused without being parsed at all.
func RelabelRegexCost(regex string) (int, error) {
	src := anchoredRelabelRegex(regex)
	cost, _ := parseCost(src, MaxRelabelRegexCost)
	if cost > MaxRelabelRegexCost {
		return cost, fmt.Errorf("%w (limit %d)", ErrRelabelRegexTooLarge, MaxRelabelRegexCost)
	}
	re, err := syntax.Parse(src, syntax.Perl)
	if err != nil {
		return 0, err
	}
	var ranges int
	size := progCost(re, MaxRelabelRegexCost, &ranges)
	cost = max(cost, size+ranges)
	if cost > MaxRelabelRegexCost {
		return cost, fmt.Errorf("%w (limit %d)", ErrRelabelRegexTooLarge, MaxRelabelRegexCost)
	}
	return cost, nil
}

// The parse-side charges. unicodeClassCharge is at least the number of ranges
// the parser appends for the largest Unicode table plus its case-fold table
// (1318, for Ll, in the Unicode version Go 1.27 ships);
// TestUnicodeClassChargeCoversEveryTable holds it there. foldGroupCharge covers
// the per-rune folding of an ASCII class (\w, [:alpha:], …) under (?i), which
// walks at most the 63 runes between U+0041 and U+007F.
const (
	unicodeClassCharge = 2048
	foldGroupCharge    = 128
	// The runes Go's parser folds one at a time (regexp/syntax minFold and
	// maxFold); a range outside them, or covering all of them, costs nothing.
	minFold = 0x0041
	maxFold = 0x1e943
)

// parseCost scans src the way regexp/syntax tokenises it and charges what the
// PARSER will spend: one per byte, plus unicodeClassCharge per \p/\P, plus —
// when the regex can turn case folding on anywhere — the folded width of every
// class range and foldGroupCharge per ASCII class. It stops once the total
// passes limit. It must never charge LESS than the parser spends on a regex
// the parser accepts; it may charge more (folding is assumed everywhere if any
// group enables it, even one that only turns it off).
//
// complete is false where it stopped tokenising before the end: the parser
// rejects the regex at that same place, so nothing after it is ever parsed —
// and FuzzRelabelRegexCostAgreesWithCompile holds that it never happens on a
// regex the parser accepts, which is what keeps the charge from silently
// stopping short.
func parseCost(src string, limit int) (cost int, complete bool) {
	fold := mayFoldCase(src)
	cost = len(src)
	for i := 0; i < len(src) && cost <= limit; {
		switch src[i] {
		case '\\':
			if i+1 >= len(src) {
				return cost, false // trailing backslash
			}
			switch c := src[i+1]; c {
			case 'Q':
				// \Q…\E is literal text up to \E, or to the end.
				end := strings.Index(src[i+2:], `\E`)
				if end < 0 {
					return cost, true
				}
				i += 2 + end + 2
				continue
			case 'p', 'P':
				n, ok := unicodeClassLen(src[i:])
				if !ok {
					return cost, false
				}
				cost += unicodeClassCharge
				i += n
				continue
			default:
				if fold && isPerlClass(c) {
					cost += foldGroupCharge
				}
			}
			i += 2
		case '[':
			c, n, ok := classCost(src[i:], fold)
			cost += c
			if !ok {
				return cost, false
			}
			i += n
		default:
			i++
		}
	}
	return cost, true
}

// mayFoldCase reports whether any `(?flags` group turns on case folding (i).
// Deliberately context-free — an `(?i` inside a class or a \Q…\E quote counts
// too — because over-charging is the safe direction.
func mayFoldCase(src string) bool {
	for i := 0; ; {
		j := strings.Index(src[i:], "(?")
		if j < 0 {
			return false
		}
		i += j + 2
		for ; i < len(src) && strings.IndexByte("imsU-", src[i]) >= 0; i++ {
			if src[i] == 'i' {
				return true
			}
		}
	}
}

func isPerlClass(c byte) bool { return strings.IndexByte("dDsSwW", c) >= 0 }

// unicodeClassLen is how many bytes the \p/\P escape at the start of s spans:
// `\pL` (one rune of name) or `\p{Name}`. ok is false where the parser rejects
// it; whether the NAME exists is left to the parser, which refuses an unknown
// one at the same place.
func unicodeClassLen(s string) (int, bool) {
	c, size := utf8.DecodeRuneInString(s[2:])
	switch {
	case size == 0 || c == utf8.RuneError && size == 1:
		return 0, false
	case c != '{':
		return 2 + size, true
	}
	end := strings.IndexByte(s, '}')
	if end < 0 {
		return 0, false
	}
	return end + 1, true
}

// classCost charges the bracketed class at the start of s (s[0] == '['),
// mirroring regexp/syntax's parseClass token for token, and returns the charge
// and the bytes it spans. ok is false where the parser rejects the class.
func classCost(s string, fold bool) (cost, n int, ok bool) {
	t := 1
	if t < len(s) && s[t] == '^' {
		t++
	}
	for first := true; t >= len(s) || s[t] != ']' || first; first = false {
		if t >= len(s) {
			return cost, t, false // missing ]
		}
		// POSIX [:alnum:]; an unknown name is the parser's error.
		if len(s)-t > 2 && s[t] == '[' && s[t+1] == ':' {
			if end := strings.Index(s[t+2:], ":]"); end >= 0 {
				if fold {
					cost += foldGroupCharge
				}
				t += 2 + end + 2
				continue
			}
		}
		if s[t] == '\\' && t+1 < len(s) {
			if c := s[t+1]; c == 'p' || c == 'P' {
				n, ok := unicodeClassLen(s[t:])
				if !ok {
					return cost, t, false
				}
				cost += unicodeClassCharge
				t += n
				continue
			} else if isPerlClass(c) {
				if fold {
					cost += foldGroupCharge
				}
				t += 2
				continue
			}
		}
		lo, size, ok := classChar(s[t:])
		if !ok {
			return cost, t, false
		}
		t += size
		hi := lo
		// [a-] means (a|-): only a '-' followed by something other than ']'
		// makes a range.
		if len(s)-t >= 2 && s[t] == '-' && s[t+1] != ']' {
			if hi, size, ok = classChar(s[t+1:]); !ok || hi < lo {
				return cost, t, false
			}
			t += 1 + size
		}
		if fold {
			cost += foldedRangeCost(lo, hi)
		}
	}
	return cost, t + 1, true
}

// foldedRangeCost is how many runes regexp/syntax's appendFoldedRange folds
// one by one for [lo, hi], including its two shortcuts.
func foldedRangeCost(lo, hi rune) int {
	if lo <= minFold && hi >= maxFold || hi < minFold || lo > maxFold {
		return 1
	}
	return int(min(hi, maxFold)-max(lo, minFold)) + 1
}

// classChar decodes one class character at the start of s — an escape or a
// literal rune — exactly as regexp/syntax's parseClassChar and parseEscape do,
// returning the rune and the bytes it spans. ok is false where they return an
// error.
func classChar(s string) (r rune, n int, ok bool) {
	if s == "" {
		return 0, 0, false
	}
	if s[0] != '\\' {
		r, n = utf8.DecodeRuneInString(s)
		return r, n, r != utf8.RuneError || n != 1
	}
	c, size := utf8.DecodeRuneInString(s[1:])
	if size == 0 || c == utf8.RuneError && size == 1 {
		return 0, 0, false
	}
	t := 1 + size
	switch {
	case c >= '1' && c <= '7' && (t >= len(s) || s[t] < '0' || s[t] > '7'):
		return 0, 0, false // a backreference, which Go does not support
	case c >= '0' && c <= '7':
		r = c - '0'
		for i := 1; i < 3 && t < len(s) && s[t] >= '0' && s[t] <= '7'; i++ {
			r = r*8 + rune(s[t]-'0')
			t++
		}
		return r, t, true
	case c == 'x':
		return hexEscape(s, t)
	}
	switch c {
	case 'a':
		return '\a', t, true
	case 'f':
		return '\f', t, true
	case 'n':
		return '\n', t, true
	case 'r':
		return '\r', t, true
	case 't':
		return '\t', t, true
	case 'v':
		return '\v', t, true
	}
	if c < utf8.RuneSelf && !isAlnum(byte(c)) {
		return c, t, true
	}
	return 0, 0, false
}

// hexEscape decodes the `\x` escape whose digits start at s[t]: `\x{…}` with
// at least one hex digit and a value no larger than the last rune, or exactly
// two hex digits.
func hexEscape(s string, t int) (rune, int, bool) {
	if t >= len(s) {
		return 0, 0, false
	}
	if s[t] != '{' {
		if len(s)-t < 2 || unhex(s[t]) < 0 || unhex(s[t+1]) < 0 {
			return 0, 0, false
		}
		return unhex(s[t])*16 + unhex(s[t+1]), t + 2, true
	}
	var r rune
	digits := 0
	for t++; t < len(s) && s[t] != '}'; t++ {
		v := unhex(s[t])
		if v < 0 {
			return 0, 0, false
		}
		if r = r*16 + v; r > utf8.MaxRune {
			return 0, 0, false
		}
		digits++
	}
	if t >= len(s) || digits == 0 {
		return 0, 0, false
	}
	return r, t + 1, true
}

func unhex(c byte) rune {
	switch {
	case c >= '0' && c <= '9':
		return rune(c - '0')
	case c >= 'a' && c <= 'f':
		return rune(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return rune(c-'A') + 10
	}
	return -1
}

func isAlnum(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// progCost is the size of the program re compiles to, by the arithmetic of
// regexp/syntax's own size check (calcSize): repetition MULTIPLIES its
// operand's size rather than being expanded, so the walk is linear in the tree
// however large the program. It saturates just past limit so no product can
// overflow. Every character class's range count is added to ranges — once per
// class NODE, since the compiled copies of a repeated class share its ranges.
func progCost(re *syntax.Regexp, limit int, ranges *int) int {
	sat := func(n int) int { return min(n, limit+1) }
	var size int
	switch re.Op {
	case syntax.OpLiteral:
		size = len(re.Rune)
	case syntax.OpCharClass:
		*ranges = sat(*ranges + len(re.Rune)/2)
		size = 1
	case syntax.OpCapture, syntax.OpStar:
		size = 2 + progCost(re.Sub[0], limit, ranges)
	case syntax.OpPlus, syntax.OpQuest:
		size = 1 + progCost(re.Sub[0], limit, ranges)
	case syntax.OpConcat, syntax.OpAlternate:
		for _, sub := range re.Sub {
			size = sat(size + progCost(sub, limit, ranges))
		}
		if re.Op == syntax.OpAlternate && len(re.Sub) > 1 {
			size += len(re.Sub) - 1
		}
	case syntax.OpRepeat:
		sub := progCost(re.Sub[0], limit, ranges)
		switch {
		case re.Max == -1 && re.Min == 0:
			size = 2 + sub
		case re.Max == -1:
			size = 1 + re.Min*sub
		default:
			size = re.Max*sub + re.Max - re.Min
		}
	}
	return sat(max(1, size))
}
