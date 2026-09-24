package kubemeta

import (
	"errors"
	"fmt"
	"regexp"
	"regexp/syntax"
	"runtime"
	"strings"
	"testing"
	"unicode"
)

// The three shapes whose cost is not proportional to their text, each inside
// an 8 KiB byte ceiling. Measured on the unbounded compile: ~350 MB allocated
// and ~52 MB retained (counted repetition), ~2.5 s to PARSE (case-folded
// ranges), ~80 MB and ~250 ms to parse (merged Unicode tables). The bound must
// refuse every one of them without paying for it — the allocation assertion is
// what shows the refusal came from measuring rather than from compiling.
//
// Reverse-patch check: with RelabelRegexCost reduced to a plain
// regexp.Compile, every case fails both assertions.
func TestRelabelRegexCostRefusesAmplifyingShapesWithoutCompilingThem(t *testing.T) {
	cases := map[string]string{
		"counted repetition": strings.Repeat("a{1000}", 1170),
		"case-folded ranges": "(?i)" + strings.Repeat(`[B-\x{1E942}]`, 500),
		"merged unicode":     "[" + strings.Repeat(`\pL`, 2700) + "]",
		"unicode classes":    strings.Repeat(`\pL`, 2700),
		"repeated literal":   "(?:abcdefghijklmnopq){1000}",
	}
	for name, regex := range cases {
		t.Run(name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			_, err := RelabelRegexCost(regex)
			runtime.ReadMemStats(&after)
			if !errors.Is(err, ErrRelabelRegexTooLarge) {
				t.Fatalf("RelabelRegexCost error = %v, want ErrRelabelRegexTooLarge", err)
			}
			if got := after.TotalAlloc - before.TotalAlloc; got > 4<<20 {
				t.Errorf("measuring the regex allocated %d bytes: it was compiled or fully parsed to be refused", got)
			}
			if _, err := CompileRelabelRegex(regex); !errors.Is(err, ErrRelabelRegexTooLarge) {
				t.Errorf("CompileRelabelRegex error = %v: the agent's compile must refuse what the parse door refuses", err)
			}
		})
	}
}

// The legitimately-large shape — one keep allowlist with a long alternation,
// filling the whole 8 KiB byte budget — and the ordinary ones must all fit.
func TestRelabelRegexCostAdmitsOrdinaryAndAllowlistRegexes(t *testing.T) {
	var names []string
	for i := 0; ; i++ {
		n := fmt.Sprintf("app_%d_http_request_duration_seconds_bucket", i)
		if len(strings.Join(append(names, n), "|")) > 8<<10 {
			break
		}
		names = append(names, n)
	}
	allow := strings.Join(names, "|")
	for _, regex := range []string{
		"", ".*", "(.*)_total", "[a-z0-9_]+", `\p{L}+`, `[\p{L}\p{N}_]+`, "(?i)[a-z]+", "(?i)(get|post)",
		"[0-9a-f]{64}", ".{1,1000}", "(?i)[\\x{0}-\\x{10FFFF}]", allow, "(?i)" + allow[:len(allow)-8],
	} {
		cost, err := RelabelRegexCost(regex)
		if err != nil {
			t.Errorf("RelabelRegexCost(%.40q) = %d, %v: an ordinary regex was refused", regex, cost, err)
		}
	}
}

// The parse door validates with RelabelRegexCost and the agent compiles with
// CompileRelabelRegex; they must agree on what is a regex at all, including the
// escapes, classes and edge cases the cost scan tokenises itself.
func TestRelabelRegexCostAgreesWithCompile(t *testing.T) {
	for _, regex := range []string{
		"a)|(b", "(", "[", "]", `\`, `\q`, `\_`, `\1`, `\8`, `\01`, `\0`, `\101`, `[\1]`, `[\01-\07]`,
		`\x41`, `\x4`, `\xZZ`, `\x{}`, `\x{41}`, `\x{110000}`, `\x{10FFFF}`, `[\x{41}-\x{5A}]`, `[z-a]`,
		`[a-]`, `[-a]`, `[]a]`, `[^]a]`, `[^]`, `[]`, `[a-\d]`, `[\d-z]`, `[[:alpha:]]`, `[[:foo:]]`, `[[:alpha]`,
		`\pL`, `\p{Greek}`, `\p{Foo}`, `\p`, `[\p`, `\p{L`, `[\pL-z]`, `\PL`, `\p{^L}`, `\QA[B\E`, `\Q[`,
		`(?i)[a-z]`, `(?i:x)`, `(?P<n>x)`, `(?<n>x)`, `(?z)`, "\xff", "[\xff]", `[\b]`, `[\Q]`, `a{1001}`,
		`(?i)[\x{100}-\x{17F}]`, `[\a\f\n\r\t\v]`, `[\z]`, `[\-]`, `[a\-z]`,
	} {
		_, costErr := RelabelRegexCost(regex)
		_, compileErr := regexp.Compile(anchoredRelabelRegex(regex))
		if (costErr == nil) != (compileErr == nil) {
			t.Errorf("%q: RelabelRegexCost error %v, regexp.Compile error %v", regex, costErr, compileErr)
		}
		if cost, complete := parseCost(anchoredRelabelRegex(regex), MaxRelabelRegexCost); compileErr == nil && !complete {
			t.Errorf("%q: the cost scan stopped short (cost %d) on a regex the parser accepts", regex, cost)
		}
	}
}

// Under (?i) the parser folds every rune of a class range one at a time, so the
// charge is the range's width — decoded from whatever escape spells its ends —
// and the parser's own two shortcuts cost nothing.
func TestFoldedClassRangeIsChargedByItsWidth(t *testing.T) {
	cases := []struct {
		regex string
		over  bool
	}{
		{`(?i)[B-\x{1E942}]`, true},
		{"(?i)[B-\U0001E942]", true},
		{`(?i)[\101-\x{9FFF}]`, true},
		{`(?i)[\x{0}-\x{10FFFF}]`, false}, // covers every foldable rune: one step
		{`(?i)[\x{1F000}-\x{10FFFF}]`, false},
		{`[B-\x{1E942}]`, false},     // no folding, no per-rune work
		{`(?-i)[B-\x{1E942}]`, true}, // conservative: any flag group naming i
	}
	for _, c := range cases {
		_, err := RelabelRegexCost(c.regex)
		if got := errors.Is(err, ErrRelabelRegexTooLarge); got != c.over {
			t.Errorf("%q: refused as too large = %v, want %v (err %v)", c.regex, got, c.over, err)
		}
	}
}

// unicodeClassCharge must cover the ranges the parser appends for any \p name
// it accepts — the table plus its fold table — or a class of many of them is
// under-charged.
func TestUnicodeClassChargeCoversEveryTable(t *testing.T) {
	ranges := func(tab *unicode.RangeTable) int {
		if tab == nil {
			return 0
		}
		// appendTable adds a stride-1 range whole and every rune of a wider
		// stride on its own.
		n := 0
		add := func(lo, hi, stride uint32) {
			if stride == 1 {
				n++
			} else {
				n += int((hi-lo)/stride) + 1
			}
		}
		for _, r := range tab.R16 {
			add(uint32(r.Lo), uint32(r.Hi), uint32(r.Stride))
		}
		for _, r := range tab.R32 {
			add(r.Lo, r.Hi, r.Stride)
		}
		return n
	}
	check := func(kind, name string, tab, fold *unicode.RangeTable) {
		if n := ranges(tab) + ranges(fold); n > unicodeClassCharge {
			t.Errorf("%s %s appends %d ranges, over unicodeClassCharge %d", kind, name, n, unicodeClassCharge)
		}
	}
	for name, tab := range unicode.Categories {
		check("category", name, tab, unicode.FoldCategory[name])
	}
	for name, tab := range unicode.Scripts {
		check("script", name, tab, unicode.FoldScript[name])
	}
}

// progCost upper-bounds the program the compiler builds, the same way the
// compiler's own size check does.
func TestProgCostBoundsTheCompiledProgram(t *testing.T) {
	for _, regex := range []string{
		"a{2,5}", "a{2,}", "a{0,}", "a{0}", "(a|b)*c+d?", "(?i)abc", "(a*)*", "[a-z]{3}(?:x|yz){1,4}", "^$",
		strings.Repeat("a{100}", 10), "(?:)", "a|b|c|dd",
	} {
		cost, err := RelabelRegexCost(regex)
		if err != nil {
			t.Fatalf("%q: %v", regex, err)
		}
		if n := compiledInsts(t, regex); n > cost+2 {
			t.Errorf("%q: compiles to %d instructions, cost %d", regex, n, cost)
		}
	}
}

// compiledInsts is the instruction count of the program regexp.Compile builds
// for regex: its own tree plus the Fail and Match instructions every program
// carries.
func compiledInsts(t testing.TB, regex string) int {
	re, err := syntax.Parse(anchoredRelabelRegex(regex), syntax.Perl)
	if err != nil {
		t.Fatal(err)
	}
	prog, err := syntax.Compile(re.Simplify())
	if err != nil {
		t.Fatal(err)
	}
	return len(prog.Inst)
}
