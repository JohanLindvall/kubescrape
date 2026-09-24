package regexcost

import (
	"regexp/syntax"
	"strings"
	"testing"
)

func parse(t *testing.T, pat string) *syntax.Regexp {
	t.Helper()
	re, err := syntax.Parse(pat, syntax.Perl) // the flags regexp.Compile parses with
	if err != nil {
		t.Fatal(err)
	}
	return re
}

// realInsts is what regexp.Compile actually builds for pat.
func realInsts(t *testing.T, pat string) int {
	t.Helper()
	prog, err := syntax.Compile(parse(t, pat).Simplify())
	if err != nil {
		t.Fatal(err)
	}
	return len(prog.Inst)
}

// The estimate is regexp/syntax's own arithmetic, so it tracks the real
// program — the property both callers' ceilings rest on. A human-written
// pattern and every multiplier shape land within a few instructions of it.
func TestInstsTracksTheRealProgram(t *testing.T) {
	const limit = 1 << 20
	for _, pat := range []string{
		`literal`,
		`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`,
		`^(\S+) (\S+) (\S+) \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+) "([^"]*)" "([^"]*)"$`,
		`.{0,1000}`,
		`\w{1000}`,
		`(?:abcdefghijklmnopqrst){1000}`,
		strings.Repeat(`\w{1000}`, 15),
		`a+b*c?d{3,}e{2,5}`,
		`x{999}|y{999}|z{999}`,
	} {
		est, real := Insts(parse(t, pat), limit), realInsts(t, pat)
		// The estimate may run slightly over (it assumes the pessimistic star)
		// or under by the fixed instructions every program carries; never by
		// a factor.
		if diff := est - real; diff < -4 || diff > real/50+4 {
			t.Errorf("%q: estimated %d instructions, the compiled program has %d", pat, est, real)
		}
	}
}

// Past the limit the estimate saturates at limit+1 — whatever the shape that
// crosses it: a concatenation, an alternation, or a counted repeat of a
// subexpression that is itself already past the limit.
func TestInstsSaturatesAtLimitPlusOne(t *testing.T) {
	var alt []string
	for c := 'a'; c <= 't'; c++ {
		alt = append(alt, string(c)+"{999}") // distinct, or the parser factors them
	}
	for _, tc := range []struct {
		pat   string
		limit int
	}{
		{strings.Repeat(`\w{1000}`, 1000), 1 << 14},
		{strings.Join(alt, "|"), 1 << 13},
		{`(?:` + strings.Repeat("ab", 4500) + `){2}`, 1 << 13}, // regexp caps nested counts, not a repeated literal
	} {
		if got := Insts(parse(t, tc.pat), tc.limit); got != tc.limit+1 {
			t.Errorf("%.40q: Insts = %d, want the saturation value %d", tc.pat, got, tc.limit+1)
		}
	}
	// And a pattern under the limit is not saturated.
	if got := Insts(parse(t, `abc`), 8); got != 3 {
		t.Errorf("Insts(abc) = %d, want 3", got)
	}
}
