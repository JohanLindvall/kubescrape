package tailer

import (
	"regexp/syntax"
	"strings"

	"github.com/JohanLindvall/multiline"
	"github.com/JohanLindvall/multiline/patterns"
)

// The trace-joining matcher dominates per-line CPU when used as-is: in steady
// state (no group in progress) every log line is tested against every
// start-state pattern, several of which are unanchored and scan the whole
// line ("\bpanic: ", java's ".(Exception|Error|...):"). traceMatcher wraps
// the compiled state machine with a literal prefilter: a set of substrings
// derived from the start patterns via regexp/syntax such that ANY line
// matching ANY start pattern must contain at least one of them. Lines
// containing none (practically all lines) skip the regexes entirely —
// strings.Contains is SIMD-accelerated and ~two orders of magnitude cheaper.
//
// The literals are computed from the actual patterns at init, so a pattern
// change in the multiline package cannot silently break the implication: if
// no literal set can be proven for every start transition, the prefilter is
// disabled and the unwrapped matcher is used. Correctness is additionally
// covered by a differential test (TestPrefilterMatchesUnfiltered).
var traceMatcher = compileTraceMatcher()

func compileTraceMatcher() multiline.Matcher {
	inner := patterns.MustCompile(patterns.All...)
	lits, ok := startLiterals(patterns.All)
	if !ok {
		return inner
	}
	return &prefilteredMatcher{inner: inner, literals: lits}
}

type prefilteredMatcher struct {
	inner    multiline.Matcher
	literals []string
}

// Step skips the start-state regexes for lines that cannot possibly begin a
// group. Any other active set (a group in progress) runs the full matcher —
// continuation patterns are anchored and cheap, and groups are rare.
func (m *prefilteredMatcher) Step(line string, active []int) ([]int, int) {
	if len(active) == 1 && active[0] == 0 && !m.maybeStart(line) {
		return nil, -1
	}
	return m.inner.Step(line, active)
}

func (m *prefilteredMatcher) Format(index int) string { return m.inner.Format(index) }

func (m *prefilteredMatcher) maybeStart(line string) bool {
	for _, lit := range m.literals {
		if strings.Contains(line, lit) {
			return true
		}
	}
	return false
}

// startLiterals derives the prefilter literal set from every start-state
// transition of the given pattern sets. ok is false when any pattern's
// required literals cannot be proven (the prefilter must then be disabled).
func startLiterals(sets []patterns.StateSet) ([]string, bool) {
	var lits []string
	for _, set := range sets {
		for _, st := range set.States {
			if st.Name != patterns.StartState {
				continue
			}
			for _, tr := range st.Transitions {
				ls, ok := requiredLiterals(tr.Pattern)
				if !ok {
					return nil, false
				}
				lits = append(lits, ls...)
			}
		}
	}
	return dedupeLiterals(lits), len(lits) > 0
}

// requiredLiterals returns strings such that every match of pattern contains
// at least one of them.
func requiredLiterals(pattern string) ([]string, bool) {
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return nil, false
	}
	return literalsOf(re.Simplify())
}

// literalsOf walks the parse tree. A concat needs any single child's literal
// set — but contiguous runs of EXACT children are product-expanded first
// ("E"+"(xception|rror)"+":" yields {"Exception:","Error:"} rather than the
// weak factored suffix {"xception","rror"}), which is what keeps ordinary
// lowercase "error ..." log lines from reaching the regexes at all. An
// alternation needs the union across every branch (a branch with no provable
// literal poisons the whole set).
func literalsOf(re *syntax.Regexp) ([]string, bool) {
	switch re.Op {
	case syntax.OpLiteral:
		if re.Flags&syntax.FoldCase != 0 || len(re.Rune) < 3 {
			// Case-folded or too short to be a useful (selective) probe.
			return nil, false
		}
		return []string{string(re.Rune)}, true
	case syntax.OpConcat:
		var best []string
		found := false
		consider := func(ls []string) {
			if len(ls) == 0 || minLen(ls) < 3 {
				return
			}
			if !found || moreSelective(ls, best) {
				best, found = ls, true
			}
		}
		// Product-expand each maximal contiguous run of exact children: a
		// match contains those children's matches concatenated, so the
		// product strings are required substrings.
		i := 0
		for i < len(re.Sub) {
			acc, j := []string{""}, i
			for j < len(re.Sub) {
				ls, ok := exactSet(re.Sub[j])
				if !ok {
					break
				}
				if acc = product(acc, ls); acc == nil {
					break // capped out; fall back to per-child literals
				}
				j++
			}
			if j > i && acc != nil {
				consider(acc)
			}
			if j == i {
				j++
			}
			i = j
		}
		// Fallback: each child's own literal set.
		for _, sub := range re.Sub {
			if ls, ok := literalsOf(sub); ok {
				consider(ls)
			}
		}
		return best, found
	case syntax.OpAlternate:
		var all []string
		for _, sub := range re.Sub {
			ls, ok := literalsOf(sub)
			if !ok {
				return nil, false
			}
			all = append(all, ls...)
		}
		return all, true
	case syntax.OpCapture, syntax.OpPlus:
		return literalsOf(re.Sub[0])
	default:
		// Quantifiers with a zero minimum, classes, anchors, etc. guarantee
		// nothing.
		return nil, false
	}
}

// exactSet returns the finite set of strings a node can match exactly (ok
// false when the node is not a small finite case-sensitive language).
func exactSet(re *syntax.Regexp) ([]string, bool) {
	switch re.Op {
	case syntax.OpLiteral:
		if re.Flags&syntax.FoldCase != 0 {
			return nil, false
		}
		return []string{string(re.Rune)}, true
	case syntax.OpEmptyMatch:
		return []string{""}, true
	case syntax.OpCapture:
		return exactSet(re.Sub[0])
	case syntax.OpAlternate:
		var all []string
		for _, sub := range re.Sub {
			ls, ok := exactSet(sub)
			if !ok {
				return nil, false
			}
			all = append(all, ls...)
			if len(all) > maxProductSet {
				return nil, false
			}
		}
		return all, true
	case syntax.OpConcat:
		acc := []string{""}
		for _, sub := range re.Sub {
			ls, ok := exactSet(sub)
			if !ok {
				return nil, false
			}
			if acc = product(acc, ls); acc == nil {
				return nil, false
			}
		}
		return acc, true
	default:
		return nil, false
	}
}

// Caps keep the product expansion tiny (the bundled patterns need a handful).
const (
	maxProductSet = 16
	maxProductLen = 64
)

// product cross-concatenates two string sets; nil when a cap is exceeded.
func product(a, b []string) []string {
	if len(a)*len(b) > maxProductSet {
		return nil
	}
	out := make([]string, 0, len(a)*len(b))
	for _, x := range a {
		for _, y := range b {
			if len(x)+len(y) > maxProductLen {
				return nil
			}
			out = append(out, x+y)
		}
	}
	return out
}

func minLen(ls []string) int {
	m := 1 << 30
	for _, l := range ls {
		m = min(m, len(l))
	}
	return m
}

// moreSelective prefers the literal set whose weakest (shortest) member is
// longest, then the one with fewer alternatives.
func moreSelective(a, b []string) bool {
	if la, lb := minLen(a), minLen(b); la != lb {
		return la > lb
	}
	return len(a) < len(b)
}

// dedupeLiterals removes duplicates and literals that contain another literal
// (if the shorter one is present the longer check is redundant).
func dedupeLiterals(lits []string) []string {
	var out []string
	for _, l := range lits {
		redundant := false
		for _, o := range lits {
			if o != l && strings.Contains(l, o) {
				redundant = true // a strictly-contained literal covers l
				break
			}
			if o == l && containsBefore(out, l) {
				redundant = true
				break
			}
		}
		if !redundant {
			out = append(out, l)
		}
	}
	return out
}

func containsBefore(out []string, l string) bool {
	for _, o := range out {
		if o == l {
			return true
		}
	}
	return false
}
