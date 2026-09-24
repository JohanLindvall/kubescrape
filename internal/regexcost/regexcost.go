// Package regexcost estimates what a regular expression compiles INTO, from its
// parse tree and before anything is compiled.
//
// A pattern's length is not its cost: a counted repeat multiplies what it
// repeats, so a few kilobytes of `x{999}|...` is an 800k-instruction program
// whose compile alone takes hundreds of milliseconds and whose retained program
// is tens of megabytes. Every place that compiles a pattern it did not write — a
// template composing one from a label value (internal/agent/attrs), a Starlark
// script building one from a record (internal/agent/transform) — must refuse
// such a pattern from the parse, since compiling it to find out IS the cost.
// There were two copies of this arithmetic, one per caller; the refusal
// policies (the error each returns, the other bounds each applies, the caches
// behind them) stay the callers' own.
package regexcost

import "regexp/syntax"

// Insts estimates the instruction count of re's compiled program. It is
// regexp/syntax's own size arithmetic — the calcSize behind its ErrLarge
// ceiling — case for case, so the estimate tracks the real program closely (it
// is exact up to the few instructions every program carries and the rewrites
// Simplify makes). It reads the tree and expands nothing, so refusing a pattern
// costs its parse, not its compile.
//
// Every result SATURATES at limit+1, which is all a caller comparing against
// limit needs and is what keeps the products small: a sub-result is at most
// limit+1 and syntax caps a repeat count at 1000, so no intermediate exceeds
// 1000*(limit+1). limit must therefore be far below math.MaxInt/1000; the
// callers' ceilings are in the thousands.
func Insts(re *syntax.Regexp, limit int) int {
	var size int
	switch re.Op {
	case syntax.OpLiteral:
		size = len(re.Rune)
	case syntax.OpCapture, syntax.OpStar:
		size = 2 + Insts(re.Sub[0], limit)
	case syntax.OpPlus, syntax.OpQuest:
		size = 1 + Insts(re.Sub[0], limit)
	case syntax.OpConcat, syntax.OpAlternate:
		for _, sub := range re.Sub {
			if size += Insts(sub, limit); size > limit {
				return limit + 1
			}
		}
		if re.Op == syntax.OpAlternate && len(re.Sub) > 1 {
			size += len(re.Sub) - 1
		}
	case syntax.OpRepeat:
		sub := Insts(re.Sub[0], limit)
		if sub > limit {
			return limit + 1
		}
		switch {
		case re.Max == -1 && re.Min == 0:
			size = 2 + sub // x*
		case re.Max == -1:
			size = 1 + re.Min*sub // xxx+
		default:
			size = re.Max*sub + (re.Max - re.Min) // x{2,5} = xx(x(x(x)?)?)?
		}
	}
	return min(max(1, size), limit+1)
}
