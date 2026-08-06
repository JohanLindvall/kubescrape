//go:build race

package server

// raceEnabled reports whether this binary was built with -race. The race
// detector changes escape analysis and adds bookkeeping allocations, so
// testing.AllocsPerRun ceilings are meaningless under it — the budget test
// skips rather than being widened to a number that would let a real regression
// through.
const raceEnabled = true
