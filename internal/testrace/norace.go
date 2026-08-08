//go:build !race

// Package testrace reports whether the enclosing binary was built with the
// race detector. It is imported ONLY from _test.go files: the detector
// changes escape analysis and adds bookkeeping allocations, so
// testing.AllocsPerRun ceilings are meaningless under it — allocation-budget
// tests skip when Enabled rather than being widened to a number that would
// let a real regression through.
package testrace

// Enabled reports whether this binary was built with -race.
const Enabled = false
