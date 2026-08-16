package transform

import "testing"

// int() of a long string is one uninterruptible, quadratic Starlark step (Go's
// big.Int decimal parse is O(len²) and runs inside a single step, so neither
// the step limit nor the wall clock can interrupt it), so it must be refused
// before the parse. A short string still works. Regression for boundedInt /
// maxIntStringLen.
func TestBoundedIntRejectsLongString(t *testing.T) {
	if err := runBody(t, `x = int("123456789")`); err != nil {
		t.Fatalf("int of a short numeric string errored: %v", err)
	}
	// "9" * 100000 builds a 100k-char string (within the string cap); int() of
	// it must be refused as over the character limit.
	err := runBody(t, `x = int("9" * 100000)`)
	mustContain(t, err, "character")
}
