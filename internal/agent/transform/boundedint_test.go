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

// The builtin is int(x, base=10), so a guard that reads args[0] alone closes
// nothing for `int(x = "…")` — the keyword form spent 3.0s in ONE
// uninterruptible step, past the wall-clock budget, and returned no error.
func TestBoundedIntRejectsLongStringViaKeyword(t *testing.T) {
	err := runBody(t, `x = int(x = "9" * 100000)`)
	mustContain(t, err, "character")
}

// The guard must not break the legitimate forms.
func TestBoundedIntKeepsOrdinaryConversions(t *testing.T) {
	for _, body := range []string{
		`x = int("ff", 16)`,
		`x = int(3.7)`,
		`x = int(True)`,
		`x = int("123")`,
		`x = int(x = "123")`,
		`x = int("7f", base = 16)`,
	} {
		if err := runBody(t, body); err != nil {
			t.Errorf("%s errored: %v", body, err)
		}
	}
}
