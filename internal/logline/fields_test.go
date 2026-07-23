package logline

import "testing"

func TestRawScalarString(t *testing.T) {
	// The raw-token renderer must match what DecodeAny + a type switch
	// produced: strings unescaped, bools as literals, numbers round-tripped
	// through float64; objects/arrays/null/malformed rejected.
	cases := []struct {
		raw  string
		want string
		ok   bool
	}{
		{`"plain"`, "plain", true},
		{`"esc\"aped\n"`, "esc\"aped\n", true},
		{`"unicode é"`, "unicode é", true},
		{`true`, "true", true},
		{`false`, "false", true},
		{`42`, "42", true},
		{`42.50`, "42.5", true}, // float64 round-trip, as before
		{`-0.125`, "-0.125", true},
		{`1e3`, "1000", true},
		{`null`, "", false},
		{`{"a":1}`, "", false},
		{`[1,2]`, "", false},
		{`"unterminated`, "", false},
		{`truthy`, "", false},
		{`falsey`, "", false},
		{`not-a-number`, "", false},
	}
	for _, c := range cases {
		got, ok := RawScalarString([]byte(c.raw))
		if ok != c.ok || got != c.want {
			t.Errorf("RawScalarString(%q) = %q,%v want %q,%v", c.raw, got, ok, c.want, c.ok)
		}
	}
}
