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

// Logfmt values must decode their escapes like the JSON path does: the same
// logical value must not match selectors or mint label values differently
// depending on the line format (raw `a \"b\" c` vs decoded `a "b" c`).
func TestLogfmtValuesUnescaped(t *testing.T) {
	ki := NewKeyIndex()
	ki.Add("msg")
	ki.Add("plain")
	var f Fields
	f.Reset(`msg="a \"b\" c" plain=ok`)
	if got := ki.Get(&f, "msg"); got != `a "b" c` {
		t.Fatalf("msg = %q, want unescaped", got)
	}
	if got := ki.Get(&f, "plain"); got != "ok" {
		t.Fatalf("plain = %q", got)
	}
}

// UNQUOTED values hold no escapes — go-kit/logrus/slog quote only for a
// space, quote or '='. Unescaping them anyway deleted the backslashes of a
// Windows path or a regex, and for a recognised letter minted a label value
// with a real control character in it.
func TestUnquotedLogfmtValuesAreVerbatim(t *testing.T) {
	ki := NewKeyIndex()
	ki.Add("path")
	ki.Add("re")
	ki.Add("arg")
	var f Fields
	f.Reset(`path=C:\logs\app.log re=\d+\s+ok arg=a\nb`)
	for key, want := range map[string]string{
		"path": `C:\logs\app.log`,
		"re":   `\d+\s+ok`,
		"arg":  `a\nb`,
	} {
		if got := ki.Get(&f, key); got != want {
			t.Errorf("%s = %q; want %q verbatim", key, got, want)
		}
	}
}

// A 64-bit id used as a logMetrics label must not lose precision: float64
// cannot hold one, so adjacent ids collapsed into a single series while the
// record attribute lifted from the same field stayed exact.
func TestIntegerFieldsKeepFullPrecision(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`9007199254740993`, `9007199254740993`}, // 2^53+1: unrepresentable as float64
		{`1234567890123456789`, `1234567890123456789`},
		{`-42`, `-42`},
		{`3.5`, `3.5`},  // fractions still go through the float path
		{`1e3`, `1000`}, // as do exponents
	} {
		got, ok := RawScalarString([]byte(tc.in))
		if !ok || got != tc.want {
			t.Errorf("RawScalarString(%s) = %q (ok=%v), want %q", tc.in, got, ok, tc.want)
		}
	}
}
