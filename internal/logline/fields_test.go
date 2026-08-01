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

// Extracted values live in a slot slice parallel to the KeyIndex's keys, not a
// map keyed by the line's own bytes (which cost an allocation per key per
// line). The slots are per-Fields state reused across lines, so a key present
// on one line must not survive into the next, and a key no rule registered must
// not resolve at all.
func TestFieldsSlotsResetBetweenLines(t *testing.T) {
	ki := NewKeyIndex()
	ki.Add("a")
	ki.Add("b")
	var f Fields

	f.Reset(`a=1 b=2`)
	if got := ki.Get(&f, "a"); got != "1" {
		t.Fatalf("a = %q, want 1", got)
	}
	if got := ki.Get(&f, "b"); got != "2" {
		t.Fatalf("b = %q, want 2", got)
	}

	// b is absent from the second line: its slot must be cleared, not stale.
	f.Reset(`a=9`)
	if got := ki.Get(&f, "a"); got != "9" {
		t.Errorf("a = %q, want 9", got)
	}
	if got := ki.Get(&f, "b"); got != "" {
		t.Errorf("b = %q; the previous line's value leaked through the slot", got)
	}

	// Same across a format switch, where a different arm of Parse fills them.
	f.Reset(`{"a":"json"}`)
	if got := ki.Get(&f, "a"); got != "json" {
		t.Errorf("a = %q, want json", got)
	}
	if got := ki.Get(&f, "b"); got != "" {
		t.Errorf("b = %q, want empty", got)
	}

	// A key nothing registered resolves empty rather than indexing a slot.
	if got := ki.Get(&f, "never-registered"); got != "" {
		t.Errorf("unregistered key = %q, want empty", got)
	}
}

// An empty logfmt value must read as empty and must not trip the zero-length
// aliasing of the line.
func TestEmptyLogfmtValue(t *testing.T) {
	ki := NewKeyIndex()
	ki.Add("a")
	ki.Add("b")
	var f Fields
	f.Reset(`a= b=x`)
	if got := ki.Get(&f, "a"); got != "" {
		t.Errorf("a = %q, want empty", got)
	}
	if got := ki.Get(&f, "b"); got != "x" {
		t.Errorf("b = %q, want x", got)
	}
}

// Values now alias the line rather than copying it. A value read out before the
// Fields is reused must keep reading the line it came from — Go strings are
// immutable and the header pins the old backing array, so the reuse cannot
// rewrite it under the caller.
func TestLogfmtValueSurvivesReset(t *testing.T) {
	ki := NewKeyIndex()
	ki.Add("msg")
	var f Fields
	f.Reset(`msg=hello`)
	got := ki.Get(&f, "msg")
	f.Reset(`msg=goodbye`)
	if got != "hello" {
		t.Errorf("retained value = %q, want hello", got)
	}
	if now := ki.Get(&f, "msg"); now != "goodbye" {
		t.Errorf("msg = %q, want goodbye", now)
	}
}
