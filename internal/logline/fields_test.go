package logline

import (
	"strconv"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

func TestRawScalarString(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

// A JSON number renders the same string here as it does through the record and
// lifted-attribute paths (pcommon's ES6 rendering, which logattrs.FloatString
// replicates). Fixed-point here meant a line field read "0.0000005" where an
// attribute lifted from the very same key read "5e-7": adding or removing an
// unrelated logAttributes rule silently renamed every log-metric series
// labelled by it, and a selector written for one spelling stopped matching.
func TestRawScalarStringMatchesAttributeRendering(t *testing.T) {
	t.Parallel()
	for _, tok := range []string{"5e-7", "0.0000005", "1e21", "2.5e22", "1e-6", "1.5e-7", "-3e21", "42.5"} {
		got, ok := RawScalarString([]byte(tok))
		if !ok {
			t.Fatalf("RawScalarString(%q) rejected a number", tok)
		}
		f, err := strconv.ParseFloat(tok, 64)
		if err != nil {
			t.Fatal(err)
		}
		if want := pcommon.NewValueDouble(f).AsString(); got != want {
			t.Errorf("RawScalarString(%q) = %q, but the same value on a record reads %q", tok, got, want)
		}
		if want := logattrs.FloatString(f); got != want {
			t.Errorf("RawScalarString(%q) = %q, logattrs.FloatString = %q", tok, got, want)
		}
	}
}

// A BARE logfmt key yields the sentinel "true", which is prose, not a field:
// `weight=10 disk error` must not resolve error="true" and fire a selector
// written as `error=true`. It cannot be admitted even in principle, because
// whether the words are scanned at all depends on an unrelated '=' elsewhere on
// the line (the no-'=' fast path), so the identical sentence would resolve two
// ways depending on its neighbours.
func TestBareLogfmtKeysResolveToNothing(t *testing.T) {
	t.Parallel()
	ki := NewKeyIndex()
	ki.Add("error")
	ki.Add("disk")
	ki.Add("weight")
	for _, line := range []string{`weight=10 disk error`, `disk error`} {
		var f Fields
		f.Reset(line)
		for _, key := range []string{"error", "disk"} {
			if got := ki.Get(&f, key); got != "" {
				t.Errorf("line %q: %s = %q, want nothing (a bare word is not a field)", line, key, got)
			}
		}
	}
	var f Fields
	f.Reset(`weight=10 disk error`)
	if got := ki.Get(&f, "weight"); got != "10" {
		t.Errorf("weight = %q, want 10 — real pairs on the same line still resolve", got)
	}
}

// Duplicate keys resolve FIRST-wins in JSON (lightning's GetPaths contract) and
// LAST-wins in logfmt (the scan overwrites the slot). Both inputs are
// malformed and neither reader dictates an answer, so the asymmetry is
// documented rather than papered over — this pins what the documentation says,
// here and in the twin extractor in pkg/logattrs.
func TestDuplicateKeyResolutionIsAsDocumented(t *testing.T) {
	t.Parallel()
	ki := NewKeyIndex()
	ki.Add("level")
	for line, want := range map[string]string{
		`{"level":"info","level":"warn"}`: "info",
		`level=info level=warn`:           "warn",
	} {
		var f Fields
		f.Reset(line)
		if got := ki.Get(&f, "level"); got != want {
			t.Errorf("line %q: level = %q, want %q", line, got, want)
		}
	}
}
