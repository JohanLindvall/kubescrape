package logline

import "testing"

func TestParseSelectors(t *testing.T) {
	t.Parallel()
	lookup := func(m map[string]string) func(string) string {
		return func(k string) string { return m[k] }
	}
	set, err := ParseSelectors(
		[]string{"level=error", "env!=dev"},
		[]string{"msg=timeout"},
	)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		vals map[string]string
		want bool
	}{
		{"all match", map[string]string{"level": "error", "env": "prod", "msg": "read timeout"}, true},
		{"exact miss", map[string]string{"level": "info", "env": "prod", "msg": "read timeout"}, false},
		{"negation excludes", map[string]string{"level": "error", "env": "dev", "msg": "timeout"}, false},
		{"regex miss", map[string]string{"level": "error", "env": "prod", "msg": "ok"}, false},
	}
	for _, c := range cases {
		var ctx MatchContext
		if got := set.Match(lookup(c.vals), &ctx); got != c.want {
			t.Errorf("%s: match = %v, want %v", c.name, got, c.want)
		}
	}

	if _, err := ParseSelectors([]string{"bogus"}, nil); err == nil {
		t.Error("selector without operator: want error")
	}
}

func TestEmptySelectorsMatchAll(t *testing.T) {
	t.Parallel()
	set, err := ParseSelectors(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var ctx MatchContext
	if !set.Match(func(string) string { return "" }, &ctx) {
		t.Error("empty selector set should match everything")
	}
}

// A selector grammar too lenient compiled into silent misbehavior: "=" (empty
// label) resolved every lookup to "" and matched EVERY line — one such drop
// rule silently discarded a node's whole log stream, defeating the deliberate
// empty-match refusal in NewLineFilter — "!" compiled into a dead
// never-matching rule, and "a!b" read as a != "b" instead of erroring.
func TestParseSelectorRejectsMalformedGrammar(t *testing.T) {
	t.Parallel()
	for _, in := range []string{"=", "!", "=x", "!=x", "a!b", "!x=y"} {
		if _, err := ParseSelectors([]string{in}, nil); err == nil {
			t.Errorf("exact selector %q: want error", in)
		}
		if _, err := ParseSelectors(nil, []string{in}); err == nil {
			t.Errorf("regex selector %q: want error", in)
		}
	}
	// Still legal: an empty VALUE ("label=" matches an absent/empty label).
	if _, err := ParseSelectors([]string{"level="}, nil); err != nil {
		t.Errorf("empty value: %v", err)
	}
}

// Regex selector patterns are RE2 passed VERBATIM. The old unescape layer
// rewrote `C:\\data` — the standard spelling for the literal `C:\data` — into
// `C:\data`, where \d is a digit class: the rule missed `C:\data` and matched
// `C:5ata` instead.
func TestRegexSelectorsAreVerbatim(t *testing.T) {
	t.Parallel()
	set, err := ParseSelectors(nil, []string{`path=C:\\data`})
	if err != nil {
		t.Fatal(err)
	}
	match := func(v string) bool {
		var ctx MatchContext
		return set.Match(func(string) string { return v }, &ctx)
	}
	if !match(`C:\data`) {
		t.Error(`C:\\data must match the literal C:\data`)
	}
	if match("C:5ata") {
		t.Error(`C:\\data must not behave as a digit class`)
	}
}

// Exact-selector unescaping is one left-to-right pass. The sequential
// ReplaceAll pair made the language ambiguous: pass one manufactured a \" that
// pass two consumed, so `\\"` (literal backslash + bare quote) decoded to `"`.
func TestExactSelectorUnescapeSinglePass(t *testing.T) {
	t.Parallel()
	set, err := ParseSelectors([]string{`msg=a\\"b`}, nil)
	if err != nil {
		t.Fatal(err)
	}
	match := func(v string) bool {
		var ctx MatchContext
		return set.Match(func(string) string { return v }, &ctx)
	}
	if !match(`a\"b`) {
		t.Error(`a\\"b must decode to a\"b (backslash escape consumed, bare quote verbatim)`)
	}
	if match(`a"b`) {
		t.Error(`a\\"b must not double-decode to a"b`)
	}
}
