package logline

import (
	"fmt"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

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

// The memo is a table keyed by the selector hash: every stored outcome must
// read back, hashes sharing their low bits (the index) must not shadow each
// other, and Reset must forget everything — including across the wrap of the
// per-line stamp, where a slot written 2^32 lines ago would otherwise read as
// current.
func TestMatchContextMemoizesEveryHashUntilReset(t *testing.T) {
	t.Parallel()
	var c MatchContext
	c.Reset()
	const n = 1000
	// Every hash shares its low 16 bits, so they all probe from one slot.
	hash := func(i int) uint64 { return uint64(i+1)<<16 | 0x5a5a }
	for i := range n {
		if _, known := c.Cached(hash(i)); known {
			t.Fatalf("hash %d known before it was stored", i)
		}
		c.Store(hash(i), i%3 == 0)
	}
	for i := range n {
		got, known := c.Cached(hash(i))
		if !known || got != (i%3 == 0) {
			t.Fatalf("hash %d: (%v, %v), want (%v, true)", i, got, known, i%3 == 0)
		}
	}
	c.Reset()
	for i := range n {
		if _, known := c.Cached(hash(i)); known {
			t.Fatalf("hash %d survived Reset", i)
		}
	}
	// Across the stamp's wrap: an entry written under the stamp the wrap
	// restarts at must not read as current afterwards.
	var w MatchContext
	w.Reset() // stamp 1
	w.Store(hash(7), true)
	w.gen = ^uint32(0)
	w.Reset() // wraps back to stamp 1
	w.Store(hash(9), false)
	if _, known := w.Cached(hash(7)); known {
		t.Fatal("an entry from before the stamp's wrap reads as current")
	}
	if got, known := w.Cached(hash(9)); got || !known {
		t.Fatalf("hash 9 after the wrap: (%v, %v), want (false, true)", got, known)
	}
	// A context used without a Reset still memoizes.
	var fresh MatchContext
	fresh.Store(0, true)
	if got, known := fresh.Cached(0); !got || !known {
		t.Fatalf("un-Reset context: (%v, %v), want (true, true)", got, known)
	}
}

// memoRules builds n two-selector rule sets sharing their FIRST selector
// (true for every line) with a distinct, false second one — the shape that
// made the old slice memo quadratic: n distinct outcomes stored per line, and
// the shared true one looked up n times.
func memoRules(tb testing.TB, n int) []*Selectors {
	tb.Helper()
	rules := make([]*Selectors, n)
	for i := range rules {
		s, err := ParseSelectors([]string{"ns=prod", fmt.Sprintf("app=a%d", i)}, nil)
		if err != nil {
			tb.Fatal(err)
		}
		rules[i] = s
	}
	return rules
}

func memoLookup(k string) string {
	if k == "ns" {
		return "prod"
	}
	return "other"
}

// Matching a line against hundreds of rules through one context allocates
// nothing once the context is warm — the table grows only past the largest
// line it has seen.
func TestMatchContextIsAllocationFreeWhenWarm(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector adds allocations")
	}
	rules := memoRules(t, 200)
	var c MatchContext
	line := func() {
		c.Reset()
		for _, r := range rules {
			if r.Match(memoLookup, &c) {
				t.Fatal("no rule should match")
			}
		}
	}
	if got := testing.AllocsPerRun(100, line); got != 0 {
		t.Fatalf("allocs per line = %v, want 0", got)
	}
}

// BenchmarkSelectorMemo is the per-line cost of evaluating every rule through
// the memo, which must stay flat per rule as the rule count grows.
func BenchmarkSelectorMemo(b *testing.B) {
	for _, n := range []int{10, 50, 200} {
		b.Run(fmt.Sprintf("rules=%d", n), func(b *testing.B) {
			rules := memoRules(b, n)
			var c MatchContext
			b.ReportAllocs()
			for b.Loop() {
				c.Reset()
				for _, r := range rules {
					r.Match(memoLookup, &c)
				}
			}
		})
	}
}
