// Package logline is the log-line matching and field-extraction DSL shared
// by the log-derived metrics engine (internal/metrics) and the keep/drop log
// rules every log producer and the ingest path apply: label selectors (exact
// and regex, with per-line memoization), the keep/drop/sample LineFilter, and
// single-pass JSON/logfmt field extraction for exactly the keys the rules
// reference. Both synthetic keys live here, in the shared DSL, even though
// neither tier resolves both: LineKey ("__line__") is the whole raw line, and
// SeverityKey ("__severity__") is the RULES tier's enriched severity — named
// here so the metrics engine, which shares the selector language but cannot
// resolve it, refuses a config using it instead of compiling a rule that
// matches nothing.
package logline

import (
	"fmt"
	"math/bits"
	"regexp"
	"strings"

	"github.com/JohanLindvall/haste/rapidhash"
)

// A Selectors matches a line against a conjunction of label selectors. Every
// selector must hold for the set to match. Selectors are either exact
// (key=value / key!=value) or a regex against the value, spelled the same
// way (key=re / key!=re) and told apart only by which input list carries them
// — see ParseSelectors. There is no =~ operator: "key=~re" in the regex list
// compiles to a pattern beginning with '~'.
type Selectors struct {
	exact []exactSelector
	regex []regexSelector
}

type exactSelector struct {
	label, value string
	hash         uint64
	want         bool // false for a negated (!=) selector
}

type regexSelector struct {
	label string
	re    *regexp.Regexp
	hash  uint64
	want  bool
}

// MatchContext memoizes selector outcomes across the metrics evaluated for a
// single line: two metrics that share a selector (same label+expression, hence
// same hash) evaluate the underlying lookup once. Reset it per line.
//
// It is an open-addressed table keyed by the selector hash, and every slot
// carries the GENERATION (line) that wrote it, so Reset is one increment
// rather than a clear. It used to be two slices searched linearly — and a TRUE
// shared selector scanned the whole false list first — which made one line
// QUADRATIC in the distinct selectors it evaluated: at 50 two-selector rules
// the memo cost more than evaluating without it, at 200 about five times more
// (33-38 us against 6-7 us per line, on every line through logMetrics and the
// logs rules). A lookup is now one masked index and, at the half-full load
// the table keeps, a probe or two. The table grows only when a line evaluates
// more distinct selectors than any line before it, so a pooled context is
// allocation-free once warm, as the slices were.
type MatchContext struct {
	slots []memoSlot // power-of-two length; nil until the first Store
	gen   uint32     // this line's stamp; a slot stamped otherwise is empty
	n     int        // slots stamped with gen
}

// memoSlot is one memoized outcome, live only while gen is the context's.
type memoSlot struct {
	hash uint64
	gen  uint32
	hit  bool
}

// minMemoSlots is the table's first size: 8 selectors before it grows.
const minMemoSlots = 16

// Reset forgets the memoized results: once per line, before the selectors run.
func (c *MatchContext) Reset() {
	c.n = 0
	c.gen++
	if c.gen == 0 {
		// Wrapped after 2^32 lines: a slot stamped 2^32 lines ago would read
		// as current. Clear once and restart the stamps.
		clear(c.slots)
		c.gen = 1
	}
}

// Cached returns the memoized result for hash, if known; Store records one.
// (Two calls rather than an eval(hash, func() bool) so the hot path does not
// allocate a closure per selector per line.)
func (c *MatchContext) Cached(hash uint64) (result, known bool) {
	if c.n == 0 {
		return false, false
	}
	// The load factor stays at or below one half, so the probe always
	// reaches an empty slot.
	mask := uint64(len(c.slots) - 1)
	for i := hash & mask; ; i = (i + 1) & mask {
		s := &c.slots[i]
		if s.gen != c.gen {
			return false, false
		}
		if s.hash == hash {
			return s.hit, true
		}
	}
}

// Store records a selector's result under its hash for the rest of the line.
func (c *MatchContext) Store(hash uint64, result bool) {
	s, known := c.slot(hash)
	if !known {
		c.claim(s, hash)
	}
	s.hit = result
}

// slot is Cached and Store's probe done once, for Match: the slot holding
// hash, or — known false — the empty slot it belongs in, which the caller
// fills through claim before the next slot call. It makes room for that one
// insertion first, so the pointer stays valid.
func (c *MatchContext) slot(hash uint64) (s *memoSlot, known bool) {
	if c.gen == 0 {
		c.gen = 1 // used without a Reset: 0 is what an empty slot carries
	}
	if 2*(c.n+1) > len(c.slots) {
		c.grow()
	}
	// The selector hashes are avalanche-finished (pairHash), so the low bits
	// index the table without a further mix.
	mask := uint64(len(c.slots) - 1)
	for i := hash & mask; ; i = (i + 1) & mask {
		s = &c.slots[i]
		if s.gen != c.gen {
			return s, false
		}
		if s.hash == hash {
			return s, true
		}
	}
}

// claim stamps the empty slot slot returned for hash as this line's.
func (c *MatchContext) claim(s *memoSlot, hash uint64) {
	s.hash, s.gen = hash, c.gen
	c.n++
}

// grow doubles the table and re-inserts this line's entries; the stale ones
// from earlier lines are simply not carried.
func (c *MatchContext) grow() {
	old := c.slots
	c.slots = make([]memoSlot, max(minMemoSlots, 2*len(old)))
	mask := uint64(len(c.slots) - 1)
	for _, s := range old {
		if s.gen != c.gen {
			continue
		}
		i := s.hash & mask
		for c.slots[i].gen == c.gen {
			i = (i + 1) & mask
		}
		c.slots[i] = s
	}
}

// LabelKeys returns the distinct label names the selectors read, so a caller
// can arrange for those to be resolvable.
func (s *Selectors) LabelKeys() []string {
	keys := make([]string, 0, len(s.exact)+len(s.regex))
	for _, sel := range s.exact {
		keys = append(keys, sel.label)
	}
	for _, sel := range s.regex {
		keys = append(keys, sel.label)
	}
	return keys
}

// Match reports whether every selector holds for the given label lookup.
func (s *Selectors) Match(lookup func(string) string, ctx *MatchContext) bool {
	for i := range s.exact {
		sel := &s.exact[i]
		m, known := ctx.slot(sel.hash)
		if !known {
			ctx.claim(m, sel.hash)
			m.hit = lookup(sel.label) == sel.value
		}
		if m.hit != sel.want {
			return false
		}
	}
	for i := range s.regex {
		sel := &s.regex[i]
		m, known := ctx.slot(sel.hash)
		if !known {
			ctx.claim(m, sel.hash)
			m.hit = sel.re.MatchString(lookup(sel.label))
		}
		if m.hit != sel.want {
			return false
		}
	}
	return true
}

// ParseSelectors compiles exact and regex selector strings into a Selectors.
// Empty inputs yield a set that matches everything.
//
// The selector language: "label=value" / "label!=value". An EXACT value may
// spell a literal backslash or double quote as \\ and \" (one left-to-right
// pass; anything else after a backslash is verbatim). A REGEX value is passed
// to RE2 UNTOUCHED — backslash is the regex escape character there, and an
// extra unescape layer silently rewrote patterns (`C:\\data`, the standard
// spelling for a literal `C:\data`, became `C:\data`, where \d is a digit
// class matching `C:5ata`).
func ParseSelectors(exact, regex []string) (*Selectors, error) {
	set := &Selectors{}
	for _, in := range exact {
		label, expr, want, hash, err := parseSelector(in, false)
		if err != nil {
			return nil, err
		}
		set.exact = append(set.exact, exactSelector{label: label, value: expr, want: want, hash: hash})
	}
	for _, in := range regex {
		label, expr, want, hash, err := parseSelector(in, true)
		if err != nil {
			return nil, err
		}
		re, err := regexp.Compile(expr)
		if err != nil {
			return nil, fmt.Errorf("invalid regex selector %q: %w", in, err)
		}
		// The memo caches raw match outcomes by hash; an exact and a regex
		// selector over the same label+expression text test different things,
		// so the regex kind must not share the exact kind's slot.
		hash = pairHash(hash, regexSelectorKind)
		set.regex = append(set.regex, regexSelector{label: label, re: re, want: want, hash: hash})
	}
	return set, nil
}

// parseSelector splits "label=value" or "label!=value" into its parts. want is
// false for the negated form. regex leaves the expression verbatim for RE2
// (see ParseSelectors); exact values get the \\ / \" unescape.
//
// The grammar is strict where leniency compiled into silent misbehavior: an
// empty label ("=") resolved every lookup to "" and MATCHED EVERY LINE —
// {action: drop, match: ["="]} silently dropped a node's whole log stream,
// defeating NewLineFilter's deliberate refusal of an empty match list — and a
// bare '!' with no '=' ("a!b") read as a != "b" instead of erroring.
func parseSelector(in string, regex bool) (label, expr string, want bool, hash uint64, err error) {
	i := strings.IndexAny(in, "!=")
	if i <= 0 {
		return "", "", false, 0, fmt.Errorf("invalid selector %q (want label=value or label!=value)", in)
	}
	label = in[:i]
	want = in[i] == '='
	rest := in[i+1:]
	if !want {
		var found bool
		if rest, found = strings.CutPrefix(rest, "="); !found {
			return "", "", false, 0, fmt.Errorf("invalid selector %q ('!' must be followed by '=')", in)
		}
	}
	expr = rest
	if !regex {
		expr = unescapeSelector(rest)
	}
	// Hash label and expression separately: a "\n"-joined string let
	// "a\nb"="c" and "a"="b\nc" share a memo slot.
	hash = pairHash(rapidhash.Sum64String(label), rapidhash.Sum64String(expr))
	return label, expr, want, hash, nil
}

// regexSelectorKind discriminates regex-selector memo hashes from exact ones.
const regexSelectorKind = 0x9e3779b97f4a7c15

// unescapeSelector decodes an exact selector value's escapes — \\ and \" —
// in ONE left-to-right pass; any other byte after a backslash is verbatim.
// The old sequential ReplaceAll pair made the language ambiguous: pass one
// (\\→\) manufactured a \" that pass two consumed, so the input `\\"` (a
// literal backslash followed by a bare quote, legal since quotes need no
// escaping) decoded to `"` instead of `\"`.
func unescapeSelector(s string) string {
	i := strings.IndexByte(s, '\\')
	if i < 0 {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	b.WriteString(s[:i])
	for ; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) && (s[i+1] == '\\' || s[i+1] == '"') {
			i++
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// pairHash folds two 64-bit hashes into one memo key with an avalanche
// finish. It keys ONLY the per-line selector match memo — it need not (and
// deliberately does not) match internal/metrics' series hash domain.
func pairHash(h1, h2 uint64) uint64 {
	const (
		prime1 uint64 = 11400714785074694791
		prime2 uint64 = 14029467366897019727
		prime5 uint64 = 2870177450012600261
	)
	h := prime5 + h1*prime1 + bits.RotateLeft64(h2, 29)*prime2
	h ^= h >> 33
	h *= prime2
	h ^= h >> 29
	h *= prime1
	h ^= h >> 32
	return h
}
