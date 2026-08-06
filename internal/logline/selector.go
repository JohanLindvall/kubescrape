// Package logline is the log-line matching and field-extraction DSL shared
// by the log-derived metrics engine (internal/metrics) and the tailer's log
// rules: label selectors (exact and regex, with per-line memoization), the
// keep/drop/sample LineFilter, and single-pass JSON/logfmt field extraction
// for exactly the keys the rules reference. Both synthetic keys live here, in
// the shared DSL, even though neither tier resolves both: LineKey ("__line__")
// is the whole raw line, and SeverityKey ("__severity__") is the RULES tier's
// enriched severity — named here so the metrics engine, which shares the
// selector language but cannot resolve it, refuses a config using it instead of
// compiling a rule that matches nothing.
package logline

import (
	"fmt"
	"math/bits"
	"regexp"
	"slices"
	"strings"

	"github.com/cespare/xxhash/v2"
)

// A Selectors matches a line against a conjunction of label selectors. Every
// selector must hold for the set to match. Selectors are either exact
// (key=value / key!=value) or a regex against the value (key=~re / key!~re,
// expressed through separate exact/regex input lists — see ParseSelectors).
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
type MatchContext struct {
	trueHashes, falseHashes []uint64
}

func (c *MatchContext) Reset() {
	c.trueHashes = c.trueHashes[:0]
	c.falseHashes = c.falseHashes[:0]
}

// Cached returns the memoized result for hash, if known; Store records one.
// (Two calls rather than an eval(hash, func() bool) so the hot path does not
// allocate a closure per selector per line.)
func (c *MatchContext) Cached(hash uint64) (result, known bool) {
	if slices.Contains(c.falseHashes, hash) {
		return false, true
	}
	if slices.Contains(c.trueHashes, hash) {
		return true, true
	}
	return false, false
}

func (c *MatchContext) Store(hash uint64, result bool) {
	if result {
		c.trueHashes = append(c.trueHashes, hash)
	} else {
		c.falseHashes = append(c.falseHashes, hash)
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
		hit, known := ctx.Cached(sel.hash)
		if !known {
			hit = lookup(sel.label) == sel.value
			ctx.Store(sel.hash, hit)
		}
		if hit != sel.want {
			return false
		}
	}
	for i := range s.regex {
		sel := &s.regex[i]
		hit, known := ctx.Cached(sel.hash)
		if !known {
			hit = sel.re.MatchString(lookup(sel.label))
			ctx.Store(sel.hash, hit)
		}
		if hit != sel.want {
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
	hash = pairHash(xxhash.Sum64String(label), xxhash.Sum64String(expr))
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
