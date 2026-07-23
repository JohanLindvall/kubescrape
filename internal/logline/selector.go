// Package logline is the log-line matching and field-extraction DSL shared
// by the log-derived metrics engine (internal/metrics) and the tailer's log
// rules: label selectors (exact and regex, with per-line memoization), the
// keep/drop/sample LineFilter, and single-pass JSON/logfmt field extraction
// for exactly the keys the rules reference. The synthetic LineKey ("__line__")
// resolves to the whole raw line.
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

// cached returns the memoized result for hash, if known; store records one.
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

// labelKeys returns the distinct label names the selectors read, so a caller
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

// match reports whether every selector holds for the given label lookup.
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
func ParseSelectors(exact, regex []string) (*Selectors, error) {
	set := &Selectors{}
	for _, in := range exact {
		label, expr, want, hash, err := parseSelector(in)
		if err != nil {
			return nil, err
		}
		set.exact = append(set.exact, exactSelector{label: label, value: expr, want: want, hash: hash})
	}
	for _, in := range regex {
		label, expr, want, hash, err := parseSelector(in)
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
// false for the negated form.
func parseSelector(in string) (label, expr string, want bool, hash uint64, err error) {
	i := strings.IndexAny(in, "!=")
	if i == -1 {
		return "", "", false, 0, fmt.Errorf("invalid selector: %s", in)
	}
	label = in[:i]
	want = in[i] == '='
	rest := in[i+1:]
	if !want {
		rest = strings.TrimPrefix(rest, "=") // "label!=value"
	}
	expr = unescapeSelector(rest)
	// Hash label and expression separately: a "\n"-joined string let
	// "a\nb"="c" and "a"="b\nc" share a memo slot.
	hash = pairHash(xxhash.Sum64String(label), xxhash.Sum64String(expr))
	return label, expr, want, hash, nil
}

// regexSelectorKind discriminates regex-selector memo hashes from exact ones.
const regexSelectorKind = 0x9e3779b97f4a7c15

func unescapeSelector(s string) string {
	s = strings.ReplaceAll(s, `\\`, `\`)
	return strings.ReplaceAll(s, `\"`, `"`)
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
