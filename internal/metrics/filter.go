package metrics

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/cespare/xxhash/v2"
)

// A selectorSet matches a line against a conjunction of label selectors. Every
// selector must hold for the set to match. Selectors are either exact
// (key=value / key!=value) or a regex against the value (key=~re / key!~re,
// expressed through separate exact/regex input lists — see parseSelectors).
type selectorSet struct {
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

// matchContext memoizes selector outcomes across the metrics evaluated for a
// single line: two metrics that share a selector (same label+expression, hence
// same hash) evaluate the underlying lookup once. Reset it per line.
type matchContext struct {
	trueHashes, falseHashes []uint64
}

func (c *matchContext) reset() {
	c.trueHashes = c.trueHashes[:0]
	c.falseHashes = c.falseHashes[:0]
}

// cached returns the memoized result for hash, if known; store records one.
// (Two calls rather than an eval(hash, func() bool) so the hot path does not
// allocate a closure per selector per line.)
func (c *matchContext) cached(hash uint64) (result, known bool) {
	if slices.Contains(c.falseHashes, hash) {
		return false, true
	}
	if slices.Contains(c.trueHashes, hash) {
		return true, true
	}
	return false, false
}

func (c *matchContext) store(hash uint64, result bool) {
	if result {
		c.trueHashes = append(c.trueHashes, hash)
	} else {
		c.falseHashes = append(c.falseHashes, hash)
	}
}

// labelKeys returns the distinct label names the selectors read, so a caller
// can arrange for those to be resolvable.
func (s *selectorSet) labelKeys() []string {
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
func (s *selectorSet) match(lookup func(string) string, ctx *matchContext) bool {
	for i := range s.exact {
		sel := &s.exact[i]
		hit, known := ctx.cached(sel.hash)
		if !known {
			hit = lookup(sel.label) == sel.value
			ctx.store(sel.hash, hit)
		}
		if hit != sel.want {
			return false
		}
	}
	for i := range s.regex {
		sel := &s.regex[i]
		hit, known := ctx.cached(sel.hash)
		if !known {
			hit = sel.re.MatchString(lookup(sel.label))
			ctx.store(sel.hash, hit)
		}
		if hit != sel.want {
			return false
		}
	}
	return true
}

// parseSelectors compiles exact and regex selector strings into a selectorSet.
// Empty inputs yield a set that matches everything.
func parseSelectors(exact, regex []string) (*selectorSet, error) {
	set := &selectorSet{}
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
	hash = xxhash.Sum64String(label + "\n" + expr)
	return label, expr, want, hash, nil
}

func unescapeSelector(s string) string {
	s = strings.ReplaceAll(s, `\\`, `\`)
	return strings.ReplaceAll(s, `\"`, `"`)
}
