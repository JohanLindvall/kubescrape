package logline

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
)

// LineRule is one ordered keep/drop rule over log lines (the `rules` list of
// the logs config). Selectors use the same DSL and key resolution as the
// log-metrics `match`/`matchRegexp`: keys resolve against the caller's lookup
// (record and resource attributes; the tailer adds the synthetic
// `__severity__`) with the line's own JSON/logfmt fields as fallback, and
// `__line__` matches the whole raw line.
type LineRule struct {
	// Action is "keep" or "drop".
	Action string `json:"action"`
	// Match / MatchRegexp must all hold for the rule to match (exact and
	// regex selectors respectively; key!=value negates).
	Match       []string `json:"match,omitempty"`
	MatchRegexp []string `json:"matchRegexp,omitempty"`
	// Sample, on a keep rule, keeps only this fraction of the matching lines
	// (deterministic: every round(1/sample)-th line), dropping the rest.
	Sample float64 `json:"sample,omitempty"`
}

// LineFilter is an ordered first-match-wins line filter; lines matching no
// rule are kept. Compiled once, evaluated per exported log record.
type LineFilter struct {
	rules []lineFilterRule
	keys  KeyIndex
	pool  sync.Pool
}

type lineFilterRule struct {
	match  *Selectors
	drop   bool
	every  uint64 // keep 1 in every (0 = all)
	picked atomic.Uint64
}

// filterCtx is the pooled per-line evaluation state, mirroring addContext:
// the lookup closure is bound once so evaluation allocates nothing.
type filterCtx struct {
	ctx    MatchContext
	line   Fields
	filter *LineFilter
	lookup func(string) string
	raw    string
	fn     func(string) string
}

func (fc *filterCtx) resolve(key string) string {
	if key == LineKey {
		return fc.raw
	}
	if fc.lookup != nil {
		if v := fc.lookup(key); v != "" {
			return v
		}
	}
	return fc.filter.keys.Get(&fc.line, key)
}

// NewLineFilter compiles rules; empty input yields a nil filter (keep all).
func NewLineFilter(rules []LineRule) (*LineFilter, error) {
	if len(rules) == 0 {
		return nil, nil
	}
	f := &LineFilter{keys: NewKeyIndex()}
	f.rules = make([]lineFilterRule, len(rules))
	for i := range rules {
		r := &rules[i]
		cr := &f.rules[i]
		switch r.Action {
		case "keep":
		case "drop":
			cr.drop = true
		default:
			return nil, fmt.Errorf("logs rule %d: action %q (want keep or drop)", i, r.Action)
		}
		if r.Sample != 0 {
			if cr.drop {
				return nil, fmt.Errorf("logs rule %d: sample is only valid on keep rules", i)
			}
			// The 1e-9 floor keeps 1/sample within uint64 (a pathological
			// 1e-20 would overflow the float→uint64 conversion into an
			// implementation-defined counter).
			if r.Sample < 1e-9 || r.Sample > 1 {
				return nil, fmt.Errorf("logs rule %d: sample %v (want 1e-9 <= sample <= 1)", i, r.Sample)
			}
			cr.every = uint64(math.Round(1 / r.Sample))
		}
		if len(r.Match) == 0 && len(r.MatchRegexp) == 0 {
			return nil, errors.New("logs rule: empty match would apply to every line; use an explicit __line__ selector instead")
		}
		match, err := ParseSelectors(r.Match, r.MatchRegexp)
		if err != nil {
			return nil, fmt.Errorf("logs rule %d: %w", i, err)
		}
		cr.match = match
		for _, key := range match.LabelKeys() {
			f.keys.Add(key)
		}
	}
	f.pool = sync.Pool{New: func() any {
		fc := &filterCtx{filter: f}
		fc.fn = fc.resolve
		return fc
	}}
	return f, nil
}

// Keep reports whether the line should be exported. lookup resolves attribute
// keys (nil allowed); line is the raw body. Safe on a nil receiver (keep) and
// for concurrent use.
func (f *LineFilter) Keep(lookup func(string) string, line string) bool {
	if f == nil {
		return true
	}
	fc := f.pool.Get().(*filterCtx)
	fc.ctx.Reset()
	fc.line.Reset(line)
	fc.lookup, fc.raw = lookup, line

	keep := true
	for i := range f.rules {
		r := &f.rules[i]
		if !r.match.Match(fc.fn, &fc.ctx) {
			continue
		}
		keep = !r.drop
		if keep && r.every > 1 {
			keep = (r.picked.Add(1)-1)%r.every == 0
		}
		break
	}
	fc.lookup, fc.raw = nil, ""
	f.pool.Put(fc)
	return keep
}
