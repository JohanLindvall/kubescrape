package attrs

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Filter selects which resource attributes are exported. An attribute key
// is kept when it matches the enable set (or no enable set is configured)
// and does not match the disable set (or no disable set is configured).
// Patterns are regular expressions matched against the full key
// ("k8s.pod.label.app" matches `k8s\.pod\.label\..*` but not `k8s\.pod`).
//
// A nil *Filter keeps everything.
type Filter struct {
	enable  *regexp.Regexp
	disable *regexp.Regexp
	// keep memoizes the verdict per KEY. The decision is a pure function of the
	// key, and the key set of a resource is closed and tiny (~25 names) while
	// the filter runs on every attribute of every resource built — 300k anchored
	// regex evaluations per scrape on a 12k-object split path. Bounded and
	// evicting (genCache), because label keys reach it and those are data.
	keep *genCache[bool]
}

// maxFilterKeys bounds the memoized verdicts (two generations of it).
const maxFilterKeys = 4096

// NewFilterFromLists compiles a filter from regex LISTS — the only config
// form. The comma-separated flag form it replaced could not express a pattern
// containing a comma at all: `.{1,3}` split into two fragments that RE2 still
// compiled, as literal braces, so the filter silently dropped every attribute
// it was meant to keep.
func NewFilterFromLists(enable, disable []string) (*Filter, error) {
	f := &Filter{}
	var err error
	if f.enable, err = compileList(enable); err != nil {
		return nil, fmt.Errorf("enable patterns: %w", err)
	}
	if f.disable, err = compileList(disable); err != nil {
		return nil, fmt.Errorf("disable patterns: %w", err)
	}
	if f.enable == nil && f.disable == nil {
		return nil, nil // keep-everything filters stay nil (no-op fast path)
	}
	f.keep = newGenCache[bool](maxFilterKeys)
	return f, nil
}

func compileList(patterns []string) (*regexp.Regexp, error) {
	var parts []string
	for _, p := range patterns {
		if p = strings.TrimSpace(p); p != "" {
			parts = append(parts, "(?:"+p+")")
		}
	}
	if len(parts) == 0 {
		return nil, nil
	}
	return regexp.Compile("^(?:" + strings.Join(parts, "|") + ")$")
}

// Keep reports whether an attribute key survives the filter.
//
// A REFUSED key is reported once, at Debug, on the memo MISS — the one place
// the decision is actually taken, so the line costs nothing on the hot path
// and cannot repeat per resource. It is the only explanation an operator gets
// for an attribute that a correct config produced and this filter removed:
// enable/disable are anchored whole-key regexes, and the commonest mistake
// (`k8s.pod` for `k8s\.pod\..*`) matches nothing and silently strips
// everything the enable list was supposed to keep. Debug rather than Warn
// because dropping keys is exactly what a configured filter is for.
func (f *Filter) Keep(key string) bool {
	if f == nil {
		return true
	}
	if keep, ok := f.keep.load(key); ok {
		return keep
	}
	keep := f.match(key)
	f.keep.store(key, keep)
	if !keep && slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		slog.Debug("resourceAttributes enable/disable removed an attribute key",
			"key", key, "reason", f.refusal(key))
	}
	return keep
}

// refusal says WHICH half of the filter refused the key — the two are
// different mistakes (an enable list too narrow, a disable list too wide) and
// a line that does not distinguish them sends the operator to the wrong one.
func (f *Filter) refusal(key string) string {
	if f.enable != nil && !f.enable.MatchString(key) {
		return "no enable pattern matched"
	}
	return "a disable pattern matched"
}

// match is Keep's decision without the memo: kept when it matches the enable
// set (or there is none) and does not match the disable set.
func (f *Filter) match(key string) bool {
	if f.enable != nil && !f.enable.MatchString(key) {
		return false
	}
	if f.disable != nil && f.disable.MatchString(key) {
		return false
	}
	return true
}

// Apply removes filtered-out attributes from a resource.
func (f *Filter) Apply(res pcommon.Resource) {
	if f == nil {
		return
	}
	res.Attributes().RemoveIf(func(key string, _ pcommon.Value) bool {
		return !f.Keep(key)
	})
}
