package attrs

import (
	"fmt"
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
}

// NewFilter compiles a filter from comma-separated regex lists; empty
// strings mean "enable all" and "disable none" respectively.
func NewFilter(enable, disable string) (*Filter, error) {
	f := &Filter{}
	var err error
	if f.enable, err = compileSet(enable); err != nil {
		return nil, fmt.Errorf("enable patterns: %w", err)
	}
	if f.disable, err = compileSet(disable); err != nil {
		return nil, fmt.Errorf("disable patterns: %w", err)
	}
	if f.enable == nil && f.disable == nil {
		return nil, nil // keep-everything filters stay nil (no-op fast path)
	}
	return f, nil
}

// compileSet turns a comma-separated pattern list into one fully anchored
// alternation; nil for an empty list.
func compileSet(patterns string) (*regexp.Regexp, error) {
	var parts []string
	for _, p := range strings.Split(patterns, ",") {
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
func (f *Filter) Keep(key string) bool {
	if f == nil {
		return true
	}
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
