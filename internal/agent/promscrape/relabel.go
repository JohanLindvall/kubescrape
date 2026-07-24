package promscrape

// The keep/drop subset of Prometheus metric_relabel_configs, applied per
// sample for monitor endpoints that declare metricRelabelings. Semantics
// match Prometheus: sourceLabels values joined with ";" ("__name__" is the
// metric name), matched against the FULLY ANCHORED regex; keep drops samples
// whose join does NOT match, drop drops those that do. Other actions
// (replace, labelmap, ...) are not interpreted (documented).
//
// Compiled filters are cached by their rule fingerprint — targets are
// re-fetched every cycle but their rules rarely change. Only targets that
// carry rules pay the per-sample join (one reused buffer per scrape).

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

type compiledRelabel struct {
	keep bool
	src  []string
	re   *regexp.Regexp
}

// relabelFilter is one endpoint's compiled rule chain plus its per-scrape
// scratch buffer (a session: one per scrape, not shared).
type relabelFilter struct {
	rules []compiledRelabel
	buf   []byte
}

// relabelCache caches compiled chains by fingerprint.
type relabelCache struct {
	mu sync.Mutex
	m  map[string][]compiledRelabel
}

// session compiles (or reuses) the chain for a target and returns a fresh
// session around it. Returns nil for targets without rules; a compile error
// fails the scrape (silently ignoring a filter would export what the user
// asked to drop).
func (c *relabelCache) session(rules []kubemeta.RelabelRule) (*relabelFilter, error) {
	if len(rules) == 0 {
		return nil, nil
	}
	var fp strings.Builder
	for _, r := range rules {
		fp.WriteString(r.Action)
		fp.WriteByte(0)
		fp.WriteString(strings.Join(r.SourceLabels, "\x01"))
		fp.WriteByte(0)
		fp.WriteString(r.Regex)
		fp.WriteByte(0)
	}
	key := fp.String()
	c.mu.Lock()
	compiled, ok := c.m[key]
	c.mu.Unlock()
	if !ok {
		for _, r := range rules {
			if r.Action != "keep" && r.Action != "drop" {
				continue // parse already restricted to keep/drop; belt and braces
			}
			rx := r.Regex
			if rx == "" {
				rx = "(.*)" // Prometheus default
			}
			re, err := regexp.Compile("^(?:" + rx + ")$")
			if err != nil {
				return nil, fmt.Errorf("metricRelabelings regex %q: %w", r.Regex, err)
			}
			compiled = append(compiled, compiledRelabel{keep: r.Action == "keep", src: r.SourceLabels, re: re})
		}
		c.mu.Lock()
		if c.m == nil {
			c.m = map[string][]compiledRelabel{}
		}
		c.m[key] = compiled
		c.mu.Unlock()
	}
	return &relabelFilter{rules: compiled}, nil
}

// Keep reports whether a sample survives the chain.
func (f *relabelFilter) Keep(name string, labels []Label) bool {
	for i := range f.rules {
		r := &f.rules[i]
		f.buf = f.buf[:0]
		for j, src := range r.src {
			if j > 0 {
				f.buf = append(f.buf, ';')
			}
			f.buf = append(f.buf, labelOrName(name, labels, src)...)
		}
		matched := r.re.Match(f.buf)
		if r.keep && !matched {
			return false
		}
		if !r.keep && matched {
			return false
		}
	}
	return true
}

func labelOrName(name string, labels []Label, key string) string {
	if key == "__name__" {
		return name
	}
	for _, l := range labels {
		if l.Name == key {
			return l.Value
		}
	}
	return ""
}
