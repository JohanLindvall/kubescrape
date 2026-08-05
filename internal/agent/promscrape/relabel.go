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
	"strconv"
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

// relabelCache caches compiled chains by fingerprint. It is process-global (one
// per Scraper), shared across the concurrent scrape goroutines, and keyed by the
// full rule text — so a controller minting monitors with templated or hashed
// regexes would otherwise leak a compiled chain per distinct fingerprint for the
// process' life. Bounded like its siblings (tlsClients, podCache, warned).
type relabelCache struct {
	mu sync.Mutex
	m  map[string][]compiledRelabel
}

// maxRelabelChains bounds the compiled-chain cache. The distinct-chain count of
// a real fleet is tiny (rules rarely differ across monitors and rarely change),
// so the cap only defends against a generator churning fingerprints; evict-one
// rather than clear (like tlsClients) so a steady population above the cap does
// not recompile every chain each cycle.
const maxRelabelChains = 1024

// session compiles (or reuses) the chain for a target and returns a fresh
// session around it. Returns nil for targets without rules; a compile error
// fails the scrape (silently ignoring a filter would export what the user
// asked to drop).
func (c *relabelCache) session(rules []kubemeta.RelabelRule) (*relabelFilter, error) {
	if len(rules) == 0 {
		return nil, nil
	}
	// Length-prefixed via appendLP (the package's one injective-join rule): a
	// regex or source label may contain any delimiter byte, and two rule chains
	// fingerprinting to one key would hand one endpoint the other's compiled
	// filter. The source-label COUNT is part of the encoding — the parts are
	// self-delimiting, but rule boundaries are not without it.
	fp := make([]byte, 0, 128)
	for _, r := range rules {
		fp = appendLP(fp, r.Action)
		fp = strconv.AppendInt(fp, int64(len(r.SourceLabels)), 10)
		fp = append(fp, ';')
		for _, src := range r.SourceLabels {
			fp = appendLP(fp, src)
		}
		fp = appendLP(fp, r.Regex)
	}
	key := string(fp)
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
		// Evict one arbitrary entry at the cap before inserting the new chain.
		if _, present := c.m[key]; !present && len(c.m) >= maxRelabelChains {
			for k := range c.m {
				delete(c.m, k)
				break
			}
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
	return labelValue(labels, key)
}
