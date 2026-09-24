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
// carry rules pay the per-sample join (one reused buffer per scrape), and a
// rule whose join repeats on consecutive samples pays a memcmp instead of its
// regex (relabelFilter.last).

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
	// last is one last-seen memo per rule: the join the rule last evaluated
	// and its verdict. An exposition is family-ordered, so a rule on __name__
	// or on a low-cardinality label sees the same join on consecutive samples,
	// and the memo turns that anchored-regex run into a memcmp — the parser's
	// lastMetric/lastKV pattern, and the reason it is last-seen rather than a
	// map: a rule on a high-cardinality label (pod, id) misses every time, and a
	// map memo there measured thousands of allocations per scrape for no gain
	// where this costs a compare. Allocated once per session.
	last []relabelLast
}

// relabelLast is one rule's last-seen join and verdict.
type relabelLast struct {
	val     []byte
	ok      bool // val holds a join this rule evaluated
	matched bool
}

// maxRelabelMemoBytes bounds the join a rule's memo remembers. A longer one is
// simply re-evaluated each time: a join that long is a label value that rarely
// repeats, and the memo must not pin a target's longest value per rule.
const maxRelabelMemoBytes = 256

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
//
// evicted reports that this call had to make room at the cap. The cache cannot
// log for itself — it holds no logger and runs on the concurrent scrape
// goroutines, under its own mutex — so the fact is RETURNED and the caller
// reports it once the lock is gone. Without it the cache thrashed in total
// silence: every scrape recompiles its chain, no counter moves, and the only
// symptom is a node quietly burning CPU.
func (c *relabelCache) session(rules []kubemeta.RelabelRule) (f *relabelFilter, evicted bool, err error) {
	if len(rules) == 0 {
		return nil, false, nil
	}
	key := relabelFingerprint(rules)
	c.mu.Lock()
	compiled, ok := c.m[key]
	c.mu.Unlock()
	if !ok {
		for _, r := range rules {
			if r.Action != "keep" && r.Action != "drop" {
				continue // parse already restricted to keep/drop; belt and braces
			}
			// The ONE spelling of the anchored wrap and the empty-regex
			// default, shared with the metadata service's parse door
			// (internal/servicemonitors), which refuses an endpoint whose
			// regex this would fail on — so the two cannot disagree.
			re, err := kubemeta.CompileRelabelRegex(r.Regex)
			if err != nil {
				return nil, false, fmt.Errorf("metricRelabelings regex %q: %w", r.Regex, err)
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
			evicted = true
		}
		c.m[key] = compiled
		c.mu.Unlock()
	}
	return &relabelFilter{rules: compiled, last: make([]relabelLast, len(compiled))}, evicted, nil
}

// relabelFingerprint is a rule chain's cache key, and its INJECTIVITY is
// load-bearing: the cache is process-global, so two chains fingerprinting to
// one key would hand one endpoint the other's compiled filter and export
// series its own rule asked to drop. Length-prefixed via appendLP (the
// package's one injective-join rule), because a regex or source label may
// contain any delimiter byte. The source-label COUNT is part of the encoding —
// the parts are self-delimiting, but rule boundaries are not without it.
// TestRelabelFingerprintIsInjective pins it.
func relabelFingerprint(rules []kubemeta.RelabelRule) string {
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
	return string(fp)
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
		var matched bool
		if l := &f.last[i]; l.ok && string(l.val) == string(f.buf) {
			matched = l.matched
		} else {
			matched = r.re.Match(f.buf)
			if len(f.buf) <= maxRelabelMemoBytes {
				l.val = append(l.val[:0], f.buf...)
				l.ok, l.matched = true, matched
			} else {
				l.ok = false
			}
		}
		if r.keep && !matched {
			return false
		}
		if !r.keep && matched {
			return false
		}
	}
	return true
}
