package route

import (
	"maps"
	"path"
	"strings"
	"sync"
	"sync/atomic"
)

// maxNamespaceMemo bounds the namespace→destination memo. A node sees the
// namespaces of its own pods plus, through a KSM split, those a cluster-wide
// exporter describes: hundreds, rarely more. Past the bound the memo stops
// LEARNING and a new name takes the glob scan it always took, so the bound
// costs speed, never an answer. It also bounds the one-time cost of learning,
// since each new name publishes a copy of the map (at most ~131k entry copies
// over the process' life).
const maxNamespaceMemo = 512

// maxMemoNamespaceBytes is the longest name the memo keeps: a Kubernetes
// namespace is a DNS label, at most 63 bytes. A longer value cannot be a real
// namespace (a transform script, or a sender the enricher could not resolve,
// can put anything in the attribute), so it is matched and not remembered —
// the memo holds namespaces, not whatever a payload chose to carry.
const maxMemoNamespaceBytes = 63

// nsMemo remembers match's namespace verdicts. The verdict is a pure function
// of the name for a Router, whose destinations are fixed in New, and the same
// few names recur on every resource of every export — while the scan they
// replace is a path.Match per pattern per route, linear in the route count and
// re-done for each resource, and for a name no route claims (the commonest
// answer) it walks EVERY pattern.
//
// Readers take one atomic load and a map probe, no lock and no allocation:
// the map behind the pointer is never written once published. A miss
// publishes a copy carrying the new entry under mu, which serialises only the
// learning.
type nsMemo struct {
	m  atomic.Pointer[map[string]int32]
	mu sync.Mutex
}

// get returns the remembered destination for name.
func (c *nsMemo) get(name string) (int, bool) {
	m := c.m.Load()
	if m == nil {
		return 0, false
	}
	idx, ok := (*m)[name]
	return int(idx), ok
}

// put remembers name's destination, within the bounds above.
func (c *nsMemo) put(name string, idx int) {
	if len(name) > maxMemoNamespaceBytes {
		return
	}
	if m := c.m.Load(); m != nil && len(*m) >= maxNamespaceMemo {
		return // full: checked before the lock, so a saturated memo costs a load
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.m.Load()
	var next map[string]int32
	if cur != nil {
		if _, ok := (*cur)[name]; ok || len(*cur) >= maxNamespaceMemo {
			return // a racing miss published it first, or it filled meanwhile
		}
		next = make(map[string]int32, len(*cur)+1)
		maps.Copy(next, *cur)
	} else {
		next = make(map[string]int32, 1)
	}
	// Cloned: name aliases the payload's attribute value, and a memo entry
	// outlives every payload — a retained substring would pin whatever buffer
	// the value was decoded from.
	next[strings.Clone(name)] = int32(idx)
	c.m.Store(&next)
}

// namespaceDest is decide's namespace half: the first route with a glob
// matching name, or -1 for the default chain.
func (r *Router) namespaceDest(name string) int {
	if idx, ok := r.nsMemo.get(name); ok {
		return idx
	}
	idx, _ := r.globNamespace(name)
	r.nsMemo.put(name, idx)
	return idx
}

// globNamespace is the scan the memo stands in front of, returning the route
// and the glob that claimed name (-1 and "" for none). It is the ONLY
// derivation: the memo stores its answers and nothing else, and a narrated
// decision reads it directly for the pattern the memo does not keep.
func (r *Router) globNamespace(name string) (idx int, pat string) {
	for i, d := range r.dests {
		for _, p := range d.Namespaces {
			if ok, _ := path.Match(p, name); ok {
				return i, p
			}
		}
	}
	return -1, ""
}
