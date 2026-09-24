package attrs

import (
	"sync"
	"sync/atomic"
)

// genCache is a two-generation cache keyed by a string, for values whose
// computation costs orders of magnitude more than the lookup: a compiled
// regexp, a filter verdict, a prefixed label key. Every one of them is
// evaluated once per RESOURCE BUILT, and a split path builds one resource per
// described object.
//
// The cap is an EVICTION, not an admission stop. Both bound memory; only one
// bounds the work. Refusing to admit past the cap turns a working set LARGER
// than the cap into a recompute on every single call — measured at ~30µs
// against ~25ns for a cached pattern — on exactly the data-derived input the
// cap exists for (a template composing a regex from a label value). At the cap
// the current generation is retired to the previous one and a fresh generation
// starts; a lookup checks both and PROMOTES a previous-generation hit, so
// entries in continuous use survive a rotation and memory stays bounded at 2x
// the cap.
//
// A COUNT bounds memory only while entries are alike in size. Where one entry
// can cost a million times another (a compiled regexp: a few hundred bytes for
// a route pattern, tens of MB for a repeat-heavy one) the cache is also given a
// WEIGHT budget (newWeightedGenCache): a generation rotates at whichever of the
// two it reaches first, so memory is bounded at about 2x the weight budget
// (plus one entry's weight per generation, the one that crossed it) however
// the entries are sized.
//
// Hits are lock-free (one atomic pointer load plus a sync.Map read); the mutex
// is taken only to admit an entry. A nil *genCache is a no-op cache.
type genCache[V any] struct {
	mu   sync.Mutex
	cur  atomic.Pointer[sync.Map]
	prev atomic.Pointer[sync.Map]
	n    int // entries admitted into cur; guarded by mu
	max  int
	// weigh, when set, is an entry's cost against maxWeight; w is the weight
	// admitted into cur (guarded by mu).
	weigh     func(V) int
	w         int
	maxWeight int
}

func newGenCache[V any](limit int) *genCache[V] {
	c := &genCache[V]{max: limit}
	c.cur.Store(&sync.Map{})
	return c
}

// newWeightedGenCache is newGenCache that also rotates once the weight
// admitted into the current generation reaches maxWeight.
func newWeightedGenCache[V any](limit, maxWeight int, weigh func(V) int) *genCache[V] {
	c := newGenCache[V](limit)
	c.weigh, c.maxWeight = weigh, maxWeight
	return c
}

// load returns a cached value. A hit in the previous generation is promoted
// into the current one, which is what keeps a hot entry cached across
// rotations.
func (c *genCache[V]) load(key string) (V, bool) {
	var zero V
	if c == nil {
		return zero, false
	}
	if v, ok := c.cur.Load().Load(key); ok {
		return v.(V), true
	}
	if prev := c.prev.Load(); prev != nil {
		if v, ok := prev.Load(key); ok {
			val := v.(V)
			c.store(key, val)
			return val, true
		}
	}
	return zero, false
}

// store admits key, rotating generations at the cap.
func (c *genCache[V]) store(key string, v V) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.cur.Load()
	if _, loaded := cur.LoadOrStore(key, v); loaded {
		return
	}
	c.n++
	if c.weigh != nil {
		c.w += c.weigh(v)
	}
	if c.n >= c.max || (c.weigh != nil && c.w >= c.maxWeight) {
		c.prev.Store(cur)
		c.cur.Store(&sync.Map{})
		c.n, c.w = 0, 0
	}
}
