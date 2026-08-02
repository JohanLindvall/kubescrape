package tailbuffer

// The decision cache: what a span arriving AFTER its trace was judged is
// measured against.
//
// Spans keep arriving after a decision — a slow process's last span, a sender's
// retry, a span held behind a re-shard hop — and there are only three things to
// do with one: apply the verdict its trace already got, start a new window for
// it, or drop it blind. The first is the only one that keeps a trace whole, so
// the verdict is remembered for a while.
//
// It is bounded in BOTH dimensions, because trace ids come off the wire and a
// map keyed by them is a map keyed by whatever the cluster emits: by ENTRY COUNT
// (decisionCacheSize) and by AGE (decisionCacheTTL). Past either, the trace is
// unknown again and a span for it starts a fresh window — which may decide the
// trace a SECOND time, and a second decision re-charges rateLimiting and
// composite (tailsample.Decide documents that; every other policy is a pure
// function of the trace and answers identically). That is the cost of the bound,
// and it is why an eviction under the size cap is counted while a TTL expiry is
// not: the first says the cap is too small for the arrival pattern, the second
// is the cache working as configured.

import (
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// decisionCache remembers verdicts by trace id. Not safe for concurrent use: it
// lives under Buffer.mu.
type decisionCache struct {
	max int
	ttl time.Duration
	m   map[pcommon.TraceID]*decision
	// fifo is insertion order, for TTL expiry and for capacity eviction. Entries
	// are POINTERS and a superseded one is marked stale rather than removed from
	// the middle, so a trace decided twice cannot have its second verdict evicted
	// by its first slot reaching the front.
	fifo []*decision
	head int
}

type decision struct {
	id    pcommon.TraceID
	keep  bool
	at    time.Time
	stale bool
}

func newDecisionCache(max int, ttl time.Duration) *decisionCache {
	return &decisionCache{max: max, ttl: ttl, m: make(map[pcommon.TraceID]*decision, min(max, 1024))}
}

// get returns the remembered verdict, if there is a live one.
func (c *decisionCache) get(id pcommon.TraceID, now time.Time) (keep, ok bool) {
	d, ok := c.m[id]
	if !ok {
		return false, false
	}
	if now.Sub(d.at) >= c.ttl {
		// Expired: drop it here rather than waiting for the FIFO to reach it, so
		// a lookup never answers from a verdict older than the TTL.
		delete(c.m, id)
		d.stale = true
		return false, false
	}
	return d.keep, true
}

// put remembers a verdict, expiring what has aged out and evicting the oldest
// live entry if the cache is full.
func (c *decisionCache) put(id pcommon.TraceID, keep bool, now time.Time) {
	if old, ok := c.m[id]; ok {
		old.stale = true // its FIFO slot must not evict the new entry
	}
	c.expire(now)
	if len(c.m) >= c.max {
		c.evict()
	}
	d := &decision{id: id, keep: keep, at: now}
	c.m[id] = d
	c.fifo = append(c.fifo, d)
	c.compact()
}

// expire drops entries older than the TTL from the front. It stops at the first
// live one: the FIFO is insertion-ordered and every entry has the same TTL.
func (c *decisionCache) expire(now time.Time) {
	for c.head < len(c.fifo) {
		d := c.fifo[c.head]
		if d.stale {
			c.head++
			continue
		}
		if now.Sub(d.at) < c.ttl {
			return
		}
		delete(c.m, d.id)
		d.stale = true
		c.head++
	}
}

// evict drops the oldest live entry to make room, and counts it: a late span for
// that trace will now start a fresh window instead of following its decision.
func (c *decisionCache) evict() {
	for c.head < len(c.fifo) {
		d := c.fifo[c.head]
		c.head++
		if d.stale {
			continue
		}
		delete(c.m, d.id)
		d.stale = true
		obs.TailSampleCacheEvicted.Inc()
		return
	}
}

// compact drops the consumed prefix once it is more than half the slice, so the
// FIFO's memory tracks the live entries rather than every decision ever made.
func (c *decisionCache) compact() {
	if c.head == 0 || c.head < len(c.fifo)/2 {
		return
	}
	n := copy(c.fifo, c.fifo[c.head:])
	clear(c.fifo[n:])
	c.fifo = c.fifo[:n]
	c.head = 0
}

// len reports the live entry count (for tests).
func (c *decisionCache) len() int { return len(c.m) }
