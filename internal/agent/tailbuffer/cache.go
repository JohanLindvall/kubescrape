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
// # Two lifetimes, one entry
//
// An entry answers two different questions, and they expire differently:
//
//   - "what was the verdict?" — bounded by AGE (decisionCacheTTL). Past it, a
//     straggler is indistinguishable from a new trace and gets a fresh window,
//     which is the semantic this knob buys.
//   - "have we decided this trace before?" — bounded by the CAPACITY
//     (decisionCacheSize) alone. It outlives the verdict, because it is what
//     stops the re-decision from CHARGING the rateLimiting and composite budgets
//     a second time (tailsample.Trace.Charged). The re-decision still happens
//     and may answer differently — the bucket may have emptied meanwhile — but a
//     trace's late spans no longer shrink the budget available to genuinely new
//     traces.
//
// The map is therefore bounded by decisionCacheSize and nothing else: it fills
// to the cap and evicts the oldest, where before a quiet shard's entries also
// aged out. That is the knob meaning what it says rather than sometimes less
// (~100 B an entry, so the 100000 default is ~10 MB at full occupancy), and it
// is why an eviction is counted only when it takes a verdict that was still
// LIVE: evicting an expired tombstone is the cache reclaiming space, evicting a
// live verdict is the cap being too small for the arrival pattern. Past the
// capacity the trace is unknown again in both senses, and a span for it starts a
// fresh window that DOES re-charge — the residual cost of a bounded memory, and
// exactly what the eviction counter is for.

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
	// fifo is insertion order, for capacity eviction. Entries are POINTERS and a
	// superseded one is marked stale rather than removed from the middle, so a
	// trace decided twice cannot have its second verdict evicted by its first
	// slot reaching the front.
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

// get returns the remembered verdict, if there is a live one. An entry past the
// TTL answers no — its trace is a new trace as far as the window goes — but it
// STAYS, because seen still needs it.
func (c *decisionCache) get(id pcommon.TraceID, now time.Time) (keep, ok bool) {
	d, ok := c.m[id]
	if !ok || now.Sub(d.at) >= c.ttl {
		return false, false
	}
	return d.keep, true
}

// seen reports whether this trace has been decided before, at any age. It is
// what a re-decision passes to the evaluator as Trace.Charged, so the rate
// budgets are not billed twice for one trace.
func (c *decisionCache) seen(id pcommon.TraceID) bool {
	_, ok := c.m[id]
	return ok
}

// put remembers a verdict, evicting the oldest entry if the cache is full.
func (c *decisionCache) put(id pcommon.TraceID, keep bool, now time.Time) {
	if old, ok := c.m[id]; ok {
		old.stale = true // its FIFO slot must not evict the new entry
		delete(c.m, id)
	}
	if len(c.m) >= c.max {
		c.evict(now)
	}
	d := &decision{id: id, keep: keep, at: now}
	c.m[id] = d
	c.fifo = append(c.fifo, d)
	c.compact()
}

// evict drops the oldest entry to make room. It counts only when the verdict it
// takes was still LIVE: that is the cap being too small for the arrival pattern
// — a late span for that trace now starts a fresh window AND re-charges the rate
// budgets — whereas reclaiming an expired tombstone is the cache working.
func (c *decisionCache) evict(now time.Time) {
	for c.head < len(c.fifo) {
		d := c.fifo[c.head]
		c.head++
		if d.stale {
			continue
		}
		delete(c.m, d.id)
		d.stale = true
		if now.Sub(d.at) < c.ttl {
			obs.TailSampleCacheEvicted.Inc()
		}
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

// len reports the entry count (for tests).
func (c *decisionCache) len() int { return len(c.m) }
