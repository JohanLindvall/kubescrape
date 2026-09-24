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
//   - "did the decision BILL the rate budgets?" — bounded by the CAPACITY
//     (decisionCacheSize) alone. It outlives the verdict, because it is what
//     stops the re-decision from CHARGING the rateLimiting and composite budgets
//     a second time (tailsample.Trace.Charged). The re-decision still happens
//     and may answer differently — the bucket may have emptied meanwhile — but a
//     trace's late spans no longer shrink the budget available to genuinely new
//     traces.
//
//     What is remembered is the SPEND the evaluator reported
//     (tailsample.Decision.Charged), never the mere fact of a decision: a
//     rateLimiting policy that REFUSED a trace deducted nothing, and a
//     re-decision told otherwise would peek a budget the trace never paid and
//     admit its spans free — once per fresh window, for as long as the entry
//     lives.
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

	"github.com/JohanLindvall/kubescrape/internal/agent/tailsample"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// decisionCache remembers verdicts by trace id. Not safe for concurrent use: it
// lives under Buffer.mu.
//
// Everything is stored BY VALUE and nothing it stores holds a pointer: the map
// fills to its cap and stays there (100000 entries by default), and a map or
// slice whose element type carries no pointer is one the garbage collector
// never has to scan. Entries used to be a *decision each, carrying a time.Time
// (whose *Location is a pointer), which was one allocation per decision and a
// hundred thousand objects for every GC cycle to mark.
type decisionCache struct {
	max int
	ttl time.Duration
	m   map[pcommon.TraceID]decision
	// fifo is insertion order, for capacity eviction. A superseded entry is
	// not removed from the middle: its slot names the seq of the put that
	// wrote it, and a slot is LIVE only while the map entry for its id still
	// carries that seq (live). So a trace decided twice cannot have its second
	// verdict evicted by its first slot reaching the front.
	fifo []cacheSlot
	head int
	// seq numbers the puts; 64 bits, so it never wraps.
	seq uint64
	// base is the clock reading of the first put, the zero point decision.at
	// counts from. An offset from a reading of the CALLER's clock, rather than
	// a wall-clock integer, so the TTL arithmetic keeps time.Time's monotonic
	// reading (a stepped wall clock must not age every verdict at once) and
	// stays in whatever clock a test injects.
	base    time.Time
	hasBase bool
}

type decision struct {
	// charged is WHICH rate buckets the deciding evaluation actually SPENT
	// (see the two lifetimes above), not that it happened — per bucket, so a
	// re-decision Peeks only the buckets this trace paid.
	charged tailsample.ChargedMask
	// at is when the verdict was made, as an offset from decisionCache.base.
	at   time.Duration
	seq  uint64
	keep bool
}

// cacheSlot is one FIFO position: the trace and the put that wrote it.
type cacheSlot struct {
	id  pcommon.TraceID
	seq uint64
}

func newDecisionCache(limit int, ttl time.Duration) *decisionCache {
	return &decisionCache{max: limit, ttl: ttl, m: make(map[pcommon.TraceID]decision, min(limit, 1024))}
}

// age is how long ago d was decided, as of now.
func (c *decisionCache) age(d decision, now time.Time) time.Duration {
	return now.Sub(c.base) - d.at
}

// live reports whether s is still the slot of its trace's current entry.
func (c *decisionCache) live(s cacheSlot) bool {
	d, ok := c.m[s.id]
	return ok && d.seq == s.seq
}

// get returns the remembered verdict, if there is a live one. An entry past the
// TTL answers no — its trace is a new trace as far as the window goes — but it
// STAYS, because charged() still reads it: the spend it recorded outlives the
// verdict (see the two lifetimes above).
func (c *decisionCache) get(id pcommon.TraceID, now time.Time) (keep, ok bool) {
	d, ok := c.m[id]
	if !ok || c.age(d, now) >= c.ttl {
		return false, false
	}
	return d.keep, true
}

// charged reports which rate buckets a remembered decision for this trace
// SPENT, at any age. It is what a re-decision passes to the evaluator as
// Trace.Charged, so a bucket is neither billed twice for one trace nor
// skipped for one the trace never paid it.
func (c *decisionCache) charged(id pcommon.TraceID) tailsample.ChargedMask {
	return c.m[id].charged // the zero value (a miss) is "spent nothing"
}

// put remembers a verdict and whether producing it billed the rate budgets,
// evicting the oldest entry if the cache is full.
//
// The charge mask ACCUMULATES over the entry being replaced; it is not
// overwritten. A re-decision passes the remembered mask back as Trace.Charged,
// which makes charge() take its Peek arm for those buckets — so that
// evaluation spends nothing there and reports them unset BY CONSTRUCTION.
// Storing the new mask verbatim would forget the payment after exactly one
// re-decision, and the trace's THIRD window would bill the budgets a second
// time; the fact is meant to outlive the verdict and be bounded by the
// capacity alone (see the two lifetimes above). A mask that simply took the
// caller's value could not also express "the evaluator REFUSED this trace, so
// nothing was spent" — that case is the FIRST put for an id, where there is
// no earlier entry to accumulate over, so the two coexist only because this
// one ORs.
func (c *decisionCache) put(id pcommon.TraceID, keep bool, charged tailsample.ChargedMask, now time.Time) {
	if !c.hasBase {
		c.base, c.hasBase = now, true
	}
	if old, ok := c.m[id]; ok {
		charged |= old.charged
		// Deleting it is what retires its FIFO slot: the slot names old.seq,
		// which no entry carries any more, so it cannot evict the new one.
		delete(c.m, id)
	}
	if len(c.m) >= c.max {
		c.evict(now)
	}
	c.seq++
	c.m[id] = decision{keep: keep, charged: charged, at: now.Sub(c.base), seq: c.seq}
	c.fifo = append(c.fifo, cacheSlot{id: id, seq: c.seq})
	c.compact()
}

// evict drops the oldest entry to make room. It counts only when the verdict it
// takes was still LIVE: that is the cap being too small for the arrival pattern
// — a late span for that trace now starts a fresh window AND re-charges the rate
// budgets — whereas reclaiming an expired tombstone is the cache working.
func (c *decisionCache) evict(now time.Time) {
	for c.head < len(c.fifo) {
		s := c.fifo[c.head]
		c.head++
		d, ok := c.m[s.id]
		if !ok || d.seq != s.seq {
			continue // a superseded slot
		}
		delete(c.m, s.id)
		if c.age(d, now) < c.ttl {
			obs.TailSampleCacheEvicted.Inc()
		}
		return
	}
}

// compact drops the consumed prefix once it is more than half the slice, so the
// FIFO's memory tracks the live entries rather than every decision ever made.
func (c *decisionCache) compact() {
	// Drop leading dead slots. head is otherwise advanced ONLY by evict, which
	// runs only once the map is at its cap — so below the cap head stayed at 0,
	// the `c.head == 0` guard below returned immediately, and the FIFO grew by
	// one entry per re-decision forever. A bounded map behind an unbounded
	// slice: every late span that re-decided a trace leaked a slot for the
	// process' life.
	for c.head < len(c.fifo) && !c.live(c.fifo[c.head]) {
		c.head++
	}
	// A re-decided trace's dead slot is wherever it happened to sit, not
	// necessarily at the front, so leading-drop alone does not bound it.
	// Rebuild once the slice has drifted well past the live set — amortized
	// O(1) per put, and the only thing that reclaims interior slots while the
	// cache is under its cap.
	if len(c.fifo)-c.head > 2*len(c.m)+compactFloor {
		n := 0
		for _, s := range c.fifo[c.head:] {
			if c.live(s) {
				c.fifo[n] = s
				n++
			}
		}
		c.fifo = c.fifo[:n]
		c.head = 0
		return
	}
	if c.head == 0 || c.head < len(c.fifo)/2 {
		return
	}
	n := copy(c.fifo, c.fifo[c.head:])
	c.fifo = c.fifo[:n]
	c.head = 0
}

// compactFloor keeps the rebuild above from running on a nearly-empty cache,
// where 2*len(m) is a handful of entries and the scan would fire constantly.
const compactFloor = 64

// len reports the entry count (for tests).
func (c *decisionCache) len() int { return len(c.m) }
