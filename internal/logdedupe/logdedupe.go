// Package logdedupe is the one implementation of "throttle a log line that
// would otherwise repeat forever".
//
// Both binaries produce complaints about a STATE rather than an event: a
// monitor field the scraper cannot honour, a Secret ref the metadata service
// cannot resolve. The condition persists, the code that notices it runs every
// cycle for every target on every node, and an unthrottled line is therefore a
// permanent flood proportional to fleet size — while the useful information in
// it is one line.
//
// It was written twice, and the copies disagreed on the decision that matters:
// WHAT TO DO WHEN THE TABLE FILLS. The scraper suppresses further keys and says
// so once. The metadata service's copy — written later, without reference to
// the first — aged entries out and then CLEARED the map, which is the
// intuitive choice and is wrong:
//
//	at cap+1 distinct keys the table re-fills, overflows and clears again, so
//	every key logs on EVERY cycle, forever. That is worse than the unbounded
//	map the cap was added to replace, and it is silent — with the suppression
//	state gone, an operator cannot tell a genuinely new failing key from a
//	cleared table.
//
// So the rule is decided here, once: **saturation suppresses, it never
// clears**. An operator holding max distinct warnings has plenty to act on, and
// the saturation notice tells them the list is truncated.
package logdedupe

import (
	"sync"
	"sync/atomic"
	"time"
)

// Throttle gates a KEYLESS repeating warning to at most once per interval,
// across concurrent callers, without a mutex: one atomic and a
// CompareAndSwap. The zero value is ready (first Allow fires immediately).
//
// The subtle half — the part three hand-rolled copies (the ingest enricher's
// rejected-peer warn, the resharder's failed-hop warn, tailbuffer's
// failed-export warn) each had to re-derive — is the LOSER rule: a caller
// whose CompareAndSwap loses raced a winner that is about to log the same
// condition, so it must stay silent. Logging on a lost race is exactly the
// duplicate the throttle exists to prevent.
//
// Table (below) is the keyed sibling for conditions that vary by target;
// Throttle is for one condition per holder, where a map would be waste.
type Throttle struct{ last atomic.Int64 }

// Allow reports whether the caller may log now, and claims the slot if so.
func (t *Throttle) Allow(interval time.Duration) bool {
	now := time.Now().UnixNano()
	last := t.last.Load()
	return now-last >= int64(interval) && t.last.CompareAndSwap(last, now)
}

// Table is a bounded per-key log throttle. The zero value is not usable; call
// New. It is safe for concurrent use.
type Table struct {
	mu        sync.Mutex
	seen      map[string]time.Time
	saturated bool

	max    int
	window time.Duration
	now    func() time.Time
}

// New creates a table holding at most max keys.
//
// window is how long a key stays suppressed after it logs. ZERO means "once per
// process": the key is never allowed again, which is right for a complaint
// about static configuration (the scraper's invalid-interval warnings — the
// operator must edit a CR, and until they do there is nothing new to say).
// A POSITIVE window re-warns at that cadence, which is right for a condition an
// operator may fix out of band and want confirmation about (the metadata
// service's unresolvable Secret refs).
func New(max int, window time.Duration) *Table {
	if max < 1 {
		max = 1
	}
	return &Table{seen: make(map[string]time.Time), max: max, window: window, now: time.Now}
}

// Allow reports whether key may log now, and records that it did.
//
// saturated is true on the ONE call that discovers the table is full, so the
// caller can emit a single "further warnings are suppressed" line. It is never
// true again for the life of the table — including after a reclaim sweep frees
// slots and the table fills a second time, which is therefore silent. That is
// deliberate but worth knowing: the notice says "the list you are reading is
// truncated", not "a truncation just happened". Unlike the clear() this package
// exists to prevent, nothing extra is LOGGED as a result — the second
// truncation suppresses quietly rather than re-arming a flood.
//
// The caller logs; this type only decides. That split keeps the message, its
// level and its attributes at the call site, where they belong.
func (t *Table) Allow(key string) (allow, saturated bool) {
	now := t.now()
	t.mu.Lock()
	defer t.mu.Unlock()

	if last, ok := t.seen[key]; ok {
		// A key already in the table is suppressed until its window elapses;
		// with no window, forever. Note this returns BEFORE the cap check, so a
		// full table never stops an existing key from re-warning on schedule —
		// only new keys are refused.
		if t.window <= 0 || now.Sub(last) < t.window {
			return false, false
		}
		t.seen[key] = now
		return true, false
	}

	if len(t.seen) >= t.max {
		// Reclaim what has genuinely expired first — only possible with a
		// window, and it is honest bookkeeping rather than the clear() this
		// package exists to prevent: an entry past its window would have been
		// allowed to log anyway.
		if t.window > 0 {
			for k, at := range t.seen {
				if now.Sub(at) >= t.window {
					delete(t.seen, k)
				}
			}
		}
		if len(t.seen) >= t.max {
			// Suppress. Do NOT insert (that would evict nothing and grow past
			// the cap) and do NOT clear.
			first := !t.saturated
			t.saturated = true
			return false, first
		}
	}
	t.seen[key] = now
	return true, false
}

// Len reports how many keys are currently held (tests).
func (t *Table) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.seen)
}
