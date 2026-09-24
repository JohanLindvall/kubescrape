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
//
// Three shapes live here, and each decides without logging: Throttle for one
// keyless condition, Table for a condition that varies by key, and Outage for a
// RUN of failures that must open loud, restate on a schedule and say when it
// ended.
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
//
// The interval is measured on the MONOTONIC clock, as Table's is (it compares
// time.Time values, which carry the monotonic reading). This used to compare
// wall-clock UnixNano stamps, which carry none: a backwards wall-clock step of
// D — an NTP/chrony step at boot, a VM restored from a snapshot — made now-last
// negative and silenced every Throttle that had already fired for D plus its
// interval, i.e. every re-warn this repo gates on one. A unit test cannot step
// the wall clock, so this paragraph is the pin: do not go back to time.Now()
// arithmetic here.
type Throttle struct {
	// last is the claiming call's offset from epoch, plus one so that zero
	// keeps meaning "never fired".
	last atomic.Int64
}

// epoch is the process-local origin Throttle measures from. time.Since on a
// Time carrying a monotonic reading reads only the runtime's nanotime, which
// no wall-clock step can move — and it is a cheaper read than time.Now's.
var epoch = time.Now()

// Allow reports whether the caller may log now, and claims the slot if so.
func (t *Throttle) Allow(interval time.Duration) bool {
	now := int64(time.Since(epoch)) + 1
	return t.claim(t.last.Load(), now, interval)
}

// AllowAt is Allow on a clock the CALLER read, for a holder that already keeps
// an injectable clock (the store.now pattern) — so the cadence of the line it
// throttles can be pinned by a test without sleeping, which Allow's own clock
// read cannot offer. now is measured from the same epoch Allow uses, and a
// time.Now() reading carries the monotonic clock, so a production caller
// passing one keeps Allow's immunity to wall-clock steps. Use one or the other
// on a given Throttle, not both: a test clock and the real one do not agree.
func (t *Throttle) AllowAt(now time.Time, interval time.Duration) bool {
	return t.claim(t.last.Load(), int64(now.Sub(epoch))+1, interval)
}

// stampAt records a fire at now without asking — for a caller that has already
// decided to log whatever the throttle says (Outage's first-of-run line), so
// the next interval is measured from the line that was actually written. Not
// for concurrent use against Allow: it overwrites rather than racing for the
// slot, which is right only for a holder that serialises its callers (Outage).
func (t *Throttle) stampAt(now time.Time) { t.last.Store(int64(now.Sub(epoch)) + 1) }

// claim is Allow with the clock reading and the observed last passed in, so
// the rule can be tested without a clock. A never-fired throttle (last == 0)
// fires at once whatever now is — without that arm, a Throttle consulted within
// its interval of PROCESS START would refuse its first fire, since now is an
// offset from epoch rather than from 1970. Otherwise the interval must have
// elapsed, and the CompareAndSwap is the loser rule: a caller whose swap fails
// raced a winner that is about to log the same condition, so it stays silent.
func (t *Throttle) claim(last, now int64, interval time.Duration) bool {
	if last != 0 && now-last < int64(interval) {
		return false
	}
	return t.last.CompareAndSwap(last, now)
}

// Outage decides how to report a RUN of failures: the transition shape every
// "the destination is failing" line in this repo takes. The FIRST failure of a
// run is always loud, the repeats are restated at most once per interval (a
// Throttle), and the recovery is reported once, carrying what the run cost.
//
// It exists because that shape was hand-rolled about ten times, and the rule a
// Throttle alone cannot express is the one the copies kept losing: FIRST OF
// RUN. A Throttle's zero value fires once per PROCESS, so a copy that consulted
// only the throttle opened a second outage — one starting within the interval
// of the previous run's last loud line — at Debug or not at all, directly
// under the previous run's "recovered" line, which reads as a condition that
// got better and stayed better. The run's own state is what says a failure
// opens a run; the opening failure still CLAIMS the throttle slot, so the
// second failure of the run, moments later, is not an immediate second loud
// line saying the same thing.
//
// Outage only decides; the caller logs, choosing the message, the level and
// the attributes — the split Table makes. The conventional keys for what it
// counts are `failures` (Failures) and `outage` (Lasted), see internal/cli's
// key vocabulary.
//
// It is NOT safe for concurrent use: plain fields, so a holder shared by
// goroutines guards it with a mutex of its own (usually the one already
// guarding the state the failure is about — decide under it, log after
// releasing it). The zero value is ready.
//
// Time is the CALLER's reading, for the reason Throttle.AllowAt exists: a
// holder that keeps an injectable clock pins the cadence in a test without
// sleeping, and a production caller passing time.Now() keeps the monotonic
// reading the throttle measures on.
type Outage struct {
	failures int
	since    time.Time
	warn     Throttle
}

// Fail records one failure at now. first is true on the failure that OPENS a
// run; loud is true when the caller should log at its loud level — always on
// the first failure of a run, and after that at most once per every. A repeat
// that is not loud is the caller's to log at Debug or not at all.
//
// every == 0 makes every failure loud: the shape of a producer whose own
// back-off already spaces its attempts, which uses Outage only for the count
// and the duration.
func (o *Outage) Fail(now time.Time, every time.Duration) (first, loud bool) {
	o.failures++
	if o.failures == 1 {
		// The opening line is loud whatever the throttle says, so it claims
		// the slot UNCONDITIONALLY: the repeats are measured from the line
		// actually written. Asking the throttle first claimed only when the
		// previous run's slot had expired, so a run opening inside it wrote a
		// loud line that claimed nothing, and its second failure went loud
		// again the moment the OLD slot lapsed — seconds after the opening.
		o.since = now
		o.warn.stampAt(now)
		return true, true
	}
	return false, o.warn.AllowAt(now, every)
}

// Recover ends the open run at now. ok is true exactly once per run — on the
// first success after a failure — with how many failures the run had and how
// long it lasted (rounded as Lasted rounds); a success outside a run reports
// ok false and changes nothing.
//
// The throttle is deliberately NOT reset: the next run's first failure is loud
// anyway, and its claim is what bounds a flapping condition's repeats.
func (o *Outage) Recover(now time.Time) (failures int, lasted time.Duration, ok bool) {
	if o.failures == 0 {
		return 0, 0, false
	}
	failures, lasted = o.failures, o.Lasted(now)
	o.failures, o.since = 0, time.Time{}
	return failures, lasted, true
}

// Failing reports whether a run is open: the last outcome recorded was a
// failure.
func (o *Outage) Failing() bool { return o.failures > 0 }

// Failures is how many failures the open run has had, this one included, and
// 0 outside a run.
func (o *Outage) Failures() int { return o.failures }

// Lasted is how long the open run has lasted at now, rounded to a second — the
// rounding every `outage=` in this repo's log lines uses — and 0 outside a run.
func (o *Outage) Lasted(now time.Time) time.Duration {
	if o.failures == 0 {
		return 0
	}
	return now.Sub(o.since).Round(time.Second)
}

// Table is a bounded per-key log throttle. The zero value is not usable; call
// New. It is safe for concurrent use.
type Table struct {
	mu        sync.Mutex
	seen      map[string]time.Time
	saturated bool
	// reclaimAt is the earliest instant at which a reclaim sweep of a FULL
	// table can free a slot: the oldest surviving entry's expiry as of the last
	// sweep, or zero for "unknown — sweep". Without it a saturated table walked
	// the whole map under the lock on every refused NEW key, and a refused key
	// is never inserted, so it paid the walk again on its next call — linear in
	// the cap, measured at ~170x an existing key's call at 1024 keys — on a
	// table the metadata service consults per pod per unresolved endpoint in
	// the node-targets derivation: a tenant creating enough broken monitors
	// multiplied that derivation's cost and serialised it on this mutex.
	//
	// It is never LATE, which is what makes skipping the sweep safe: entries
	// are deleted only by the sweep; every later insert is stamped at or after
	// the sweep (now is read under the lock, so stamps are non-decreasing in
	// lock order) and so expires no earlier than the oldest survivor; and an
	// existing key's refresh only moves its own expiry later, which costs at
	// most one sweep that frees nothing and re-derives the bound.
	reclaimAt time.Time
	// sweeps counts reclaim sweeps (tests).
	sweeps int

	max    int
	window time.Duration
	now    func() time.Time
}

// New creates a table holding at most limit keys.
//
// window is how long a key stays suppressed after it logs. ZERO means "once per
// process": the key is never allowed again, which is right for a complaint
// about static configuration (the scraper's invalid-interval warnings — the
// operator must edit a CR, and until they do there is nothing new to say).
// A POSITIVE window re-warns at that cadence, which is right for a condition an
// operator may fix out of band and want confirmation about (the metadata
// service's unresolvable Secret refs).
func New(limit int, window time.Duration) *Table {
	if limit < 1 {
		limit = 1
	}
	return &Table{seen: make(map[string]time.Time), max: limit, window: window, now: time.Now}
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
	t.mu.Lock()
	defer t.mu.Unlock()
	// Read under the lock so stamps are ordered with the lock: reclaimAt's
	// never-late argument needs every insert after a sweep to carry a stamp no
	// earlier than that sweep's.
	now := t.now()

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
		//
		// Only when something CAN have expired (reclaimAt): a sweep that is
		// bound to free nothing is a whole-map walk under the lock, paid by
		// every refused new key.
		if t.window > 0 && !now.Before(t.reclaimAt) {
			t.reclaimLocked(now)
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

// reclaimLocked deletes every entry past its window and records when the
// oldest survivor will be (reclaimAt). t.mu must be held.
func (t *Table) reclaimLocked(now time.Time) {
	t.sweeps++
	var oldest time.Time
	for k, at := range t.seen {
		if now.Sub(at) >= t.window {
			delete(t.seen, k)
			continue
		}
		if oldest.IsZero() || at.Before(oldest) {
			oldest = at
		}
	}
	t.reclaimAt = time.Time{}
	if !oldest.IsZero() {
		t.reclaimAt = oldest.Add(t.window)
	}
}

// Len reports how many keys are currently held (tests).
func (t *Table) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.seen)
}
