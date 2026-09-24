package store

// Tombstone expiry: the pending list every stamp is recorded on, and the sweep
// that walks it on Run's ticker so its cost is what expired rather than what
// the store holds.

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// pendingExpiry is one stamped tombstone waiting to be swept: a pod record or
// a container entry.
//
// It is a HINT, never the truth. The truth is the expireAt on the record or
// entry itself, which the sweep re-reads before removing anything: a tombstone
// can be resurrected (UpsertPod clears the stamp), re-stamped later (a
// replayed DeletePod), replaced (a restart re-indexes the ID) or already gone
// (its pod swept it), and each of those simply makes the hint a no-op. The
// cost of a stale hint is one map probe.
//
// isPod says which map to look in, rather than the emptiness of uid or id: a
// record keyed by an empty UID is degenerate but perfectly representable, and
// a discriminator that reads it as a container id would leave that record's
// tombstone in the store forever.
type pendingExpiry struct {
	when  time.Time
	uid   types.UID
	id    string
	isPod bool
}

// stampLocked records a tombstone that sweep must revisit. EVERY assignment to
// a record's or an entry's expireAt goes through here (there are exactly two
// callers, deletePodLocked and expireEntryLocked); one that did not would be a
// tombstone nothing ever reclaims, since the sweep no longer scans the store
// looking for them.
//
// The list is kept in stamp order, which is also expiry order: every stamp is
// now+ttl for one ttl fixed at construction, and now does not go backwards
// (time.Now carries a monotonic reading; the injectable test clock only
// advances). sweep therefore stops at the first unexpired entry instead of
// walking the rest. Were that ever violated, the entries behind the head would
// be swept in a later window rather than leak — a delay, not a loss.
func (s *Store) stampLocked(p pendingExpiry) {
	s.pending = append(s.pending, p)
}

// expired reports whether a tombstone stamp has lapsed. A zero expireAt is
// the live marker (no tombstone), never "expired at the epoch"; a stamped
// entry expires strictly AFTER its instant, so an injected test clock sitting
// exactly on the stamp still resolves. Every expiry decision — the lookups'
// present-but-unswept checks and sweep itself — goes through here so they
// cannot disagree on either edge.
func expired(expireAt, now time.Time) bool {
	return !expireAt.IsZero() && now.After(expireAt)
}

// sweep removes expired tombstones. Run calls it periodically; tests call it
// directly.
//
// It walks the PENDING list, not the store. sweep holds the exclusive write
// lock, so every container lookup, every pod lookup and every node-targets
// request waits behind it, and the informer's own upserts queue up too — the
// cost has to be proportional to what expired. Scanning byContainer and pods
// instead made it proportional to the whole store whether or not anything was
// due: at 20k pods a sweep with NOTHING to remove measured 0.77-1.02 ms
// against 73-99 ns now, and it runs on a ticker of ttl/4 clamped to
// [5s, 60s] — so a short -cache-ttl paid that every five seconds for nothing.
// Both figures are indicative; the durable claim is
// TestSweepCostDoesNotScaleWithTheStore, which measures the SHAPE.
func (s *Store) sweep() {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()

	n := 0
	for ; n < len(s.pending); n++ {
		p := s.pending[n]
		if !expired(p.when, now) {
			break // stamped in expiry order: nothing behind this is due either
		}
		if p.isPod {
			s.sweepPodLocked(p.uid, now)
		} else {
			s.sweepContainerLocked(p.id, now)
		}
	}
	if n > 0 {
		// Something was actually removed. A sweep that finds nothing due — the
		// common case, on a ticker as short as every five seconds — must leave
		// the token alone, or the memo it guards would lapse on that ticker
		// rather than on change. And it stamps NO node: a tombstone left byNode
		// in deletePodLocked, so removing one cannot change any PodsOnNode
		// answer, and stamping would lapse node memos on the same ticker.
		defer s.bumpLocked()
	}
	if n == 0 {
		return
	}
	// Compact rather than reslice. A reslice walks the backing array forward
	// until its capacity runs out and then reallocates, and in the steady state
	// of one stamp arriving per sweep that is a fresh array every time.
	rest := copy(s.pending, s.pending[n:])
	clear(s.pending[rest:]) // the moved-from tail keeps its strings alive
	s.pending = s.pending[:rest]
	s.pending = shrinkPending(s.pending)
}

// shrinkPending gives back a pending array that a burst grew and occupancy no
// longer justifies.
//
// A rollout of a 5000-pod deployment stamps three tombstones per pod and then
// drains them inside one TTL. Reusing the array is the point of compacting, but
// keeping the PEAK of it for the process lifetime is not — and releasing it only
// when the list drains to ZERO was not enough, because under steady churn it
// never does: a single fresh stamp per tick kept an 11,136-slot array (~696 KiB)
// resident behind ten live entries for as long as the churn lasted. So the test
// is OCCUPANCY: past maxIdlePendingStamps, an array less than a quarter full is
// copied into one sized max(2*len, maxIdlePendingStamps). That copies fewer than
// cap/4 entries, and the new array is at least half full, so a shrink cannot be
// undone by the next append and the two cannot thrash — each shrink at least
// halves the capacity, so the copying over a whole drain is geometric.
//
// The pending array is the store's largest burst-retained structure, not its
// only one: Go maps never shrink either, so pods and byContainer keep the bucket
// arrays of their peak too (~29 B per slot against 64 B per stamp here). Those
// are the price of map lookups; this one had no such excuse.
func shrinkPending(p []pendingExpiry) []pendingExpiry {
	c := cap(p)
	if c <= maxIdlePendingStamps || len(p) >= c/4 {
		return p
	}
	if len(p) == 0 {
		return nil
	}
	return append(make([]pendingExpiry, 0, max(2*len(p), maxIdlePendingStamps)), p...)
}

// maxIdlePendingStamps is the pending capacity sweep always keeps for reuse:
// 1024 stamps, ~64 KB, comfortably more than steady-state churn between two
// ticks. Above it the array is sized by occupancy (shrinkPending), so a burst's
// high-water mark is not resident forever — drained or not.
const maxIdlePendingStamps = 1024

// sweepContainerLocked removes one container entry if the stamp that listed it
// is still the entry's own and has lapsed. A restart that re-indexed the ID
// installed a fresh entry with no stamp, and this must not remove that.
func (s *Store) sweepContainerLocked(id string, now time.Time) {
	if e := s.byContainer[id]; e != nil && expired(e.expireAt, now) {
		delete(s.byContainer, id)
	}
}

// sweepPodLocked retires one lapsed pod tombstone and every index that still
// points at it.
func (s *Store) sweepPodLocked(uid types.UID, now time.Time) {
	rec := s.pods[uid]
	if rec == nil || !expired(rec.expireAt, now) {
		return // already gone, resurrected, or re-stamped by a replayed delete
	}
	for _, ip := range recordAddresses(rec) {
		s.dropClaimantLocked(ip, rec)
	}
	s.removeRecordLocked(rec, uid)
}

// Run sweeps expired tombstones until ctx is done.
func (s *Store) Run(ctx context.Context) {
	interval := min(max(s.ttl/4, 5*time.Second), time.Minute)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sweep()
		}
	}
}

func (s *Store) expireEntryLocked(id string, e *containerEntry) {
	if s.ttl <= 0 {
		delete(s.byContainer, id)
		return
	}
	e.expireAt = s.now().Add(s.ttl)
	s.stampLocked(pendingExpiry{when: e.expireAt, id: id})
}
