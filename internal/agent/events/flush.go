package events

// The batch's way out: the count trigger and the paced retry (flushIfFull,
// flushDue, tryFlush), the export (flush) and what an acknowledged export
// commits (settle, applyPendingBookmark). Retention is the other half of the
// same bound and lives here too: a collector outage settles nothing, and past
// retainCap the oldest unexported entries are shed (shedOldest) rather than
// held without limit.

import (
	"context"
	"fmt"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
	"github.com/JohanLindvall/kubescrape/internal/agent/route"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// flushWarnEvery rate-limits the export-failure warning during an outage.
const flushWarnEvery = time.Minute

// flushDue reports whether the count trigger may attempt an export again. It
// is unrestricted while flushes are landing; after a failure the batch stays
// at or above flushAt (nothing settles), so the trigger holds for every event
// that arrives and would re-export per event.
func (r *Reader) flushDue() bool {
	return r.flushFailedAt.IsZero() || r.now().Sub(r.flushFailedAt) >= r.cfg.FlushInterval
}

// flushIfFull is the COUNT trigger, shared by the watch (handle) and the
// backlog walk (replayItem): export once the batch reaches flushAt, paced by
// flushDue after a failure.
func (r *Reader) flushIfFull(ctx context.Context) {
	if len(r.batch) >= r.flushAt() && r.flushDue() {
		r.tryFlush(ctx)
	}
}

// tryFlush exports the batch and treats a TRANSIENT failure as a flush
// failure, not a stream failure.
//
// Propagating it tore down the API-server watch, and past BatchSize the batch
// is retained, so the count trigger held forever and every subsequent event
// cost another failing export and another teardown — 7681 of them to reach the
// retention cap from cold, with backoff.ResetIfHealthy needing a 30s-long
// stream so the backoff pinned at its 30s cap. Worse than the churn was what
// it did to the position: only one event per round was handled, so seenRV fell
// behind the cluster's event rate, aged out of the API server's watch window
// within minutes, and the resulting Gone expired into a relist that a COLD
// reader cannot take (relist needs a non-zero watermark) — so the watch
// silently restarted at the CURRENT revision and the whole gap was discarded,
// counted only by obs.EventRelists{stage="watch"}, whose name asserts a relist
// that did not happen.
//
// Keeping the watch open costs nothing the design does not already carry: the
// batch is bounded by retainCap, the collector's failure is counted by
// obs.EventsExportFailures, and seenRV keeps tracking the live stream. A
// PERMANENT rejection is not seen here at all — flush settles it and returns
// nil, as everywhere else.
//
// It reports whether the flush landed (or settled permanently): exportUntil
// retries on false.
func (r *Reader) tryFlush(ctx context.Context) bool {
	if err := r.flush(ctx); err != nil {
		now := r.now()
		r.flushFailedAt = now
		if _, loud := r.exportOutage.Fail(now, flushWarnEvery); loud {
			msg := "event export failed; the watch stays open and the batch is retained"
			if r.flushTicker == nil {
				// No watch loop yet: this is the backlog walk, which holds at the
				// cap rather than shedding (replayItem).
				msg = "event export failed during the backlog replay; the batch is retained and the walk pauses at the retained-batch cap until an export lands, so nothing is shed"
			}
			r.log.Warn(msg, "error", err, "buffered", len(r.batch), "cap", r.retainCap())
		}
		return false
	}
	// Measure the next flush from THIS flush, not from the last tick (see
	// flushTicker). nil during replayBacklog, before the watch's ticker exists.
	if r.flushTicker != nil {
		r.flushTicker.Reset(r.cfg.FlushInterval)
	}
	// Offer to persist after EVERY flush, not only the ticker-driven one.
	// persist self-throttles on PersistInterval, so this cannot write more
	// often than configured — but leaving it in the ticker branch alone meant
	// resetting that ticker here STARVED it: a rate that trips the count
	// trigger faster than FlushInterval reset the ticker before it could fire,
	// so the position was never written and the PersistInterval bound on
	// post-crash replay silently stopped holding (measured: 0 writes under a
	// count-triggered load that produced 10 with the trigger disabled).
	r.persist(ctx, false)
	r.flushFailedAt = time.Time{}
	if failures, lasted, ok := r.exportOutage.Recover(r.now()); ok {
		r.log.Info("event export recovered", "buffered", len(r.batch),
			"failures", failures, "outage", lasted)
		// Re-arm the overflow warning with the recovery, not with the process:
		// the next outage that sheds events is a new loss, and a latch held for
		// the process' life reported only the first one — on a singleton that
		// runs for weeks, usually not the one being investigated.
		r.overflowWarned = false
	}
	return true
}

// flush exports the batch; the position advances only after the collector
// acknowledges it.
func (r *Reader) flush(ctx context.Context) error {
	if len(r.batch) == 0 {
		return nil
	}
	// Convert ONCE per batch, not once per export ATTEMPT — a batch is
	// RETAINED across a transient failure AND across a redelivers=false stream
	// restart, and re-converting re-observed the LogMetrics on every lap
	// (logchain.Pending owns that discipline). settle() clears the pair with
	// the batch, and stream()'s restart reset clears it too.
	ld := r.pending.Render(r.convert)
	// The high-water position over the RENDERED PREFIX, not the whole batch. A
	// relist delivers the backlog in store order and each object carries its
	// own resourceVersion, so the last entry is routinely older than one
	// earlier in the batch — committing it walked the position BACKWARDS
	// (redelivery on restart), and in the other order committed a high RV while
	// lower-RV entries of the same backlog were still undelivered (outright
	// loss on a kill right after). So take the maximum — but only over
	// [:rendered], the entries this payload actually carries: a redelivers=false
	// restart appends fresh entries past that boundary that are NOT exported
	// yet, and committing their RV would lose them (see the `rendered` field).
	//
	// Seeded from the ZERO Position rather than the first entry: newerRV treats
	// an empty revision as older than any, and every timestamp is after the
	// zero time, so the result is the same — and an empty prefix yields the
	// zero Position instead of indexing an entry that is not there.
	covered := min(r.rendered, len(r.batch))
	var high Position
	for _, e := range r.batch[:covered] {
		if newerRV(e.rv, high.ResourceVersion) {
			high.ResourceVersion = e.rv
		}
		if e.ts.After(high.Watermark) {
			high.Watermark = e.ts
		}
	}
	count := ld.LogRecordCount()
	if count > 0 {
		// route.Reoffer: a failed export retains this batch and re-sends this
		// same rendering, so a router splitting it may hold the default share
		// back while a tenant route fails instead of spooling one copy per
		// attempt (route/reoffer.go).
		if err := r.cfg.Exporter.ExportLogs(route.Reoffer(ctx), ld); err != nil {
			// The records counted are the EXPORTED count (the rules may have
			// dropped some of the batch), matching EventsExported.
			if !logchain.SettlePermanent(err, r.log, "event batch", count,
				logchain.SettleCounters{Batches: obs.EventsDropped, Records: obs.EventsDroppedRecords},
				"events", len(r.batch)) {
				// This pipeline's OWN transient-failure counter, not
				// kubescrape_log_export_failures_total: that one documents
				// itself as the tailer's "files rewound", and the singleton
				// that collects events runs with -logs=false, so an operator
				// alerting on a rewinding tailer was paged by a pod that owns
				// no file. journald was given its own counter for exactly this
				// reason and its two siblings were left behind.
				obs.EventsExportFailures.Inc()
				return fmt.Errorf("exporting events: %w", err)
			}
		} else {
			obs.EventsExported.Add(float64(count))
		}
	}
	r.settle(high, covered)
	return nil
}

// settle advances the position to what the exported payload covered and drops
// exactly those entries, RETAINING any appended past the rendered prefix (a
// redelivers=false restart's fresh, not-yet-exported entries — see the
// `rendered` field). covered is the prefix length the payload rendered, and
// high the position it reached: the newest revision and the latest event time
// over that prefix (see flush).
func (r *Reader) settle(high Position, covered int) {
	covered = min(covered, len(r.batch))
	emptied := covered == len(r.batch)
	// These entries are done with: the chain will not see them again unless a
	// relist replays them, which is a new DELIVERY and is observed again.
	r.forgetObserved(r.batch[:covered])
	// Slide the retained tail to the front (copy is memmove-safe for the
	// overlap) and clear the vacated slots so settled entries aren't pinned.
	n := copy(r.batch, r.batch[covered:])
	clear(r.batch[n:])
	r.batch = r.batch[:n]
	// The converted payload belongs to the prefix just settled; the tail
	// converts afresh (and is observed by log-metrics) on the next flush.
	r.dropRendering()
	// The resource memo is per BATCH. Retained tail entries already hold the
	// resources they were built with, so dropping it costs at most one rebuild
	// per involved object on the next flush and keeps a settled object's
	// resource from outliving the entries that referenced it.
	clear(r.resCache)
	// The replay list's entries are the batch's LEADING ones, so a settled
	// prefix retires exactly that many of them. Reaching zero with the walk
	// finished is what secures the replay.
	r.replayOwed -= min(covered, r.replayOwed)
	r.maybeSecureReplay()
	// See replaySecured: a mid-replay commit positions a restarted stream
	// past backlog the store-order replay has not delivered yet, so the
	// position holds until the whole backlog has been exported.
	unsecured := !r.replaySecured
	if !unsecured && high.ResourceVersion != "" && newerRV(high.ResourceVersion, r.committed.ResourceVersion) {
		r.committed.ResourceVersion = high.ResourceVersion
	}
	// A bookmark seen while the batch was pending is covered only when this
	// flush EMPTIED the batch: its revision vouches for everything the stream
	// delivered before it, which includes entries retained past the rendered
	// prefix (a redelivers=false restart's fresh appends), so applying it over
	// a partial flush put the committed position ABOVE unexported tail entries
	// — a watch resumed from it never re-delivers them, and the persisted
	// position stopped being a lower bound on what was delivered. With a tail
	// retained the bookmark stays pending for the flush that covers it. The
	// bookmark proves the backlog fully delivered, so once it applies this
	// flush's own watermark may commit directly (recomputed below).
	if r.pendingRV != "" && emptied {
		r.applyPendingBookmark()
	}
	unsecured = !r.replaySecured
	// Clamp to wall clock first. The watermark is a running MAXIMUM over
	// event timestamps written by whichever component reported each event,
	// so one reporter with a fast clock would otherwise latch a boundary in
	// the future and make the relay filter discard everything until real
	// time caught up — on the one path whose whole purpose is recovering
	// events that were never delivered. The clamp applies to the held
	// accumulator too — secureReplay folds it in verbatim.
	mark := &r.committed.Watermark
	if unsecured {
		mark = &r.heldWatermark
	}
	if high.Watermark.After(*mark) {
		if now := r.now(); high.Watermark.After(now) {
			high.Watermark = now
		}
		if high.Watermark.After(*mark) {
			*mark = high.Watermark
		}
	}
	if r.committed.ResourceVersion != "" {
		// The replay (if one was pending) is secured up to this position: a
		// restart resumes from it instead of relisting the full TTL again, and
		// what the stream delivers from here on is new rather than replayed.
		r.relist = false
	}
}

// dropRendering discards the batch's OTLP rendering together with the count of
// entries it covers. The two are ONE piece of state: `rendered` describes the
// pending payload, so a rendering dropped without its count would let settle
// retire entries no payload carried (see the `rendered` field), and a count
// reset without its rendering would retain a payload settle can no longer
// size. Every site that drops one drops both — a stream restart that re-reads
// the batch, a settle, a shed of a fully rendered batch — through here.
func (r *Reader) dropRendering() {
	r.pending.Discard()
	r.rendered = 0
}

// applyPendingBookmark commits the pending bookmark. Its two callers decide
// WHEN a bookmark is covered — handle's idle arm (nothing buffered) and
// settle's emptied-batch arm — and this is the one rule for HOW it applies.
//
// The position may only move FORWARD: bookmarks arrive interleaved with events,
// so one seen before the last few entries of a flush carries an OLDER revision
// than that flush committed, and applying it unconditionally walked the
// committed position backwards by up to a flush window — a restart or leader
// handover then redelivered everything in between. And the pending copy is
// SPENT: leaving it set hands the next emptying flush a bookmark already
// applied, which reaches secureReplay a second time. (That half is the
// discipline held rather than a defect repaired: the API server's bookmarks
// are monotone within one stream, and only expire() can empty the committed
// revision a stale pendingRV would have to misfire against.)
//
// A bookmark covers everything the stream delivered before it, so it also
// disarms a relist and secures a replay.
func (r *Reader) applyPendingBookmark() {
	if newerRV(r.pendingRV, r.committed.ResourceVersion) {
		r.committed.ResourceVersion = r.pendingRV
	}
	r.pendingRV = ""
	r.relist = false
	r.secureReplay()
}

// maxRetained bounds the batch across failed flushes, in entries.
//
// A collector outage settles nothing, so the batch grows for the whole length
// of the outage — at the CLUSTER's event rate, since the watch stays open
// across a failed export (tryFlush), which on a busy cluster fills this cap in
// minutes. Retention is at most ~2 KB per entry, where every event names a
// distinct involved object: the built resource (~1 KB; repeats share one — see
// resource()), the body, and the event's own attributes as plain fields
// (eventMeta — they were a boxed map, another ~1.1 KB). So the cap is at most
// ~16 MB against the singleton's 256Mi limit.
//
// The cap is what makes the loss bounded and OBSERVABLE
// (obs.EventsOverflowDropped) rather than an OOM that takes the whole batch,
// the whole outage window (StartMode=end after the restart) and the co-located
// pipelines with it.
const maxRetained = 8192

// maxRetainedCeiling is the absolute ceiling the floor below may not lift the
// cap past. -events-batch-size has no upper bound of its own, so the 2*BatchSize
// floor turned an extreme value into an extreme budget (100000 => ~400 MB
// retained against a 256Mi limit) — the cap stopped being a cap. Past this
// ceiling the count-triggered flush is simply unreachable and the FlushInterval
// ticker does the flushing: slower, never an OOM.
const maxRetainedCeiling = 2 * maxRetained

// retainCap is maxRetained, floored above BatchSize so the count-triggered
// flush stays reachable however BatchSize is tuned, and ceilinged so the floor
// cannot repeal the memory bound.
func (r *Reader) retainCap() int {
	return min(max(maxRetained, 2*r.cfg.BatchSize), maxRetainedCeiling)
}

// flushAt is the batch length that triggers an export: BatchSize, clamped to
// the retention cap because the batch can never grow past that.
//
// -events-batch-size has no upper bound, and the ceiling above stops retainCap
// following it, so a BatchSize over maxRetainedCeiling made the raw count
// trigger UNREACHABLE — the batch sat at the cap shedding the oldest entries
// forever. On the watch path that was survivable (the FlushInterval ticker
// flushes), but the backlog walk services no ticker: measured against a 30,000
// event backlog, BatchSize=512 exported 29,696 while BatchSize=20000 exported
// NOTHING and dropped 13,696 into kubescrape_events_overflow_dropped_total —
// documented as outright loss — before the watch was even opened. Both paths
// use this, so the trigger cannot be unreachable on either.
func (r *Reader) flushAt() int {
	return min(r.cfg.BatchSize, r.retainCap())
}

// shedChunk is how many entries one shed drops. Dropping exactly one per
// admitted event made the shed O(batch): at the cap EVERY event sheds, and
// each shed memmoved the whole post-prefix tail (measured 47.5 µs against
// 2.8 µs below the cap, 1.18 MB moved per event). Dropping a run amortises the
// move over that many admissions; the surplus loss is bounded by one chunk at
// the moment the collector recovers, against a cap of maxRetained.
const shedChunk = maxRetained / 64

// shedOldest drops a run of entries to admit new ones at the cap, starting at
// the oldest entry the pending payload does NOT cover. The rendered prefix is
// kept — its entries are not lost (they export when the collector recovers),
// and dropping inside it would make settle slide fresh entries as covered by a
// payload that never carried them. Only when EVERYTHING is rendered is the
// rendering discarded first, so the drop cannot orphan it; the re-render costs
// nothing but the conversion, since the surviving entries are already in the
// observed set and the chain will not count them twice.
func (r *Reader) shedOldest() {
	if r.rendered >= len(r.batch) {
		r.dropRendering()
	}
	i := r.rendered
	n := min(shedChunk, len(r.batch)-i)
	if n <= 0 {
		return
	}
	// Shed entries are gone for good, so their observation proof goes too — it
	// would otherwise be a key no batch entry can ever retire.
	r.forgetObserved(r.batch[i : i+n])
	copy(r.batch[i:], r.batch[i+n:])
	clear(r.batch[len(r.batch)-n:])
	r.batch = r.batch[:len(r.batch)-n]
	// Shed entries the replay list contributed stop being OWED. The walk itself
	// never sheds — it holds at the cap instead (replayItem), because what it
	// has not read is still in the API server — and it exports its tail before
	// the watch opens, so owed entries reach this only if a future path lets
	// them outlive the walk. Should one, they are counted loss that no export
	// in THIS replay will carry, so holding the position for them would wedge it
	// exactly as waiting for a bookmark did.
	if r.replayOwed > i {
		r.replayOwed -= min(i+n, r.replayOwed) - i
	}
	obs.EventsOverflowDropped.Add(float64(n))
	if !r.overflowWarned {
		r.overflowWarned = true
		r.log.Warn("event batch at capacity with nothing committing; dropping the oldest unexported events, which the watch will not re-deliver",
			"cap", r.retainCap(), "dropped", n, "perShed", shedChunk)
	}
}
