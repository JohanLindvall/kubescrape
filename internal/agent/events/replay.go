package events

// The backlog REPLAY: how the reader recovers a position the API server no
// longer holds. When the stored resourceVersion ages out of the watch window
// (expire), or cannot be read (loadPosition), the backlog is re-read as a
// paginated LIST (replayBacklog), filtered by the exported watermark (wanted),
// and the position is held until every listed entry has been exported
// (maybeSecureReplay, secureReplay). The other halves of what secures a
// replay live where the position moves: settle retires the owed entries, and
// a bookmark secures one outright (applyPendingBookmark).

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/JohanLindvall/kubescrape/internal/agent/backoff"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// Stage labels for obs.EventRelists / obs.EventGapDiscarded: WHERE the
// resourceVersion was found to have aged out. They are metric label VALUES, so
// the set is named once here rather than spelled at each call site, and
// publishMetrics gives every one of them a series.
const (
	stageWatch  = "watch"  // the watch itself returned Gone / Expired
	stageReplay = "replay" // a paginated backlog list lost its snapshot mid-walk
)

// replaySlack widens the relist replay boundary. It absorbs the second
// truncation of metav1.Time plus modest node clock skew; anything it lets
// through a second time is a duplicate, which the pipeline already tolerates,
// while anything it excludes is lost outright.
const replaySlack = time.Minute

// replayPageSize bounds one backlog page. The backlog is a whole --event-ttl
// window (an hour by default) and can be six figures on a busy cluster, so it
// is paged rather than materialised whole: the singleton runs against a 256Mi
// limit and one unbounded List response would be the OOM the retained-batch cap
// exists to prevent, arriving before a single entry is exported. It matches
// client-go's reflector default.
const replayPageSize = 500

// replayProgressEvery is how often the backlog walk reports progress while it
// holds the reader goroutine (see replayBacklog).
const replayProgressEvery = 10 * time.Second

// replayBacklog re-reads the events the watch cannot position us into and
// returns the revision the watch that follows must start at.
//
// WHY A LIST AND NOT A WATCH FROM "". A watch started at "" delivers the same
// backlog as synthetic ADDED events, and that is what this used to do — but
// those events arrive in STORE order carrying their own arbitrary revisions,
// and the stream never says where the backlog ENDS. So the position had to be
// held (see replaySecured) until something proved the watcher caught up, and
// the only such signal the API offers is a BOOKMARK, which for core/v1 events
// never comes. A List answers both questions itself: it TERMINATES (an empty
// Continue), and its snapshot revision is a boundary no item can exceed.
//
// The order the pages arrive in still tells us nothing, so the position stays
// held for the whole walk and until every listed entry has been exported —
// what changed is that the release is now reachable.
//
// WHY NOT client-go's pager.ListPager, which is in the module already. It pages
// exactly like this; the one behaviour it adds is FullListIfExpired, which
// recovers a compacted continue token by re-issuing the List UNPAGED — the
// single unbounded response the paging exists to prevent (a whole --event-ttl
// window materialised at once against the singleton's 256Mi limit). With that
// off it returns the same error this loop already handles, and its per-ITEM
// callback hides the page boundary where the continue token, the snapshot
// revision and the interleaved flush all live. Hand-rolled deliberately.
//
// THE COST OF THE LIST-THEN-WATCH SHAPE, accepted rather than fixed: the watch
// that follows is positioned at the FIRST page's snapshot, so the walk's own
// duration is charged against the API server's compaction window — the old
// watch-from-"" start had no such exposure, since it began at the CURRENT
// revision. This is the shape client-go's own reflector uses, and the recovery
// is the one it uses too: the watch fails Gone and expire arms another relist.
// What keeps that from re-exporting the backlog lap after lap is the flush at
// the END of this walk — a lap that COMPLETES commits the snapshot revision and
// the watermark together, so the next lap's wanted() drops everything this one
// shipped (bar the replaySlack window). A lap aborted mid-walk by the continue
// token expiring commits nothing and does re-export, which is at-least-once
// behaving as documented.
func (r *Reader) replayBacklog(ctx context.Context) (string, error) {
	// No ResourceVersion: a quorum read, so the snapshot is the most recent
	// state and the watch that resumes from it cannot start behind one.
	opts := metav1.ListOptions{Limit: replayPageSize}
	pages, listed, kept := 0, 0, 0
	walkStarted := time.Now()
	walkLogged := walkStarted
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		list, err := r.cfg.Client.CoreV1().Events(r.cfg.Namespace).List(ctx, opts)
		if err != nil {
			if isExpired(err) {
				// The snapshot the continue token pins aged out mid-walk. The
				// pages already read cannot be completed by a second snapshot
				// (that one starts at a different revision), so the replay is
				// abandoned and re-armed: expire counts it and keeps relist
				// set, and what was already exported is filtered out of the
				// next attempt by the watermark, exactly as after a Gone watch.
				r.expire(stageReplay)
			}
			return "", fmt.Errorf("listing the event backlog: %w", err)
		}
		if r.replayRV == "" {
			// Every page of a continued list is served from the FIRST page's
			// snapshot, so this revision covers the whole walk.
			r.replayRV = list.ResourceVersion
		}
		for i := range list.Items {
			ok, err := r.replayItem(ctx, &list.Items[i])
			if err != nil {
				// ctx ended while the walk was held at the cap (see replayItem).
				// The replay is unsecured, so the position is held and the next
				// stream — or the successor leader — re-lists; nothing was shed.
				return "", err
			}
			if ok {
				kept++
			}
		}
		listed += len(list.Items)
		pages++
		// The walk BLOCKS the single reader goroutine — no watch is open, no
		// ticker is serviced — and an --event-ttl window on a busy cluster is
		// six figures, i.e. hundreds of pages. Without a progress line the only
		// thing an operator sees is a pipeline that exports nothing and says
		// nothing, which is indistinguishable from a wedge. Cadence-based
		// rather than page-based: what matters is how long the silence is, not
		// how many pages fit inside it.
		if time.Since(walkLogged) >= replayProgressEvery {
			walkLogged = time.Now()
			r.log.Info("replaying the event backlog", "pages", pages,
				"listed", listed, "kept", kept, "elapsed", time.Since(walkStarted).Round(time.Second))
		}
		if list.Continue == "" {
			break
		}
		if list.Continue == opts.Continue {
			// A token that does not advance is a walk that never terminates,
			// and this loop holds the single reader goroutine. It is an error
			// rather than a break: breaking would secure the replay over a
			// backlog only partly read, which is the silent loss the hold
			// exists to prevent. A page COUNT limit would do the same to a
			// legitimately huge backlog, so the guard is exactly "no progress".
			return "", fmt.Errorf("the event backlog list repeated its continue token after %d pages", pages)
		}
		opts.Continue = list.Continue
	}
	if r.replayRV == "" {
		// Without a boundary the watch could only be started at "" again, which
		// is the unbounded backlog this function exists to replace, and nothing
		// could ever secure the replay. Fail the stream instead: Run retries it
		// with the relist still armed.
		return "", errors.New("the event backlog list carries no resourceVersion; the watch after it cannot be positioned")
	}
	// The backlog is read: everything the watch delivers from here is NEWER
	// than the snapshot, so the watermark filter must not touch it.
	r.replaying = false
	// Read the boundary out BEFORE securing, which consumes r.replayRV: the
	// watch must be positioned at it, and returning the emptied field started
	// the watch at "" — the unbounded server-side backlog this replaced.
	rv := r.replayRV
	r.noteSeen(rv)
	// Export the TAIL before the watch is established. The count trigger leaves
	// up to flushAt entries behind on every walk, and the next opportunity to
	// export them is the FlushInterval ticker inside the watch loop — i.e. only
	// if establishing the watch succeeds, which is exactly the step the stale
	// snapshot revision (see above) can fail. Owed entries there hold the
	// position, so a Gone watch re-lapped the whole backlog with nothing
	// committed and nothing filtering it. This is also what secures the replay
	// on the ordinary path, and what makes an aborted lap terminate.
	//
	// HELD until it lands, not gated on flushDue: a single paced attempt left
	// the tail to the watch loop whenever the last count-triggered export had
	// failed within FlushInterval, and there — the collector still down — the
	// watch's own events fill the batch to the cap and the shed that follows is
	// counted loss. Waiting here loses nothing: no watch is open yet, so every
	// event since the snapshot is still in the API server for the watch (or,
	// past the compaction window, the relist) to deliver once this returns.
	if err := r.exportUntil(ctx, 0); err != nil {
		return "", err
	}
	r.log.Info("replayed the event backlog", "resourceVersion", rv,
		"pages", pages, "listed", listed, "kept", kept, "awaitingExport", r.replayOwed,
		"elapsed", time.Since(walkStarted).Round(time.Millisecond))
	// A backlog that was empty, entirely filtered, or already flushed secures
	// right here — otherwise the flush that drains the last owed entry does it.
	r.maybeSecureReplay()
	return rv, nil
}

// replayItem consumes one backlog item, in the order the walk applies them: the
// watermark filter, the ingest, the owed count, then the COUNT-triggered
// export. It reports whether the item was kept.
//
// This walk BLOCKS the single reader goroutine, so the count trigger is the
// only one there is until it returns: the FlushInterval ticker and persist both
// live in the watch loop that has not started yet (persist would write nothing
// anyway — every commit is held while the replay is unsecured). That is why the
// trigger is flushAt rather than BatchSize, why the tail is exported above, and
// why a failed export makes the walk WAIT at the cap rather than read on into a
// shed (below).
//
// It is a method rather than the loop body it was so a test can drive the exact
// mid-walk state — an export with `replaying` still set — that no other path
// can produce: the flag is cleared before the watch opens, and driving a WATCH
// event with it set would pin the filter to the watch path, which is precisely
// where it must never be applied.
//
// BACK-PRESSURE, NEVER A SHED. A failed count-triggered export makes flushDue
// false for FlushInterval, and the walk pages at API-server speed — so the
// batch used to reach retainCap within that interval and ingest shed runs of
// entries that are still in the API server and could simply have been waited
// for, and once the collector answered the replay secured at the snapshot
// revision and nothing ever re-listed them (measured on a 30,000-event backlog
// with ONE failed export: 21,888 shed for good). At the cap the walk therefore
// retries the export in place (exportUntil) before reading on. A long collector
// outage costs a blocked walk — and possibly a continue token aging out, which
// is an ordinary relist the watermark filters — never loss. The error is ctx's,
// returned only when the wait was cut short.
func (r *Reader) replayItem(ctx context.Context, e *corev1.Event) (bool, error) {
	// The walk checks ctx only once per PAGE, and a shutdown or lost lease
	// lands mid-page: ingesting the rest of it (up to replayPageSize items)
	// under a dead context ran every uncached pod lookup into
	// context.Canceled, and Run's final flush then exported those events under
	// their name-only identity — which the successor, re-listing the unsecured
	// replay, ships AGAIN resolved, as duplicates carrying two different
	// resources. Stopping here loses nothing: the replay is unsecured, so the
	// position is held and whoever streams next re-lists this item.
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !r.wanted(e) {
		return false, nil
	}
	if limit := r.retainCap(); len(r.batch) >= limit {
		if err := r.exportUntil(ctx, limit-1); err != nil {
			return false, err
		}
	}
	r.ingest(ctx, e)
	// The hold above keeps ingest from shedding here, so this entry and every
	// owed one before it is still in the batch. Counted BEFORE the trigger,
	// never folded into one "ingest and maybe flush" step: a flush settles the
	// owed prefix (settle retires replayOwed), so an entry counted after the
	// flush that exported it would stay owed forever and hold the position for
	// good.
	r.replayOwed++
	r.flushIfFull(ctx)
	return true, nil
}

// exportUntil exports the batch until at most keep entries remain, retrying a
// failed export IN PLACE, with backoff, until one lands or ctx ends (journald's
// flushRetry shape). It is the backlog walk's back-pressure (replayItem, and the
// tail in replayBacklog), and it is confined to the walk on purpose: there no
// watch is open, so everything not yet read is still in the API server and
// waiting costs nothing. The watch path cannot wait — the watch would stall
// behind it and age out of the API server's window — so it paces its retries
// through flushDue and sheds at the cap instead (shedOldest).
//
// Each successful export settles at least one entry, so the loop terminates
// once the collector answers. The error is ctx's, returned when the wait was
// cut short (leader handover, shutdown): the replay is then unsecured, the
// position held, and whoever streams next re-lists.
func (r *Reader) exportUntil(ctx context.Context, keep int) error {
	bo := backoff.New(r.cfg.RestartBackoff)
	for len(r.batch) > keep {
		if err := ctx.Err(); err != nil {
			return err
		}
		if r.tryFlush(ctx) {
			bo.Reset()
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		bo.Sleep(ctx)
	}
	return nil
}

// expire drops the committed resourceVersion so the next stream relists,
// keeping the watermark to filter the replay.
func (r *Reader) expire(stage string) {
	r.committed.ResourceVersion = ""
	// The pending bookmark dies with the revision it was pending against, and
	// this is the only path into a relist that could be carrying one (nothing
	// else empties the committed revision). Carried into the replay, the first
	// flush that empties the batch would apply it through settle's pending
	// branch — which SECURES the replay, releasing the position hold over a
	// backlog only partly read. TestExpiryClearsResourceVersionButKeepsWatermark
	// pins it.
	r.pendingRV = ""
	// The revision aged out of the API server's watch window, so the
	// in-process memory of it is dead too and must not be resumed from.
	r.seenRV = ""
	// Never DISARM an armed relist: loadPosition arms one with a ZERO
	// watermark when the stored position is unreadable, and an unsecured
	// replay holds its exported high-water in heldWatermark precisely so the
	// committed watermark stays zero — recomputing from the committed
	// watermark alone read both as "nothing to recover" and restarted the
	// next stream at the CURRENT revision, silently discarding the gap on
	// exactly the recovery path. Beyond an armed flag, anything exported
	// justifies the replay (the committed watermark filters it; a held one
	// folds in when the replay secures); only a truly cold reader — nothing
	// exported, nothing armed — restarts per the start mode, so a cold
	// -events-start=end still skips the backlog the operator asked to skip.
	r.relist = r.relist || !r.committed.Watermark.IsZero() || !r.heldWatermark.IsZero()
	// Count the fall-back only where the next stream actually takes it: the
	// armed relist, or a -events-start=start whose cold policy replays anyway.
	// The counter is registered as watches that "fell back to a relist", and
	// bumping it before the decision made the two OPPOSITE outcomes of this
	// function indistinguishable to an alert — under auto/end a watermark-less
	// expiry restarts at the CURRENT revision and DISCARDS the gap, which
	// tryFlush's comment already names as harm ("whose name asserts a relist
	// that did not happen").
	if r.relist || r.cfg.StartMode == StartBeginning {
		obs.EventRelists.WithLabelValues(stage).Inc()
		// A relist is not loss, but it IS the explanation for the duplicate
		// burst that follows (the backlog is replayed and filtered by a
		// watermark with a minute of slack) and for the pause while the walk
		// runs. Info rather than Warn: it is the recovery working, and the
		// counter is what an alert reads.
		r.log.Info("the stored resourceVersion aged out of the api server's watch window; replaying the event backlog and filtering it by the exported watermark",
			"stage", stage, "watermark", r.committed.Watermark)
		return
	}
	// The discard arm is the pipeline's one silent-loss path: it must move a
	// counter of its own, not just a Warn on one pod's stderr.
	obs.EventGapDiscarded.WithLabelValues(stage).Inc()
	r.log.Warn("event watch expired before anything was exported; restarting per the start mode and discarding the gap",
		"stage", stage, "startMode", r.cfg.StartMode)
}

// wanted filters a REPLAYED event against the watermark. A replay re-reads
// everything still within the TTL; the watermark drops what was already
// exported. Timestamps come from the REPORTING component's clock, so the
// comparison is biased toward re-emitting (at-least-once).
//
// It applies ONLY to the backlog list, which is the only thing that can carry
// an already-exported event. Event timestamps come from whichever component
// reported the event, and a stream carries several — kubelet events are
// second-truncated metav1.Time, scheduler and controller-manager events are
// microsecond MicroTime — so applied to a WATCH this dropped any event whose
// reporter's clock trailed the watermark, permanently and silently (noteSeen
// has already advanced the resume point past it) and uncounted. Worse, one
// component with a fast clock latched a FUTURE watermark, which is persisted in
// the position ConfigMap: the blackout then survived restarts and leader
// handover.
//
// That is why the filter is scoped by `replaying`, which replayBacklog clears
// the moment the last page is read, rather than by the stream's lifetime: the
// watch that follows a replay is positioned at the list's snapshot and delivers
// nothing the replay could already have carried.
func (r *Reader) wanted(e *corev1.Event) bool {
	if !r.replaying || r.replayFrom.IsZero() {
		return true
	}
	when := eventTime(e)
	return when.IsZero() || !when.Before(r.replayFrom)
}

// maybeSecureReplay secures the replay once its backlog is fully accounted
// for: the last page read (replaying cleared) and every entry it contributed
// settled or shed. That is the securing condition the API server cannot give
// us — see replaySecured.
func (r *Reader) maybeSecureReplay() {
	if r.replaySecured || r.replaying || r.replayOwed > 0 {
		return
	}
	r.secureReplay()
}

// secureReplay marks the current replay's backlog fully exported: the position
// may advance again, it advances to the LIST SNAPSHOT's revision (the boundary
// no listed item can exceed, and where the following watch is positioned), and
// the held exported high-water folds into the committed watermark. Idempotent;
// a no-op outside a replay.
func (r *Reader) secureReplay() {
	if r.replaySecured && r.heldWatermark.IsZero() {
		return
	}
	r.replaySecured = true
	// The snapshot revision, not the maximum over the entries: a consistent
	// list returned everything that existed at it, so committing it skips
	// nothing — while the entry maximum would leave the position below events
	// the list proved are already handled, and on an idle cluster the watch
	// would age out of the API server's window again with nothing to advance
	// it (the whole reason bookmarks exist).
	if r.replayRV != "" && newerRV(r.replayRV, r.committed.ResourceVersion) {
		r.committed.ResourceVersion = r.replayRV
	}
	r.replayRV, r.replayOwed = "", 0
	if r.heldWatermark.After(r.committed.Watermark) {
		r.committed.Watermark = r.heldWatermark
	}
	r.heldWatermark = time.Time{}
	if r.committed.ResourceVersion != "" {
		// A restart resumes from the position instead of relisting the full
		// TTL again.
		r.relist = false
	}
}
