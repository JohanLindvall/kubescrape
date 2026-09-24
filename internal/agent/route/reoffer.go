package route

// Reoffer: whether a split payload's DEFAULT share may be held back while a
// route share fails.
//
// Delivery through the router is at-least-once per destination: a failed
// destination fails the whole export, the producer retries, and the retry
// re-splits the payload and re-sends every share — so destinations that had
// already succeeded receive duplicates. That is cheap for a direct client and
// expensive for the default chain with -buffer-dir, which "succeeds" by
// SPOOLING, and does so while the collector is down: during a route outage
// every retry of a producer that re-offers its batch until it lands spooled
// ANOTHER copy of the default share, one per attempt, spending the
// -buffer-max-bytes every other producer on the node shares and replaying them
// all to the default backend on recovery. The tailer re-sends its batch every
// sweep with a fresh observed timestamp, so no content hash could fold them.
//
// Withholding the default share whenever a route fails is NOT safe in general:
// a single-shot producer never re-sends (promscrape drops a failed chunk, and
// its cadvisor/KSM chunks span namespaces, so they do split), and it would lose
// the default tenant's data to spare a duplicate. So the answer is per PAYLOAD
// and rides the CALL, the otlpexport.Own / transform.Handoff shape: Reoffer(ctx)
// is the caller's promise that a failed export's records come back — rebuilt or
// re-sent — until they are delivered or permanently rejected, and (once it has
// committed a position) across its own restart. For a marked payload the router sends the route shares
// FIRST, and when any of them fails TRANSIENTLY it withholds the default share
// and returns the route failure (never a permanent one), so the retry is the
// share's first delivery rather than its second. A permanently rejected route
// share withholds nothing: no retry can deliver it, and the default share goes
// out as it would unmarked. An unmarked payload keeps the old order and sends
// every share every attempt.
//
// The cost, stated: while a route fails transiently, a marked payload's default
// share is DELAYED with it — held by its producer, which was not advancing past
// that batch either way (the failure fails the whole export) — instead of
// duplicated. What the producer can hold is its own bound: the tailer's is the
// file on disk, the other three's a retained batch (and, for the events reader,
// its retained batch's overflow, which is counted loss either way). And a
// producer's FIRST run, before it has committed any position, cannot re-read
// across a restart — journald seeks to the journal tail, the events reader
// starts where -events-start says, an Event Hubs group where -azure-start says
// — so a restart inside a route outage that began on a first run loses the
// default share it was holding, where unmarked it would have been delivered
// (once per attempt). That window closes at the first successful export.
//
// Who marks (each checked against its failure path: the records of a failed
// export come back until delivered or permanently rejected):
//
//   - agent/tailer — exportWithRetry: a failed flush rewinds its files and the
//     next sweep re-reads and re-sends the same records; the checkpointed
//     offsets bring them back across a restart.
//   - agent/events — the reader's flush: a failed export retains the batch
//     (logchain.Pending re-exports the SAME rendering), and the persisted
//     position is only ever a lower bound on what was delivered.
//   - agent/journald — flushRetry re-sends the SAME payload until it settles;
//     the cursor commits only after success.
//   - agent/azurediag — deliver retries each signal in place, and the group's
//     offsets commit only after both signals are acked.
//
// journald's and the Azure reader's resources carry no k8s.namespace.name, so
// only a transform script's route() splits them — but when it does, the
// duplicate-spooling above is theirs too.
//
// Who must NOT mark:
//
//   - agent/promscrape, agent/cumagg, internal/metrics, agent/cgroupstats —
//     a failed chunk is dropped or re-rendered as NEW points; nothing comes back.
//   - agent/otlpingest — the SENDER retries, on a budget of its own; past it
//     the records are gone, and a withheld default share with them.
//   - agent/tailbuffer — its drain retries a decided keep a bounded number of
//     times and then drops it.

import "context"

// reofferKey is the context key for the re-offer marker. Unexported and of a
// private type, so nothing outside this package can set it by accident.
type reofferKey struct{}

// Reoffer marks ctx as carrying a payload whose producer re-offers the records
// of a failed export until they are delivered or permanently rejected. The
// router may then hold a split payload's default share back while a route
// share fails transiently (see reoffer.go's doc for who may mark, and why).
//
// The marker is a context VALUE, so it survives context.WithoutCancel and
// deadline wrappers — the same contract otlpexport.Own rides on.
func Reoffer(ctx context.Context) context.Context {
	if Reoffered(ctx) {
		return ctx
	}
	return context.WithValue(ctx, reofferKey{}, true)
}

// Reoffered reports whether ctx carries the Reoffer marker.
func Reoffered(ctx context.Context) bool {
	v, _ := ctx.Value(reofferKey{}).(bool)
	return v
}
