package tailer

// Per-entry observation dedup: the identities of the entries already through
// the per-record chain, so the re-read that follows a rewind rebuilds them
// without counting them a second time (logchain.Input.Observed).

import (
	"github.com/JohanLindvall/haste/rapidhash"
)

// obsKey identifies ONE emitted entry: the exact segment-qualified byte range
// it was built from, plus a hash of the body those bytes produced.
//
// The body hash is LOAD-BEARING, not a refinement of a range that would nearly
// do on its own. Two independent reasons, the second of which this comment
// used to deny:
//
//   - Over one range, dropping or admitting an interior line changes which
//     logical lines a multi-line group joined, so one range yields two
//     different records (TestObservedIdentityIsPerEntryNotPerRange).
//   - Segment ids and offsets genuinely REPEAT over replaced content. A fresh
//     tail id is issued only for a replacement the tailer SEES, and everything
//     that sees one compares the file against f.readPos and f.fp (read.go:
//     the pre-read fingerprint re-verify is guarded by `f.readPos > 0`, and
//     handleRotation's arms test `st.Size() < f.readPos` and `read == 0`).
//     With readPos back at 0 — where a failed export's rewind leaves it —
//     a truncate-and-rewrite of the same length trips none of them, and the
//     replacement is read at the old offsets under the old tail id.
//     TestTruncatedFileIsObservedAgainAtTheSameOffsets walks exactly that path
//     and asserts no new id is issued: there the hash is the ONLY discriminator.
//
// RESIDUAL HOLE, since the hash discriminates only what the body does: a later
// entry keying identically — same segment, same range, and either a
// byte-identical body or a 2^-64 rapidhash collision — is taken for the recorded
// one and its observation suppressed. Reaching one at all needs the bytes
// re-read, which needs a rewind, so what it costs depends on how the recorded
// identity got into the set:
//
//   - REWOUND (its batch never shipped). Its observation has no delivery of its
//     own, and the suppressed record's delivery has no observation: they
//     cancel. Every distinct key is banked once by a pass that delivered
//     nothing, so counted stays >= delivered — this direction degrades towards
//     the honest over-count the mechanism replaces, never below it.
//   - WITHHELD (delivered, commit clamped by the watermark). Its observation is
//     already matched by a delivery, so a suppression here loses one for good.
//     But the rewind that re-reads it lands on that segment's committed
//     frontier, and while that frontier is above zero the pre-read fingerprint
//     re-verify runs on the next read and issues a fresh tail id for a replaced
//     head. At a zero frontier nothing is checked — and there the replacement
//     has to reproduce the withheld entry's bytes at its exact offsets for the
//     key to match, so the delivery that goes uncounted is a byte-identical
//     duplicate of the record already counted. What is left over that is the
//     hash collision.
//
// Closing it would take an identity the file's content cannot forge (a per-file
// observation epoch bumped whenever a read could have crossed replaced bytes) —
// more machinery than a 2^-64 loss of one record's observation is worth.
type obsKey struct {
	startSeg, endSeg int
	start, end       int64
	body             uint64
}

// obsRange is the CHEAP half of the identity: the segment-qualified byte range,
// with no body hash. It is what reReadable needs, and reReadable is what decides
// whether the hash is needed at all — see obsKey.
func (e entry) obsRange() obsKey {
	return obsKey{
		startSeg: e.start.seg, endSeg: e.end.seg,
		start: e.start.off, end: e.end.off,
	}
}

// obsKey is the entry's full identity for the observed set. Taken from the
// entry as the BATCH holds it, before any per-flush rewriting of the body (the
// parse hook, the scrubber), so the same bytes always produce the same key.
//
// The body hash is why the two halves are separate: it is computed at the two
// points that genuinely consume it (a lookup against a NON-EMPTY set, an
// insertion reReadable has already approved) rather than once per entry per
// flush on the pinned path. A file with an empty set and nothing re-readable
// after this flush pays neither — but that is not the same as "the steady
// state", and alreadyObserved is where the state it does not cover is written
// down.
func (e entry) obsKey() obsKey {
	k := e.obsRange()
	k.body = rapidhash.Sum64String(e.body)
	return k
}

// maxObservedEntries bounds the per-file observed set. It is a REACHABLE
// working size and not a theoretical ceiling: every entry a flush withholds
// goes in, and a single group held open on one stream withholds everything
// every OTHER stream flushes for as long as the group lives, so the set grows
// by a whole batch per flush until it closes. Filling the cap takes 8k lines on
// the other streams inside one group's lifetime — a busy container's second at
// the 1s -logs-multiline-timeout, and longer than a second whenever
// continuation lines keep refreshing the group's staleness stamp (up to
// maxGroupBuffered of them). Past it new identities are simply not
// recorded, so the worst case degrades to the honest over-count this whole
// mechanism replaces; the identities already held stay valid. What sitting at
// the cap COSTS is measured at alreadyObserved.
const maxObservedEntries = 8192

// alreadyObserved reports whether this exact entry has been through the
// per-record chain before — what buildRecord passes to
// logchain.Input.Observed.
//
// The emptiness check is not an optimisation of the map lookup, it is what
// keeps the body out of the hash — for the files whose set IS empty. That is
// not "every file that has never had an export fail", which is what this said:
// observe also records an entry whose commit the watermark clamp WITHHELD, and
// withholding needs no failure at all. With -logs-multiline on (the default) it
// is the ordinary two-stream shape — a container writing an exception to stderr
// while stdout keeps logging withholds every stdout entry emitted while the
// group is open, i.e. until a non-continuation line closes it or
// -logs-multiline-timeout (1s) ages it out. That is the shape
// TestWithheldCommitReleasedOnceGroupResolves drives.
//
// So the cost, measured rather than assumed, on BenchmarkIngestFlush's own line
// mix and flush cadence with the file's observed set empty against one sitting
// at maxObservedEntries — which is what a group held open across the batch
// produces:
//
//   - per line, ALLOCATIONS ARE UNCHANGED — 4 allocs/op and ~240 B/op either
//     way, BenchmarkIngestFlush/enrich's own figures. The map is allocated once
//     per file and reused: prune deletes from it, it does not drop it.
//   - the added work is CPU, and it is NOT flat in the size of the set: one
//     rapidhash of the body per entry looked up here, one more per entry recorded
//     in observe, and one pruneObserved pass over the WHOLE set per touched
//     file per flush. Only the hashes are per entry; the pass amortises over
//     the batch, so the per-line overhead is O(|observed| / BatchSize) on top
//     of them — and the two ends of that ratio are far apart. At the cap it
//     measured +6-10% per line with the default -logs-batch-size of 1024 (8 map
//     probes per line) and 2.1x — the flush path roughly doubles — at a 64-line
//     batch (128 probes per line). Those two are ratios on one machine; the
//     O(...) is the part that travels, and a bigger batch is what makes a
//     saturated set cheap.
//   - the set is bounded by the withheld window, not by the outage: it drains
//     to empty on the flush after the group closes and the re-offered high
//     commits.
func (f *file) alreadyObserved(e entry) bool {
	if len(f.observed) == 0 {
		return false
	}
	_, ok := f.observed[e.obsKey()]
	return ok
}

// observe records one just-flushed entry's identity, if a rewind could still
// bring its bytes back.
//
// Called after the export outcome is known, the same way whatever that outcome
// was: on a failure nothing committed, so every entry of the batch qualifies;
// on a success only the entries the watermark clamp withheld do (their bytes
// are delivered but the file's committed offset is still behind them, and a
// LATER failure rewinds to exactly that frontier).
//
// reReadable is not merely an economy filter. Identities are not unique for all
// time: over replaced content a segment id and a byte range repeat, and then
// only the body tells two records apart (see obsKey's residual hole). Every
// identity held past the point where a rewind could reach its bytes is one more
// thing a later entry can be mistaken for, so recording too much costs memory
// AND widens that window — dropping an identity the moment its bytes are
// unreachable is what keeps the window as narrow as obsKey claims.
//
// It is also the gate on the body hash for THIS side: reReadable reads only the
// byte range, so a flush whose batch all commits records nothing and hashes
// nothing here. The lookup side is separate and is not free while the set is
// non-empty — see alreadyObserved.
func (f *file) observe(e *entry) {
	if !f.reReadable(e.obsRange()) || len(f.observed) >= maxObservedEntries {
		return
	}
	if f.observed == nil {
		f.observed = make(map[obsKey]struct{}, 8)
	}
	f.observed[e.obsKey()] = struct{}{}
}

// pruneObserved forgets the identities whose bytes no rewind can reach again.
// Once per touched file per flush, before the flush's own entries go in.
func (f *file) pruneObserved() {
	for k := range f.observed {
		if !f.reReadable(k) {
			delete(f.observed, k)
		}
	}
}

// reReadable reports whether a rewind could still bring an entry's bytes back:
// a rewind seeks to the committed frontier of the entry's own segment, so
// anything at or below that frontier is gone for good, as is anything in a
// segment id that no longer resolves (retired, or truncated away).
func (f *file) reReadable(k obsKey) bool {
	c, ok := f.committedIn(k.endSeg)
	return ok && k.end > c
}
