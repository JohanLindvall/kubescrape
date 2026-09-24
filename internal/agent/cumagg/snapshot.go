package cumagg

import "time"

// snapChunk is how many series one chunked lock-hold touches: a render's value
// copy (Snapshotter.Take) and the delivery mark after a successful send
// (afterDelivered) alike. It trades the number of acquisitions (cheap,
// uncontended most of the time) against the length of one stall on the receive
// path, which is what actually hurts: the aggregators fold a span or an edge
// under this mutex per request — servicegraph's from inside the pairing store's
// OWN mutex — so a hold across the whole cardinality cap is a receive-path
// stall. At the 20k cap the whole copy is ~5 ms, and this makes the longest
// hold about a fortieth of that.
//
// ONE width for both passes and both aggregators. It used to be spelled three
// times — each aggregator's snapChunk and this package's markChunk — all 512,
// under comments calling it each caller's to tune independently; nobody ever
// had, and the passes have the same cost profile and the same reason to chunk.
const snapChunk = 512

// The render scratch shrinks when it has been much larger than the live series
// count for several renders running: a burst to the cardinality cap otherwise
// sizes it to that peak for the process' life, and the eviction that reclaimed
// the series reclaims nothing here. The hysteresis is the point — the
// alternative to a stable scratch is one or two slice allocations per series
// per export, which is what it exists to avoid — so a workload that merely
// oscillates keeps its capacity, and only one that has genuinely shrunk pays a
// single reallocation.
const (
	snapShrinkFactor = 4  // shrink at under a quarter occupancy
	snapShrinkRuns   = 3  // ... sustained for this many renders
	snapShrinkFloor  = 64 // ... but never below this many slots
)

// Snapshotter is a render's reusable scratch: it copies every live series'
// values out under the Store's mutex, in chunks, so the caller can build the
// pdata payload WITHOUT the lock. S is the Store's series type, E the caller's
// snapshot element (what one series looks like as of the instant the render
// read it: label slices ALIASED — built once at admission, never mutated —
// and per-bucket arrays COPIED, because the receive path writes them under
// the mutex the build runs without).
//
// Why the build must not hold the lock: both aggregators fold a request into a
// series under it — spanmetrics per span with the ingest RPC waiting on it,
// servicegraph per edge from inside the pairing store's own mutex — so a render
// holding it across the whole cardinality-cap build was a stall on every trace
// push once per interval (measured 46.7 ms in servicegraph, ~89 ms in
// spanmetrics). Each aggregator used to carry its own copy of this algorithm,
// line for line — the pointer pass, the chunked copy, the grow/shrink/unalias
// switch and its constants — and only one of the two was tested. What stays the
// caller's is what genuinely differs: the element type and the three callbacks.
//
// It is NOT safe for concurrent use: the scratch is reused across renders, so
// the caller serializes them (each aggregator's renderMu, which covers renders
// that bypass Export as well — Export itself is serialized by the Store's
// exportGate). Lock order is that caller lock BEFORE the Store's mutex.
type Snapshotter[S Series, E any] struct {
	// CopyLocked copies s's values into e, reusing e's slices. It runs under
	// the Store's mutex (everything it reads is written under it) and must not
	// take it; the series is already marked Rendered.
	CopyLocked func(e *E, s S)
	// Fit readies e for a copy — pre-sizes its per-bucket arrays — so the copy
	// under the mutex is pure memmove. It runs WITHOUT the lock, on every slot a
	// render covers.
	Fit func(e *E)
	// Release drops whatever e ALIASES from a series (its label slice) while
	// keeping the arrays it owns. It is called on the slots past a smaller
	// render's length: those keep their bucket and exemplar CAPACITY — reusing
	// that is what the scratch is for — but must not keep the labels of series
	// the render no longer covers, or a burst to the cardinality cap followed
	// by mass stale eviction would pin every one of those label sets (a slice
	// plus its retained <= MaxLabelBytes strings, tens of MB at the cap) for
	// the process' life. Which field aliases is the caller's knowledge, so each
	// aggregator pins its OWN Release through its real constructor
	// (TestRenderScratchDoesNotPin* in servicegraph and spanmetrics): this
	// package's test runs a harness that supplies its own.
	Release func(e *E)

	ptrs  []S // the series order the copy walks, taken in one cheap pass
	snap  []E // the copied values, read lock-free by the build
	small int // consecutive renders that used far less of snap than it holds
}

// Take evicts the stale series and snapshots every survivor, marking each one
// rendered, and returns the snapshot for a lock-free build. The slice is the
// scratch itself: valid until the next Take.
//
// Hold 1 evicts and takes the series POINTERS in ONE walk
// (Store.LivePointersLocked) — one pointer write each, the cheapest pass that
// can exist over a map, and the only one a render cannot chunk, because a slice
// can be walked across lock releases and a map cannot. Sizing the scratch (and,
// on the first render, the per-slot arrays) stays outside the lock. Holds 2..n
// copy the values in snapChunk-series chunks, so one stall is bounded to a
// chunk rather than the whole cardinality cap. A request folded into a series
// between two chunks runs freely and puts that series back in Observed, exactly
// as one landing between the render and the delivery mark always could; a
// series ADMITTED during the walk is simply not in this payload, and its
// cumulative values ride the next one.
func (sn *Snapshotter[S, E]) Take(st *Store[S], now time.Time) []E {
	st.mu.Lock()
	ptrs := st.LivePointersLocked(sn.ptrs[:0], now)
	sn.ptrs = ptrs
	st.mu.Unlock()

	sn.grow(len(ptrs))
	snap := sn.snap
	st.eachChunked(ptrs, func(i int, s S) {
		st.MarkRenderedLocked(s)
		sn.CopyLocked(&snap[i], s)
	})
	clear(ptrs) // do not pin evicted series until the next render
	return snap[:len(ptrs)]
}

// grow sizes the scratch to n slots, reusing its buffers across renders while
// never pinning the labels (Release) or — once the scratch has stayed
// oversized long enough — the per-bucket arrays of series a smaller render
// does not cover. Called without the Store's lock.
func (sn *Snapshotter[S, E]) grow(n int) {
	switch {
	case cap(sn.snap) < n:
		grown := make([]E, n)
		copy(grown, sn.snap)
		sn.snap = grown
		sn.small = 0
	case sn.oversized(n):
		// Start over at the live size. Keeping the tail costs its per-bucket
		// arrays per slot — exemplars at 48 B each dominate — which measured
		// 24.3 MB still reachable in servicegraph after one burst to the 20000
		// cardinality cap and a mass stale eviction that left ONE series. The
		// scratch is worth having for the steady state, not for the peak the
		// process once saw.
		sn.snap = make([]E, n, max(snapShrinkFloor, 2*n))
		sn.small = 0
	default:
		whole := sn.snap[:cap(sn.snap)] // slots an earlier, larger render filled
		for i := n; i < len(whole); i++ {
			sn.Release(&whole[i])
		}
		sn.snap = sn.snap[:n]
	}
	for i := range sn.snap {
		sn.Fit(&sn.snap[i])
	}
}

// oversized reports whether the scratch has been oversized long enough to be
// worth rebuilding, keeping the run length that decides it.
func (sn *Snapshotter[S, E]) oversized(n int) bool {
	if cap(sn.snap) <= snapShrinkFloor || n*snapShrinkFactor >= cap(sn.snap) {
		sn.small = 0
		return false
	}
	sn.small++
	return sn.small >= snapShrinkRuns
}

// eachChunked calls f for every entry of ptrs (with its index), taking the
// series lock once per snapChunk of them rather than once for the whole slice.
// f runs under the lock and must not take it. It is the one chunk walker behind
// both chunked passes — Take's value copy and afterDelivered's mark — and
// TestSnapshotReleasesTheLockBetweenChunks / TestDeliveryMarkReleasesTheLockBetweenChunks
// pin it through each: the aggregators' own render-stall tests gate on the
// pdata BUILD moving under the lock, and passed unchanged with the copy taken
// in one hold.
func (st *Store[S]) eachChunked(ptrs []S, f func(i int, s S)) {
	for start := 0; start < len(ptrs); start += snapChunk {
		end := min(start+snapChunk, len(ptrs))
		st.mu.Lock()
		for i := start; i < end; i++ {
			f(i, ptrs[i])
		}
		st.mu.Unlock()
	}
}
