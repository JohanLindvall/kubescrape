package events

// The observed set: the positional proof that lets a re-delivered event tell
// the per-record chain it has already been counted (logchain.Input.Observed).
// A watch restart re-delivers every buffered entry and they convert afresh;
// without the proof the operator's log metrics stepped once per restart.

// obsKey identifies an event OCCURRENCE exactly: the object's UID and the
// resourceVersion of the write that produced this delivery.
//
// It is a positional proof in the sense logchain.Input.Observed demands, and
// the two halves are both load-bearing. The resourceVersion is etcd's revision
// for that write, so a REPEAT — which Kubernetes aggregates into the same
// object re-sent as Modified with a growing count, the "BackOff x47" case — is
// a different revision and a different key: a genuine new occurrence is never
// mistaken for a re-delivery. The UID keeps two objects apart if a cluster ever
// hands out a revision this reader has seen on another key. An entry missing
// BOTH (a synthetic event with no metadata) claims nothing, because the
// asymmetry runs one way: under-claiming re-counts, over-claiming destroys
// observations invisibly.
type obsKey struct {
	uid string
	rv  string
}

func (k obsKey) valid() bool { return k.uid != "" || k.rv != "" }

// tracksObservation reports whether anything the chain counts is configured.
// With nothing counted per record there is nothing to observe twice, so the
// set is not maintained at all and costs a nil map lookup. The answer is
// logchain.Config.CountsRecords — the predicate that sits next to the gate it
// mirrors — so it cannot drift from what Input.Observed suppresses (a
// hand-written copy here once left enrichment out, the one on by default).
// Scrub counts although the convert chain runs without it: the reader
// scrubs at ingest, which reads the set too.
func (r *Reader) tracksObservation() bool {
	return r.cfg.Chain.CountsRecords()
}

// wasObserved reports whether an earlier convert already ran the per-record
// chain over this occurrence.
func (r *Reader) wasObserved(k obsKey) bool {
	if !k.valid() {
		return false
	}
	_, ok := r.observed[k]
	return ok
}

// markObserved records that the chain has run over these occurrences. It is
// called with the whole rendered batch AFTER the render, never entry by entry
// inside it: two entries of one batch carrying the same key (which no delivery
// path produces, but nothing structurally forbids) must both be counted, and
// marking as we went would suppress the second — the over-claiming direction,
// which destroys observations invisibly.
//
// Past the cap the set is CLEARED rather than grown. The live need is one key
// per un-settled batch entry, which retainCap already bounds; anything above
// that is keys for entries a re-delivery never brought back, and clearing them
// costs at most one re-observation of what is still buffered — the safe
// direction, and the behaviour that existed before the set did.
func (r *Reader) markObserved(entries []entry) {
	if !r.tracksObservation() {
		return
	}
	if r.observed == nil {
		r.observed = make(map[obsKey]struct{}, len(entries))
	} else if len(r.observed)+len(entries) > r.retainCap() {
		clear(r.observed)
	}
	for i := range entries {
		if k := entries[i].okey; k.valid() {
			r.observed[k] = struct{}{}
		}
	}
}

// forgetObserved drops the keys of entries LEAVING the batch: settled
// (delivered, all-dropped or permanently rejected) or shed.
//
// Delivery is at-least-once and observation is once per DELIVERY, so a settled
// entry that a later relist replays is a new delivery and is observed again;
// keeping its key would also make the set grow without any bound the batch
// provides. What must NOT be forgotten is a stream restart's clear of the batch
// (stream), which is precisely the case where the same delivery comes back.
func (r *Reader) forgetObserved(entries []entry) {
	if len(r.observed) == 0 {
		return
	}
	for i := range entries {
		delete(r.observed, entries[i].okey)
	}
}
