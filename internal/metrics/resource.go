package metrics

import (
	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// A resource's IDENTITY is the label set resourceIdentity builds — nothing
// else. resourceString renders that set and resAccum folds it, so the hashed
// identity and the serialized identity are two views of ONE derivation. This
// matters because the two are compared in different places: the fold is the
// series map's key, the string is what the sample carries into the export, and
// when they disagree the map lookup SUCCEEDS on a key that renders as something
// else. Nothing counts that, nothing logs it — two live samples share one
// serialized identity, which exports as duplicate same-timestamp points and,
// for a histogram, one merged point whose buckets summed both.
//
// The rules live in labels.set: an empty key or value is dropped, a value is
// cut to maxLabelValueBytes, and a repeated key resolves LAST-WINS. Restating
// them in a hand-rolled fold is what this package has already got wrong twice
// (the truncation mirror, and the duplicate-key contract below), so the fold
// goes through set() and resourceAccum's one-pass fast path is licensed by a
// PROOF rather than by a caller's promise.
//
// Values are cut with a reslice rather than set()'s copy: this label set is
// folded or serialized and dropped inside the call, so there is no retained
// view to pin the caller's strings — which is the only thing that copy is for.
func resourceIdentity(res pcommon.Map, into labels) labels {
	ls := into
	res.Range(func(k string, v pcommon.Value) bool {
		s := v.AsString()
		ls = ls.set(k, s[:truncLabelCut(s)])
		return true
	})
	return ls
}

// resAccum folds a resource's identity label set into the order-independent
// accumulator that keys the series, in the resource hash domain
// (combineResHash — see there for why the resource has its own).
func (l labels) resAccum() xxh3.Uint128 {
	var rk xxh3.Uint128
	for _, e := range l {
		rk = xor128(rk, combineResHash(strHash(e.key), strHash(e.value)))
	}
	return rk
}

// resourceIdentityAttrs bounds the fast path's uniqueness proof: a resource
// wider than this cannot be proved key-unique in one pass, so it materializes
// the identity instead — which is correct for every input. Nothing is refused
// and nothing is mishandled, so this is a performance knob and not a
// correctness one.
//
// It is otlpingest's maxObservedResourceAttrs because that is where the one
// BOUNDED wire path stops observing at all: nothing arriving through the
// ingest LOG chain can pay the fallback. It is NOT a bound on what reaches
// this store. The unbounded door is EmitDirect — a transform script's
// emit_metric folding the SENDER's own resource at whatever width the sender
// chose, with nothing between the two that caps or dedupes it; resourceAccum
// documents it, because the same door is why that fold had to become total.
// It is the reason this is a fallback and not a refusal.
//
// What the fallback costs, measured rather than promised. It is QUADRATIC and
// it ALLOCATES:
//
//	attributes           64 (fast)      65      128      256     512     1024
//	fold                     6.7us    28us     87us    303us   1.4ms    5.0ms
//	allocations                  0       1        1        1       1        1
//	bytes                        0  2.3KiB   4.9KiB   9.5KiB  18KiB    32KiB
//
// The allocation count is the stable figure and the one to hold this to:
// exactly one heap allocation per fold, the identity slice — 32 bytes per
// attribute before size-class rounding, i.e. sized by the sender rather than
// by us. The times are medians of interleaved runs on a loaded machine (n=10
// through 256, n=5 above) and are indicative only; the SHAPE is what to read,
// ~4x per doubling of width (measured 2.8-4.5x), because resourceIdentity
// resolves last-wins through set(), which scans what it has already collected:
// n(n-1)/2 key comparisons, each dereferencing a string. The 65-attribute
// column additionally pays the fast path's wasted first pass — 64 keys hashed,
// folded and probed, then thrown away.
//
// Note what the table does NOT buy, because it is what widening it would be
// worth: the fast path is quadratic too (the probe is the same n(n-1)/2
// compares — see resourceAccum), so 64 is a ~4x step on a curve that was
// already rising, not a cliff between flat and quadratic. Doubling the table
// to 128 MOVES that step rather than removing it, and charges every fold in
// the fleet a 2 KiB frame clear instead of 1 KiB (+25 to +45ns per fold,
// measured both in isolation and on a 7-attribute resource) on a path where
// the bounded senders are capped at 64 and the extra half can never be used.
// So the table is sized to the bound that actually exists, and what pays the
// fallback is a resource an operator grew past 64 attributes with
// static/template attrs — folded once per file per flush — or a script
// emitting under a wide pushed one.
const resourceIdentityAttrs = 64

// resourceAccum is the order-independent hash accumulator of a resource's
// attributes, matching labels.hashAccum so a resource and extra resource labels
// fold into one series key. It is a TOTAL function of the identity above: any
// resource, whatever its provenance, hashes as the identity it RENDERS.
//
// The fold is XOR, which is blind to even multiplicity — a pair folded twice
// CANCELS — so a resource repeating a key cannot simply be folded entry by
// entry: {k=v, k=v} would hash as the EMPTY resource and merge with an
// unrelated attribute-less one, and {k=p, k=q} would fold both pairs while
// rendering only q. That used to be a CONTRACT on the caller ("res must not
// repeat a key"), honoured by an ordering inside another package's loop
// (otlpingest.dedupeResourceKeys) and by picking the right one of two
// near-identical functions. It is arithmetic now, for two reasons the review
// that changed it turned up: the contract's blast radius was wider than the one
// function that stated it — resLabelsAccum below cancelled through
// pcommon.Map.Get, which is FIRST-wins against a LAST-wins render, so even the
// door the contract told you to use gave a wrong answer once a resourceLabel
// overrode a duplicated key — and a wire resource may legally repeat a key
// (OTLP encodes attributes as a repeated KeyValue and pdata does not dedupe on
// decode), so the shape is reachable from an unauthenticated listener.
//
// THE DOOR IT REACHES THIS STORE THROUGH IS EmitDirect: a transform script's
// emit_metric folds whatever resource the payload in front of it carries, and
// for an ingested METRICS or TRACES push that is the SENDER's own — at
// whatever width, with whatever repeated keys, the sender chose. otlpingest's
// dedupeResourceKeys does NOT cover it: that runs on the LOG chain only, and
// there only for a resource narrow enough to be OBSERVED, since it sits behind
// the same 64-attribute cap — so the widest resources are precisely the ones
// it skips. Hence a resource arriving here can be undeduped AND unbounded in
// width, which is why resourceIdentityAttrs is a fallback and not a refusal.
// Every other producer hands over a resource built from a Go map, which cannot
// repeat a key.
//
// The fast path folds in ONE pass and materializes nothing, which it is allowed
// to do only while it can PROVE the resource key-unique: the probe reuses the
// key hash the fold is already computing, so it adds no hashing. What it does
// add, measured against the same fold without it, is not "one compare" — it is
// two costs, and the second is not a compare at all:
//
//   - a QUADRATIC scan. "Every key seen so far" totals n(n-1)/2 compares for
//     the resource — 21 at 7 attributes, 2016 at 64 — at ~2ns each. Measured
//     against the unprobed fold: +70ns on 208ns and +107ns on 260ns for two
//     7-attribute resources, +4.3us on 2.3us at 64, where the probe therefore
//     costs about twice the fold it licenses.
//   - a FIXED clear of the seen table, paid at EVERY width including a
//     one-attribute resource: 1 KiB of frame that the prologue zeroes — 64
//     16-byte stores, emitted before the first attribute is looked at, which
//     the assembly shows plainly. ~30ns in isolation, and the same ~30ns is
//     the ENTIRE probe delta at one attribute, where no compare runs at all.
//
// The two cross at around six attributes: below that the clear is the whole
// cost, above it the scan runs away with it. So at the widths agent resources
// actually have the probe is not one compare but tens of nanoseconds of frame
// clear plus tens of compares, and the term that matters as resources widen is
// the quadratic one.
//
// Timings are indicative (medians of interleaved runs on a loaded machine); the
// compare counts are arithmetic and the fold stays 0 allocations either way.
//
// Note what the probe's exactness does and does not affect — a hash collision
// between two DIFFERENT keys, or a resource too wide for the table, only sends
// this through the materializing definition, which is correct for every input.
// The proof can fail safe; it cannot be wrong.
//
// On a key-unique resource the result is bit-for-bit what the entry-by-entry
// fold produced before the proof existed, which is every resource any agent
// builds (they come from Go maps) — no series churns.
func resourceAccum(res pcommon.Map) xxh3.Uint128 {
	var seen [resourceIdentityAttrs]xxh3.Uint128
	n := 0
	unique := true
	var rk xxh3.Uint128
	res.Range(func(k string, v pcommon.Value) bool {
		s := v.AsString()
		if k == "" || s == "" {
			// set() drops these, so the hash must too — and an entry that does
			// not fold cannot collide with one that does, so it stays out of
			// the proof as well.
			return true
		}
		hk := strHash(k)
		for i := 0; i < n; i++ {
			if seen[i] == hk {
				unique = false
				return false
			}
		}
		if n == len(seen) {
			unique = false // too wide to prove; not proof of a duplicate
			return false
		}
		seen[n] = hk
		n++
		// The TRUNCATED value, as set() would keep it (a reslice, no copy):
		// hashing the full value while rendering the cut one split one rendered
		// identity into two live samples.
		rk = xor128(rk, combineResHash(hk, strHash(s[:truncLabelCut(s)])))
		return true
	})
	if !unique {
		return resourceIdentity(res, make(labels, 0, res.Len())).resAccum()
	}
	return rk
}

// resourceFold is a resource together with what an observation needs derived
// from it: the accumulator that keys the series, and — for a caller whose extra
// resource labels may REPLACE one of its keys — the identity that override
// cancel resolves against. They travel together because they are resolved
// together, once per resource (Bind), and read per line.
//
// ident is a HINT and never a contract: it is what resourceIdentity returns for
// res, and a fold that leaves it nil keys the very same series one allocation
// later, because identity() materializes it. The asymmetry is deliberate. This
// package has twice been bitten by a fold whose correctness rested on its
// caller handing it the right thing (the key-uniqueness contract resourceAccum
// now proves for itself, and this cancel reading the FIRST value of a repeated
// key while the render kept the LAST), and the rule taken from that is that a
// caller here may make an observation slower, never wrong.
//
// ident is READ-ONLY and shared by every observation against the resource:
// nothing may set() into it, since an append that fits its spare capacity would
// rewrite the identity under every other observation.
type resourceFold struct {
	res   pcommon.Map
	accum xxh3.Uint128
	ident labels
}

// identity is the resource's last-wins label set — for each key, the value it
// contributes to the RENDERED identity, which is exactly what an override has
// to cancel. Bind resolves it once per resource and hands it forward here; a
// caller that had no reason to (it declares no resource labels, or holds only
// the map) pays for it on the spot rather than being trusted to have done it.
func (r resourceFold) identity() labels {
	if r.ident != nil {
		return r.ident
	}
	return resourceIdentity(r.res, make(labels, 0, r.res.Len()))
}

// resLabelsAccum folds the extra resource labels with the same override
// semantics resourceString applies: a label replacing an existing resource key
// subtracts the replaced pair, so the hash keys the MERGED set — hashing both
// pairs made {svc:foo}+override svc=bar collide-or-diverge from {svc:bar}
// inconsistently with its serialized identity, yielding duplicate data points
// within one exported resource group.
//
// It takes the resource's IDENTITY rather than the resource, for a correctness
// reason and a cost one. Correctness: it must cancel the value the resource
// RENDERS, and on a resource repeating a key the two disagree — the identity is
// last-wins while pcommon.Map.Get returns the FIRST match, so reading the map
// with Get folded out a pair the fold had never folded in, leaving a hash that
// keys a series whose rendered resource shows something else (pinned by
// TestOverrideCancelsTheValueTheResourceRenders). Cost: this runs per
// OBSERVATION, i.e. per log line, while the value a key renders is a fact about
// the resource — resolving it off the map here means walking the whole map per
// line (only the last occurrence decides, so no early return is possible), and
// the per-line cost then grows with the resource's attribute count for a fact
// that did not change. The identity's own lookup stops at the first match,
// set() having already resolved the repeats.
//
// Both sides come through set() — ident from resourceIdentity, extra from
// metricRule.observe's rbuf, which is why extra cannot itself repeat a key and
// each cancel runs at most once per key — so the values on both are already
// truncated as the render truncates them.
func resLabelsAccum(ident, extra labels) xxh3.Uint128 {
	var rk xxh3.Uint128
	for _, e := range extra {
		if e.key == "" || e.value == "" {
			continue
		}
		hk := strHash(e.key)
		if s, _ := ident.get(e.key); s != "" {
			// The subtraction must cancel resourceAccum's addition exactly, so
			// it folds the value the identity KEEPS.
			rk = xor128(rk, combineResHash(hk, strHash(s))) // self-inverse: folds the replaced pair back OUT
		}
		rk = xor128(rk, combineResHash(hk, strHash(e.value)))
	}
	return rk
}

// resourceString serializes a resource's attributes plus any extra resource
// labels into the sorted label string used to key and later emit the per-metric
// resource. Only materialized on a series' cold path.
func resourceString(res pcommon.Map, extra labels) string {
	ls := resourceIdentity(res, make(labels, 0, res.Len()+len(extra)))
	for _, e := range extra {
		ls = ls.set(e.key, e.value)
	}
	return ls.String()
}
