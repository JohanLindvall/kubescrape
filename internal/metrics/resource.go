package metrics

import (
	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// resourceAccum is the order-independent hash accumulator of a resource's
// attributes (rendered as strings), matching labels.hashAccum so a resource and
// extra resource labels can be folded into one series key.
//
// CONTRACT: res must not repeat a key. The fold is XOR, so a pair folded twice
// CANCELS — an undeduped {k=v, k=v} hashes as the EMPTY resource and merges
// with an unrelated attribute-less one. Every agent-built resource comes from a
// Go map and satisfies this for free; a WIRE resource does not (OTLP encodes
// attributes as a repeated KeyValue and pdata does not dedupe on decode), which
// is why both wire doors fold through set() first — otlpingest.dedupeResourceKeys
// before Bind, and uniqueResourceAccum for a script's resource. Do not call this
// on a resource of unknown provenance; call uniqueResourceAccum.
//
// Values are hashed at their TRUNCATED length (truncLabelCut — a reslice, no
// copy): resourceString goes through set(), which truncates, and the hashed
// identity must be the rendered identity. Hashing the full value while
// rendering the cut one made two resources differing only past the bound two
// live samples sharing one serialized identity — duplicate points every
// export, merged corrupt points for histograms.
func resourceAccum(res pcommon.Map) xxh3.Uint128 {
	var rk xxh3.Uint128
	res.Range(func(k string, v pcommon.Value) bool {
		s := v.AsString()
		if k == "" || s == "" {
			return true // resourceString's set drops these; the hash must too
		}
		hk, hv := strHash(k), strHash(s[:truncLabelCut(s)])
		rk = xor128(rk, combineResHash(hk, hv))
		return true
	})
	return rk
}

// resLabelsAccum folds the extra resource labels with the same override
// semantics resourceString applies: a label replacing an existing resource
// key subtracts the replaced pair, so the hash keys the MERGED set — hashing
// both pairs made {svc:foo}+override svc=bar collide-or-diverge from
// {svc:bar} inconsistently with its serialized identity, yielding duplicate
// data points within one exported resource group.
func resLabelsAccum(res pcommon.Map, extra labels) xxh3.Uint128 {
	var rk xxh3.Uint128
	for _, e := range extra {
		if e.key == "" || e.value == "" {
			continue
		}
		hk := strHash(e.key)
		if v, ok := res.Get(e.key); ok {
			if s := v.AsString(); s != "" {
				// The subtraction must cancel resourceAccum's addition exactly,
				// so it folds the SAME truncated view.
				hv := strHash(s[:truncLabelCut(s)])
				rk = xor128(rk, combineResHash(hk, hv)) // self-inverse: folds the replaced pair back OUT
			}
		}
		// e.value came through set() and is already truncated.
		hv := strHash(e.value)
		rk = xor128(rk, combineResHash(hk, hv))
	}
	return rk
}

// resourceString serializes a resource's attributes plus any extra resource
// labels into the sorted label string used to key and later emit the per-metric
// resource. Only materialized on a series' cold path.
func resourceString(res pcommon.Map, extra labels) string {
	ls := make(labels, 0, res.Len()+len(extra))
	res.Range(func(k string, v pcommon.Value) bool {
		ls = ls.set(k, v.AsString())
		return true
	})
	for _, e := range extra {
		ls = ls.set(e.key, e.value)
	}
	return ls.String()
}
