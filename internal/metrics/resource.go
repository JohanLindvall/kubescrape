package metrics

import (
	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// resourceAccum is the order-independent hash accumulator of a resource's
// attributes (rendered as strings), matching labels.hashAccum so a resource and
// extra resource labels can be folded into one series key.
func resourceAccum(res pcommon.Map) resKey {
	var rk resKey
	res.Range(func(k string, v pcommon.Value) bool {
		s := v.AsString()
		if k == "" || s == "" {
			return true // resourceString's set drops these; the hash must too
		}
		hk, hv := xxhash.Sum64String(k), xxhash.Sum64String(s)
		rk.accum += combineResHash(hk, hv)
		rk.check += combineResCheck(hk, hv)
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
func resLabelsAccum(res pcommon.Map, extra labels) resKey {
	var rk resKey
	for _, e := range extra {
		if e.key == "" || e.value == "" {
			continue
		}
		hk := xxhash.Sum64String(e.key)
		if v, ok := res.Get(e.key); ok {
			if s := v.AsString(); s != "" {
				hv := xxhash.Sum64String(s)
				rk.accum -= combineResHash(hk, hv)
				rk.check -= combineResCheck(hk, hv)
			}
		}
		hv := xxhash.Sum64String(e.value)
		rk.accum += combineResHash(hk, hv)
		rk.check += combineResCheck(hk, hv)
	}
	return rk
}

// resKey carries a resource's two order-independent hash accumulators (the
// series key contribution and the collision-check contribution).
type resKey struct{ accum, check uint64 }

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
