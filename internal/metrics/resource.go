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
		hk, hv := xxhash.Sum64String(k), xxhash.Sum64String(v.AsString())
		rk.accum += combineHash(hk, hv)
		rk.check += combineCheck(hk, hv)
		return true
	})
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
