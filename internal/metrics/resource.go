package metrics

import (
	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// resourceAccum is the order-independent hash accumulator of a resource's
// attributes (rendered as strings), matching labels.hashAccum so a resource and
// extra resource labels can be folded into one series key.
func resourceAccum(res pcommon.Map) uint64 {
	var h uint64
	res.Range(func(k string, v pcommon.Value) bool {
		h ^= combineHash(xxhash.Sum64String(k), xxhash.Sum64String(v.AsString()))
		return true
	})
	return h
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
