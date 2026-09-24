package promscrape

// The package's label-slice helpers. A sample's labels are a []Label in
// exposition order, never a map: the parser emits them that way, a scrape has
// a handful per sample, and a linear scan over them is cheaper than any index.
// Every reader here resolves a name through labelValue, i.e. FIRST match —
// which is why both fronts refuse a repeated label name outright rather than
// letting the first and the last occurrence disagree downstream.

import (
	"math"
	"strconv"
)

// labelValue returns the value of the first label called name, "" if none. A
// missing label and an empty one are therefore the same thing, which is the
// Prometheus reading (a missing label matches against "").
func labelValue(labels []Label, name string) string {
	for _, l := range labels {
		if l.Name == name {
			return l.Value
		}
	}
	return ""
}

// hasLabel reports whether labels already carry name: the duplicate-name check
// appendProtoLabels applies to wire label pairs, and the collision check for the
// SYNTHESIZED component labels (`le`, `quantile`, in withComponent), which
// appendProtoLabels cannot see because they are appended after it returns.
func hasLabel(labels []Label, name string) bool {
	for i := range labels {
		if labels[i].Name == name {
			return true
		}
	}
	return false
}

// labelOrName resolves a relabel source label: `__name__` is the metric name,
// anything else the sample's label value.
func labelOrName(name string, labels []Label, key string) string {
	if key == "__name__" {
		return name
	}
	return labelValue(labels, key)
}

// appendLabelsExcept appends every label but the one called except to dst.
func appendLabelsExcept(dst []Label, labels []Label, except string) []Label {
	for _, l := range labels {
		if l.Name != except {
			dst = append(dst, l)
		}
	}
	return dst
}

// labelFloat parses a numeric label value — `le` on a histogram bucket, or
// `quantile` on a summary. Both decide where a point LANDS, so a nonsense value
// is not a nonsense label, it is a corrupted distribution.
//
// strconv.ParseFloat happily returns NaN for "NaN"/"nan" and ±Inf for
// "Inf"/"Infinity", and this was the only gate on either label. A single junk
// row from a scraped target — which is whatever a pod annotation or a
// ServiceMonitor points at, not necessarily anything the operator wrote —
// therefore entered the bucket accumulator with an unorderable bound, and the
// sort that establishes the cumulative bucket order put it wherever the
// comparison happened to fall. +Inf is the ONE exception and is legitimate: it
// is the histogram's mandatory overflow bucket.
func labelFloat(labels []Label, name string) (float64, bool) {
	v, err := strconv.ParseFloat(labelValue(labels, name), 64)
	if err != nil {
		return 0, false // missing label or unparseable value
	}
	if math.IsNaN(v) || math.IsInf(v, -1) {
		return 0, false
	}
	return v, true
}
