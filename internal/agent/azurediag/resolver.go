package azurediag

// The metric/rule key resolver, mirroring the tailer's metricResolver and
// journald's entryResolver exactly: record attributes first, then the
// resource, with the synthetic __severity__ available to RULES only (the
// metric label resolver has no such key in production, so it must not have
// one here either).

import (
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

type entryResolver struct {
	rec, res pcommon.Map
	sev      string
	labelFn  func(string) string
	valueFn  func(string) (float64, bool)
	ruleFn   func(string) string
}

func newEntryResolver() *entryResolver {
	r := &entryResolver{}
	r.labelFn = r.label
	r.valueFn = r.value
	r.ruleFn = r.rule
	return r
}

func (r *entryResolver) lookup(k string) (pcommon.Value, bool) {
	if v, ok := r.rec.Get(k); ok {
		return v, true
	}
	return r.res.Get(k)
}

func (r *entryResolver) label(k string) string {
	if v, ok := r.lookup(k); ok {
		return v.AsString()
	}
	return ""
}

func (r *entryResolver) rule(k string) string {
	if k == "__severity__" {
		return r.sev
	}
	return r.label(k)
}

func (r *entryResolver) value(k string) (float64, bool) {
	v, ok := r.lookup(k)
	if !ok {
		return 0, false
	}
	switch v.Type() {
	case pcommon.ValueTypeDouble:
		return v.Double(), true
	case pcommon.ValueTypeInt:
		return float64(v.Int()), true
	default:
		f, err := strconv.ParseFloat(v.AsString(), 64)
		return f, err == nil
	}
}
