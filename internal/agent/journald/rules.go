package journald

// Keep/drop/sample rules and log-derived metrics for journal entries — the
// same cost levers the container-log tailer has.
//
// journald previously had logAttributes and logScrubbing but neither rules nor
// logMetrics, so the only way to sample down a node's kubelet/containerd
// chatter — which dominates a typical journal — was a Starlark transform, the
// escape hatch rather than the ergonomic path. The asymmetry was arbitrary:
// these are the same package-level pieces the tailer uses.

import (
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// entryResolver resolves metric label/value and rule keys for one journal
// record: the record's own attributes (line-derived + enriched) first, then the
// unit's resource attributes. Closures are bound once per convert; per record
// only the rec/res/sev fields change, so evaluation allocates none.
type entryResolver struct {
	rec, res pcommon.Map
	sev      string // lowercased severity text, for __severity__
	labelFn  func(string) string
	valueFn  func(string) (float64, bool)
	ruleFn   func(string) string
}

func newEntryResolver() *entryResolver {
	r := &entryResolver{}
	r.labelFn = r.label
	r.valueFn = r.value
	r.ruleFn = r.ruleLookup
	return r
}

// ruleLookup adds the synthetic __severity__ key (the enriched severity text,
// lowercased) to the usual record/resource chain, matching the tailer's rules.
func (r *entryResolver) ruleLookup(k string) string {
	if k == "__severity__" {
		return r.sev
	}
	return r.label(k)
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

// lowerSeverity normalises the severity text for __severity__ selectors.
func lowerSeverity(s string) string {
	switch s {
	case "":
		return ""
	case "TRACE", "trace":
		return "trace"
	case "DEBUG", "debug":
		return "debug"
	case "INFO", "info":
		return "info"
	case "WARN", "warn":
		return "warn"
	case "ERROR", "error":
		return "error"
	case "FATAL", "fatal":
		return "fatal"
	}
	out := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		out[i] = c
	}
	return string(out)
}
