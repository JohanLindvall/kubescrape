// Package logchain holds the pieces every log-producing pipeline shares.
//
// The tailer, journald, the Kubernetes-events reader and the Azure-diagnostics
// reader all run the same chain over a record — scrub, lift attributes,
// enrich, log-metrics, keep/drop rules — and each had its own copy of the key
// resolver that chain needs. Four byte-identical copies plus a fifth
// open-coded one in the agent's -test-config harness meant every future
// synthetic key had five places to land, and the copies had already drifted
// where it mattered (one producer never named its scope, and two JSON field
// extractors disagreed about integer precision).
package logchain

import (
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// SeverityKey is the synthetic key exposing a record's enriched severity to
// keep/drop rules. It is available to RULES only: metric labels have no such
// key in production, so a resolver used for labels must not invent one.
const SeverityKey = "__severity__"

// Resolver resolves metric label/value keys and rule keys for ONE record: the
// record's own attributes (line-derived + enriched) first, then the resource's
// (k8s metadata, the journal unit, the ARM resource).
//
// The three closures are bound once, at construction; per record only the
// Record/Resource/Severity fields change, so evaluating a record allocates no
// closures. Callers keep one Resolver per flush/convert and re-point it.
type Resolver struct {
	// Record and Resource are the attribute maps consulted, in that order.
	Record, Resource pcommon.Map
	// Severity is the lowercased severity text SeverityKey resolves to.
	Severity string

	labelFn func(string) string
	valueFn func(string) (float64, bool)
	ruleFn  func(string) string
}

// New returns a Resolver with its closures bound.
func New() *Resolver {
	r := &Resolver{}
	r.labelFn = r.label
	r.valueFn = r.value
	r.ruleFn = r.ruleKey
	return r
}

// Set re-points the resolver at one record. Cheaper than allocating a new one
// per record, which is the point of keeping the closures bound.
func (r *Resolver) Set(rec, res pcommon.Map, severity string) {
	r.Record, r.Resource, r.Severity = rec, res, severity
}

// LabelFn resolves a metric LABEL key (no synthetic keys).
func (r *Resolver) LabelFn() func(string) string { return r.labelFn }

// ValueFn resolves a metric VALUE key, numerically.
func (r *Resolver) ValueFn() func(string) (float64, bool) { return r.valueFn }

// RuleFn resolves a RULE key, including the synthetic SeverityKey.
func (r *Resolver) RuleFn() func(string) string { return r.ruleFn }

func (r *Resolver) lookup(k string) (pcommon.Value, bool) {
	if v, ok := r.Record.Get(k); ok {
		return v, true
	}
	return r.Resource.Get(k)
}

func (r *Resolver) label(k string) string {
	if v, ok := r.lookup(k); ok {
		return v.AsString()
	}
	return ""
}

func (r *Resolver) ruleKey(k string) string {
	if k == SeverityKey {
		return r.Severity
	}
	return r.label(k)
}

func (r *Resolver) value(k string) (float64, bool) {
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

// LowerSeverity normalises a record's severity text for SeverityKey selectors.
//
// One implementation, because there were five: journald's allocation-free
// version, the tailer's (which alone handled WARNING, so the same log line
// selected differently depending on which pipeline carried it) and three plain
// strings.ToLower calls that allocate on every record for input that is almost
// always already one of these constants.
func LowerSeverity(s string) string {
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
	case "WARNING", "warning":
		return "warning"
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
