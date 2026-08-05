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
	"fmt"
	"math"
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
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
	// Lifted are resource attributes extracted from THIS line by
	// logAttributes, consulted between the two.
	//
	// They are a separate slice only because of where the tailer sits: every
	// other producer builds its exported resource first and hands the merged
	// map over as Resource, so their rules and metric labels see line-lifted
	// keys. The tailer resolves against the FILE's base resource while the
	// lifted attributes are still a pending []Attr driving the flush grouping
	// — so the identical logAttributes + logs.rules config selected
	// differently depending on which producer carried the line, which is
	// exactly what "one chain, one order" is supposed to rule out. A slice
	// scan, not a map: rules number in the handful and the scan runs only for
	// keys a rule or label actually asks for.
	Lifted []logattrs.Attr
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
	r.Lifted = nil
}

// SetLifted adds this line's logAttributes-lifted RESOURCE attributes, which
// rank between the record's and the resource's. Call after Set.
func (r *Resolver) SetLifted(lifted []logattrs.Attr) { r.Lifted = lifted }

// LabelFn resolves a metric LABEL key (no synthetic keys).
func (r *Resolver) LabelFn() func(string) string { return r.labelFn }

// ValueFn resolves a metric VALUE key, numerically.
func (r *Resolver) ValueFn() func(string) (float64, bool) { return r.valueFn }

// RuleFn resolves a RULE key, including the synthetic SeverityKey.
func (r *Resolver) RuleFn() func(string) string { return r.ruleFn }

// liftedValue returns the last lifted attribute for k (last wins, matching
// how Put applies them to a resource).
func (r *Resolver) liftedValue(k string) (any, bool) {
	for i := len(r.Lifted) - 1; i >= 0; i-- {
		if r.Lifted[i].Key == k {
			return r.Lifted[i].Val, true
		}
	}
	return nil, false
}

func (r *Resolver) label(k string) string {
	if v, ok := r.Record.Get(k); ok {
		return v.AsString()
	}
	if v, ok := r.liftedValue(k); ok {
		return attrString(v)
	}
	if v, ok := r.Resource.Get(k); ok {
		return v.AsString()
	}
	return ""
}

// attrString renders a lifted value the way pcommon.Value.AsString would —
// EXACTLY, floats included. A bare FormatFloat('f') diverged from pcommon's
// ES6 rendering outside [1e-6, 1e21): the same lifted field minted label
// "0.0000005" through this path (a lifted RESOURCE attribute, read from the
// lifted slice) and "5e-7" through the record path (target: log, read back
// off pdata by AsString) — two series for one value, the divergence the
// shared resolver exists to end.
func attrString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		return strconv.FormatBool(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return floatAsString(x)
	case nil:
		return ""
	default:
		return fmt.Sprint(x)
	}
}

// floatAsString replicates pcommon's unexported float64AsString (pdata
// pcommon/value.go): ES6 number-to-string — 'f' inside [1e-6, 1e21), 'e' with
// an unpadded exponent outside, NaN/Infinity spelled out. Pinned against the
// real pcommon rendering by TestAttrStringMatchesPcommon.
func floatAsString(f float64) string {
	if math.IsNaN(f) {
		return "NaN"
	}
	if math.IsInf(f, 1) {
		return "Infinity"
	}
	if math.IsInf(f, -1) {
		return "-Infinity"
	}
	scratch := [64]byte{}
	b := scratch[:0]
	abs := math.Abs(f)
	format := byte('f')
	if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	b = strconv.AppendFloat(b, f, format, -1, 64)
	if format == 'e' {
		// clean up e-09 to e-9
		n := len(b)
		if n >= 4 && b[n-4] == 'e' && b[n-3] == '-' && b[n-2] == '0' {
			b[n-2] = b[n-1]
			b = b[:n-1]
		}
	}
	return string(b)
}

func (r *Resolver) ruleKey(k string) string {
	if k == SeverityKey {
		return r.Severity
	}
	return r.label(k)
}

// value resolves a numeric key in the SAME order label() resolves a string
// one: the record's own attributes, then the line-lifted ones, then the
// resource. It ranked lifted attributes above the record's, so one key could
// take its label from the record and its VALUE from the resource — the two
// halves of a log-derived metric disagreeing about which attribute they mean.
//
// Ranking is by PRESENCE, not by convertibility: a present-but-non-numeric
// value is a miss for this key, not a reason to fall through to a lower rank.
func (r *Resolver) value(k string) (float64, bool) {
	if v, ok := r.Record.Get(k); ok {
		return numeric(v)
	}
	if lv, ok := r.liftedValue(k); ok {
		switch x := lv.(type) {
		case float64:
			return x, true
		case int64:
			return float64(x), true
		case string:
			f, err := strconv.ParseFloat(x, 64)
			return f, err == nil
		default:
			return 0, false
		}
	}
	v, ok := r.Resource.Get(k)
	if !ok {
		return 0, false
	}
	return numeric(v)
}

// numeric converts a pcommon.Value the way value() needs it.
func numeric(v pcommon.Value) (float64, bool) {
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
// always already one of these constants. The constant list covers BOTH
// vocabularies that reach it: enrich's levels (trace..fatal) and journald's
// syslog texts (emerg/alert/crit/err/notice — stamped lowercase, and the
// pipeline whose allocation-free copy this replaced must stay free per entry).
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
	case "NOTICE", "notice":
		return "notice"
	case "WARN", "warn":
		return "warn"
	case "WARNING", "warning":
		return "warning"
	case "ERROR", "error":
		return "error"
	case "ERR", "err":
		return "err"
	case "CRIT", "crit":
		return "crit"
	case "ALERT", "alert":
		return "alert"
	case "EMERG", "emerg":
		return "emerg"
	case "FATAL", "fatal":
		return "fatal"
	}
	// Anything else lowers only if something is actually upper: returning the
	// input unchanged keeps unusual-but-already-lower severities free of the
	// per-record allocation too.
	upper := -1
	for i := 0; i < len(s); i++ {
		if 'A' <= s[i] && s[i] <= 'Z' {
			upper = i
			break
		}
	}
	if upper < 0 {
		return s
	}
	out := make([]byte, len(s))
	copy(out, s[:upper])
	for i := upper; i < len(s); i++ {
		c := s[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		out[i] = c
	}
	return string(out)
}
