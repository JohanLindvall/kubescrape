package cumagg

import (
	"fmt"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/config"
)

// ParseStaleAfter reads an operator's staleAfter spelling. field names the
// config path for the error message ("serviceGraph.staleAfter"), def is what an
// unset value means.
//
// The three readings, in one place because they were once two:
//
//   - empty takes the default;
//   - "0" (or "0s") DISABLES eviction and keeps every series for the process'
//     life — an explicit choice, spelled explicitly;
//   - anything unparseable, and anything NEGATIVE, is an error naming the field
//     and the value.
//
// A negative value used to be clamped to zero by agent/spanmetrics, which meant
// `staleAfter: "-15m"` passed -check-config, passed the constructor, and
// silently turned the cardinality cap into the one-way latch that this field
// exists to prevent — the failure mode is invisible until a burst of label
// values has already blinded the aggregate. Refusing it costs a startup error;
// accepting it costs the feature.
//
// The default is returned alongside every error, so a constructor can fall back
// and keep aggregating: reporting a bad value is Validate's job (and
// -check-config runs it), and refusing to aggregate over it would take the
// telemetry down for a typo.
//
// The policy itself is config.Duration's — empty takes the default, "0" is
// legal under ZeroDisables, negative and unparseable are errors naming field
// and value. This wrapper adds only the two things the aggregators need on
// top: the default beside the error, and the disable-spelling hint. It used to
// re-implement the whole policy, which is exactly the drift internal/config
// exists to end (a third spelling of the negative-value error had already
// appeared).
func ParseStaleAfter(field, value string, def time.Duration) (time.Duration, error) {
	d, err := config.Duration(field, value, def, config.ZeroDisables())
	if err != nil {
		return def, fmt.Errorf("%w (use \"0\" to disable eviction)", err)
	}
	return d, nil
}

// ValidateBuckets checks configured histogram bucket bounds: every bound
// positive (the units are seconds) and the sequence strictly increasing.
// field names the config key for the message ("buckets", "histogramBuckets").
//
// Why validate at all: the OTLP spec requires a histogram's ExplicitBounds to
// be strictly increasing, and neither aggregator de-duplicates, so
// `buckets: [0.1, 0.1, 1]` shipped a histogram with a bucket that can never
// receive a value and a backend that either rejects the metric or renders a
// nonsense distribution. Why here: agent/spanmetrics and agent/servicegraph
// are the same aggregator shape and had already drifted on exactly this kind
// of rule once (see ParseStaleAfter) — spanmetrics' copy of this check was
// written admitting it duplicated servicegraph's.
func ValidateBuckets(field string, bounds []float64) error {
	var prev float64
	for i, b := range bounds {
		if b <= 0 {
			return fmt.Errorf("%s[%d] = %v (want > 0, in seconds)", field, i, b)
		}
		if i > 0 && b <= prev {
			return fmt.Errorf("%s must be strictly increasing (%v after %v)", field, b, prev)
		}
		prev = b
	}
	return nil
}

// Builtins is the set of label names an aggregator owns itself. A configured
// dimension that collides with one of them is DROPPED rather than rendered: the
// attributes go into one pcommon.Map, so a colliding key would overwrite the
// real label and two different series would render byte-identical label sets
// while their keys still held them apart — a duplicate series in one payload,
// which is a conflict downstream, not extra detail.
//
// agent/spanmetrics shipped exactly that through `dimensions: ["span.name"]`:
// an extra dimension resolves from span/resource ATTRIBUTES, and span.name is a
// span FIELD, so the duplicate resolved to "" and blanked the real label.
//
// # Where the guard runs
//
// One rule, one implementation, and it is applied at the single point where
// each aggregator's label NAMES become known — which is the earliest point at
// which it CAN run, and the only one that covers both the series key and the
// rendered attributes:
//
//   - spanmetrics' names are configured, so Filter runs once in its
//     constructor. Re-testing them per span would buy nothing (the set cannot
//     change after that) and cost a lookup per span per dimension.
//   - servicegraph's names arrive on each Edge, already prefixed client_ /
//     server_ by the processor, so Has runs where the key and the label set are
//     built. Deciding in its constructor is not possible: the names are not
//     there yet, and a hand-built Edge would walk straight past a guard that
//     only ever looked at the config.
//
// The timing differs because the two name sources differ; the RULE does not,
// and that is what drifted.
type Builtins map[string]bool

// NewBuiltins is the set of names.
func NewBuiltins(names ...string) Builtins {
	b := make(Builtins, len(names))
	for _, n := range names {
		b[n] = true
	}
	return b
}

// Has reports whether name is one the aggregator owns.
func (b Builtins) Has(name string) bool { return b[name] }

// Filter returns the configured names with the built-ins — and any repeats —
// removed, preserving order. The repeat check is the same defect read twice: a
// name listed twice would also render one attribute for two key positions.
func (b Builtins) Filter(names []string) []string {
	if len(names) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(names))
	out := make([]string, 0, len(names))
	for _, n := range names {
		if b.Has(n) || seen[n] {
			continue
		}
		seen[n] = true
		out = append(out, n)
	}
	return out
}
