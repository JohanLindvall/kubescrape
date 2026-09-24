package cumagg

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
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

// ResolveStaleAfter is ParseStaleAfter for a CONSTRUCTOR: it returns the value
// to run with and never an error, falling back to def — and it is not SILENT
// about the fallback. A constructor never refuses to aggregate over a bad value
// (Validate is what reports it, and -check-config runs that), but a start that
// has somehow got past Validate must not then apply a DIFFERENT eviction policy
// with nothing to grep for: with eviction off the cardinality cap becomes the
// one-way latch ParseStaleAfter exists to prevent.
//
// The line says "invalid", not "unparseable": a NEGATIVE value parses fine and
// is refused all the same, and a message contradicting its own error attribute
// sends the reader looking for a typo that is not there. Both aggregators' arms
// used to carry that wording separately.
func ResolveStaleAfter(field, value string, def time.Duration, log *slog.Logger) time.Duration {
	d, err := ParseStaleAfter(field, value, def)
	if err != nil {
		if log == nil {
			log = slog.Default()
		}
		log.Warn(field+" is invalid; using the default eviction age", "error", err, "staleAfter", d)
	}
	return d
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
//   - spanmetrics' names are configured, so Configure runs once in its
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

// Configure is what an aggregator's constructor calls on its configured
// dimension list: it returns the names with an empty name, the built-ins and
// any repeats removed, preserving order, and logs each drop at DEBUG under log
// (nil logs nothing). section is the config path ("traceMetrics.dimensions").
//
// The repeat check is the built-in defect read twice: a name listed twice would
// also render one attribute for two key positions. The empty name is the third,
// and the one that had drifted: a stray `- ` in a YAML list decodes to "",
// servicegraph refused it in a loop of its own while this rule — spanmetrics'
// — kept it, so every calls, size and duration point carried an attribute with
// an EMPTY KEY.
//
// Debug, not Warn: the WARNING for a drop is DimensionWarnings', which
// cmd/kubescrape-agent's configWarnings emits on -check-config and every start
// alike, so a constructor warning too printed each line twice. Both
// constructors spelled that apply-then-trace pair out for themselves; it is one
// walk here, so the kept list and the trace cannot disagree either.
//
// A nil Builtins is valid and applies the empty and repeat rules alone, which
// is what servicegraph's configured dimensions need: they are prefixed client_
// / server_ before they become labels, so no configured name can collide with
// a built-in there (Has guards the prefixed names per Edge instead).
func (b Builtins) Configure(section string, names []string, log *slog.Logger) []string {
	trace := log != nil && log.Enabled(context.Background(), slog.LevelDebug)
	kept, warns := b.filter(section, names, trace)
	for _, w := range warns {
		log.Debug(w)
	}
	return kept
}

// DimensionWarnings is one sentence for every name Configure drops, saying
// which entry and why; section is the config path it came from
// ("traceMetrics.dimensions"). Nothing when Configure drops nothing.
//
// It is PURE, and deliberately so: the constructors apply Configure at a real
// start only, so a warning logged there never reached -check-config — the dry
// run printed `config is valid` for a list the start then quietly shortened.
// cmd/kubescrape-agent's configWarnings emits these instead, from the same
// function on both paths, which is the promise that list makes.
func (b Builtins) DimensionWarnings(section string, names []string) []string {
	_, warns := b.filter(section, names, true)
	return warns
}

// filter is Configure and DimensionWarnings in one walk, so the rule and its
// report cannot disagree about what was dropped.
func (b Builtins) filter(section string, names []string, report bool) (kept, warns []string) {
	if len(names) == 0 {
		return nil, nil
	}
	seen := make(map[string]bool, len(names))
	kept = make([]string, 0, len(names))
	for i, n := range names {
		var why string
		switch {
		case n == "":
			why = "is empty: there is no attribute to resolve, and rendering it would put an attribute with an empty KEY on every data point"
		case b.Has(n):
			why = "collides with a label every data point already carries (" + b.list() + "): rendering it too would overwrite the real label and render two series identically"
		case seen[n]:
			why = "repeats an earlier entry"
		default:
			seen[n] = true
			kept = append(kept, n)
			continue
		}
		if report {
			warns = append(warns, fmt.Sprintf("%s[%d] %q is ignored: it %s", section, i, n, why))
		}
	}
	return kept, warns
}

// list is the built-in names, sorted, for a message.
func (b Builtins) list() string {
	return strings.Join(slices.Sorted(maps.Keys(b)), ",")
}
