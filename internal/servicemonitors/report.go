package servicemonitors

// The per-monitor report of fields kubescrape parsed but does not interpret.

import (
	"slices"
	"strings"
)

// maxIgnoredFields bounds the report ONE monitor produces, which is the third
// door of the same shape as maxRelabelIgnored and is here because bounding the
// second one alone would only have moved the growth up a level: every endpoint
// may contribute report entries carrying a DISTINCT tenant-chosen value
// (`action=`, `separator=`), and this function's whole output is joined into
// ONE log record by warnIgnored. When it was written the ENDPOINT LIST was
// unbounded, so a ~1.5 MiB CR of minimal endpoints each carrying a handful of
// distinct unsupported actions was tens of thousands of distinct entries,
// re-emitted on every edit of the CR. maxEndpointsPerMonitor now bounds that
// list, but at 128 — chosen against retained bytes and derivation CPU, not
// against what a person will read — so this ceiling still binds first and still
// earns its keep.
//
// A monitor whose report needs more than this many DISTINCT tenant-valued
// entries is not one an operator is going to read to the end anyway.
//
// The ceiling counts the TENANT-ECHO entries only — the `action=`/`separator=`
// arms, the only entries that embed a value and so the only ones whose number
// a CR can grow. Every other entry is a name from a closed compile-time set
// (the ignored-field names, the monitor-level guard rails, the refusal
// verdicts), so it is bounded by construction, and it is kept whole. Cutting the
// sorted list as one used to let a flood of separator echoes — which sort under
// `m` — push out the entries that say an endpoint yields NO targets at all:
// `path(oversize)`, `port(unset)`, `tlsConfig.*(oversize)` and, for a
// PodMonitor, the `podMetricsEndpoints(capped)` list cap, leaving the one
// warning line that names them naming only ordinary relabel clauses.
const maxIgnoredFields = 64

// ignoredFieldsCapped is the constant that stands for the remainder, sorted
// last on purpose (the tilde sorts after every field name kubescrape emits) so
// the entries it replaces are the alphabetic tail rather than an arbitrary
// prefix of the reader's attention.
const ignoredFieldsCapped = "~(more fields omitted)"

// isValueEcho reports whether a report entry embeds a tenant-chosen VALUE —
// "field=value", the spelling both echo arms of relabelChain use. No name in
// the closed set carries an '='.
func isValueEcho(entry string) bool { return strings.Contains(entry, "=") }

// IgnoredFields returns the distinct endpoint fields present on these
// endpoints that kubescrape does not interpret, sorted, with the tenant-valued
// entries bounded by maxIgnoredFields.
//
// kubescrape deliberately implements a SUBSET of the ServiceMonitor and
// PodMonitor spec, which is fine — but a partially-applied CR must not be
// silent. An operator who applies a monitor with `relabelings` renaming a
// label, or a `sampleLimit` guarding against a cardinality bomb, otherwise
// sees targets appear and never learns those clauses did nothing.
func IgnoredFields(eps []Endpoint) []string {
	seen := map[string]bool{}
	var out []string
	for _, ep := range eps {
		for _, f := range ep.Ignored {
			if !seen[f] {
				seen[f] = true
				out = append(out, f)
			}
		}
	}
	slices.Sort(out)
	// Sorted FIRST, then cut: the kept echoes have to be a property of the
	// monitor rather than of the order its endpoints happened to be walked, or
	// two identical CRs would report differently. The cut filters in place, so
	// what survives is a subsequence of a sorted list and stays sorted.
	kept, echoes := out[:0], 0
	for _, f := range out {
		if isValueEcho(f) {
			if echoes++; echoes > maxIgnoredFields {
				continue
			}
		}
		kept = append(kept, f)
	}
	if echoes > maxIgnoredFields {
		kept = append(kept, ignoredFieldsCapped)
	}
	return kept
}
