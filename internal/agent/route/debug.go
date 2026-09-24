package route

// The export-time answer to the one question this package gets asked during an
// incident: "why did this tenant's telemetry go to the default chain instead of
// route X?" Nothing here changes a decision — it NARRATES the decision already
// taken, at Debug, once per EXPORT.
//
// Per export and not per resource, deliberately: the router sits on the export
// path of every producer on the node (a KSM scrape is one payload of a few
// thousand resources), so a line per resource would be a fleet-wide flood and a
// per-resource render would be a real cost on a path that is otherwise a copy.
// A summary plus a few worked examples answers the question at a fixed price,
// and the examples are what make it actionable: a count of "went to default"
// does not say whether the namespace attribute was missing or whether the glob
// simply did not match, and those have opposite fixes.
//
// Every walk here is behind ONE Enabled check taken per export (slog evaluates
// arguments eagerly, so an unguarded summary would be paid at Info too).

import (
	"context"
	"log/slog"
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// namespaceAttr is the resource attribute routing keys on. Named here so the
// Debug line can SAY which attribute it read — an operator whose resources
// carry a differently-spelled namespace key otherwise sees only "no match".
const namespaceAttr = "k8s.namespace.name"

// debugSamples bounds the worked examples on the line: enough to show the
// shape of the decision, few enough that the record stays one readable line
// however many resources the payload holds.
const debugSamples = 3

// logger is the router's logger. nil means the process default, which is what
// production uses: both binaries install a logfmt handler with slog.SetDefault
// before anything routes, so nothing has to be wired through New (whose
// signature is shared with the pre-route fork in main). Tests set the field.
func (r *Router) logger() *slog.Logger {
	if r.log != nil {
		return r.log
	}
	return slog.Default()
}

// debugEnabled reports whether the narration below is worth building. One call
// per export.
func (r *Router) debugEnabled() bool {
	return r.logger().Enabled(context.Background(), slog.LevelDebug)
}

// explainExport narrates one export's routing. groups and whole are split's
// answer: groups is nil on the uncopied fast path (where no group slice was
// ever built), which forwards the whole payload to whole — a route index, or
// -1 for the default chain.
//
// The counts (routed, defaulted, byRoute) are split's OWN answer — groups[i],
// or whole for every resource on the fast path — so the line cannot claim a
// split the router did not take. The reasons and the worked examples come from
// decide, the one derivation split's match also runs, asked to narrate.
func (r *Router) explainExport(signal string, n int, res func(int) pcommon.Resource, groups []int, whole int) {
	var (
		byRoute  = make([]int, len(r.dests))
		reasons  = map[string]int{}
		samples  []string
		def, rtd int
	)
	for i := range n {
		v := r.decide(res(i).Attributes(), true)
		idx := whole
		if groups != nil {
			idx = groups[i]
		}
		reasons[v.reason]++
		if idx >= 0 {
			byRoute[idx]++
			rtd++
		} else {
			def++
		}
		if len(samples) < debugSamples {
			samples = append(samples, sample(r.destName(idx), v.ns, v.pat, v.reason))
		}
	}
	args := []any{
		"signal", signal,
		"resources", n,
		"attr", namespaceAttr,
		"routed", rtd,
		"defaulted", def,
		"reasons", joinCounts(reasonOrder(reasons), reasons),
		"examples", strings.Join(samples, " "),
	}
	if len(r.dests) > 0 {
		names := make([]string, len(r.dests))
		for i, d := range r.dests {
			names[i] = d.Name
		}
		args = append(args, "byRoute", joinCountsIdx(names, byRoute))
	}
	if groups == nil {
		// Said explicitly: the fast path forwards the caller's payload
		// UNCOPIED, so an operator reading this line knows no split happened at
		// all — not merely that every group happened to go to one place.
		if whole >= 0 {
			args = append(args, "route", r.destName(whole))
			r.logger().Debug("routing sent this whole export to one route (no split, no copy)", args...)
			return
		}
		r.logger().Debug("routing sent this whole export to the default chain (no split)", args...)
		return
	}
	r.logger().Debug("routing split this export across destinations", args...)
}

// destName renders a destination index for the line; -1 is the default chain.
func (r *Router) destName(idx int) string {
	if idx < 0 || idx >= len(r.dests) {
		return "default"
	}
	return r.dests[idx].Name
}

// sample renders one worked example as ns:route[reason] (with the matching glob
// where there was one), which stays a single logfmt-safe token.
func sample(dest, ns, pat, reason string) string {
	if ns == "" {
		ns = "-"
	}
	if pat != "" {
		return ns + ":" + dest + "[" + reason + "=" + pat + "]"
	}
	return ns + ":" + dest + "[" + reason + "]"
}

// reasonOrder gives the reason histogram a stable rendering order, so two
// consecutive lines can be compared by eye (and by a test) rather than
// re-shuffled by map iteration.
func reasonOrder(m map[string]int) []string {
	all := []string{reasonScriptMarker, reasonNamespaceGlob, reasonNoGlobMatched, reasonNoNamespace, reasonMarkerNamesNoRoute}
	out := make([]string, 0, len(m))
	for _, k := range all {
		if m[k] > 0 {
			out = append(out, k)
		}
	}
	return out
}

func joinCounts(keys []string, m map[string]int) string {
	var sb strings.Builder
	for _, k := range keys {
		if sb.Len() > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(strconv.Itoa(m[k]))
	}
	return sb.String()
}

func joinCountsIdx(names []string, counts []int) string {
	var sb strings.Builder
	for i, n := range names {
		if sb.Len() > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(n)
		sb.WriteByte('=')
		sb.WriteString(strconv.Itoa(counts[i]))
	}
	return sb.String()
}
