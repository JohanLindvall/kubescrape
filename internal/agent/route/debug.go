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
	"path"
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

// why re-derives one resource's routing verdict for the narration. It
// deliberately does NOT call match: match's reporting arm counts
// obs.RouteUnknown and warns, and an explanation must never move an operator's
// counters or duplicate their warnings.
//
// idx is the destination index (-1 = default), reason names the branch taken,
// and ns/pat are whatever the branch had to show for itself.
func (r *Router) why(res pcommon.Resource) (idx int, ns, pat, reason string) {
	attrs := res.Attributes()
	if v, ok := attrs.Get(ScriptMarker); ok {
		want := v.Str()
		for i, d := range r.dests {
			if d.Name == want {
				return i, "", "", "scriptMarker"
			}
		}
		return -1, "", "", "scriptMarkerNamesNoRoute"
	}
	v, ok := attrs.Get(namespaceAttr)
	if !ok {
		// The commonest surprise, and the one a counter cannot distinguish: the
		// self-metrics, node and cadvisor-rollup resources carry no namespace at
		// all, so they can only ever be default — routing them needs a
		// script marker, not another glob.
		return -1, "", "", "noNamespaceAttribute"
	}
	ns = v.Str()
	for i, d := range r.dests {
		for _, p := range d.Namespaces {
			if ok, _ := path.Match(p, ns); ok {
				return i, ns, p, "namespaceGlob"
			}
		}
	}
	return -1, ns, "", "noGlobMatched"
}

// explainExport narrates one export's routing. groups is split's answer, or nil
// for the all-default fast path (where no group slice was ever built).
//
// The counts come from the SAME derivation the samples do, so the line cannot
// claim a split the router did not take.
func (r *Router) explainExport(signal string, n int, res func(int) pcommon.Resource, groups []int) {
	var (
		byRoute  = make([]int, len(r.dests))
		reasons  = map[string]int{}
		samples  []string
		def, rtd int
	)
	for i := 0; i < n; i++ {
		idx, ns, pat, reason := r.why(res(i))
		reasons[reason]++
		if idx >= 0 {
			byRoute[idx]++
			rtd++
		} else {
			def++
		}
		if len(samples) < debugSamples {
			samples = append(samples, sample(r.destName(idx), ns, pat, reason))
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
		// all — not merely that every group happened to be the default one.
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
	all := []string{"scriptMarker", "namespaceGlob", "noGlobMatched", "noNamespaceAttribute", "scriptMarkerNamesNoRoute"}
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
