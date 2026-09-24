package route

// "Why did this tenant's logs go to the default chain instead of route X?" was
// unanswerable from this package's output: it had one Warn (a script naming an
// unknown route), one Error path and no Debug at all, so a route that matched
// nothing and a route that was never consulted produced the identical silence.

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// debugRouter builds a router whose Debug output the test can read.
func debugRouter(def Exporter, dests []Destination) (*Router, func() string) {
	var buf bytes.Buffer
	r := New(def, dests)
	r.log = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return r, buf.String
}

// The split case: the line must name the attribute that was read, the glob that
// matched, the destination each group went to, and — for the resources that
// fell through — WHICH of the two default reasons applied. A count of
// "defaulted" alone is the report that sends an operator to fix the wrong
// thing: a missing k8s.namespace.name needs a script marker, a non-matching
// namespace needs a different glob.
func TestRoutingDecisionIsExplainedAtDebug(t *testing.T) {
	r, dump := debugRouter(&capDest{}, []Destination{
		{Name: "tenant-a", Namespaces: []string{"team-a-*"}, Exporter: &capDest{}},
		{Name: "tenant-b", Namespaces: []string{"team-b"}, Exporter: &capDest{}},
	})
	ld := nsLogs("team-a-one", "kube-system", "team-b")
	// A resource with no namespace attribute at all — the self-metrics / node /
	// cadvisor-rollup shape, which can only ever be default.
	rl := ld.ResourceLogs().AppendEmpty()
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("no namespace")

	if err := r.ExportLogs(context.Background(), ld); err != nil {
		t.Fatal(err)
	}
	out := dump()
	for _, want := range []string{
		"signal=logs",
		"resources=4",
		"attr=k8s.namespace.name",
		"routed=2",
		"defaulted=2",
		`byRoute="tenant-a=1,tenant-b=1"`,
		"namespaceGlob=2",
		"noGlobMatched=1",
		"noNamespaceAttribute=1",
		// The worked examples carry the matching glob and the reason a resource
		// fell through, which is the half a histogram cannot express.
		"team-a-one:tenant-a[namespaceGlob=team-a-*]",
		"kube-system:default[noGlobMatched]",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the routing decision line does not carry %q:\n%s", want, out)
		}
	}
	// slog writes level= itself; a second pair of that name destroys the
	// record's severity for a logfmt reader (found live on the cluster).
	for line := range strings.SplitSeq(strings.TrimSpace(out), "\n") {
		if n := strings.Count(line, " level="); n != 1 {
			t.Errorf("line has %d ` level=` pairs, want exactly 1 (slog's own): %q", n, line)
		}
	}
}

// The all-default answer is the one an operator hits when a route is
// misconfigured, and it must say that no split happened at all — otherwise the
// absence of a per-route part reads exactly like a delivered-but-empty group.
func TestAllDefaultExportSaysWhyAtDebug(t *testing.T) {
	r, dump := debugRouter(&capDest{}, []Destination{
		{Name: "tenant-a", Namespaces: []string{"team-a-*"}, Exporter: &capDest{}},
	})
	if err := r.ExportLogs(context.Background(), nsLogs("prod", "prod")); err != nil {
		t.Fatal(err)
	}
	out := dump()
	for _, want := range []string{"no split", "signal=logs", "routed=0", "defaulted=2", "noGlobMatched=2", `byRoute="tenant-a=0"`} {
		if !strings.Contains(out, want) {
			t.Errorf("the all-default line does not carry %q:\n%s", want, out)
		}
	}
}

// Metrics and traces take the same three call sites, and a signal-less line
// cannot be correlated with the producer that emitted the payload.
func TestEverySignalNamesItselfInTheDecisionLine(t *testing.T) {
	for _, tc := range []struct {
		signal string
		export func(*Router) error
	}{
		{"logs", func(r *Router) error { return r.ExportLogs(context.Background(), nsLogs("team-a-one")) }},
		{"metrics", func(r *Router) error { return r.ExportMetrics(context.Background(), nsMetrics("team-a-one")) }},
		{"traces", func(r *Router) error { return r.ExportTraces(context.Background(), nsTraces("team-a-one")) }},
	} {
		r, dump := debugRouter(&capDest{}, []Destination{
			{Name: "tenant-a", Namespaces: []string{"team-a-*"}, Exporter: &capDest{}},
		})
		if err := tc.export(r); err != nil {
			t.Fatalf("%s: %v", tc.signal, err)
		}
		if out := dump(); !strings.Contains(out, "signal="+tc.signal) {
			t.Errorf("%s export did not name its signal:\n%s", tc.signal, out)
		}
	}
}

// The Debug line's reasons and worked examples come from decide asked to
// narrate, the routing from decide through match — and the two differ in how
// they reach the namespace half: the narration scans the globs (the memo keeps
// no patterns), the split asks the memo. This pins that asking for the
// narration never changes WHERE a resource goes, on every branch, on a routed
// Router and on the destination-less one (the self chain's preRoute), with
// match run twice so the memo's answer is compared as well as the scan's.
func TestNarratedDecisionIsTheRoutingDecision(t *testing.T) {
	type resource struct {
		name   string
		attrs  func(pcommon.Map)
		reason string // the branch decide must name on the routed Router
	}
	resources := []resource{
		{"marker naming a route beats the namespace", func(m pcommon.Map) {
			m.PutStr(ScriptMarker, "tenant-b")
			m.PutStr(namespaceAttr, "team-a-one")
		}, "scriptMarker"},
		{"marker naming no route", func(m pcommon.Map) { m.PutStr(ScriptMarker, "nope") }, "scriptMarkerNamesNoRoute"},
		{"non-string marker", func(m pcommon.Map) { m.PutInt(ScriptMarker, 7) }, "scriptMarkerNamesNoRoute"},
		{"no namespace attribute", func(pcommon.Map) {}, "noNamespaceAttribute"},
		{"glob hit", func(m pcommon.Map) { m.PutStr(namespaceAttr, "team-a-one") }, "namespaceGlob"},
		{"exact hit on the second route", func(m pcommon.Map) { m.PutStr(namespaceAttr, "team-b") }, "namespaceGlob"},
		{"glob hit past a malformed glob", func(m pcommon.Map) { m.PutStr(namespaceAttr, "team-c") }, "namespaceGlob"},
		{"glob miss", func(m pcommon.Map) { m.PutStr(namespaceAttr, "kube-system") }, "noGlobMatched"},
		{"non-string namespace", func(m pcommon.Map) { m.PutInt(namespaceAttr, 5) }, "noGlobMatched"},
	}
	routers := map[string]*Router{
		"routed": New(&capDest{}, []Destination{
			{Name: "tenant-a", Namespaces: []string{"team-a-*"}, Exporter: &capDest{}},
			{Name: "tenant-b", Namespaces: []string{"team-b"}, Exporter: &capDest{}},
			{Name: "tenant-c", Namespaces: []string{"[", "team-c"}, Exporter: &capDest{}},
		}),
		"destination-less": New(&capDest{}, nil),
	}
	before := obs.RouteUnknown.Value()
	for rname, r := range routers {
		for _, tc := range resources {
			res := pcommon.NewResource()
			tc.attrs(res.Attributes())
			v := r.decide(res.Attributes(), true)
			for pass := range 2 { // the scan, then the memo
				if want, _ := r.match(res, false); v.idx != want {
					t.Errorf("%s router, %s (pass %d): the narrated decision says destination %d, match() routes to %d",
						rname, tc.name, pass, v.idx, want)
				}
			}
			if rname == "routed" && v.reason != tc.reason {
				t.Errorf("%s router, %s: the narrated decision names branch %q, want %q", rname, tc.name, v.reason, tc.reason)
			}
		}
	}
	if got := obs.RouteUnknown.Value() - before; got != 0 {
		t.Errorf("explaining a routing decision moved kubescrape_routed_unknown_total by %v; the narration must move no counter", got)
	}
}

// The decision line's COUNTS are split's own answer, never the narration's
// re-run of decide: whatever the narrated decision says, the
// routed/defaulted/byRoute numbers must be the destinations the router actually
// used. Driven with answers the narrated decision disagrees with, so a line
// counting its own re-derivation shows up as the wrong numbers.
func TestDecisionLineCountsWhatSplitDecided(t *testing.T) {
	dests := []Destination{{Name: "tenant-a", Namespaces: []string{"team-a-*"}, Exporter: &capDest{}}}
	ld := nsLogs("team-a-one", "kube-system") // decided: tenant-a, then default
	res := func(i int) pcommon.Resource { return ld.ResourceLogs().At(i).Resource() }

	for _, tc := range []struct {
		name   string
		groups []int
		whole  int
		want   []string
	}{
		{"split", []int{-1, -1}, -1, []string{"routed=0", "defaulted=2", `byRoute="tenant-a=0"`, "routing split this export"}},
		{"fast path to one route", nil, 0, []string{"routed=2", "defaulted=0", `byRoute="tenant-a=2"`, "route=tenant-a"}},
		{"fast path to the default chain", nil, -1, []string{"routed=0", "defaulted=2", `byRoute="tenant-a=0"`, "default chain (no split)"}},
	} {
		r, dump := debugRouter(&capDest{}, dests)
		r.explainExport("logs", 2, res, tc.groups, tc.whole)
		out := dump()
		for _, want := range tc.want {
			if !strings.Contains(out, want) {
				t.Errorf("%s: the decision line does not carry %q — it is counting something other than split's answer:\n%s", tc.name, want, out)
			}
		}
		// The REASONS stay the narrated decision's: they name the branch, not the count.
		if !strings.Contains(out, "namespaceGlob=1") || !strings.Contains(out, "noGlobMatched=1") {
			t.Errorf("%s: the reasons histogram lost the decided branches:\n%s", tc.name, out)
		}
	}
}

// slog evaluates arguments eagerly, so the narration must be built ONLY when
// Debug is on: with it off the router would walk every resource of every
// payload, deriving and joining strings nothing renders, on the export path of
// every producer on the node.
//
// The all-default fast path is covered by TestAllDefaultExportIsAllocationFree
// (an unguarded call there costs 14-19 allocations per export). This is the
// SPLIT path's half, where the copy allocates anyway and only a budget can tell
// the narration apart from it.
func TestTheDecisionLineIsNotBuiltBelowDebug(t *testing.T) {
	var buf bytes.Buffer
	r := New(&capDest{}, []Destination{{Name: "tenant-a", Namespaces: []string{"team-a-*"}, Exporter: &capDest{}}})
	r.log = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ld := nsLogs("team-a-one", "kube-system")
	if err := r.ExportLogs(context.Background(), ld); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Errorf("routing logged at Info:\n%s", buf.String())
	}
	if testrace.Enabled {
		return // the detector changes escape analysis and adds bookkeeping allocations
	}
	// splitInfoAllocs is the split path's own cost (the per-destination copies)
	// with nothing narrated. Measured, not derived: raise it only with a reason
	// that is not "the explanation started running at Info".
	const splitInfoAllocs = 24
	got := testing.AllocsPerRun(20, func() {
		if err := r.ExportLogs(context.Background(), ld); err != nil {
			t.Fatal(err)
		}
	})
	if got > splitInfoAllocs {
		t.Errorf("split export at Info = %.0f allocs, want <= %d: the Debug narration is being built for a record nothing renders", got, splitInfoAllocs)
	}
}

// nsMetrics/nsTraces are nsLogs' siblings for the other two signals.
func nsMetrics(namespaces ...string) pmetric.Metrics {
	md := pmetric.NewMetrics()
	for _, ns := range namespaces {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("k8s.namespace.name", ns)
		m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName("m")
		m.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(1)
	}
	return md
}

func nsTraces(namespaces ...string) ptrace.Traces {
	td := ptrace.NewTraces()
	for _, ns := range namespaces {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("k8s.namespace.name", ns)
		rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("s")
	}
	return td
}
