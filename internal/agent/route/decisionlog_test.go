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

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

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
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
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
