package route

// A payload whose every resource goes to ONE route is the usual shape of a
// routed tenant's push, and it used to be copied resource by resource into a
// fresh per-destination payload — 2,915 allocations and 158 KB for 100 KSM
// resources — although nothing needed splitting and the destination only reads
// what it is handed, exactly as the default chain's uncopied fast path already
// relies on.

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// capMetricsTraces records metrics and traces payloads as handed over.
type capMetricsTraces struct {
	metrics []pmetric.Metrics
	traces  []ptrace.Traces
}

func (c *capMetricsTraces) ExportLogs(context.Context, plog.Logs) error { return nil }
func (c *capMetricsTraces) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	c.metrics = append(c.metrics, md)
	return nil
}
func (c *capMetricsTraces) ExportTraces(_ context.Context, td ptrace.Traces) error {
	c.traces = append(c.traces, td)
	return nil
}

func TestSingleRoutePayloadIsForwardedUncopied(t *testing.T) {
	def := &capDest{}
	dest := &capMetricsTraces{}
	logDest := &capDest{}
	r := New(def, []Destination{{Name: "tenant-a", Namespaces: []string{"team-a-*"}, Exporter: dest}})
	rl := New(def, []Destination{{Name: "tenant-a", Namespaces: []string{"team-a-*"}, Exporter: logDest}})
	ctx := context.Background()

	ld := nsLogs("team-a-one", "team-a-two", "team-a-one")
	if err := rl.ExportLogs(ctx, ld); err != nil {
		t.Fatal(err)
	}
	if len(logDest.logs) != 1 || logDest.logs[0] != ld {
		t.Errorf("logs: the route got %d payloads, not the caller's own — the one-route payload was copied", len(logDest.logs))
	}

	md := nsMetrics("team-a-one", "team-a-two")
	if err := r.ExportMetrics(ctx, md); err != nil {
		t.Fatal(err)
	}
	if len(dest.metrics) != 1 || dest.metrics[0] != md {
		t.Errorf("metrics: the route got %d payloads, not the caller's own", len(dest.metrics))
	}

	td := nsTraces("team-a-one", "team-a-two")
	if err := r.ExportTraces(ctx, td); err != nil {
		t.Fatal(err)
	}
	if len(dest.traces) != 1 || dest.traces[0] != td {
		t.Errorf("traces: the route got %d payloads, not the caller's own", len(dest.traces))
	}
	if len(def.logs) != 0 || len(def.traces) != 0 {
		t.Errorf("the default chain received a part of a payload routed whole: logs=%d traces=%d", len(def.logs), len(def.traces))
	}

	// Not mutated: the producer re-sends this same object on retry.
	if got := bodies([]plog.Logs{ld}); len(got) != 3 || got[1] != "from team-a-two" {
		t.Errorf("the caller's payload changed: %v", got)
	}
}

// A marker still forces the copy even when every resource names the same route:
// the marker must be stripped before anything is sent, and only a COPY may be.
func TestMarkedSingleRoutePayloadIsStillCopiedAndStripped(t *testing.T) {
	dest := &capDest{}
	r := New(&capDest{}, []Destination{{Name: "tenant-a", Namespaces: []string{"nothing-*"}, Exporter: dest}})
	ld := nsLogs("team-b", "team-c")
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		ld.ResourceLogs().At(i).Resource().Attributes().PutStr(ScriptMarker, "tenant-a")
	}
	if err := r.ExportLogs(context.Background(), ld); err != nil {
		t.Fatal(err)
	}
	if len(dest.logs) != 1 || dest.logs[0] == ld {
		t.Fatalf("a marked payload must reach the route as a COPY (got %d payloads, identity=%v)",
			len(dest.logs), len(dest.logs) == 1 && dest.logs[0] == ld)
	}
	if _, ok := dest.logs[0].ResourceLogs().At(0).Resource().Attributes().Get(ScriptMarker); ok {
		t.Errorf("%s reached the route destination", ScriptMarker)
	}
	if _, ok := ld.ResourceLogs().At(0).Resource().Attributes().Get(ScriptMarker); !ok {
		t.Error("the marker was stripped from the CALLER's payload")
	}
}

// The uncopied forward must cost the router nothing, like the default one.
func TestSingleRouteExportIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector changes escape analysis and adds bookkeeping allocations")
	}
	dest := &noopDest{}
	r := New(&noopDest{}, []Destination{{Name: "tenant-a", Namespaces: []string{"team-*"}, Exporter: dest}})
	ctx := context.Background()
	for _, n := range []int{1, 100, 4000} {
		md := routedMetrics(n)
		if err := r.ExportMetrics(ctx, md); err != nil { // warm the namespace memo
			t.Fatal(err)
		}
		got := testing.AllocsPerRun(20, func() {
			if err := r.ExportMetrics(ctx, md); err != nil {
				t.Fatal(err)
			}
		})
		if got > 0 {
			t.Errorf("%d resources all routed to one destination: %.0f allocs/export, want 0 — the payload is being copied", n, got)
		}
	}
	if dest.n == 0 {
		t.Fatal("the route received nothing")
	}
}

// The Debug line must not say "split" for an export that was not split, nor
// "default" for one that went to a route.
func TestSingleRouteExportSaysSoAtDebug(t *testing.T) {
	r, dump := debugRouter(&capDest{}, []Destination{
		{Name: "tenant-a", Namespaces: []string{"team-a-*"}, Exporter: &capDest{}},
	})
	if err := r.ExportLogs(context.Background(), nsLogs("team-a-one", "team-a-two")); err != nil {
		t.Fatal(err)
	}
	out := dump()
	for _, want := range []string{"to one route (no split, no copy)", "route=tenant-a", "routed=2", "defaulted=0"} {
		if !strings.Contains(out, want) {
			t.Errorf("the one-route line does not carry %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "split this export") {
		t.Errorf("an export forwarded whole was narrated as a split:\n%s", out)
	}
}
