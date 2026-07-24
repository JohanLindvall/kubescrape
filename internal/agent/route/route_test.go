package route

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type capDest struct {
	logs   []plog.Logs
	traces []ptrace.Traces
	err    error
}

func (c *capDest) ExportLogs(_ context.Context, ld plog.Logs) error {
	if c.err != nil {
		return c.err
	}
	c.logs = append(c.logs, ld)
	return nil
}
func (c *capDest) ExportMetrics(context.Context, pmetric.Metrics) error { return c.err }
func (c *capDest) ExportTraces(_ context.Context, td ptrace.Traces) error {
	if c.err != nil {
		return c.err
	}
	c.traces = append(c.traces, td)
	return nil
}

func nsLogs(namespaces ...string) plog.Logs {
	ld := plog.NewLogs()
	for _, ns := range namespaces {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("k8s.namespace.name", ns)
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("from " + ns)
	}
	return ld
}

func bodies(lds []plog.Logs) []string {
	var out []string
	for _, ld := range lds {
		rls := ld.ResourceLogs()
		for i := 0; i < rls.Len(); i++ {
			sl := rls.At(i).ScopeLogs().At(0)
			out = append(out, sl.LogRecords().At(0).Body().Str())
		}
	}
	return out
}

func TestRoutesSplitByNamespaceGlob(t *testing.T) {
	def, teamA := &capDest{}, &capDest{}
	r := New(def, []Destination{{Name: "team-a", Namespaces: []string{"team-a-*"}, Exporter: teamA}})

	if err := r.ExportLogs(context.Background(), nsLogs("team-a-prod", "other", "team-a-dev")); err != nil {
		t.Fatal(err)
	}
	if got := bodies(teamA.logs); len(got) != 2 || got[0] != "from team-a-prod" || got[1] != "from team-a-dev" {
		t.Fatalf("team-a got %v", got)
	}
	if got := bodies(def.logs); len(got) != 1 || got[0] != "from other" {
		t.Fatalf("default got %v", got)
	}
}

func TestAllDefaultForwardsUntouched(t *testing.T) {
	def := &capDest{}
	r := New(def, []Destination{{Name: "x", Namespaces: []string{"nomatch-*"}, Exporter: &capDest{}}})
	ld := nsLogs("a", "b")
	if err := r.ExportLogs(context.Background(), ld); err != nil {
		t.Fatal(err)
	}
	if len(def.logs) != 1 || def.logs[0].ResourceLogs().Len() != 2 {
		t.Fatal("all-default payload was split or copied")
	}
}

func TestRouteFailureFailsExport(t *testing.T) {
	def := &capDest{}
	bad := &capDest{err: errors.New("route down")}
	r := New(def, []Destination{{Name: "bad", Namespaces: []string{"team-*"}, Exporter: bad}})
	if err := r.ExportLogs(context.Background(), nsLogs("team-x", "other")); err == nil {
		t.Fatal("route failure must propagate for the producer's retry")
	}
	// The default part was still attempted (at-least-once per destination).
	if got := bodies(def.logs); len(got) != 1 || got[0] != "from other" {
		t.Fatalf("default got %v", got)
	}
}

func TestTracesRouting(t *testing.T) {
	def, teamA := &capDest{}, &capDest{}
	r := New(def, []Destination{{Name: "team-a", Namespaces: []string{"team-a"}, Exporter: teamA}})
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("k8s.namespace.name", "team-a")
	rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("s")
	if err := r.ExportTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	if len(teamA.traces) != 1 || len(def.traces) != 0 {
		t.Fatalf("traces routed wrong: teamA=%d def=%d", len(teamA.traces), len(def.traces))
	}
}
