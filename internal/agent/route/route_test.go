package route

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
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

// The router must NOT mutate its input: the ingest batcher retries the same
// payload object in place, and the spanmetrics tap Consumes it after the
// forward — an in-place split loses the retried batch and blinds the tap.
func TestRouterDoesNotMutateInput(t *testing.T) {
	def, teamA := &capDest{}, &capDest{}
	r := New(def, []Destination{{Name: "team-a", Namespaces: []string{"team-a-*"}, Exporter: teamA}})

	ld := nsLogs("team-a-prod", "other")
	if err := r.ExportLogs(context.Background(), ld); err != nil {
		t.Fatal(err)
	}
	if ld.ResourceLogs().Len() != 2 {
		t.Fatalf("input emptied: %d resources left of 2", ld.ResourceLogs().Len())
	}
	// A second export of the SAME object (the batcher's in-place retry) must
	// re-split identically, not forward an emptied payload.
	def2, teamA2 := &capDest{}, &capDest{}
	r2 := New(def2, []Destination{{Name: "team-a", Namespaces: []string{"team-a-*"}, Exporter: teamA2}})
	if err := r2.ExportLogs(context.Background(), ld); err != nil {
		t.Fatal(err)
	}
	if len(bodies(teamA2.logs)) != 1 || len(bodies(def2.logs)) != 1 {
		t.Fatalf("retry re-split lost data: teamA=%v def=%v", bodies(teamA2.logs), bodies(def2.logs))
	}
}

// One route's permanent rejection must not condemn the whole payload: the
// tailer DROPS a permanently rejected batch and advances, so classifying a
// mixed failure permanent discarded the default-destined records a retry
// would have delivered.
func TestMixedFailureIsNotPermanent(t *testing.T) {
	def := &capDest{err: errors.New("connection refused")} // transient
	bad := &capDest{err: &otlpexport.HTTPStatusError{Code: 400}}
	r := New(def, []Destination{{Name: "team-a", Namespaces: []string{"team-a-*"}, Exporter: bad}})

	err := r.ExportLogs(context.Background(), nsLogs("default", "team-a-web"))
	if err == nil {
		t.Fatal("a failed destination must fail the export")
	}
	if otlpexport.IsPermanent(err) {
		t.Error("a mixed failure classified permanent; the default chain's records would be dropped unsent")
	}

	// Every destination permanent: the verdict IS the payload's.
	def.err = &otlpexport.HTTPStatusError{Code: 413}
	if err := r.ExportLogs(context.Background(), nsLogs("default", "team-a-web")); !otlpexport.IsPermanent(err) {
		t.Error("an all-permanent failure classified transient; the batch would retry forever")
	}
}
