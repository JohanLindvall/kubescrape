package route

// What an operator can see when a tenant's destination is not working. Routing
// is fan-out to somebody else's collector, so a failing route is the one an
// operator is LEAST likely to be watching — and until now the only evidence was
// the ABSENCE of kubescrape_routed_payload_parts_total, which reads exactly
// like a route nothing matched.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

func capturedLogger() (*slog.Logger, func() string) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h), buf.String
}

func TestAFailingDestinationIsCountedAndNamed(t *testing.T) {
	dest := &capDest{err: errors.New("dial tcp 10.1.2.3:4317: connect: connection refused")}
	r := New(&capDest{}, []Destination{{Name: "tenant-a", Namespaces: []string{"app-*"}, Exporter: dest}})
	log, dump := capturedLogger()
	r.health[0] = otlpexport.NewFailureReporter(log, "a routing destination", "route", "tenant-a")

	beforeFail := obs.RouteFailures.WithLabelValues("tenant-a", "logs").Value()
	beforeOK := obs.Routed.WithLabelValues("tenant-a", "logs").Value()
	if err := r.ExportLogs(context.Background(), nsLogs("app-one")); err == nil {
		t.Fatal("a failing destination must fail the export")
	}
	if got := obs.RouteFailures.WithLabelValues("tenant-a", "logs").Value() - beforeFail; got != 1 {
		t.Errorf("kubescrape_routed_failures_total{tenant-a,logs} delta = %v, want 1", got)
	}
	if got := obs.Routed.WithLabelValues("tenant-a", "logs").Value() - beforeOK; got != 0 {
		t.Errorf("a refused part must not count as routed, delta = %v", got)
	}
	out := dump()
	for _, want := range []string{"route=tenant-a", "signal=logs", "class=transient", "nothing is listening"} {
		if !strings.Contains(out, want) {
			t.Errorf("the failure line does not carry %q:\n%s", want, out)
		}
	}
}

// A route whose destination cannot take traces is a WIRING fault that repeats
// on every export and produced no counter at all.
func TestARouteThatCannotTakeTracesIsCounted(t *testing.T) {
	// logsOnly implements Exporter but not TracesExporter.
	r := New(&capDest{}, []Destination{{Name: "logs-only", Namespaces: []string{"app-*"}, Exporter: logsOnlyDest{}}})
	before := obs.RouteFailures.WithLabelValues("logs-only", "traces").Value()
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("k8s.namespace.name", "app-one")
	rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("s")
	if err := r.ExportTraces(context.Background(), td); err == nil {
		t.Fatal("expected an error for a destination that cannot take traces")
	}
	if got := obs.RouteFailures.WithLabelValues("logs-only", "traces").Value() - before; got != 1 {
		t.Errorf("kubescrape_routed_failures_total{logs-only,traces} delta = %v, want 1", got)
	}
}

// A script routing to a name no route defines delivers the records to the
// DEFAULT chain — nothing is dropped, so the only symptom is silent
// mis-tenanting. The name is script-chosen and therefore stays off the metric.
func TestUnknownScriptRouteIsCountedWithoutPuttingTheNameInALabel(t *testing.T) {
	r := New(&capDest{}, []Destination{{Name: "tenant-a", Namespaces: []string{"app-*"}, Exporter: &capDest{}}})
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(ScriptMarker, "typo-tenant")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("x")

	before := obs.RouteUnknown.Value()
	if err := r.ExportLogs(context.Background(), ld); err != nil {
		t.Fatalf("an unknown route must degrade to the default chain, not fail: %v", err)
	}
	if got := obs.RouteUnknown.Value() - before; got != 1 {
		t.Errorf("kubescrape_routed_unknown_total delta = %v, want 1", got)
	}
}

// logsOnlyDest implements Exporter but NOT TracesExporter — the shape a route
// built without traces support has.
type logsOnlyDest struct{}

func (logsOnlyDest) ExportLogs(context.Context, plog.Logs) error          { return nil }
func (logsOnlyDest) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }
