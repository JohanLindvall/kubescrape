package otlpingest

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

func pushLogs(bodies map[string][]string) plog.Logs {
	ld := plog.NewLogs()
	for svc, lines := range bodies {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", svc)
		lrs := rl.ScopeLogs().AppendEmpty().LogRecords()
		for _, b := range lines {
			lr := lrs.AppendEmpty()
			lr.Body().SetStr(b)
			lr.SetSeverityText("info")
		}
	}
	return ld
}

// The operator's logs.rules reach pushed logs: the same compiled chain the
// producers run drops records from an ingested payload after enrichment, the
// drop is counted, the empty groups are pruned, and the push is still acked.
func TestIngestedLogsRunTheRules(t *testing.T) {
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", MatchRegexp: []string{"__line__=level=debug"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := &captureExporter{}
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: exp,
		Rules:    rules,
	})

	before := obs.LogRulesDropped.Value()
	ld := pushLogs(map[string][]string{
		"noisy": {"level=debug chatter", "level=debug more"},
		"web":   {"level=debug drop me", "level=error keep me"},
	})
	if err := grpcExportLogs(s, ld); err != nil {
		t.Fatal(err)
	}
	if len(exp.logs) != 1 {
		t.Fatalf("exports = %d, want 1", len(exp.logs))
	}
	got := exp.logs[0]
	if got.LogRecordCount() != 1 {
		t.Fatalf("records forwarded = %d, want 1", got.LogRecordCount())
	}
	// The all-dropped resource is pruned, not forwarded empty.
	if got.ResourceLogs().Len() != 1 {
		t.Fatalf("resources forwarded = %d, want 1 (noisy pruned)", got.ResourceLogs().Len())
	}
	if obs.LogRulesDropped.Value() != before+3 {
		t.Errorf("LogRulesDropped moved %v, want 3", obs.LogRulesDropped.Value()-before)
	}

	// A payload the rules empty entirely is ACKED without a send.
	if err := grpcExportLogs(s, pushLogs(map[string][]string{"noisy": {"level=debug x"}})); err != nil {
		t.Fatalf("all-dropped push must still ack: %v", err)
	}
	if len(exp.logs) != 1 {
		t.Fatalf("all-dropped push was forwarded: %d exports", len(exp.logs))
	}
}

// logMetrics observe EVERY ingested record — including ones the rules then
// drop — keyed by the sender's (enriched) resource.
func TestIngestedLogsFeedLogMetrics(t *testing.T) {
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name:        "ingested_errors",
		Type:        "counter",
		MatchRegexp: []string{"__line__=this"},
		Value:       "1",
		Labels:      []string{"service=$service.name"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", MatchRegexp: []string{"__line__=drop"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := &captureExporter{}
	s := NewServer(ServerConfig{
		Enricher:   newEnricher(newMeta(), MetricsAuto),
		Exporter:   exp,
		Rules:      rules,
		LogMetrics: set,
	})
	ld := pushLogs(map[string][]string{"web": {"keep this", "drop this"}})
	if err := grpcExportLogs(s, ld); err != nil {
		t.Fatal(err)
	}

	mexp := &copyMetricsExporter{}
	if err := set.Export(context.Background(), mexp, 0); err != nil {
		t.Fatal(err)
	}
	if len(mexp.md) != 1 {
		t.Fatalf("metric exports = %d", len(mexp.md))
	}
	md := mexp.md[0]
	if md.ResourceMetrics().Len() != 1 {
		t.Fatalf("resources = %d", md.ResourceMetrics().Len())
	}
	m := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	if m.Name() != "ingested_errors" {
		t.Fatalf("metric = %q", m.Name())
	}
	// Both records observed, the dropped one included. Summed across the
	// series' data points (the renderer may emit several).
	var got float64
	labelled := false
	dps := m.Sum().DataPoints()
	for d := 0; d < dps.Len(); d++ {
		dp := dps.At(d)
		got += dp.DoubleValue() + float64(dp.IntValue())
		if v, ok := dp.Attributes().Get("service"); ok && v.Str() == "web" {
			labelled = true
		}
	}
	if got != 2 {
		t.Fatalf("value = %v, want 2 (metrics observe every record, kept or dropped)", got)
	}
	if !labelled {
		t.Fatal("service=web label (resolved from the sender's resource) missing")
	}
}

// grpcExportLogs pushes through the real gRPC handler path (interceptor-free).
func grpcExportLogs(s *Server, ld plog.Logs) error {
	g := &logsGRPC{s: s}
	req := plogotlp.NewExportRequestFromLogs(ld)
	_, err := g.Export(context.Background(), req)
	return err
}

// copyMetricsExporter deep-copies what it captures: DynamicMetricSet.Export
// reuses and clears its payload after each ExportMetrics call.
type copyMetricsExporter struct{ md []pmetric.Metrics }

func (c *copyMetricsExporter) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	cp := pmetric.NewMetrics()
	md.CopyTo(cp)
	c.md = append(c.md, cp)
	return nil
}
