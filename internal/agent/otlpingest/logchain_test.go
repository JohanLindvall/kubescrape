package otlpingest

import (
	"context"
	"fmt"
	"strings"
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

// The two severity edges the adversarial review confirmed: a record whose
// sender set only SeverityNumber, and a STRUCTURED (map) body whose level
// field only enrichment can surface. Both must select identically to the
// tailed form of the same logical line.
func TestIngestedSeverityEdges(t *testing.T) {
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", Match: []string{"__severity__=error"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := &captureExporter{}
	s := NewServer(ServerConfig{
		Enricher: NewEnricher(Config{Meta: newMeta(), MetricsMode: MetricsAuto, EnrichLines: true}),
		Exporter: exp,
		Rules:    rules,
	})

	ld := plog.NewLogs()
	lrs := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()

	// SeverityNumber-only (an SDK's legal shape): __severity__ resolves from
	// the number band.
	numberOnly := lrs.AppendEmpty()
	numberOnly.Body().SetStr("boom")
	numberOnly.SetSeverityNumber(plog.SeverityNumberError2) // 18: error band

	// A structured body carrying the level: enrichment must read the JSON
	// rendering (Str() is empty for a map body) or the rule misses.
	structured := lrs.AppendEmpty()
	structured.Body().SetEmptyMap().PutStr("level", "error")

	// The survivor.
	keep := lrs.AppendEmpty()
	keep.Body().SetStr("all fine")
	keep.SetSeverityText("info")

	if err := grpcExportLogs(s, ld); err != nil {
		t.Fatal(err)
	}
	if len(exp.logs) != 1 {
		t.Fatalf("exports = %d, want 1", len(exp.logs))
	}
	if got := exp.logs[0].LogRecordCount(); got != 1 {
		t.Fatalf("records forwarded = %d, want 1 (both error shapes dropped)", got)
	}
	body := exp.logs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().AsString()
	if body != "all fine" {
		t.Fatalf("survivor = %q", body)
	}
}

// The unauthenticated-sender bounds the adversarial review measured: a body
// over 1 MiB skips line-derived processing (attribute rules still apply), a
// resource wider than 64 attributes or past the first 256 of a push is not
// observed into the metric set — and every skip is counted while the data
// itself still forwards.
func TestIngestChainBounds(t *testing.T) {
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "seen", Type: "counter", Value: "1",
	}})
	if err != nil {
		t.Fatal(err)
	}
	exp := &captureExporter{}
	s := NewServer(ServerConfig{
		Enricher:   newEnricher(newMeta(), MetricsAuto),
		Exporter:   exp,
		LogMetrics: set,
	})

	bodyBig := obs.IngestChainSkipped.WithLabelValues("body_too_large").Value()
	tooWide := obs.IngestChainSkipped.WithLabelValues("resource_too_wide").Value()
	capped := obs.IngestChainSkipped.WithLabelValues("resources_capped").Value()

	ld := plog.NewLogs()
	// A resource wider than the bound.
	wide := ld.ResourceLogs().AppendEmpty()
	for i := 0; i < maxObservedResourceAttrs+1; i++ {
		wide.Resource().Attributes().PutStr(fmt.Sprintf("k%03d", i), "v")
	}
	wide.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("wide")
	// A record whose STRING body is over the cap, and a structured body over
	// the cap (estimated without materializing).
	normal := ld.ResourceLogs().AppendEmpty()
	lrs := normal.ScopeLogs().AppendEmpty().LogRecords()
	lrs.AppendEmpty().Body().SetStr(strings.Repeat("x", maxChainBodyBytes+1))
	bigMap := lrs.AppendEmpty().Body().SetEmptyMap()
	bigMap.PutStr("payload", strings.Repeat("y", maxChainBodyBytes+1))
	lrs.AppendEmpty().Body().SetStr("small and fine")
	// More resources than one push may observe.
	for i := 0; i < maxObservedResources; i++ {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", fmt.Sprintf("svc-%d", i))
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("z")
	}

	if err := grpcExportLogs(s, ld); err != nil {
		t.Fatal(err)
	}
	// Everything still forwards.
	if len(exp.logs) != 1 || exp.logs[0].LogRecordCount() != 4+maxObservedResources {
		t.Fatalf("forwarded %d records, want %d", exp.logs[0].LogRecordCount(), 4+maxObservedResources)
	}
	if got := obs.IngestChainSkipped.WithLabelValues("body_too_large").Value() - bodyBig; got != 2 {
		t.Errorf("body_too_large moved %v, want 2", got)
	}
	if got := obs.IngestChainSkipped.WithLabelValues("resource_too_wide").Value() - tooWide; got != 1 {
		t.Errorf("resource_too_wide moved %v, want 1", got)
	}
	// 2 + maxObservedResources resources total; the wide one was refused for
	// width (index 0), so indexes >= maxObservedResources are capped: 2.
	if got := obs.IngestChainSkipped.WithLabelValues("resources_capped").Value() - capped; got != 2 {
		t.Errorf("resources_capped moved %v, want 2", got)
	}
}

// Duplicate resource keys — legal on the OTLP wire, impossible in every
// agent-built resource — must not defeat the metric store's sum-fold
// identity: {k=p,k=q} vs {k=q,k=p} used to MERGE two senders' series, and
// {k=v,k=v} minted a series distinct from {k=v} that rendered identically
// (duplicate points in one payload). The boundary dedupes last-wins.
func TestIngestDedupesDuplicateResourceKeys(t *testing.T) {
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "dup", Type: "counter", Value: "1",
		Labels: []string{"svc=$service.name"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	s := NewServer(ServerConfig{
		Enricher:   newEnricher(newMeta(), MetricsAuto),
		Exporter:   &captureExporter{},
		LogMetrics: set,
	})

	// Raw OTLP allows repeated keys; build them via FromRaw-then-append on the
	// wire shape: pdata's Map has no public duplicate-insert, so go through
	// the JSON unmarshaler like a hostile sender would.
	raw := []byte(`{"resourceLogs":[{"resource":{"attributes":[
		{"key":"service.name","value":{"stringValue":"loser"}},
		{"key":"service.name","value":{"stringValue":"winner"}}
	]},"scopeLogs":[{"logRecords":[{"body":{"stringValue":"hello"}}]}]}]}`)
	var um plog.JSONUnmarshaler
	ld, err := um.UnmarshalLogs(raw)
	if err != nil {
		t.Fatal(err)
	}
	if ld.ResourceLogs().At(0).Resource().Attributes().Len() != 2 {
		t.Fatal("test premise broken: duplicate keys were deduped at decode")
	}
	if err := grpcExportLogs(s, ld); err != nil {
		t.Fatal(err)
	}

	mexp := &copyMetricsExporter{}
	if err := set.Export(context.Background(), mexp, 0); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, md := range mexp.md {
		for r := 0; r < md.ResourceMetrics().Len(); r++ {
			ms := md.ResourceMetrics().At(r).ScopeMetrics().At(0).Metrics()
			for mi := 0; mi < ms.Len(); mi++ {
				dps := ms.At(mi).Sum().DataPoints()
				for d := 0; d < dps.Len(); d++ {
					if v, ok := dps.At(d).Attributes().Get("svc"); ok {
						found = true
						if v.Str() != "winner" {
							t.Errorf("svc label = %q, want the last-wins winner", v.Str())
						}
					}
				}
			}
		}
	}
	if !found {
		t.Fatal("no labelled data point exported")
	}
}
