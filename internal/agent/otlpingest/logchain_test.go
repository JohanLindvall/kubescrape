package otlpingest

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
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
		Enricher:    NewEnricher(Config{Meta: newMeta(), MetricsMode: MetricsAuto}),
		EnrichLines: true,
		Exporter:    exp,
		Rules:       rules,
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
	for i := range maxObservedResourceAttrs + 1 {
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
	for i := range maxObservedResources {
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

// The observe bounds turn off log-metric OBSERVATION and nothing else: the rules
// run on every record under every one of them, so a drop rule still drops a
// record on a resource past the first maxObservedResources of a push and on one
// wider than maxObservedResourceAttrs. The skip warnings once said the opposite
// ("forwarded WITHOUT being observed or rule-evaluated"), and TestIngestChainBounds
// configures no rules, so nothing pinned which of the two was true.
func TestRulesStillRunPastTheObservationBounds(t *testing.T) {
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "seen", Type: "counter", Value: "1",
	}})
	if err != nil {
		t.Fatal(err)
	}
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", MatchRegexp: []string{"__line__=^drop-me$"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := &captureExporter{}
	s := NewServer(ServerConfig{
		Enricher:   newEnricher(newMeta(), MetricsAuto),
		Exporter:   exp,
		LogMetrics: set,
		Rules:      rules,
	})

	ld := plog.NewLogs()
	addResource := func(width int, name string) {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", name)
		for i := 1; i < width; i++ {
			rl.Resource().Attributes().PutStr(fmt.Sprintf("k%03d", i), "v")
		}
		lrs := rl.ScopeLogs().AppendEmpty().LogRecords()
		lrs.AppendEmpty().Body().SetStr("keep-me")
		lrs.AppendEmpty().Body().SetStr("drop-me")
	}
	addResource(maxObservedResourceAttrs+1, "wide") // index 0: over the width bound
	for i := 1; i < maxObservedResources; i++ {
		addResource(1, fmt.Sprintf("svc-%d", i))
	}
	addResource(1, "past-the-cap") // index maxObservedResources: over the count bound

	tooWide := obs.IngestChainSkipped.WithLabelValues(chainSkipTooWide).Value()
	capped := obs.IngestChainSkipped.WithLabelValues(chainSkipResources).Value()
	if err := grpcExportLogs(s, ld); err != nil {
		t.Fatal(err)
	}
	if got := obs.IngestChainSkipped.WithLabelValues(chainSkipTooWide).Value() - tooWide; got != 1 {
		t.Fatalf("resource_too_wide moved %v, want 1: the fixture no longer reaches the width bound", got)
	}
	if got := obs.IngestChainSkipped.WithLabelValues(chainSkipResources).Value() - capped; got != 1 {
		t.Fatalf("resources_capped moved %v, want 1: the fixture no longer reaches the count bound", got)
	}
	if len(exp.logs) != 1 {
		t.Fatalf("exports = %d, want 1", len(exp.logs))
	}
	rls := exp.logs[0].ResourceLogs()
	if rls.Len() != maxObservedResources+1 {
		t.Fatalf("forwarded %d resources, want %d", rls.Len(), maxObservedResources+1)
	}
	for i := 0; i < rls.Len(); i++ {
		name, _ := rls.At(i).Resource().Attributes().Get("service.name")
		lrs := rls.At(i).ScopeLogs().At(0).LogRecords()
		if lrs.Len() != 1 || lrs.At(0).Body().Str() != "keep-me" {
			t.Errorf("resource %d (%s) forwarded %d records; the drop rule must have removed drop-me there too",
				i, name.Str(), lrs.Len())
		}
	}
}

// Duplicate resource keys — legal on the OTLP wire, impossible in every
// agent-built resource — are normalized last-wins at this boundary. The metric
// store's own identity no longer depends on it (metrics.resourceAccum proves
// key-uniqueness before it takes its one-pass shortcut), but this chain reads
// the resource through logchain.Resolver, whose Get is FIRST-wins against that
// last-wins identity — so the "winner" label asserted below is what the dedupe
// still buys — and the payload forwarded on would otherwise carry the
// ambiguity to the collector.
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

// --- a structured RESOURCE value is rendered once per resource, never per record ---

// arrayResourcePush is one resource whose service.name is an ARRAY of elems
// one-byte strings — legal OTLP, and a map/array/bytes value is what AsString
// has to render rather than return — carrying `records` records.
func arrayResourcePush(elems, records int) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.Resource().Attributes().PutEmptySlice("service.name")
	sl.EnsureCapacity(elems)
	for range elems {
		sl.AppendEmpty().SetStr("x")
	}
	lrs := rl.ScopeLogs().AppendEmpty().LogRecords()
	for range records {
		lrs.AppendEmpty().Body().SetStr("line")
	}
	return ld
}

// allocatedBy is the heap f allocates, garbage included — the transient cost an
// amplification is made of, which a live-heap delta would not show.
func allocatedBy(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// The rules resolve a resource key once PER RECORD, and for an array value that
// read used to RENDER it (AsString boxes the whole value and marshals it): a
// sender's structure multiplied by its own record count — measured at ~3 GB for
// a 100k-element array and 1000 records. The value is now rendered once, into
// the view the chain reads, and the rules still see exactly what AsString
// renders.
func TestStructuredResourceValueRendersOncePerResource(t *testing.T) {
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", MatchRegexp: []string{`service.name=^\["x","x"`}},
	})
	if err != nil {
		t.Fatal(err)
	}
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: &captureExporter{},
		Rules:    rules,
	})

	// Within the bound: ~8 KB of rendered text, rendered once rather than
	// 2000 times (~100 KB of transient heap per render).
	const elems, records = 2000, 2000
	ld := arrayResourcePush(elems, records)
	var forward bool
	alloc := allocatedBy(func() { _, forward = s.applyLogChain(ld) })
	if alloc > 8<<20 {
		t.Errorf("the chain allocated %d bytes for %d records under one %d-element resource value: "+
			"the value is being rendered per record", alloc, records, elems)
	}
	// And rendered FAITHFULLY: the drop rule matches the value's AsString, so
	// every record is dropped, exactly as before the view existed.
	if forward {
		t.Errorf("records survived a drop rule matching the rendered resource value")
	}
}

// A value whose text would exceed the bound is not rendered at all: it resolves
// empty for the chain, the skip is counted, and the FORWARDED payload keeps it
// verbatim — the view is a copy, and the park/restore that avoids deep-copying
// the value must put it back.
func TestOversizedStructuredResourceValueResolvesEmptyAndIsForwardedIntact(t *testing.T) {
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", MatchRegexp: []string{`service.name=x`}},
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
	elems := maxResourceValueTextBytes/4 + 1 // `"x",` per element: just past the bound
	ld := arrayResourcePush(elems, 100)
	ld.ResourceLogs().At(0).Resource().Attributes().PutStr("deployment.environment", "prod")
	before := obs.IngestChainSkipped.WithLabelValues(chainSkipValueTooLarge).Value()

	alloc := allocatedBy(func() {
		if err := grpcExportLogs(s, ld); err != nil {
			t.Fatal(err)
		}
	})
	if got := obs.IngestChainSkipped.WithLabelValues(chainSkipValueTooLarge).Value() - before; got != 1 {
		t.Errorf("resource_value_too_large moved %v, want 1 (one resource)", got)
	}
	if len(exp.logs) != 1 || exp.logs[0].LogRecordCount() != 100 {
		t.Fatalf("the value resolved as something a `service.name=x` drop rule matched; " +
			"an unrendered value must resolve empty")
	}
	res := exp.logs[0].ResourceLogs().At(0).Resource().Attributes()
	v, ok := res.Get("service.name")
	if !ok || v.Type() != pcommon.ValueTypeSlice || v.Slice().Len() != elems {
		t.Fatalf("the forwarded resource lost its value: %v %v", ok, v.Type())
	}
	if env, _ := res.Get("deployment.environment"); env.Str() != "prod" || res.Len() != 2 {
		t.Errorf("the forwarded resource changed: %v", res.AsRaw())
	}
	// Nothing rendered it: a render of this value alone is ~1 MB of transient
	// heap, a per-record one ~100 MB.
	if alloc > 4<<20 {
		t.Errorf("the push allocated %d bytes; the oversized value was rendered", alloc)
	}
}

// Binding the VIEW rather than the resource must not move a series: a rendered
// map/array labels exactly as AsString would have rendered it.
func TestStructuredResourceValueLabelsAsItRenders(t *testing.T) {
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "seen", Type: "counter", Value: "1",
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
	ld := arrayResourcePush(2, 1)
	want := `["x","x"]`
	if got, _ := ld.ResourceLogs().At(0).Resource().Attributes().Get("service.name"); got.AsString() != want {
		t.Fatalf("fixture renders %q", got.AsString())
	}
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
	rm := mexp.md[0].ResourceMetrics().At(0)
	if v, _ := rm.Resource().Attributes().Get("service.name"); v.AsString() != want {
		t.Errorf("series resource service.name = %q, want %q", v.AsString(), want)
	}
	dp := rm.ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0)
	if v, _ := dp.Attributes().Get("svc"); v.Str() != want {
		t.Errorf("svc label = %q, want %q", v.Str(), want)
	}
}

// The dedupe used to round-trip the resource through AsRaw/FromRaw, boxing
// every value — a sender's structure allocated again in full for one repeated
// key — and re-emitting the attributes in Go's randomised map order. It now
// removes the earlier occurrences in place.
func TestDedupeResourceKeysCompactsWithoutBoxing(t *testing.T) {
	raw := []byte(`{"resourceLogs":[{"resource":{"attributes":[
		{"key":"service.name","value":{"stringValue":"loser"}},
		{"key":"blob","value":{"arrayValue":{}}},
		{"key":"host.name","value":{"stringValue":"h"}},
		{"key":"service.name","value":{"stringValue":"winner"}}
	]},"scopeLogs":[{"logRecords":[{"body":{"stringValue":"hello"}}]}]}]}`)
	var um plog.JSONUnmarshaler
	ld, err := um.UnmarshalLogs(raw)
	if err != nil {
		t.Fatal(err)
	}
	m := ld.ResourceLogs().At(0).Resource().Attributes()
	blob, _ := m.Get("blob")
	for range 100000 {
		blob.Slice().AppendEmpty().SetStr("x")
	}

	alloc := allocatedBy(func() { dedupeResourceKeys(m) })
	if alloc > 64<<10 {
		t.Errorf("dedupe allocated %d bytes for a four-key resource: it boxed the values", alloc)
	}
	var keys []string
	m.Range(func(k string, _ pcommon.Value) bool { keys = append(keys, k); return true })
	if got := strings.Join(keys, ","); got != "blob,host.name,service.name" {
		t.Errorf("keys after dedupe = %s, want the last occurrences in their original order", got)
	}
	if v, _ := m.Get("service.name"); v.Str() != "winner" {
		t.Errorf("service.name = %q, want the last-wins winner", v.Str())
	}
	if v, _ := m.Get("blob"); v.Slice().Len() != 100000 {
		t.Errorf("blob has %d elements, want it untouched", v.Slice().Len())
	}
}

// The dedupe runs BEFORE enrichment, because enrichment reads the resource too:
// its lookup takes the FIRST value under an id key. Deduped afterwards, a
// resource repeating container.id [A, B] was resolved as A and forwarded
// carrying B beside A's Kubernetes identity.
func TestRepeatedLookupKeyResolvesTheValueTheResourceKeeps(t *testing.T) {
	meta := newMeta()
	meta.containers["cafe02"] = &kubemeta.ContainerMetadata{
		Container: kubemeta.Container{Name: "sidecar", ID: "containerd://cafe02"},
		Pod:       kubemeta.Pod{Name: "web-1", Namespace: "default", UID: "pod-uid-1", NodeName: "node1"},
	}
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", MatchRegexp: []string{"__line__=never-matches-anything"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := &captureExporter{}
	s := NewServer(ServerConfig{Enricher: newEnricher(meta, MetricsAuto), Exporter: exp, Rules: rules})

	raw := []byte(`{"resourceLogs":[{"resource":{"attributes":[
		{"key":"container.id","value":{"stringValue":"cafe01"}},
		{"key":"container.id","value":{"stringValue":"cafe02"}}
	]},"scopeLogs":[{"logRecords":[{"body":{"stringValue":"hello"}}]}]}]}`)
	var um plog.JSONUnmarshaler
	ld, err := um.UnmarshalLogs(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := grpcExportLogs(s, ld); err != nil {
		t.Fatal(err)
	}
	if len(exp.logs) != 1 {
		t.Fatalf("exports = %d", len(exp.logs))
	}
	res := exp.logs[0].ResourceLogs().At(0).Resource().Attributes()
	var ids []string
	res.Range(func(k string, v pcommon.Value) bool {
		if k == "container.id" {
			ids = append(ids, v.Str())
		}
		return true
	})
	if len(ids) != 1 {
		t.Fatalf("container.id forwarded %d times (%v), want once", len(ids), ids)
	}
	name, _ := res.Get("k8s.container.name")
	want := map[string]string{"cafe01": "app", "cafe02": "sidecar"}[ids[0]]
	if name.Str() != want {
		t.Errorf("forwarded container.id=%s beside k8s.container.name=%q, which is %q's: the lookup resolved "+
			"a different id than the resource keeps", ids[0], name.Str(), want)
	}
}

// --- an over-cap STRING body is rule-evaluated on its prefix ---

// Under an allowlist ruleset — keep what matches, drop the rest — a record whose
// text matches the keep rule must be KEPT however long its body is. The over-cap
// view used to be "", so the catch-all drop took it, under a warning claiming
// such records were not rule-evaluated at all.
func TestOverCapStringBodyIsRuleEvaluatedOnItsPrefix(t *testing.T) {
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "keep", MatchRegexp: []string{"__line__=^important"}},
		{Action: "drop", MatchRegexp: []string{"__line__=.*"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := &captureExporter{}
	s := NewServer(ServerConfig{Enricher: newEnricher(newMeta(), MetricsAuto), Exporter: exp, Rules: rules})
	big := "important " + strings.Repeat("x", maxChainBodyBytes)
	ld := pushLogs(map[string][]string{"web": {big, "important small", "unimportant small"}})
	before := obs.IngestChainSkipped.WithLabelValues(chainSkipBody).Value()
	if err := grpcExportLogs(s, ld); err != nil {
		t.Fatal(err)
	}
	if got := obs.IngestChainSkipped.WithLabelValues(chainSkipBody).Value() - before; got != 1 {
		t.Errorf("body_too_large moved %v, want 1", got)
	}
	if len(exp.logs) != 1 {
		t.Fatalf("exports = %d", len(exp.logs))
	}
	var kept []int
	lrs := exp.logs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	for i := 0; i < lrs.Len(); i++ {
		kept = append(kept, len(lrs.At(i).Body().Str()))
	}
	if len(kept) != 2 || kept[0] != len(big) {
		t.Errorf("forwarded bodies of lengths %v, want the over-cap 'important' record (%d bytes, untruncated) "+
			"and the small one", kept, len(big))
	}
}

// logs.rules over a WIDE resource cost O(records x width): the resolver reads a
// resource key with pcommon.Map.Get, a linear walk, once per record, and a rule
// key the record does not carry falls through to it every time. Both factors
// are the sender's, and the width bound that exists (maxObservedResourceAttrs)
// only turns observation off — the rules deliberately still run. Measured
// before the fix at 2.17 s for 40k attributes and 40k records, a ~400 KB push.
func TestRuleEvaluationIsLinearInResourceWidth(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector's slowdown makes a wall-clock ceiling meaningless")
	}
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", Match: []string{"k8s.namespace.name=payments"}}, // on no resource or record here
	})
	if err != nil {
		t.Fatal(err)
	}
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: exporterFunc(func(plog.Logs) error { return nil }),
		Rules:    rules,
	})
	const width, records = 50_000, 50_000
	ld := wireLogs(t, func(add func(k, v string)) {
		for i := range width {
			add("attr."+strconv.Itoa(i), "v")
		}
	}, records)

	start := time.Now()
	if err := s.forwardLogs(context.Background(), ld); err != nil {
		t.Fatal(err)
	}
	took := time.Since(start)

	if got := ld.LogRecordCount(); got != records {
		t.Fatalf("%d records left, want all %d: the rule's key is on nothing, so nothing may drop", got, records)
	}
	// Generous: the per-record walk took seconds on this shape and the
	// projection takes tens of milliseconds, so load cannot blur the two.
	if limit := time.Second; took > limit {
		t.Errorf("rules over a %d-attribute resource and %d records took %v, want < %v: rule evaluation is "+
			"O(records x resource width)", width, records, took, limit)
	}
}

// The wide-resource projection must select EXACTLY as the full resource does:
// the resolver's ranking (record, then line-lifted, then resource) and the
// synthetic __severity__ key, read through the lazily filled map.
func TestWideResourceRulesSelectLikeANarrowOne(t *testing.T) {
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", Match: []string{"team=payments"}},
		{Action: "drop", Match: []string{"__severity__=debug"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: exporterFunc(func(plog.Logs) error { return nil }),
		Rules:    rules,
	})
	for _, width := range []int{8, maxObservedResourceAttrs + 1, 4 * maxObservedResourceAttrs} {
		t.Run(strconv.Itoa(width), func(t *testing.T) {
			ld := plog.NewLogs()
			for _, team := range []string{"payments", "search"} {
				rl := ld.ResourceLogs().AppendEmpty()
				a := rl.Resource().Attributes()
				for i := 0; i < width-1; i++ {
					a.PutStr("attr."+strconv.Itoa(i), "v")
				}
				a.PutStr("team", team) // LAST, so a truncated walk would miss it
				lrs := rl.ScopeLogs().AppendEmpty().LogRecords()
				plain := lrs.AppendEmpty() // resolves team from the resource
				plain.Body().SetStr(team + " plain")
				plain.SetSeverityText("info")
				own := lrs.AppendEmpty() // the record's own team outranks the resource's
				own.Body().SetStr(team + " own")
				own.SetSeverityText("info")
				own.Attributes().PutStr("team", "platform")
				dbg := lrs.AppendEmpty() // dropped by severity whatever its team
				dbg.Body().SetStr(team + " debug")
				dbg.SetSeverityText("debug")
			}
			if err := s.forwardLogs(context.Background(), ld); err != nil {
				t.Fatal(err)
			}
			var kept []string
			rls := ld.ResourceLogs()
			for i := 0; i < rls.Len(); i++ {
				lrs := rls.At(i).ScopeLogs().At(0).LogRecords()
				for j := 0; j < lrs.Len(); j++ {
					kept = append(kept, lrs.At(j).Body().Str())
				}
			}
			want := []string{"payments own", "search plain", "search own"}
			if fmt.Sprint(kept) != fmt.Sprint(want) {
				t.Errorf("kept %q, want %q", kept, want)
			}
		})
	}
}
