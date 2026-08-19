package otlpingest

// Two properties of the ingest log chain that the four producers already have:
// side effects are attributed to a DELIVERY rather than to a receive attempt,
// and the chain includes the logAttributes lift, so one config selects
// identically however a line arrived.

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"

	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

// A NACKed push is retransmitted by its sender — that is what answering it
// Unavailable/503 asks for — and the retransmission re-runs the whole chain.
// Counting the rule drops inside the chain therefore multiplied
// kubescrape_log_rules_dropped_total by the number of attempts an outage
// spanned, for records that were never delivered once.
func TestRuleDropsOfANACKedPushAreNotCounted(t *testing.T) {
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", MatchRegexp: []string{"__line__=drop"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := &captureExporter{fail: true}
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: exp,
		Rules:    rules,
	})

	// One payload, pushed five times exactly as an SDK's retry policy would:
	// the same bytes, re-decoded per attempt.
	ld := pushLogs(map[string][]string{"web": {"keep this", "drop this"}})
	raw, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	before := obs.LogRulesDropped.Value()
	for i := 0; i < 5; i++ {
		req := plogotlp.NewExportRequest()
		if err := req.UnmarshalProto(raw); err != nil {
			t.Fatal(err)
		}
		g := &logsGRPC{s: s}
		if _, err := g.Export(context.Background(), req); err == nil {
			t.Fatalf("attempt %d: the failing forward must surface to the sender", i)
		}
	}
	if got := obs.LogRulesDropped.Value() - before; got != 0 {
		t.Errorf("kubescrape_log_rules_dropped_total moved %v across 5 undelivered attempts of 1 dropped "+
			"record, want 0: the operator's drop rate must not spike during the outage they use it to "+
			"diagnose", got)
	}

	// And the delivery that finally succeeds counts it exactly once.
	exp.fail = false
	req := plogotlp.NewExportRequest()
	if err := req.UnmarshalProto(raw); err != nil {
		t.Fatal(err)
	}
	if _, err := (&logsGRPC{s: s}).Export(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if got := obs.LogRulesDropped.Value() - before; got != 1 {
		t.Errorf("after the delivery the counter moved %v, want 1", got)
	}
}

// A payload the rules empty entirely is ACKED without a send, so its drops ARE
// delivered as far as the sender is concerned and must be counted.
func TestRuleDropsOfAnAllDroppedPushAreCounted(t *testing.T) {
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", MatchRegexp: []string{"__line__=drop"}},
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
	if err := grpcExportLogs(s, pushLogs(map[string][]string{"web": {"drop one", "drop two"}})); err != nil {
		t.Fatal(err)
	}
	if len(exp.logs) != 0 {
		t.Fatalf("an all-dropped push was forwarded")
	}
	if got := obs.LogRulesDropped.Value() - before; got != 2 {
		t.Errorf("kubescrape_log_rules_dropped_total moved %v, want 2", got)
	}
}

// renameExtractor is the documented canonical logAttributes shape: the lifted
// name differs from the line's key, which is the only case where the line-field
// fallback in logline.ResolveKey cannot paper over a missing lift.
func renameExtractor(t *testing.T) *logattrs.Extractor {
	t.Helper()
	ext, err := logattrs.New(&logattrs.Config{Rules: []logattrs.Rule{
		{Key: "lvl", Attribute: "level", Target: logattrs.TargetLog},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return ext
}

const renamedLine = `{"lvl":"debug","msg":"hi"}`

// The drop-rule half: the identical body selects the same way pushed as tailed.
// Without the lift the rule key `level` resolved to "" (the line has `lvl`) and
// the record shipped.
func TestPushedLineSelectsLikeATailedOneAfterALiftedRename(t *testing.T) {
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", Match: []string{"level=debug"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := &captureExporter{}
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: exp,
		LogAttrs: renameExtractor(t),
		Rules:    rules,
	})
	if err := grpcExportLogs(s, pushLogs(map[string][]string{"web": {renamedLine}})); err != nil {
		t.Fatal(err)
	}
	if len(exp.logs) != 0 {
		t.Fatalf("the pushed record was forwarded; the tailer drops the identical line")
	}

	// The producers' own chain, over the same body, as the oracle.
	chain := logchain.NewChain[string](logchain.Config{LogAttrs: renameExtractor(t), Rules: rules}, false)
	body, lifted := chain.Line(renamedLine)
	dest := plog.NewLogs().ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	kept := chain.Emit(&testProducer{dest: dest.LogRecords(), body: body},
		logchain.Input[string]{Body: body, Lifted: lifted, Resource: dest.Scope().Attributes(), BoundKey: "k"})
	if kept {
		t.Fatal("the oracle kept the line; the fixture no longer proves anything")
	}
}

// The LOSS half, which is the one that matters: an allowlist ruleset (keep on
// the lifted name plus a catch-all drop) DISCARDED the pushed record — acked to
// its sender, and counted as an operator-chosen drop.
func TestAllowlistRulesDoNotSilentlyDiscardPushedRecords(t *testing.T) {
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "keep", Match: []string{"level=debug"}},
		{Action: "drop", MatchRegexp: []string{"__line__=.*"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := &captureExporter{}
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: exp,
		LogAttrs: renameExtractor(t),
		Rules:    rules,
	})
	if err := grpcExportLogs(s, pushLogs(map[string][]string{"web": {renamedLine}})); err != nil {
		t.Fatal(err)
	}
	if len(exp.logs) != 1 || exp.logs[0].LogRecordCount() != 1 {
		t.Fatalf("the pushed record was discarded by an allowlist the tailer's identical line passes")
	}
	// The lift also reaches the FORWARDED record: target: log attributes are
	// what an operator sees in the backend.
	lr := exp.logs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	if v, ok := lr.Attributes().Get("level"); !ok || v.Str() != "debug" {
		t.Errorf("forwarded record attributes = %v, want level=debug", lr.Attributes().AsRaw())
	}
}

// The metric-label half: a label keyed on the lifted name rendered EMPTY for
// pushed lines and filled for tailed ones, splitting one logical metric in two.
func TestLogMetricLabelResolvesALiftedNameOnPushedLines(t *testing.T) {
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name:        "ingested_lines",
		Type:        "counter",
		MatchRegexp: []string{"__line__=."},
		Value:       "1",
		Labels:      []string{"lvlout=$level"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	s := NewServer(ServerConfig{
		Enricher:   newEnricher(newMeta(), MetricsAuto),
		Exporter:   &captureExporter{},
		LogAttrs:   renameExtractor(t),
		LogMetrics: set,
	})
	if err := grpcExportLogs(s, pushLogs(map[string][]string{"web": {renamedLine}})); err != nil {
		t.Fatal(err)
	}
	mexp := &copyMetricsExporter{}
	if err := set.Export(context.Background(), mexp, 0); err != nil {
		t.Fatal(err)
	}
	if len(mexp.md) != 1 {
		t.Fatalf("metric exports = %d", len(mexp.md))
	}
	dps := mexp.md[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints()
	// Summed across the series' data points (the renderer may emit several).
	var total float64
	for d := 0; d < dps.Len(); d++ {
		dp := dps.At(d)
		total += dp.DoubleValue() + float64(dp.IntValue())
		if v, ok := dp.Attributes().Get("lvlout"); !ok || v.Str() != "debug" {
			t.Fatalf("label lvlout = %v, want debug: a label keyed on a lifted name must not render "+
				"empty for pushed lines and filled for tailed ones", dp.Attributes().AsRaw())
		}
	}
	if total != 1 {
		t.Errorf("observed value = %v, want 1", total)
	}
}

// A rule keyed on a RESOURCE-target lift resolves too (Resolver.SetLifted),
// even though the sender's grouping means the lifted attribute cannot be
// written onto the shared resource itself.
func TestResourceTargetLiftResolvesForRulesWithoutRewritingTheSharedResource(t *testing.T) {
	ext, err := logattrs.New(&logattrs.Config{Rules: []logattrs.Rule{
		{Key: "tenant", Attribute: "tenant.id", Target: logattrs.TargetResource},
	}})
	if err != nil {
		t.Fatal(err)
	}
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", Match: []string{"tenant.id=noisy"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := &captureExporter{}
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: exp,
		LogAttrs: ext,
		Rules:    rules,
	})
	ld := pushLogs(map[string][]string{"web": {
		`{"tenant":"noisy","msg":"drop me"}`,
		`{"tenant":"quiet","msg":"keep me"}`,
	}})
	if err := grpcExportLogs(s, ld); err != nil {
		t.Fatal(err)
	}
	if len(exp.logs) != 1 || exp.logs[0].LogRecordCount() != 1 {
		t.Fatalf("resource-target lift did not reach the rules: %d records forwarded",
			exp.logs[0].LogRecordCount())
	}
	res := exp.logs[0].ResourceLogs().At(0).Resource().Attributes()
	if _, ok := res.Get("tenant.id"); ok {
		t.Error("a line's resource-target lift was written onto the resource it SHARES with other " +
			"records: one line's value would be stamped on every record beside it")
	}
}

// testProducer is the minimal logchain.Producer for the oracle above.
type testProducer struct {
	dest plog.LogRecordSlice
	body string
}

func (p *testProducer) Dest() plog.LogRecordSlice { return p.dest }
func (p *testProducer) Stamp(lr plog.LogRecord)   { lr.Body().SetStr(p.body) }
