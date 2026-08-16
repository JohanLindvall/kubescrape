package otlpingest

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/logline"
)

// A rules-only config (no LogMetrics) must still dedupe duplicate resource keys
// before the rules resolver reads them: the resolver reads FIRST-wins while the
// store/downstream render LAST-wins, so a drop rule keyed on the last-wins value
// would silently fail to drop. Regression for the dedupe being gated on
// metric-observation eligibility.
func TestRulesOnlyDedupesResourceKeys(t *testing.T) {
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", MatchRegexp: []string{"service.name=winner"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := &captureExporter{}
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: exp,
		Rules:    rules, // NO LogMetrics
	})

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
	// The record's resource renders last-wins "winner"; the rule drops it, so
	// nothing is forwarded. Before the fix the resolver read first-wins "loser"
	// and the record shipped.
	forwarded := 0
	for _, l := range exp.logs {
		forwarded += l.LogRecordCount()
	}
	if forwarded != 0 {
		t.Errorf("record was forwarded (%d); the drop rule keyed on the last-wins value did not fire — dedupe did not run", forwarded)
	}
}
