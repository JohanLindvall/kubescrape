package otlpingest

import (
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// nestedBody wraps a leaf under depth single-entry maps: ~10 wire bytes per
// level buys an unauthenticated sender the whole 16 MiB body cap below any
// depth cut-off, which is what made the flat 16-byte charge for an unwalked
// subtree unenforceable.
func nestedBody(lr plog.LogRecord, depth int, leaf func(pcommon.Map)) {
	m := lr.Body().SetEmptyMap()
	for i := 0; i < depth; i++ {
		m = m.PutEmptyMap("n")
	}
	leaf(m)
}

// The body bound must hold at EVERY nesting depth: the estimate is the only
// gate before AsString, so a subtree the walk refuses to descend into has to
// count as over budget. It is also the depth scrubValue stops at, so what sits
// below it is unscrubbed and must not become the text view enrichment,
// log-metrics and the rules read.
func TestDeepBodyIsRefusedRatherThanRendered(t *testing.T) {
	lr := plog.NewLogRecord()
	nestedBody(lr, maxBodyScrubDepth+2, func(m pcommon.Map) {
		m.PutStr("payload", strings.Repeat("y", maxChainBodyBytes+1))
	})
	if body, ok := chainBody(lr); ok {
		t.Fatalf("a %d-deep body of %d bytes was admitted (rendered %d bytes); "+
			"an unmeasurable subtree must count as over budget",
			maxBodyScrubDepth+2, maxChainBodyBytes+1, len(body))
	}

	// The trade the fail-closed cut-off buys: a SMALL body nested past the cap
	// also skips line-derived processing. Deliberate — it is the same subtree
	// the scrub walk never reached.
	small := plog.NewLogRecord()
	nestedBody(small, maxBodyScrubDepth+2, func(m pcommon.Map) { m.PutStr("payload", "tiny") })
	if _, ok := chainBody(small); ok {
		t.Errorf("a body nested past the scrub depth was admitted")
	}

	// Shallow bodies are unaffected: measured, admitted, rendered.
	shallow := plog.NewLogRecord()
	nestedBody(shallow, 2, func(m pcommon.Map) { m.PutStr("payload", "hunter2") })
	body, ok := chainBody(shallow)
	if !ok || !strings.Contains(body, "hunter2") {
		t.Errorf("shallow body: got (%q, %v), want it rendered", body, ok)
	}
}

// End to end: the refusal is the documented one — body_too_large moves, the
// record is still forwarded.
func TestDeepBodyCountsBodyTooLargeAndStillForwards(t *testing.T) {
	exp := &captureExporter{}
	s := NewServer(ServerConfig{
		// -enrich is the default, and it alone puts every record through
		// chainBody: the bound has to hold on the shipped configuration.
		Enricher: NewEnricher(Config{Meta: newMeta(), MetricsMode: MetricsAuto, EnrichLines: true}),
		Exporter: exp,
	})

	before := obs.IngestChainSkipped.WithLabelValues("body_too_large").Value()

	ld := plog.NewLogs()
	lrs := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	nestedBody(lrs.AppendEmpty(), maxBodyScrubDepth+2, func(m pcommon.Map) {
		m.PutStr("payload", strings.Repeat("y", maxChainBodyBytes+1))
	})
	lrs.AppendEmpty().Body().SetStr("small and fine")

	if err := grpcExportLogs(s, ld); err != nil {
		t.Fatal(err)
	}
	if got := obs.IngestChainSkipped.WithLabelValues("body_too_large").Value() - before; got != 1 {
		t.Errorf("body_too_large moved %v, want 1", got)
	}
	if len(exp.logs) != 1 || exp.logs[0].LogRecordCount() != 2 {
		t.Fatalf("forwarded records = %d, want 2 (the refusal skips processing, never delivery)",
			exp.logs[0].LogRecordCount())
	}
}
