package main

// The logAttributes half of the ingest log chain's wiring.
//
// logs.rules and logMetrics have always reached pushed logs; the EXTRACTOR is
// what makes them agree with the tailer, because a rule that RENAMES a line key
// (`attribute:` != `key:`, the documented canonical use) is what a rule key or
// a metric label then selects on. Without it the same config selected one way
// for a tailed line and another for the identical pushed one.

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

func TestStartIngestLiftsLogAttributesFromPushedLines(t *testing.T) {
	on, grpcAddr, httpAddr := *ingestOn, *ingestGRPC, *ingestHTTP
	defer func() { *ingestOn, *ingestGRPC, *ingestHTTP = on, grpcAddr, httpAddr }()
	*ingestOn, *ingestGRPC, *ingestHTTP = true, "", freeAddr(t)

	ctx, p := testPipelines(t)
	out := &capIngestOut{}
	p.out = out
	p.attrBuilders = &attrs.Builders{}
	// The same compiled extractor the tailer and journald get (main.go builds
	// it once and hands it to every producer).
	ext, err := compileLogAttrs(&logattrs.Config{Rules: []logattrs.Rule{{Key: "user", Attribute: "app.user"}}})
	if err != nil {
		t.Fatal(err)
	}
	p.logAttrs = ext
	if err := p.startIngest(ctx); err != nil {
		t.Fatal(err)
	}

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "app")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr(`user=bob msg="hi"`)
	postLogs(t, "http://"+*ingestHTTP+"/v1/logs", ld)

	logs := out.got()
	if len(logs) != 1 {
		t.Fatalf("forwarded %d payloads, want 1", len(logs))
	}
	rec := logs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	v, ok := rec.Attributes().Get("app.user")
	if !ok {
		t.Fatalf("the logAttributes rule never ran on the pushed line: a rule or metric keyed on the renamed attribute selects differently pushed than tailed (attrs: %v)", rec.Attributes().AsRaw())
	}
	if v.Str() != "bob" {
		t.Fatalf("app.user = %q, want %q", v.Str(), "bob")
	}
}
