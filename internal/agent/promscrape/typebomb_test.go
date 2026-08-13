package promscrape

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// A scrape target is input the process does not control: any pod with
// prometheus.io/scrape annotations decides what the node's agent parses. A
// ~2 MiB gzip body of `# TYPE <16 KiB name> counter` lines and no samples at
// all used to retain >1.5 GB of live heap in the parser's TYPE table and OOMKill
// the DaemonSet pod, while the scrape recorded outcome "ok" with zero samples
// and zero malformed — indistinguishable from an idle exporter.
//
// pkg/promparse bounds the retention (TestTypeTableRetentionIsBoundedByBytes);
// this is the other half: the refusal must reach an operator. It rides the
// counter a TYPE line this parser will not honour already moves, so no new
// metric is involved — but nothing pinned that the parser's verdict survives
// the scrape session, which is where reportMalformed and its warnOnce dedupe
// live.
func TestTypeBombIsCountedMalformed(t *testing.T) {
	const lines = 200
	// Far past the parser's family-token bound, and past any plausible future
	// value of it: this is the reported attack's own name length.
	name := strings.Repeat("a", 16<<10)
	var sb strings.Builder
	for i := 0; i < lines; i++ {
		sb.WriteString("# TYPE " + name + " counter\n")
	}
	sb.WriteString("real_metric 1\n") // one honest sample, so the export path runs

	exp := &captureExporter{}
	s := New(Config{Node: "n1", Interval: time.Minute, Exporter: exp, StartTime: time.Now()})
	before := obs.ScrapeMalformed.WithLabelValues(pipelineTargets).Value()

	cb := newBatcher(func(pcommon.Resource) {}, time.Now(), time.Now())
	samples, err := s.parseAndExport(context.Background(), strings.NewReader(sb.String()), false, false, cb, pipelineTargets, "t")
	if err != nil {
		t.Fatal(err)
	}
	if got := obs.ScrapeMalformed.WithLabelValues(pipelineTargets).Value() - before; got != lines {
		t.Fatalf("malformed counted = %v, want %d: a target whose TYPE lines the parser refuses must be "+
			"distinguishable from an idle one", got, lines)
	}
	if samples != 1 || exp.points() != 1 {
		t.Fatalf("parsed %d samples, exported %d points, want 1 and 1 — refusing the declarations must not cost the exposition its data",
			samples, exp.points())
	}
}
