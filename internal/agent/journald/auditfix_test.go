package journald

import (
	"context"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// A truncation is a READ-side sanitation event, so it must be counted for every
// entry the batch held — not only for the ones that survived the rules. Counting
// after the successful export skipped the two settle-without-export paths (the
// rules emptying the payload, a permanent rejection), which made the metric
// depend on batch COMPOSITION: the same cut message tallied or not according to
// whether some unrelated sibling entry happened to survive. On the
// heavily-sampled journal the rules exist for, that reads 0 while messages are
// being cut on every flush.
func TestAuditFixTruncationCountedWhenTheRulesDropTheBatch(t *testing.T) {
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", MatchRegexp: []string{"__line__=x"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	before := obs.JournalTruncated.Value()

	exp := &captureExporter{}
	r := New(Config{Exporter: exp, Rules: rules, MaxEntryBytes: 20})
	body, origLen := r.sanitize(strings.Repeat("x", 200), "unit.service")
	if origLen == 0 {
		t.Fatal("precondition: the message must be truncated")
	}
	r.ingest(mkEntry("c1", "a.service", "", "6"), body, origLen)
	if err := r.flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	exp.mu.Lock()
	sends := len(exp.batches)
	exp.mu.Unlock()
	if sends != 0 {
		t.Fatalf("precondition: the rules must empty the payload, got %d exports", sends)
	}
	if got := obs.JournalTruncated.Value(); got != before+1 {
		t.Fatalf("JournalTruncated delta = %v across an all-dropped batch, want 1: "+
			"a truncation counted only when a sibling entry survives makes the metric batch-dependent", got-before)
	}
}
