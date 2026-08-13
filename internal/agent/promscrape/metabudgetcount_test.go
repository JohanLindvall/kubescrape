package promscrape

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// The exhaustion counter is the only signal for the objects a spent allowance
// never asks about: no request is issued, so kubescrape_metadata_requests_total
// cannot move, and kubescrape_summary_unresolved_total covers the summary
// pipeline alone. It must count SCRAPES, not shed objects — a 200-pod node
// reporting 200 would not be comparable with kubescrape_scrapes_total, which is
// the rate an operator divides it by.
func TestExhaustedAllowanceIsCountedOncePerScrape(t *testing.T) {
	before := obs.ScrapeMetaBudgetExhausted.WithLabelValues(pipelineSummary).Value()
	s := &Scraper{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := s.scrapeContext(context.Background(), 20*time.Millisecond, pipelineSummary)
	defer cancel()
	b := metaBudgetFrom(ctx)
	b.spent.Add(int64(time.Hour)) // allowance spent
	for range 5 {
		if _, done, may := s.metaLookup(ctx); may {
			done()
			t.Fatal("a lookup was issued past a spent allowance")
		}
	}
	if got := obs.ScrapeMetaBudgetExhausted.WithLabelValues(pipelineSummary).Value() - before; got != 1 {
		t.Fatalf("counter moved %v times for 5 shed lookups in ONE scrape, want exactly 1", got)
	}
}
