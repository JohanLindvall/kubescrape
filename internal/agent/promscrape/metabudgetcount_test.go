package promscrape

import (
	"context"
	"io"
	"log/slog"
	"strings"
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
	s := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	ctx, cancel := s.scrapeContext(context.Background(), 20*time.Millisecond, pipelineSummary)
	defer cancel()
	b := metaBudgetFrom(ctx)
	b.spent.Add(int64(time.Hour)) // allowance spent
	for range 5 {
		if _, done, may := s.metaLookup(ctx, nil); may {
			done()
			t.Fatal("a lookup was issued past a spent allowance")
		}
	}
	if got := obs.ScrapeMetaBudgetExhausted.WithLabelValues(pipelineSummary).Value() - before; got != 1 {
		t.Fatalf("counter moved %v times for 5 shed lookups in ONE scrape, want exactly 1", got)
	}
}

// `unattributed` is documented as the count of OBJECTS never asked about, and
// it is most of what the line is for ("how much of this node's cadvisor and
// summary data should an operator not trust to join"). One cadvisor container
// row issues TWO lookups — the container id, then its pod — so charging per
// LOOKUP reported 400 for the 200 objects a 200-pod node shed, with an
// inflation factor between 1x and 2x that the line itself cannot disclose.
func TestShedObjectsAreCountedOncePerObjectNotPerLookup(t *testing.T) {
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: time.Second,
		Targets: staticTargets{}, Exporter: &captureExporter{}, StartTime: time.Now(),
		Kubelet: KubeletConfig{Meta: &fakeMetaSource{}},
	})
	s.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := s.scrapeContext(context.Background(), 20*time.Millisecond, pipelineCadvisor)
	defer cancel()
	b := metaBudgetFrom(ctx)
	b.spent.Add(int64(time.Hour)) // allowance spent

	// The cadvisor container row: a container id the cache does not know, plus
	// namespace+pod labels — the two-lookup shape.
	if _, resolved, _ := s.resolveContext(ctx, appCID, "ns1", "pod1", uid1, "app"); resolved {
		t.Fatal("a lookup resolved past a spent allowance")
	}
	if got := b.shed.Load(); got != 1 {
		t.Fatalf("shed = %d for ONE object, want 1: the count is per object, not per lookup", got)
	}
	// A second object charges a second time — the dedupe is per resolution.
	if _, _, _ = s.resolveContext(ctx, "", "ns1", "pod1", "", ""); b.shed.Load() != 2 {
		t.Fatalf("shed = %d after a second object, want 2", b.shed.Load())
	}
}

// The line's two numbers must relate. The allowance is half of the SCRAPE
// BUDGET, which is the minimum of -scrape-timeout and whatever intervals clamp
// it; printing the unclamped flag beside it read `allowance=15s
// scrapeTimeout=60s`, and an operator raising the timeout as the line's own
// comment advises moved the allowance not at all.
func TestExhaustedAllowanceNamesTheBudgetItWasCutFrom(t *testing.T) {
	var buf strings.Builder
	s := New(Config{
		Node: "n1", Interval: 30 * time.Second, Timeout: 60 * time.Second,
		Targets: staticTargets{}, Exporter: &captureExporter{}, StartTime: time.Now(),
	})
	s.log = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	// What the kubelet pipelines actually pass: min(timeout, interval).
	budget := s.kubeletTimeout()
	ctx, cancel := s.scrapeContext(context.Background(), budget, pipelineCadvisor)
	defer cancel()
	b := metaBudgetFrom(ctx)
	b.spent.Add(int64(time.Hour))
	if _, _, may := s.metaLookup(ctx, nil); may {
		t.Fatal("a lookup was issued past a spent allowance")
	}
	s.reportMetaBudget(ctx)

	out := buf.String()
	if want := "scrapeBudget=" + budget.String(); !strings.Contains(out, want) {
		t.Errorf("the warning does not name the budget the allowance was cut from (%q missing):\n%s", want, out)
	}
	if want := "allowance=" + (budget / metaBudgetDivisor).String(); !strings.Contains(out, want) {
		t.Errorf("the warning does not name the allowance (%q missing):\n%s", want, out)
	}
	if budget == s.cfg.Timeout {
		t.Fatal("the fixture no longer exercises a CLAMPED budget")
	}
}

// And per object across the whole SCRAPE, not per chunk: a batcher clears its
// per-chunk resource map on every flush and a refused lookup caches nothing, so
// an object present in several chunks reached the shed path once per chunk —
// the same inflation, by the chunk count instead of the lookup count. The line
// must say the same number however the scrape happened to be chunked.
func TestShedObjectsAreCountedOncePerScrapeAcrossChunks(t *testing.T) {
	shed := func(batchPoints int) (int64, int) {
		exp := &captureExporter{}
		s := New(Config{
			Node: "n1", Interval: time.Hour, Timeout: time.Second, BatchPoints: batchPoints,
			Targets: staticTargets{}, Exporter: exp, StartTime: time.Now(),
			Kubelet: KubeletConfig{Meta: &fakeMetaSource{}},
		})
		s.log = slog.New(slog.NewTextHandler(io.Discard, nil))
		ctx, cancel := s.scrapeContext(context.Background(), time.Minute, pipelineCadvisor)
		defer cancel()
		b := metaBudgetFrom(ctx)
		b.spent.Add(int64(time.Hour)) // allowance spent
		cb := newCadvisorBatcher(ctx, s, time.Now())
		if _, err := s.parseAndExport(ctx, strings.NewReader(cadvisorBody), false, false, cb, pipelineCadvisor, "cadvisor"); err != nil {
			t.Fatal(err)
		}
		return b.shed.Load(), len(exp.batches)
	}
	one, oneChunks := shed(10_000)
	many, manyChunks := shed(2)
	if oneChunks != 1 || manyChunks < 2 {
		t.Fatalf("the fixture no longer chunks as intended: %d and %d exports", oneChunks, manyChunks)
	}
	if one == 0 {
		t.Fatal("nothing was shed: the fixture no longer exercises the path")
	}
	if many != one {
		t.Errorf("shed = %d over %d chunks, %d in one: the count must be per object, whatever the chunking", many, manyChunks, one)
	}
}

// A metadata-service blackhole exhausts every enriched pipeline's allowance in
// the same cycle, and the line exists to say WHICH pipeline shed HOW MANY
// objects. One shared keyless gate let whichever scrape ended first take the
// window, so the other pipeline's `unattributed=` count was suppressed — and
// the same ordering repeated every window, so it never appeared at all.
func TestExhaustedAllowanceIsReportedPerPipeline(t *testing.T) {
	var buf strings.Builder
	s := New(Config{Logger: slog.New(slog.NewTextHandler(&buf, nil))})
	exhaust := func(pipeline string, shed int64) {
		ctx, cancel := s.scrapeContext(context.Background(), time.Minute, pipeline)
		b := metaBudgetFrom(ctx)
		b.spent.Add(int64(time.Hour))
		if _, _, may := s.metaLookup(ctx, nil); may {
			t.Fatal("a lookup was issued past a spent allowance")
		}
		b.shed.Store(shed)
		cancel() // reports, as every scrape entry point's deferred cancel does
	}
	exhaust(pipelineSummary, 100)
	exhaust(pipelineCadvisor, 200)
	exhaust(pipelineTargets, 300)

	out := buf.String()
	for _, want := range []string{
		"pipeline=" + pipelineSummary + " unattributed=100",
		"pipeline=" + pipelineCadvisor + " unattributed=200",
		"pipeline=" + pipelineTargets + " unattributed=300",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the exhausted-allowance line for %q is missing: one pipeline's report suppressed another's:\n%s", want, out)
		}
	}
	// Still ONE line per pipeline per window: the throttle is per pipeline, not
	// gone.
	exhaust(pipelineCadvisor, 1)
	if n := strings.Count(buf.String(), "metadata allowance"); n != 3 {
		t.Errorf("%d allowance lines for three pipelines and a repeat inside the window, want 3:\n%s", n, buf.String())
	}
}
