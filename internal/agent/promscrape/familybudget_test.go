package promscrape

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/promparse"
)

// retainedAccBytes is what the converter's accumulators actually hold alive
// while a family is open, walked off the data structures the way a heap profile
// would rather than read out of c.accBytes — the charge is the thing under test,
// and a bound that only ever agrees with its own arithmetic is not a bound.
func retainedAccBytes(c *converter) int {
	n := 0
	for key, acc := range c.hists {
		n += accOverheadBytes + len(key) + labelRetainBytes*len(acc.labels) + bucketRetainBytes*len(acc.buckets)
		for i := range acc.exemplars {
			n += exemplarRetained(&acc.exemplars[i])
		}
	}
	for key, acc := range c.summs {
		n += accOverheadBytes + len(key) + labelRetainBytes*len(acc.labels) + quantileRetainBytes*len(acc.quantiles)
	}
	return n
}

// countingSink is a sink that keeps no pdata: these tests emit tens of
// thousands of points and only ever ask how many.
type countingSink struct {
	numbers, hists, summs int
	histSums              []float64 // the sum of every emitted histogram point
}

func (s *countingSink) addNumber(Sample, bool) { s.numbers++ }
func (s *countingSink) addHistogram(_ string, acc *histAcc) {
	s.hists++
	s.histSums = append(s.histSums, acc.sum)
}
func (s *countingSink) addSummary(string, *summAcc) { s.summs++ }

// histSample builds one component sample of a histogram family whose label set
// is identified by id.
func histSample(id string, role promparse.SampleRole, le string, value float64) Sample {
	labels := []Label{{Name: "id", Value: id}}
	name := "h_sum"
	switch role {
	case RoleHistogramBucket:
		labels = append(labels, Label{Name: "le", Value: le})
		name = "h_bucket"
	case RoleHistogramCount:
		name = "h_count"
	}
	return Sample{Name: name, Family: "h", Role: role, Labels: labels, Value: value}
}

// A scrape target is input the process does not control, and holding
// accumulators "for the current family only" bounds nothing when the target
// decides how big a family is. The reported shape is a 524 KiB gzipped
// exposition — one histogram family of hundreds of thousands of one-line label
// sets, which compresses to almost nothing because the lines are nearly
// identical — driving 447 MiB of peak heap. Scaled down here to ~10x the
// budget's worth of attempted retention, which is cheap to run.
func TestFamilyAccumulatorRetentionIsBoundedByBytes(t *testing.T) {
	// Each label set costs roughly accOverheadBytes + its key + its labels, so
	// this attempts ~80 MiB against an 8 MiB budget.
	const sets = 300_000

	sink := &countingSink{}
	conv := newConverter(sink, nil)
	for i := 0; i < sets; i++ {
		id := strconv.Itoa(i)
		for _, s := range []Sample{
			histSample(id, RoleHistogramBucket, "1", 1),
			histSample(id, RoleHistogramBucket, "+Inf", 2),
			histSample(id, RoleHistogramSum, "", 1.5),
			histSample(id, RoleHistogramCount, "", 2),
		} {
			if err := conv.add(s); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Measured BEFORE finish: the family is still open, which is exactly when
	// the agent is holding the heap.
	if retained := retainedAccBytes(conv); retained > maxFamilyAccBytes {
		t.Fatalf("the open family retains %d bytes of accumulators, want <= %d "+
			"(%d label sets offered: a target chooses its own label sets, so holding a whole family "+
			"is bounded by nothing at all)", retained, maxFamilyAccBytes, sets)
	}
	if conv.dropped == 0 {
		t.Fatalf("nothing was counted dropped although %d label sets were offered against an %d-byte budget",
			sets, maxFamilyAccBytes)
	}
	// Degrade, do not refuse: the label sets that fit are still converted.
	if len(conv.hists) < 1000 {
		t.Fatalf("only %d label sets admitted: the budget must clip the family, not empty it", len(conv.hists))
	}
	admitted := len(conv.hists)
	if err := conv.finish(); err != nil {
		t.Fatal(err)
	}
	if sink.hists != admitted {
		t.Fatalf("emitted %d histogram points, want %d (every admitted label set)", sink.hists, admitted)
	}
}

// The refusal must not corrupt what it refuses to hold. labelKey's memo records
// the last label set SEEN, so a refused accumulator that leaves the memo
// pointing at the previously admitted one folds the refused set's _sum into
// ANOTHER series' data point — a silently wrong histogram, which is strictly
// worse than the drop.
func TestRefusedLabelSetDoesNotFoldIntoAnotherPoint(t *testing.T) {
	const poison = 987654.5

	sink := &countingSink{}
	conv := newConverter(sink, nil)
	// Spend the budget. Every one of these carries sum 1.
	for i := 0; conv.dropped == 0; i++ {
		if i > 1_000_000 {
			t.Fatalf("the budget was never spent after %d label sets", i)
		}
		id := strconv.Itoa(i)
		for _, s := range []Sample{
			histSample(id, RoleHistogramBucket, "1", 1),
			histSample(id, RoleHistogramSum, "", 1),
		} {
			if err := conv.add(s); err != nil {
				t.Fatal(err)
			}
		}
	}
	// A brand-new label set arriving after the budget is spent: its bucket is
	// refused, and then its _sum must be refused too rather than landing
	// somewhere.
	for _, s := range []Sample{
		histSample("refused", RoleHistogramBucket, "1", 1),
		histSample("refused", RoleHistogramSum, "", poison),
	} {
		if err := conv.add(s); err != nil {
			t.Fatal(err)
		}
	}
	if err := conv.finish(); err != nil {
		t.Fatal(err)
	}

	if len(sink.histSums) == 0 {
		t.Fatal("no points were emitted at all, so the scan below proves nothing")
	}
	for i, sum := range sink.histSums {
		if sum == poison {
			t.Fatalf("point %d of %d carries the refused label set's sum (%v): a refused accumulator left the "+
				"labelKey memo pointing at an admitted one, so the refused series' components folded into it",
				i, len(sink.histSums), poison)
		}
	}
}

// The budget is per FAMILY, released with the accumulators it charged. A charge
// left standing over a flushed family would silently stop converting everything
// after the first big one — the same defect wearing the opposite mask.
func TestFamilyBudgetIsReleasedWithTheFamily(t *testing.T) {
	sink := &countingSink{}
	conv := newConverter(sink, nil)
	for i := 0; conv.dropped == 0; i++ {
		if i > 1_000_000 {
			t.Fatalf("the budget was never spent after %d label sets", i)
		}
		if err := conv.add(histSample(strconv.Itoa(i), RoleHistogramBucket, "1", 1)); err != nil {
			t.Fatal(err)
		}
	}
	// A second family, well within the budget on its own.
	second := Sample{Name: "g_bucket", Family: "g", Role: RoleHistogramBucket,
		Labels: []Label{{Name: "id", Value: "only"}, {Name: "le", Value: "1"}}, Value: 3}
	before := conv.dropped
	if err := conv.add(second); err != nil {
		t.Fatal(err)
	}
	if conv.dropped != before {
		t.Fatalf("the next family's first sample was dropped too: the budget did not travel with the family it charged")
	}
	if err := conv.finish(); err != nil {
		t.Fatal(err)
	}
	if sink.hists == 0 {
		t.Fatal("the second family emitted no point at all")
	}
}

// The refusal must reach an operator, and it must reach them as OUR refusal:
// the exposition is well-formed, so counting it malformed would send them
// looking for a defect in the target. Rides the scrape session, which is where
// the per-scrape counters and the warnOnce dedupe live.
func TestFamilyBudgetDropsAreCountedOnTheScrapeSession(t *testing.T) {
	// One line per label set — a body that gzips to almost nothing, which is the
	// reported shape: small on the wire, enormous in the heap.
	var sb strings.Builder
	sb.WriteString("# TYPE h histogram\n")
	for i := 0; i < 60_000; i++ {
		sb.WriteString(`h_bucket{id="`)
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(`",le="1"} 1` + "\n")
	}

	exp := &captureExporter{}
	s := New(Config{Node: "n1", Interval: time.Minute, Exporter: exp, StartTime: time.Now()})
	beforeDropped := obs.ScrapeSamplesDropped.WithLabelValues(pipelineTargets, "accumulator").Value()
	beforeMalformed := obs.ScrapeMalformed.WithLabelValues(pipelineTargets).Value()

	cb := newBatcher(func(pcommon.Resource) {}, time.Now(), time.Now())
	if _, err := s.parseAndExport(context.Background(), strings.NewReader(sb.String()), false, false, cb, pipelineTargets, "t"); err != nil {
		t.Fatal(err)
	}

	if got := obs.ScrapeSamplesDropped.WithLabelValues(pipelineTargets, "accumulator").Value() - beforeDropped; got <= 0 {
		t.Fatalf("dropped counted = %v, want > 0: a target whose family the converter refuses to hold must be "+
			"distinguishable from one that simply has fewer series", got)
	}
	if got := obs.ScrapeMalformed.WithLabelValues(pipelineTargets).Value() - beforeMalformed; got != 0 {
		t.Fatalf("malformed delta = %v, want 0: every line here is valid exposition, and the drop is ours", got)
	}
	if exp.points() == 0 {
		t.Fatal("nothing was exported: the budget must clip the family, not the scrape")
	}
}

// One label set can be as expensive as many: a single series with a runaway
// number of buckets is the same unbounded append. Its _count still arrives, so
// the point it emits stays valid (fillHistogramPoint closes it with the
// overflow bucket) — the resolution is what degrades.
func TestOneLabelSetWithRunawayBucketsIsBounded(t *testing.T) {
	sink := &countingSink{}
	conv := newConverter(sink, nil)
	const buckets = 2_000_000
	for i := 0; i < buckets; i++ {
		if err := conv.add(histSample("one", RoleHistogramBucket, strconv.Itoa(i), float64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	if retained := retainedAccBytes(conv); retained > maxFamilyAccBytes {
		t.Fatalf("one label set retains %d bytes after %d bucket lines, want <= %d", retained, buckets, maxFamilyAccBytes)
	}
	if conv.dropped == 0 {
		t.Fatalf("%d bucket lines against an %d-byte budget dropped nothing", buckets, maxFamilyAccBytes)
	}
	if len(conv.hists) != 1 {
		t.Fatalf("%d accumulators, want 1", len(conv.hists))
	}
}
