package promscrape

import (
	"context"
	"slices"
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
	summSums              []float64 // the sum of every emitted summary point
}

func (s *countingSink) addNumber(Sample, bool) { s.numbers++ }
func (s *countingSink) addHistogram(_ string, acc *histAcc) {
	s.hists++
	s.histSums = append(s.histSums, acc.sum)
}
func (s *countingSink) addSummary(_ string, acc *summAcc) {
	s.summs++
	s.summSums = append(s.summSums, acc.sum)
}

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

// summSample builds one component sample of a summary family whose label set
// is identified by id — summary twin of histSample.
func summSample(id string, role promparse.SampleRole, quantile string, value float64) Sample {
	labels := []Label{{Name: "id", Value: id}}
	name := "s_sum"
	switch role {
	case RoleSummaryQuantile:
		labels = append(labels, Label{Name: "quantile", Value: quantile})
		name = "s"
	case RoleSummaryCount:
		name = "s_count"
	}
	return Sample{Name: name, Family: "s", Role: role, Labels: labels, Value: value}
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
	for i := range sets {
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

// The same bound on a SUMMARY family: summ() charges its accumulators and every
// quantile row (quantileRetainBytes) against the one per-family budget, and a
// target is as free to choose a summary's label sets as a histogram's.
func TestSummaryAccumulatorRetentionIsBoundedByBytes(t *testing.T) {
	const sets = 300_000

	sink := &countingSink{}
	conv := newConverter(sink, nil)
	for i := range sets {
		id := strconv.Itoa(i)
		for _, s := range []Sample{
			summSample(id, RoleSummaryQuantile, "0.5", 1),
			summSample(id, RoleSummaryQuantile, "0.99", 2),
			summSample(id, RoleSummarySum, "", 1.5),
			summSample(id, RoleSummaryCount, "", 2),
		} {
			if err := conv.add(s); err != nil {
				t.Fatal(err)
			}
		}
	}
	if retained := retainedAccBytes(conv); retained > maxFamilyAccBytes {
		t.Fatalf("the open summary family retains %d bytes of accumulators, want <= %d (%d label sets offered)",
			retained, maxFamilyAccBytes, sets)
	}
	if conv.dropped == 0 {
		t.Fatalf("nothing was counted dropped although %d label sets were offered against an %d-byte budget",
			sets, maxFamilyAccBytes)
	}
	if len(conv.summs) < 1000 {
		t.Fatalf("only %d label sets admitted: the budget must clip the family, not empty it", len(conv.summs))
	}
	admitted := len(conv.summs)
	if err := conv.finish(); err != nil {
		t.Fatal(err)
	}
	if sink.summs != admitted {
		t.Fatalf("emitted %d summary points, want %d (every admitted label set)", sink.summs, admitted)
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

// The summary twin of TestRefusedLabelSetDoesNotFoldIntoAnotherPoint: summ()
// shares the labelKey memo with hist(), so a refused SUMMARY label set that
// left lastSummAcc pointing at the previously admitted accumulator would fold
// its _sum — and its _count — into another series' Summary point, silently.
func TestRefusedSummaryLabelSetDoesNotFoldIntoAnotherPoint(t *testing.T) {
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
			summSample(id, RoleSummaryQuantile, "0.5", 1),
			summSample(id, RoleSummarySum, "", 1),
		} {
			if err := conv.add(s); err != nil {
				t.Fatal(err)
			}
		}
	}
	// A brand-new label set arriving after the budget is spent: its quantile is
	// refused, and then its _sum must be refused too rather than landing
	// somewhere.
	for _, s := range []Sample{
		summSample("refused", RoleSummaryQuantile, "0.5", 1),
		summSample("refused", RoleSummarySum, "", poison),
	} {
		if err := conv.add(s); err != nil {
			t.Fatal(err)
		}
	}
	if err := conv.finish(); err != nil {
		t.Fatal(err)
	}

	if len(sink.summSums) == 0 {
		t.Fatal("no points were emitted at all, so the scan below proves nothing")
	}
	for i, sum := range sink.summSums {
		if sum == poison {
			t.Fatalf("point %d of %d carries the refused summary's sum (%v): a refused accumulator left the "+
				"labelKey memo pointing at an admitted one, so the refused series' components folded into it",
				i, len(sink.summSums), poison)
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
	for i := range 60_000 {
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
	for i := range buckets {
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

// retainedAfterFlush is the text the converter's REUSE buffers still reference
// once no family is open: the recycled accumulators' label and exemplar slices,
// the labelKey scratch and memo, and the emit-order list, each read across its
// whole backing array. Nothing here is charged to accBytes any more, so any of
// it that survives is heap the family budget no longer accounts for.
func retainedAfterFlush(c *converter) int {
	n := 0
	labels := func(ls []Label) {
		for _, l := range ls[:cap(ls)] {
			n += len(l.Name) + len(l.Value)
		}
	}
	for _, acc := range c.histFree {
		labels(acc.labels)
		for _, e := range acc.exemplars[:cap(acc.exemplars)] {
			labels(e.Labels)
		}
	}
	for _, acc := range c.summFree {
		labels(acc.labels)
	}
	labels(c.keyLbl)
	labels(c.lastLbl)
	for _, k := range c.order[:cap(c.order)] {
		n += len(k)
	}
	return n
}

// The family budget bounds what the OPEN family holds and is released when it
// closes — so whatever outlives the family has to be released with it too.
// Recycled accumulators, the labelKey scratch and memo, and the emit-order
// list were all only resliced, which hid their old entries from len() and not
// from the GC: families whose label count DECREASES leave one long value at
// every position no later family reaches. Measured 130 MiB retained after 128
// in-budget histogram families of 1 MiB values, with dropped=0. Scaled down to
// 64 families of 256 KiB (and the long value rides on an exemplar and a
// summary too, so every reuse path is exercised).
func TestConverterReleasesFamilyTextOnFlush(t *testing.T) {
	const families, longLen = 64, 256 << 10
	long := strings.Repeat("x", longLen)
	c := newConverter(&countingSink{}, nil)
	for k := families; k >= 1; k-- {
		labels := make([]Label, 0, k+1)
		for i := 0; i < k; i++ {
			v := ""
			if i == k-1 {
				v = long
			}
			labels = append(labels, Label{Name: "l" + strconv.Itoa(i), Value: v})
		}
		fam := "h" + strconv.Itoa(k)
		ex := &Exemplar{Labels: []Label{{Name: "trace", Value: long}}, Value: 1}
		bucket := append(slices.Clip(labels), Label{Name: "le", Value: "+Inf"})
		for _, s := range []Sample{
			{Name: fam + "_bucket", Family: fam, Role: RoleHistogramBucket, Labels: bucket, Value: 1, Exemplar: ex},
			{Name: fam + "_count", Family: fam, Role: RoleHistogramCount, Labels: labels, Value: 1},
		} {
			if err := c.add(s); err != nil {
				t.Fatal(err)
			}
		}
		sfam := "s" + strconv.Itoa(k)
		quantile := append(slices.Clip(labels), Label{Name: "quantile", Value: "0.5"})
		if err := c.add(Sample{Name: sfam, Family: sfam, Role: RoleSummaryQuantile, Labels: quantile, Value: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.finish(); err != nil {
		t.Fatal(err)
	}
	if c.dropped != 0 || c.malformed != 0 {
		t.Fatalf("dropped=%d malformed=%d, want 0: every family here is well inside the budget", c.dropped, c.malformed)
	}
	// Once the last family is flushed nothing is open, so nothing of any of
	// them may still be referenced — not even one long value's worth.
	if n := retainedAfterFlush(c); n >= longLen {
		t.Fatalf("the converter still references %d bytes of label text after its last family flushed "+
			"(%d families each left a %d-byte value at a position no later family reaches)", n, families, longLen)
	}
}
