package tailer

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// metricSum reads a log-derived counter's exported total.
type metricSum struct{ md []pmetric.Metrics }

func (c *metricSum) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	cp := pmetric.NewMetrics()
	md.CopyTo(cp)
	c.md = append(c.md, cp)
	return nil
}

func (c *metricSum) sum(name string) float64 {
	total := 0.0
	for _, md := range c.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)
					if m.Name() != name || m.Type() != pmetric.MetricTypeSum {
						continue
					}
					dps := m.Sum().DataPoints()
					// The last point: a counter's first export is preceded by
					// synthetic zero points that give rate() a baseline.
					if dps.Len() > 0 {
						total += dps.At(dps.Len() - 1).DoubleValue()
					}
				}
			}
		}
	}
	return total
}

// countingTailer is driveTailer plus a log-derived counter over every line and
// a drop rule, so one flush exercises both observation halves of the chain.
func countingTailer(t *testing.T, dir string, exp *fakeExporter) (*Tailer, *metrics.DynamicMetricSet) {
	t.Helper()
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "log_lines_total", Type: metrics.CounterType, Value: "1",
	}})
	if err != nil {
		t.Fatal(err)
	}
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", MatchRegexp: []string{"__line__=noise"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	tl := driveTailer(dir, exp)
	tl.cfg.LogMetrics = set
	tl.cfg.Rules = rules
	return tl, set
}

// A failed export REWINDS the file and the next sweep re-reads the same bytes.
// That is the durability design and it is right — but rebuilding a record also
// re-ran the per-record chain, so every log-derived metric and every
// kubescrape_log_rules_dropped_total increment was multiplied by the number of
// rewinds a collector outage spanned. Measured with the shape below: 10
// observations for 5 lines and 4 drops for 2 dropped lines, after a single
// rewind — and the multiplier grows with the length of the outage, so the
// series lies worst exactly while an operator reads it to diagnose that outage.
//
// Delivery stays at-least-once (the records really are re-sent). Observation is
// once per RECORD — not per delivery, which is the weaker claim: a record
// withheld by a commit clamp and then rewound is delivered twice and observed
// once, and that is the direction to be wrong in.
func TestTailerObservesOncePerRecordAcrossARewind(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{fail: 3} // exportWithRetry's whole budget: the batch rewinds
	tl, set := countingTailer(t, dir, exp)

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir,
		timeNowCRI()+" stdout F keep one",
		timeNowCRI()+" stdout F noise two",
		timeNowCRI()+" stdout F keep three",
		timeNowCRI()+" stdout F noise four",
		timeNowCRI()+" stdout F keep five",
	)
	tl.scanDir(nil, false)
	dropped := obs.LogRulesDropped.Value()
	rewinds := obs.LogExportFailures.Value()
	driveUntil(t, ctx, tl, func() bool { return len(exp.get()) == 3 }, "the three kept records delivered")

	if got := obs.LogExportFailures.Value() - rewinds; got != 1 {
		t.Fatalf("precondition: want exactly one rewind, got %v", got)
	}
	if exp.attempts < 4 {
		t.Fatalf("precondition: want the batch re-sent after the rewind, attempts = %d", exp.attempts)
	}
	mexp := &metricSum{}
	if err := set.Export(ctx, mexp, 0); err != nil {
		t.Fatal(err)
	}
	if got := mexp.sum("log_lines_total"); got != 5 {
		t.Errorf("log_lines_total = %v across a rewind, want 5 — the chain observes a "+
			"record once per DELIVERY, not once per pass over its bytes", got)
	}
	if got := obs.LogRulesDropped.Value() - dropped; got != 2 {
		t.Errorf("kubescrape_log_rules_dropped_total delta = %v, want 2: the two dropped "+
			"lines were re-counted when the rewind re-read them", got)
	}
}

// THE regression this whole mechanism must never cause, and the defect the
// first attempt at it shipped: suppressing the observation of a record that
// never went through the chain. Silent loss is strictly worse than the
// over-count above — an inflated counter during an outage is visibly weird, a
// missing one is indistinguishable from no traffic.
//
// consume SKIPS lines without feeding them (a rate-DROPPED line, a blank one,
// an oversized line's discard window), and a skipped line sits BELOW the
// position the admitted lines around it reach. The rate limiter is the case
// that makes it observable: its verdict is a function of the token bucket, not
// of the bytes, so the pass after a rewind legitimately ADMITS a line the pass
// before dropped. A per-BYTE-RANGE frontier covers that line and shipped it
// with its observation suppressed — measured on exactly this sequence:
// log_lines_total = 2 for 3 delivered records. Per-ENTRY identities give 3.
// (With no suppression at all it is 5, which is the honest over-count.)
//
// The bucket is driven by hand rather than by sleeping: allowLine refills by
// elapsed time, and RateLimit is set low enough that the refill over a whole
// test run is a rounding error, so f.tokens is the only thing deciding.
func TestRateDroppedLineAdmittedLaterIsStillObserved(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl, set := countingTailer(t, dir, exp)
	tl.cfg.RateLimit = 0.001 // refill over the whole test is ~0 tokens
	tl.cfg.RateBurst = 3
	tl.cfg.RateDrop = true

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, timeNowCRI()+" stdout F keep one")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true) // the bucket initialises to the burst, so line 1 is admitted

	f := tl.files[filepath.Join(dir, logName)]
	if f == nil {
		t.Fatalf("the log file is not tracked: %v", tl.files)
	}
	dropped := obs.LogRulesDropped.Value()
	rateDropped := obs.LogRateLimited.WithLabelValues("drop").Value()

	f.tokens = 0
	writeLog(t, dir, timeNowCRI()+" stdout F keep two")
	tl.sweep(ctx, true) // line 2: no token, so it is DROPPED and never fed

	f.tokens = 1
	writeLog(t, dir, timeNowCRI()+" stdout F keep three")
	tl.sweep(ctx, true) // line 3: admitted, so its end is ABOVE the dropped line's

	if got := obs.LogRateLimited.WithLabelValues("drop").Value() - rateDropped; got != 1 {
		t.Fatalf("precondition: want exactly one rate-dropped line, got %v", got)
	}
	if len(tl.batch) != 2 {
		t.Fatalf("precondition: want lines 1 and 3 in the batch, got %v", bodies(tl))
	}

	exp.mu.Lock()
	exp.fail = 4 // exhaust exportWithRetry's budget: the file rewinds to 0
	exp.mu.Unlock()
	tl.flush(ctx)
	if f.committed != 0 {
		t.Fatalf("precondition: want a rewind to offset 0, committed = %d", f.committed)
	}

	// The re-read finds the bucket refilled, so the line the first pass dropped
	// is admitted this time — at a position the first pass's batch already
	// reached past.
	exp.mu.Lock()
	exp.fail = 0
	exp.mu.Unlock()
	f.tokens = 3
	tl.sweep(ctx, true)
	tl.flush(ctx)

	delivered := exp.get()
	if len(delivered) != 3 {
		t.Fatalf("delivered %v, want all three lines", delivered)
	}
	mexp := &metricSum{}
	if err := set.Export(ctx, mexp, 0); err != nil {
		t.Fatal(err)
	}
	if got := mexp.sum("log_lines_total"); got != 3 {
		t.Errorf("log_lines_total = %v for %d delivered records, want 3: the line the "+
			"first pass rate-DROPPED went through the chain for the first time on the "+
			"second pass, and a frontier covering it suppresses an observation that "+
			"never happened", got, len(delivered))
	}
	if got := obs.LogRulesDropped.Value() - dropped; got != 0 {
		t.Errorf("kubescrape_log_rules_dropped_total delta = %v, want 0 (no line matches "+
			"the drop rule here)", got)
	}
	// Everything committed, so nothing can be re-read and nothing needs
	// remembering: the set must not grow with the file.
	if len(f.observed) != 0 {
		t.Errorf("%d observed identities retained after a full commit, want 0", len(f.observed))
	}
}

// The identity is the entry, and an entry is its byte range AND the body those
// bytes produced. The range alone is nearly enough — within one segment id a
// given offset always holds the same bytes — but not quite: over the SAME
// range, admitting or dropping an interior line changes which logical lines a
// multi-line group joined, so the same range can produce two different records
// on two passes. Suppressing the second would lose an observation.
func TestObservedIdentityIsPerEntryNotPerRange(t *testing.T) {
	f := &file{}
	f.tail = 1
	at := func(body string, start, end int64) entry {
		return entry{file: f, body: body, start: pos{seg: 1, off: start}, end: pos{seg: 1, off: end}}
	}
	first := at("panic: boom\ngoroutine 1 [running]:", 0, 60)
	f.observe(&first)

	if !f.alreadyObserved(first) {
		t.Fatal("the very entry that was observed is not recognised — an identity that " +
			"does not round-trip suppresses nothing at all")
	}
	// Same file, same segment, same byte range — a re-read that joined one more
	// line into the group.
	regrouped := at("panic: boom\nmain.main()\ngoroutine 1 [running]:", 0, 60)
	if f.alreadyObserved(regrouped) {
		t.Error("a DIFFERENT record over the same byte range was taken for the observed " +
			"one: its log metrics and its rules-drop would be lost")
	}
	// Same body, same offsets, different incarnation (a truncation issues a
	// fresh tail id) — the bytes are new even though the numbers repeat.
	next := first
	next.start.seg, next.end.seg = 2, 2
	if f.alreadyObserved(next) {
		t.Error("a later incarnation's entry was taken for the observed one")
	}
	// And the neighbours: one byte off in either direction is a different entry.
	if f.alreadyObserved(at(first.body, 0, 61)) || f.alreadyObserved(at(first.body, 1, 60)) {
		t.Error("an entry over a different range was taken for the observed one")
	}
}

// The same regression seen end to end, through the one production sequence
// that puts LIVE identities up against reused offsets: an export has to FAIL
// first (otherwise everything commits and nothing is retained — the earlier
// version of this test skipped the failure and so could not fail for the
// property it names), and then the file is TRUNCATED while the rewind has it
// back at offset zero. The replacement lines are written to the SAME byte
// ranges, in the SAME segment — the rewind already put the read position at 0,
// so nothing about the file's identity has to change and no fresh tail id is
// issued. The body is the only witness that these are different records.
func TestTruncatedFileIsObservedAgainAtTheSameOffsets(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{fail: 4} // exhaust the retry budget: the file rewinds
	tl, set := countingTailer(t, dir, exp)

	// A FIXED-WIDTH timestamp, unlike timeNowCRI's RFC3339Nano (which trims
	// trailing zeros): the replacement lines have to land on byte-for-byte the
	// same ranges for this to test anything.
	ts := func() string { return time.Now().UTC().Format("2006-01-02T15:04:05.000000000Z07:00") }
	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir,
		ts()+" stdout F keep one",
		ts()+" stdout F keep two",
	)
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	f := tl.files[filepath.Join(dir, logName)]
	if f == nil {
		t.Fatalf("the log file is not tracked: %v", tl.files)
	}
	if len(f.observed) != 2 {
		t.Fatalf("precondition: want the two rewound entries remembered, got %d", len(f.observed))
	}
	before := fileSize(t, filepath.Join(dir, logName))
	seg := f.tail

	if err := os.Truncate(filepath.Join(dir, logName), 0); err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir,
		ts()+" stdout F keep AAA",
		ts()+" stdout F keep BBB",
	)
	if after := fileSize(t, filepath.Join(dir, logName)); after != before {
		t.Fatalf("precondition: the replacement lines must reuse the same byte ranges "+
			"(%d bytes, was %d)", after, before)
	}
	driveUntil(t, ctx, tl, func() bool { return len(exp.get()) == 2 }, "the post-truncation records")
	if f.tail != seg {
		t.Fatalf("precondition: this path must NOT issue a fresh segment id, or the "+
			"reused offsets are trivially distinguishable (%d -> %d)", seg, f.tail)
	}

	mexp := &metricSum{}
	if err := set.Export(ctx, mexp, 0); err != nil {
		t.Fatal(err)
	}
	// 2 pre-truncation lines (observed, then rewound and lost with the
	// truncation) + 2 post-truncation ones.
	if got := mexp.sum("log_lines_total"); got != 4 {
		t.Errorf("log_lines_total = %v, want 4: the replacement lines occupy the ranges "+
			"the rewound entries were observed under, and only their content says they "+
			"are different records", got)
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}
