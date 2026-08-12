package journald

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"go.opentelemetry.io/collector/pdata/plog"
)

// feeder is fakeOpener's cursor-resume replay with a tap the test controls:
// entries become visible to the reader only once released, so a test can get a
// cursor committed BEFORE the collector starts failing. That ordering is the
// whole point — with no committed cursor the reader cannot reopen at all, and
// the re-read this file is about never happens.
type feeder struct {
	mu       sync.Mutex
	all      []rawEntry
	released int
	opens    int
	// opened is when the FIRST open landed — the phase reference for the flush
	// ticker, which stream creates microseconds later.
	opened time.Time
}

func (f *feeder) release(n int) {
	f.mu.Lock()
	f.released = min(f.released+n, len(f.all))
	f.mu.Unlock()
}

func (f *feeder) openCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.opens
}

func (f *feeder) openedAt() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.opened
}

// open positions a fresh source just after afterCursor, exactly as
// sdjournal.SeekCursor does.
func (f *feeder) open(_ Config, afterCursor string) (source, error) {
	f.mu.Lock()
	f.opens++
	if f.opens == 1 {
		f.opened = time.Now()
	}
	pos := 0
	for pos < len(f.all) && f.all[pos].cursor <= afterCursor {
		pos++
	}
	f.mu.Unlock()
	return &feedSource{f: f, pos: pos}, nil
}

type feedSource struct {
	f   *feeder
	pos int
}

func (s *feedSource) next(ctx context.Context) (rawEntry, bool, error) {
	for {
		s.f.mu.Lock()
		if s.pos < s.f.released {
			e := s.f.all[s.pos]
			s.pos++
			s.f.mu.Unlock()
			return e, true, nil
		}
		s.f.mu.Unlock()
		select {
		case <-ctx.Done():
			return rawEntry{}, false, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func (s *feedSource) close() error { return nil }

// A collector outage must not inflate the counters an operator reads DURING
// that outage.
//
// An export failure used to tear the reader down; Run then reopened at the
// committed cursor and re-read the whole in-flight batch, and re-reading
// re-runs the per-record chain — so every configured log metric and every
// kubescrape_log_rules_dropped_total increment was multiplied by the number of
// restarts the outage spanned. Measured before the fix, with the shape below:
// 46 observations for 6 entries across 8 restarts, and 9 rule drops for 1
// actually-dropped entry.
//
// Delivery is at-least-once and duplicate records are the accepted cost of
// that; OBSERVATION is exactly-once per delivery, because these series are
// cumulative and nothing downstream can undo a double count.
func TestJournaldObservesOncePerDeliveryAcrossACollectorOutage(t *testing.T) {
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "journal_lines_total", Type: metrics.CounterType, Value: "1",
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
	fd := &feeder{all: []rawEntry{
		mkEntry("c01", "kubelet.service", "first", "6"),
		mkEntry("c02", "kubelet.service", "two", "6"),
		mkEntry("c03", "kubelet.service", "noise three", "6"),
		mkEntry("c04", "kubelet.service", "four", "6"),
		mkEntry("c05", "kubelet.service", "five", "6"),
		mkEntry("c06", "kubelet.service", "six", "6"),
	}}
	pos, err := positions.Open(filepath.Join(t.TempDir(), "positions.json"))
	if err != nil {
		t.Fatal(err)
	}
	exp := &captureExporter{}
	r := New(Config{
		Exporter: exp, FlushInterval: 20 * time.Millisecond, RestartBackoff: 5 * time.Millisecond,
		LogMetrics: set, Rules: rules, Positions: pos,
	})
	r.open = fd.open
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	// Land one entry first: the reader now HAS a committed cursor, which is
	// what the re-read path needs.
	fd.release(1)
	waitFor(t, "the first entry exported", func() bool { return len(exp.records()) == 1 })
	// Read the cursor through the positions store, whose accessor is locked:
	// r.cursor belongs to the Run goroutine.
	waitFor(t, "the cursor committed", func() bool { return pos.JournalCursor() != "" })
	dropped := obs.LogRulesDropped.Value()
	failures := obs.JournalExportFailures.Value()
	rewinds := obs.LogExportFailures.Value()

	// Now the collector goes away for eight attempts.
	exp.mu.Lock()
	exp.failures = 8
	exp.mu.Unlock()
	fd.release(5)
	waitFor(t, "the batch delivered once the collector returns",
		func() bool { return len(exp.records()) == 5 })
	time.Sleep(60 * time.Millisecond) // nothing further may arrive

	mexp := &metricCapture{}
	if err := set.Export(context.Background(), mexp, 0); err != nil {
		t.Fatal(err)
	}
	// 6 entries observed; the 5th ("noise three") is dropped by the rules but
	// metrics see every entry, so the total is 6, not 5.
	if got := mexp.sum("journal_lines_total"); got != 6 {
		t.Errorf("journal_lines_total = %v after an outage spanning 8 failed attempts, want 6 — "+
			"one observation per entry per DELIVERY, not per export attempt", got)
	}
	if got := obs.LogRulesDropped.Value() - dropped; got != 1 {
		t.Errorf("kubescrape_log_rules_dropped_total delta = %v, want 1: "+
			"the one dropped entry was re-counted on every retry", got)
	}
	if got := obs.JournalExportFailures.Value() - failures; got != 8 {
		t.Errorf("kubescrape_journal_export_failures_total delta = %v, want 8 "+
			"(one per failed attempt)", got)
	}
	// kubescrape_log_export_failures_total documents itself as the TAILER's
	// rewind counter ("files rewound"). This reader rewinds nothing and runs
	// happily with -logs=false, where no file exists to rewind, so a journal
	// export failure landing there described something that cannot happen.
	if got := obs.LogExportFailures.Value() - rewinds; got != 0 {
		t.Errorf("kubescrape_log_export_failures_total delta = %v, want 0: "+
			"journald must not bump the tailer's files-rewound counter", got)
	}
	// The reader must not have been torn down: a failed export is a collector
	// problem, and restarting the journal source is what caused the re-read.
	if fd.openCount() != 1 {
		t.Errorf("journal source opened %d times, want 1: an export failure must "+
			"retry in place, not restart the reader", fd.openCount())
	}
	if got := exp.records(); len(got) != 5 || got[1] != "two" {
		t.Errorf("delivered %v, want the first entry plus the four kept ones of the outage batch", got)
	}
}

// The reopen path's clear of the converted payload is now DEFENSIVE — an
// export failure retries in place, so nothing reaches a reopen with a batch
// buffered — which is exactly why it needs its own test: the invariant it keeps
// is silent and catastrophic. A payload outliving the batch it describes makes
// the first flush after the reopen export the PREVIOUS entries and then commit
// the NEW batch's cursor, skipping everything the new batch held.
func TestReopenDiscardsAPayloadThatOutlivedItsBatch(t *testing.T) {
	exp := &captureExporter{}
	r := New(Config{Exporter: exp, FlushInterval: time.Hour, RestartBackoff: time.Millisecond})
	r.cursor = "c00"
	// A batch and its rendering, as an interrupted stream would leave them.
	r.ingest(mkEntry("c01", "a.service", "stale", "6"), "stale", 0)
	stale := r.pending.Render(r.convert)
	if stale.LogRecordCount() != 1 {
		t.Fatalf("precondition: the retained payload must hold the stale entry, got %d",
			stale.LogRecordCount())
	}

	r.open = fakeOpener([]rawEntry{
		mkEntry("c01", "a.service", "stale", "6"),
		mkEntry("c02", "a.service", "fresh", "6"),
	}, true)
	_ = r.stream(context.Background())

	// The reopen re-reads from the COMMITTED cursor, so both entries come back
	// and both must be exported. A retained payload would have exported only
	// the stale one and then committed c02 on top of it — "fresh" gone.
	if got := exp.records(); len(got) != 2 || got[0] != "stale" || got[1] != "fresh" {
		t.Fatalf("exported %v, want [stale fresh]: the payload of the discarded batch "+
			"must go with it, or the re-read entries are committed past unexported", got)
	}
	if r.cursor != "c02" {
		t.Fatalf("cursor = %q, want c02", r.cursor)
	}
}

// stampExporter records when each batch was accepted, and can hold the flush
// for a fixed duration afterwards. The delay is not colour: it is what makes
// the cadence test's failure mode deterministic — see the comment there.
type stampExporter struct {
	mu    sync.Mutex
	delay time.Duration
	when  []time.Time
}

func (e *stampExporter) ExportLogs(_ context.Context, _ plog.Logs) error {
	e.mu.Lock()
	e.when = append(e.when, time.Now())
	d := e.delay
	e.mu.Unlock()
	// Outside the lock: stamps() must stay readable while a flush is in flight.
	time.Sleep(d)
	return nil
}

func (e *stampExporter) stamps() []time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]time.Time(nil), e.when...)
}

// THE MECHANISM: the flush ticker measures its interval from the last FLUSH,
// not from the last tick and not from the stream's start. stream resets it
// after every flush, whatever triggered that flush.
//
// This is the deterministic half of the cadence coverage, and it is a
// single-event test rather than a statistical one: it puts a BATCH-SIZE flush
// half an interval into the ticker's cycle and then measures when the next,
// ticker-driven, flush lands. Three implementations, three separated answers,
// no averaging needed:
//
//	reset after every flush (correct)  : half a period + a full one -> 1.00x
//	free-running ticker, no reset      : the tick already pending     -> 0.50x
//	free-running ticker + the old guard: that tick skipped, the next  -> 1.50x
//
// so a window of 1.00x +/- 0.25x rejects both defects with 100ms of slack on
// either side at a 400ms interval. The lower bound is what a lost
// ticker.Reset trips; the upper bound is what the guard
// TestJournaldFlushCadenceHonoursTheInterval was written for trips.
func TestFlushTickerRestartsFromTheLastFlush(t *testing.T) {
	const interval = 400 * time.Millisecond
	fd := &feeder{all: []rawEntry{
		mkEntry("c01", "kubelet.service", "one", "6"),
		mkEntry("c02", "kubelet.service", "two", "6"),
		mkEntry("c03", "kubelet.service", "three", "6"),
	}}
	exp := &stampExporter{}
	r := New(Config{
		Exporter: exp, FlushInterval: interval, RestartBackoff: 5 * time.Millisecond,
		// Two entries flush; the third then waits for the ticker alone.
		BatchSize: 2, MaxBatchBytes: 1 << 30,
	})
	r.open = fd.open
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	// The ticker is created a few microseconds after the source is opened, so
	// the open is the phase reference. Releasing at half a period is what puts
	// the two wrong answers on opposite sides of the right one.
	waitFor(t, "the journal source opened", func() bool { return fd.openCount() == 1 })
	time.Sleep(time.Until(fd.openedAt().Add(interval / 2)))
	// All three at once: the loop takes them one at a time, so the second
	// triggers the batch flush and the third lands in the batch behind it.
	fd.release(3)

	waitFor(t, "the batch-size flush and the ticker flush behind it",
		func() bool { return len(exp.stamps()) >= 2 })
	stamps := exp.stamps()
	gap := stamps[1].Sub(stamps[0])
	if gap < interval*3/4 || gap > interval*5/4 {
		t.Fatalf("the ticker flush landed %v after the batch flush, want %v +/- 25%%: "+
			"below that the ticker is free-running from the stream's start (the reset "+
			"is gone), above it the tick meant for these entries was skipped by a "+
			"lastFlush guard and they waited for the one after",
			gap.Round(time.Millisecond), interval)
	}
	t.Logf("batch flush -> ticker flush: %v (%.2fx the %v interval)",
		gap.Round(time.Millisecond), float64(gap)/float64(interval), interval)
}

// -journald-flush-interval is documented, in the flag help and on
// Config.FlushInterval, as "flush at least this often" — an UPPER bound on how
// long an entry waits. THE PROMISE, where the test above pins the mechanism
// that keeps it.
//
// It delivered at up to 2.00x. The tick handler guarded on
// time.Since(lastFlush) >= FlushInterval while lastFlush was stamped AFTER the
// flush completed, so the tick the entries were waiting for arrived early
// against its own guard, was skipped, and the flush slid to the tick after it.
//
// THE EXPORT DELAY IS THE LOAD-BEARING PART OF THIS TEST. The guard skips a
// tick exactly when that tick's goroutine wakeup latency is smaller than the
// previous flush's own duration — so with an instant exporter it is a coin
// flip per tick, the buggy cadence averages 1.5x, and a threshold at 1.5x is a
// threshold at the bug's central tendency: measured, the previous version of
// this test passed once in eight runs against a deliberately restored guard
// (gaps came back as a mix of 100ms and 200ms at a 100ms interval). Holding
// every export for 60ms against a 200ms interval makes the skip certain
// instead — wakeup latency would have to swing by 60ms between consecutive
// ticks to un-skip one — so the two cadences separate cleanly:
//
//	fixed: interval + export        = 260ms per flush
//	buggy: two intervals            = 400ms per flush
//
// The threshold is the midpoint, 330ms, and it is applied to the MEAN over
// seven gaps rather than to any single gap, so one scheduling hiccup cannot
// fail it: the fix has 70ms of headroom per gap on average.
func TestJournaldFlushCadenceHonoursTheInterval(t *testing.T) {
	const (
		interval  = 200 * time.Millisecond
		exportFor = 60 * time.Millisecond
		flushes   = 8
		// The midpoint of the fixed cadence (interval+exportFor) and the buggy
		// one (2*interval).
		budget = interval + exportFor + (interval-exportFor)/2
	)
	all := make([]rawEntry, 0, 256)
	for i := 0; i < 256; i++ {
		all = append(all, mkEntry(cursorAt(i), "kubelet.service", "line", "6"))
	}
	fd := &feeder{all: all}
	exp := &stampExporter{delay: exportFor}
	r := New(Config{
		Exporter: exp, FlushInterval: interval, RestartBackoff: 5 * time.Millisecond,
		BatchSize: 1 << 20, MaxBatchBytes: 1 << 30, // only the ticker may flush
	})
	r.open = fd.open
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	// A steady trickle, so there is always something to flush.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		tk := time.NewTicker(interval / 5)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				fd.release(1)
			}
		}
	}()

	waitFor(t, "eight flushes", func() bool { return len(exp.stamps()) >= flushes })
	stamps := exp.stamps()[:flushes]

	// Between the FIRST and last stamp: the delay from the stream's start to
	// the first flush is not a gap and would only dilute the measurement.
	span := stamps[flushes-1].Sub(stamps[0])
	mean := span / (flushes - 1)
	if mean > budget {
		t.Fatalf("%d flushes averaged %v apart (%v total), want <= %v: FlushInterval "+
			"promises an upper bound, and skipping the tick these entries were waiting "+
			"for costs a whole extra interval each time (gaps: %v)",
			flushes, mean.Round(time.Millisecond), span.Round(time.Millisecond),
			time.Duration(budget), gapsFrom(stamps[0], stamps[1:]))
	}
	t.Logf("%d flushes averaged %v apart (%.2fx the %v interval, each export held %v)",
		flushes, mean.Round(time.Millisecond), float64(mean)/float64(interval), interval, exportFor)
}

// A shutdown must not be charged to the collector. backoff.Sleep returns EARLY
// when ctx is done, so the cancellation that ends a retry wait used to be spent
// immediately on one more ExportLogs — an attempt the collector never saw, a
// context.Canceled "failure", and one more
// kubescrape_journal_export_failures_total on every rolling update.
//
// Counted rather than timed: the assertion is on the number of export ATTEMPTS
// the reader makes across the whole shutdown, which the bug changes from 2 to 3.
func TestShutdownDoesNotSpendAnotherExportAttempt(t *testing.T) {
	fd := &feeder{all: []rawEntry{mkEntry("c01", "kubelet.service", "one", "6")}}
	exp := &captureExporter{failures: 1 << 30} // the collector is down for good
	r := New(Config{
		Exporter: exp, FlushInterval: 10 * time.Millisecond,
		// Long enough that the cancel below lands inside the backoff wait.
		RestartBackoff: 2 * time.Second,
	})
	r.open = fd.open
	before := obs.JournalExportFailures.Value()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()

	fd.release(1)
	waitFor(t, "the first export attempt", func() bool { return exp.attempts() >= 1 })
	cancel()
	<-done

	// One attempt from the stream (which then waits out its backoff and is
	// cancelled), one from Run's final flush on its detached budgeted context.
	if got := exp.attempts(); got != 2 {
		t.Errorf("%d export attempts across the shutdown, want 2 (the streaming one "+
			"and the final flush): a cancelled context must end the retry loop, not "+
			"buy it another attempt", got)
	}
	if got := obs.JournalExportFailures.Value() - before; got != 2 {
		t.Errorf("kubescrape_journal_export_failures_total delta = %v, want 2: only "+
			"attempts the collector actually refused may count", got)
	}
}

func cursorAt(i int) string { return string(rune('a'+i/26)) + string(rune('a'+i%26)) }

func gapsFrom(start time.Time, stamps []time.Time) []time.Duration {
	out := make([]time.Duration, 0, len(stamps))
	prev := start
	for _, ts := range stamps {
		out = append(out, ts.Sub(prev).Round(time.Millisecond))
		prev = ts
	}
	return out
}
