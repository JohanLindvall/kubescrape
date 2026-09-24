package journald

// Tests for the reader's OBSERVABILITY contracts: the repairs it makes to an
// entry before exporting it change the record the operator receives and are
// invisible in that record, so each needs a counter for the rate and a
// throttled line for the unit.

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

type journalCapture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *journalCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *journalCapture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

func captureLog(r *Reader, level slog.Level) *journalCapture {
	c := &journalCapture{}
	r.log = slog.New(slog.NewTextHandler(c, &slog.HandlerOptions{Level: level}))
	return c
}

// TestInvalidUTF8IsCountedAndNamesItsUnit. The journal stores raw BYTES, so a
// producer writing binary makes the exported body differ from what it wrote —
// silently, because a replacement rune looks exactly like a character the
// producer chose. The unit is the actionable half and the counter is the rate.
func TestInvalidUTF8IsCountedAndNamesItsUnit(t *testing.T) {
	r := sanitizer(1 << 20)
	logs := captureLog(r, slog.LevelInfo)

	before := obs.JournalEntryDefects.WithLabelValues(defectInvalidUTF8).Value()
	body, _ := r.sanitize("ok \xff\xfe bytes", "binary-logger.service")
	if strings.Contains(body, "\xff") {
		t.Fatalf("body %q still holds the invalid bytes", body)
	}
	if got := obs.JournalEntryDefects.WithLabelValues(defectInvalidUTF8).Value() - before; got != 1 {
		t.Fatalf("%s counted %v, want 1", defectInvalidUTF8, got)
	}
	out := logs.String()
	if !strings.Contains(out, "not valid UTF-8") || !strings.Contains(out, "unit=binary-logger.service") {
		t.Fatalf("the repair did not name its unit; got:\n%s", out)
	}

	// THROTTLED: a unit that logs raw bytes does it on every message, so the
	// line must not follow the entry rate. The counter still does.
	for range 20 {
		r.sanitize("ok \xff bytes", "binary-logger.service")
	}
	if n := strings.Count(logs.String(), "not valid UTF-8"); n != 1 {
		t.Fatalf("the warning fired %d times inside its window, want 1 (the counter carries the rate)", n)
	}
	if got := obs.JournalEntryDefects.WithLabelValues(defectInvalidUTF8).Value() - before; got != 21 {
		t.Fatalf("%s counted %v, want 21: throttling the LINE must not throttle the counter", defectInvalidUTF8, got)
	}
}

// TestValidMessageReportsNoDefect: the counter must mean something, so the
// overwhelmingly common path may not touch it.
func TestValidMessageReportsNoDefect(t *testing.T) {
	r := sanitizer(1 << 20)
	before := obs.JournalEntryDefects.WithLabelValues(defectInvalidUTF8).Value()
	if _, origLen := r.sanitize("a perfectly ordinary message", "ok.service"); origLen != 0 {
		t.Fatalf("origLen = %d, want 0", origLen)
	}
	if got := obs.JournalEntryDefects.WithLabelValues(defectInvalidUTF8).Value() - before; got != 0 {
		t.Fatalf("a valid message counted %v defects, want 0", got)
	}
}

// TestTruncationPastTheInvalidBytesReportsNoDefect pins the deliberate
// asymmetry: only the SURVIVING bytes are probed, so an over-cap message whose
// invalid bytes fall past the cut exports a body byte-identical to what the
// producer wrote for as far as it goes. The truncation itself is already
// carried by log.truncated and kubescrape_journal_truncated_total; reporting a
// UTF-8 defect for bytes nobody receives would be a false positive on a
// counter an operator is meant to act on.
func TestTruncationPastTheInvalidBytesReportsNoDefect(t *testing.T) {
	r := sanitizer(8)
	before := obs.JournalEntryDefects.WithLabelValues(defectInvalidUTF8).Value()
	body, origLen := r.sanitize("cleanASCII\xff\xfe", "mixed.service")
	if origLen == 0 || len(body) > 8 {
		t.Fatalf("sanitize = (%q, %d), want a truncated body within the cap", body, origLen)
	}
	if got := obs.JournalEntryDefects.WithLabelValues(defectInvalidUTF8).Value() - before; got != 0 {
		t.Fatalf("counted %v defects for bytes that were cut away anyway, want 0", got)
	}
}

// TestMissingTimestampIsCountedAndNamesItsUnit. An entry with no realtime stamp
// is dated with the AGENT's clock, which looks exactly as authoritative as the
// producer's and is what makes a post-restart backlog appear to have happened
// all at once.
func TestMissingTimestampIsCountedAndNamesItsUnit(t *testing.T) {
	r := New(Config{MaxEntryBytes: 1 << 20, BatchSize: 10, MaxBatchBytes: 1 << 20})
	logs := captureLog(r, slog.LevelInfo)

	before := obs.JournalEntryDefects.WithLabelValues(defectNoTimestamp).Value()
	r.ingest(rawEntry{unit: "clockless.service", priority: "6"}, "hello", 0)
	if got := obs.JournalEntryDefects.WithLabelValues(defectNoTimestamp).Value() - before; got != 1 {
		t.Fatalf("%s counted %v, want 1", defectNoTimestamp, got)
	}
	if len(r.batch) != 1 || r.batch[0].ts.IsZero() {
		t.Fatal("the entry must still be exported, dated with our own clock")
	}
	out := logs.String()
	if !strings.Contains(out, "carried no timestamp") || !strings.Contains(out, "unit=clockless.service") {
		t.Fatalf("the substitution did not name its unit; got:\n%s", out)
	}

	// An entry that DOES carry a stamp reports nothing.
	r.ingest(rawEntry{unit: "ok.service", priority: "6", realtime: time.Now()}, "hello", 0)
	if got := obs.JournalEntryDefects.WithLabelValues(defectNoTimestamp).Value() - before; got != 1 {
		t.Fatalf("%s counted %v after a stamped entry, want it unchanged at 1", defectNoTimestamp, got)
	}
}

// TestExportRecoveryIsReported: without it a collector outage ends in silence
// that reads exactly like the reader having stopped, and the only evidence
// delivery resumed is a counter that stops moving.
func TestExportRecoveryIsReported(t *testing.T) {
	r := New(Config{MaxEntryBytes: 1 << 20, BatchSize: 10, MaxBatchBytes: 1 << 20})
	logs := captureLog(r, slog.LevelInfo)

	for range 4 {
		r.exportOutage.Fail(time.Now().Add(-90*time.Second), 0)
	}
	// flushRetry's success arm, reached with an empty batch (flush returns nil
	// immediately), which is the settled state a recovered export leaves.
	if err := r.flushRetry(t.Context()); err != nil {
		t.Fatalf("flushRetry: %v", err)
	}
	out := logs.String()
	if !strings.Contains(out, "journal export recovered") || !strings.Contains(out, "failures=4") {
		t.Fatalf("recovery was not reported with its failure count; got:\n%s", out)
	}
	if n := r.exportOutage.Failures(); n != 0 {
		t.Fatalf("exportOutage.Failures() = %d after recovery, want 0", n)
	}
	// Quiet in the steady state.
	if err := r.flushRetry(t.Context()); err != nil {
		t.Fatalf("flushRetry: %v", err)
	}
	if n := strings.Count(logs.String(), "journal export recovered"); n != 1 {
		t.Fatalf("recovery logged %d times, want 1", n)
	}
}

// TestOneDefectClassDoesNotSuppressTheOther. The two read-side repairs are
// independent conditions with different remedies, and they co-occur on exactly
// the same kind of unit — one emitting raw bytes is also the kind that arrives
// without a realtime stamp. Under a single keyless throttle whichever fired
// first silenced the other for the whole window, so a node could be
// substituting its own clock for every entry of a unit with nothing in the log
// to say so and only a counter climbing.
func TestOneDefectClassDoesNotSuppressTheOther(t *testing.T) {
	r := New(Config{MaxEntryBytes: 1 << 20, BatchSize: 10, MaxBatchBytes: 1 << 20})
	logs := captureLog(r, slog.LevelInfo)

	// Invalid UTF-8 first, claiming its own window.
	r.sanitize("ok \xff bytes", "binary-logger.service")
	// The other class, immediately afterwards and well inside that window.
	r.ingest(rawEntry{unit: "clockless.service", priority: "6"}, "hello", 0)

	out := logs.String()
	if !strings.Contains(out, "not valid UTF-8") {
		t.Fatalf("the UTF-8 repair was not reported; got:\n%s", out)
	}
	if !strings.Contains(out, "carried no timestamp") || !strings.Contains(out, "unit=clockless.service") {
		t.Fatalf("the missing-timestamp substitution was suppressed by the UTF-8 warning's throttle; got:\n%s", out)
	}
	// Each class is still throttled within itself.
	for range 5 {
		r.sanitize("ok \xff bytes", "binary-logger.service")
		r.ingest(rawEntry{unit: "clockless.service", priority: "6"}, "hello", 0)
	}
	if n := strings.Count(logs.String(), "not valid UTF-8"); n != 1 {
		t.Fatalf("UTF-8 repair logged %d times, want 1: the per-class gate is not throttling", n)
	}
	if n := strings.Count(logs.String(), "carried no timestamp"); n != 1 {
		t.Fatalf("missing-timestamp logged %d times, want 1: the per-class gate is not throttling", n)
	}
}

// TestPerUnitDebugAnswersWhyAUnitIsNotShipping. Below the aggregate counters
// the reader had nothing to say about an individual unit, so the commonest
// operator question — "kubelet.service is in the journal, where are its logs?"
// — was unanswerable without a packet capture: the two candidate answers
// (nothing is arriving; the rules are dropping it) look identical from
// kubescrape_journal_entries_total alone. The report is per BATCH and per UNIT,
// never per entry, and every part of it is behind an Enabled guard because slog
// evaluates arguments eagerly.
func TestPerUnitDebugAnswersWhyAUnitIsNotShipping(t *testing.T) {
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", MatchRegexp: []string{"__line__=healthz"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	entries := []rawEntry{
		mkEntry("c1", "kubelet.service", "GET /healthz ok", "6"),
		mkEntry("c2", "kubelet.service", "pod started", "6"),
		mkEntry("c3", "kubelet.service", "probe healthz again", "6"),
	}
	exp := &captureExporter{}
	r := New(Config{
		Exporter:      exp,
		FlushInterval: 20 * time.Millisecond,
		Units:         []string{"kubelet.service", "containerd.service"},
		Chain:         logchain.Config{Rules: rules},
	})
	logs := captureLog(r, slog.LevelDebug)
	r.open = fakeOpener(entries, false)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	waitFor(t, "the kept entry exported", func() bool { return len(exp.records()) == 1 })
	waitFor(t, "the batch report", func() bool { return strings.Contains(logs.String(), "journal batch settled") })

	out := logs.String()
	// Which units the journal will hand over at all: the match set is applied
	// inside libsystemd, so a unit missing from it never reaches this process.
	if !strings.Contains(out, "journal source opened") || !strings.Contains(out, "start=tail") ||
		!strings.Contains(out, "units=kubelet.service,containerd.service") {
		t.Fatalf("the source's position and match set were not reported; got:\n%s", out)
	}
	// The unit IS arriving...
	if !strings.Contains(out, "journal unit active") || !strings.Contains(out, "unit=kubelet.service") {
		t.Fatalf("no per-unit line, so an arriving unit cannot be told from a silent one; got:\n%s", out)
	}
	// ...and entries vs records is what separates "nothing arrived" from "the
	// rules dropped it".
	if !strings.Contains(out, "entries=3") || !strings.Contains(out, "records=1") {
		t.Fatalf("the batch report did not separate ingested entries from delivered records; got:\n%s", out)
	}
	// And the resume point moved, which is what a restart depends on.
	if !strings.Contains(out, "journal cursor committed") {
		t.Fatalf("the cursor commit was not reported; got:\n%s", out)
	}
}

// TestPerUnitDebugCostsNothingAtInfo pins the guard. The tally walks the batch
// and fills a map; slog evaluates arguments eagerly, so an unguarded report
// pays that on every flush at Info — the exact defect this campaign found
// elsewhere — while emitting nothing at all.
func TestPerUnitDebugCostsNothingAtInfo(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector adds bookkeeping allocations")
	}
	r := New(Config{MaxEntryBytes: 1 << 20, BatchSize: 1000, MaxBatchBytes: 1 << 20})
	captureLog(r, slog.LevelInfo)
	// Enough distinct units that the counts boxed into the Debug call cannot
	// come from the runtime's small-integer cache: an unguarded report is 3
	// allocations here, a guarded one is none.
	for i := range 300 {
		r.ingest(mkEntry("c", fmt.Sprintf("unit-%d.service", i), "hello", "6"), "hello", 0)
	}
	batch := append([]entry(nil), r.batch...)
	allocs := testing.AllocsPerRun(20, func() {
		r.batch = append(r.batch[:0], batch...) // refilling the batch allocates nothing
		r.debugBatch(context.Background(), 0, 0)
	})
	if allocs > 0 {
		t.Fatalf("debugBatch allocated %v at Info: the tally is running unguarded", allocs)
	}
	if r.unitDebug.Len() != 0 {
		t.Fatalf("the per-unit table holds %d keys after Info-level settles, want 0", r.unitDebug.Len())
	}
}

// TestCursorWriteFailureWarnsOnceAndRecovers. The positions file is the SAME
// store the tailer writes, and the tailer throttles its failure; the journal
// cursor used to warn on every retry (every cursorPersistEvery, and on every
// batch before the first cursor ever landed) with no end marker. A read-only
// mount or a full disk persists, so the useful information is the first line,
// a periodic reminder and the line that says it is over.
func TestCursorWriteFailureWarnsOnceAndRecovers(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root writes into a read-only directory anyway")
	}
	dir := filepath.Join(t.TempDir(), "positions")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	pos := mustOpenPositions(t, filepath.Join(dir, "positions.json"))
	r := New(Config{Positions: pos, Exporter: &captureExporter{}})
	r.cursorPersistEvery = 0 // every commit is a write attempt
	now := time.Unix(1_700_000_000, 0)
	r.now = func() time.Time { return now }
	logs := captureLog(r, slog.LevelInfo)
	ctx := context.Background()
	n := 0
	commit := func() {
		t.Helper()
		n++
		c := fmt.Sprintf("c%03d", n)
		r.ingest(mkEntry(c, "a.service", "m", "6"), "m", 0)
		if err := r.flush(ctx); err != nil {
			t.Fatal(err)
		}
		now = now.Add(5 * time.Second)
	}
	readOnly := func(ro bool) {
		t.Helper()
		mode := os.FileMode(0o700)
		if ro {
			mode = 0o500
		}
		if err := os.Chmod(dir, mode); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	const failed, recovered = "writing journal cursor to positions file", "journal cursor write recovered"
	count := func(msg string, want int, when string) {
		t.Helper()
		if got := strings.Count(logs.String(), msg); got != want {
			t.Fatalf("%s: %q logged %d times, want %d:\n%s", when, msg, got, want, logs.String())
		}
	}

	readOnly(true)
	for range 6 {
		commit()
	}
	count(failed, 1, "six failed writes inside a minute")
	now = now.Add(cursorWarnEvery)
	commit()
	count(failed, 2, "a failed write once the interval elapsed")
	count(recovered, 0, "still failing")

	readOnly(false)
	commit()
	commit()
	count(recovered, 1, "the first write that lands, and one after it")
	if got := pos.JournalCursor(); got != fmt.Sprintf("c%03d", n) {
		t.Fatalf("stored cursor = %q, want the newest commit", got)
	}

	// A new outage speaks at once, inside the interval the last line claimed.
	readOnly(true)
	commit()
	count(failed, 3, "the first failure of a new outage")
}

// TestPerUnitDebugNamesWhatTheResourceCarries. The per-unit line exists to be
// matched against the exported resources, so it must group and name exactly as
// convert does: an entry carrying neither a unit nor a syslog identifier is the
// resource named "journald" (it used to be reported as unit=""), and a syslog
// identifier that equals a real unit's name is a SEPARATE group — two resources,
// so two lines, where the report used to fold them into one tally.
func TestPerUnitDebugNamesWhatTheResourceCarries(t *testing.T) {
	r := New(Config{MaxEntryBytes: 1 << 20, BatchSize: 100, MaxBatchBytes: 1 << 20})
	logs := captureLog(r, slog.LevelDebug)
	r.ingest(rawEntry{unit: "cron.service", priority: "6", realtime: time.Now()}, "a", 0)
	r.ingest(rawEntry{ident: "cron.service", priority: "6", realtime: time.Now()}, "b", 0)
	r.ingest(rawEntry{priority: "6", realtime: time.Now()}, "c", 0)
	r.debugBatch(context.Background(), 3, 0)

	out := logs.String()
	if !strings.Contains(out, "units=3") {
		t.Fatalf("the batch line does not count convert's three groups:\n%s", out)
	}
	if n := strings.Count(out, "journal unit active\" unit=cron.service entries=1"); n != 2 {
		t.Fatalf("%d per-unit lines for cron.service, want 2 (the unit and the same-named syslog identifier are two resources):\n%s", n, out)
	}
	if !strings.Contains(out, "journal unit active\" unit=journald entries=1") {
		t.Fatalf("an entry with neither a unit nor an identifier is not reported by the name its resource carries (journald):\n%s", out)
	}
}
