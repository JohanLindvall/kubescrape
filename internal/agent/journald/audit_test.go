package journald

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog/plogotlp"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// TestAuditNilPositionsStartsAtTail confirms the documented contract: with no
// positions store, loadCursor returns "" so every start opens at the journal
// TAIL — pre-existing entries are history and never exported, only entries that
// arrive after the reader opened are seen.
func TestAuditNilPositionsStartsAtTail(t *testing.T) {
	j := &tailJournal{entries: []rawEntry{
		mkEntry("h0", "a.service", "history-0", "6"),
		mkEntry("h1", "a.service", "history-1", "6"),
	}}
	exp := &captureExporter{}
	r := New(Config{Exporter: exp, FlushInterval: 10 * time.Millisecond, RestartBackoff: 5 * time.Millisecond})
	r.open = j.open
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	defer func() { cancel(); <-done }()

	waitFor(t, "journal opened at tail", func() bool { return j.opened() > 0 })
	j.append(mkEntry("n0", "a.service", "new-0", "6"))
	waitFor(t, "the post-open entry", func() bool { return len(exp.records()) == 1 })

	got := exp.records()
	if len(got) != 1 || got[0] != "new-0" {
		t.Fatalf("records = %v; nil positions must start at tail (no history)", got)
	}
}

// TestAuditTruncationIsCountedAndMarked is a regression guard for commit
// 9c30b18: an over-long journal message is truncated, counted in
// obs.JournalTruncated, and marked with log.truncated + log.original_length.
func TestAuditTruncationIsCountedAndMarked(t *testing.T) {
	before := obs.JournalTruncated.Value()
	big := strings.Repeat("A", 5000)
	entries := []rawEntry{mkEntry("c00", "a.service", big, "6")}
	exp, _ := startReader(t, Config{MaxEntryBytes: 100}, entries, false, 0)
	waitFor(t, "one record", func() bool { return len(exp.records()) == 1 })

	exp.mu.Lock()
	defer exp.mu.Unlock()
	lr := exp.batches[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	if got := len(lr.Body().Str()); got != 100 {
		t.Fatalf("body len = %d, want 100 (truncated)", got)
	}
	if v, ok := lr.Attributes().Get("log.truncated"); !ok || !v.Bool() {
		t.Errorf("log.truncated missing: %v", lr.Attributes().AsRaw())
	}
	if v, ok := lr.Attributes().Get("log.original_length"); !ok || v.Int() != 5000 {
		t.Errorf("log.original_length = %v, want 5000", v)
	}
	if got := obs.JournalTruncated.Value(); got != before+1 {
		t.Errorf("JournalTruncated delta = %v, want 1", got-before)
	}
}

// TestAuditByteCapCountsBodiesOnly documents a scope limit of MaxBatchBytes: it
// bounds the summed BODY bytes only, not the marshaled OTLP payload. With
// enrichment on, each record also carries parsed attributes (exception.*, etc.),
// resource attributes, timestamps and per-record framing — none counted. The
// exported wire payload can therefore run several times past MaxBatchBytes. A
// gRPC collector rejecting an oversized message with ResourceExhausted is
// TRANSIENT (not in otlpexport.IsPermanent), so such a batch is retried, never
// split. The scrape path re-checks bytes between points for exactly this reason;
// journald does not. (Default 4x gRPC headroom usually absorbs it — hence low.)
func TestAuditByteCapCountsBodiesOnly(t *testing.T) {
	trace := strings.Repeat("   at Acme.Worker.Run() in /src/W.cs:line 42\\r\\n", 10)
	line := `{"@l":"Error","@mt":"boom {X}","@x":"System.InvalidOperationException: boom\r\n` + trace + `"}`
	var entries []rawEntry
	for i := 0; i < 20; i++ {
		entries = append(entries, mkEntry(mkCursor(i), "a.service", line, "6"))
	}
	exp, _ := startReader(t, Config{Enrich: true, MaxBatchBytes: 2000, BatchSize: 1000}, entries, false, 0)
	waitFor(t, "all 20 records", func() bool { return len(exp.records()) == 20 })

	exp.mu.Lock()
	defer exp.mu.Unlock()
	worst := 0
	for _, ld := range exp.batches {
		b, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
		if err != nil {
			t.Fatal(err)
		}
		if len(b) > worst {
			worst = len(b)
		}
	}
	t.Logf("largest marshaled batch = %d bytes vs MaxBatchBytes=2000 (bodies-only cap)", worst)
	if worst <= 2000 {
		t.Skipf("payload stayed within cap (%d)", worst)
	}
}

func mkCursor(i int) string { return "c" + string(rune('a'+i)) }

// erroringSource yields a fixed list, then (if err != nil) returns that read
// error, else blocks like a live follower.
type erroringSource struct {
	mu      sync.Mutex
	entries []rawEntry
	err     error
}

func (s *erroringSource) next(ctx context.Context) (rawEntry, bool, error) {
	s.mu.Lock()
	if len(s.entries) > 0 {
		e := s.entries[0]
		s.entries = s.entries[1:]
		s.mu.Unlock()
		return e, true, nil
	}
	s.mu.Unlock()
	if s.err != nil {
		return rawEntry{}, false, s.err
	}
	<-ctx.Done()
	return rawEntry{}, false, ctx.Err()
}

func (s *erroringSource) close() error { return nil }

type recordingOpener struct {
	mu     sync.Mutex
	opened []string
}

// opener resumes by cursor and injects a read error on the FIRST open only.
func (o *recordingOpener) opener(all []rawEntry) openFunc {
	return func(_ Config, after string) (source, error) {
		o.mu.Lock()
		first := len(o.opened) == 0
		o.opened = append(o.opened, after)
		o.mu.Unlock()
		var kept []rawEntry
		for _, e := range all {
			if after == "" || e.cursor > after {
				kept = append(kept, e)
			}
		}
		var err error
		if first {
			err = fmt.Errorf("injected read error")
		}
		return &erroringSource{entries: kept, err: err}, nil
	}
}

func (o *recordingOpener) reopens() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.opened...)
}

// TestAuditReaderErrorReopensFromCommittedCursor exercises the task's specific
// angle: a reader (src.next) error AFTER a cursor has been committed. The
// buffered entries are flushed on the way out (committing their cursor), and the
// source is reopened FROM THE COMMITTED CURSOR — nothing re-emitted, nothing
// lost.
func TestAuditReaderErrorReopensFromCommittedCursor(t *testing.T) {
	entries := []rawEntry{
		mkEntry("c00", "a.service", "one", "6"),
		mkEntry("c01", "a.service", "two", "6"),
	}
	o := &recordingOpener{}
	exp := &captureExporter{}
	// BatchSize 100 + long flush interval: both entries are buffered (uncommitted)
	// when the read error hits, so the error path must flush them.
	r := New(Config{Exporter: exp, BatchSize: 100, FlushInterval: 2 * time.Second, RestartBackoff: 5 * time.Millisecond})
	r.open = o.opener(entries)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	defer func() { cancel(); <-done }()

	waitFor(t, "both entries flushed on the read error", func() bool { return len(exp.records()) == 2 })
	// Let a couple of restart cycles run; nothing must be re-emitted.
	time.Sleep(80 * time.Millisecond)
	if got := exp.records(); len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("records = %v; want exactly one/two", got)
	}
	reopens := o.reopens()
	if len(reopens) < 2 {
		t.Fatalf("expected at least one reopen after the error, got %v", reopens)
	}
	// The first reopen after the error must resume from the committed cursor c01,
	// not from "" (which for a real journal is the tail = silent loss).
	if reopens[1] != "c01" {
		t.Fatalf("reopen[1] resumed from %q, want the committed cursor c01", reopens[1])
	}
}

// TestAuditShutdownFlushesPendingBatch: on ctx cancel, Run's final flush exports
// entries that were buffered but never hit a size/interval/byte flush.
func TestAuditShutdownFlushesPendingBatch(t *testing.T) {
	entries := []rawEntry{
		mkEntry("c00", "a.service", "one", "6"),
		mkEntry("c01", "a.service", "two", "6"),
		mkEntry("c02", "a.service", "three", "6"),
	}
	exp := &captureExporter{}
	// Nothing flushes on its own (huge size + interval); only the shutdown flush can.
	r := New(Config{Exporter: exp, BatchSize: 1000, FlushInterval: time.Hour, RestartBackoff: 5 * time.Millisecond})
	r.open = fakeOpener(entries, false)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()

	time.Sleep(60 * time.Millisecond) // let the entries get ingested
	if got := len(exp.records()); got != 0 {
		t.Fatalf("entries exported before shutdown (%d); the flush isolation is broken", got)
	}
	cancel()
	<-done
	if got := exp.records(); len(got) != 3 {
		t.Fatalf("shutdown flush exported %v, want all three buffered entries", got)
	}
}

// TestAuditTruncationCountedOncePerDelivery pins the fix for the per-read
// double-count: a batch whose first export fails is re-read from the committed
// cursor and re-sanitized, so counting truncations at read time would tally
// them twice. Counting at flush (after a successful export, like JournalEntries)
// makes the metric reflect delivered records exactly once.
func TestAuditTruncationCountedOncePerDelivery(t *testing.T) {
	before := obs.JournalTruncated.Value()
	entries := []rawEntry{
		mkEntry("c00", "a.service", strings.Repeat("x", 200), "6"), // > MaxEntryBytes
	}
	// failures:1 → the first export fails, the batch is re-read and re-sanitized,
	// then the retry succeeds.
	exp, _ := startReader(t, Config{MaxEntryBytes: 20}, entries, false, 1)
	waitFor(t, "record delivered after retry", func() bool { return len(exp.records()) == 1 })
	// A short settle so any erroneous second count would have landed.
	time.Sleep(40 * time.Millisecond)
	if got := obs.JournalTruncated.Value(); got != before+1 {
		t.Fatalf("JournalTruncated rose by %v across a failed-then-retried export, want 1 "+
			"(a per-read counter would double-count the re-read)", got-before)
	}
}
