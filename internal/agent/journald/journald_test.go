package journald

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/JohanLindvall/enrich"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

// captureExporter records exported batches; failures makes the next n exports
// error.
type captureExporter struct {
	mu       sync.Mutex
	batches  []plog.Logs
	failures int
	// tries counts EVERY call, delivered or refused — what a test asserting on
	// how many times the reader went to the collector reads.
	tries int
}

func (c *captureExporter) ExportLogs(_ context.Context, ld plog.Logs) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tries++
	if c.failures > 0 {
		c.failures--
		return errors.New("injected export failure")
	}
	c.batches = append(c.batches, ld)
	return nil
}

func (c *captureExporter) attempts() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tries
}

func (c *captureExporter) records() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, ld := range c.batches {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			rl := ld.ResourceLogs().At(i)
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				lrs := rl.ScopeLogs().At(j).LogRecords()
				for k := 0; k < lrs.Len(); k++ {
					out = append(out, lrs.At(k).Body().Str())
				}
			}
		}
	}
	return out
}

// fakeSource replays a fixed list of entries, then either ends (ended=true, to
// exercise the restart path) or blocks like a live follower until ctx is done.
type fakeSource struct {
	mu      sync.Mutex
	entries []rawEntry
	ended   bool
}

func (f *fakeSource) next(ctx context.Context) (rawEntry, bool, error) {
	f.mu.Lock()
	if len(f.entries) > 0 {
		e := f.entries[0]
		f.entries = f.entries[1:]
		f.mu.Unlock()
		return e, true, nil
	}
	f.mu.Unlock()
	if f.ended {
		return rawEntry{}, false, nil
	}
	<-ctx.Done()
	return rawEntry{}, false, ctx.Err()
}

func (f *fakeSource) close() error { return nil }

// fakeOpener builds an opener that, on each (re)open, replays the entries whose
// cursor sorts after afterCursor — mirroring cursor-based resume.
func fakeOpener(all []rawEntry, ended bool) openFunc {
	return func(_ Config, afterCursor string) (source, error) {
		var kept []rawEntry
		for _, e := range all {
			if afterCursor == "" || e.cursor > afterCursor {
				kept = append(kept, e)
			}
		}
		return &fakeSource{entries: kept, ended: ended}, nil
	}
}

// entry builds a rawEntry with the common journal fields.
func mkEntry(cursor, unit, msg, priority string) rawEntry {
	return rawEntry{
		message:   msg,
		priority:  priority,
		unit:      unit,
		pid:       "42",
		ident:     "stub",
		transport: "journal",
		cursor:    cursor,
		realtime:  time.UnixMicro(1_700_000_000_000000),
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func startReader(t *testing.T, cfg Config, entries []rawEntry, ended bool, failures int) (*captureExporter, context.CancelFunc) {
	t.Helper()
	exp := &captureExporter{failures: failures}
	cfg.Exporter = exp
	cfg.FlushInterval = 20 * time.Millisecond
	cfg.RestartBackoff = 10 * time.Millisecond
	r := New(cfg)
	r.open = fakeOpener(entries, ended)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return exp, cancel
}

func TestJournalExport(t *testing.T) {
	entries := []rawEntry{
		mkEntry("c00", "kubelet.service", "one", "6"),
		mkEntry("c01", "kubelet.service", "two", "6"),
		mkEntry("c02", "sshd.service", "three", "6"),
	}
	exp, _ := startReader(t, Config{}, entries, false, 0)
	waitFor(t, "3 records", func() bool { return len(exp.records()) == 3 })

	// Per-unit resources: kubelet entries share one, sshd gets its own.
	exp.mu.Lock()
	defer exp.mu.Unlock()
	units := map[string]int{}
	for _, ld := range exp.batches {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			rl := ld.ResourceLogs().At(i)
			svc, _ := rl.Resource().Attributes().Get("service.name")
			unit, _ := rl.Resource().Attributes().Get("systemd.unit")
			n := 0
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				n += rl.ScopeLogs().At(j).LogRecords().Len()
			}
			units[svc.Str()+"/"+unit.Str()] += n
			lr := rl.ScopeLogs().At(0).LogRecords().At(0)
			if lr.SeverityNumber() != plog.SeverityNumberInfo || lr.SeverityText() != "info" {
				t.Errorf("severity = %v %q", lr.SeverityNumber(), lr.SeverityText())
			}
			if pid, ok := lr.Attributes().Get("process.pid"); !ok || pid.Int() != 42 {
				t.Errorf("process.pid = %v", pid)
			}
			if tr, ok := lr.Attributes().Get("systemd.transport"); !ok || tr.Str() != "journal" {
				t.Errorf("systemd.transport = %v", tr)
			}
		}
	}
	if units["kubelet/kubelet.service"] != 2 || units["sshd/sshd.service"] != 1 {
		t.Errorf("per-unit record counts = %v", units)
	}
}

func TestJournalCursorResume(t *testing.T) {
	entries := []rawEntry{
		mkEntry("c00", "a.service", "one", "6"),
		mkEntry("c01", "a.service", "two", "6"),
		mkEntry("c02", "a.service", "three", "6"),
	}
	posPath := filepath.Join(t.TempDir(), "positions.json")
	pos, _ := positions.Open(posPath)

	exp, cancel := startReader(t, Config{Positions: pos}, entries, false, 0)
	waitFor(t, "first run's 3 records", func() bool { return len(exp.records()) == 3 })
	waitFor(t, "cursor committed", func() bool { return pos.JournalCursor() == "c02" })
	cancel()

	// A fresh reader resumes past the committed cursor: nothing re-emitted.
	exp2, _ := startReader(t, Config{Positions: mustOpenPositions(t, posPath)}, entries, false, 0)
	time.Sleep(150 * time.Millisecond)
	if got := exp2.records(); len(got) != 0 {
		t.Fatalf("resumed run re-emitted %v", got)
	}
}

func TestJournalExportFailureRereads(t *testing.T) {
	entries := []rawEntry{
		mkEntry("c00", "a.service", "one", "6"),
		mkEntry("c01", "a.service", "two", "6"),
	}
	// One injected export failure: the uncommitted batch is re-read after the
	// reader restarts (no cursor committed), so both entries arrive.
	exp, _ := startReader(t, Config{
		Positions: mustOpenPositions(t, filepath.Join(t.TempDir(), "positions.json")),
	}, entries, false, 1)

	waitFor(t, "re-read records", func() bool { return len(exp.records()) == 2 })
	if got := exp.records(); got[0] != "one" || got[1] != "two" {
		t.Fatalf("records = %v", got)
	}
}

func TestJournalRestartAfterExit(t *testing.T) {
	// A source that ends after one entry: the reader restarts it and (thanks to
	// the committed cursor) does not duplicate.
	entries := []rawEntry{mkEntry("c0", "a.service", "only", "4")}
	exp, _ := startReader(t, Config{
		Positions: mustOpenPositions(t, filepath.Join(t.TempDir(), "positions.json")),
	}, entries, true, 0)

	waitFor(t, "one record", func() bool { return len(exp.records()) == 1 })
	time.Sleep(200 * time.Millisecond) // several restart cycles
	if got := exp.records(); len(got) != 1 {
		t.Fatalf("records after restarts = %v", got)
	}
}

func TestJournalEnrich(t *testing.T) {
	// The JSON message carries its own level, which must win over the journal's
	// PRIORITY (6 = info) when enrichment is on.
	msg := `{"@t":"2026-01-02T03:04:05Z","level":"error","msg":"boom"}`
	entries := []rawEntry{mkEntry("c0", "a.service", msg, "6")}
	exp, _ := startReader(t, Config{Enrich: true}, entries, false, 0)
	waitFor(t, "one record", func() bool { return len(exp.records()) == 1 })

	exp.mu.Lock()
	defer exp.mu.Unlock()
	lr := exp.batches[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	if lr.SeverityNumber() != plog.SeverityNumberError || lr.SeverityText() != "error" {
		t.Errorf("severity = %v %q; want the line's own level over PRIORITY", lr.SeverityNumber(), lr.SeverityText())
	}
	if !lr.Timestamp().AsTime().Equal(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("timestamp = %v; want the line's own", lr.Timestamp().AsTime())
	}
}

func TestJournalLogAttrs(t *testing.T) {
	// Structured JSON messages carry a tenant; as a resource attribute it splits
	// records into separate resources even within one unit.
	entries := []rawEntry{
		mkEntry("c00", "a.service", `{"tenant":"x","req":"r1"}`, "6"),
		mkEntry("c01", "a.service", `{"tenant":"y","req":"r2"}`, "6"),
	}
	ex, err := logattrs.New(&logattrs.Config{Rules: []logattrs.Rule{
		{Key: "tenant", Attribute: "tenant.id", Target: logattrs.TargetResource},
		{Key: "req", Target: logattrs.TargetLog},
	}})
	if err != nil {
		t.Fatal(err)
	}
	exp, _ := startReader(t, Config{LogAttrs: ex}, entries, false, 0)
	waitFor(t, "2 records", func() bool { return len(exp.records()) == 2 })

	exp.mu.Lock()
	defer exp.mu.Unlock()
	tenants := map[string]int{}
	for _, ld := range exp.batches {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			rl := ld.ResourceLogs().At(i)
			v, _ := rl.Resource().Attributes().Get("tenant.id")
			n := 0
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				n += rl.ScopeLogs().At(j).LogRecords().Len()
			}
			tenants[v.Str()] += n
		}
	}
	if tenants["x"] != 1 || tenants["y"] != 1 {
		t.Errorf("tenant record counts = %+v (want one resource each)", tenants)
	}
}

// permanentExporter rejects the first n batches with a permanent (4xx)
// collector error, delivering afterwards.
type permanentExporter struct {
	captureExporter
	rejections int
}

func (p *permanentExporter) ExportLogs(ctx context.Context, ld plog.Logs) error {
	p.mu.Lock()
	if p.rejections > 0 {
		p.rejections--
		p.mu.Unlock()
		return &otlpexport.HTTPStatusError{Code: 400, Body: "bad payload"}
	}
	p.mu.Unlock()
	return p.captureExporter.ExportLogs(ctx, ld)
}

// A permanently rejected batch is dropped and the cursor committed past it —
// no infinite reopen-and-reread loop on a poison batch.
func TestJournalPermanentRejectionSkips(t *testing.T) {
	entries := []rawEntry{
		mkEntry("c00", "a.service", "poison", "6"),
	}
	posPath := filepath.Join(t.TempDir(), "positions.json")
	pos, _ := positions.Open(posPath)

	exp := &permanentExporter{rejections: 1}
	cfg := Config{Positions: pos, Exporter: exp, FlushInterval: 20 * time.Millisecond, RestartBackoff: 10 * time.Millisecond}
	r := New(cfg)
	r.open = fakeOpener(entries, false)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	// The cursor advances past the dropped batch and nothing is re-emitted.
	waitFor(t, "cursor committed past the poison batch", func() bool { return pos.JournalCursor() == "c00" })
	time.Sleep(100 * time.Millisecond)
	if got := exp.records(); len(got) != 0 {
		t.Fatalf("dropped batch re-emitted: %v", got)
	}
}

// The drop counts RECORDS, not just the batch it was in.
//
// A journal batch is up to Config.BatchSize (1024) entries, so
// kubescrape_journal_dropped_batches_total answers "did a permanent rejection
// happen" and cannot answer "how much was lost" — which is the question an
// operator has when a poison batch is skipped past. The reader holds the
// decoded payload at that point, so the count is free.
func TestJournalPermanentRejectionCountsRecords(t *testing.T) {
	entries := []rawEntry{
		mkEntry("c00", "a.service", "poison one", "6"),
		mkEntry("c01", "a.service", "poison two", "6"),
		mkEntry("c02", "a.service", "poison three", "6"),
	}
	posPath := filepath.Join(t.TempDir(), "positions.json")
	pos, _ := positions.Open(posPath)

	beforeBatches := obs.JournalDropped.Value()
	beforeRecords := obs.JournalDroppedRecords.Value()

	exp := &permanentExporter{rejections: 1}
	cfg := Config{
		Positions: pos, Exporter: exp,
		BatchSize:     3, // all three entries in one rejected batch
		FlushInterval: 20 * time.Millisecond, RestartBackoff: 10 * time.Millisecond,
	}
	r := New(cfg)
	r.open = fakeOpener(entries, false)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	waitFor(t, "cursor committed past the poison batch", func() bool { return pos.JournalCursor() == "c02" })

	if got := obs.JournalDropped.Value() - beforeBatches; got != 1 {
		t.Fatalf("dropped batches = %v, want 1", got)
	}
	if got := obs.JournalDroppedRecords.Value() - beforeRecords; got != 3 {
		t.Fatalf("dropped records = %v, want 3 — the batch counter alone cannot "+
			"tell an operator whether one entry or a thousand was lost", got)
	}
}

// Batches flush before their summed bodies exceed MaxBatchBytes, so one
// exported payload never blows the collector's request cap.
func TestJournalBatchByteCap(t *testing.T) {
	big := strings.Repeat("x", 400)
	entries := []rawEntry{
		mkEntry("c00", "a.service", big, "6"),
		mkEntry("c01", "a.service", big, "6"),
		mkEntry("c02", "a.service", big, "6"),
	}
	exp, _ := startReader(t, Config{MaxBatchBytes: 1000, BatchSize: 100}, entries, false, 0)
	waitFor(t, "3 records", func() bool { return len(exp.records()) == 3 })

	exp.mu.Lock()
	defer exp.mu.Unlock()
	for _, ld := range exp.batches {
		bytes := 0
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			rl := ld.ResourceLogs().At(i)
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				lrs := rl.ScopeLogs().At(j).LogRecords()
				for k := 0; k < lrs.Len(); k++ {
					bytes += len(lrs.At(k).Body().Str())
				}
			}
		}
		if bytes > 1000 {
			t.Errorf("batch bodies = %d bytes, over the 1000-byte cap", bytes)
		}
	}
	if len(exp.batches) < 2 {
		t.Errorf("batches = %d, want the byte cap to split them", len(exp.batches))
	}
}

// Bodies are exported as valid UTF-8 and the entry cap never splits a rune.
func TestJournalSanitizesBodies(t *testing.T) {
	entries := []rawEntry{
		mkEntry("c00", "a.service", "bad \xff bytes", "6"),
		mkEntry("c01", "a.service", "smiles ☺☺☺☺", "6"), // truncation lands mid-rune
	}
	exp, _ := startReader(t, Config{MaxEntryBytes: 12}, entries, false, 0)
	waitFor(t, "2 records", func() bool { return len(exp.records()) == 2 })

	got := exp.records()
	if got[0] != "bad � byte" { // sanitized, then capped at 12 bytes
		t.Errorf("body = %q, want the invalid byte replaced", got[0])
	}
	for i, body := range got {
		if !utf8.ValidString(body) {
			t.Errorf("body %d = %q is not valid UTF-8", i, body)
		}
		if len(body) > 12 {
			t.Errorf("body %d = %q exceeds MaxEntryBytes", i, body)
		}
	}
	// "smiles " is 7 bytes; each ☺ is 3 — the 12-byte cap falls mid-rune and
	// must back off to 10 bytes.
	if got[1] != "smiles ☺" {
		t.Errorf("body = %q, want rune-safe truncation", got[1])
	}
}

func TestSeverityMapping(t *testing.T) {
	cases := []struct {
		priority string
		num      plog.SeverityNumber
		text     string
	}{
		// The top three syslog severities are FATAL in the OTel logs data
		// model, not ERROR. These read Fatal/Error3/Error2 — one grade low
		// across the board, and one grade off the enrich package's own
		// syslogSeverity table, which logenrich.Apply overwrites this number
		// with whenever it parses the body.
		{"0", plog.SeverityNumberFatal3, "emerg"},
		{"1", plog.SeverityNumberFatal2, "alert"},
		{"2", plog.SeverityNumberFatal, "crit"},
		{"3", plog.SeverityNumberError, "err"},
		{"4", plog.SeverityNumberWarn, "warning"},
		{"5", plog.SeverityNumberInfo2, "notice"},
		{"6", plog.SeverityNumberInfo, "info"},
		{"7", plog.SeverityNumberDebug, "debug"},
		{"8", plog.SeverityNumberUnspecified, ""},
		{"", plog.SeverityNumberUnspecified, ""},
		{"junk", plog.SeverityNumberUnspecified, ""},
	}
	for _, c := range cases {
		num, text := severity(c.priority)
		if num != c.num || text != c.text {
			t.Errorf("severity(%q) = %v,%q want %v,%q", c.priority, num, text, c.num, c.text)
		}
	}
}

// TestSeverityAgreesWithEnrich pins the journal's syslog mapping against the
// enrich package's, which implements the same OTel table and which this reader
// runs over every message.
//
// The two disagreeing is not a cosmetic inconsistency: convert calls
// logenrich.Apply with overwrite semantics, so whenever enrich manages to parse
// a level out of the body, ITS number replaces the one severity() derived from
// the journal priority. With the two tables apart, one journal entry's exported
// severity depended on whether its text happened to look like a log line to a
// parser — the same crit entry landing on 18 or on 21 for no reason a consumer
// could see.
func TestSeverityAgreesWithEnrich(t *testing.T) {
	// enrich's syslogSeverity is unexported, so go through its public parse of
	// a syslog-shaped priority; the numbers are the exported constants.
	want := map[string]int{
		"0": enrich.Fatal3LevelNo,
		"1": enrich.Fatal2LevelNo,
		"2": enrich.FatalLevelNo,
		"3": enrich.ErrorLevelNo,
		"4": enrich.WarnLevelNo,
		"5": enrich.Info2LevelNo,
		"6": enrich.InfoLevelNo,
		"7": enrich.DebugLevelNo,
	}
	for prio, w := range want {
		if got, _ := severity(prio); int(got) != w {
			t.Errorf("severity(%q) = %d, but enrich maps that syslog severity to %d — "+
				"logenrich.Apply overwrites ours with theirs whenever the body parses", prio, got, w)
		}
	}
}

// TestSeverityTextIsLowercase pins the casing every log producer in the repo
// now shares. enrich writes lowercase level names and Apply overwrites the
// text along with the number, so anything else is contradicted one line later
// on whichever subset of records happens to parse.
func TestSeverityTextIsLowercase(t *testing.T) {
	for _, prio := range []string{"0", "1", "2", "3", "4", "5", "6", "7"} {
		_, text := severity(prio)
		if text != strings.ToLower(text) {
			t.Errorf("severity(%q) text = %q, want lowercase", prio, text)
		}
	}
}

func mustOpenPositions(t *testing.T, path string) *positions.Store {
	t.Helper()
	s, err := positions.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// --- tail-seek semantics (audit regression guards) ---

// tailJournal models the REAL sdjournal seek semantics, which fakeOpener does
// not: opening with an empty cursor starts at the TAIL (SeekTail+Previous), so
// entries already read but not yet committed cannot be recovered by reopening.
// fakeOpener behaves like SeekHead and therefore hides that whole class of
// loss.
type tailJournal struct {
	mu      sync.Mutex
	entries []rawEntry
	opens   int
}

func (j *tailJournal) append(e rawEntry) {
	j.mu.Lock()
	j.entries = append(j.entries, e)
	j.mu.Unlock()
}

func (j *tailJournal) opened() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.opens
}

func (j *tailJournal) at(i int) (rawEntry, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if i < len(j.entries) {
		return j.entries[i], true
	}
	return rawEntry{}, false
}

// open positions just after afterCursor, or at the tail when it is empty.
func (j *tailJournal) open(_ Config, afterCursor string) (source, error) {
	j.mu.Lock()
	j.opens++
	pos := len(j.entries) // tail: only genuinely new entries are seen
	if afterCursor != "" {
		pos = 0
		for i, e := range j.entries {
			if e.cursor == afterCursor {
				pos = i + 1
				break
			}
		}
	}
	j.mu.Unlock()
	return &tailSource{j: j, pos: pos}, nil
}

type tailSource struct {
	j   *tailJournal
	pos int
}

func (s *tailSource) next(ctx context.Context) (rawEntry, bool, error) {
	for {
		if e, ok := s.j.at(s.pos); ok {
			s.pos++
			return e, true, nil
		}
		select {
		case <-ctx.Done():
			return rawEntry{}, false, ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func (s *tailSource) close() error { return nil }

// TestJournalNoLossBeforeFirstCommit guards the at-least-once bar in the window
// where NO cursor has been committed yet (the first run, or no positions store
// at all). A transient export failure used to restart the reader, whose reopen
// seeked to the journal tail — silently dropping every buffered entry. The
// reader must instead keep retrying the pending batch until it lands.
func TestJournalNoLossBeforeFirstCommit(t *testing.T) {
	j := &tailJournal{entries: []rawEntry{mkEntry("c00", "a.service", "history", "6")}}
	exp := &captureExporter{failures: 2}
	r := New(Config{
		Exporter:       exp,
		FlushInterval:  20 * time.Millisecond,
		RestartBackoff: 5 * time.Millisecond,
	})
	r.open = j.open
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	defer func() { cancel(); <-done }()

	// Only entries appended after the reader opened the journal are in scope.
	waitFor(t, "journal opened", func() bool { return j.opened() > 0 })
	j.append(mkEntry("c01", "a.service", "one", "6"))
	j.append(mkEntry("c02", "a.service", "two", "6"))

	waitFor(t, "both entries despite the failed exports", func() bool {
		return len(exp.records()) == 2
	})
	if got := exp.records(); got[0] != "one" || got[1] != "two" {
		t.Fatalf("records = %v (history must not be re-read, nothing may be lost)", got)
	}
}

// An entry with only a syslog identifier must not share a resource group
// with an entry whose UNIT has the same name: the group's resource carries
// systemd.unit only for real units, so sharing mis-attributed whichever
// entry arrived second.
func TestIdentDoesNotConflateWithUnit(t *testing.T) {
	entries := []rawEntry{
		{cursor: "c00", ident: "foo.service", message: "ident-only", priority: "6", realtime: time.Unix(10, 0)},
		{cursor: "c01", unit: "foo.service", message: "real-unit", priority: "6", realtime: time.Unix(11, 0)},
	}
	exp, _ := startReader(t, Config{}, entries, false, 0)
	waitFor(t, "2 records", func() bool { return len(exp.records()) == 2 })

	exp.mu.Lock()
	defer exp.mu.Unlock()
	byBody := map[string]bool{} // body -> resource has systemd.unit
	for _, ld := range exp.batches {
		rls := ld.ResourceLogs()
		for i := 0; i < rls.Len(); i++ {
			rl := rls.At(i)
			_, hasUnit := rl.Resource().Attributes().Get("systemd.unit")
			sls := rl.ScopeLogs()
			for j := 0; j < sls.Len(); j++ {
				recs := sls.At(j).LogRecords()
				for k := 0; k < recs.Len(); k++ {
					byBody[recs.At(k).Body().AsString()] = hasUnit
				}
			}
		}
	}
	if byBody["ident-only"] {
		t.Fatal("ident-only entry landed on a resource with systemd.unit")
	}
	if !byBody["real-unit"] {
		t.Fatal("real-unit entry lost its systemd.unit resource attribute")
	}
}

// A failed export leaves the CONVERTED payload cached (flush converts once per
// batch, deliberately, so retries do not re-observe log metrics). stream()
// discards the batch on every reopen; if it does not discard the cache with it,
// the first flush after a restart exports the PREVIOUS batch and then commits
// the NEW batch's cursor — every entry the new batch held beyond the stale
// payload is skipped, permanently and silently.
//
// Since flushRetry, an export failure no longer ends the stream at all (it
// retries the same batch in place), so what this drives is a reopen over a
// journal that GREW: the first stream delivers its batch on the retry and the
// second must pick up from that batch's cursor, exporting the new entry and
// nothing else twice.
func TestStalePayloadIsNotReusedAfterRestart(t *testing.T) {
	first := []rawEntry{
		mkEntry("c01", "a.service", "one", "6"),
		mkEntry("c02", "a.service", "two", "6"),
	}
	all := append(append([]rawEntry{}, first...), mkEntry("c03", "a.service", "three", "6"))

	exp := &captureExporter{failures: 1} // one transient failure, then success
	r := New(Config{Exporter: exp, FlushInterval: time.Hour, RestartBackoff: time.Millisecond})
	// A cursor HAS been committed before, so Run's restart path applies (with
	// none it deliberately retries the pending batch instead of reopening).
	r.cursor = "c00"
	ctx := context.Background()

	r.open = fakeOpener(first, true)
	_ = r.stream(ctx) // converts, export fails, cursor stays at c00

	r.open = fakeOpener(all, true) // the journal grew; the reader restarts
	_ = r.stream(ctx)

	got := exp.records()
	for _, want := range []string{"one", "two", "three"} {
		found := false
		for _, g := range got {
			if g == want {
				found = true
			}
		}
		if !found {
			t.Errorf("entry %q was never exported, but the cursor advanced to %q (exported: %v)",
				want, r.cursor, got)
		}
	}
}
