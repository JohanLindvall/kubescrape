// Tests for the per-file pipeline (pipeline.go): CRI parsing, multiline
// joining, oversize/truncation handling and rate limiting.
package tailer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/multiline/patterns"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestMultiline(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := New(Config{
		Dir:              dir,
		PollInterval:     20 * time.Millisecond,
		FlushInterval:    50 * time.Millisecond,
		BatchSize:        1000,
		Multiline:        true,
		MultilineTimeout: 200 * time.Millisecond,
		MetadataWait:     time.Second,
		Metadata:         fakeMeta{},
		Exporter:         exp,
	})
	tl.retryBackoff = 10 * time.Millisecond
	stop := startTailer(t, tl)
	defer stop()

	// A Go panic followed by a normal line: the trace joins into one entry.
	writeLog(t, dir,
		"2026-07-05T10:00:00Z stderr F panic: boom",
		"2026-07-05T10:00:00Z stderr F ",
		"2026-07-05T10:00:00Z stderr F goroutine 1 [running]:",
		"2026-07-05T10:00:00Z stderr F main.main()",
		"2026-07-05T10:00:00Z stderr F \t/app/main.go:10 +0x20",
		"2026-07-05T10:00:01Z stdout F normal line",
	)
	waitFor(t, func() bool { return len(exp.get()) >= 2 }, "aggregated records")

	var joined, plain bool
	for _, r := range exp.get() {
		if strings.Contains(r, "panic: boom") && strings.Contains(r, "\n") &&
			strings.Contains(r, "main.go:10") {
			joined = true
		}
		if r == "normal line" {
			plain = true
		}
	}
	if !joined || !plain {
		t.Fatalf("joined=%v plain=%v records=%q", joined, plain, exp.get())
	}
}

func TestNonCRILinePassthrough(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	stop := startTailer(t, tl)
	defer stop()

	// A line that is not CRI-formatted is forwarded as-is rather than lost.
	writeLog(t, dir, "plain text, no CRI prefix")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "passthrough record")
	if exp.get()[0] != "plain text, no CRI prefix" {
		t.Fatalf("records = %v", exp.get())
	}
}

// TestDeferredCRIEmissionOffsets pins the closed-run ledger fix: the stage
// defers a multi-fragment run's emission until the next line for the key is
// fed, so the entry's commit offset must be the F line's end (not the
// triggering line's), and the triggering P fragment must keep watermark
// coverage (previously the callback stole its registration).
func TestDeferredCRIEmissionOffsets(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	f := &file{
		path:        filepath.Join(dir, logName),
		source:      &compiledSource{name: "containers", containerd: true},
		containerID: "0123456789abcdef",
		resolved:    true,
		resource:    pcommon.NewResource(),
	}
	tl.newPipeline(f)
	tl.files[f.path] = f

	l1 := timeNowCRI() + " stdout P hello-"
	l2 := timeNowCRI() + " stdout F world"
	l3 := timeNowCRI() + " stdout P dangling-"
	ctx := context.Background()
	off := int64(0)
	for _, l := range []string{l1, l2, l3} {
		end := off + int64(len(l)) + 1
		tl.feedLine(ctx, f, l, off, end)
		off = end
	}
	endF := int64(len(l1) + len(l2) + 2)

	// Feeding l3 flushed the closed run: exactly one entry, bounded by the
	// run's own lines.
	if len(tl.batch) != 1 {
		t.Fatalf("batch entries: %d", len(tl.batch))
	}
	e := tl.batch[0]
	if e.body != "hello-world" {
		t.Fatalf("body %q", e.body)
	}
	if e.start.off != 0 || e.end.off != endF {
		t.Fatalf("entry range [%d,%d), want [0,%d)", e.start.off, e.end.off, endF)
	}
	// The dangling fragment must still clamp the watermark.
	wm, ok := f.watermark()
	if !ok || wm.off != endF {
		t.Fatalf("watermark = %+v,%v, want off %d,true (fragment lost coverage)", wm, ok, endF)
	}
}

// The multiline package's default matcher prefilters its start-state regexes
// with literals derived from the patterns (>10x per-line CPU; see
// BenchmarkIngestLine). If a future pattern change makes the literal set
// unprovable the matcher silently falls back to full regex evaluation —
// still correct, but the per-line budget regresses. This is the alarm.
func TestPrefilterEnabled(t *testing.T) {
	lits := patterns.MustCompile(patterns.All...).StartLiterals()
	if len(lits) == 0 {
		t.Fatal("the compiled matcher has no start literals; the prefilter is disabled and per-line CPU regresses ~12x")
	}
	t.Logf("start literals: %q", lits)
}

// Pause mode: an exhausted file stops being read but nothing is lost — the
// backlog drains as tokens refill.
func TestRateLimitPause(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.cfg.RateLimit = 40 // lines/s
	tl.cfg.RateBurst = 10
	stop := startTailer(t, tl)
	defer stop()

	rateLines(t, dir, 0, 60)

	// Shortly after the burst is consumed only a fraction may have passed
	// (burst 10 + <=400ms of refill ≈ 26 max, with margin).
	time.Sleep(300 * time.Millisecond)
	if n := len(exp.get()); n >= 60 {
		t.Fatalf("rate limit had no effect: %d records immediately", n)
	}

	// Everything arrives eventually, in order (pause loses nothing).
	waitFor(t, func() bool { return len(exp.get()) == 60 }, "all 60 records")
	got := exp.get()
	for i, body := range got {
		if want := fmt.Sprintf("line-%03d", i); body != want {
			t.Fatalf("record %d = %q, want %q (order must be preserved)", i, body, want)
		}
	}
}

// Drop mode: excess lines are discarded, reading never stalls.
func TestRateLimitDrop(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.cfg.RateLimit = 20
	tl.cfg.RateBurst = 10
	tl.cfg.RateDrop = true
	stop := startTailer(t, tl)
	defer stop()

	rateLines(t, dir, 0, 200)

	// The file must drain promptly (drop mode never pauses); with burst 10 and
	// ~20/s refill only a fraction of the 200 survives.
	waitFor(t, func() bool { return len(exp.get()) >= 5 }, "some records")
	time.Sleep(500 * time.Millisecond)
	n := len(exp.get())
	if n >= 200 {
		t.Fatalf("drop mode exported all %d records", n)
	}
	// Survivors keep their original relative order.
	got := exp.get()
	last := -1
	for _, body := range got {
		var idx int
		if _, err := fmt.Sscanf(body, "line-%d", &idx); err != nil {
			t.Fatalf("unexpected body %q", body)
		}
		if idx <= last {
			t.Fatalf("out-of-order survivor %q after line-%03d", body, last)
		}
		last = idx
	}
}

// Pause-mode rate limiting leaves complete read lines in pending. An in-place
// truncation must not discard them (reopen salvages pending before the reset):
// pause mode's contract is "no loss".
func TestPausePendingSurvivesTruncation(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.RateLimit = 1
	tl.cfg.RateBurst = 1

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir,
		"2026-07-05T10:00:00Z stdout F line-1",
		"2026-07-05T10:00:01Z stdout F line-2",
		"2026-07-05T10:00:02Z stdout F line-3",
	)
	tl.scanDir(nil, false)
	tl.sweep(ctx, true) // burst covers line-1; line-2/3 pause in pending
	path := filepath.Join(dir, logName)
	f := tl.files[path]
	if f == nil || !f.limited || len(f.pending) == 0 {
		t.Fatalf("precondition: want paused file with pending (limited=%v pending=%d)",
			f != nil && f.limited, len(f.pending))
	}

	// Copytruncate lands while paused: content replaced, smaller.
	if err := os.WriteFile(path, []byte("2026-07-05T10:00:03Z stdout F fresh\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	driveUntil(t, ctx, tl, func() bool {
		got := exp.get()
		return slices.Contains(got, "line-2") && slices.Contains(got, "line-3") && slices.Contains(got, "fresh")
	}, "pause-retained lines survive the truncation")
}

// Rate-limit PAUSE mode across a rename rotation: the paused backlog of the
// rotated-away inode must still be drained (the drain bypasses the limiter),
// with nothing lost.
func TestRateLimitPauseAcrossRotation(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.RateLimit = 5 // lines/s
	tl.cfg.RateBurst = 2 // only 2 lines pass before the file pauses

	tl.scanDir(tl.loadCheckpoints(), true)
	var lines []string
	for i := 0; i < 20; i++ {
		lines = append(lines, fmt.Sprintf("2026-07-05T10:00:00Z stdout F line-%02d", i))
	}
	writeLog(t, dir, lines...)
	tl.scanDir(nil, false)

	tl.sweep(ctx, true) // consumes the burst, then pauses (f.limited)
	tl.flush(ctx)
	if f := tl.files[filepath.Join(dir, logName)]; !f.limited {
		t.Fatalf("setup: file not paused by the rate limit (tokens=%v)", f.tokens)
	}

	// Rotate while paused: the backlog lives only in the rotated-away inode.
	rotateAway(t, dir, 1)
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F after-rotation")

	// Sweep until the (rate-limited) new inode's line gets through too; the
	// bucket refills at 5/s, so a few hundred ms of sweeps suffice.
	for i := 0; i < 30 && !slices.Contains(exp.get(), "after-rotation"); i++ {
		tl.sweep(ctx, true)
		tl.flush(ctx)
		time.Sleep(50 * time.Millisecond)
	}

	got := exp.get()
	for i := 0; i < 20; i++ {
		want := fmt.Sprintf("line-%02d", i)
		if !slices.Contains(got, want) {
			t.Fatalf("AT-LEAST-ONCE VIOLATED: %q lost — rate-limit pause + rotation; exported = %v", want, got)
		}
	}
	if !slices.Contains(got, "after-rotation") {
		t.Fatalf("post-rotation line missing: %v", got)
	}
}

// A line longer than MaxEntryBytes+4096 with no newline is discarded, and the
// discard must keep the offset invariant lineStart+len(pending) == readPos —
// otherwise every subsequent offset (checkpoint, log.file.position) is
// silently corrupt.
func TestOversizedUnterminatedLineKeepsOffsetsExact(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.MaxEntryBytes = 1024
	tl.cfg.FileAttributes = true

	tl.scanDir(tl.loadCheckpoints(), true)
	// 6000 bytes, no newline yet: exceeds MaxEntryBytes+4096, discarded unseen.
	path := filepath.Join(dir, logName)
	if err := os.WriteFile(path, []byte(strings.Repeat("y", 6000)), 0o644); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false)
	tl.sweep(ctx, true) // oversized pending discarded, discard window open
	tl.flush(ctx)
	if got := exp.get(); len(got) != 0 {
		t.Fatalf("discarded blob exported: %v", got)
	}

	// The line's eventual terminator closes the discard window WITHOUT
	// exporting the mid-line suffix; the next real line's position must be
	// the true byte offset.
	fh, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fh.WriteString("suffix-of-oversized-line\n"); err != nil {
		t.Fatal(err)
	}
	_ = fh.Close()
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F tail")
	tl.sweep(ctx, true)
	tl.flush(ctx)
	got := exp.get()
	if slices.Contains(got, "suffix-of-oversized-line") {
		t.Fatalf("mid-line suffix of the discarded line exported as a record: %v", got)
	}
	if !slices.Contains(got, "tail") {
		t.Fatalf("tail line missing: %v", got)
	}
	f := tl.files[path]
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if f.committed != st.Size() {
		t.Fatalf("committed = %d, want file size %d (offset invariant broken by the discard)", f.committed, st.Size())
	}
	idx := slices.Index(got, "tail")
	rec, ok := exp.record(idx)
	if !ok {
		t.Fatal("tail record not found")
	}
	// log.file.position carries the record's START offset: the true byte
	// position of the tail line right after the discarded oversized line
	// (6000 blob bytes + 25 suffix bytes incl. its newline).
	if pos, ok := rec.Attributes().Get("log.file.position"); !ok || pos.Int() != 6025 {
		t.Fatalf("log.file.position = %v, want 6025", pos.Int())
	}
}

// An entry truncated by the multiline byte cap carries log.truncated.
func TestTruncatedEntryCarriesAttribute(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveMultilineTailer(dir, exp)

	tl.scanDir(tl.loadCheckpoints(), true)
	// A joined Go panic exceeding the 64-byte cap: the stage truncates the
	// group and flags the entry.
	start, rest := panicLines()
	writeLog(t, dir, append(start, rest...)...)
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	time.Sleep(80 * time.Millisecond) // age the buffered group out
	tl.sweep(ctx, true)               // FlushBefore emits it (capped -> Truncated)
	tl.flush(ctx)

	got := exp.get()
	if len(got) == 0 {
		t.Fatal("nothing exported")
	}
	found := false
	for i := range got {
		rec, ok := exp.record(i)
		if !ok {
			break
		}
		if v, ok := rec.Attributes().Get("log.truncated"); ok && v.Bool() {
			found = true
			if len(rec.Body().Str()) > 64 {
				t.Fatalf("truncated record body is %d bytes, cap 64", len(rec.Body().Str()))
			}
		}
	}
	if !found {
		t.Fatalf("no record carries log.truncated (exports: %d records, first %q)", len(got), got[0])
	}
}
