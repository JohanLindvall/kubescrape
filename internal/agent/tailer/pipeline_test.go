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

	"github.com/JohanLindvall/kubescrape/internal/testrace"
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

// TestClosedRunEmissionOffsets pins the closed-run ledger accounting: an
// F-closed multi-fragment run emits within the F line's own AddParsed
// (multiline >= v0.0.11 FinalMatcher), so the entry's range must be the run's
// own [runStart, F-line end) — and a P fragment fed AFTERWARDS starts a fresh
// run that keeps watermark coverage of its own bytes.
func TestClosedRunEmissionOffsets(t *testing.T) {
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

	// The F line flushed its run within its own feed: exactly one entry,
	// bounded by the run's own lines.
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

// consume advances f.pending by RE-SLICING, so its spare capacity drains to
// zero and the next chunk's append allocates a fresh >= 64 KiB array — garbage
// proportional to the log volume read. One buffer per file: the only per-chunk
// allocations left are consume's per-line string(line).
func TestPendingBufferIsReusedAcrossChunks(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	ctx := context.Background()
	tl, f := benchTailer(t, Config{Multiline: true})
	chunk, lines := benchChunk()

	allocs := testing.AllocsPerRun(20, func() {
		tl.ingestChunk(ctx, f, chunk, false)
		tl.batch = tl.batch[:0]
	})
	if allocs > float64(lines) {
		t.Fatalf("ingestChunk allocated %v times for %d lines: the carry buffer is "+
			"being reallocated per chunk (want <= one string(line) per line)", allocs, lines)
	}
	if cap(f.pendingBase) < len(chunk) {
		t.Fatalf("carry buffer cap = %d, want >= the chunk size %d (it must be REUSED, not regrown)",
			cap(f.pendingBase), len(chunk))
	}
}

// BenchmarkIngestLine REPORTS the per-line budget; a benchmark cannot fail a
// build, so this is what holds it. The line path (CRI parse, offset ledger,
// both multiline stages, batch append) is walked once per log line on every
// node in the fleet: a per-line closure, a fmt call, a map operation on a
// non-string key or a re-sliced scratch buffer all cost an allocation here and
// nothing else would notice. The only allocation the path is allowed is the
// batch slice growing, which amortizes to well under one per line.
func TestIngestLineAllocationBudget(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	tl, f := benchTailer(t, Config{Multiline: true})
	lines := benchLines(1024)
	feedAll(tl, f, lines) // grow the batch and warm every intern cache
	tl.batch = tl.batch[:0]

	i := 0
	allocs := testing.AllocsPerRun(len(lines), func() {
		feedOne(tl, f, lines[i])
		if i++; i == len(lines) {
			i = 0
			tl.batch = tl.batch[:0]
		}
	})
	if allocs > 0.5 {
		t.Fatalf("the per-line path allocates %v times per line, want ~0 "+
			"(BenchmarkIngestLine reports 0 allocs/op and the design depends on it)", allocs)
	}
}

// The flush path is the production shape: pipeline + record building + enrich +
// export. It is NOT allocation-free — every record is a pdata log record — but
// its budget is a small constant per line, not a function of the line's
// content. A regression here is a per-line map or closure in the flush loop.
func TestIngestFlushAllocationBudget(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	ctx := context.Background()
	tl, f := benchTailer(t, Config{Multiline: true, Enrich: true})
	lines := benchLines(1024)
	feedAll(tl, f, lines)
	tl.flush(ctx)

	i := 0
	allocs := testing.AllocsPerRun(len(lines), func() {
		feedOne(tl, f, lines[i])
		if i++; i == len(lines) {
			i = 0
			tl.flush(ctx)
		}
	})
	if allocs > 4 {
		t.Fatalf("the flush path allocates %v times per line, want <= 4 "+
			"(BenchmarkIngestFlush/enrich reports 4 allocs/op)", allocs)
	}
}

// One over-long line must not pin a huge carry buffer on the file for good: a
// node tracks thousands of files. It is released once the file drains.
func TestOversizedPendingBufferIsReleasedWhenIdle(t *testing.T) {
	ctx := context.Background()
	tl, f := benchTailer(t, Config{MaxEntryBytes: 1 << 20})

	// A line just under the discard bound, delivered whole: the buffer has to
	// grow past maxIdlePendingBytes to hold it.
	big := make([]byte, tl.cfg.MaxEntryBytes)
	for i := range big {
		big[i] = 'y'
	}
	tl.ingestChunk(ctx, f, append(big, '\n'), false)
	if cap(f.pendingBase) != 0 {
		t.Fatalf("carry buffer of %d bytes kept after the oversized line drained", cap(f.pendingBase))
	}

	// Still usable, and the ordinary steady-state buffer is NOT thrown away.
	chunk, _ := benchChunk()
	tl.ingestChunk(ctx, f, chunk, false)
	if cap(f.pendingBase) == 0 || cap(f.pendingBase) > maxIdlePendingBytes {
		t.Fatalf("carry buffer cap = %d after a normal chunk, want (0, %d]",
			cap(f.pendingBase), maxIdlePendingBytes)
	}
	if !emitted(tl, "handled request") && len(bodies(tl)) == 0 {
		t.Fatal("no records produced after the buffer was released")
	}
}

// The newline TERMINATING an oversized discarded line is not a line: in DROP
// mode the rate limiter used to consume it (dropping "a line" that was never
// one) while leaving f.discarding set, so the discard window stayed open over
// the next GOOD line and swallowed it. Pause mode was unaffected — it returns
// with the fragment still pending and re-evaluates it once tokens refill.
func TestOversizedLineTailNotSwallowedInRateDropMode(t *testing.T) {
	ctx := context.Background()
	tl, f := benchTailer(t, Config{
		MaxEntryBytes: 1024,
		RateLimit:     1, // 1/s: the bucket does not refill within the test
		RateBurst:     1,
		RateDrop:      true,
	})

	// An oversized unterminated line: its prefix is discarded, opening the
	// discard window.
	tl.ingestChunk(ctx, f, []byte(strings.Repeat("y", tl.cfg.MaxEntryBytes+4097)), false)
	if !f.discarding {
		t.Fatal("precondition: oversized prefix was not discarded")
	}

	// Empty the bucket, then deliver the discarded line's terminating fragment:
	// it must close the window without consulting the limiter.
	f.tokens, f.lastRefill = 0, time.Now()
	tl.ingestChunk(ctx, f, []byte("suffix-of-oversized-line\n"), false)
	if f.discarding {
		t.Fatal("the oversized line's terminator did not close the discard window")
	}

	// One token back: the next good line must be exported, not swallowed.
	f.tokens, f.lastRefill = 1, time.Now()
	tl.ingestChunk(ctx, f, []byte(timeNowCRI()+" stdout F survivor\n"), false)

	if emitted(tl, "suffix-of-oversized-line") {
		t.Fatalf("mid-line suffix exported as a record: %v", bodies(tl))
	}
	if !emitted(tl, "survivor") {
		t.Fatalf("the line after the oversized one was swallowed: %v", bodies(tl))
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

// A never-F-closed P-run is flushed line by line, and hasRun is spent by the
// first of those emissions — the rest must resolve their start from the
// position captured at FEED time (the stage payload). Deriving it from the
// CURRENT tail id at flush time stamped fragments carried across a rename
// rotation into the NEW segment: the watermark then sat in a segment newer
// than the old segment's commit candidates, flush's clamp read them as
// unconstrained, and the segment retired with its lines still buffered —
// lost on the next rewind or crash, with no counter moving.
func TestFlushedPRunKeepsFeedTimeSegment(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	f := &file{
		path:        filepath.Join(dir, logName),
		source:      &compiledSource{name: "containers", containerd: true, multiline: true},
		containerID: "0123456789abcdef",
		resolved:    true,
		resource:    pcommon.NewResource(),
	}
	tl.newPipeline(f)
	tl.files[f.path] = f

	ctx := context.Background()
	off := int64(0)
	for _, l := range []string{
		timeNowCRI() + " stdout P frag-a",
		timeNowCRI() + " stdout P frag-b",
	} {
		end := off + int64(len(l)) + 1
		tl.feedLine(ctx, f, l, off, end)
		off = end
	}
	oldSeg := f.curSeg()

	// A rename rotation with the run still buffered: the pipeline is carried,
	// the old tail closes into a segment, and a fresh tail id is issued.
	tl.reopen(ctx, f, true, true)
	if len(f.segments) != 1 || f.segments[0].id != oldSeg {
		t.Fatalf("segments = %+v, want the old tail recorded as %d", f.segments, oldSeg)
	}

	// Flush the carried run: every emitted fragment's positions must stay in
	// the OLD segment, where its bytes were fed.
	if err := f.criStage.FlushBefore(ctx, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if len(tl.batch) == 0 {
		t.Fatal("the flushed run emitted nothing")
	}
	for _, e := range tl.batch {
		if e.start.seg != oldSeg || e.end.seg != oldSeg {
			t.Fatalf("entry %q spans segments [%d,%d], want [%d,%d]: a flush-time tail id leaked into a carried fragment",
				e.body, e.start.seg, e.end.seg, oldSeg, oldSeg)
		}
	}
}

// With Multiline OFF the CRI stage still buffers P-runs, so the age-out
// clocks must be stamped for it too. Unstamped, sweep fell back to the
// wall-clock cutoff, and during any backlog catch-up (lag beyond
// MultilineTimeout) a P-run buffered across a MaxBytesPerSweep boundary was
// torn into per-fragment records.
func TestBackloggedPRunSurvivesSweepBoundaryWithMultilineOff(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := New(Config{
		Dir:              dir,
		PollInterval:     20 * time.Millisecond,
		FlushInterval:    time.Millisecond,
		BatchSize:        1 << 20,
		MultilineTimeout: 300 * time.Millisecond,
		MetadataWait:     time.Second,
		Metadata:         fakeMeta{},
		Exporter:         exp,
	})
	tl.retryBackoff = time.Millisecond
	tl.scanDir(tl.loadCheckpoints(), true)

	// A backlog: the lines' own timestamps are far behind the wall clock.
	l1 := "2026-07-05T10:00:00Z stdout P frag-a"
	l2 := "2026-07-05T10:00:01Z stdout F frag-b"
	writeLog(t, dir, l1, l2)
	tl.scanDir(nil, false)

	tl.cfg.MaxBytesPerSweep = len(l1) + 1
	tl.sweep(ctx, true) // reads only the P fragment; this sweep's age-out must not tear its run
	tl.cfg.MaxBytesPerSweep = 1 << 20
	tl.sweep(ctx, true) // the F line closes and joins the run

	// Idle past the timeout: the wall-clock fallback flushes the closed run.
	time.Sleep(350 * time.Millisecond)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	got := exp.get()
	if slices.Contains(got, "frag-a") {
		t.Fatalf("backlogged P-run torn at the sweep boundary: %q", got)
	}
	if !slices.Contains(got, "frag-afrag-b") {
		t.Fatalf("joined record missing: %q", got)
	}
}

// A capped trace ending on non-accepting continuation lines once left their
// FIFO items orphaned: the caps dropped the lines from retention with no
// emission to pop them, and the watermark reported the file buffered forever
// — a gone file never settled (drainGone re-ran every sweep, with the fd, the
// files-map entry and the checkpoint pinned for the process life). multiline
// >= v0.0.11 charges cap-dropped lines to the last emitted entry's Lines
// (sum(Lines) equals the lines consumed, pinned upstream by
// FuzzCappedConservation), so every pushed FIFO item is popped exactly and
// the file settles — the property this test pins now that the orphan
// reconciliation machinery is deleted.
func TestGoneFileSettlesDespiteCapDroppedTrailingLines(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveMultilineTailer(dir, exp) // MaxEntryBytes: 64
	tl.scanDir(tl.loadCheckpoints(), true)
	path := filepath.Join(dir, logName)

	// A rust panic over the 64-byte cap whose last line lands in a
	// NON-ACCEPTING state ("stack backtrace:" with no frames after it): the
	// line is consumed and dropped by the byte cap, but still charged to the
	// group's eventual entry.
	ts := timeNowCRI()
	writeLog(t, dir,
		ts+" stderr F thread 'main' panicked at src/main.rs:5:5:",
		ts+" stderr F index out of bounds: the len is 1 but the index is 99",
		ts+" stderr F stack backtrace:",
	)
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false) // the listing proves the file gone
	driveUntil(t, ctx, tl, func() bool {
		_, tracked := tl.files[path]
		return !tracked
	}, "gone file settled and released")
}

// A never-completing group — an exception header followed by endless
// frame-shaped continuation lines with no quiet gap — must not grow the
// offset FIFO without bound while pinning `committed` at the group's start:
// past maxGroupBuffered consumed lines boundGroup force-flushes the (already
// truncated) group so memory stays bounded and the checkpoint advances.
func TestNeverCompletingGroupIsBounded(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveMultilineTailer(dir, exp) // MaxEntryBytes: 64
	tl.scanDir(tl.loadCheckpoints(), true)

	ts := timeNowCRI()
	lines := make([]string, 0, maxGroupBuffered+6)
	lines = append(lines, ts+` stdout F Exception in thread "main" java.lang.RuntimeException: boom`)
	for range maxGroupBuffered + 5 {
		lines = append(lines, ts+" stdout F \tat com.example.Main.main(Main.java:1)")
	}
	writeLog(t, dir, lines...)
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	f := tl.files[filepath.Join(dir, logName)]
	if n := len(f.stStdout.live()); n >= maxGroupBuffered {
		t.Fatalf("offset FIFO unbounded under a never-completing group: %d live items", n)
	}
	if len(exp.get()) == 0 {
		t.Fatal("no truncated entry emitted for the bounded group")
	}
	if f.committed == 0 {
		t.Fatal("committed still pinned at the group start after the bound flush")
	}
}

// The STAGE-1 sibling of the test above: a run of P fragments whose F never
// arrives (a workload writing one endless logical line). Nothing reaches the
// stage-2 FIFO, so boundGroup cannot see it, and every fragment refreshes the
// age-out stamps, so FlushBefore never fires — the watermark then pinned at
// runStart forever: checkpoint frozen (a crash re-ingests the whole window),
// idle-close blocked, one unretirable segment per carried rotation. Past
// double MaxEntryBytes of consumed run bytes the crossing fragment is
// rewritten as the F line that closes the run, and the checkpoint advances.
func TestEndlessFragmentRunDoesNotWedgeTheCheckpoint(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.cfg.MaxEntryBytes = 256 // force-close a run past 512 consumed bytes

	tl.scanDir(tl.loadCheckpoints(), true)
	ts := timeNowCRI()
	frag := strings.Repeat("x", 40)
	lines := make([]string, 0, 64)
	for range 64 { // ~5 KiB of fragments, no F anywhere
		lines = append(lines, ts+" stdout P "+frag)
	}
	writeLog(t, dir, lines...)
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	f := tl.files[filepath.Join(dir, logName)]
	if f == nil {
		t.Fatal("file not tracked")
	}
	if f.committed == 0 {
		t.Fatal("committed still pinned at the run start: the endless P-run wedged the checkpoint")
	}
	if len(exp.get()) == 0 {
		t.Fatal("no entry emitted from the force-closed run")
	}
}
