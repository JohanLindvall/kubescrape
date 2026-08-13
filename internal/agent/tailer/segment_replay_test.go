package tailer

// Regression tests for incomplete-segment replay after a restart.

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// feedSegments must not range over the LIVE f.segments slice: replaySegment
// retires the segment it is replaying when the source is unrecoverable, and
// retire compacts the slice with slices.DeleteFunc, which nils the vacated tail
// of the backing array. The loop then read a nil *segment and panicked on
// sg.id — in the tailer's single sweep goroutine, taking log collection down
// for the whole node. A second, still-recoverable segment must also survive the
// first one's retirement and be replayed.
func TestFeedSegmentsSurvivesRetireDuringReplay(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	pos := mustOpenPositions(t, filepath.Join(t.TempDir(), "pos.json"))
	path := filepath.Join(dir, logName)
	writeLog(t, dir, timeNowCRI()+" stdout F live")
	ino := inodeOfPath(t, path)

	// A rotated-away file that still exists and must be replayed.
	rot := path + ".1"
	writeLines(t, rot, timeNowCRI()+" stdout F rotated-line")
	rotIno := inodeOfPath(t, rot)
	rst, err := os.Stat(rot)
	if err != nil {
		t.Fatal(err)
	}

	// Pending[0] names an inode nothing on disk has (the runtime pruned that
	// rotation) so it is retired mid-loop; Pending[1] is the recoverable one.
	if err := pos.SetLogs(map[string]positions.LogPos{path: {
		Offset: 0, Inode: ino,
		Pending: []positions.Prefix{
			{Inode: rotIno + 999999, From: 0, To: 50},
			{Inode: rotIno, From: 0, To: rst.Size()},
		},
	}}); err != nil {
		t.Fatal(err)
	}

	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Positions = pos
	tl.scanDir(tl.loadCheckpoints(), true)

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic in sweep/feedSegments: %v", r)
			}
		}()
		tl.sweep(ctx, true)
	}()
	tl.flush(ctx)

	if got := exp.get(); !slices.Contains(got, "rotated-line") {
		t.Fatalf("the still-recoverable segment was skipped after an earlier one retired: exported %v", got)
	}
}

// A rotation that happened while the agent was DOWN is recovered from the
// checkpointed identity. This must not be conditional on a NON-ZERO committed
// offset: a checkpoint is persisted as soon as a file is discovered, so an
// export failing since startup leaves identity known and offset 0 — the case
// where the ENTIRE rotated inode is unshipped, i.e. maximum loss, not a no-op.
func TestRotationWhileDownRecoveredAtZeroOffset(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	pos := mustOpenPositions(t, filepath.Join(t.TempDir(), "pos.json"))
	path := filepath.Join(dir, logName)

	// The previous incarnation: read but never committed (export was failing).
	writeLog(t, dir, timeNowCRI()+" stdout F unshipped-old")
	oldIno := inodeOfPath(t, path)
	fh, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := fh.Stat()
	fp, err := computeFingerprint(fh, min(int64(1024), st.Size()))
	if err != nil {
		t.Fatal(err)
	}
	_ = fh.Close()
	if err := pos.SetLogs(map[string]positions.LogPos{path: {
		Offset: 0, Inode: oldIno,
		FingerprintLen: fp.Len, FingerprintHash: fp.Hash,
	}}); err != nil {
		t.Fatal(err)
	}

	// Agent is down: rename rotation, then a fresh log at the same path.
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, timeNowCRI()+" stdout F new-incarnation")

	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Positions = pos
	tl.scanDir(tl.loadCheckpoints(), true)
	for range 5 {
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}

	got := exp.get()
	if !slices.Contains(got, "new-incarnation") {
		t.Fatalf("new inode not read: %v", got)
	}
	if !slices.Contains(got, "unshipped-old") {
		t.Fatalf("rotated-while-down inode's uncommitted content lost: exported %v", got)
	}
}

// A replay stopped by the per-sweep byte budget leaves the segment's
// remainder owed: the tail must not be read until the replay finishes, and a
// resumed pass must continue from the FEED frontier, not the commit frontier.
// Reading the tail early fed newer lines into the same pipeline key — an F
// line closed the old segment's still-open P-run and the joiner fused
// fragments across the gap into a record no file ever contained — and a
// committed-offset resume re-fed the buffered fragment into its own run,
// duplicating it inside one joined record.
func TestBudgetCutReplayDefersTailRead(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	pos := mustOpenPositions(t, filepath.Join(t.TempDir(), "pos.json"))
	path := filepath.Join(dir, logName)

	l1 := timeNowCRI() + " stdout F old-one"
	l2 := timeNowCRI() + " stdout P frag-a"
	l3 := timeNowCRI() + " stdout F frag-b"
	rot := path + ".1"
	writeLines(t, rot, l1, l2, l3)
	rotIno := inodeOfPath(t, rot)
	rst, err := os.Stat(rot)
	if err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, timeNowCRI()+" stdout F tail-one")
	if err := pos.SetLogs(map[string]positions.LogPos{path: {
		Offset: 0, Inode: inodeOfPath(t, path),
		Pending: []positions.Prefix{{Inode: rotIno, From: 0, To: rst.Size()}},
	}}); err != nil {
		t.Fatal(err)
	}

	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Positions = pos
	// The budget lands exactly on the P fragment's line boundary: the replay
	// pass ends mid-segment with the fragment's run open in the pipeline.
	tl.cfg.MaxBytesPerSweep = len(l1) + len(l2) + 2
	tl.scanDir(tl.loadCheckpoints(), true)

	driveUntil(t, ctx, tl, func() bool {
		all := strings.Join(exp.get(), "\x00")
		return strings.Contains(all, "old-one") && strings.Contains(all, "frag-b") &&
			strings.Contains(all, "tail-one")
	}, "segment and tail content delivered")

	for _, r := range exp.get() {
		if strings.Contains(r, "frag-a") && strings.Contains(r, "tail-one") {
			t.Fatalf("old-segment fragment fused with the tail line: %q", r)
		}
		if strings.Contains(r, "frag-afrag-a") {
			t.Fatalf("budget-resumed replay re-fed a buffered fragment: %q", r)
		}
	}
	if got := exp.get(); !slices.Contains(got, "tail-one") {
		t.Fatalf("tail line not delivered standalone: %q", got)
	}
}

// A checkpointed segment containing an oversized line whose newline is out of
// one pass's reach (longer than the pass budget plus the discard escape's cap)
// must still make progress. The discard progress used to be pass-local
// (`cur`/`carry`/`discarding` locals) while the resume point was
// max(committed, fedTo), which only FED lines advance: every pass re-read the
// same prefix, discarded it, and returned with the frontier unmoved — the
// segment sat pinned at one offset until the stall limit retired it
// (obs.LogPrefixLost) and the readable remainder (the trailing line here) was
// lost. skipTo/discarding now persist the frontier on the segment, so each
// pass advances past what it discarded and the segment finishes and retires
// normally.
func TestReplayOversizedLineDoesNotWedgeSegment(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	pos := mustOpenPositions(t, filepath.Join(t.TempDir(), "pos.json"))
	path := filepath.Join(dir, logName)

	l1 := timeNowCRI() + " stdout F seg-head"
	huge := timeNowCRI() + " stdout F " + strings.Repeat("x", 200_000)
	l3 := timeNowCRI() + " stdout F seg-tail"
	rot := path + ".1"
	writeLines(t, rot, l1, huge, l3)
	rotIno := inodeOfPath(t, rot)
	rst, err := os.Stat(rot)
	if err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, timeNowCRI()+" stdout F live-tail")
	if err := pos.SetLogs(map[string]positions.LogPos{path: {
		Offset: 0, Inode: inodeOfPath(t, path),
		Pending: []positions.Prefix{{Inode: rotIno, From: 0, To: rst.Size()}},
	}}); err != nil {
		t.Fatal(err)
	}

	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Positions = pos
	// Small caps: the discard escape fires at MaxEntryBytes+oversizeSlack
	// (4160 here) and the budget is far below the oversized line, so no
	// single pass can reach its newline — the shape that used to wedge.
	tl.cfg.MaxEntryBytes = 64
	tl.cfg.MaxBytesPerSweep = 512
	tl.scanDir(tl.loadCheckpoints(), true)
	f := tl.files[path]
	if f == nil {
		t.Fatal("file not tracked")
	}

	driveUntil(t, ctx, tl, func() bool {
		all := strings.Join(exp.get(), "\x00")
		return strings.Contains(all, "seg-tail") && strings.Contains(all, "live-tail") &&
			len(f.segments) == 0
	}, "oversized-line segment replayed past the discard, remainder delivered, segment retired")
	for _, r := range exp.get() {
		if strings.Contains(r, "xxxx") {
			t.Fatalf("a fragment of the discarded oversized line was exported: %.80q", r)
		}
	}
}

// A file that VANISHES while a rotated segment's replay is unfinished must not
// have its last bytes drained yet: the gone inode's lines are NEWER than the
// segment's still-owed remainder, and feeding them into the same pipeline keys
// makes the joiner fuse fragments across the gap into a record that existed in
// no file. drainGone waits — its own feedSegments keeps the replay advancing
// every sweep, and the drain takes its turn once the segments are done. Same
// rule as readFile's gate on the live tail, one path over.
func TestGoneDrainWaitsForAnUnfinishedSegmentReplay(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	pos := mustOpenPositions(t, filepath.Join(t.TempDir(), "pos.json"))
	path := filepath.Join(dir, logName)

	// Equal-length lines with a near-now, FIXED-WIDTH timestamp. Both halves
	// are load-bearing. Fixed width because timeNowCRI() is RFC3339Nano, which
	// trims trailing zeros: a per-line budget derived from one line then
	// overshoots a shorter next line, and replaySegment's overrun rule ("spend
	// the budget mid-line, keep reading until that line progresses") pulls the
	// FOLLOWING line in too — which finished this whole replay inside the
	// first drainGone and left the guard below unexercised. Near-now because
	// the fragment run has to survive from one sweep to the next, and the
	// CRI stage ages groups out against the line's own clock.
	ts := time.Now().UTC().Format("2006-01-02T15:04:05.000000000Z")
	l1 := ts + " stdout F old-line"
	l2 := ts + " stdout P frag-one"
	l3 := ts + " stdout F frag-two"
	if len(l1) != len(l2) || len(l2) != len(l3) {
		t.Fatalf("setup: the segment's lines must be equal length for a one-line-per-pass budget (%d/%d/%d)",
			len(l1), len(l2), len(l3))
	}
	rot := path + ".1"
	writeLines(t, rot, l1, l2, l3)
	rotIno := inodeOfPath(t, rot)
	rst, err := os.Stat(rot)
	if err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, ts+" stdout F tail-one")
	if err := pos.SetLogs(map[string]positions.LogPos{path: {
		Offset: 0, Inode: inodeOfPath(t, path),
		Pending: []positions.Prefix{{Inode: rotIno, From: 0, To: rst.Size()}},
	}}); err != nil {
		t.Fatal(err)
	}

	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Positions = pos
	// Exactly one replayed line per pass, landing on a line boundary so the
	// overrun rule never fires: pass 1 takes old-line, pass 2 opens the P
	// fragment run, pass 3 closes it.
	tl.cfg.MaxBytesPerSweep = len(l1) + 1
	// The fragment run has to stay open across three sweeps; the default 1s
	// age-out is close enough to the wall time of a loaded machine to tear it,
	// which would show up as a flake rather than as the property failing.
	tl.cfg.MultilineTimeout = time.Minute
	tl.scanDir(tl.loadCheckpoints(), true)

	tl.sweep(ctx, true) // pass 1: old-line; the tail is not read (replay gate)
	tl.flush(ctx)
	f := tl.files[path]
	if f == nil {
		t.Fatal("setup: file not tracked")
	}
	if len(f.segments) == 0 || f.segmentsFed {
		t.Fatalf("setup: want an unfinished replay (segments=%d fed=%v)", len(f.segments), f.segmentsFed)
	}

	// The live file is deleted with its single line still unread: from here
	// its only copy is behind our fd, and the gone drain owns it.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false)

	// The sweep that reaches drainGone with the replay STILL unfinished: its
	// feedSegments opens the fragment run, and the drain must wait.
	tl.sweep(ctx, true)
	tl.flush(ctx)
	if f.segmentsFed {
		t.Fatalf("setup: the replay finished inside the first gone drain, so the guard never faced an "+
			"unfinished one (budget=%d, line=%d bytes, segment=%d bytes)",
			tl.cfg.MaxBytesPerSweep, len(l1)+1, rst.Size())
	}
	for _, r := range exp.get() {
		if strings.Contains(r, "tail-one") {
			t.Fatalf("the gone inode's line was drained while the segment still owed %q: exported %q "+
				"— fused into the open fragment run, a record that existed in no file (all: %v)",
				l3, r, exp.get())
		}
	}

	driveUntil(t, ctx, tl, func() bool { _, tracked := tl.files[path]; return !tracked },
		"the gone file draining once its segment replay finished")

	got := exp.get()
	for _, r := range got {
		if strings.Contains(r, "frag-one") && strings.Contains(r, "tail-one") {
			t.Fatalf("the gone inode's line was drained into the segment's open fragment run: %q "+
				"— a record that existed in no file (exported %v)", r, got)
		}
	}
	for _, want := range []string{"old-line", "frag-onefrag-two", "tail-one"} {
		if !slices.Contains(got, want) {
			t.Fatalf("%q not delivered: %v", want, got)
		}
	}
}

// A replay pass whose pipeline a REWIND purged under it made no progress by
// construction (the purge resets the segment's fed frontier) — but the thing
// that is stuck is the collector, not the segment's source, and the stall
// budget exists only for a source that cannot be read. Charged anyway, a
// collector outage spanning two passes retires a perfectly readable segment,
// counting obs.LogPrefixLost for lines that were about to ship: the one case
// that must NOT spend the budget.
func TestFailedExportsDoNotSpendTheSegmentStallBudget(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	pos := mustOpenPositions(t, filepath.Join(t.TempDir(), "pos.json"))
	path := filepath.Join(dir, logName)

	rot := path + ".1"
	writeLines(t, rot,
		timeNowCRI()+" stdout F seg-one",
		timeNowCRI()+" stdout F seg-two")
	rotIno := inodeOfPath(t, rot)
	rst, err := os.Stat(rot)
	if err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, timeNowCRI()+" stdout F tail-one")
	if err := pos.SetLogs(map[string]positions.LogPos{path: {
		Offset: 0, Inode: inodeOfPath(t, path),
		Pending: []positions.Prefix{{Inode: rotIno, From: 0, To: rst.Size()}},
	}}); err != nil {
		t.Fatal(err)
	}

	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Positions = pos
	tl.cfg.BatchSize = 1                         // flush (and so rewind) inside the replay pass
	tl.segmentStallLimit = 10 * time.Millisecond // any two charged passes would give up
	tl.scanDir(tl.loadCheckpoints(), true)

	// The collector is down for several passes; each one is purged by the
	// rewind its own flush triggers.
	prefixBefore := obs.LogPrefixLost.Value()
	exp.mu.Lock()
	exp.fail = 1 << 20
	exp.mu.Unlock()
	for range 4 {
		tl.sweep(ctx, true)
		tl.flush(ctx)
		time.Sleep(5 * time.Millisecond)
	}
	if got := obs.LogPrefixLost.Value(); got != prefixBefore {
		t.Fatalf("LogPrefixLost = %v, want %v: the segment was given up on because failed exports kept "+
			"re-owing its range — its source was readable the whole time", got, prefixBefore)
	}

	exp.mu.Lock()
	exp.fail = 0
	exp.mu.Unlock()
	driveUntil(t, ctx, tl, func() bool {
		got := exp.get()
		return slices.Contains(got, "seg-one") && slices.Contains(got, "seg-two") &&
			slices.Contains(got, "tail-one")
	}, "the segment and the tail delivered once the collector recovered")
	if got := obs.LogPrefixLost.Value(); got != prefixBefore {
		t.Fatalf("LogPrefixLost = %v, want %v: nothing was lost", got, prefixBefore)
	}
}
