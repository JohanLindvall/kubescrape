package tailer

// Edge-case coverage from the systematic gap analysis: metadata-resolution
// failure/backoff, oversized unterminated lines, dead-segment commit guards,
// log.truncated, unresolved-gone files, inode-only identity, source claiming,
// defaulting, shrunk-checkpoint restarts, corrupt checkpoints, and
// consumed-archive fd release.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
)

// flakyMeta fails the first `fails` Container calls, then delegates to
// fakeMeta. calls counts every invocation (backoff assertions).
type flakyMeta struct {
	fails    int
	calls    int
	notFound bool
}

func (m *flakyMeta) Container(ctx context.Context, id string, wait time.Duration) (*kubemeta.ContainerMetadata, error) {
	m.calls++
	if m.calls <= m.fails {
		if m.notFound {
			return nil, &metaclient.StatusError{Code: 404}
		}
		return nil, errors.New("metadata service unreachable")
	}
	return fakeMeta{}.Container(ctx, id, wait)
}

// Nothing is read until it can be attributed: a file whose metadata lookup
// fails is skipped (data waits on disk), retries are gated by a backoff, and
// once resolution succeeds everything exports from offset zero.
func TestMetadataFailureBackoffThenRecovery(t *testing.T) {
	for _, notFound := range []bool{false, true} {
		name := "generic-error"
		if notFound {
			name = "not-found"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			ctx := context.Background()
			exp := &fakeExporter{}
			meta := &flakyMeta{fails: 1, notFound: notFound}
			tl := driveTailer(dir, exp)
			tl.cfg.Metadata = meta

			tl.scanDir(tl.loadCheckpoints(), true)
			writeLog(t, dir, "2026-07-05T10:00:00Z stdout F precious")
			tl.scanDir(nil, false)

			tl.sweep(ctx, true) // resolution fails
			tl.flush(ctx)
			f := tl.files[filepath.Join(dir, logName)]
			if f == nil {
				t.Fatal("file not tracked")
			}
			if f.readPos != 0 || len(exp.get()) != 0 {
				t.Fatalf("unresolved file was read: readPos=%d exports=%v", f.readPos, exp.get())
			}
			if meta.calls != 1 {
				t.Fatalf("calls = %d, want 1", meta.calls)
			}

			// Inside the backoff window: no further metadata call.
			tl.sweep(ctx, true)
			if meta.calls != 1 {
				t.Fatalf("backoff not honored: calls = %d, want 1", meta.calls)
			}

			// Backoff elapses; resolution succeeds; the waiting data exports
			// from offset 0.
			f.nextMetaTry = time.Time{}
			tl.sweep(ctx, true)
			tl.flush(ctx)
			if got := exp.get(); !slices.Equal(got, []string{"precious"}) {
				t.Fatalf("exports after recovery = %v, want [precious]", got)
			}
		})
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

// A candidate naming a DEAD segment id (a truncated-away incarnation) must
// resolve to nothing: neither the tail checkpoint nor any live segment may
// move. The segment-qualified position IS the staleness check that the old
// rotation-generation protocol provided.
func TestDeadSegmentCandidateCommitsNothing(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)

	f := &file{path: filepath.Join(dir, logName), committed: 7,
		source: &compiledSource{name: "containers", containerd: true}}
	f.readPos = 42
	tl.newPipeline(f) // issues tail id 1
	deadSeg := f.tail
	f.newTail() // the old incarnation's id is now dead

	inf := &batchInfo{
		cands: map[*file]map[int]int64{f: {deadSeg: 100}},
		highs: map[*file]pos{f: {seg: deadSeg, off: 100}},
	}
	tl.commitBatch(inf)
	if f.committed != 7 {
		t.Fatalf("dead-segment candidate applied to the tail: committed = %d, want 7", f.committed)
	}
	if len(f.segments) != 0 {
		t.Fatalf("dead-segment candidate materialized a segment: %v", f.segments)
	}

	// The old gen-checked pipelined model SKIPPED the rewind of a stale-gen
	// file at apply time (a rotation might have reset its offsets in between).
	// Synchronous flush removed that interleaving, so failBatch now rewinds
	// EVERY batched file unconditionally — including one whose only candidate
	// names a dead segment: rewind is idempotent (readPos back to committed)
	// and cannot corrupt the already-restarted offsets.
	f.readPos = 99 // pretend read-ahead past committed
	tl.failBatch(inf, errors.New("boom"))
	if f.readPos != f.committed {
		t.Fatalf("failBatch did not rewind the dead-segment file: readPos=%d committed=%d", f.readPos, f.committed)
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

// A discovered file whose metadata never resolves and which is then deleted
// must settle out of the tracking map without export, wedge, or panic.
func TestUnresolvedGoneFileSettles(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Metadata = &flakyMeta{fails: 1 << 30}
	lostBefore := obs.LogUnresolvedLost.Value()

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F never-attributed")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true) // unresolved
	path := filepath.Join(dir, logName)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false) // marks gone
	tl.sweep(ctx, true)    // drains (no-op: unresolved) and releases
	if _, tracked := tl.files[path]; tracked {
		t.Fatal("unresolved gone file still tracked")
	}
	if got := exp.get(); len(got) != 0 {
		t.Fatalf("unresolved file exported: %v", got)
	}
	if got := obs.LogUnresolvedLost.Value(); got != lostBefore+1 {
		t.Fatalf("LogUnresolvedLost = %v, want %v (the loss must be visible)", got, lostBefore+1)
	}
}

// FingerprintBytes < 0 disables content fingerprints: identity is the inode
// alone. Resume must still work, and a rename rotation must still be detected.
func TestNegativeFingerprintBytesInodeOnlyIdentity(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	ckpt := filepath.Join(dir, "ckpt.json")
	tl := driveTailer(dir, exp)
	tl.cfg.CheckpointFile = ckpt
	tl.cfg.FingerprintBytes = -1

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F one")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)
	tl.saveCheckpoints()

	// Restart on the same inode: resume, not re-read.
	exp2 := &fakeExporter{}
	tl2 := driveTailer(dir, exp2)
	tl2.cfg.CheckpointFile = ckpt
	tl2.cfg.FingerprintBytes = -1
	tl2.scanDir(tl2.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F two")
	tl2.scanDir(nil, false)
	tl2.sweep(ctx, true)
	tl2.flush(ctx)
	if got := exp2.get(); !slices.Equal(got, []string{"two"}) {
		t.Fatalf("inode-only resume exports = %v, want [two]", got)
	}

	// Rename rotation: new inode at the path is detected and read from zero.
	rotateAway(t, dir, 1)
	writeLog(t, dir, "2026-07-05T10:00:02Z stdout F three")
	for i := 0; i < 3 && !slices.Contains(exp2.get(), "three"); i++ {
		tl2.sweep(ctx, true)
		tl2.flush(ctx)
	}
	if got := exp2.get(); !slices.Contains(got, "three") {
		t.Fatalf("rotation missed under inode-only identity: %v", got)
	}
}

// A file matched by two sources is claimed by the FIRST (config order), and
// keeps that source's attributes.
func TestFirstMatchingSourceClaimsFile(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{
		{Name: "first", Include: []string{filepath.Join(dir, "*.log")}, Attributes: map[string]string{"who": "first"}},
		{Name: "second", Include: []string{filepath.Join(dir, "*.log")}, Attributes: map[string]string{"who": "second"}},
	}, false)

	tl.scanDir(tl.loadCheckpoints(), true)
	path := filepath.Join(dir, "app.log")
	writeLines(t, path, "hello")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	f := tl.files[path]
	if f == nil || f.source.name != "first" {
		t.Fatalf("file claimed by %q, want first", f.source.name)
	}
	if len(tl.files) != 1 {
		t.Fatalf("tracked files = %d, want 1", len(tl.files))
	}
	if !slices.Contains(exp.get(), "hello") {
		t.Fatal("record not exported")
	}
	exp.mu.Lock()
	who, okAttr := exp.resAttrs["who"]
	exp.mu.Unlock()
	if !okAttr || who != "first" {
		t.Fatalf("resource who = %v, want first", who)
	}
}

// New() defaulting: batch size, rate burst 2x limit.
func TestNewConfigDefaults(t *testing.T) {
	tl := New(Config{Dir: "/tmp", RateLimit: 5, Metadata: fakeMeta{}, Exporter: nullExporter{}})
	if tl.cfg.RateBurst != 10 {
		t.Errorf("RateBurst = %v, want 10 (2x RateLimit)", tl.cfg.RateBurst)
	}
	if tl.cfg.BatchSize <= 0 {
		t.Errorf("BatchSize = %d, want a positive default", tl.cfg.BatchSize)
	}
	if tl.cfg.MaxEntryBytes <= 0 || tl.cfg.MaxBytesPerSweep <= 0 {
		t.Errorf("size defaults missing: MaxEntryBytes=%d MaxBytesPerSweep=%d", tl.cfg.MaxEntryBytes, tl.cfg.MaxBytesPerSweep)
	}
}

// A checkpoint whose offset lies beyond the current file size — but whose head
// fingerprint still matches (the file shrank while the agent was down, head
// intact) — must restart at zero, not Seek past EOF and read nothing forever.
func TestCheckpointBeyondSizeWithMatchingHeadRestarts(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	ckpt := filepath.Join(dir, "ckpt.json")
	path := filepath.Join(dir, logName)

	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.CheckpointFile = ckpt
	tl.cfg.FingerprintBytes = 8 // head-only fingerprint survives the shrink

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir,
		"2026-07-05T10:00:00Z stdout F aaaa",
		"2026-07-05T10:00:01Z stdout F bbbb",
	)
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)
	tl.saveCheckpoints()

	// Shrink below the committed offset, head bytes preserved.
	if err := os.Truncate(path, 16); err != nil {
		t.Fatal(err)
	}

	exp2 := &fakeExporter{}
	tl2 := driveTailer(dir, exp2)
	tl2.cfg.CheckpointFile = ckpt
	tl2.cfg.FingerprintBytes = 8
	tl2.scanDir(tl2.loadCheckpoints(), true)
	tl2.sweep(ctx, true)
	tl2.flush(ctx)
	f := tl2.files[path]
	if f == nil {
		t.Fatal("file not tracked after restart")
	}
	if f.readPos > 16 {
		t.Fatalf("readPos = %d beyond file size 16: Seek past EOF", f.readPos)
	}
}

// A corrupt standalone checkpoint file must warn-and-continue (files treated
// as checkpoint-less), and the next save must overwrite it.
func TestCorruptCheckpointFileIgnored(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	ckpt := filepath.Join(dir, "ckpt.json")
	if err := os.WriteFile(ckpt, []byte("{garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.CheckpointFile = ckpt

	saved := tl.loadCheckpoints()
	if len(saved) != 0 {
		t.Fatalf("corrupt checkpoint yielded entries: %v", saved)
	}
	tl.scanDir(saved, true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F after")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)
	tl.saveCheckpoints()
	data, err := os.ReadFile(ckpt)
	if err != nil || strings.HasPrefix(string(data), "{garbage") {
		t.Fatalf("checkpoint not overwritten: %v %q", err, data)
	}
}

// A fully-consumed archive's fd is released at EOF (committed >= readPos), and
// idle sweeps do not reopen it.
func TestConsumedArchiveReleasesFd(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newArchiveTailer(dir, exp)
	path := filepath.Join(dir, "app.log.gz")

	tl.scanDir(tl.loadCheckpoints(), true)
	writeGzip(t, path, "only-line")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)
	tl.sweep(ctx, true) // EOF settling sweep

	f := tl.files[path]
	if f == nil {
		t.Fatal("archive not tracked")
	}
	if !slices.Contains(exp.get(), "only-line") {
		t.Fatalf("archive line missing: %v", exp.get())
	}
	if !f.archiveDone {
		t.Fatal("archive not marked done")
	}
	if f.f != nil || f.gz != nil {
		t.Fatalf("consumed archive retains fd (f=%v gz=%v)", f.f != nil, f.gz != nil)
	}
	// Idle sweeps stay no-ops (no reopen).
	before := f.readPos
	tl.sweep(ctx, true)
	if f.readPos != before || f.f != nil {
		t.Fatal("idle sweep reopened a consumed archive")
	}
}

// The carried-prefix restart fallback: the checkpoint names a rotated inode
// that no longer exists anywhere. The loss must be COUNTED (obs.LogPrefixLost)
// and the new inode's lines must still ship — no wedge on the missing prefix.
func TestMissingRotatedPrefixCountedAndSkipped(t *testing.T) {
	dir := t.TempDir()

	start, rest := panicLines()
	oldPath := filepath.Join(dir, "0.log.rotated")
	writeLines(t, oldPath, start...)
	oldSt, err := os.Stat(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	oldFh, err := os.Open(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	oldFp, err := computeFingerprint(oldFh, min(1024, oldSt.Size()))
	_ = oldFh.Close()
	if err != nil {
		t.Fatal(err)
	}
	writeLines(t, filepath.Join(dir, logName), rest...)

	posPath := filepath.Join(dir, "positions.json")
	seed, _ := positions.Open(posPath)
	if err := seed.SetLogs(map[string]positions.LogPos{
		filepath.Join(dir, logName): {
			Offset: 0, Inode: 0,
			Pending: []positions.Prefix{{
				Inode: inodeOfPath(t, oldPath), FingerprintLen: oldFp.Len, FingerprintHash: oldFp.Hash,
				From: 0, To: oldSt.Size(),
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	// The rotated file is pruned before the restart: unrecoverable.
	if err := os.Remove(oldPath); err != nil {
		t.Fatal(err)
	}

	lostBefore := obs.LogPrefixLost.Value()
	exp := &fakeExporter{}
	tl := newMultilineTailer(dir, "", exp, mustOpenPositions(t, posPath))
	stop := startTailer(t, tl)
	defer stop()

	waitFor(t, func() bool { return len(exp.get()) > 0 }, "new inode's lines despite the lost prefix")
	if got := obs.LogPrefixLost.Value(); got != lostBefore+1 {
		t.Fatalf("LogPrefixLost = %v, want %v (the pruned prefix must be counted)", got, lostBefore+1)
	}
}
