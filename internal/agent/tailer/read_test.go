// Tests for reading and metadata resolution (read.go): readFile truncation
// decisions, copytruncate guards and resolve backoff.
package tailer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
)

func TestAttrFilter(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	filter, err := attrs.NewFilterFromLists(nil, []string{`k8s\.pod\.label\..*`})
	if err != nil {
		t.Fatal(err)
	}
	builder, err := attrs.NewBuilder(&attrs.Config{Static: map[string]string{"cluster": "test"}}, filter)
	if err != nil {
		t.Fatal(err)
	}
	tl.cfg.Attrs = builder
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F hi")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "record")

	exp.mu.Lock()
	defer exp.mu.Unlock()
	if _, ok := exp.resAttrs["k8s.pod.label.app"]; ok {
		t.Fatalf("filtered attribute exported: %v", exp.resAttrs)
	}
	if exp.resAttrs["k8s.pod.name"] != "pod1" {
		t.Fatalf("kept attributes damaged: %v", exp.resAttrs)
	}
	if exp.resAttrs["cluster"] != "test" {
		t.Fatalf("static attribute missing: %v", exp.resAttrs)
	}
}

func TestTruncationRestartsAtZero(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F one")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "pre-truncation record")

	// In-place truncation (copytruncate-style) with shorter new content.
	path := filepath.Join(dir, logName)
	if err := os.Truncate(path, 0); err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F two")
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "post-truncation record")
	if got := exp.get(); got[1] != "two" {
		t.Fatalf("records = %v", got)
	}
}

// TestCopyTruncateWithBufferedGroupCommitsNewOffsets is the regression test
// for the truncation reopen path: entries flushed out of the old content's
// pipeline carry old offsets, which must never drive the new inode's
// checkpoint (the non-carry reopen bumps the generation exactly like the
// carry path). Before the fix, the committed offset landed in the replaced
// content's offset space and a restart skipped that many bytes of the new
// content.
func TestCopyTruncateWithBufferedGroupCommitsNewOffsets(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, filepath.Join(t.TempDir(), "chk"), exp)
	tl.statusEvery = 20 * time.Millisecond
	stop := startTailer(t, tl)

	// A fat exported line, then an unterminated CRI P-fragment that stays
	// buffered in the pipeline.
	fat := strings.Repeat("x", 2048)
	writeLog(t, dir, timeNowCRI()+" stdout F "+fat)
	waitFor(t, func() bool { return len(exp.get()) >= 1 }, "fat line exported")
	writeLog(t, dir, timeNowCRI()+" stdout P dangling-fragment")
	// The fragment is buffered once it has been read (ReadPos advanced) but
	// not committed (the watermark holds the checkpoint at the fat line).
	waitFor(t, func() bool {
		for _, fs := range tl.Status() {
			if fs.ReadPos > fs.Committed && fs.Committed > 0 {
				return true
			}
		}
		return false
	}, "fragment buffered")

	// copytruncate: replace the content with something short.
	if err := os.Truncate(filepath.Join(dir, logName), 0); err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, timeNowCRI()+" stdout F after-truncate")
	waitFor(t, func() bool {
		for _, r := range exp.get() {
			if r == "after-truncate" {
				return true
			}
		}
		return false
	}, "post-truncate line exported")

	// The committed offset must stay within the new content's size.
	size, err := os.Stat(filepath.Join(dir, logName))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		for _, fs := range tl.Status() {
			if fs.Path == filepath.Join(dir, logName) {
				return fs.Committed > 0 && fs.Committed <= size.Size()
			}
		}
		return false
	}, "committed offset within the new content")
	stop()
}

// TestCopyTruncateRefillPastOffsetKeepsPrefix: readFile's identity re-check
// (tailer.go:1365-1366, 1380) only hashes the file head when the sweep read ZERO
// new bytes:
//
//	read == 0 && !st.ModTime().Equal(f.lastMod) && !f.fp.matches(f.f)
//
// A copytruncate (logrotate's copytruncate, or any writer that truncates and
// keeps writing) whose writer refills the file PAST our read offset before the
// next sweep therefore yields bytes (read > 0) and is never identified as a
// rewrite: the sweep reads from the stale offset into the middle of the NEW
// content. Everything the new content holds below the old offset is skipped
// forever, and the first line read is a mid-line fragment.
//
// state: committed = readPos = 108 (3 old lines shipped)
//
//	-> writer truncates to 0 and writes 10 new lines (370 bytes)
//	-> sweep: inode unchanged, size 370 >= readPos 108, read yields 262 bytes
//	-> new-01..new-02 (and half of new-03) are never read: DATA LOST.
//
// Fix: hoist the identity check ahead of the read — stat first, and whenever the
// mtime changed since the last sweep verify f.fp before consuming bytes at the
// stale offset (the fingerprint hash is 1 KiB per changed file per sweep), or
// track the expected size and re-verify whenever size < readPos OR the head hash
// moved. The current post-read check can only ever catch a rewrite that lands at
// a size <= the old offset.
//
// Severity: MEDIUM-HIGH — silent, unbounded loss; needs copytruncate rotation
// (not what kubelet does, but standard for logrotate-managed plain sources,
// which the tailer explicitly supports) and a writer that outruns one poll
// interval.
func TestCopyTruncateRefillPastOffsetKeepsPrefix(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	path := filepath.Join(dir, logName)

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir,
		"2026-07-05T10:00:00Z stdout F old-1",
		"2026-07-05T10:00:00Z stdout F old-2",
		"2026-07-05T10:00:00Z stdout F old-3",
	)
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx) // the three old lines are shipped and committed

	f := tl.files[path]
	if f.committed == 0 {
		t.Fatal("setup: nothing committed")
	}

	// copytruncate: the content is copied away, the file truncated in place, and
	// the writer immediately refills it past our committed offset.
	if err := os.Truncate(path, 0); err != nil {
		t.Fatal(err)
	}
	var lines []string
	for i := 1; i <= 10; i++ {
		lines = append(lines, "2026-07-05T10:00:0"+string(rune('0'+i%10))+"Z stdout F new-"+string(rune('0'+i/10))+string(rune('0'+i%10)))
	}
	writeLog(t, dir, lines...)
	if st, err := os.Stat(path); err != nil || st.Size() <= f.committed {
		t.Fatalf("setup: refilled size must exceed the committed offset (%d)", f.committed)
	}

	for i := 0; i < 3; i++ {
		tl.scanDir(nil, false)
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}

	got := exp.get()
	for i := 1; i <= 10; i++ {
		want := "new-0" + string(rune('0'+i%10))
		if i == 10 {
			want = "new-10"
		}
		if !slices.Contains(got, want) {
			t.Fatalf("AT-LEAST-ONCE VIOLATED: %q never exported after copytruncate refill; exported = %v", want, got)
		}
	}
}

// A checkpoint whose offset is past the (truncated) file's size must restart at
// zero, not skip the new content.
func TestCheckpointBeyondTruncatedSizeRereads(t *testing.T) {
	dir := t.TempDir()
	cp := filepath.Join(t.TempDir(), "checkpoint")
	ctx := context.Background()
	exp := &fakeExporter{}

	tl := driveTailer(dir, exp)
	tl.cfg.Positions = mustOpenPositions(t, cp)
	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir,
		"2026-07-05T10:00:00Z stdout F old-1",
		"2026-07-05T10:00:00Z stdout F old-2",
		"2026-07-05T10:00:00Z stdout F old-3",
	)
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)
	tl.saveCheckpoints()

	// The file is truncated in place and refilled with SHORTER content while the
	// tailer is down; the checkpoint offset now exceeds its size.
	if err := os.Truncate(filepath.Join(dir, logName), 0); err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F fresh")

	tl2 := driveTailer(dir, exp)
	tl2.cfg.Positions = mustOpenPositions(t, cp)
	tl2.scanDir(tl2.loadCheckpoints(), true)
	tl2.sweep(ctx, true)
	tl2.flush(ctx)

	if got := exp.get(); !slices.Contains(got, "fresh") {
		t.Fatalf("post-truncation content skipped by a stale checkpoint offset; exported = %v", got)
	}
}

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
	tl.cfg.Positions = mustOpenPositions(t, ckpt)
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
	tl2.cfg.Positions = mustOpenPositions(t, ckpt)
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

// A file whose metadata never resolves must not monopolise the sweep. Each
// lookup can block server-side for the whole -metadata-wait, and they all run
// on the single goroutine that serves every file on the node, so the retry
// interval has to grow past the cost of the sweep it is spacing out.
func TestMetadataBackoffGrows(t *testing.T) {
	var d time.Duration
	seen := []time.Duration{}
	for i := 0; i < 8; i++ {
		d = nextMetaBackoff(d)
		seen = append(seen, d)
	}
	if seen[0] != minMetaBackoff {
		t.Errorf("first retry = %v, want %v", seen[0], minMetaBackoff)
	}
	for i := 1; i < len(seen); i++ {
		if seen[i] < seen[i-1] {
			t.Fatalf("backoff shrank: %v", seen)
		}
	}
	if got := seen[len(seen)-1]; got != maxMetaBackoff {
		t.Errorf("backoff capped at %v, want %v", got, maxMetaBackoff)
	}
}

// countingMeta fails every lookup until succeed is set, counting the calls and
// optionally taking `delay` per call — the lookup blocks server-side for the
// whole -metadata-wait, which is what the backoff and the sweep budget both
// exist to bound.
type countingMeta struct {
	calls   int
	delay   time.Duration
	succeed bool
}

func (m *countingMeta) Container(_ context.Context, id string, _ time.Duration) (*kubemeta.ContainerMetadata, error) {
	m.calls++
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	if !m.succeed {
		return nil, errors.New("container not found")
	}
	return &kubemeta.ContainerMetadata{
		ContainerID: id,
		Container:   kubemeta.Container{Name: "app", ID: id},
		Pod:         kubemeta.Pod{Name: "pod1", Namespace: "ns1", UID: "uid1", NodeName: "node1"},
	}, nil
}

// trackedFile discovers one CRI log file and returns its (unresolved) record.
func trackedFile(t *testing.T, tl *Tailer, dir string) *file {
	t.Helper()
	writeLines(t, filepath.Join(dir, logName), "2026-07-05T10:00:00Z stdout F hello")
	tl.scanDir(nil, false)
	f := tl.files[filepath.Join(dir, logName)]
	if f == nil {
		t.Fatal("setup: file not tracked")
	}
	if f.resolved {
		t.Fatal("setup: file resolved before any lookup")
	}
	return f
}

// nextMetaBackoff computing a delay is worth nothing unless resolveMetadata
// HONOURS it. Every lookup can block server-side for the whole -metadata-wait
// and they all run on the single sweep goroutine that serves every file on the
// node, so retrying an unresolvable file on every sweep is what lets a handful
// of them monopolise the tailer: nothing is read, no rotation is noticed, and a
// file rotating twice inside that window loses the middle incarnation.
func TestResolveMetadataAppliesAndResetsTheBackoff(t *testing.T) {
	dir := t.TempDir()
	meta := &countingMeta{}
	tl := driveTailer(dir, &fakeExporter{})
	tl.cfg.Metadata = meta
	ctx := context.Background()
	f := trackedFile(t, tl, dir)

	// First attempt: the lookup runs and arms the backoff.
	before := time.Now()
	if tl.resolveMetadata(ctx, f) {
		t.Fatal("resolve reported success with a failing metadata source")
	}
	if meta.calls != 1 {
		t.Fatalf("lookups = %d, want 1", meta.calls)
	}
	if f.metaBackoff != minMetaBackoff {
		t.Errorf("metaBackoff = %v after one failure, want %v", f.metaBackoff, minMetaBackoff)
	}
	// The retry is jittered over [d, 1.25d) so a metadata-service rollout does
	// not put every file on the node — and every node in the fleet — on the
	// same schedule.
	assertRetryWindow(t, before, f.nextMetaTry, minMetaBackoff)

	// Second attempt inside the window: no lookup at all.
	if tl.resolveMetadata(ctx, f) {
		t.Fatal("resolve reported success")
	}
	if meta.calls != 1 {
		t.Fatalf("lookups = %d; a retry inside the backoff window must not touch the metadata service — that is the whole point of computing a delay", meta.calls)
	}

	// Once the window elapses the lookup runs again and the backoff DOUBLES.
	f.nextMetaTry = time.Now().Add(-time.Millisecond)
	before = time.Now()
	if tl.resolveMetadata(ctx, f) {
		t.Fatal("resolve reported success")
	}
	if meta.calls != 2 {
		t.Fatalf("lookups = %d, want 2 once the backoff window elapsed", meta.calls)
	}
	if f.metaBackoff != 2*minMetaBackoff {
		t.Errorf("metaBackoff = %v after two failures, want %v", f.metaBackoff, 2*minMetaBackoff)
	}
	assertRetryWindow(t, before, f.nextMetaTry, 2*minMetaBackoff)

	// A success RESETS it: a container that was merely racing the API server
	// must not carry a minute-long penalty into its next rotation.
	meta.succeed = true
	f.nextMetaTry = time.Now().Add(-time.Millisecond)
	if !tl.resolveMetadata(ctx, f) {
		t.Fatal("resolve failed with a working metadata source")
	}
	if f.metaBackoff != 0 {
		t.Errorf("metaBackoff = %v after a successful resolve, want 0 — a stale penalty delays the NEXT file that reuses this record", f.metaBackoff)
	}
	if !f.resolved {
		t.Error("file not marked resolved")
	}
}

// assertRetryWindow checks that next lands in [base+d, base+1.25d), the range
// jitterMetaBackoff spreads a retry over.
func assertRetryWindow(t *testing.T, base, next time.Time, d time.Duration) {
	t.Helper()
	lo, hi := base.Add(d), base.Add(d+d/4)
	if next.Before(lo) {
		t.Errorf("next retry at +%v, before the backoff of %v: the file is retried early and the backoff bounds nothing",
			next.Sub(base), d)
	}
	// The upper bound is the jitter cap plus whatever the call itself took.
	if next.After(hi.Add(time.Second)) {
		t.Errorf("next retry at +%v, past the jitter cap of %v: the retry is delayed well beyond what was computed",
			next.Sub(base), d+d/4)
	}
}

// The sweep's resolve budget is a SEPARATE bound from the per-file backoff: on
// a node where many files are newly discovered at once (a rollout), every one
// of them is due a lookup, each can block for -metadata-wait, and they all run
// on the goroutine that also does every file's reading. Without a ceiling on
// the sweep's total resolve time a burst of unresolvable files decides how
// often every OTHER file on the node is read.
func TestSweepResolveBudgetBoundsLookups(t *testing.T) {
	const files = 6
	setup := func(budget time.Duration) (*Tailer, *countingMeta) {
		dir := t.TempDir()
		meta := &countingMeta{delay: 20 * time.Millisecond}
		tl := driveTailer(dir, &fakeExporter{})
		tl.cfg.Metadata = meta
		tl.resolveBudget = budget
		for i := 0; i < files; i++ {
			name := fmt.Sprintf("pod%d_ns1_app-0123456789abcde%d.log", i, i)
			writeLines(t, filepath.Join(dir, name), "2026-07-05T10:00:00Z stdout F hello")
		}
		tl.scanDir(nil, false)
		if len(tl.files) != files {
			t.Fatalf("setup: tracked %d files, want %d", len(tl.files), files)
		}
		return tl, meta
	}

	// A budget shorter than a single lookup: the sweep stops resolving after
	// the first one and gets on with the rest of its work. The unreached files
	// are retried next sweep — nothing is lost, the data waits on disk.
	tl, meta := setup(5 * time.Millisecond)
	tl.sweep(context.Background(), true)
	if meta.calls >= files {
		t.Errorf("the sweep issued %d of %d lookups under a %v budget; with no ceiling a burst of unresolvable files decides how often every other file on the node is read",
			meta.calls, files, tl.resolveBudget)
	}
	if meta.calls == 0 {
		t.Error("the sweep issued no lookup at all: the budget must allow progress, or no file ever resolves")
	}

	// ...and with a budget that comfortably covers them, every file IS
	// attempted in one sweep — so the bound above is the budget, not the
	// per-file backoff.
	tl, meta = setup(10 * time.Second)
	tl.sweep(context.Background(), true)
	if meta.calls != files {
		t.Errorf("lookups = %d, want %d: a generous budget must not hold files back", meta.calls, files)
	}
}

// resolvePlain must not LATCH a nil NodeInfo: it runs once per file and the
// built resource is the file's identity for life, so resolving before a
// CONFIGURED node-info provider's first successful fetch permanently stripped
// the node attributes from every record the file exports. With the provider
// returning nil the file now stays unresolved (nothing is read before it can
// be attributed — the containerd path's rule) and the next sweep retries; the
// shipped agent's provider (selfmeta.Poll with a non-nil Initial) never
// returns nil, so this defers only for wirings whose provider genuinely
// resolves later. A nil Config.NodeInfo (node metadata not wanted at all)
// still resolves immediately.
func TestPlainResolveDefersUntilNodeInfoAvailable(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	var node atomic.Pointer[attrs.NodeInfo]
	tl := New(Config{
		Sources:       []Source{{Name: "plain", Include: []string{filepath.Join(dir, "*.log")}}},
		PollInterval:  20 * time.Millisecond,
		FlushInterval: time.Millisecond,
		BatchSize:     1 << 20,
		MetadataWait:  time.Second,
		Metadata:      fakeMeta{},
		NodeInfo:      func() *attrs.NodeInfo { return node.Load() },
		Exporter:      exp,
	})
	tl.retryBackoff = time.Millisecond

	tl.scanDir(tl.loadCheckpoints(), true)
	path := filepath.Join(dir, "app.log")
	writeLines(t, path, "plain-one")
	tl.scanDir(nil, false)
	f := tl.files[path]
	if f == nil {
		t.Fatal("setup: file not tracked")
	}

	// Provider configured but unresolved: resolution defers, nothing is read.
	for range 3 {
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}
	if f.resolved {
		t.Fatal("plain file resolved against a nil NodeInfo — the nil would be latched into its resource")
	}
	if got := exp.get(); len(got) != 0 {
		t.Fatalf("records exported before attribution: %v", got)
	}

	// The provider yields: the file resolves and its records carry node attrs.
	node.Store(&attrs.NodeInfo{Name: "node1"})
	driveUntil(t, ctx, tl, func() bool { return slices.Contains(exp.get(), "plain-one") },
		"plain line delivered once node info resolved")
	exp.mu.Lock()
	defer exp.mu.Unlock()
	if exp.resAttrs["k8s.node.name"] != "node1" {
		t.Fatalf("node attribute missing after deferred resolve: %v", exp.resAttrs)
	}
}
