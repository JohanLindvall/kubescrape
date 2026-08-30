package tailer

// Tests for the tailer's OBSERVABILITY contracts: the paths that decide not to
// collect a file, or to stop collecting one, must be visible in the two things
// an operator has during an incident — a metric for the rate and a log line for
// the context a metric cannot carry.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// capture collects the log lines a test provokes. slog handlers are called
// from the sweep goroutine, so the buffer is mutex-guarded.
type capture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *capture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *capture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

// logTo wires a level-filtered text logger into the tailer and returns the
// capture buffer.
func logTo(tl *Tailer, level slog.Level) *capture {
	c := &capture{}
	tl.log = slog.New(slog.NewTextHandler(c, &slog.HandlerOptions{Level: level}))
	return c
}

// TestSkippedFileIsCountedOncePerFileAndNamedAtDebug pins the discovery-skip
// contract. Every one of these decisions is re-taken on EVERY discovery pass,
// so the counter has to move once per FILE — a counter that moved per pass
// would read as a fleet-wide storm on a node that is merely doing what it was
// configured to do — and the path has to appear at Debug, because "why is this
// pod's log missing?" is not answerable from a count.
func TestSkippedFileIsCountedOncePerFileAndNamedAtDebug(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, "2026-07-05T10:00:00.000000000Z stdout F hello")

	tl := newTestTailer(dir, "", &fakeExporter{})
	tl.cfg.ExcludeNamespaces = []string{"ns1"}
	logs := logTo(tl, slog.LevelDebug)

	before := obs.LogFilesSkipped.WithLabelValues(skipExcludedNS).Value()
	tl.scanDir(nil, true)
	if got := obs.LogFilesSkipped.WithLabelValues(skipExcludedNS).Value() - before; got != 1 {
		t.Fatalf("after one scan, %s counted %v, want 1", skipExcludedNS, got)
	}
	// The line names the file, which is the whole point of having it.
	out := logs.String()
	if !strings.Contains(out, "log file not tracked") ||
		!strings.Contains(out, logName) ||
		!strings.Contains(out, "reason="+skipExcludedNS) {
		t.Fatalf("skip was not named at Debug; got:\n%s", out)
	}

	// A second (and third) pass over the same, still-skipped file must not
	// count again: the diff against the previous pass is what turns a state
	// into an event.
	tl.scanDir(nil, false)
	tl.scanDir(nil, false)
	if got := obs.LogFilesSkipped.WithLabelValues(skipExcludedNS).Value() - before; got != 1 {
		t.Fatalf("after three scans, %s counted %v, want 1 (once per file, not once per pass)", skipExcludedNS, got)
	}
}

// TestSkippedFileIsNotReportedWhenAnotherSourceClaimsIt is the exemption that
// keeps the namespaces ALLOWLIST quiet: "prod through source A, the rest
// through source B" is routing, not a skip, and reporting it would put a Debug
// line and a counter bump on every correctly-routed file.
func TestSkippedFileIsNotReportedWhenAnotherSourceClaimsIt(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, "2026-07-05T10:00:00.000000000Z stdout F hello")

	tl := New(Config{
		Sources: []Source{
			{Name: "prod", Include: []string{filepath.Join(dir, "*.log")}, Namespaces: []string{"prod"}},
			{Name: "rest", Include: []string{filepath.Join(dir, "*.log")}},
		},
		PollInterval:  20 * time.Millisecond,
		FlushInterval: 50 * time.Millisecond,
		BatchSize:     10,
		MetadataWait:  time.Second,
		Metadata:      fakeMeta{},
		Exporter:      &fakeExporter{},
	})
	before := obs.LogFilesSkipped.WithLabelValues(skipNamespaceNotSel).Value()
	tl.scanDir(nil, true)
	if _, tracked := tl.files[filepath.Join(dir, logName)]; !tracked {
		t.Fatal("the catch-all source should have claimed the file")
	}
	if got := obs.LogFilesSkipped.WithLabelValues(skipNamespaceNotSel).Value() - before; got != 0 {
		t.Fatalf("a file another source claimed counted %v skips, want 0", got)
	}
}

// TestNonRegularFileIsReportedAsSkipped covers the "should not happen" branch
// that does happen: a FIFO in a log directory is never opened (the open would
// block the single sweep goroutine node-wide), and until now nothing said so.
func TestNonRegularFileIsReportedAsSkipped(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, logName)
	if err := syscall.Mkfifo(fifo, 0o644); err != nil {
		t.Skipf("cannot create a FIFO here: %v", err)
	}
	tl := newTestTailer(dir, "", &fakeExporter{})
	logs := logTo(tl, slog.LevelDebug)
	before := obs.LogFilesSkipped.WithLabelValues(skipNonRegular).Value()
	tl.scanDir(nil, true)
	if got := obs.LogFilesSkipped.WithLabelValues(skipNonRegular).Value() - before; got != 1 {
		t.Fatalf("%s counted %v, want 1", skipNonRegular, got)
	}
	if !strings.Contains(logs.String(), "reason="+skipNonRegular) {
		t.Fatalf("the FIFO was not named at Debug; got:\n%s", logs.String())
	}
}

// TestUnresolvedFilesAreGaugedAndWarnedOnceTheyAreOld pins the tailer's one
// silent failure mode: a tracked file that cannot be attributed is never read,
// so it moves no byte counter, loses nothing and — before this — showed up
// nowhere. The warning is age-gated because every file is unresolved for its
// first sweep.
func TestUnresolvedFilesAreGaugedAndWarnedOnceTheyAreOld(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, "2026-07-05T10:00:00.000000000Z stdout F hello")

	tl := newTestTailer(dir, "", &fakeExporter{})
	logs := logTo(tl, slog.LevelInfo)
	tl.scanDir(nil, true)
	f, ok := tl.files[filepath.Join(dir, logName)]
	if !ok {
		t.Fatal("file not tracked")
	}
	f.resolved = false

	// Freshly discovered: gauged, but not yet worth waking anyone.
	tl.publishStatus()
	if got := obs.LogFilesUnresolved.Value(); got != 1 {
		t.Fatalf("kubescrape_log_files_unresolved = %v, want 1", got)
	}
	if strings.Contains(logs.String(), "without resolving their metadata") {
		t.Fatalf("a file unresolved for an instant must not warn; got:\n%s", logs.String())
	}

	// Old enough that this is no longer a container racing the API server.
	f.discovered = time.Now().Add(-2 * unresolvedWarnAfter)
	tl.publishStatus()
	out := logs.String()
	if !strings.Contains(out, "without resolving their metadata") || !strings.Contains(out, logName) {
		t.Fatalf("an old unresolved file did not warn with its path; got:\n%s", out)
	}

	// Once it resolves the gauge returns to zero, so a nonzero value always
	// means the condition is CURRENT rather than "happened once".
	f.resolved = true
	tl.publishStatus()
	if got := obs.LogFilesUnresolved.Value(); got != 0 {
		t.Fatalf("kubescrape_log_files_unresolved = %v after resolving, want 0", got)
	}
}

// TestExportFailureLogsTheTransitionAndTheRecovery pins the shape a persisting
// condition takes in this repo (cmd/kubescrape/apiserver.go's model): the first
// failure always logs, the repeats are throttled behind the counter, and the
// recovery says so — otherwise an outage ends in silence that reads exactly
// like the process having stopped exporting.
func TestExportFailureLogsTheTransitionAndTheRecovery(t *testing.T) {
	tl := newTestTailer(t.TempDir(), "", &fakeExporter{})
	logs := logTo(tl, slog.LevelInfo)
	inf := &batchInfo{kept: 3, cands: map[*file]map[int]int64{}}

	tl.failBatch(inf, os.ErrDeadlineExceeded)
	if n := strings.Count(logs.String(), "exporting logs failed, rewinding"); n != 1 {
		t.Fatalf("first failure logged %d times, want 1", n)
	}
	// The repeats are throttled: the window has just opened, so nothing more
	// may be emitted until it lapses.
	for range 5 {
		tl.failBatch(inf, os.ErrDeadlineExceeded)
	}
	if n := strings.Count(logs.String(), "still failing"); n != 0 {
		t.Fatalf("throttled repeats logged %d times inside the window, want 0", n)
	}
	if tl.exportFailures != 6 {
		t.Fatalf("exportFailures = %d, want 6 (the count the log line carries)", tl.exportFailures)
	}

	tl.commitBatch(inf)
	out := logs.String()
	if !strings.Contains(out, "log export recovered") || !strings.Contains(out, "failures=6") {
		t.Fatalf("recovery was not reported with its failure count; got:\n%s", out)
	}
	if tl.exportFailures != 0 {
		t.Fatalf("exportFailures = %d after recovery, want 0", tl.exportFailures)
	}
	// A second successful batch must not log again — Info stays quiet in the
	// steady state.
	tl.commitBatch(inf)
	if n := strings.Count(logs.String(), "log export recovered"); n != 1 {
		t.Fatalf("recovery logged %d times, want 1", n)
	}
}

// TestRotationArmIsNamedAtDebug: kubescrape_log_rotations_total counts all
// three arms together, and which one was taken is exactly what decides whether
// the old incarnation's remainder is recoverable (a rename preserves it as a
// segment; an in-place truncation destroys it unmeasurably).
func TestRotationArmIsNamedAtDebug(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, logName)
	writeLines(t, path, "2026-07-05T10:00:00.000000000Z stdout F one")

	tl := newTestTailer(dir, "", &fakeExporter{})
	logs := logTo(tl, slog.LevelDebug)
	ctx := context.Background()
	tl.scanDir(nil, true)
	f := tl.files[path]
	f.resolved = true
	if err := tl.readFile(ctx, f); err != nil {
		t.Fatalf("readFile: %v", err)
	}
	// Truncate in place: the size falls below our read position.
	if err := os.WriteFile(path, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := tl.readFile(ctx, f); err != nil {
		t.Fatalf("readFile after truncate: %v", err)
	}
	out := logs.String()
	if !strings.Contains(out, "log file rotated") || !strings.Contains(out, "reason=truncated") {
		t.Fatalf("the truncation arm was not named at Debug; got:\n%s", out)
	}
}

// TestResolveBudgetExhaustionIsCountedAndWarned pins the tailer's counterpart
// of promscrape's ScrapeMetaBudgetExhausted. A metadata lookup BLOCKS
// server-side and every file on the node is resolved by the one sweep
// goroutine, so past the shared budget files are not even ASKED about — no
// request is issued, so kubescrape_metadata_requests_total cannot move, and the
// files stay tracked, unresolved and unread with nothing to show for it.
//
// Counted once per SWEEP (not once per unreached file) so a rate is comparable
// with the sweep cadence.
func TestResolveBudgetExhaustionIsCountedAndWarned(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{
		"pod1_ns1_app-1111111111111111.log",
		"pod2_ns1_app-2222222222222222.log",
		"pod3_ns1_app-3333333333333333.log",
	} {
		writeLines(t, filepath.Join(dir, n), "2026-07-05T10:00:00.000000000Z stdout F hello")
	}
	tl := newTestTailer(dir, "", &fakeExporter{})
	// Every resolution fails slowly, and the budget is spent by the first one.
	tl.cfg.Metadata = slowFailingMeta{delay: 20 * time.Millisecond}
	tl.resolveBudget = time.Millisecond
	logs := logTo(tl, slog.LevelInfo)

	before := obs.LogMetadataBudgetExhausted.Value()
	tl.scanDir(nil, true)
	tl.sweep(context.Background(), true)

	if got := obs.LogMetadataBudgetExhausted.Value() - before; got != 1 {
		t.Fatalf("kubescrape_log_metadata_budget_exhausted_total moved by %v, want 1 (once per sweep)", got)
	}
	out := logs.String()
	if !strings.Contains(out, "metadata-resolution budget ran out") {
		t.Fatalf("budget exhaustion was not warned; got:\n%s", out)
	}
	// The line has to say HOW MANY files were left unread — one file's error
	// cannot convey the scale, and the scale is the thing that distinguishes
	// "a pod is starting" from "the metadata service is gone".
	if !strings.Contains(out, "files=") {
		t.Fatalf("the warning did not carry the number of unreached files; got:\n%s", out)
	}
	// The two conditions CO-OCCUR — a hanging metadata service makes lookups
	// fail and spends the budget in the same sweep — and they say different
	// things. They therefore hold separate throttles: sharing one would let
	// whichever fired first silence the other for its whole window, on exactly
	// the outage both exist to describe.
	if !strings.Contains(out, "fetching container metadata failed") {
		t.Fatalf("the budget warning suppressed the lookup warning; they must not share a throttle. got:\n%s", out)
	}
}

// slowFailingMeta makes every lookup cost real time and then fail, which is the
// shape a hanging metadata service has (a refused one fails instantly and the
// sweep simply gets faster — see the metabudget discussion in promscrape).
type slowFailingMeta struct{ delay time.Duration }

func (m slowFailingMeta) Container(ctx context.Context, id string, wait time.Duration) (*kubemeta.ContainerMetadata, error) {
	select {
	case <-time.After(m.delay):
	case <-ctx.Done():
	}
	return nil, errors.New("metadata service unreachable")
}

// TestSkipsSurviveAFailedListing: a failed glob proves nothing about the paths
// it did not list, and FORGETTING a skipped path is what makes it count again.
// A directory that flaps between readable and not would otherwise report every
// file it holds on every recovery, which breaks the "once per file" claim
// kubescrape_log_files_skipped_total's help text makes — and does it on a node
// that is already having a bad time.
func TestSkipsSurviveAFailedListing(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, "2026-07-05T10:00:00.000000000Z stdout F hello")

	tl := newTestTailer(dir, "", &fakeExporter{})
	tl.cfg.ExcludeNamespaces = []string{"ns1"}

	before := obs.LogFilesSkipped.WithLabelValues(skipExcludedNS).Value()
	tl.scanDir(nil, true)

	// A pass in which the listing proved nothing: the remembered verdict must
	// carry forward untouched.
	tl.reportSkips(map[string]string{}, nil, false)
	// ...and the pass after it, with the listing back, must not re-count.
	tl.scanDir(nil, false)

	if got := obs.LogFilesSkipped.WithLabelValues(skipExcludedNS).Value() - before; got != 1 {
		t.Fatalf("%s counted %v across a failed listing, want 1", skipExcludedNS, got)
	}
}

// TestUnstattableFileIsWarnedNotOnlyCounted is the one discovery skip that is a
// FAILURE rather than a selection, and so the one that must be visible without
// -log-level=debug. An EACCES here is the classic first-live-run defect (the
// log hostPath mounted with the wrong ownership), and the counter alone cannot
// say which file or which errno — an unreadable mount and a failing disk are
// the same number and different jobs.
func TestUnstattableFileIsWarnedNotOnlyCounted(t *testing.T) {
	dir := t.TempDir()
	// A self-referential symlink: readdir LISTS the name (glob never stats), and
	// os.Stat then fails ELOOP. That is the shape being reported — the glob
	// produced a path this pass cannot turn into a file — and unlike an EACCES
	// it reproduces for any uid, root included.
	loop := filepath.Join(dir, logName)
	if err := os.Symlink(loop, loop); err != nil {
		t.Skipf("cannot create a symlink here: %v", err)
	}

	tl := newTestTailer(dir, "", &fakeExporter{})
	// Info, deliberately: the whole point is that this one does not need Debug.
	logs := logTo(tl, slog.LevelInfo)

	before := obs.LogFilesSkipped.WithLabelValues(skipStatError).Value()
	tl.scanDir(nil, true)
	if got := obs.LogFilesSkipped.WithLabelValues(skipStatError).Value() - before; got != 1 {
		t.Fatalf("%s counted %v, want 1", skipStatError, got)
	}
	out := logs.String()
	if !strings.Contains(out, "level=WARN") ||
		!strings.Contains(out, "could not be stat'd") ||
		!strings.Contains(out, logName) ||
		!strings.Contains(out, "files=1") {
		t.Fatalf("an unstattable log file was not warned about at Info; got:\n%s", out)
	}
	// The errno has to survive into the line: an unreadable mount and a failing
	// disk are the same counter and different jobs.
	if !strings.Contains(out, "error=") {
		t.Fatalf("the warning must carry the errno; got:\n%s", out)
	}
}

// TestVanishedFileIsCountedButNotWarned is the bound that keeps the warning
// above from becoming the flood the Debug default was avoiding: an ENOENT
// between the readdir and the stat is a rename rotation caught mid-scan, which
// is benign and constant on a busy node. It still COUNTS — the metric's help
// enumerates stat_error as any unstattable path — but it must not warn.
func TestVanishedFileIsCountedButNotWarned(t *testing.T) {
	tl := newTestTailer(t.TempDir(), "", &fakeExporter{})
	logs := logTo(tl, slog.LevelInfo)

	before := obs.LogFilesSkipped.WithLabelValues(skipStatError).Value()
	tl.reportSkips(
		map[string]string{"/var/log/containers/gone.log": skipStatError},
		map[string]string{"/var/log/containers/gone.log": "stat /var/log/containers/gone.log: no such file or directory"},
		true)
	if got := obs.LogFilesSkipped.WithLabelValues(skipStatError).Value() - before; got != 1 {
		t.Fatalf("a vanished file counted %v, want 1 (it is still an unstattable path)", got)
	}
	if strings.Contains(logs.String(), "could not be stat'd") {
		t.Fatalf("a rotation race must not warn; got:\n%s", logs.String())
	}
}

// TestUnstattableFilesWarnOnceForTheWholeMountIsThrottled: a wrongly-mounted log
// directory fails every file in it at once, for one reason, with one remedy. The
// line therefore names one example plus how many others shared the pass, and the
// keyless throttle keeps a persisting state off the two-second sweep.
func TestUnstattableFilesWarnOnceForTheWholeMountIsThrottled(t *testing.T) {
	tl := newTestTailer(t.TempDir(), "", &fakeExporter{})
	logs := logTo(tl, slog.LevelInfo)

	skipped := map[string]string{}
	detail := map[string]string{}
	for _, p := range []string{"/a.log", "/b.log", "/c.log"} {
		skipped[p] = skipStatError
		detail[p] = "stat " + p + ": permission denied"
	}
	tl.reportSkips(skipped, detail, true)
	if n := strings.Count(logs.String(), "could not be stat'd"); n != 1 {
		t.Fatalf("three unstattable files produced %d warnings, want 1 aggregate line", n)
	}
	if !strings.Contains(logs.String(), "files=3") {
		t.Fatalf("the aggregate line must say how many files share the condition; got:\n%s", logs.String())
	}

	// A second pass with a NEW unstattable file is inside the throttle window,
	// so it stays silent: the condition is a state, not an event.
	skipped2 := map[string]string{"/d.log": skipStatError}
	detail2 := map[string]string{"/d.log": "stat /d.log: permission denied"}
	tl.reportSkips(skipped2, detail2, true)
	if n := strings.Count(logs.String(), "could not be stat'd"); n != 1 {
		t.Fatalf("the throttle let a second line through within the window; got %d", n)
	}
}

// TestRotationClassificationReadsTheFingerprintOnce pins the cost of the
// rotation report. The copytruncate arm's guard preads -logs-fingerprint-bytes
// off the file head and FNV-hashes them; handleRotation runs once per tracked
// file per sweep — the 500ms poll plus every fsnotify event — so classifying
// twice (once to log the arm, once to act on it) doubled that read on every
// file on the node, and slog's eager argument evaluation means an Enabled
// guard around a separate reporting switch would not have removed it.
//
// The assertion is allocation-based because the read has no other observable:
// computeFingerprint allocates its buffer per call, so the whole classification
// must cost no more than ONE fp.matches. Measured against a live
// fp.matches rather than a literal, so a change in what a fingerprint read
// costs cannot silently turn the budget into "two are fine".
func TestRotationClassificationReadsTheFingerprintOnce(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector adds bookkeeping allocations")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, logName)
	writeLines(t, path, "2026-07-05T10:00:00.000000000Z stdout F one")

	tl := newTestTailer(dir, "", &fakeExporter{})
	ctx := context.Background()
	tl.scanDir(nil, true)
	f := tl.files[path]
	f.resolved = true
	if err := tl.readFile(ctx, f); err != nil {
		t.Fatalf("readFile: %v", err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// The shape that reaches the copytruncate guard and finds nothing wrong:
	// same inode, no new bytes (read == 0), an mtime that moved since our last
	// read, and a head that still matches. handleRotation is then inert, so it
	// can be run repeatedly.
	f.lastMod = time.Time{}
	if !f.fp.matches(f.f) {
		t.Fatal("test setup: the head fingerprint should still match")
	}

	perRead := testing.AllocsPerRun(50, func() { _ = f.fp.matches(f.f) })
	whole := testing.AllocsPerRun(50, func() { tl.handleRotation(ctx, f, st, 0) })
	if whole > perRead*1.5 {
		t.Fatalf("handleRotation allocated %v, about %v fingerprint reads (one costs %v): "+
			"the classification is being computed twice", whole, whole/perRead, perRead)
	}
	if f.f == nil || f.readPos == 0 {
		t.Fatal("handleRotation should have been inert in this shape")
	}
}

// TestFailedPositionsSaveIsThrottledAndItsRecoveryLogged pins the shape of the
// checkpoint-write complaint. saveCheckpoints runs on a 10s cadence and the
// two conditions that actually fail it — a read-only mount, a full disk — do
// not change between attempts, so an unthrottled Warn is six identical lines a
// minute from every node in the fleet describing one unchanging fact; the rate
// belongs to kubescrape_positions_save_errors_total. The first failure must
// still log immediately (an operator learns of the outage when it starts), and
// the recovery must log once, or the throttled stream has no end marker.
func TestFailedPositionsSaveIsThrottledAndItsRecoveryLogged(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: permissions cannot make a write fail")
	}
	dir := t.TempDir()
	posDir := t.TempDir()
	tl := newTestTailer(dir, filepath.Join(posDir, "positions.json"), &fakeExporter{})
	logs := logTo(tl, slog.LevelDebug)

	// A read-only directory fails the store's temp-file write, which is the
	// read-only-mount case exactly.
	if err := os.Chmod(posDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(posDir, 0o755) })
	for range 5 {
		tl.saveCheckpoints()
	}
	if n := strings.Count(logs.String(), `msg="writing positions file"`); n != 1 {
		t.Fatalf("failed positions save logged %d times over 5 attempts, want 1 inside the throttle window; got:\n%s", n, logs.String())
	}
	if err := os.Chmod(posDir, 0o755); err != nil {
		t.Fatal(err)
	}
	tl.saveCheckpoints()
	if !strings.Contains(logs.String(), "positions file write recovered") {
		t.Fatalf("the recovery was not logged, so the throttled warnings have no end marker; got:\n%s", logs.String())
	}
	// And a later outage warns again: the throttle must not latch the
	// condition off for the process' lifetime.
	tl.positionsWarn = logdedupe.Throttle{}
	if err := os.Chmod(posDir, 0o500); err != nil {
		t.Fatal(err)
	}
	// The document must differ from the one the recovery wrote, or the store
	// skips the write (positions.Store.save) and there is no I/O to fail. That
	// is the intended semantics, not a hole: an unchanged document is already
	// durable at that path, so nothing is being lost and there is nothing to
	// complain about. A node with any log traffic at all changes an offset
	// every sweep, so the read-only mount surfaces on the next save that has
	// something to say.
	tl.checkpoints = map[string]checkpoint{"/var/log/containers/late_ns_c-0.log": {Offset: 42, Inode: 9}}
	tl.saveCheckpoints()
	if n := strings.Count(logs.String(), `msg="writing positions file"`); n != 2 {
		t.Fatalf("a second outage warned %d times in total, want 2", n)
	}
}

// TestUnchangedPositionsDocumentIsNotRewritten pins the other half of that
// semantics, because it is what makes the tailer's save cheap: one save
// marshals the whole document and fsyncs twice, and re-writing bytes that are
// already durable at that path buys nothing. It runs on the single sweep
// goroutine, so the 10-second cadence on a quiet node — and every repeat save
// inside one sweep — used to be a whole-node stall for no change at all.
func TestUnchangedPositionsDocumentIsNotRewritten(t *testing.T) {
	dir := t.TempDir()
	posDir := t.TempDir()
	posPath := filepath.Join(posDir, "positions.json")
	tl := newTestTailer(dir, posPath, &fakeExporter{})

	tl.checkpoints = map[string]checkpoint{"/var/log/containers/a_ns_c-0.log": {Offset: 10, Inode: 1}}
	tl.saveCheckpoints()
	st, err := os.Stat(posPath)
	if err != nil {
		t.Fatal(err)
	}
	// A rename replaces the inode, so an unchanged inode is proof no write
	// happened — a timestamp would be too coarse to see.
	for range 5 {
		tl.saveCheckpoints()
	}
	st2, err := os.Stat(posPath)
	if err != nil {
		t.Fatal(err)
	}
	if inodeOf(st2) != inodeOf(st) {
		t.Fatalf("an unchanged positions document was rewritten (inode %d -> %d); that is two fsyncs on the sweep goroutine for no change",
			inodeOf(st), inodeOf(st2))
	}

	// A changed document still writes.
	tl.checkpoints["/var/log/containers/b_ns_c-0.log"] = checkpoint{Offset: 20, Inode: 2}

	tl.saveCheckpoints()
	st3, err := os.Stat(posPath)
	if err != nil {
		t.Fatal(err)
	}
	if inodeOf(st3) == inodeOf(st2) {
		t.Fatal("a changed positions document was not written")
	}
	stored, err := positions.Open(posPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := stored.Logs()["/var/log/containers/b_ns_c-0.log"].Offset; got != 20 {
		t.Fatalf("stored offset = %d, want 20", got)
	}
}
