package tailer

// The tailer's transform seam (Config.Transform): the script runs ONCE per
// just-built batch, in place, BEFORE the retry loop — the retries re-send the
// already-transformed payload through the chain below the transform layer.
// An all-dropped-by-transform batch commits its offsets without a send; a
// script error behaves like a failed export (rewind, and the re-read next
// sweep re-runs whatever program is active by then — hot reloads included).

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/transform"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"go.opentelemetry.io/collector/pdata/plog"
)

func mustTransform(t *testing.T, cfg string) *transform.Wrapper {
	t.Helper()
	prog, err := transform.Compile([]byte(cfg))
	if err != nil {
		t.Fatal(err)
	}
	// The wrapper's downstream is irrelevant here: the tailer uses only
	// TransformLogs and exports through its own (inner) exporter.
	return transform.Wrap(nil, nil, prog)
}

// A batch whose export fails N times and then succeeds runs the script
// EXACTLY ONCE, and what the collector finally receives is the transformed
// payload (a single prefix — a per-attempt re-run would stack them).
func TestTransformRunsOncePerBatchAcrossRetries(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{fail: 2} // exportWithRetry's 3 attempts: fail, fail, deliver
	tl := driveTailer(dir, exp)
	w := mustTransform(t, "logs: |\n  def transform(batch):\n      for r in batch:\n          r.body = \"[t] \" + r.body\n")
	calls := 0
	tl.cfg.Transform = func(ld plog.Logs) (int, error) {
		calls++
		return w.TransformLogs(ld)
	}

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir,
		"2026-07-05T10:00:00Z stdout F one",
		"2026-07-05T10:00:01Z stdout F two")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	if calls != 1 {
		t.Fatalf("transform ran %d times for one batch; must run once, before the retry loop", calls)
	}
	if exp.attempts != 3 {
		t.Fatalf("export attempts = %d, want 3 (two failures, one delivery)", exp.attempts)
	}
	got := exp.get()
	if len(got) != 2 || got[0] != "[t] one" || got[1] != "[t] two" {
		t.Fatalf("delivered records = %q; want the transformed bodies, prefixed exactly once", got)
	}
}

// A transform that drops everything acks the batch: offsets commit, nothing
// is ever sent.
func TestTransformDropsAllCommitsWithoutSend(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	w := mustTransform(t, "logs: |\n  def transform(batch):\n      for r in batch:\n          r.drop()\n")
	tl.cfg.Transform = w.TransformLogs

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir,
		"2026-07-05T10:00:00Z stdout F noisy one",
		"2026-07-05T10:00:01Z stdout F noisy two")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	if exp.attempts != 0 {
		t.Fatalf("export attempts = %d; an all-dropped batch must not be sent", exp.attempts)
	}
	path := filepath.Join(dir, logName)
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	f := tl.files[path]
	if f == nil || f.committed != st.Size() {
		t.Fatalf("committed = %v, want %d — dropped records must still advance offsets", f.committed, st.Size())
	}
}

// A script error fails the batch like a failed export: files rewind (nothing
// was sent), and the re-read next sweep runs whatever program is active by
// then — here a hot-reload fix, exactly the recovery an operator performs.
func TestTransformErrorRewindsAndRerunsReloadedScript(t *testing.T) {
	failuresBefore := obs.LogExportFailures.Value()
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	w := mustTransform(t, "logs: |\n  def transform(batch):\n      fail(\"boom\")\n")
	tl.cfg.Transform = w.TransformLogs

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F payload")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	if exp.attempts != 0 {
		t.Fatalf("export attempts = %d; a transform error must fail the batch before any send", exp.attempts)
	}
	path := filepath.Join(dir, logName)
	if f := tl.files[path]; f == nil || f.committed != 0 {
		t.Fatalf("committed = %v after a transform error, want 0 (rewind)", f.committed)
	}
	if got := obs.LogExportFailures.Value() - failuresBefore; got < 1 {
		t.Fatalf("kubescrape_log_export_failures_total moved by %v; a transform error is a failed export", got)
	}

	// The fix arrives by hot reload; the rewound bytes re-read and re-run it.
	good, err := transform.Compile([]byte("logs: |\n  def transform(batch):\n      for r in batch:\n          r.body = \"[fixed] \" + r.body\n"))
	if err != nil {
		t.Fatal(err)
	}
	w.Swap(good)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(exp.get()) == 0 {
		tl.sweep(ctx, true)
		tl.flush(ctx)
		time.Sleep(10 * time.Millisecond)
	}
	got := exp.get()
	if len(got) != 1 || got[0] != "[fixed] payload" {
		t.Fatalf("after the reload the re-read batch delivered %q; want the fixed script applied once", got)
	}
	if strings.Count(got[0], "[fixed] ") != 1 {
		t.Fatalf("prefix applied %d times", strings.Count(got[0], "[fixed] "))
	}
}

// The script's drops are counted once the batch's records SETTLE, not when the
// batch is transformed: a failed flush rewinds the files, the next sweep
// rebuilds the batch from the same bytes, and the script drops the same
// records again — so counting at transform time made one intended drop read as
// one per failed flush of the outage (measured: 3 for 1 across two failed
// flushes). The permanent-rejection arm settles its records too: they are
// never re-read, so their drops are counted there or nowhere.
func TestTransformDropsAreCountedOncePerSettledBatch(t *testing.T) {
	const script = "logs: |\n  def transform(batch):\n      for r in batch:\n          if r.body == \"drop\":\n              r.drop()\n"
	dropped := obs.TransformDropped.WithLabelValues("logs")
	run := func(t *testing.T, exp *fakeExporter) {
		t.Helper()
		dir := t.TempDir()
		ctx := context.Background()
		tl := driveTailer(dir, exp)
		tl.cfg.Transform = mustTransform(t, script).TransformLogs
		tl.scanDir(tl.loadCheckpoints(), true)
		writeLog(t, dir,
			"2026-07-05T10:00:00Z stdout F drop",
			"2026-07-05T10:00:01Z stdout F keep")
		tl.scanDir(nil, false)
		path := filepath.Join(dir, logName)
		st, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			tl.sweep(ctx, true)
			tl.flush(ctx)
			if f := tl.files[path]; f != nil && f.committed == st.Size() {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("the batch never settled")
	}

	t.Run("delivered after two failed flushes", func(t *testing.T) {
		before := dropped.Value()
		exp := &fakeExporter{fail: 6} // exportWithRetry's 3 attempts per flush: two failed flushes, then delivery
		run(t, exp)
		if got := exp.get(); len(got) != 1 || got[0] != "keep" {
			t.Fatalf("delivered %q, want [keep]", got)
		}
		if exp.attempts != 7 {
			t.Fatalf("export attempts = %d, want 7 — the premise is two rewound flushes", exp.attempts)
		}
		if got := dropped.Value() - before; got != 1 {
			t.Fatalf("transform_dropped moved by %v for one dropped record, want 1", got)
		}
	})

	t.Run("permanently rejected", func(t *testing.T) {
		before := dropped.Value()
		run(t, &fakeExporter{permanentN: 1})
		if got := dropped.Value() - before; got != 1 {
			t.Fatalf("transform_dropped moved by %v for one dropped record of an advanced batch, want 1", got)
		}
	})
}
