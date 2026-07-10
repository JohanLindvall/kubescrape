package tailer

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
)

func newPipelinedTailer(dir, checkpoint string, exp LogExporter) *Tailer {
	tl := New(Config{
		Dir:             dir,
		CheckpointFile:  checkpoint,
		PollInterval:    20 * time.Millisecond,
		FlushInterval:   50 * time.Millisecond,
		BatchSize:       1000,
		MetadataWait:    time.Second,
		Metadata:        fakeMeta{},
		Exporter:        exp,
		PipelinedExport: true,
	})
	tl.retryBackoff = 10 * time.Millisecond
	return tl
}

// Pipelined happy path: everything is exported once, in order, and offsets
// commit (verified by a restart reading nothing back).
func TestPipelinedExport(t *testing.T) {
	dir := t.TempDir()
	cp := filepath.Join(t.TempDir(), "checkpoint")
	exp := &fakeExporter{}
	tl := newPipelinedTailer(dir, cp, exp)
	stop := startTailer(t, tl)

	writeLog(t, dir,
		timeNowCRI()+" stdout F one",
		timeNowCRI()+" stdout F two",
	)
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "first batch")
	writeLog(t, dir, timeNowCRI()+" stdout F three")
	waitFor(t, func() bool { return len(exp.get()) == 3 }, "second batch")
	stop()

	got := exp.get()
	if got[0] != "one" || got[1] != "two" || got[2] != "three" {
		t.Fatalf("records = %q", got)
	}

	// A restart resumes from the committed offsets: nothing is re-read.
	tl2 := newPipelinedTailer(dir, cp, exp)
	stop2 := startTailer(t, tl2)
	defer stop2()
	writeLog(t, dir, timeNowCRI()+" stdout F four")
	waitFor(t, func() bool { return len(exp.get()) == 4 }, "post-restart record")
	if got := exp.get(); got[3] != "four" {
		t.Fatalf("after restart records = %q (duplicates mean offsets did not commit)", got)
	}
}

// A failed in-flight export rewinds at the next settle; the re-read includes
// the read-ahead that happened while the export was in flight — everything
// arrives exactly once here because the failed batch never landed.
func TestPipelinedExportFailureRewinds(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{fail: 3} // first exportWithRetry (3 attempts) fails
	tl := newPipelinedTailer(dir, "", exp)
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir,
		timeNowCRI()+" stdout F one",
		timeNowCRI()+" stdout F two",
	)
	// Give the first (failing) flush time to be handed off, then extend the
	// file — this read-ahead must survive the rewind purge and re-read.
	time.Sleep(150 * time.Millisecond)
	writeLog(t, dir,
		timeNowCRI()+" stdout F three",
		timeNowCRI()+" stdout F four",
	)

	waitFor(t, func() bool { return len(exp.get()) == 4 }, "all 4 records after rewind")
	got := exp.get()
	for i, want := range []string{"one", "two", "three", "four"} {
		if got[i] != want {
			t.Fatalf("records = %q (want ordered, exactly once)", got)
		}
	}
}

// gatedExporter blocks ExportLogs until released, so a test can hold an
// export in flight while the tailer keeps working.
type gatedExporter struct {
	inner   *fakeExporter
	started chan struct{}
	release chan struct{}
}

func (g *gatedExporter) ExportLogs(ctx context.Context, ld plog.Logs) error {
	select {
	case g.started <- struct{}{}:
	default:
	}
	<-g.release
	return g.inner.ExportLogs(ctx, ld)
}

// A rotation detected while an export is in flight settles the export first;
// records from both generations arrive exactly once, in order.
func TestPipelinedRotationSettles(t *testing.T) {
	dir := t.TempDir()
	inner := &fakeExporter{}
	exp := &gatedExporter{inner: inner, started: make(chan struct{}, 1), release: make(chan struct{})}
	tl := newPipelinedTailer(dir, "", exp)
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir,
		timeNowCRI()+" stdout F old-one",
		timeNowCRI()+" stdout F old-two",
	)
	// Wait until the first export is actually in flight.
	select {
	case <-exp.started:
	case <-time.After(5 * time.Second):
		t.Fatal("export never started")
	}

	// Rotate while the export is held: rename away and write a new file.
	path := filepath.Join(dir, logName)
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir,
		timeNowCRI()+" stdout F new-one",
		timeNowCRI()+" stdout F new-two",
	)
	// Let the sweep hit the rotation (it settles: blocks on the gate).
	time.Sleep(200 * time.Millisecond)
	close(exp.release)

	waitFor(t, func() bool { return len(inner.get()) == 4 }, "both generations")
	got := inner.get()
	for i, want := range []string{"old-one", "old-two", "new-one", "new-two"} {
		if got[i] != want {
			t.Fatalf("records = %q (want both generations ordered, exactly once)", got)
		}
	}
}
