package tailer

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// A file deleted DURING a collector outage, after a rotation left a carried
// prefix, must still deliver the prefix's lines once the collector recovers:
// the gone path never reads the file again, so it must feed the carried
// prefixes itself, and settledGone must not release the retained fds while the
// prefix is unexported.
func TestGoneFileDeliversCarriedPrefix(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	path := filepath.Join(dir, logName)

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F one")
	tl.scanDir(nil, false)

	exp.fail = 3 * 4 // the next four flushes (3 attempts each) fail

	tl.sweep(ctx, true) // reads "one"
	tl.flush(ctx)       // FAILS -> rewind

	// Rename rotation while the collector is down: inode A becomes a carried
	// prefix, inode B holds "two".
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F two")
	tl.sweep(ctx, true) // re-reads "one", rotation -> carried=[A]
	tl.flush(ctx)       // FAILS -> rewind (re-arms carriedFed)
	if len(tl.files[path].carried) != 1 {
		t.Fatalf("setup: carried = %+v, want the rotated-away inode A", tl.files[path].carried)
	}

	// Another sweep opens inode B and reads "two" (fd now held), still failing.
	tl.sweep(ctx, true)
	tl.flush(ctx) // FAILS -> rewind
	if f := tl.files[path]; f.f == nil {
		t.Fatal("setup: inode B's fd not held before deletion")
	}

	// The pod is deleted mid-outage: both the live file and the rotated copy
	// vanish by NAME; only the retained fds still reach the bytes.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path + ".1"); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false) // marks it gone

	// Drain + recover: everything must ship.
	for i := 0; i < 5; i++ {
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}
	got := exp.get()
	for _, want := range []string{"one", "two"} {
		if !slices.Contains(got, want) {
			t.Fatalf("line %q lost after deletion during outage; exported = %v", want, got)
		}
	}
}

// A deleted plain file with an UNTERMINATED final line must deliver the tail
// and settle — settledGone's pending check would otherwise hold the fd and the
// files-map entry forever (the missing newline can never arrive).
func TestGoneFileUnterminatedTailDeliversAndSettles(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{{
		Name:    "plain",
		Include: []string{filepath.Join(dir, "*.txt")},
	}}, false)
	path := filepath.Join(dir, "app.txt")

	tl.scanDir(tl.loadCheckpoints(), true)
	if err := os.WriteFile(path, []byte("done line\nunterminated tail"), 0o644); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false) // gone
	for i := 0; i < 4; i++ {
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}

	got := exp.get()
	if !slices.Contains(got, "unterminated tail") {
		t.Fatalf("unterminated tail lost: %v", got)
	}
	if _, tracked := tl.files[path]; tracked {
		t.Fatal("gone file with unterminated tail never settled (fd/map leak)")
	}
}
