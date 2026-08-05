// Tests for offset persistence (checkpoint.go + agent/positions).
package tailer

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
)

func TestPositionsStoreResume(t *testing.T) {
	dir := t.TempDir()
	posPath := filepath.Join(dir, "positions.json")
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.cfg.Positions, _ = positions.Open(posPath)
	stop := startTailer(t, tl)
	writeLog(t, dir,
		"2026-07-05T10:00:00Z stdout F one",
		"2026-07-05T10:00:01Z stdout F two",
	)
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "2 records")
	stop() // shutdown persists offsets to the positions store

	// The positions file recorded the offset; it is not empty.
	if st, _ := positions.Open(posPath); len(st.Logs()) == 0 {
		t.Fatal("positions file has no log offsets after save")
	}

	// A fresh tailer sharing the positions store resumes and does not re-read.
	exp2 := &fakeExporter{}
	tl2 := newTestTailer(dir, "", exp2)
	tl2.cfg.Positions, _ = positions.Open(posPath)
	stop2 := startTailer(t, tl2)
	defer stop2()
	writeLog(t, dir, "2026-07-05T10:00:02Z stdout F three")
	waitFor(t, func() bool { return len(exp2.get()) == 1 }, "only the new record")
	if got := exp2.get(); got[0] != "three" {
		t.Fatalf("resumed tailer re-read: %v", got)
	}
}

func TestCheckpointResume(t *testing.T) {
	dir := t.TempDir()
	cp := filepath.Join(t.TempDir(), "checkpoints.json")

	// First run: consume two lines, checkpoint on shutdown.
	exp1 := &fakeExporter{}
	tl1 := newTestTailer(dir, cp, exp1)
	stop1 := startTailer(t, tl1)
	writeLog(t, dir,
		"2026-07-05T10:00:00Z stdout F one",
		"2026-07-05T10:00:01Z stdout F two",
	)
	waitFor(t, func() bool { return len(exp1.get()) == 2 }, "first run records")
	stop1()

	// Data written while the agent is down.
	writeLog(t, dir, "2026-07-05T10:00:02Z stdout F three")

	// Second run resumes from the checkpoint: only the new line arrives.
	exp2 := &fakeExporter{}
	tl2 := newTestTailer(dir, cp, exp2)
	stop2 := startTailer(t, tl2)
	defer stop2()
	waitFor(t, func() bool { return len(exp2.get()) == 1 }, "resumed record")
	if exp2.get()[0] != "three" {
		t.Fatalf("records = %v (history re-ingested?)", exp2.get())
	}
}

// TestNewFileCheckpointedOnDiscovery pins the crash-window fix: a newly
// discovered file must have a checkpoint entry persisted immediately, so a
// kill -9 before the 10s periodic save cannot make the restart treat it as
// pre-existing history (skip-to-end = silent loss of its unread lines).
func TestNewFileCheckpointedOnDiscovery(t *testing.T) {
	dir := t.TempDir()
	chk := filepath.Join(t.TempDir(), "chk")
	exp := &fakeExporter{}
	tl := newTestTailer(dir, chk, exp)
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, timeNowCRI()+" stdout F hello")
	waitFor(t, func() bool {
		data, err := os.ReadFile(chk)
		if err != nil {
			return false
		}
		return strings.Contains(string(data), logName)
	}, "checkpoint entry persisted at discovery, before the periodic save")
}

// TestSaveKeepsUnappliedCheckpointForUnstattablePath pins the on-disk half of
// the transient-stat protection. claimPath keeps a stored-but-unapplied entry
// whose path fails stat with a non-ENOENT error (ELOOP here; EIO, EACCES) out
// of the in-memory prune, because its absence is unproven — and saveCheckpoints
// must agree: it used to rebuild the on-disk doc from t.files alone, so the
// immediate save triggered by discovering ANY other file destroyed the
// protected entry's offset (and its Pending segments) on disk, and a crash
// before the path recovered lost them for good.
func TestSaveKeepsUnappliedCheckpointForUnstattablePath(t *testing.T) {
	dir := t.TempDir()
	posPath := filepath.Join(t.TempDir(), "positions.json")

	loop := filepath.Join(dir, "loop.log")
	good := filepath.Join(dir, "app.log")

	// Seed a positions file holding a previous run's offset for loop.log.
	seed := map[string]any{"logs": map[string]any{loop: map[string]any{"offset": 5, "inode": 42}}}
	data, err := json.Marshal(seed)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(posPath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	// loop.log fails stat with ELOOP (a self-referential symlink) — transient
	// in the sense that it is not ENOENT; app.log is fine.
	if err := os.Symlink(loop, loop); err != nil {
		t.Fatal(err)
	}
	writeLines(t, good, "hello")

	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{{
		Name:    "plain",
		Include: []string{filepath.Join(dir, "*.log")},
	}}, false)
	tl.cfg.Positions = mustOpenPositions(t, posPath)

	// The initial scan lists both names (the glob sees the symlink), discovers
	// app.log — which persists immediately — and fails to stat loop.log.
	tl.scanDir(tl.loadCheckpoints(), true)

	if !tl.lastListingOK {
		t.Fatal("precondition: the listing should be OK (the glob lists the symlink by name)")
	}
	if _, ok := tl.checkpoints[loop]; !ok {
		t.Fatal("precondition: the unapplied entry must survive the in-memory prune")
	}
	if _, ok := tl.files[good]; !ok {
		t.Fatal("precondition: app.log should be tracked")
	}

	// scanDir already ran saveCheckpoints (discovered && checkpointing); the
	// stored offset must still be on disk.
	logs := mustOpenPositions(t, posPath).Logs()
	if _, ok := logs[loop]; !ok {
		t.Fatalf("stored offset destroyed by save: on-disk positions = %v", logs)
	}
	if _, ok := logs[good]; !ok {
		t.Fatalf("discovered file missing from save: on-disk positions = %v", logs)
	}
}

// A corrupt positions file must not wedge startup: it loads as empty (files
// treated as checkpoint-less) and the next save overwrites it.
func TestCorruptPositionsFileIgnored(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	ckpt := filepath.Join(dir, "ckpt.json")
	if err := os.WriteFile(ckpt, []byte("{garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Positions = mustOpenPositions(t, ckpt)

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
