package tailer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
)

type fakeMeta struct{}

func (fakeMeta) Container(_ context.Context, id string, _ time.Duration) (*kubemeta.ContainerMetadata, error) {
	return &kubemeta.ContainerMetadata{
		ContainerID: id,
		Container:   kubemeta.Container{Name: "app", ID: id},
		Pod:         kubemeta.Pod{Name: "pod1", Namespace: "ns1", UID: "uid1", NodeName: "node1"},
	}, nil
}

type fakeExporter struct {
	mu      sync.Mutex
	fail    int // fail this many exports before succeeding
	records []string
	batches int
}

func (f *fakeExporter) ExportLogs(_ context.Context, ld plog.Logs) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail > 0 {
		f.fail--
		return errors.New("collector down")
	}
	f.batches++
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			lrs := rl.ScopeLogs().At(j).LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				f.records = append(f.records, lrs.At(k).Body().Str())
			}
		}
	}
	return nil
}

func (f *fakeExporter) get() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.records...)
}

const logName = "pod1_ns1_app-0123456789abcdef.log"

func writeLog(t *testing.T, dir string, lines ...string) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, logName), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, l := range lines {
		if _, err := fmt.Fprintln(f, l); err != nil {
			t.Fatal(err)
		}
	}
}

func newTestTailer(dir, checkpoint string, exp *fakeExporter) *Tailer {
	tl := New(Config{
		Dir:            dir,
		CheckpointFile: checkpoint,
		PollInterval:   20 * time.Millisecond,
		FlushInterval:  50 * time.Millisecond,
		BatchSize:      1000,
		MetadataWait:   time.Second,
		Metadata:       fakeMeta{},
		Exporter:       exp,
	})
	tl.retryBackoff = 10 * time.Millisecond
	return tl
}

// startTailer runs the tailer and returns after its initial directory scan
// has certainly happened, so files created afterwards are treated as new
// (read from the beginning) rather than pre-existing (skipped to the end).
func startTailer(t *testing.T, tl *Tailer) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { tl.Run(ctx); close(done) }()
	time.Sleep(100 * time.Millisecond)
	return func() {
		cancel()
		<-done
	}
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestTailAndExport(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	stop := startTailer(t, tl)
	defer stop()

	// The file appears after the tailer starts, so it is read from the top.
	writeLog(t, dir,
		"2026-07-05T10:00:00Z stdout F hello",
		"2026-07-05T10:00:01Z stderr F oops",
		"2026-07-05T10:00:02Z stdout P multi ",
		"2026-07-05T10:00:03Z stdout F line",
	)
	waitFor(t, func() bool { return len(exp.get()) == 3 }, "3 log records")

	got := exp.get()
	if got[0] != "hello" || got[1] != "oops" || got[2] != "multi line" {
		t.Fatalf("records = %v", got)
	}

	// Appends are picked up incrementally.
	writeLog(t, dir, "2026-07-05T10:00:04Z stdout F more")
	waitFor(t, func() bool { return len(exp.get()) == 4 }, "4th record")
}

func TestExportFailureRewinds(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{fail: 3} // one full retry cycle fails
	tl := newTestTailer(dir, "", exp)
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F precious")
	// After the failed cycle the file is rewound and re-read; the next
	// export succeeds and the record must not be lost.
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "record after retry")
	if exp.get()[0] != "precious" {
		t.Fatalf("records = %v", exp.get())
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

func TestPreexistingFileStartsAtEnd(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, "2026-07-05T09:59:59Z stdout F history")

	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F fresh")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "fresh record")
	if exp.get()[0] != "fresh" {
		t.Fatalf("records = %v", exp.get())
	}
}

func TestRotation(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F before")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "pre-rotation record")

	// Rotate: rename away and write a fresh file at the same path.
	path := filepath.Join(dir, logName)
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F after")
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "post-rotation record")
	if got := exp.get(); got[1] != "after" {
		t.Fatalf("records = %v", got)
	}
}

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

func TestParseFileName(t *testing.T) {
	id, ns, ok := parseFileName("mypod_myns_app-abc123.log")
	if !ok || id != "abc123" || ns != "myns" {
		t.Fatalf("id=%q ns=%q ok=%v", id, ns, ok)
	}
	for _, bad := range []string{"noext", "nodash.log", "trailing-.log"} {
		if _, _, ok := parseFileName(bad); ok {
			t.Errorf("parseFileName(%q) should fail", bad)
		}
	}
}

func TestExcludeNamespaces(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.cfg.ExcludeNamespaces = []string{"ns1"}
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F excluded")
	time.Sleep(3 * time.Second) // > dir rescan interval
	if got := exp.get(); len(got) != 0 {
		t.Fatalf("excluded namespace produced records: %v", got)
	}
}
