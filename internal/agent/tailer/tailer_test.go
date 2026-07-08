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

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/logattrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
)

type fakeMeta struct{}

func (fakeMeta) Container(_ context.Context, id string, _ time.Duration) (*kubemeta.ContainerMetadata, error) {
	return &kubemeta.ContainerMetadata{
		ContainerID: id,
		Container:   kubemeta.Container{Name: "app", ID: id},
		Pod: kubemeta.Pod{
			Name: "pod1", Namespace: "ns1", UID: "uid1", NodeName: "node1",
			Labels: map[string]string{"app": "x"},
		},
	}, nil
}

type fakeExporter struct {
	mu       sync.Mutex
	fail     int // fail this many exports before succeeding
	records  []string
	full     []plog.Logs    // deep copies of the exported batches
	resAttrs map[string]any // resource attributes of the last batch
	batches  int
}

func (f *fakeExporter) ExportLogs(_ context.Context, ld plog.Logs) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail > 0 {
		f.fail--
		return errors.New("collector down")
	}
	f.batches++
	cp := plog.NewLogs()
	ld.CopyTo(cp)
	f.full = append(f.full, cp)
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		f.resAttrs = rl.Resource().Attributes().AsRaw()
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

// record returns the exported log record at global index i.
func (f *fakeExporter) record(i int) (plog.LogRecord, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, ld := range f.full {
		for r := 0; r < ld.ResourceLogs().Len(); r++ {
			rl := ld.ResourceLogs().At(r)
			for s := 0; s < rl.ScopeLogs().Len(); s++ {
				lrs := rl.ScopeLogs().At(s).LogRecords()
				for k := 0; k < lrs.Len(); k++ {
					if n == i {
						return lrs.At(k), true
					}
					n++
				}
			}
		}
	}
	return plog.LogRecord{}, false
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

func TestEnrichedRecords(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.cfg.Enrich = true
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir,
		`2026-07-05T10:00:00Z stdout F {"@t":"2026-01-02T03:04:05Z","level":"error","traceid":"0af7651916cd43dd8448eb211c80319c","msg":"boom"}`,
		"2026-07-05T10:00:01Z stdout F plain line",
	)
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "2 log records")

	lr, ok := exp.record(0)
	if !ok {
		t.Fatal("record 0 missing")
	}
	if lr.SeverityNumber() != plog.SeverityNumberError || lr.SeverityText() != "error" {
		t.Errorf("severity = %v %q", lr.SeverityNumber(), lr.SeverityText())
	}
	if !lr.Timestamp().AsTime().Equal(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("timestamp = %v; want the line's own", lr.Timestamp().AsTime())
	}
	if lr.TraceID().IsEmpty() {
		t.Error("trace id not set")
	}

	// The plain line keeps the CRI timestamp and default severity.
	lr, ok = exp.record(1)
	if !ok {
		t.Fatal("record 1 missing")
	}
	if !lr.Timestamp().AsTime().Equal(time.Date(2026, 7, 5, 10, 0, 1, 0, time.UTC)) {
		t.Errorf("plain-line timestamp = %v; want the CRI one", lr.Timestamp().AsTime())
	}
	if lr.SeverityNumber() != plog.SeverityNumberUnspecified {
		t.Errorf("plain-line severity = %v", lr.SeverityNumber())
	}
}

func TestLogAttrsGrouping(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	ex, err := logattrs.New(&logattrs.Config{Rules: []logattrs.Rule{
		{Key: "tenant", Attribute: "tenant.id", Target: logattrs.TargetResource},
		{Key: "req", Target: logattrs.TargetLog},
	}})
	if err != nil {
		t.Fatal(err)
	}
	tl.cfg.LogAttrs = ex
	stop := startTailer(t, tl)
	defer stop()

	// Two lines for tenant A, one for tenant B, one non-structured — the
	// tenant attribute is a resource attribute, so A and B must land in
	// separate ResourceLogs.
	writeLog(t, dir,
		`2026-07-05T10:00:00Z stdout F {"tenant":"a","req":"r1"}`,
		`2026-07-05T10:00:01Z stdout F {"tenant":"b","req":"r2"}`,
		`2026-07-05T10:00:02Z stdout F {"tenant":"a","req":"r3"}`,
		`2026-07-05T10:00:03Z stdout F plain line`,
	)
	waitFor(t, func() bool { return len(exp.get()) == 4 }, "4 records")

	exp.mu.Lock()
	tenantCounts := map[string]int{}
	for _, ld := range exp.full {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			rl := ld.ResourceLogs().At(i)
			tenant := "<none>"
			if v, ok := rl.Resource().Attributes().Get("tenant.id"); ok {
				tenant = v.Str()
			}
			n := 0
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				n += rl.ScopeLogs().At(j).LogRecords().Len()
			}
			tenantCounts[tenant] += n
		}
	}
	exp.mu.Unlock() // record() below locks exp.mu itself
	if tenantCounts["a"] != 2 || tenantCounts["b"] != 1 || tenantCounts["<none>"] != 1 {
		t.Errorf("tenant record counts = %+v", tenantCounts)
	}
	// The log-target attribute lands on the record.
	lr, ok := exp.record(0)
	if !ok {
		t.Fatal("record 0 missing")
	}
	if v, ok := lr.Attributes().Get("req"); !ok || v.Str() != "r1" {
		t.Errorf("req attribute = %v", v.AsRaw())
	}
}

func TestPositionsStoreResume(t *testing.T) {
	dir := t.TempDir()
	posPath := filepath.Join(dir, "positions.json")
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.cfg.Positions = positions.Open(posPath)
	stop := startTailer(t, tl)
	writeLog(t, dir,
		"2026-07-05T10:00:00Z stdout F one",
		"2026-07-05T10:00:01Z stdout F two",
	)
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "2 records")
	stop() // shutdown persists offsets to the positions store

	// The positions file recorded the offset; it is not empty.
	if len(positions.Open(posPath).Logs()) == 0 {
		t.Fatal("positions file has no log offsets after save")
	}

	// A fresh tailer sharing the positions store resumes and does not re-read.
	exp2 := &fakeExporter{}
	tl2 := newTestTailer(dir, "", exp2)
	tl2.cfg.Positions = positions.Open(posPath)
	stop2 := startTailer(t, tl2)
	defer stop2()
	writeLog(t, dir, "2026-07-05T10:00:02Z stdout F three")
	waitFor(t, func() bool { return len(exp2.get()) == 1 }, "only the new record")
	if got := exp2.get(); got[0] != "three" {
		t.Fatalf("resumed tailer re-read: %v", got)
	}
}

func newMultilineTailer(dir, checkpoint string, exp *fakeExporter, pos *positions.Store) *Tailer {
	tl := New(Config{
		Dir:              dir,
		CheckpointFile:   checkpoint,
		Positions:        pos,
		PollInterval:     20 * time.Millisecond,
		FlushInterval:    30 * time.Millisecond,
		BatchSize:        1000,
		Multiline:        true,
		MultilineTimeout: 3 * time.Second,
		MetadataWait:     time.Second,
		Metadata:         fakeMeta{},
		Exporter:         exp,
	})
	tl.retryBackoff = 10 * time.Millisecond
	return tl
}

func writeLines(t *testing.T, path string, lines ...string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
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

// panicLines builds a Go panic to split across a rename rotation: the first
// frames go in the old inode, the rest (and the terminating line) in the new
// inode. The CRI timestamps must be near now, or the multiline stage's
// age-out (FlushBefore against the real clock) flushes the group before the
// continuation arrives.
func panicLines() (start, rest []string) {
	ts := timeNowCRI()
	start = []string{
		ts + " stderr F panic: boom",
		ts + " stderr F ",
		ts + " stderr F goroutine 1 [running]:",
	}
	rest = []string{
		ts + " stderr F main.main()",
		ts + " stderr F \t/app/main.go:10 +0x20",
		ts + " stdout F normal line",
	}
	return start, rest
}

func timeNowCRI() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func panicRecords(exp *fakeExporter) (joined, count int) {
	for _, r := range exp.get() {
		if strings.Contains(r, "panic: boom") {
			count++
			if strings.Contains(r, "main.go:10") {
				joined++
			}
		}
	}
	return joined, count
}

func TestMultilineJoinsAcrossRotation(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newMultilineTailer(dir, "", exp, nil)
	stop := startTailer(t, tl)
	defer stop()

	path := filepath.Join(dir, logName)
	// Warm up so the file is open (its fd survives the rename below).
	writeLog(t, dir, timeNowCRI()+" stdout F first")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "warmup record")

	// The trace begins in the current inode, then the file is rotated away and
	// the continuation lands in a fresh inode — the group straddles both.
	start, rest := panicLines()
	writeLines(t, path, start...)
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeLines(t, path, rest...)

	waitFor(t, func() bool { j, _ := panicRecords(exp); return j == 1 }, "trace joined across rotation")
	if j, n := panicRecords(exp); j != 1 || n != 1 {
		t.Fatalf("panic joined=%d count=%d (want 1/1 — not split): %q", j, n, exp.get())
	}
}

func TestMultilineRecoversPrefixAfterRestart(t *testing.T) {
	dir := t.TempDir()

	// The rotated-away file holds the start of a trace.
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

	// The current file holds the continuation and the terminating line.
	writeLines(t, filepath.Join(dir, logName), rest...)

	// A checkpoint as a crash would have left it: committed 0 on the new inode,
	// with a pending prefix naming the rotated file.
	posPath := filepath.Join(dir, "positions.json")
	seed := positions.Open(posPath)
	if err := seed.SetLogs(map[string]positions.LogPos{
		filepath.Join(dir, logName): {
			Offset: 0, Inode: 0,
			Pending: &positions.Prefix{
				Inode: inodeOfPath(t, oldPath), FingerprintLen: oldFp.Len, FingerprintHash: oldFp.Hash,
				From: 0, To: oldSt.Size(),
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	exp := &fakeExporter{}
	tl := newMultilineTailer(dir, "", exp, positions.Open(posPath))
	stop := startTailer(t, tl)
	defer stop()

	waitFor(t, func() bool { j, _ := panicRecords(exp); return j == 1 },
		"trace reconstructed from rotated prefix + new inode")
	if j, n := panicRecords(exp); j != 1 || n != 1 {
		t.Fatalf("prefix recovery joined=%d count=%d (want 1/1): %q", j, n, exp.get())
	}
}

func inodeOfPath(t *testing.T, path string) uint64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return inodeOf(st)
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

func TestAttrFilter(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	filter, err := attrs.NewFilter("", `k8s\.pod\.label\..*`)
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

func TestRotationDrainsOldFile(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F before")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "pre-rotation record")

	// Rotate: rename away, append a late line to the OLD (renamed) file —
	// mimicking a writer that has not reopened yet — then start the new
	// file. Nothing may be lost.
	path := filepath.Join(dir, logName)
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	old, err := os.OpenFile(path+".1", os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintln(old, "2026-07-05T10:00:01Z stdout F late")
	old.Close()
	writeLog(t, dir, "2026-07-05T10:00:02Z stdout F after")

	waitFor(t, func() bool { return len(exp.get()) == 3 }, "all records across rotation")
	got := exp.get()
	if got[1] != "late" || got[2] != "after" {
		t.Fatalf("records = %v", got)
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

func TestDeletionDrainsRemainder(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F first")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "first record")

	// Stop sweeping long enough to append + delete between sweeps is racy;
	// instead append and remove back to back — whichever sweep sees it, the
	// final drain in drop() must deliver the appended line.
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F last")
	if err := os.Remove(filepath.Join(dir, logName)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "record drained on deletion")
	if got := exp.get(); got[1] != "last" {
		t.Fatalf("records = %v", got)
	}
}

func TestFingerprintGuardsCheckpointResume(t *testing.T) {
	dir := t.TempDir()
	cpDir := t.TempDir()
	cp := filepath.Join(cpDir, "checkpoints.json")
	path := filepath.Join(dir, logName)

	// First run: establish a checkpoint past line one.
	writeLog(t, dir, "2026-07-05T09:00:00Z stdout F old-one", "2026-07-05T09:00:01Z stdout F old-two")
	exp1 := &fakeExporter{}
	tl1 := newTestTailer(dir, cp, exp1)
	tl1.cfg.FingerprintBytes = 64
	stop1 := startTailer(t, tl1)
	// Pre-existing file: skipped to end; append so something commits.
	writeLog(t, dir, "2026-07-05T09:00:02Z stdout F old-three")
	waitFor(t, func() bool { return len(exp1.get()) == 1 }, "first run record")
	stop1()

	// Replace the file with different content of GREATER length than the
	// committed offset (the inode may or may not be reused; the fingerprint
	// must catch the replacement either way).
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir,
		"2026-07-06T09:00:00Z stdout F new-one",
		"2026-07-06T09:00:01Z stdout F new-two",
		"2026-07-06T09:00:02Z stdout F new-three",
		"2026-07-06T09:00:03Z stdout F new-four",
	)

	exp2 := &fakeExporter{}
	tl2 := newTestTailer(dir, cp, exp2)
	tl2.cfg.FingerprintBytes = 64
	stop2 := startTailer(t, tl2)
	defer stop2()
	// A different file must be read from the top, not from the stale offset.
	waitFor(t, func() bool { return len(exp2.get()) == 4 }, "replacement read from zero")
	if got := exp2.get(); got[0] != "new-one" {
		t.Fatalf("records = %v", got)
	}
}

func TestFingerprintAllowsResume(t *testing.T) {
	dir := t.TempDir()
	cp := filepath.Join(t.TempDir(), "checkpoints.json")

	exp1 := &fakeExporter{}
	tl1 := newTestTailer(dir, cp, exp1)
	tl1.cfg.FingerprintBytes = 64
	stop1 := startTailer(t, tl1)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F one", "2026-07-05T10:00:01Z stdout F two")
	waitFor(t, func() bool { return len(exp1.get()) == 2 }, "first run records")
	stop1()

	writeLog(t, dir, "2026-07-05T10:00:02Z stdout F three")
	exp2 := &fakeExporter{}
	tl2 := newTestTailer(dir, cp, exp2)
	tl2.cfg.FingerprintBytes = 64
	stop2 := startTailer(t, tl2)
	defer stop2()
	waitFor(t, func() bool { return len(exp2.get()) == 1 }, "resumed record")
	if got := exp2.get(); got[0] != "three" {
		t.Fatalf("records = %v (history re-ingested?)", got)
	}
}

func TestWatchDeliversWithoutPolling(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.cfg.Watch = true
	tl.cfg.PollInterval = time.Hour // events must carry everything
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F via-events")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "event-driven record")

	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F more-events")
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "second event-driven record")
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
