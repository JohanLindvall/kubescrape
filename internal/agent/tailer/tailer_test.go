package tailer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/logattrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
	"github.com/JohanLindvall/kubescrape/internal/obs"
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
	defer func() { _ = f.Close() }()
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

func TestFileAttributes(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.cfg.FileAttributes = true
	stop := startTailer(t, tl)
	defer stop()

	line0 := `2026-07-05T10:00:00Z stdout F hello`
	writeLog(t, dir, line0, `2026-07-05T10:00:01Z stdout F world`)
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "2 records")

	// log.file.position is the record's start: record 0 begins at 0, record 1
	// just after the first physical line (its bytes + newline).
	for i, want := range []int64{0, int64(len(line0) + 1)} {
		lr, ok := exp.record(i)
		if !ok {
			t.Fatalf("record %d missing", i)
		}
		if name, ok := lr.Attributes().Get("log.file.name"); !ok || name.Str() != logName {
			t.Errorf("record %d log.file.name = %v, want %s", i, name.AsRaw(), logName)
		}
		if pos, ok := lr.Attributes().Get("log.file.position"); !ok || pos.Int() != want {
			t.Errorf("record %d log.file.position = %v, want %d", i, pos.AsRaw(), want)
		}
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
	defer func() { _ = f.Close() }()
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

	exp := &fakeExporter{}
	tl := newMultilineTailer(dir, "", exp, mustOpenPositions(t, posPath))
	stop := startTailer(t, tl)
	defer stop()

	waitFor(t, func() bool { j, _ := panicRecords(exp); return j == 1 },
		"trace reconstructed from rotated prefix + new inode")
	if j, n := panicRecords(exp); j != 1 || n != 1 {
		t.Fatalf("prefix recovery joined=%d count=%d (want 1/1): %q", j, n, exp.get())
	}
}

// panicLines3 splits a Go panic into three parts for a double rename rotation.
func panicLines3() (a, b, c []string) {
	ts := timeNowCRI()
	a = []string{
		ts + " stderr F panic: boom",
		ts + " stderr F ",
	}
	b = []string{
		ts + " stderr F goroutine 1 [running]:",
		ts + " stderr F main.main()",
	}
	c = []string{
		ts + " stderr F \t/app/main.go:10 +0x20",
		ts + " stdout F normal line",
	}
	return a, b, c
}

func TestMultilineJoinsAcrossDoubleRotation(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newMultilineTailer(dir, "", exp, nil)
	stop := startTailer(t, tl)
	defer stop()

	path := filepath.Join(dir, logName)
	writeLog(t, dir, timeNowCRI()+" stdout F first")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "warmup record")

	// The trace straddles two rename rotations: part A in inode0, B in inode1,
	// C (with the terminator) in inode2.
	a, b, c := panicLines3()
	base := obs.LogRotations.Value()
	writeLines(t, path, a...)
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeLines(t, path, b...)
	// The join only works across rotations the tailer actually observes (it
	// follows the symlink, not every intermediate rotated file). Wait until it
	// has processed the first rotation, then let it read B from inode1 before
	// rotating again, so the group genuinely spans two hops. (Reading B into
	// the still-open group can't be observed via an emitted record, so settle
	// briefly — well under the 3s multiline timeout.)
	waitFor(t, func() bool { return obs.LogRotations.Value() >= base+1 }, "first rotation observed")
	time.Sleep(300 * time.Millisecond)
	if err := os.Rename(path, path+".2"); err != nil {
		t.Fatal(err)
	}
	writeLines(t, path, c...)

	waitFor(t, func() bool { j, _ := panicRecords(exp); return j == 1 }, "trace joined across two rotations")
	if j, n := panicRecords(exp); j != 1 || n != 1 {
		t.Fatalf("panic joined=%d count=%d (want 1/1 across two rotations): %q", j, n, exp.get())
	}
}

func TestMultilineRecoversAcrossDoubleRotation(t *testing.T) {
	dir := t.TempDir()
	a, b, c := panicLines3()

	// Two rotated-away files hold the first two segments of the trace.
	old1 := filepath.Join(dir, "0.log.1")
	old2 := filepath.Join(dir, "0.log.2")
	writeLines(t, old1, a...)
	writeLines(t, old2, b...)
	writeLines(t, filepath.Join(dir, logName), c...) // current inode

	pend := func(path string) positions.Prefix {
		st, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		fh, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		fp, err := computeFingerprint(fh, min(1024, st.Size()))
		_ = fh.Close()
		if err != nil {
			t.Fatal(err)
		}
		return positions.Prefix{Inode: inodeOf(st), FingerprintLen: fp.Len, FingerprintHash: fp.Hash, From: 0, To: st.Size()}
	}

	posPath := filepath.Join(dir, "positions.json")
	seed, _ := positions.Open(posPath)
	if err := seed.SetLogs(map[string]positions.LogPos{
		filepath.Join(dir, logName): {
			// Oldest first — the order they must be replayed.
			Pending: []positions.Prefix{pend(old1), pend(old2)},
		},
	}); err != nil {
		t.Fatal(err)
	}

	exp := &fakeExporter{}
	tl := newMultilineTailer(dir, "", exp, mustOpenPositions(t, posPath))
	stop := startTailer(t, tl)
	defer stop()

	waitFor(t, func() bool { j, _ := panicRecords(exp); return j == 1 },
		"trace reconstructed from two rotated prefixes + new inode")
	if j, n := panicRecords(exp); j != 1 || n != 1 {
		t.Fatalf("double-rotation recovery joined=%d count=%d (want 1/1): %q", j, n, exp.get())
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
	_, _ = fmt.Fprintln(old, "2026-07-05T10:00:01Z stdout F late")
	_ = old.Close()
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

// A deleted file is drained (bytes appended since the last read are exported)
// and then dropped from tracking.
func TestFileDeletionDrainsAndDrops(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, timeNowCRI()+" stdout F before-delete")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "first record")

	// Append one more line and remove the file before the next sweep is
	// guaranteed to have read it — the drop path must drain the fd first.
	writeLog(t, dir, timeNowCRI()+" stdout F final-words")
	if err := os.Remove(filepath.Join(dir, logName)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		for _, r := range exp.get() {
			if r == "final-words" {
				return true
			}
		}
		return false
	}, "final line drained after deletion")

	// Exactly the two records, no duplicates from the drain.
	got := exp.get()
	if len(got) != 2 || got[0] != "before-delete" || got[1] != "final-words" {
		t.Fatalf("records = %q", got)
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

// TestRotationDrainsFullBacklog: a rename rotation must drain the entire
// unread remainder of the rotated-away inode, not a byte-budgeted prefix.
func TestRotationDrainsFullBacklog(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, filepath.Join(t.TempDir(), "chk"), exp)
	tl.cfg.MaxBytesPerSweep = 1024 // the old budget would abandon most of the backlog
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, timeNowCRI()+" stdout F first")
	waitFor(t, func() bool { return len(exp.get()) >= 1 }, "first line exported")

	// Build a backlog far over 4*MaxBytesPerSweep, then rotate before the
	// tailer can catch up.
	var lines []string
	for i := 0; i < 120; i++ {
		lines = append(lines, timeNowCRI()+" stdout F backlog-"+strings.Repeat("y", 100)+"-"+strconv.Itoa(i))
	}
	writeLog(t, dir, lines...)
	if err := os.Rename(filepath.Join(dir, logName), filepath.Join(dir, logName+".1")); err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, timeNowCRI()+" stdout F post-rotate")

	waitFor(t, func() bool {
		recs := exp.get()
		n := 0
		for _, r := range recs {
			if strings.HasPrefix(r, "backlog-") {
				n++
			}
		}
		return n == 120
	}, "all 120 backlog lines exported")
}

func mustOpenPositions(t *testing.T, path string) *positions.Store {
	t.Helper()
	s, err := positions.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestDeferredCRIEmissionOffsets pins the closed-run ledger fix: the stage
// defers a multi-fragment run's emission until the next line for the key is
// fed, so the entry's commit offset must be the F line's end (not the
// triggering line's), and the triggering P fragment must keep watermark
// coverage (previously the callback stole its registration).
func TestDeferredCRIEmissionOffsets(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	f := &file{
		path:        filepath.Join(dir, logName),
		source:      &compiledSource{name: "containers", containerd: true},
		containerID: "0123456789abcdef",
		resolved:    true,
		resource:    pcommon.NewResource(),
	}
	tl.newPipeline(f)
	tl.files[f.path] = f

	l1 := timeNowCRI() + " stdout P hello-"
	l2 := timeNowCRI() + " stdout F world"
	l3 := timeNowCRI() + " stdout P dangling-"
	ctx := context.Background()
	off := int64(0)
	for _, l := range []string{l1, l2, l3} {
		end := off + int64(len(l)) + 1
		tl.feedLine(ctx, f, l, off, end)
		off = end
	}
	endF := int64(len(l1) + len(l2) + 2)

	// Feeding l3 flushed the closed run: exactly one entry, bounded by the
	// run's own lines.
	if len(tl.batch) != 1 {
		t.Fatalf("batch entries: %d", len(tl.batch))
	}
	e := tl.batch[0]
	if e.body != "hello-world" {
		t.Fatalf("body %q", e.body)
	}
	if e.start != 0 || e.offset != endF {
		t.Fatalf("entry range [%d,%d), want [0,%d)", e.start, e.offset, endF)
	}
	// The dangling fragment must still clamp the watermark.
	wm, ok := f.watermark()
	if !ok || wm != endF {
		t.Fatalf("watermark = %d,%v, want %d,true (fragment lost coverage)", wm, ok, endF)
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

// TestUnknownFileAutoReadsFromStart pins the "auto" unknown-files semantics:
// when the checkpoint store already has entries (the agent ran before), a
// file present at startup without an entry appeared while the agent was down
// — its content is unshipped, not history, and must be read from the start.
func TestUnknownFileAutoReadsFromStart(t *testing.T) {
	dir := t.TempDir()
	chk := filepath.Join(t.TempDir(), "chk")

	// First run: establish a checkpoint entry for one file.
	exp1 := &fakeExporter{}
	tl1 := newTestTailer(dir, chk, exp1)
	stop1 := startTailer(t, tl1)
	writeLog(t, dir, timeNowCRI()+" stdout F first-run")
	waitFor(t, func() bool { return len(exp1.get()) >= 1 }, "first run exported")
	stop1()

	// While "down": a NEW file appears with content.
	otherName := "pod2_ns1_app-fedcba9876543210.log"
	other := filepath.Join(dir, otherName)
	if err := os.WriteFile(other, []byte(timeNowCRI()+" stdout F while-down\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Second run (auto default): the unknown file must be read from offset 0.
	exp2 := &fakeExporter{}
	tl2 := newTestTailer(dir, chk, exp2)
	stop2 := startTailer(t, tl2)
	defer stop2()
	waitFor(t, func() bool {
		for _, r := range exp2.get() {
			if r == "while-down" {
				return true
			}
		}
		return false
	}, "content written while down is shipped")
}

// rotateAway renames the live log file aside (the first half of a kubelet
// rename+recreate rotation), leaving the path momentarily absent.
func rotateAway(t *testing.T, dir string, gen int) {
	t.Helper()
	path := filepath.Join(dir, logName)
	if err := os.Rename(path, fmt.Sprintf("%s.%d", path, gen)); err != nil {
		t.Fatal(err)
	}
}

// TestListingDuringRotationDoesNotDropFile reproduces a directory listing
// racing a rename+recreate rotation: scanDir runs in the instant the path is
// absent (between the rename and the recreate) and marks the live file gone.
// A later listing sees the recreated path and must unmark it — otherwise the
// next sweep drops the file with its state and checkpoint, losing every
// inode rotated away before rediscovery.
func TestListingDuringRotationDoesNotDropFile(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	ctx := context.Background()

	tl.scanDir(nil, true) // initial scan: empty dir
	writeLog(t, dir, timeNowCRI()+" stdout F one")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	path := filepath.Join(dir, logName)
	rotateAway(t, dir, 1)
	tl.scanDir(nil, false) // listing in the absent window: marks gone
	writeLog(t, dir, timeNowCRI()+" stdout F two")
	tl.scanDir(nil, false) // path is back: must clear gone

	tl.sweep(ctx, true)
	if _, ok := tl.files[path]; !ok {
		t.Fatal("file dropped after a listing raced the rename+recreate rotation")
	}
	tl.sweep(ctx, true) // reopen marked the file dirty; read the new inode
	tl.flush(ctx)
	got := exp.get()
	if len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("expected [one two] across the rotation, got %v", got)
	}
}

// TestGoneFileBackBeforeSweepSurvives covers the sweep-side guard: the file
// is marked gone by a listing that raced the rotation and NO further listing
// runs before the sweep. The sweep must re-stat the path and, finding it
// alive, keep the file instead of dropping it.
func TestGoneFileBackBeforeSweepSurvives(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	ctx := context.Background()

	tl.scanDir(nil, true)
	writeLog(t, dir, timeNowCRI()+" stdout F one")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	path := filepath.Join(dir, logName)
	rotateAway(t, dir, 1)
	tl.scanDir(nil, false) // marks gone
	writeLog(t, dir, timeNowCRI()+" stdout F two")

	tl.sweep(ctx, true) // no listing between recreate and sweep
	if _, ok := tl.files[path]; !ok {
		t.Fatal("sweep dropped a file whose path was alive again")
	}
	tl.sweep(ctx, true) // reopen marked the file dirty; read the new inode
	tl.flush(ctx)
	got := exp.get()
	if len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("expected [one two] across the rotation, got %v", got)
	}

	// A genuinely deleted file must still be dropped.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	if _, ok := tl.files[path]; ok {
		t.Fatal("genuinely deleted file was not dropped")
	}
}

// TestEventSweepsNotStarvedByContinuousWrites guards the debounce against
// per-event re-arming: a file written more often than the debounce interval
// must still get event-driven sweeps (the poll interval here is far too long
// to deliver anything within the test deadline). With per-event Reset the
// debounce timer never fires under sustained writes, sweeps degrade to the
// poll fallback, and sub-poll-interval rename rotations silently lose whole
// segments.
func TestEventSweepsNotStarvedByContinuousWrites(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := New(Config{
		Dir:           dir,
		Watch:         true,
		PollInterval:  time.Hour, // events must carry the test alone
		FlushInterval: 50 * time.Millisecond,
		BatchSize:     1000,
		MetadataWait:  time.Second,
		Metadata:      fakeMeta{},
		Exporter:      exp,
	})
	tl.retryBackoff = 10 * time.Millisecond
	stop := startTailer(t, tl)
	defer stop()

	// Continuous writes: an event at least every few milliseconds.
	writerCtx, cancelWriter := context.WithCancel(context.Background())
	defer cancelWriter()
	go func() {
		for i := 0; writerCtx.Err() == nil; i++ {
			writeLog(t, dir, timeNowCRI()+" stdout F line"+strconv.Itoa(i))
			time.Sleep(2 * time.Millisecond)
		}
	}()

	waitFor(t, func() bool { return len(exp.get()) > 0 }, "event-driven sweep exports under sustained writes")
}

// TestIdleCloseReleasesAndReopens: a fully-caught-up idle file's fd closes
// after IdleClose, and the file transparently reopens and resumes on new
// activity without loss or duplication.
func TestIdleCloseReleasesAndReopens(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, filepath.Join(t.TempDir(), "chk"), exp)
	tl.cfg.IdleClose = 200 * time.Millisecond
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, timeNowCRI()+" stdout F before-idle")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "first line exported")

	// Age the file's mtime past IdleClose so housekeeping closes the fd.
	old := time.Now().Add(-time.Minute)
	if err := os.Chtimes(filepath.Join(dir, logName), old, old); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond) // let housekeeping close the idle fd

	writeLog(t, dir, timeNowCRI()+" stdout F after-idle")
	waitFor(t, func() bool {
		recs := exp.get()
		return len(recs) == 2 && recs[1] == "after-idle"
	}, "file reopened and resumed after idle close")
}
