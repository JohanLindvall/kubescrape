// Shared fixtures and drivers for the tailer tests: the fake metadata
// client and exporter, tailer constructors, file writers and sweep drivers.
package tailer

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"go.opentelemetry.io/collector/pdata/plog"
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
	attempts int // every ExportLogs call, including failed ones
}

func (f *fakeExporter) ExportLogs(_ context.Context, ld plog.Logs) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
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
	writeLines(t, filepath.Join(dir, logName), lines...)
}

func newTestTailer(dir, checkpoint string, exp *fakeExporter) *Tailer {
	var pos *positions.Store
	if checkpoint != "" {
		pos, _ = positions.Open(checkpoint)
	}
	tl := New(Config{
		Dir:           dir,
		Positions:     pos,
		PollInterval:  20 * time.Millisecond,
		FlushInterval: 50 * time.Millisecond,
		BatchSize:     1000,
		MetadataWait:  time.Second,
		Metadata:      fakeMeta{},
		Exporter:      exp,
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

// driveUntil pumps sweep+flush on a synchronously-driven tailer (no Run
// goroutine) until cond holds, failing after 10s. It is the driven twin of
// waitFor, which watches a tailer already running its own sweep loop.
func driveUntil(t *testing.T, ctx context.Context, tl *Tailer, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		tl.sweep(ctx, true)
		tl.flush(ctx)
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out driving until %s", what)
}

func newMultilineTailer(dir, checkpoint string, exp *fakeExporter, pos *positions.Store) *Tailer {
	if pos == nil && checkpoint != "" {
		pos, _ = positions.Open(checkpoint)
	}
	tl := New(Config{
		Dir:              dir,
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

func inodeOfPath(t *testing.T, path string) uint64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return inodeOf(st)
}

func mustOpenPositions(t *testing.T, path string) *positions.Store {
	t.Helper()
	s, err := positions.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// rotateAway renames the live log file aside (the first half of a kubelet
// rename+recreate rotation), leaving the path momentarily absent.
func rotateAway(t *testing.T, dir string, seq int) {
	t.Helper()
	path := filepath.Join(dir, logName)
	if err := os.Rename(path, fmt.Sprintf("%s.%d", path, seq)); err != nil {
		t.Fatal(err)
	}
}

func emitted(tl *Tailer, body string) bool {
	for _, e := range tl.batch {
		if e.body == body {
			return true
		}
	}
	return false
}

func bodies(tl *Tailer) []string {
	var out []string
	for _, e := range tl.batch {
		out = append(out, e.body)
	}
	return out
}

// driveTailer builds a tailer that is driven synchronously by the test (no Run
// goroutine), so the sweep/flush interleavings below are exact.
func driveTailer(dir string, exp *fakeExporter) *Tailer {
	tl := New(Config{
		Dir:           dir,
		PollInterval:  20 * time.Millisecond,
		FlushInterval: time.Millisecond,
		BatchSize:     1 << 20, // never auto-flush mid-sweep; the test flushes
		MetadataWait:  time.Second,
		Metadata:      fakeMeta{},
		Exporter:      exp,
	})
	tl.retryBackoff = time.Millisecond
	return tl
}

// driveMultilineTailer is driveTailer with stack-trace joining enabled and a
// small entry cap — multiline is baked into the compiled sources at New()
// time, so it cannot be toggled on a driveTailer after construction.
func driveMultilineTailer(dir string, exp *fakeExporter) *Tailer {
	tl := New(Config{
		Dir:              dir,
		PollInterval:     20 * time.Millisecond,
		FlushInterval:    time.Millisecond,
		BatchSize:        1 << 20,
		Multiline:        true,
		MultilineTimeout: 50 * time.Millisecond,
		MaxEntryBytes:    64,
		MetadataWait:     time.Second,
		Metadata:         fakeMeta{},
		Exporter:         exp,
	})
	tl.retryBackoff = time.Millisecond
	return tl
}

func newSourceTailer(exp *fakeExporter, sources []Source, multiline bool) *Tailer {
	tl := New(Config{
		Sources:          sources,
		PollInterval:     20 * time.Millisecond,
		FlushInterval:    30 * time.Millisecond,
		BatchSize:        1000,
		Multiline:        multiline,
		MultilineTimeout: 3 * time.Second,
		MetadataWait:     time.Second,
		Metadata:         fakeMeta{},
		NodeInfo:         func() *attrs.NodeInfo { return &attrs.NodeInfo{Name: "node1"} },
		Exporter:         exp,
	})
	tl.retryBackoff = 10 * time.Millisecond
	return tl
}

func writeGzip(t *testing.T, path string, lines ...string) {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	for _, l := range lines {
		if _, err := zw.Write([]byte(l + "\n")); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

// newArchiveTailer builds a tailer over a single compressed (*.log.gz) source
// in dir — the fixture nearly every archive test uses.
func newArchiveTailer(dir string, exp *fakeExporter) *Tailer {
	return newSourceTailer(exp, []Source{{
		Name:    "archives",
		Include: []string{filepath.Join(dir, "*.log.gz")},
	}}, false)
}

// appendGzip appends a fresh gzip member (its own lines) to an existing
// archive — a valid multi-member gzip, head intact, so the file grows without
// its identity changing.
func appendGzip(t *testing.T, path string, lines ...string) {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	for _, l := range lines {
		if _, err := zw.Write([]byte(l + "\n")); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	fh, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fh.Close() }()
	if _, err := fh.Write(buf.Bytes()); err != nil {
		t.Fatal(err)
	}
}

// rateLines writes n single-fragment CRI lines with current timestamps.
func rateLines(t *testing.T, dir string, from, n int) {
	t.Helper()
	lines := make([]string, 0, n)
	for i := 0; i < n; i++ {
		lines = append(lines, fmt.Sprintf("%s stdout F line-%03d", timeNowCRI(), from+i))
	}
	writeLog(t, dir, lines...)
}
