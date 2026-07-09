package tailer

import (
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
)

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

func TestSourceMatches(t *testing.T) {
	s := compileSources([]Source{{
		Include: []string{"/var/log/**/*.log"},
		Exclude: []string{"/var/log/azure/*.log", "/var/log/containers/*.log"},
	}}, "", false)[0]

	for path, want := range map[string]bool{
		"/var/log/syslog.log":             true,
		"/var/log/pods/ns/app/0.log":      true,
		"/var/log/azure/agent.log":        false, // excluded
		"/var/log/containers/web_x.log":   false, // excluded
		"/var/log/syslog":                 false, // not *.log
		"/etc/passwd":                     false, // not included
		"/var/log/deep/a/b/c/service.log": true,
	} {
		if got := s.matches(path); got != want {
			t.Errorf("matches(%q) = %v, want %v", path, got, want)
		}
	}
}

func TestLoadSourcesConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "logs.yaml")
	os.WriteFile(path, []byte(`sources:
  - name: containers
    include: ["/var/log/containers/*.log"]
    containerd: true
  - name: host
    include: ["/var/log/**/*.log"]
    exclude: ["/var/log/containers/*.log"]
    multiline: true
    attributes:
      service.name: host-logs
`), 0o644)

	srcs, err := LoadSourcesConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(srcs) != 2 || !srcs[0].Containerd || srcs[1].Attributes["service.name"] != "host-logs" {
		t.Fatalf("sources = %+v", srcs)
	}
	if srcs[1].Multiline == nil || !*srcs[1].Multiline {
		t.Errorf("host multiline = %v", srcs[1].Multiline)
	}

	// A source without include patterns is rejected.
	os.WriteFile(path, []byte("sources:\n  - name: bad\n"), 0o644)
	if _, err := LoadSourcesConfig(path); err == nil {
		t.Error("missing include: want error")
	}
}

func TestPlainSourceTailing(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{{
		Name:       "hostlogs",
		Include:    []string{filepath.Join(dir, "*.log")},
		Attributes: map[string]string{"log.source": "host"},
	}}, false)
	stop := startTailer(t, tl)
	defer stop()

	// A non-CRI file: lines are exported verbatim (no CRI parsing).
	writeLines(t, filepath.Join(dir, "app.log"), "plain line one", "not a CRI line 2")
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "2 plain records")
	if got := exp.get(); got[0] != "plain line one" || got[1] != "not a CRI line 2" {
		t.Fatalf("records = %v", got)
	}

	exp.mu.Lock()
	ra := exp.resAttrs
	exp.mu.Unlock()
	if ra["log.source"] != "host" {
		t.Errorf("configured attribute missing: %v", ra)
	}
	if ra["service.name"] != "hostlogs" {
		t.Errorf("service.name not defaulted to source name: %v", ra)
	}
	if ra["k8s.node.name"] != "node1" {
		t.Errorf("node attribute missing: %v", ra)
	}
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

func TestCompressedSourceReadWhole(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	// A .gz source; multiline on to prove archives use the pipeline too.
	tl := newSourceTailer(exp, []Source{{
		Name:    "archives",
		Include: []string{filepath.Join(dir, "*.log.gz")},
	}}, true)
	stop := startTailer(t, tl)
	defer stop()

	// Unlike plain tailing, a compressed archive that appears is read in full
	// (not skipped to the end), including a multi-line Python traceback.
	writeGzip(t, filepath.Join(dir, "old.log.gz"),
		"line one",
		"Traceback (most recent call last):",
		`  File "x.py", line 3, in <module>`,
		"    raise RuntimeError('boom')",
		"line after")

	waitFor(t, func() bool { return len(exp.get()) == 3 }, "3 records (line + joined traceback + line)")
	got := exp.get()
	if got[0] != "line one" || got[2] != "line after" {
		t.Fatalf("records = %q", got)
	}
	if !strings.Contains(got[1], "Traceback") || !strings.Contains(got[1], "raise RuntimeError") {
		t.Fatalf("traceback not joined from archive: %q", got[1])
	}

	// The archive is read once: no duplicate records over subsequent sweeps.
	time.Sleep(200 * time.Millisecond)
	if n := len(exp.get()); n != 3 {
		t.Fatalf("archive re-read: %d records", n)
	}
}

func TestSourceEncoding(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{{
		Include:  []string{filepath.Join(dir, "*.log")},
		Encoding: "windows-1252",
	}}, false)
	stop := startTailer(t, tl)
	defer stop()

	// windows-1252/latin1 bytes: 0xE9 = é, 0xC0 = À. Two lines.
	raw := []byte("caf\xe9\n\xc0 la carte\n")
	if err := os.WriteFile(filepath.Join(dir, "app.log"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "2 decoded records")
	if got := exp.get(); got[0] != "café" || got[1] != "À la carte" {
		t.Fatalf("decoded records = %q (want café / À la carte)", got)
	}
}

func TestSourceIncludeExclude(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "skip"), 0o755); err != nil {
		t.Fatal(err)
	}
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{{
		Include: []string{filepath.Join(dir, "**", "*.log")},
		Exclude: []string{filepath.Join(dir, "skip", "*.log")},
	}}, false)
	stop := startTailer(t, tl)
	defer stop()

	writeLines(t, filepath.Join(dir, "keep.log"), "kept")
	writeLines(t, filepath.Join(dir, "skip", "dropped.log"), "dropped")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "the kept record")
	time.Sleep(150 * time.Millisecond) // give the excluded file a chance to (wrongly) appear
	if got := exp.get(); len(got) != 1 || got[0] != "kept" {
		t.Fatalf("records = %v (excluded file leaked?)", got)
	}
}

// plainTrace reports how many exported records contain a Python traceback
// header, and how many of those also contain the final frame (i.e. joined).
func plainTrace(exp *fakeExporter) (joined, count int) {
	for _, r := range exp.get() {
		if strings.Contains(r, "Traceback (most recent call last):") {
			count++
			if strings.Contains(r, "raise RuntimeError") {
				joined++
			}
		}
	}
	return joined, count
}

func TestPlainMultilineJoinsAcrossRotation(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{{
		Name:    "host",
		Include: []string{filepath.Join(dir, "app.log")},
	}}, true) // plain + multiline
	stop := startTailer(t, tl)
	defer stop()

	path := filepath.Join(dir, "app.log")
	writeLines(t, path, "first plain line")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "warmup record")

	// A Python traceback (no CRI prefix) straddling a rename rotation must
	// still join into one record — plain files use the same rotation machinery.
	writeLines(t, path,
		"Traceback (most recent call last):",
		`  File "app.py", line 10, in <module>`)
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeLines(t, path,
		"    main()",
		"    raise RuntimeError('boom')",
		"trailing normal line")

	waitFor(t, func() bool { j, _ := plainTrace(exp); return j == 1 }, "traceback joined across rotation")
	if j, n := plainTrace(exp); j != 1 || n != 1 {
		t.Fatalf("plain traceback joined=%d count=%d (want 1/1): %q", j, n, exp.get())
	}
}
