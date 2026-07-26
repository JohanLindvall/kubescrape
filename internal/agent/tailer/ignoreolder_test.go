package tailer

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A source's ignoreOlder cutoff keeps stale files out at DISCOVERY — they are
// never opened, tracked or read — while never abandoning a file we have
// already read from.
func TestIgnoreOlderSkipsStaleButNeverTrackedOrCheckpointed(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "stale.log")
	fresh := filepath.Join(dir, "fresh.log")
	resumed := filepath.Join(dir, "resumed.log")
	for _, p := range []string{stale, fresh, resumed} {
		if err := os.WriteFile(p, []byte("line\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-48 * time.Hour)
	for _, p := range []string{stale, resumed} {
		if err := os.Chtimes(p, old, old); err != nil {
			t.Fatal(err)
		}
	}

	tl := New(Config{
		Dir: dir, PollInterval: time.Hour, FlushInterval: time.Hour,
		MetadataWait: time.Second, Metadata: fakeMeta{}, Exporter: &fakeExporter{},
		Sources: []Source{{
			Name: "plain", Include: []string{filepath.Join(dir, "*.log")},
			IgnoreOlder: "24h",
		}},
	})
	// resumed.log carries a stored offset: however stale it looks, dropping it
	// would abandon the bytes past that offset and re-ingest the file whole if
	// it were ever appended to again.
	tl.scanDir(map[string]checkpoint{resumed: {Offset: 2, Inode: 1}}, true)

	if _, ok := tl.files[stale]; ok {
		t.Error("a file older than ignoreOlder was tracked")
	}
	if _, ok := tl.files[fresh]; !ok {
		t.Error("a fresh file was skipped")
	}
	if _, ok := tl.files[resumed]; !ok {
		t.Error("a checkpointed file was dropped by ignoreOlder — its unshipped tail is lost")
	}

	// A tracked file that goes idle past the cutoff must not be dropped (that
	// would delete its checkpoint and re-ingest it from zero on the next write).
	if err := os.Chtimes(fresh, old, old); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false)
	if f, ok := tl.files[fresh]; !ok || f.gone {
		t.Error("an idle tracked file was dropped by ignoreOlder")
	}
	// The stale file stays out on every later scan, without churn.
	if _, ok := tl.files[stale]; ok {
		t.Error("stale file appeared on a later scan")
	}
}

func TestIgnoreOlderValidation(t *testing.T) {
	for _, v := range []string{"nonsense", "-1h"} {
		if _, err := ValidateSources([]Source{{Include: []string{"/x/*.log"}, IgnoreOlder: v}}); err == nil {
			t.Errorf("ignoreOlder %q accepted", v)
		}
	}
	if _, err := ValidateSources([]Source{{Include: []string{"/x/*.log"}, IgnoreOlder: "24h"}}); err != nil {
		t.Errorf("valid ignoreOlder rejected: %v", err)
	}
}
