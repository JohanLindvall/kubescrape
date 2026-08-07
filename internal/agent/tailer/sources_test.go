package tailer

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bmatcuk/doublestar/v4"
	"sigs.k8s.io/yaml"
)

// LoadSourcesConfig loads a standalone config file. Production config arrives solely
// through the unified agent config (cmd/kubescrape-agent -config); this
// loader survives only for the strict-YAML parse/validate tests here.
func LoadSourcesConfig(path string) ([]Source, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg SourcesConfig
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return ValidateSources(cfg.Sources)
}

// matches reports whether the source would claim path (include minus
// exclude). Production scans use glob() + excluded() — glob output satisfies
// the includes by construction — so the combined check lives here, next to
// the test that pins the include/exclude semantics.
func (s *compiledSource) matches(path string) bool {
	included := false
	for _, g := range s.include {
		if ok, _ := doublestar.PathMatch(g, path); ok {
			included = true
			break
		}
	}
	return included && !s.excluded(path)
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
	_ = os.WriteFile(path, []byte(`sources:
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
	_ = os.WriteFile(path, []byte("sources:\n  - name: bad\n"), 0o644)
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

func TestPathAttributes(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "azdo-diag", "agent-7", "buildlogs", "tl-42")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{{
		Name:       "azdo",
		Include:    []string{filepath.Join(dir, "**", "*.log")},
		Attributes: map[string]string{"azdo.agent": "static-loses"},
		PathAttributes: []PathAttribute{
			// Named groups map verbatim.
			{Regexp: `/azdo-diag/(?P<agent>[^/]+)/`, Attributes: map[string]string{"azdo.agent": "agent"}},
			// Explicit mapping: dotted keys, plus a numeric group reference.
			{
				Regexp: `/buildlogs/(?P<timeline>[^/]+)/(.+)\.log$`,
				Attributes: map[string]string{
					"azdo.timeline.id":   "timeline",
					"azdo.display.name":  "2",
					"azdo.never.matches": "timeline", // same group, second key: both set
				},
			},
			// A non-matching rule contributes nothing (and must not error).
			{Regexp: `/gh-diag/(?P<gh>[^/]+)/`},
		},
	}}, false)
	stop := startTailer(t, tl)
	defer stop()

	writeLines(t, filepath.Join(sub, "Compile-Job.log"), "hello")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "1 record")

	exp.mu.Lock()
	ra := exp.resAttrs
	exp.mu.Unlock()
	want := map[string]string{
		"azdo.agent":        "agent-7", // the path capture beats the static
		"azdo.timeline.id":  "tl-42",
		"azdo.display.name": "Compile-Job",
	}
	for k, v := range want {
		if ra[k] != v {
			t.Errorf("attr %s = %q, want %q (all: %v)", k, ra[k], v, ra)
		}
	}
	if _, ok := ra["gh"]; ok {
		t.Errorf("non-matching rule produced an attribute: %v", ra)
	}
}

func TestPathAttributesValidation(t *testing.T) {
	valid := []Source{{
		Include: []string{"/var/log/**/*.log"},
		PathAttributes: []PathAttribute{
			{Regexp: `/x/(?P<a>[^/]+)/`},
			{Regexp: `/y/([^/]+)/`, Attributes: map[string]string{"k8s.thing": "1"}},
		},
	}}
	if _, err := ValidateSources(valid); err != nil {
		t.Fatalf("valid config refused: %v", err)
	}

	bad := []struct {
		name string
		pa   PathAttribute
	}{
		{"bad regexp", PathAttribute{Regexp: `(`}},
		{"unknown group name", PathAttribute{Regexp: `/x/(?P<a>.+)/`, Attributes: map[string]string{"k": "b"}}},
		{"group number out of range", PathAttribute{Regexp: `/x/(.+)/`, Attributes: map[string]string{"k": "2"}}},
		{"no groups at all", PathAttribute{Regexp: `/x/.+/`}},
	}
	for _, tc := range bad {
		src := []Source{{Include: []string{"/var/log/*.log"}, PathAttributes: []PathAttribute{tc.pa}}}
		if _, err := ValidateSources(src); err == nil {
			t.Errorf("%s: want error, got nil", tc.name)
		}
	}

	// Refused (not ignored) on containerd sources.
	cd := []Source{{
		Include:        []string{"/var/log/containers/*.log"},
		Containerd:     true,
		PathAttributes: []PathAttribute{{Regexp: `(?P<a>.+)`}},
	}}
	if _, err := ValidateSources(cd); err == nil || !strings.Contains(err.Error(), "containerd") {
		t.Errorf("containerd + pathAttributes: want refusal naming containerd, got %v", err)
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

// Per-source Multiline overrides the global default in both directions.
func TestPerSourceMultilineOverride(t *testing.T) {
	on, off := true, false
	for _, tc := range []struct {
		global bool
		source *bool
		want   bool
	}{
		{false, &on, true},
		{true, &off, false},
		{false, nil, false},
		{true, nil, true},
	} {
		srcs := compileSources([]Source{{
			Name:      "s",
			Include:   []string{"/tmp/*.log"},
			Multiline: tc.source,
		}}, "", tc.global)
		if got := srcs[0].multiline; got != tc.want {
			t.Errorf("global=%v source=%v: multiline = %v, want %v",
				tc.global, ptrStr(tc.source), got, tc.want)
		}
	}
}

func ptrStr(b *bool) string {
	if b == nil {
		return "nil"
	}
	if *b {
		return "true"
	}
	return "false"
}

// The listing-OK bool exists so an unreadable include base is never mistaken
// for "every file was removed" — gone-detection and checkpoint pruning both
// gate on it. doublestar's FilepathGlob IGNORES filesystem errors by default,
// so without WithFailOnIOErrors it reported success on a directory it could
// not read, the listing came back empty, and saveCheckpoints pruned the
// offsets of files it merely could not see.
func TestGlobReportsUnreadableBaseDirectories(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := t.TempDir()
	logs := filepath.Join(dir, "logs")
	if err := os.Mkdir(logs, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(logs, "a.log"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	srcs := compileSources([]Source{{Name: "s", Include: []string{filepath.Join(logs, "*.log")}}}, logs, false)
	if paths, ok := srcs[0].glob(); !ok || len(paths) != 1 {
		t.Fatalf("baseline listing: paths=%v ok=%v; want one file and ok", paths, ok)
	}

	if err := os.Chmod(logs, 0o000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(logs, 0o755) }()

	paths, ok := srcs[0].glob()
	if ok {
		t.Errorf("an unreadable include base listed as OK with %d paths; the tailer would mark every file gone and prune its offsets", len(paths))
	}
}
