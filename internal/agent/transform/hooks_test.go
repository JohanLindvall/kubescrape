package transform

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/tailsample"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

func hookWrapper(t *testing.T, cfg string) *Wrapper {
	t.Helper()
	prog, err := Compile([]byte(cfg))
	if err != nil {
		t.Fatal(err)
	}
	return Wrap(&capExp{}, nil, prog)
}

// ingest: admit(resource) — False rejects; a script error fails OPEN.
func TestAdmitResourceHook(t *testing.T) {
	w := hookWrapper(t, `
ingest: |
  def admit(resource):
      return resource["team"] != "banned"
`)
	ok := pcommon.NewMap()
	ok.PutStr("team", "good")
	if !w.AdmitResource(ok) {
		t.Fatal("admissible resource rejected")
	}
	bad := pcommon.NewMap()
	bad.PutStr("team", "banned")
	if w.AdmitResource(bad) {
		t.Fatal("banned resource admitted")
	}
	if !w.HasAdmit() {
		t.Fatal("HasAdmit = false with an ingest section")
	}

	// Error → fail open.
	we := hookWrapper(t, "ingest: |\n  def admit(resource):\n      fail(\"boom\")\n")
	if !we.AdmitResource(ok) {
		t.Fatal("a script error must fail OPEN (admit)")
	}

	// No section → admit, cheaply.
	wn := hookWrapper(t, "logs: |\n  def transform(batch): pass\n")
	if !wn.AdmitResource(ok) || wn.HasAdmit() {
		t.Fatal("no ingest section must admit and report HasAdmit=false")
	}
}

// targets: target(t) — drop by pod label, rewrite the path (URL re-renders);
// errors keep the target.
func TestTargetHook(t *testing.T) {
	w := hookWrapper(t, `
targets: |
  def target(t):
      if t.labels["scrape-tier"] == "none":
          t.drop()
          return
      if t.path == "/metrics" and t.namespace == "legacy":
          t.path = "/actuator/prometheus"
`)
	ts := []kubemeta.ScrapeTarget{
		{
			URL: "http://10.0.0.1:9090/metrics", Scheme: "http", Address: "10.0.0.1:9090", Path: "/metrics",
			Pod: kubemeta.Pod{Namespace: "legacy", Name: "app-1"},
		},
		{
			URL: "http://10.0.0.2:9090/metrics", Scheme: "http", Address: "10.0.0.2:9090", Path: "/metrics",
			Pod: kubemeta.Pod{Namespace: "x", Name: "drop-me", Labels: map[string]string{"scrape-tier": "none"}},
		},
		{
			URL: "http://10.0.0.3:9090/metrics", Scheme: "http", Address: "10.0.0.3:9090", Path: "/metrics",
			Pod: kubemeta.Pod{Namespace: "x", Name: "keep"},
		},
	}
	out := w.TransformTargets(ts)
	if len(out) != 2 {
		t.Fatalf("targets = %d, want 2", len(out))
	}
	if out[0].URL != "http://10.0.0.1:9090/actuator/prometheus" || out[0].Path != "/actuator/prometheus" {
		t.Fatalf("path rewrite: %q / %q", out[0].URL, out[0].Path)
	}
	if out[1].Pod.Name != "keep" {
		t.Fatalf("wrong survivor: %+v", out[1].Pod.Name)
	}

	// Error → every target kept untouched.
	we := hookWrapper(t, "targets: |\n  def target(t):\n      fail(\"boom\")\n")
	if got := we.TransformTargets(ts[:1]); len(got) != 1 || got[0].URL != ts[0].URL {
		t.Fatal("a script error must keep the target untouched")
	}
}

// cachedTargets builds the same three-target slice every call: one the hook
// drops, two it path-rewrites. A fresh copy per call is what lets the tests
// below compare a slice the hook saw against one it never did.
func cachedTargets() []kubemeta.ScrapeTarget {
	return []kubemeta.ScrapeTarget{
		{
			URL: "http://10.0.0.1:9090/metrics", Scheme: "http", Address: "10.0.0.1:9090", Path: "/metrics",
			Pod: kubemeta.Pod{Namespace: "x", Name: "drop-me"},
		},
		{
			URL: "http://10.0.0.2:9090/metrics", Scheme: "http", Address: "10.0.0.2:9090", Path: "/metrics",
			Pod: kubemeta.Pod{Namespace: "x", Name: "b"},
		},
		{
			URL: "http://10.0.0.3:9090/metrics", Scheme: "http", Address: "10.0.0.3:9090", Path: "/metrics",
			Pod: kubemeta.Pod{Namespace: "x", Name: "c"},
		},
	}
}

// dropAndRewrite drops "drop-me" and rewrites every other path — the two
// writes the hook is allowed to make, aimed at the two ways it used to reach
// the caller's slice (compaction into ts[:0], Path/URL through &ts[i]).
const dropAndRewrite = `
targets: |
  def target(t):
      if t.pod == "drop-me":
          t.drop()
          return
      t.path = "/rewritten"
`

// The input slice is metaclient's CACHED value (shallow-copied out under the
// treat-as-immutable contract): the hook must never write the slice or its
// elements. It used to compact survivors into ts[:0] — leaving the cached
// backing array as [B, C, C], one target scraped twice per cycle — and wrote
// Path/URL through the shared elements.
func TestTransformTargetsDoesNotMutateInput(t *testing.T) {
	w := hookWrapper(t, dropAndRewrite)
	ts := cachedTargets()
	orig := cachedTargets()

	out := w.TransformTargets(ts)
	if len(out) != 2 || out[0].Path != "/rewritten" || out[0].URL != "http://10.0.0.2:9090/rewritten" {
		t.Fatalf("hook output: %+v", out)
	}
	if !reflect.DeepEqual(ts, orig) {
		t.Fatalf("the caller's slice was mutated:\n got %+v\nwant %+v", ts, orig)
	}
}

// A cache hit re-serves the SAME slice: the second invocation must see the
// original targets, not the first invocation's output (shifted survivors,
// compounded path rewrites).
func TestTransformTargetsSecondInvocationSeesOriginals(t *testing.T) {
	w := hookWrapper(t, dropAndRewrite)
	ts := cachedTargets()
	orig := cachedTargets()

	first := w.TransformTargets(ts)
	second := w.TransformTargets(ts)
	if !reflect.DeepEqual(second, first) {
		t.Fatalf("cache-hit invocation diverged:\nfirst  %+v\nsecond %+v", first, second)
	}
	if !reflect.DeepEqual(ts, orig) {
		t.Fatalf("the cached slice drifted across invocations:\n got %+v\nwant %+v", ts, orig)
	}
}

// A script that WRITES and then errors must not ship its partial state:
// SetField has already applied t.path when fail() unwinds, so appending the
// working copy emitted a target whose Path disagreed with its never
// re-rendered URL. "Errors keep the target untouched" means byte-identical
// to the input — drop() marks included, since fail-open keeps the target.
func TestTargetHookErrorKeepsTargetUntouched(t *testing.T) {
	w := hookWrapper(t, `
targets: |
  def target(t):
      t.path = "/mutated"
      t.drop()
      fail("boom")
`)
	ts := cachedTargets()
	before := obs.TransformErrors.WithLabelValues("targets").Value()
	out := w.TransformTargets(ts)
	if !reflect.DeepEqual(out, cachedTargets()) {
		t.Fatalf("mutate-then-error must emit the pristine targets:\n got %+v\nwant %+v", out, cachedTargets())
	}
	if got := obs.TransformErrors.WithLabelValues("targets").Value() - before; got != float64(len(ts)) {
		t.Fatalf("transform_errors{targets} moved %v, want %v", got, len(ts))
	}
}

// sample: decide(trace) — True samples, False drops, None abstains; wired as
// tailsample's `type: script` policy.
func TestSampleDeciderHook(t *testing.T) {
	w := hookWrapper(t, `
sample: |
  def decide(trace):
      for s in trace.spans:
          if s.attributes["retain"] == "always":
              return True
          if s.duration_ms > 10000:
              return False
      return None
`)
	decider := w.SampleDecider()
	if decider == nil {
		t.Fatal("no decider despite a sample section")
	}

	mkTrace := func(retain string, durMs int) tailsample.Trace {
		td := ptrace.NewTraces()
		sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		if retain != "" {
			sp.Attributes().PutStr("retain", retain)
		}
		start := time.Unix(10, 0)
		sp.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
		sp.SetEndTimestamp(pcommon.NewTimestampFromTime(start.Add(time.Duration(durMs) * time.Millisecond)))
		return tailsample.Trace{Spans: []tailsample.Span{{Span: sp, Resource: pcommon.NewMap()}}}
	}
	if s, ab := decider(mkTrace("always", 5)); !s || ab {
		t.Fatalf("retain=always: sample=%v abstain=%v", s, ab)
	}
	if s, ab := decider(mkTrace("", 60000)); s || ab {
		t.Fatalf("slow trace: sample=%v abstain=%v, want drop", s, ab)
	}
	if _, ab := decider(mkTrace("", 5)); !ab {
		t.Fatal("unmatched trace must abstain")
	}

	// The evaluator end-to-end: script leaf inside a policy list.
	ev, err := tailsample.New(tailsample.Config{
		Policies: []tailsample.PolicyConfig{{Name: "custom", Type: tailsample.TypeScript}},
		Script:   decider,
	})
	if err != nil {
		t.Fatal(err)
	}
	if d := ev.Decide(mkTrace("always", 5)); !d.Sampled || d.Policy != "custom" {
		t.Fatalf("decision = %+v", d)
	}
	// And the config-time refusal without the injection.
	if _, err := tailsample.New(tailsample.Config{
		Policies: []tailsample.PolicyConfig{{Name: "custom", Type: tailsample.TypeScript}},
	}); err == nil {
		t.Fatal("type script without an injected body must refuse")
	}
}

// A hot reload that REMOVES the sample: section must be as loud as a script
// error, never a silent abstain: the sectionless file compiles, so it commits
// as an APPLIED reload, and the startup UsesScript/HasSample cross-check does
// not re-run — the counter and the throttled warn are the only signal that a
// live `type: script` policy stopped deciding. The decider is captured by
// tailsample once at startup, so ONE closure must decide normally, abstain
// loudly while the section is gone, and resume when a reload restores it.
// Warn assertions ride the throttle gate (this package's tests assert
// counters, not log output): a claimed gate is the proof the warn fired.
func TestSampleDeciderReloadRemovingSectionCountsAndWarns(t *testing.T) {
	withSample := "sample: |\n  def decide(trace):\n      return True\n"
	withoutSample := "logs: |\n  def transform(batch): pass\n"
	path := filepath.Join(t.TempDir(), "transforms.yaml")
	writeAtomic(t, path, withSample)
	prog, err := CompileFile(path)
	if err != nil {
		t.Fatal(err)
	}
	w := Wrap(&capExp{}, nil, prog)
	decider := w.SampleDecider()
	if decider == nil {
		t.Fatal("no decider despite a sample section")
	}
	tr := tailsample.Trace{} // the script returns True without reading it

	// (1) Section present: decides normally, the error counter stays still.
	errs := obs.TransformErrors.WithLabelValues("sample")
	before := errs.Value()
	if s, ab := decider(tr); !s || ab {
		t.Fatalf("with sample section: sample=%v abstain=%v, want sample", s, ab)
	}
	if got := errs.Value() - before; got != 0 {
		t.Fatalf("transform_errors{sample} moved %v on a normal decision", got)
	}

	// The REAL reload path commits the edit that removes the section.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); Reload(ctx, w, path, 20*time.Millisecond, testLogger()) }()
	defer func() { cancel(); <-done }()
	writeAtomic(t, path, withoutSample)
	waitFor(t, "the sectionless program to go active", func() bool {
		return w.Active().Hash == contentHash([]byte(withoutSample))
	})

	// (2)+(3) Section gone: abstain, counter on EVERY decision, warn once —
	// the gate claim after the first decision proves the warn fired, and the
	// second decision must keep counting while the gate throttles its warn.
	before = errs.Value()
	if s, ab := decider(tr); s || !ab {
		t.Fatalf("section gone: sample=%v abstain=%v, want abstain", s, ab)
	}
	if hookWarnGates.sampleGone.Allow(time.Minute) {
		t.Fatal("the section-removed warning did not claim its throttle gate (no warn fired)")
	}
	if s, ab := decider(tr); s || !ab {
		t.Fatalf("second decision with section gone: sample=%v abstain=%v, want abstain", s, ab)
	}
	if got := errs.Value() - before; got != 2 {
		t.Fatalf("transform_errors{sample} moved %v across two decisions, want 2", got)
	}

	// (4) Restoring the section resumes normal decisions through the SAME
	// closure, and the counter goes quiet again.
	writeAtomic(t, path, withSample)
	waitFor(t, "the restored program to go active", func() bool {
		return w.Active().Hash == contentHash([]byte(withSample))
	})
	before = errs.Value()
	if s, ab := decider(tr); !s || ab {
		t.Fatalf("after restore: sample=%v abstain=%v, want sample", s, ab)
	}
	if got := errs.Value() - before; got != 0 {
		t.Fatalf("transform_errors{sample} moved %v after the section was restored", got)
	}
}

// parse: parse(line) — dict sets body/severity/timestamp; None leaves the
// line alone; errors leave it alone.
func TestParseLineHook(t *testing.T) {
	w := hookWrapper(t, `
parse: |
  def parse(line):
      if not line.startswith("<log>"):
          return None
      return {
          "body": line[5:],
          "severity_text": "warn",
          "time_unix_nano": 1700000000000000000,
      }
`)
	p, ok := w.ParseLine("<log>hello")
	if !ok || p.Body != "hello" || !p.HasBody || p.SeverityText != "warn" || p.TimeUnixNano != 1700000000000000000 {
		t.Fatalf("parsed = %+v ok=%v", p, ok)
	}
	if _, ok := w.ParseLine("plain"); ok {
		t.Fatal("None must leave the line unparsed")
	}
	if !w.HasParse() {
		t.Fatal("HasParse = false with a parse section")
	}
}
