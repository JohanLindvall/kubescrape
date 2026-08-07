package transform

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/tailsample"
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
