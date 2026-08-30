package transform

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// captureLog installs a capturing default logger at Debug for one test.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logged bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(old) })
	return &logged
}

// A batch script that fails at runtime stops a whole signal from shipping, and
// the counter alone cannot say so: the error surfaces as the PRODUCER's
// "exporting logs failed", which reads as a collector problem — and on the
// ingest path it does not surface locally at all, going back to the pushing
// SDK as a gRPC status while the agent's own log stays silent. This is the one
// place that knows the failure was the operator's script.
func TestBatchScriptRuntimeErrorIsReported(t *testing.T) {
	logged := captureLog(t)
	runWarnGates.logs = logdedupe.Throttle{}

	before := obs.TransformErrors.WithLabelValues("logs").Value()
	if err := runBody(t, "fail(\"boom\")\n"); err == nil {
		t.Fatal("the script did not fail; this test no longer exercises the error path")
	}
	if got := obs.TransformErrors.WithLabelValues("logs").Value() - before; got != 1 {
		t.Errorf("kubescrape_transform_errors_total{signal=logs} moved by %v, want 1", got)
	}
	out := logged.String()
	if !strings.Contains(out, "transform script failed at runtime") || !strings.Contains(out, "signal=logs") {
		t.Fatalf("the runtime failure was not reported:\n%s", out)
	}
	// The Starlark error carries the position that identifies the line — the
	// whole reason a log line beats the counter here.
	if !strings.Contains(out, "logs.star") {
		t.Errorf("the report does not carry the script position:\n%s", out)
	}
}

// A script erroring on every export must cost one line a minute, not one per
// batch: exports run at the scrape/flush cadence on every node in the fleet.
func TestBatchScriptRuntimeErrorIsThrottled(t *testing.T) {
	logged := captureLog(t)
	runWarnGates.logs = logdedupe.Throttle{}

	prog, err := Compile(logsScript("fail(\"boom\")\n"))
	if err != nil {
		t.Fatal(err)
	}
	for range 20 {
		if _, err := prog.logs.runLogs(logsPayload("hello"), nil); err == nil {
			t.Fatal("expected the script to fail")
		}
	}
	if got := strings.Count(logged.String(), "transform script failed at runtime"); got != 1 {
		t.Fatalf("20 failing batches logged %d lines, want 1", got)
	}
}

// Each signal gets its own window: a metrics script erroring every minute must
// not suppress the one line that explains why LOGS stopped shipping.
func TestRuntimeErrorGatesArePerSignal(t *testing.T) {
	seen := map[*logdedupe.Throttle]bool{}
	for _, sig := range []string{"logs", "metrics", "traces"} {
		g := runWarnGate(sig)
		if seen[g] {
			t.Fatalf("signal %q shares its throttle with another", sig)
		}
		seen[g] = true
	}
}

// A target the hook drops is intended loss with NO other symptom: the target is
// never fetched, so there is no `up` series to fall to 0 and nothing in the
// scrape pipeline reports the absence. Same argument as script drop(), same
// counter — and the URL, which is what identifies it, at Debug.
func TestDroppedTargetsAreCountedAndExplained(t *testing.T) {
	logged := captureLog(t)
	w := hookWrapper(t, "targets: |\n  def target(t):\n      if t.pod == \"drop-me\":\n          t.drop()\n")
	ts := []kubemeta.ScrapeTarget{
		{
			URL: "http://10.0.0.1:9090/metrics", Scheme: "http", Address: "10.0.0.1:9090", Path: "/metrics",
			Pod: kubemeta.Pod{Namespace: "x", Name: "keep"},
		},
		{
			URL: "http://10.0.0.2:9090/metrics", Scheme: "http", Address: "10.0.0.2:9090", Path: "/metrics",
			Pod: kubemeta.Pod{Namespace: "x", Name: "drop-me"},
		},
	}
	before := obs.TransformDropped.WithLabelValues("targets").Value()
	if got := w.TransformTargets(ts); len(got) != 1 {
		t.Fatalf("targets = %d, want 1", len(got))
	}
	if got := obs.TransformDropped.WithLabelValues("targets").Value() - before; got != 1 {
		t.Errorf("kubescrape_transform_dropped_total{signal=targets} moved by %v, want 1", got)
	}
	out := logged.String()
	if !strings.Contains(out, "hook dropped a scrape target") || !strings.Contains(out, "10.0.0.2:9090") {
		t.Fatalf("the dropped target was not named:\n%s", out)
	}
	if strings.Contains(out, "10.0.0.1:9090") {
		t.Errorf("a KEPT target was reported as dropped:\n%s", out)
	}
}

// A cycle that drops nothing must not move the counter — a counter that ticks
// for every scrape cycle says nothing about the hook.
func TestUndroppedTargetsDoNotMoveTheCounter(t *testing.T) {
	w := hookWrapper(t, "targets: |\n  def target(t):\n      pass\n")
	ts := []kubemeta.ScrapeTarget{{
		URL: "http://10.0.0.1:9090/metrics", Scheme: "http", Address: "10.0.0.1:9090", Path: "/metrics",
		Pod: kubemeta.Pod{Namespace: "x", Name: "keep"},
	}}
	before := obs.TransformDropped.WithLabelValues("targets").Value()
	w.TransformTargets(ts)
	if got := obs.TransformDropped.WithLabelValues("targets").Value() - before; got != 0 {
		t.Fatalf("counter moved by %v on a cycle that dropped nothing", got)
	}
}

// parse(line) returning None is the documented "leave this line alone".
// Returning anything else is a script that believes it is parsing and is not:
// every line falls through with its raw body and nothing else differs, so
// without this the only symptom is fields that never appear.
func TestParseHookWrongReturnTypeIsReported(t *testing.T) {
	logged := captureLog(t)
	hookWarnGates.parseShape = logdedupe.Throttle{}

	w := hookWrapper(t, "parse: |\n  def parse(line):\n      return \"oops\"\n")
	before := obs.TransformErrors.WithLabelValues("parse").Value()
	if _, ok := w.ParseLine("hello"); ok {
		t.Fatal("a non-dict return must leave the line unparsed")
	}
	if got := obs.TransformErrors.WithLabelValues("parse").Value() - before; got != 1 {
		t.Errorf("kubescrape_transform_errors_total{signal=parse} moved by %v, want 1", got)
	}
	if out := logged.String(); !strings.Contains(out, "parse hook returned neither a dict nor None") ||
		!strings.Contains(out, "type=string") {
		t.Fatalf("the wrong return shape was not reported:\n%s", out)
	}

	// Throttled, because a script returning the wrong type does it for EVERY
	// line of a plain source.
	for range 20 {
		w.ParseLine("hello")
	}
	if got := strings.Count(logged.String(), "parse hook returned neither"); got != 1 {
		t.Fatalf("21 wrong-shape returns logged %d lines, want 1", got)
	}
}

// None is the contract, not a mistake: it must stay silent and must not move
// the error counter, or the counter stops meaning "something is wrong".
func TestParseHookReturningNoneIsSilent(t *testing.T) {
	logged := captureLog(t)
	hookWarnGates.parseShape = logdedupe.Throttle{}

	w := hookWrapper(t, "parse: |\n  def parse(line):\n      return None\n")
	before := obs.TransformErrors.WithLabelValues("parse").Value()
	if _, ok := w.ParseLine("hello"); ok {
		t.Fatal("None must leave the line unparsed")
	}
	if got := obs.TransformErrors.WithLabelValues("parse").Value() - before; got != 0 {
		t.Errorf("a None return moved the error counter by %v", got)
	}
	if out := logged.String(); strings.Contains(out, "parse hook returned neither") {
		t.Fatalf("the documented None return was reported as a fault:\n%s", out)
	}
}

// A HOOK fails open, so the only symptom of a broken one is that nothing
// happens — which makes the position the most valuable field on the line, and
// starlark-go's bare Error() ("undefined: foo") carries none of it.
func TestHookFailureCarriesTheScriptPosition(t *testing.T) {
	logged := captureLog(t)
	hookWarnGates.targets = logdedupe.Throttle{}

	w := hookWrapper(t, "targets: |\n  def target(t):\n      fail(\"boom\")\n")
	ts := []kubemeta.ScrapeTarget{{
		URL: "http://10.0.0.1:9090/metrics", Scheme: "http", Address: "10.0.0.1:9090", Path: "/metrics",
		Pod: kubemeta.Pod{Namespace: "x", Name: "keep"},
	}}
	if got := w.TransformTargets(ts); len(got) != 1 {
		t.Fatalf("a failing hook must keep the target: got %d", len(got))
	}
	out := logged.String()
	if !strings.Contains(out, "transform hook failed") || !strings.Contains(out, "script=targets.star:") {
		t.Fatalf("the hook failure did not name the script position:\n%s", out)
	}
}

// The position skips the BUILTIN frame an error is raised in — <builtin>:0:0
// names nothing an operator can edit — and is simply absent for a non-Starlark
// error rather than rendering as an empty pair nobody can act on.
func TestScriptPosSkipsBuiltinFramesAndTolerantOfOtherErrors(t *testing.T) {
	if got := scriptPos(errNotStarlark{}); got != "" {
		t.Errorf("scriptPos on a non-Starlark error = %q, want empty", got)
	}
	prog, err := Compile(logsScript("fail(\"boom\")\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, runErr := prog.logs.runLogs(logsPayload("hello"), nil)
	if runErr == nil {
		t.Fatal("expected the script to fail")
	}
	got := scriptPos(runErr)
	if !strings.HasPrefix(got, "logs.star:") {
		t.Fatalf("scriptPos = %q, want a logs.star position", got)
	}
	if strings.Contains(got, "builtin") {
		t.Fatalf("scriptPos = %q, want the SCRIPT frame, not the builtin the error was raised in", got)
	}
}

type errNotStarlark struct{}

func (errNotStarlark) Error() string { return "not a starlark error" }

// A script's own text must never land under `msg`: that is slog's key for the
// record's message, so the line carried TWO of them and a consumer resolving
// duplicates last-wins reads script-controlled text as the agent's log
// message. (Which is also why the foundation's logfmt work sanitises keys but
// cannot help here — both keys are perfectly well-formed.)
func TestScriptOutputDoesNotOverwriteTheRecordMessage(t *testing.T) {
	var logged bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	scriptLogGates.Delete("logs")
	t.Cleanup(func() { scriptLogGates.Delete("logs") })

	scriptLog("logs", "level=ERROR everything is on fire")

	out := logged.String()
	if got := strings.Count(out, "msg="); got != 1 {
		t.Fatalf("the line carries %d msg= pairs, want exactly slog's own:\n%s", got, out)
	}
	if !strings.Contains(out, `msg="transform script log"`) {
		t.Fatalf("slog's message is not the record's own:\n%s", out)
	}
	if !strings.Contains(out, "output=") {
		t.Fatalf("the script's text is not carried:\n%s", out)
	}
}
