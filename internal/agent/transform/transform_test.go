package transform

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

type capExp struct {
	logs    []plog.Logs
	metrics []pmetric.Metrics
	traces  []ptrace.Traces
}

func (c *capExp) ExportLogs(_ context.Context, ld plog.Logs) error {
	c.logs = append(c.logs, ld)
	return nil
}
func (c *capExp) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	c.metrics = append(c.metrics, md)
	return nil
}
func (c *capExp) ExportTraces(_ context.Context, td ptrace.Traces) error {
	c.traces = append(c.traces, td)
	return nil
}

func logsPayload(bodies ...string) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("k8s.namespace.name", "ns1")
	lrs := rl.ScopeLogs().AppendEmpty().LogRecords()
	for _, b := range bodies {
		lr := lrs.AppendEmpty()
		lr.Body().SetStr(b)
		lr.Attributes().PutStr("level", "info")
	}
	return ld
}

func TestLogTransformMutateAndDrop(t *testing.T) {
	prog, err := Compile([]byte(`
logs: |
  def transform(batch):
      for r in batch:
          if "noisy" in r.body:
              r.drop()
              continue
          r.body = r.body.replace("world", "there")
          r.attributes["seen"] = True
          r.resource["env"] = "prod"
`))
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)

	if err := w.ExportLogs(context.Background(), logsPayload("hello world", "noisy debug spam")); err != nil {
		t.Fatal(err)
	}
	if len(next.logs) != 1 {
		t.Fatalf("batches = %d", len(next.logs))
	}
	ld := next.logs[0]
	lrs := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	if lrs.Len() != 1 {
		t.Fatalf("records = %d, want the noisy one dropped", lrs.Len())
	}
	if got := lrs.At(0).Body().Str(); got != "hello there" {
		t.Fatalf("body = %q", got)
	}
	if v, ok := lrs.At(0).Attributes().Get("seen"); !ok || !v.Bool() {
		t.Fatal("record attribute not set")
	}
	if v, ok := ld.ResourceLogs().At(0).Resource().Attributes().Get("env"); !ok || v.Str() != "prod" {
		t.Fatal("resource attribute not set")
	}
	if _, ok := lrs.At(0).Attributes().Get(dropMarker); ok {
		t.Fatal("drop marker leaked into export")
	}
}

func TestAllDroppedAcksWithoutForwarding(t *testing.T) {
	prog, err := Compile([]byte("logs: |\n  def transform(batch):\n      for r in batch:\n          r.drop()\n"))
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)
	if err := w.ExportLogs(context.Background(), logsPayload("a", "b")); err != nil {
		t.Fatal(err)
	}
	if len(next.logs) != 0 {
		t.Fatal("empty payload forwarded")
	}
}

func TestMetricAndTraceTransforms(t *testing.T) {
	prog, err := Compile([]byte(`
metrics: |
  def transform(batch):
      for m in batch:
          if m.name == "kill_me":
              m.drop()
          elif m.name == "old_name":
              m.name = "new_name"
traces: |
  def transform(batch):
      for s in batch:
          if s.name == "healthz":
              s.drop()
`))
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	sm.Metrics().AppendEmpty().SetName("kill_me")
	sm.Metrics().AppendEmpty().SetName("old_name")
	if err := w.ExportMetrics(context.Background(), md); err != nil {
		t.Fatal(err)
	}
	got := next.metrics[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	if got.Len() != 1 || got.At(0).Name() != "new_name" {
		t.Fatalf("metrics after transform: len=%d name=%q", got.Len(), got.At(0).Name())
	}

	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	ss.Spans().AppendEmpty().SetName("healthz")
	ss.Spans().AppendEmpty().SetName("real-work")
	if err := w.ExportTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	spans := next.traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	if spans.Len() != 1 || spans.At(0).Name() != "real-work" {
		t.Fatalf("spans after transform: len=%d", spans.Len())
	}
}

func TestCompileRejectsWholeConfigOnAnyError(t *testing.T) {
	_, err := Compile([]byte("logs: |\n  def transform(batch): pass\nmetrics: |\n  this is not starlark ===\n"))
	if err == nil {
		t.Fatal("bad metrics script must reject the whole config")
	}
	_, err = Compile([]byte("logs: |\n  x = 1\n"))
	if err == nil {
		t.Fatal("script without transform() must fail")
	}
}

func TestRuntimeErrorFailsExport(t *testing.T) {
	prog, err := Compile([]byte("logs: |\n  def transform(batch):\n      fail(\"boom\")\n"))
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)
	if err := w.ExportLogs(context.Background(), logsPayload("x")); err == nil {
		t.Fatal("runtime error must fail the export (producer retries)")
	}
	if len(next.logs) != 0 {
		t.Fatal("failed transform forwarded anyway")
	}
}

// writeAtomic replaces path the way Kubernetes does: a ConfigMap update
// populates a new directory and swaps the ..data symlink with rename(2), so a
// reader resolving the path gets either the whole old file or the whole new
// one. os.WriteFile does NOT model that — it truncates and then writes, and a
// reader sampling the gap sees a ZERO-LENGTH file, which is valid YAML and
// compiles to a valid program with no transforms. Compile-then-commit cannot
// reject it (nothing failed), so the reloader swapped it in and the test below
// blamed the broken edit for a displacement the truncation caused: ~0.5% of
// runs on a loaded machine, always via the poll tick, which unlike the event
// path has no debounce to wait the writer out.
func writeAtomic(t *testing.T, path, content string) {
	t.Helper()
	tmp := path + ".new"
	if err := os.WriteFile(tmp, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

// waitFor polls cond to a deadline. The reloader is a goroutine woken by
// fsnotify and a ticker, so a test can assert that a state is REACHED but
// never how long reaching it takes; every wait here is on an observable the
// reloader publishes, never on an elapsed duration.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// A good edit swaps the active program; a broken one keeps the last good.
//
// Both halves wait on a happens-after, not a clock. The swap is awaited by the
// EXACT hash v2 must produce — "any hash but v1" would also accept a state the
// reloader merely passes through, and then pin the second half to it. The
// rejection is awaited by the reloads{failed} counter, bumped on the one
// branch that also declines to swap: once it moves past the baseline taken
// before the write, the reloader has read the broken file, rejected it and
// returned without touching the program, so the assertion is about a settled
// state instead of a guess at how long a compile takes. (That counter is a
// process-global, which is sound here because only Reload writes it and the
// check is baseline-relative — but it is why this test must not go parallel
// with another that reloads.)
func TestReloadSwapsAndKeepsLastGoodOnFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "transforms.yaml")
	v1 := "logs: |\n  def transform(batch):\n      for r in batch:\n          r.attributes[\"v\"] = \"1\"\n"
	v2 := "logs: |\n  def transform(batch):\n      for r in batch:\n          r.attributes[\"v\"] = \"2\"\n"
	broken := "logs: |\n  broken ===\n"
	writeAtomic(t, path, v1)
	prog, err := CompileFile(path)
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	// A short poll so the fsnotify fallback is exercised too; nothing below
	// depends on which of the two paths gets there first.
	go func() { defer close(done); Reload(ctx, w, path, 20*time.Millisecond, testLogger()) }()

	h1, h2 := contentHash([]byte(v1)), contentHash([]byte(v2))
	if got := w.Active().Hash; got != h1 {
		t.Fatalf("initial program hash %s, want v1 %s", got, h1)
	}

	// Good edit: swaps, and to exactly v2.
	writeAtomic(t, path, v2)
	waitFor(t, "the v2 program to go active", func() bool { return w.Active().Hash == h2 })

	// Broken edit: compiles to nothing, so v2 must survive it.
	mustBeV2 := func(when string) {
		t.Helper()
		if got := w.Active().Hash; got != h2 {
			t.Fatalf("broken edit displaced the last good program %s: active %s, want v2 %s", when, got, h2)
		}
	}
	failed := obs.TransformReloads.WithLabelValues("failed")
	before := failed.Value()
	writeAtomic(t, path, broken)
	waitFor(t, "the broken file to be read and rejected", func() bool { return failed.Value() > before })
	mustBeV2("at the moment the reload was rejected")
	// And still after the reloader has exited: joining it is what makes the
	// negative airtight, since no goroutine is left that could swap anything
	// in. Checking only above would leave a reloader that swaps AFTER counting
	// its failure indistinguishable from one that never swaps.
	cancel()
	<-done
	mustBeV2("after the reloader stopped")
}

func testLogger() *slog.Logger { return slog.Default() }

// drop() then rename must still drop the metric (the marker lives in the
// pdata-internal Metadata map, not the name field).
func TestMetricDropThenRename(t *testing.T) {
	prog, err := Compile([]byte("metrics: |\n  def transform(batch):\n      for m in batch:\n          m.drop()\n          m.name = \"survivor\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)
	md := pmetric.NewMetrics()
	md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetName("victim")
	if err := w.ExportMetrics(context.Background(), md); err != nil {
		t.Fatal(err)
	}
	if len(next.metrics) != 0 {
		got := next.metrics[0].ResourceMetrics()
		if got.Len() > 0 {
			t.Fatalf("drop lost to rename: metric %q exported", got.At(0).ScopeMetrics().At(0).Metrics().At(0).Name())
		}
	}
}

// A top-level comprehension in the transforms file must not hang compile —
// the step limit bounds it to a config error.
func TestCompileBoundsTopLevelComprehension(t *testing.T) {
	done := make(chan error, 1)
	go func() {
		_, err := Compile([]byte("logs: |\n  _ = [x for x in range(100000000000)]\n  def transform(batch): pass\n"))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("unbounded comprehension compiled without error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("compile hung on a top-level comprehension (no step limit)")
	}
}

// severity_number is settable (advertised in AttrNames; assigning must not
// fail the export).
func TestSetSeverityNumber(t *testing.T) {
	prog, err := Compile([]byte("logs: |\n  def transform(batch):\n      for r in batch:\n          r.severity_number = 17\n"))
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)
	if err := w.ExportLogs(context.Background(), logsPayload("x")); err != nil {
		t.Fatal(err)
	}
	sev := next.logs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SeverityNumber()
	if int(sev) != 17 {
		t.Fatalf("severity_number = %d, want 17", int(sev))
	}
}

// The transform must NOT mutate its input: the ingest batcher retries the
// same payload object, and the spanmetrics tap Consumes it after forwarding.
// A non-idempotent script re-run on the original must produce the same
// output, and the input must be unchanged for the tap.
func TestTransformDoesNotMutateInput(t *testing.T) {
	prog, err := Compile([]byte("logs: |\n  def transform(batch):\n      for r in batch:\n          r.body = \"[node] \" + r.body\n"))
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)
	ld := logsPayload("hello")

	if err := w.ExportLogs(context.Background(), ld); err != nil {
		t.Fatal(err)
	}
	// Input unchanged.
	if got := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str(); got != "hello" {
		t.Fatalf("input mutated: body = %q", got)
	}
	// Forwarded copy transformed once.
	if got := next.logs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str(); got != "[node] hello" {
		t.Fatalf("forwarded = %q", got)
	}
	// Re-export the SAME object (batcher retry) — must NOT double-apply.
	next2 := &capExp{}
	w2 := Wrap(next2, next2, prog)
	if err := w2.ExportLogs(context.Background(), ld); err != nil {
		t.Fatal(err)
	}
	if got := next2.logs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str(); got != "[node] hello" {
		t.Fatalf("retry double-applied: %q (want single [node] prefix)", got)
	}
}

// A logs-only transforms file must not break the trace pass-through when the
// downstream supports traces.
func TestLogsOnlyTransformForwardsTraces(t *testing.T) {
	prog, err := Compile([]byte("logs: |\n  def transform(batch): pass\n"))
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("s")
	if err := w.ExportTraces(context.Background(), td); err != nil {
		t.Fatalf("logs-only transform broke traces: %v", err)
	}
	if len(next.traces) != 1 {
		t.Fatal("trace not forwarded")
	}
}

// A fork shares the parent's program: the agent runs one chain through the
// namespace router and a second — carrying its own metrics — around it, and a
// reload must reach both. Two independently reloaded wrappers could otherwise
// run different scripts on the same signal.
func TestForkSharesTheReloadedProgram(t *testing.T) {
	renamer, err := Compile([]byte(`
metrics: |
  def transform(batch):
      for m in batch:
          m.name = "renamed"
`))
	if err != nil {
		t.Fatal(err)
	}
	mainNext, forkNext := &capExp{}, &capExp{}
	w := Wrap(mainNext, nil, nil) // no program yet
	f := w.Fork(forkNext, nil)

	one := func() pmetric.Metrics {
		md := pmetric.NewMetrics()
		md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetName("original")
		return md
	}
	if err := f.ExportMetrics(context.Background(), one()); err != nil {
		t.Fatal(err)
	}
	if got := forkNext.metrics[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Name(); got != "original" {
		t.Fatalf("fork applied a program before any was installed: name=%q", got)
	}

	// Swapping on the PARENT (what the reloader holds) must apply to the fork.
	w.Swap(renamer)
	if f.Active() != w.Active() {
		t.Fatal("fork and parent report different active programs")
	}
	if err := f.ExportMetrics(context.Background(), one()); err != nil {
		t.Fatal(err)
	}
	if got := forkNext.metrics[1].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Name(); got != "renamed" {
		t.Fatalf("fork metric name = %q; the parent's swapped program must apply to it", got)
	}
	if len(mainNext.metrics) != 0 {
		t.Fatalf("main chain received %d payloads; the fork must forward to its OWN downstream", len(mainNext.metrics))
	}
}

// `"k" in attrs` must answer honestly. Starlark's IN tries Container.Has
// before Mapping.Get, and Get reports found=true for a MISSING key so scripts
// can write `attrs["k"] != None` — so without Has every membership test
// answered True. A script keying a drop on one is then a total outage for
// whatever it filters: everything drops, the export forwards nothing, returns
// nil, and the producer commits its offsets.
func TestAttributeMembershipIsNotAlwaysTrue(t *testing.T) {
	prog, err := Compile([]byte("logs: |\n" +
		"  def transform(batch):\n" +
		"      for r in batch:\n" +
		"          if \"debug\" in r.attributes:\n" +
		"              r.drop()\n" +
		"          if \"k8s.namespace.name\" not in r.resource:\n" +
		"              r.drop()\n"))
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)

	ld := logsPayload("keep-me", "drop-me")
	ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(1).Attributes().PutStr("debug", "1")

	if err := w.ExportLogs(context.Background(), ld); err != nil {
		t.Fatal(err)
	}
	if len(next.logs) != 1 {
		t.Fatalf("payloads forwarded = %d; want 1 — every record matched a membership test that is always True", len(next.logs))
	}
	lrs := next.logs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	if lrs.Len() != 1 {
		t.Fatalf("records forwarded = %d; want 1 (only the one carrying `debug` drops)", lrs.Len())
	}
	if got := lrs.At(0).Body().Str(); got != "keep-me" {
		t.Errorf("forwarded %q; want keep-me", got)
	}
}
