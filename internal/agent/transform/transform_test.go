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

func TestReloadSwapsAndKeepsLastGoodOnFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "transforms.yaml")
	v1 := "logs: |\n  def transform(batch):\n      for r in batch:\n          r.attributes[\"v\"] = \"1\"\n"
	if err := os.WriteFile(path, []byte(v1), 0o644); err != nil {
		t.Fatal(err)
	}
	prog, err := CompileFile(path)
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); Reload(ctx, w, path, 20*time.Millisecond, testLogger()) }()

	waitHash := func(want bool, hash string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if (w.Active().Hash == hash) == want {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("hash never converged (want change=%v from %s, have %s)", want, hash, w.Active().Hash)
	}

	// Good edit: swaps.
	v2 := "logs: |\n  def transform(batch):\n      for r in batch:\n          r.attributes[\"v\"] = \"2\"\n"
	h1 := w.Active().Hash
	if err := os.WriteFile(path, []byte(v2), 0o644); err != nil {
		t.Fatal(err)
	}
	waitHash(false, h1)
	h2 := w.Active().Hash

	// Broken edit: keeps v2.
	if err := os.WriteFile(path, []byte("logs: |\n  broken ===\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if w.Active().Hash != h2 {
		t.Fatal("broken edit displaced the last good program")
	}
	cancel()
	<-done
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
