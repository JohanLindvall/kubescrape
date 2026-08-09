package transform

// The handoff contract's pins: the marker survives the shutdown-context
// seams, a marked payload is transformed IN PLACE with every wrapper
// semantic intact (ack-without-send, error-fails-export, drop marks swept),
// and an UNMARKED payload retried through the wrapper still gets the copy.

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestHandoffSurvivesWithoutCancel(t *testing.T) {
	ctx := Handoff(context.Background())
	if !HandedOff(ctx) {
		t.Fatal("Handoff did not mark the context")
	}
	// Every shutdown flush in the repo detaches via WithoutCancel (never a bare
	// Background); the marker must ride through it, like otlpexport.Own.
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	if !HandedOff(dctx) {
		t.Fatal("handoff marker lost across WithoutCancel + WithTimeout")
	}
	if HandedOff(context.Background()) {
		t.Fatal("unmarked context reads as handed off")
	}
	var nilCtx context.Context
	if HandedOff(nilCtx) { // the nil guard, as in otlpexport.Owned
		t.Fatal("nil context reads as handed off")
	}
	if Handoff(ctx) != ctx {
		t.Fatal("re-marking an already-marked context must be a no-op")
	}
}

// A marked logs payload is transformed in place: the caller's object carries
// the script's mutations after the export — the copy is gone.
func TestHandoffTransformsLogsInPlace(t *testing.T) {
	prog, err := Compile([]byte("logs: |\n  def transform(batch):\n      for r in batch:\n          r.body = \"[t] \" + r.body\n"))
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)
	ld := logsPayload("hello")
	if err := w.ExportLogs(Handoff(context.Background()), ld); err != nil {
		t.Fatal(err)
	}
	if got := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str(); got != "[t] hello" {
		t.Fatalf("input body = %q; a handed-off payload must be transformed in place (no copy)", got)
	}
	if got := next.logs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str(); got != "[t] hello" {
		t.Fatalf("forwarded body = %q", got)
	}
}

func TestHandoffTransformsMetricsAndTracesInPlace(t *testing.T) {
	prog, err := Compile([]byte(`
metrics: |
  def transform(batch):
      for m in batch:
          m.name = "renamed"
traces: |
  def transform(batch):
      for s in batch:
          s.name = "renamed"
`))
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)

	md := pmetric.NewMetrics()
	md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetName("original")
	if err := w.ExportMetrics(Handoff(context.Background()), md); err != nil {
		t.Fatal(err)
	}
	if got := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Name(); got != "renamed" {
		t.Fatalf("metrics input name = %q; want in-place rename", got)
	}

	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("original")
	if err := w.ExportTraces(Handoff(context.Background()), td); err != nil {
		t.Fatal(err)
	}
	if got := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name(); got != "renamed" {
		t.Fatalf("traces input span name = %q; want in-place rename", got)
	}
}

// The in-place path keeps the wrapper's semantics: everything dropped is
// acked without a send, and the drop marker never leaks off a survivor.
func TestHandoffAllDroppedAcksWithoutSend(t *testing.T) {
	prog, err := Compile([]byte("logs: |\n  def transform(batch):\n      for r in batch:\n          r.drop()\n"))
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)
	ld := logsPayload("a", "b")
	if err := w.ExportLogs(Handoff(context.Background()), ld); err != nil {
		t.Fatal(err)
	}
	if len(next.logs) != 0 {
		t.Fatal("empty payload forwarded")
	}
	if ld.ResourceLogs().Len() != 0 {
		t.Fatalf("input holds %d ResourceLogs; the in-place prune must sweep emptied groups", ld.ResourceLogs().Len())
	}
}

func TestHandoffDropMarkerDoesNotLeak(t *testing.T) {
	prog, err := Compile([]byte("logs: |\n  def transform(batch):\n      for r in batch:\n          if \"noisy\" in r.body:\n              r.drop()\n"))
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)
	ld := logsPayload("keep", "noisy spam")
	if err := w.ExportLogs(Handoff(context.Background()), ld); err != nil {
		t.Fatal(err)
	}
	lrs := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	if lrs.Len() != 1 || lrs.At(0).Body().Str() != "keep" {
		t.Fatalf("in-place prune left %d records", lrs.Len())
	}
	if _, ok := lrs.At(0).Attributes().Get(DropMarker); ok {
		t.Fatal("drop marker leaked onto a survivor on the in-place path")
	}
}

// A script error still fails the export on the in-place path. The payload may
// be left half-transformed — the marker's contract makes that unobservable
// (the caller rebuilds from source) — so nothing about its state is asserted.
func TestHandoffRuntimeErrorFailsExport(t *testing.T) {
	prog, err := Compile([]byte("logs: |\n  def transform(batch):\n      fail(\"boom\")\n"))
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)
	if err := w.ExportLogs(Handoff(context.Background()), logsPayload("x")); err == nil {
		t.Fatal("runtime error must fail the export")
	}
	if len(next.logs) != 0 {
		t.Fatal("failed transform forwarded anyway")
	}
}

// An UNMARKED producer retried through the wrapper still gets the copy: the
// input stays pristine across failed attempts, and the eventually delivered
// payload carries the script applied exactly once — not once per attempt.
func TestUnmarkedRetryStillGetsTheCopy(t *testing.T) {
	prog, err := Compile([]byte("logs: |\n  def transform(batch):\n      for r in batch:\n          r.body = \"[t] \" + r.body\n"))
	if err != nil {
		t.Fatal(err)
	}
	next := &failN{fail: 2}
	w := Wrap(next, nil, prog)
	ld := logsPayload("hello")

	var expErr error
	for attempt := 0; attempt < 3; attempt++ {
		if expErr = w.ExportLogs(context.Background(), ld); expErr == nil {
			break
		}
		// Between attempts the input must be untouched — this is the retry
		// contract the copy exists for.
		if got := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str(); got != "hello" {
			t.Fatalf("attempt %d mutated the unmarked input: body = %q", attempt, got)
		}
	}
	if expErr != nil {
		t.Fatal(expErr)
	}
	got := next.logs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str()
	if got != "[t] hello" {
		t.Fatalf("delivered body = %q; want the script applied exactly once", got)
	}
	if got := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str(); got != "hello" {
		t.Fatalf("input mutated after delivery: body = %q", got)
	}
}

// failN is capExp with the first N ExportLogs calls failing (the retrying
// unmarked producer's collector).
type failN struct {
	capExp
	fail int
}

func (f *failN) ExportLogs(ctx context.Context, ld plog.Logs) error {
	if f.fail > 0 {
		f.fail--
		return errors.New("collector down")
	}
	return f.capExp.ExportLogs(ctx, ld)
}
