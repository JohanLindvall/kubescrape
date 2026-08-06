package transform

// What a transform DISCARDS has to be counted.
//
// A transform drop is intended loss, which is exactly why silence is wrong
// here: the intent lives in an operator-edited Starlark file that hot-reloads
// with no deploy to mark it, so a predicate that quietly widened to match
// everything looks — from every metric the agent publishes — identical to a
// node with nothing to ship. The comment in hostobj.go records that happening:
// "a node's logs gone with no error logged and no counter moved".

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

func TestTransformDropsAreCounted(t *testing.T) {
	t.Run("logs", func(t *testing.T) {
		prog, err := compileStarlark("logs", "def transform(batch):\n    for r in batch:\n        if r.body != \"keep\":\n            r.drop()\n")
		if err != nil {
			t.Fatal(err)
		}
		ld := plog.NewLogs()
		sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
		for _, body := range []string{"drop", "keep", "drop", "drop"} {
			sl.LogRecords().AppendEmpty().Body().SetStr(body)
		}

		before := obs.TransformDropped.WithLabelValues("logs").Value()
		dropped, err := prog.runLogs(ld)
		if err != nil {
			t.Fatal(err)
		}
		prog.countDropped(dropped)
		if got := obs.TransformDropped.WithLabelValues("logs").Value() - before; got != 3 {
			t.Fatalf("counted %v dropped log records, want 3", got)
		}
		if n := ld.LogRecordCount(); n != 1 {
			t.Fatalf("survivors = %d, want 1", n)
		}
	})

	t.Run("metric data points", func(t *testing.T) {
		// A metric's cost lives on its points, so the unit is POINTS — and a
		// metric dropped whole costs every point it carried, or the same loss
		// would count differently depending on which drop() the script used.
		prog, err := compileStarlark("metrics", `
def transform(batch):
    for m in batch:
        if m.name == "whole":
            m.drop()
        else:
            for p in m.datapoints:
                if p.attributes["code"] == "500":
                    p.drop()
`)
		if err != nil {
			t.Fatal(err)
		}
		md := pmetric.NewMetrics()
		sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
		whole := sm.Metrics().AppendEmpty()
		whole.SetName("whole")
		for i := 0; i < 2; i++ {
			whole.SetEmptySum().DataPoints().AppendEmpty()
		}
		partial := sm.Metrics().AppendEmpty()
		partial.SetName("partial")
		psum := partial.SetEmptySum()
		for _, code := range []string{"200", "500", "500"} {
			p := psum.DataPoints().AppendEmpty()
			p.Attributes().PutStr("code", code)
		}

		before := obs.TransformDropped.WithLabelValues("metrics").Value()
		dropped, err := prog.runMetrics(md)
		if err != nil {
			t.Fatal(err)
		}
		prog.countDropped(dropped)
		// whole: 2 points (SetEmptySum in a loop leaves one point), partial: 2.
		wholePoints := 1
		if got := obs.TransformDropped.WithLabelValues("metrics").Value() - before; got != float64(wholePoints+2) {
			t.Fatalf("counted %v dropped data points, want %d", got, wholePoints+2)
		}
	})

	t.Run("spans", func(t *testing.T) {
		prog, err := compileStarlark("traces", "def transform(batch):\n    for s in batch:\n        if s.name != \"keep\":\n            s.drop()\n")
		if err != nil {
			t.Fatal(err)
		}
		td := ptrace.NewTraces()
		ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
		for _, name := range []string{"drop", "keep"} {
			ss.Spans().AppendEmpty().SetName(name)
		}

		before := obs.TransformDropped.WithLabelValues("traces").Value()
		dropped, err := prog.runTraces(td)
		if err != nil {
			t.Fatal(err)
		}
		prog.countDropped(dropped)
		if got := obs.TransformDropped.WithLabelValues("traces").Value() - before; got != 1 {
			t.Fatalf("counted %v dropped spans, want 1", got)
		}
	})
}

// failNTimesExp fails the first n forwards, then delivers.
type failNTimesExp struct {
	capExp
	failsLeft int
}

func (f *failNTimesExp) ExportLogs(ctx context.Context, ld plog.Logs) error {
	if f.failsLeft > 0 {
		f.failsLeft--
		return errTransient
	}
	return f.capExp.ExportLogs(ctx, ld)
}

var errTransient = errors.New("collector unavailable")

// Drops are counted once per DELIVERED batch, not once per attempt: a
// copy-path producer (journald, events) re-offers the same object on a
// transient failure and every attempt re-copies and re-runs the script, so
// counting at prune time inflated the counter by one batch's drops per retry
// — drop volume proportional to outage length, not operator intent.
func TestTransformDropsNotDoubleCountedAcrossRetries(t *testing.T) {
	prog, err := Compile([]byte("logs: |\n  def transform(batch):\n      for r in batch:\n          if r.body != \"keep\":\n              r.drop()\n"))
	if err != nil {
		t.Fatal(err)
	}
	next := &failNTimesExp{failsLeft: 2}
	w := Wrap(next, nil, prog)
	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	for _, body := range []string{"drop", "keep", "drop"} {
		sl.LogRecords().AppendEmpty().Body().SetStr(body)
	}

	before := obs.TransformDropped.WithLabelValues("logs").Value()
	ctx := context.Background()
	for range 2 { // the producer's transient retries
		if err := w.ExportLogs(ctx, ld); err == nil {
			t.Fatal("want transient failure")
		}
	}
	if got := obs.TransformDropped.WithLabelValues("logs").Value() - before; got != 0 {
		t.Fatalf("counted %v drops across failed attempts; nothing was delivered", got)
	}
	if err := w.ExportLogs(ctx, ld); err != nil {
		t.Fatal(err)
	}
	if got := obs.TransformDropped.WithLabelValues("logs").Value() - before; got != 2 {
		t.Fatalf("counted %v drops after delivery, want 2 (once, not per attempt)", got)
	}
}

// A script that drops NOTHING must not move the counter: an alert on this rate
// is only readable if the quiet case is genuinely quiet.
func TestTransformNoDropsNoCount(t *testing.T) {
	prog, err := compileStarlark("logs", "def transform(batch):\n    for r in batch:\n        r.attributes[\"seen\"] = \"1\"\n")
	if err != nil {
		t.Fatal(err)
	}
	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	sl.LogRecords().AppendEmpty().Body().SetStr("a")

	before := obs.TransformDropped.WithLabelValues("logs").Value()
	dropped, err := prog.runLogs(ld)
	if err != nil {
		t.Fatal(err)
	}
	prog.countDropped(dropped)
	if got := obs.TransformDropped.WithLabelValues("logs").Value() - before; got != 0 {
		t.Fatalf("counter moved by %v with nothing dropped", got)
	}
}
