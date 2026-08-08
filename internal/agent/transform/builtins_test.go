package transform

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/route"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
)

// The re module: RE2 with a cached compile, unlocking the OTTL-shaped
// conditions (IsMatch / replace_pattern) string methods cannot express.
func TestReBuiltins(t *testing.T) {
	prog, err := Compile([]byte(`
logs: |
  def transform(batch):
      for r in batch:
          if re.match("^Storage(Read|Write|Delete)$", r.attributes["cat"] or ""):
              r.drop()
              continue
          r.body = re.replace("(?i)token=[a-z0-9]+", "token=[X]", r.body)
          g = re.groups("user=(\\w+)", r.body)
          if g != None:
              r.attributes["user"] = g[1]
          r.attributes["ips"] = ",".join(re.findall("\\d+\\.\\d+\\.\\d+\\.\\d+", r.body))
          f = re.find("code=\\d+", r.body)
          if f != None:
              r.attributes["code"] = f
`))
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)

	ld := logsPayload("user=bob TOKEN=abc123 from 10.0.0.1 and 10.0.0.2 code=42", "keep me")
	rl := ld.ResourceLogs().At(0)
	lrs := rl.ScopeLogs().At(0).LogRecords()
	lrs.At(0).Attributes().PutStr("cat", "StorageRead")
	if err := w.ExportLogs(context.Background(), ld); err != nil {
		t.Fatal(err)
	}
	out := next.logs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	if out.Len() != 1 {
		t.Fatalf("records = %d, want the StorageRead one dropped", out.Len())
	}
	// The surviving record was the SECOND one; run assertions against a fresh
	// payload where the interesting record survives.
	ld2 := logsPayload("user=bob TOKEN=abc123 from 10.0.0.1 and 10.0.0.2 code=42")
	if err := w.ExportLogs(context.Background(), ld2); err != nil {
		t.Fatal(err)
	}
	got := next.logs[1].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	if !strings.Contains(got.Body().Str(), "token=[X]") {
		t.Errorf("replace failed: %q", got.Body().Str())
	}
	if v, _ := got.Attributes().Get("user"); v.Str() != "bob" {
		t.Errorf("groups failed: %q", v.Str())
	}
	if v, _ := got.Attributes().Get("ips"); v.Str() != "10.0.0.1,10.0.0.2" {
		t.Errorf("findall failed: %q", v.Str())
	}
	if v, _ := got.Attributes().Get("code"); v.Str() != "code=42" {
		t.Errorf("find failed: %q", v.Str())
	}
}

// A bad pattern is a script error (fails the export, counted), not a panic;
// log() never fails a script.
func TestReBadPatternAndLog(t *testing.T) {
	prog, err := Compile([]byte(`
logs: |
  def transform(batch):
      log("running")
      for r in batch:
          re.match("(", r.body)
`))
	if err != nil {
		t.Fatal(err)
	}
	w := Wrap(&capExp{}, nil, prog)
	if err := w.ExportLogs(context.Background(), logsPayload("x")); err == nil {
		t.Fatal("bad pattern must fail the export like any script error")
	}
}

// The new log-record fields: timestamps (rw), ids (ro, None when zero) and
// the scope name.
func TestLogRecordExtendedFields(t *testing.T) {
	prog, err := Compile([]byte(`
logs: |
  def transform(batch):
      for r in batch:
          r.attributes["had_ts"] = r.time_unix_nano
          r.attributes["scope"] = r.scope_name
          r.attributes["tid"] = r.trace_id or "none"
          r.time_unix_nano = 1700000000000000000
          r.observed_time_unix_nano = 1700000000000000001
`))
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, nil, prog)
	ld := logsPayload("hello")
	sl := ld.ResourceLogs().At(0).ScopeLogs().At(0)
	sl.Scope().SetName("my.scope")
	if err := w.ExportLogs(context.Background(), ld); err != nil {
		t.Fatal(err)
	}
	out := next.logs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	if int64(out.Timestamp()) != 1700000000000000000 || int64(out.ObservedTimestamp()) != 1700000000000000001 {
		t.Fatalf("timestamps not set: %v / %v", out.Timestamp(), out.ObservedTimestamp())
	}
	if v, _ := out.Attributes().Get("scope"); v.Str() != "my.scope" {
		t.Errorf("scope_name = %q", v.Str())
	}
	if v, _ := out.Attributes().Get("tid"); v.Str() != "none" {
		t.Errorf("zero trace id must read as None, got %q", v.Str())
	}
}

// The new span fields: kind, duration, ids, status message.
func TestSpanExtendedFields(t *testing.T) {
	prog, err := Compile([]byte(`
traces: |
  def transform(batch):
      for s in batch:
          s.attributes["kind"] = s.kind
          s.attributes["dur"] = s.duration_ms
          s.attributes["tid"] = s.trace_id or "none"
          s.attributes["msg"] = s.status_message
          if s.duration_ms > 500:
              s.attributes["slow"] = True
`))
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)

	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetName("op")
	sp.SetKind(ptrace.SpanKindServer)
	sp.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	start := time.Unix(100, 0)
	sp.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
	sp.SetEndTimestamp(pcommon.NewTimestampFromTime(start.Add(750 * time.Millisecond)))
	sp.Status().SetMessage("deadline exceeded")
	if err := w.ExportTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	got := next.traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	attr := func(k string) pcommon.Value { v, _ := got.Attributes().Get(k); return v }
	if attr("kind").Str() != "server" {
		t.Errorf("kind = %q", attr("kind").Str())
	}
	if attr("dur").Double() != 750 {
		t.Errorf("duration_ms = %v", attr("dur").Double())
	}
	if attr("tid").Str() != "0102030405060708090a0b0c0d0e0f10" {
		t.Errorf("trace_id = %q", attr("tid").Str())
	}
	if attr("msg").Str() != "deadline exceeded" {
		t.Errorf("status_message = %q", attr("msg").Str())
	}
	if !attr("slow").Bool() {
		t.Error("slow not set")
	}
}

// route("name"): the script stamps the reserved attribute; the router honors
// it before the namespace globs, strips it from what it sends, and a typo'd
// name degrades to the default chain — also stripped.
func TestScriptRouting(t *testing.T) {
	prog, err := Compile([]byte(`
logs: |
  def transform(batch):
      for r in batch:
          if "tenant-b" in r.body:
              r.route("b")
          if "typo" in r.body:
              r.route("no-such-route")
`))
	if err != nil {
		t.Fatal(err)
	}
	def := &capExp{}
	routeB := &capExp{}
	router := route.New(def, []route.Destination{{Name: "b", Exporter: routeB}})
	w := Wrap(router, nil, prog)

	if err := w.ExportLogs(context.Background(), logsPayload("for tenant-b")); err != nil {
		t.Fatal(err)
	}
	if err := w.ExportLogs(context.Background(), logsPayload("typo route")); err != nil {
		t.Fatal(err)
	}
	if err := w.ExportLogs(context.Background(), logsPayload("plain")); err != nil {
		t.Fatal(err)
	}
	if len(routeB.logs) != 1 || len(def.logs) != 2 {
		t.Fatalf("routed=%d default=%d, want 1/2", len(routeB.logs), len(def.logs))
	}
	for _, got := range routeB.logs {
		if _, ok := got.ResourceLogs().At(0).Resource().Attributes().Get(route.ScriptMarker); ok {
			t.Fatal("marker leaked to a route destination")
		}
	}
	for _, got := range def.logs {
		if _, ok := got.ResourceLogs().At(0).Resource().Attributes().Get(route.ScriptMarker); ok {
			t.Fatal("marker leaked to the default chain")
		}
	}
}

// emit_metric: one observation into a DECLARED logMetrics series, grouped by
// the item's resource; an undeclared name is a script error.
func TestEmitMetric(t *testing.T) {
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "script_events", Type: "counter", Value: "1",
	}})
	if err != nil {
		t.Fatal(err)
	}
	prog, err := Compile([]byte(`
logs: |
  def transform(batch):
      for r in batch:
          r.emit_metric("script_events", 2, {"kind": "seen"})
`))
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, nil, prog)
	w.SetMetricEmitter(set)
	if err := w.ExportLogs(context.Background(), logsPayload("x")); err != nil {
		t.Fatal(err)
	}

	exp := &capMetrics{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	total := 0.0
	labelled := false
	for _, md := range exp.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			ms := rms.At(i).ScopeMetrics().At(0).Metrics()
			for j := 0; j < ms.Len(); j++ {
				dps := ms.At(j).Sum().DataPoints()
				for d := 0; d < dps.Len(); d++ {
					dp := dps.At(d)
					total += dp.DoubleValue() + float64(dp.IntValue())
					if v, ok := dp.Attributes().Get("kind"); ok && v.Str() == "seen" {
						labelled = true
					}
				}
			}
		}
	}
	if total != 2 || !labelled {
		t.Fatalf("emitted total=%v labelled=%v, want 2/true", total, labelled)
	}

	// Undeclared metric: a script error, failing the export.
	bad, err := Compile([]byte("logs: |\n  def transform(batch):\n      for r in batch:\n          r.emit_metric(\"nope\", 1)\n"))
	if err != nil {
		t.Fatal(err)
	}
	wb := Wrap(&capExp{}, nil, bad)
	wb.SetMetricEmitter(set)
	if err := wb.ExportLogs(context.Background(), logsPayload("x")); err == nil {
		t.Fatal("undeclared metric must fail the export")
	}
}

// Fork shares the emit_metric target through a pointer, exactly like the
// program: main builds the self-chain fork (routing enabled) BEFORE it wires
// the emitter, so a Fork that copied the interface VALUE froze the fork's at
// nil forever — and a metrics script's emit_metric then failed the self
// chain's export every interval. Wiring the parent after the fork exists must
// reach both.
func TestEmitMetricThroughForkWiredAfter(t *testing.T) {
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "script_events", Type: "counter", Value: "1",
	}})
	if err != nil {
		t.Fatal(err)
	}
	prog, err := Compile([]byte(`
logs: |
  def transform(batch):
      for r in batch:
          r.emit_metric("script_events", 1)
`))
	if err != nil {
		t.Fatal(err)
	}
	w := Wrap(&capExp{}, nil, prog)
	fork := w.Fork(&capExp{}, nil) // main's order: the fork exists first...
	w.SetMetricEmitter(set)        // ...and the emitter is wired afterwards.
	if err := fork.ExportLogs(context.Background(), logsPayload("x")); err != nil {
		t.Fatalf("emit_metric through a fork wired after Fork: %v", err)
	}

	// The observation landed in the set, through the fork.
	exp := &capMetrics{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	total := 0.0
	for _, md := range exp.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			ms := rms.At(i).ScopeMetrics().At(0).Metrics()
			for j := 0; j < ms.Len(); j++ {
				dps := ms.At(j).Sum().DataPoints()
				for d := 0; d < dps.Len(); d++ {
					total += dps.At(d).DoubleValue() + float64(dps.At(d).IntValue())
				}
			}
		}
	}
	if total != 1 {
		t.Fatalf("emitted total = %v, want 1", total)
	}
}

// capMetrics deep-copies (DynamicMetricSet.Export clears its payload).
type capMetrics struct{ md []pmetric.Metrics }

func (c *capMetrics) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	cp := pmetric.NewMetrics()
	md.CopyTo(cp)
	c.md = append(c.md, cp)
	return nil
}
