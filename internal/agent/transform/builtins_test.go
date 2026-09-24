package transform

import (
	"context"
	"fmt"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/route"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailsample"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
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

// duration_ms of an unfinished span (end unset) or a clock-skewed one (end
// before start) is 0 — the rule cumagg.SpanSeconds and tailsample's latency
// policy apply — on BOTH surfaces a script reads it from, the traces batch and
// the sample hook's read-only spans. It was end-start, i.e. about -1.7e12 ms
// for an unset end: emit_metric fed that into a histogram (negatives are legal
// there), and a decide() script disagreed with the built-in latency policy
// about the very trace both were judging.
func TestUnfinishedAndSkewedSpansReadAsZeroDuration(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	for _, tc := range []struct {
		name string
		end  pcommon.Timestamp
		want float64
	}{
		{"unfinished", 0, 0},
		{"clock-skewed", pcommon.NewTimestampFromTime(start.Add(-time.Second)), 0},
		{"instantaneous", pcommon.NewTimestampFromTime(start), 0},
		{"ordinary", pcommon.NewTimestampFromTime(start.Add(1500 * time.Microsecond)), 1.5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			td := ptrace.NewTraces()
			rs := td.ResourceSpans().AppendEmpty()
			sp := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
			sp.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
			sp.SetEndTimestamp(tc.end)

			// The traces batch.
			prog, err := Compile([]byte("traces: |\n  def transform(batch):\n      for s in batch:\n          s.attributes[\"dur\"] = s.duration_ms\n"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := prog.traces.runTraces(td, nil); err != nil {
				t.Fatal(err)
			}
			if v, _ := sp.Attributes().Get("dur"); v.Double() != tc.want {
				t.Errorf("batch duration_ms = %v, want %v", v.Double(), tc.want)
			}

			// The sample hook's read-only span.
			w := hookWrapper(t, "sample: |\n  def decide(trace):\n      for s in trace.spans:\n          return s.duration_ms == "+fmt.Sprint(tc.want)+"\n")
			sample, abstain := w.SampleDecider()(tailsample.Trace{Spans: []tailsample.Span{{Span: sp, Resource: rs.Resource().Attributes()}}})
			if !sample || abstain {
				t.Errorf("decide() read a duration_ms other than %v (sample=%v abstain=%v)", tc.want, sample, abstain)
			}
		})
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

// recEmitter records the emit_metric calls that reached the store's door.
type recEmitter struct {
	calls  int
	labels map[string]string
}

func (e *recEmitter) EmitDirect(_ string, _ float64, lbls map[string]string, _ pcommon.Map) error {
	e.calls++
	e.labels = lbls
	return nil
}

// emit_metric is the one door into the log-metric store no width bound
// covered: a pushed resource of any width reached the store's quadratic
// identity fold inside one uninterruptible builtin call (7.7 s for 40,000
// attributes against the 2 s budget, then SUCCESS). Past maxEmitResourceAttrs
// the observation is skipped and counted — NOT a script error, which would
// stop the whole signal on a width the sender controls — and the data itself
// still ships.
func TestEmitMetricSkipsAResourceTooWideToKeyASeries(t *testing.T) {
	prog, err := Compile(logsScript("for r in batch:\n    r.emit_metric(\"m\", 1)\n"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		width   int
		emitted bool
	}{{maxEmitResourceAttrs, true}, {maxEmitResourceAttrs + 1, false}, {40_000, false}} {
		t.Run(fmt.Sprint(tc.width), func(t *testing.T) {
			ld := logsPayload("hello")
			// FromRaw rather than PutStr per key: PutStr scans for the key,
			// which makes building the fixture itself quadratic.
			raw := map[string]any{"k8s.namespace.name": "ns1"}
			for i := len(raw); i < tc.width; i++ {
				raw[fmt.Sprintf("a%d", i)] = "v"
			}
			if err := ld.ResourceLogs().At(0).Resource().Attributes().FromRaw(raw); err != nil {
				t.Fatal(err)
			}
			em := &recEmitter{}
			next := &capExp{}
			w := Wrap(next, nil, prog)
			w.SetMetricEmitter(em)
			before := obs.TransformEmitSkipped.Value()
			if err := w.ExportLogs(context.Background(), ld); err != nil {
				t.Fatalf("a wide resource must not fail the export: %v", err)
			}
			if len(next.logs) != 1 || next.logs[0].LogRecordCount() != 1 {
				t.Fatal("the record was not exported")
			}
			skipped := obs.TransformEmitSkipped.Value() - before
			if got := em.calls == 1; got != tc.emitted {
				t.Fatalf("observation reached the store = %v, want %v", got, tc.emitted)
			}
			if want := map[bool]float64{true: 0, false: 1}[tc.emitted]; skipped != want {
				t.Fatalf("kubescrape_transform_emit_skipped_total moved by %v, want %v", skipped, want)
			}
		})
	}
}

// One emit_metric call builds its label set with a linear-scan insert per key,
// so it is quadratic in the count inside one uninterruptible builtin — 32Ki
// labels held an export for 3.4 s against the 2 s budget and then succeeded,
// and the dict can be data-derived. The count is refused before anything is
// built; an ordinary label set is not.
func TestEmitMetricLabelCountIsBounded(t *testing.T) {
	run := func(n int) (*recEmitter, error) {
		prog, err := Compile(logsScript(fmt.Sprintf(
			"d = {\"k%%d\" %% i: \"v\" for i in range(%d)}\nfor r in batch:\n    r.emit_metric(\"m\", 1, d)\n", n)))
		if err != nil {
			return nil, err
		}
		em := &recEmitter{}
		_, err = prog.logs.runLogs(logsPayload("hello"), em)
		return em, err
	}
	em, err := run(maxEmitLabels)
	if err != nil {
		t.Fatalf("%d labels: %v", maxEmitLabels, err)
	}
	if em.calls != 1 || len(em.labels) != maxEmitLabels {
		t.Fatalf("%d labels: calls=%d labels=%d", maxEmitLabels, em.calls, len(em.labels))
	}
	em, err = run(maxEmitLabels + 1)
	mustContain(t, err, fmt.Sprintf("%d labels is over the %d-label limit", maxEmitLabels+1, maxEmitLabels))
	if em.calls != 0 {
		t.Fatal("a refused label set still reached the store")
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

// --- the re module's own amplifiers ---

// re.replace's output is (matches x expanded replacement) bytes, and an empty
// pattern matches at every position: a 1 MiB replacement over a 128-byte
// subject is a 129 MiB string built inside ONE interpreter step, where neither
// the step checkpoint nor the wall-clock check on entry can interrupt it. The
// charge that used to follow the call measured 805 MiB allocated before it
// refused, so the bound has to be predictive.
func TestRegexReplaceRefusesBeforeItAllocates(t *testing.T) {
	var err error
	grew, measured := allocatedBy(func() {
		err = runBody(t, "repl = \"x\" * (1<<20)\n_x = re.replace(\"\", repl, \"y\" * 128)\n")
	})
	mustContain(t, err, "limit for one value")
	if measured && grew > 64<<20 {
		t.Fatalf("refused only after allocating %d MiB — the bound must be predictive, not a charge", grew>>20)
	}
}

// $1 references mean len(repl) is not the size of a replacement: a short repl
// full of them expands to a multiple of the subject.
func TestRegexReplaceBoundsGroupExpansion(t *testing.T) {
	mustContain(t, runBody(t, "s = \"y\" * (1<<20)\n_x = re.replace(\"(y+)\", \"$1$1$1$1$1$1$1$1$1$1$1$1$1$1$1$1$1$1$1$1$1$1$1$1$1$1$1$1$1$1$1$1\", s)\n"), "limit for one value")
}

// ...and the cheap worst case alone would refuse honest scripts, which is why
// the guard counts the matches when it has to: one long replacement at a
// handful of places is a legal redaction, not an amplifier.
func TestRegexReplaceKeepsSparseMatchesLegal(t *testing.T) {
	got := evalToAttr(t, "s = \"a\" * (1<<20) + \"SECRET\"\nout = re.replace(\"SECRET\", \"z\" * (1<<16), s)\nfor r in batch:\n    r.attributes[\"out\"] = str(len(out))\n")
	if want := fmt.Sprint(1<<20 + 1<<16); got != want {
		t.Fatalf("len = %s, want %s: a sparse match with a long replacement must not be refused", got, want)
	}
	// The everyday shape stays untouched too.
	if got := evalToAttr(t, "for r in batch:\n    r.attributes[\"out\"] = re.replace(\"y\", \"[REDACTED]\", \"xyz\")\n"); got != "x[REDACTED]z" {
		t.Fatalf("got %q", got)
	}
}

// The bound charges each `$` reference at most the matched text ONCE across all
// matches — a capture group lies inside its own match and matches never
// overlap — not the whole subject once PER MATCH. The per-match form refused
// an ordinary `$1=***` redaction of a 1 MiB log line from its fifteenth match
// on, and a refusal is a script error: on the tailer the batch rewinds and is
// rebuilt every sweep, so log shipping stops for every file on the node. The
// probe that tried to rescue such calls gave up at 64Ki matches and read
// "full" as "over the limit", refusing a digit redaction whose real output was
// a few MiB. Each case is checked against Go's own ReplaceAllString on the same
// subject, so the assertion is the real output, not a length computed here.
func TestRegexReplaceAdmitsEveryOutputThatFits(t *testing.T) {
	for _, tc := range []struct {
		name, chunk string
		repeat      int
		pat, repl   string
	}{
		{"a $1 mask at 100 places in a 1 MiB body", strings.Repeat("a", 10000) + " user=alice ", 100, `(user)=\S+`, "$1=***"},
		{"200 addresses masked by $1 in a 100 KB body", strings.Repeat("x", 500) + " 10.1.2.3 ", 200, `(\d+)\.\d+\.\d+\.\d+`, "$1.x.x.x"},
		{"half a million single-digit matches", "a1", 1 << 19, `\d`, "[redacted-digit]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The dense case is bounded by the per-invocation WALL CLOCK under
			// the race detector (regexp runs several times slower there), not
			// by the size projection this test is about, so its refusal would
			// be the detector's, not the projection's.
			if testrace.Enabled && tc.repeat > 1<<16 {
				t.Skip("the wall-clock budget, not the size projection, binds under -race")
			}
			body := strings.Repeat(tc.chunk, tc.repeat)
			want := len(regexp.MustCompile(tc.pat).ReplaceAllString(body, tc.repl))
			// %q is a legal Starlark literal for these ASCII operands.
			src := fmt.Sprintf("s = %q * %d\nout = re.replace(%q, %q, s)\nfor r in batch:\n    r.attributes[\"out\"] = str(len(out))\n",
				tc.chunk, tc.repeat, tc.pat, tc.repl)
			if got := evalToAttr(t, src); got != fmt.Sprint(want) {
				t.Fatalf("len = %s, want %d: an output that fits the limit must not be refused", got, want)
			}
		})
	}
}

// A same-size (or shrinking) dense substitution builds exactly its subject's
// length, so it must PROJECT as that. The probe-tier version never credited the
// bytes a match REMOVES — out = len(s) + matches x len(repl) — so once the
// no-scan worst case passed the ceiling a digit mask over an 8 MiB body was
// refused as "could build past 16 MiB" while its real output was 8 MiB; the
// same happened at any size once the invocation's remaining budget fell below
// about twice the subject. Driven at the projection with a small ceiling, which
// is the same arithmetic without allocating the 8 MiB.
func TestRegexReplaceSameSizeSubstitutionProjectsItsRealSize(t *testing.T) {
	s := strings.Repeat("0123456789", 100<<10) // 1,024,000 digits
	for _, tc := range []struct {
		pat, repl string
		want      int64
	}{
		{"[0-9]", "#", int64(len(s))},        // same size: exact
		{"[0-9]{2}", "#", int64(len(s) / 2)}, // shrinking: exact
		{"[0-9]", "", int64(len(s))},         // deleting: at most the subject, no scan needed
	} {
		re := regexp.MustCompile(tc.pat)
		limit := int64(len(s)) // below the no-scan worst case, so the matches are measured
		got := replaceSize(re, tc.repl, s, limit)
		if got != tc.want {
			t.Errorf("replaceSize(%q -> %q) = %d, want %d", tc.pat, tc.repl, got, tc.want)
		}
		if real := int64(len(re.ReplaceAllString(s, tc.repl))); real > got {
			t.Fatalf("replaceSize(%q -> %q) = %d is below the real output %d — the projection must bound it", tc.pat, tc.repl, got, real)
		}
	}
}

// A pattern that matches nothing builds nothing — the subject comes back as it
// is — so it must cost nothing either. The budget is per INVOCATION, i.e. per
// batch, and ReplaceAllString copies the subject whether or not it matched: a
// redaction list run over every record charged patterns x (the batch's body
// bytes), so ten patterns that never matched refused a 16 MiB batch — and a
// refused batch fails identically on every retry. A rewrite that DOES match is
// still charged, so the cumulative bound keeps its teeth.
func TestRegexReplaceThatMatchesNothingIsNotCharged(t *testing.T) {
	bodies := make([]string, 256)
	for i := range bodies {
		bodies[i] = strings.Repeat("b", 64<<10)
	}
	run := func(patterns []string, repl string) error {
		prog, err := Compile([]byte("logs: |\n  PATTERNS = [" + strings.Join(patterns, ", ") + "]\n" +
			"  def transform(batch):\n      for r in batch:\n          for p in PATTERNS:\n" +
			"              r.body = re.replace(p, \"" + repl + "\", r.body)\n"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = prog.logs.runLogs(logsPayload(bodies...), nil)
		return err
	}
	var misses, hits []string
	for i := range 10 {
		misses = append(misses, fmt.Sprintf("%q", fmt.Sprintf("NOPE%dSECRET", i)))
		// Matches once per record and leaves the body as it was, so every one
		// of the ten passes matches again.
		hits = append(hits, `"^b"`)
	}
	if err := run(misses, "*"); err != nil {
		t.Fatalf("ten non-matching patterns over a 16 MiB batch were refused: %v", err)
	}
	// Ten MATCHING rewrites of a 16 MiB batch are 160 MiB, past the budget.
	mustContain(t, run(hits, "b"), "in one invocation")
	// And the miss really is the subject, unchanged.
	if got := evalToAttr(t, "for r in batch:\n    r.attributes[\"out\"] = re.replace(\"zz\", \"y\", \"abc\")\n"); got != "abc" {
		t.Fatalf("got %q", got)
	}
}

// re.findall's match count grows with the SUBJECT, and the match limit is the
// only size argument FindAllString has — so the limit is the bound, and it has
// to come from the caps rather than from -1.
func TestRegexFindallBoundsItsMatchCountBeforeBuildingTheList(t *testing.T) {
	var err error
	grew, measured := allocatedBy(func() {
		err = runBody(t, "_x = re.findall(\"\", \"y\" * (8<<20))\n")
	})
	mustContain(t, err, "matches")
	// 8Mi+1 matches, unbounded, is ~400 MiB of headers and boxed values; the
	// bound stops at the per-value element cap.
	if measured && grew > 128<<20 {
		t.Fatalf("refused only after allocating %d MiB — the match limit must come from the caps", grew>>20)
	}
	// An ordinary findall is unaffected.
	if got := evalToAttr(t, "for r in batch:\n    r.attributes[\"out\"] = \",\".join(re.findall(\"\\\\d+\", \"a1 b22 c333\"))\n"); got != "1,22,333" {
		t.Fatalf("got %q", got)
	}
}

// The compiled-pattern cache is keyed by the pattern STRING, and a script may
// build one from data (`re.match(r.attributes["p"], r.body)`), so a bound on
// the number of entries is not a bound on memory: the cache would sit at its
// cap holding whatever sizes happened to land there, for the life of the
// process, with no counter and no recovery short of a restart.
func TestPatternCacheIsBoundedInBytesNotJustEntries(t *testing.T) {
	t.Run("an over-long pattern is refused rather than cached", func(t *testing.T) {
		err := runBody(t, "p = \"a\" * (1<<20)\n_x = re.match(p, \"a\")\n")
		mustContain(t, err, "over the 8192-byte limit")
	})
	t.Run("the retained pattern bytes stay bounded", func(t *testing.T) {
		resetPatternCache()
		// Every distinct pattern is a fresh entry; the cache must evict on
		// bytes as well as on count.
		big := strings.Repeat("b", maxPatternBytes-8)
		for i := range 512 {
			if _, err := compiledPattern(fmt.Sprintf("%s%04d", big, i)); err != nil {
				t.Fatal(err)
			}
		}
		c := patternCacheState(t)
		if c.bytes > maxCachedPatternBytes {
			t.Fatalf("cache retains %d pattern bytes, over the %d-byte bound (%d entries)", c.bytes, maxCachedPatternBytes, c.entries)
		}
		if c.entries > maxCachedPatterns {
			t.Fatalf("cache holds %d entries, over the %d bound", c.entries, maxCachedPatterns)
		}
	})
}

// A data-derived pattern is usually a VIEW: r.body[0:16] slices the body, and
// an escape-free lifted JSON attribute aliases the whole line. The cache keeps
// its key twice (map key, and the Regexp's own source text), so storing the
// view pinned each subject for the life of the process while reCacheLen
// counted sixteen bytes — measured at 32 MiB live under a tally of 512.
func TestPatternCacheDoesNotPinTheStringAPatternWasSlicedFrom(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector's shadow memory distorts heap measurements")
	}
	resetPatternCache()
	t.Cleanup(resetPatternCache)
	const n, bodyBytes = 32, 1 << 20
	compileViews := func() {
		for i := range n {
			body := fmt.Sprintf("p%04d", i) + strings.Repeat("x", bodyBytes)
			if _, err := compiledPattern(body[:16]); err != nil {
				t.Fatal(err)
			}
		}
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	compileViews()
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)
	if c := patternCacheState(t); c.entries != n {
		t.Fatalf("cache holds %d entries, want %d", c.entries, n)
	}
	if grew := int64(after.HeapAlloc) - int64(before.HeapAlloc); grew > n*bodyBytes/4 {
		t.Fatalf("the cache keeps %d MiB live for %d sixteen-byte patterns: it pins the strings they were sliced from", grew>>20, n)
	}
}

// log() and print() take the gate on EVERY call, suppressed ones included —
// those are the calls the throttle exists to make cheap. The gate used to be a
// sync.Map LoadOrStore that built its candidate Throttle and boxed the signal
// into an `any` key each time: two allocations per call, 2048 per 1024-record
// batch for a script logging per record.
func TestScriptLogGateIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector adds allocations")
	}
	for _, signal := range []string{"logs", "metrics", "traces", "ingest", "targets", "sample", "parse"} {
		gate := scriptLogGate(signal)
		*gate = logdedupe.Throttle{}
		scriptLogAllowed(signal) // spends the window: the calls below are suppressed
		if got := testing.AllocsPerRun(100, func() { scriptLogAllowed(signal) }); got != 0 {
			t.Errorf("%s: a suppressed gate check costs %v allocations, want 0", signal, got)
		}
		*gate = logdedupe.Throttle{}
	}
	if scriptLogGate("logs") == scriptLogGate("metrics") {
		t.Fatal("two signals share one gate: one script's logging would silence another's")
	}
}

// resetPatternCache empties the process-wide pattern cache and its tallies.
func resetPatternCache() {
	reMu.Lock()
	defer reMu.Unlock()
	reCache.Range(func(k, _ any) bool {
		reCache.Delete(k)
		return true
	})
	reCacheN, reCacheLen, reCacheInsts = 0, 0, 0
}

type patternCacheSnapshot struct{ entries, bytes, insts int }

// patternCacheState reads the cache's tallies and fails the test unless they
// describe the map exactly — the accounting must track the map, or every bound
// drifts.
func patternCacheState(t *testing.T) patternCacheSnapshot {
	t.Helper()
	reMu.Lock()
	defer reMu.Unlock()
	var got patternCacheSnapshot
	reCache.Range(func(k, v any) bool {
		got.entries++
		got.bytes += len(k.(string))
		got.insts += v.(*cachedPattern).insts
		return true
	})
	if want := (patternCacheSnapshot{reCacheN, reCacheLen, reCacheInsts}); got != want {
		t.Fatalf("the cache holds %+v but its tallies say %+v", got, want)
	}
	return got
}

// A pattern's LENGTH is not its cost: a counted repeat multiplies what it
// repeats, and a capture group multiplies what every submatch step copies.
// Inside the 8 KiB text bound, `\w{1000}` x 1000 compiled to a million
// instructions (42 MB retained per cached entry, 263 ms of compile inside one
// interpreter step) and 2,700 capture groups made one re.groups call run for
// ~18 s — both on the data-derived patterns the byte bound exists for. Both are
// now refused from the parse tree, before any compile.
func TestPatternCostIsBoundedNotJustItsLength(t *testing.T) {
	var alt []string
	for i := 0; len(strings.Join(alt, "|")) < 8000; i++ {
		alt = append(alt, fmt.Sprintf("x%d\\pL{999}", i))
	}
	for _, tc := range []struct{ name, pat, want string }{
		{"a counted repeat of a class", strings.Repeat(`\w{1000}`, 1000), "instructions"},
		{"an alternation of repeated classes", strings.Join(alt, "|"), "instructions"},
		// (Nested repeats are already bounded by regexp itself — a product of
		// counts past 1000 is "invalid repeat count" — so the multiplier
		// that reaches the cache is a counted repeat of a long subexpression.)
		{"a counted repeat of a long literal", `(?:abcdefghijklmnopqrst){1000}`, "instructions"},
		{"thousands of capture groups", strings.Repeat("(a)", 2700), "capture groups"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.pat) > maxPatternBytes {
				t.Fatalf("fixture: %d bytes is past the length bound, which would refuse it for the wrong reason", len(tc.pat))
			}
			resetPatternCache()
			start := time.Now()
			_, err := compiledPattern(tc.pat)
			mustContain(t, err, tc.want)
			// Refused from the parse, not after a compile: the million-instruction
			// compile alone was hundreds of milliseconds.
			if d := time.Since(start); d > 5*time.Second {
				t.Fatalf("the refusal took %s", d)
			}
			if c := patternCacheState(t); c.entries != 0 {
				t.Fatalf("a refused pattern was cached: %+v", c)
			}
		})
	}
	// And through a script, where it is an ordinary script error.
	mustContain(t, runBody(t, "p = \"(a)\" * 100\n_x = re.groups(p, \"a\")\n"), "capture groups")

	// What a human writes is untouched: the estimate is regexp/syntax's own
	// arithmetic, so it tracks the real program closely.
	for _, pat := range []string{
		`^(\S+) (\S+) (\S+) \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+) "([^"]*)" "([^"]*)"$`,
		`.{0,1000}`, `(?i)token=[a-z0-9]+`, `\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}`,
		strings.Repeat("(a)", maxPatternGroups),
	} {
		if _, err := compiledPattern(pat); err != nil {
			t.Errorf("%q was refused: %v", pat, err)
		}
	}
}

// The cache retains PROGRAMS, so it is bounded in their (estimated) size, not
// only in pattern text: 1 MiB of 8 KiB patterns is 128 entries, and 128
// near-limit programs would be the gigabytes the text bound was meant to stop.
func TestPatternCacheIsBoundedInInstructions(t *testing.T) {
	resetPatternCache()
	t.Cleanup(resetPatternCache)
	for i := range 64 {
		// 15k instructions each, inside maxPatternInsts, in 123 bytes.
		if _, err := compiledPattern(fmt.Sprintf("q%02d", i) + strings.Repeat(`\w{1000}`, 15)); err != nil {
			t.Fatal(err)
		}
	}
	c := patternCacheState(t)
	if c.insts > maxCachedInsts {
		t.Fatalf("cache retains %d estimated instructions, over the %d bound (%d entries)", c.insts, maxCachedInsts, c.entries)
	}
	if c.entries >= 64 {
		t.Fatalf("all %d large programs were retained; the instruction bound did not evict", c.entries)
	}
}

// The HIT path is lock-free (a sync.Map load), and insertion and eviction still
// serialise under reMu: run under -race, concurrent hits, misses and evictions
// must neither race nor drift the tallies from the map.
func TestPatternCacheConcurrentHitsAndEvictions(t *testing.T) {
	resetPatternCache()
	t.Cleanup(resetPatternCache)
	var wg sync.WaitGroup
	for g := range 4 {
		wg.Go(func() {
			for i := range 100 {
				// A shared hot pattern (hits), plus distinct large ones that
				// force instruction-bound evictions.
				if re, err := compiledPattern(`^hot[0-9]+$`); err != nil || !re.MatchString("hot42") {
					t.Errorf("hot pattern: %v", err)
					return
				}
				if _, err := compiledPattern(fmt.Sprintf(`g%di%d\w{1000}\w{500}`, g, i)); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	c := patternCacheState(t)
	if c.insts > maxCachedInsts || c.entries > maxCachedPatterns || c.bytes > maxCachedPatternBytes {
		t.Fatalf("a bound was exceeded under concurrency: %+v", c)
	}
}
