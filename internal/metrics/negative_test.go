package metrics

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// A counter's exported form is an OTLP Sum with IsMonotonic(true) and
// CUMULATIVE temporality, and its StartTimestamp only ever moves when the
// stream is (re)admitted. So a negative observation folded into the running
// total ships a DECREASE on an unchanged start — a counter reset this process
// never declared, which rate()/increase() reads as one and adds the whole new
// value on top of everything already counted, permanently inflating the rate.
//
// The value is extracted from tenant-authored log content, so a workload can
// cause it at will. It is refused instead, and this asserts on the RENDERED
// pmetric because the defect is only visible there: the store's running total
// going down is the mechanism, but the monotonic flag and the unchanged start
// timestamp are what make it a lie.
func TestNegativeValueIsRefusedSoACounterStaysMonotonic(t *testing.T) {
	setTimeForTest(time.Unix(1_700_900_000, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{
		Name: "bytes_delta_total", Type: CounterType, ValueRegexp: `delta=(-?[0-9.]+)`,
	}})
	if err != nil {
		t.Fatal(err)
	}

	set.Add(nil, nil, noRes(), "delta=1000")
	first := exportOne(t, set, "bytes_delta_total")
	if first.Type() != pmetric.MetricTypeSum || !first.Sum().IsMonotonic() {
		t.Fatalf("a counter must render as a monotonic Sum, got type=%v", first.Type())
	}
	firstVal, firstStart := sumPoint(t, first)

	set.Add(nil, nil, noRes(), "delta=-500")
	second := exportOne(t, set, "bytes_delta_total")
	secondVal, secondStart := sumPoint(t, second)

	if secondStart != firstStart {
		t.Fatalf("start timestamp moved (%v -> %v); this test only means something while it does not",
			firstStart, secondStart)
	}
	if secondVal < firstVal {
		t.Errorf("the monotonic cumulative sum DECREASED on an unchanged start timestamp: %v -> %v; "+
			"rate() reads that as a counter reset and adds %v on top of everything already counted",
			firstVal, secondVal, secondVal)
	}
	if secondVal != 1000 {
		t.Errorf("counter = %v, want 1000 (the negative observation is refused, not folded in)", secondVal)
	}
	if got := set.DroppedNegative(); got != 1 {
		t.Errorf("DroppedNegative = %d, want 1; a refusal nothing counts is invisible loss", got)
	}
	if got := set.DroppedNaN(); got != 0 {
		t.Errorf("DroppedNaN = %d, want 0: a negative is finite, and the two refusals have different remedies", got)
	}
}

// sumPoint reads the LIVE data point of a rendered Sum — its value and the
// StartTimestamp that decides whether a change in that value is a reset.
//
// It is the last point because a counter's first export brings two synthetic
// zero-baseline points ahead of it (renderNumber), all three sharing the one
// start stamp.
func sumPoint(t *testing.T, m pmetric.Metric) (float64, pcommon.Timestamp) {
	t.Helper()
	dps := m.Sum().DataPoints()
	if dps.Len() == 0 {
		t.Fatalf("%s exported no data points", m.Name())
	}
	dp := dps.At(dps.Len() - 1)
	return dp.DoubleValue(), dp.StartTimestamp()
}

// A summary carries a running count and sum, and its sum reaches Prometheus as
// the counter-typed <name>_sum — so it takes the same refusal as a counter, for
// the same reason. This is the half a guard written only against
// MetricTypeSum would miss.
func TestNegativeValueIsRefusedForASummarySum(t *testing.T) {
	setTimeForTest(time.Unix(1_700_900_100, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{
		Name: "d_seconds", Type: SummaryType, ValueRegexp: `d=(-?[0-9.]+)`,
	}})
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, nil, noRes(), "d=10")
	set.Add(nil, nil, noRes(), "d=-40")

	m := exportOne(t, set, "d_seconds")
	if m.Type() != pmetric.MetricTypeSummary {
		t.Fatalf("type = %v, want Summary", m.Type())
	}
	dps := m.Summary().DataPoints()
	if dps.Len() != 1 {
		t.Fatalf("data points = %d, want 1", dps.Len())
	}
	dp := dps.At(0)
	if dp.Sum() < 0 {
		t.Errorf("summary sum = %v; a negative sum on a stream Prometheus reads as a counter is the same "+
			"undeclared reset a counter suffers", dp.Sum())
	}
	if dp.Sum() != 10 || dp.Count() != 1 {
		t.Errorf("summary = {count:%d sum:%v}, want {1 10}: the negative observation is refused whole, "+
			"so it moves neither half", dp.Count(), dp.Sum())
	}
	if got := set.DroppedNegative(); got != 1 {
		t.Errorf("DroppedNegative = %d, want 1", got)
	}
}

// The guard is a REFUSAL on two kinds, not a sign filter on the package. A
// gauge's ordinary range is signed (that is what add/sub and the min/max
// windows are for) and a histogram's bucket bounds are operator-configured and
// validated only as INCREASING — so a distribution over signed values is a
// configuration this package supports on purpose. Refusing there would
// silently delete points from a metric designed to hold them.
func TestNegativeValueIsStillAdmittedWhereTheKindIsNotMonotonic(t *testing.T) {
	setTimeForTest(time.Unix(1_700_900_200, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{
		{Name: "temp_celsius", Type: GaugeType, ValueRegexp: `temp=(-?[0-9.]+)`},
		{Name: "drift_seconds", Type: HistogramType, ValueRegexp: `drift=(-?[0-9.]+)`,
			Buckets: []float64{-10, 0, 10}},
	})
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, nil, noRes(), "temp=-12.5")
	set.Add(nil, nil, noRes(), "drift=-3")

	g := exportOne(t, set, "temp_celsius")
	if got := g.Gauge().DataPoints().At(0).DoubleValue(); got != -12.5 {
		t.Errorf("gauge = %v, want -12.5: a gauge legitimately goes negative", got)
	}
	h := exportOne(t, set, "drift_seconds")
	hdp := h.Histogram().DataPoints().At(0)
	if hdp.Count() != 1 || hdp.Sum() != -3 {
		t.Errorf("histogram = {count:%d sum:%v}, want {1 -3}: bounds may be negative, so the observation "+
			"is the operator's declared choice", hdp.Count(), hdp.Sum())
	}
	if got := set.DroppedNegative(); got != 0 {
		t.Errorf("DroppedNegative = %d, want 0: only counter and summary refuse", got)
	}
}

// -Inf is both negative and non-finite, and it must be reported as non-finite:
// that is the sharper diagnosis (the extraction produced something that is not
// a number at all), and it is the counter an operator already alerts on.
func TestNegativeInfinityIsCountedAsNonFiniteAndNotAsNegative(t *testing.T) {
	setTimeForTest(time.Unix(1_700_900_300, 0))
	defer testEpoch.Store(0)

	s := newTestSeries(seriesSpec{name: "c", kind: kindCounter, expiration: time.Hour})
	r := pcommon.NewMap()
	s.observe(nil, math.Inf(-1), resourceAccum(r), r, nil)
	if got, want := s.drops.NaN(), uint64(1); got != want {
		t.Errorf("NaN drops = %d, want %d", got, want)
	}
	if got := s.drops.Negative(); got != 0 {
		t.Errorf("negative drops = %d, want 0 (-Inf is diagnosed as non-finite)", got)
	}
}

// The counter says the rate; only the line can reach the rule whose
// value/valueRegexp is extracting a signed quantity onto a monotonic type. It
// is throttled, because a workload logging a negative delta does it on every
// line and one sweep goroutine serves every log file on the node.
func TestNegativeObservationNamesTheMetricAndIsThrottled(t *testing.T) {
	setTimeForTest(time.Unix(1_700_900_400, 0))
	defer testEpoch.Store(0)

	log, buf := capture()
	s := newTestSeries(seriesSpec{name: "bytes_total", kind: kindCounter, expiration: time.Hour, log: log})
	r := pcommon.NewMap()
	for range 2 {
		s.observe(labels{}.set("path", "/a"), -5, resourceAccum(r), r, nil)
	}
	line := buf.String()
	if !strings.Contains(line, "level=WARN") || !strings.Contains(line, "metric=bytes_total") {
		t.Errorf("want a WARN naming the metric, got:\n%s", line)
	}
	if !strings.Contains(line, "value=-5") {
		t.Errorf("want the offending value on the line, got:\n%s", line)
	}
	if !strings.Contains(line, "log-metric") {
		t.Errorf("a DynamicMetricSet refusal must speak the log-derived vocabulary, got:\n%s", line)
	}
	if !strings.Contains(line, "kubescrape_log_metrics_dropped_negative_total") {
		t.Errorf("the line must name the counter carrying the rate, got:\n%s", line)
	}
	if n := strings.Count(line, "level=WARN"); n != 1 {
		t.Errorf("WARN lines = %d, want 1 (the second refusal is inside the hour):\n%s", n, line)
	}
}

// The negative and non-finite notices hold SEPARATE throttles. The two
// conditions co-occur on a rule whose extraction is simply wrong, and one
// shared gate would let whichever fired first silence the other for the hour —
// leaving the operator one of the two remedies.
func TestNegativeAndNonFiniteNoticesDoNotSilenceEachOther(t *testing.T) {
	setTimeForTest(time.Unix(1_700_900_500, 0))
	defer testEpoch.Store(0)

	log, buf := capture()
	s := newTestSeries(seriesSpec{name: "c", kind: kindCounter, expiration: time.Hour, log: log})
	r := pcommon.NewMap()
	s.observe(nil, math.NaN(), resourceAccum(r), r, nil)
	s.observe(nil, -1, resourceAccum(r), r, nil)

	line := buf.String()
	if n := strings.Count(line, "level=WARN"); n != 2 {
		t.Fatalf("WARN lines = %d, want 2 (one per condition):\n%s", n, line)
	}
	if !strings.Contains(line, "non-finite") || !strings.Contains(line, "negative") {
		t.Errorf("want both diagnoses on the log, got:\n%s", line)
	}
}

// RegCounter.Add documents "must be >= 0" and never enforced it. It reaches the
// same guard now, so the documented invariant is true rather than merely
// written down — and the refusal speaks the SELF-metric vocabulary, since the
// Registry's drops are deliberately unpublished and a line citing
// kubescrape_log_metrics_dropped_* would point at a counter that can never
// move for it.
func TestRegistryCounterRefusesANegativeAddAndSaysSoAsASelfMetric(t *testing.T) {
	setTimeForTest(time.Unix(1_700_900_600, 0))
	defer testEpoch.Store(0)

	r := NewRegistry()
	c := r.Counter("kubescrape_test_thing_total", "help")
	c.Add(7)
	c.Add(-3)
	if got := c.Value(); got != 7 {
		t.Errorf("counter = %v, want 7 (the negative Add is refused, not folded in)", got)
	}
	if got := r.drops.Negative(); got != 1 {
		t.Errorf("registry negative drops = %d, want 1", got)
	}

	log, buf := capture()
	s := newTestSeries(seriesSpec{name: "kubescrape_x_total", kind: kindCounter, role: roleSelfMetric,
		expiration: time.Hour, log: log})
	res := pcommon.NewMap()
	s.observe(nil, -1, resourceAccum(res), res, nil)
	s.observe(nil, math.NaN(), resourceAccum(res), res, nil)
	line := buf.String()
	if strings.Contains(line, "log-metric") {
		t.Errorf("a Registry refusal must not be described as a log-metric observation:\n%s", line)
	}
	if !strings.Contains(line, "self-metric") {
		t.Errorf("want the self-metric vocabulary, got:\n%s", line)
	}
	if strings.Contains(line, "kubescrape_log_metrics_dropped") {
		t.Errorf("a Registry refusal must not cite the log-metrics drop counters, which are flat for it:\n%s", line)
	}
}

// A set built as a LITERAL leaves log nil, which is the shape this package's
// own tests use to reach retain and the export loop. The permanent-rejection
// branch used the raw field where every other line in the file goes through
// the nil-safe helper, so the one branch whose whole job is to report data
// loss panicked instead — in a repo that carries no recover() by design.
func TestPermanentRejectionReportsWithoutALogger(t *testing.T) {
	setTimeForTest(time.Unix(1_700_900_700, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{Name: "c_total", Type: CounterType, Value: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	set.log = nil // the literal-construction shape, reached through the real compiler
	set.permanent = func(error) bool { return true }
	set.Add(nil, nil, noRes(), "anything")

	// Must not panic.
	if err := set.Export(context.Background(), failExporter{}, 0); err == nil {
		t.Fatal("want the exporter's error back")
	}
	if got := set.DroppedUndelivered(); got == 0 {
		t.Error("a permanently rejected chunk must be counted as undelivered")
	}
}

type failExporter struct{}

func (failExporter) ExportMetrics(context.Context, pmetric.Metrics) error {
	return errors.New("collector refused the payload")
}

// One cycle used to log an Error saying a chunk was DROPPED and a Warn saying
// the samples were RETAINED. The Warn is the line the throttle guarantees an
// operator sees first and repeatedly, so they read it and stopped chasing the
// drop.
func TestPermanentRejectionIsNotNarratedAsRetained(t *testing.T) {
	setTimeForTest(time.Unix(1_700_900_800, 0))
	defer testEpoch.Store(0)

	log, buf := capture()
	set, err := newTestSet([]Dynamic{{Name: "c_total", Type: CounterType, Value: "1"}},
		WithLogger(log), WithPermanentClassifier(func(error) bool { return true }))
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, nil, noRes(), "anything")
	set.noteExport(set.export(context.Background(), failExporter{}, 0))

	line := buf.String()
	if strings.Contains(line, "failed; the undelivered samples are retained and re-offered") {
		t.Errorf("the transition Warn claims the samples were kept, in the same cycle that dropped them:\n%s", line)
	}
	if !strings.Contains(line, "LOST") {
		t.Errorf("want the Warn to name the loss, got:\n%s", line)
	}

	// The transient class keeps the retention wording, which is true of it.
	buf.Reset()
	set2, err := newTestSet([]Dynamic{{Name: "c2_total", Type: CounterType, Value: "1"}}, WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	set2.Add(nil, nil, noRes(), "anything")
	set2.noteExport(set2.export(context.Background(), failExporter{}, 0))
	if line := buf.String(); !strings.Contains(line, "failed; the undelivered samples are retained and re-offered") {
		t.Errorf("a transient failure DOES retain, and must still say so:\n%s", line)
	}
}

// A permanent rejection recurs: the store keeps every live series, so the next
// snapshot re-renders the same resources into the same refused chunk on every
// interval, on every node. The per-chunk Error was the one line on this path
// with no throttle — one Error per chunk per interval per node for as long as
// the rejection lasted, beside a transition Warn that was throttled. The
// counter carries the rate; the line restates the running total.
func TestPermanentRejectionLineIsThrottled(t *testing.T) {
	setTimeForTest(time.Unix(1_700_900_900, 0))
	defer testEpoch.Store(0)

	log, buf := capture()
	set, err := newTestSet([]Dynamic{{Name: "c_total", Type: CounterType, Value: "1"}},
		WithLogger(log), WithPermanentClassifier(func(error) bool { return true }))
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, nil, noRes(), "anything")
	for range 3 {
		set.noteExport(set.export(context.Background(), failExporter{}, 0))
	}

	out := buf.String()
	const msg = `msg="dropping a permanently rejected log-metrics chunk"`
	errs := 0
	for ln := range strings.SplitSeq(out, "\n") {
		if strings.Contains(ln, msg) && strings.Contains(ln, "level=ERROR") {
			errs++
			if !strings.Contains(ln, "dropped=") {
				t.Errorf("the line must restate the running total:\n%s", ln)
			}
		}
	}
	if errs != 1 {
		t.Errorf("permanent-rejection ERROR lines over three cycles = %d, want 1 (the repeats are Debug until the re-warn interval):\n%s", errs, out)
	}
	if n := strings.Count(out, msg); n != 3 {
		t.Errorf("permanent-rejection lines at any level = %d, want one per cycle (the suppressed ones at Debug):\n%s", n, out)
	}
	if got := set.DroppedUndelivered(); got != 3 {
		t.Errorf("dropped-undelivered = %d, want 3: the live sample was rejected once per cycle", got)
	}
}

// sequenceExporter answers its calls with errs in order, repeating the last.
type sequenceExporter struct {
	errs  []error
	calls int
}

func (e *sequenceExporter) ExportMetrics(context.Context, pmetric.Metrics) error {
	err := e.errs[min(e.calls, len(e.errs)-1)]
	e.calls++
	return err
}

// In a cycle where one chunk fails TRANSIENTLY and a later one PERMANENTLY, the
// export used to return the first error it met — so noteExport's Warn, the one
// that says "rejected PERMANENTLY and those observations are LOST", carried the
// transient error as the reason, and the operator chasing the loss read an
// unrelated outage. The permanent error is the one that explains the loss.
func TestPermanentRejectionIsReportedWithThePermanentError(t *testing.T) {
	setTimeForTest(time.Unix(1_700_901_000, 0))
	defer testEpoch.Store(0)

	errTransient := errors.New("TRANSIENT-unavailable")
	errPermanent := errors.New("PERMANENT-rejection")
	log, buf := capture()
	set, err := newTestSet([]Dynamic{{Name: "c_total", Type: CounterType, Value: "1"}},
		WithLogger(log), WithPermanentClassifier(func(err error) bool { return errors.Is(err, errPermanent) }))
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, nil, res(map[string]string{"k8s.pod.name": "a"}), "anything")
	set.Add(nil, nil, res(map[string]string{"k8s.pod.name": "b"}), "anything")

	// maxBytes 1: every resource is its own chunk, so the two sends are the
	// transient one and then the permanent one.
	exp := &sequenceExporter{errs: []error{errTransient, errPermanent}}
	dropped, gotErr := set.export(context.Background(), exp, 1)
	if exp.calls != 2 {
		t.Fatalf("sends = %d, want 2 (one chunk per resource)", exp.calls)
	}
	if dropped != 1 {
		t.Fatalf("dropped resources = %d, want 1", dropped)
	}
	if !errors.Is(gotErr, errPermanent) {
		t.Fatalf("export returned %v, want the PERMANENT error that explains the loss", gotErr)
	}

	set.noteExport(dropped, gotErr)
	for ln := range strings.SplitSeq(buf.String(), "\n") {
		if strings.Contains(ln, "LOST") && !strings.Contains(ln, "error=PERMANENT-rejection") {
			t.Errorf("the loss Warn carries the wrong reason:\n%s", ln)
		}
	}
}

// A transform script's emit_metric reaches the same value guard as a rule's
// extraction — refused, counted, never admitted, and NOT a script error (which
// would fail the export and have the tailer re-run the batch forever). But the
// refusal line's remedy used to say "check the rule's value/valueRegexp", a
// source that played no part: the script computed the value.
func TestEmitMetricRefusalNamesTheScriptNotARule(t *testing.T) {
	setTimeForTest(time.Unix(1_700_901_100, 0))
	defer testEpoch.Store(0)

	log, buf := capture()
	set, err := newTestSet([]Dynamic{{Name: "c_total", Type: CounterType, Value: "1"}}, WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []float64{math.NaN(), -2} {
		if err := set.EmitDirect("c_total", v, nil, noRes()); err != nil {
			t.Fatalf("EmitDirect(%v) = %v, want nil: a refused value is counted, not a script error", v, err)
		}
	}
	if set.DroppedNaN() != 1 || set.DroppedNegative() != 1 {
		t.Fatalf("drops nan=%d negative=%d, want 1 each", set.DroppedNaN(), set.DroppedNegative())
	}
	if n := set.rules[0].series.count; n != 0 {
		t.Fatalf("a refused value was admitted: %d series", n)
	}
	out := buf.String()
	if n := strings.Count(out, "emit_metric"); n != 2 {
		t.Errorf("want both refusal lines to name emit_metric as the source (%d did):\n%s", n, out)
	}
	if strings.Contains(out, "value/valueRegexp against") {
		t.Errorf("a script-supplied value must not send the operator to a rule's extraction:\n%s", out)
	}

	// A rule's own extraction keeps the rule remedy.
	buf.Reset()
	set.rules[0].series.lastNonFinite, set.rules[0].series.lastNegative = 0, 0 // re-open both hourly gates
	s := set.rules[0].series
	r := pcommon.NewMap()
	s.observe(nil, math.NaN(), resourceAccum(r), r, nil)
	if out := buf.String(); !strings.Contains(out, "value/valueRegexp") || strings.Contains(out, "emit_metric") {
		t.Errorf("a rule-fed refusal must name the rule's extraction:\n%s", out)
	}
}

// Every Registry series is built in obs's package-level var blocks, BEFORE
// either main installs its handler with slog.SetDefault. A logger captured at
// construction is therefore the stdlib bridge: the refusal Warn came out as
// `level=INFO msg="WARN ..."` with every attribute flattened into the message,
// or not at all under a Warn-level handler — and for a Registry refusal, whose
// count is deliberately unpublished, that line is the only signal there is.
func TestRegistryRefusalLogsThroughTheHandlerInstalledAfterConstruction(t *testing.T) {
	setTimeForTest(time.Unix(1_700_901_200, 0))
	defer testEpoch.Store(0)

	r := NewRegistry() // built under whatever default the test binary started with
	c := r.Counter("kubescrape_test_late_handler_total", "help")

	buf := &syncBuf{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	c.Add(-1)
	out := buf.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "metric=kubescrape_test_late_handler_total") {
		t.Fatalf("want a structured WARN naming the metric through the handler installed after construction, got:\n%q", out)
	}
}

// The Registry's door must stamp roleSelfMetric. roleLogMetric is the zero
// value, so dropping the role from Registry.add compiles, and every test that
// builds a series by hand with the role spelled out keeps passing — while every
// self-metric refusal starts sending the operator to a
// kubescrape_log_metrics_dropped_* counter that is flat for it by construction.
// This goes through Registry.Counter itself, and through slog.Default() the
// way every Registry series logs (its logger is resolved at the call).
func TestRegistryRefusalSpeaksTheSelfMetricVocabulary(t *testing.T) {
	setTimeForTest(time.Unix(1_700_901_300, 0))
	defer testEpoch.Store(0)

	r := NewRegistry()
	neg := r.Counter("kubescrape_test_vocab_negative_total", "help")
	nan := r.Counter("kubescrape_test_vocab_nan_total", "help")

	log, buf := capture()
	prev := slog.Default()
	slog.SetDefault(log)
	defer slog.SetDefault(prev)

	neg.Add(-1)
	nan.Add(math.NaN())
	out := buf.String()
	if n := strings.Count(out, "level=WARN"); n != 2 {
		t.Fatalf("want one WARN per refusal (2), got %d:\n%s", n, out)
	}
	if !strings.Contains(out, "self-metric") {
		t.Errorf("a Registry refusal must speak the self-metric vocabulary:\n%s", out)
	}
	if strings.Contains(out, "log-metric") || strings.Contains(out, "kubescrape_log_metrics_dropped") {
		t.Errorf("a Registry refusal must not describe itself as a log-metric or cite the log-metrics drop counters:\n%s", out)
	}
}
