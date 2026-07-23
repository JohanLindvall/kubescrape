package tailer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
)

func mustLineFilter(t *testing.T, rules []logline.LineRule) *logline.LineFilter {
	t.Helper()
	f, err := logline.NewLineFilter(rules)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// Rules drop matching records; offsets still advance so dropped lines are not
// re-read after a restart, and log metrics still see every line.
func TestRulesDrop(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.statusEvery = 30 * time.Millisecond
	tl.cfg.Enrich = true
	tl.cfg.Rules = mustLineFilter(t, []logline.LineRule{
		{Action: "drop", Match: []string{"__severity__=debug"}},
	})
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "lines_total", Type: metrics.CounterType, Value: "1",
		MatchRegexp: []string{"__line__=."},
	}})
	if err != nil {
		t.Fatal(err)
	}
	tl.cfg.LogMetrics = set
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir,
		timeNowCRI()+" stdout F level=debug noisy detail",
		timeNowCRI()+" stdout F level=info kept one",
		timeNowCRI()+" stdout F level=debug more noise",
		timeNowCRI()+" stdout F level=error kept two",
	)
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "2 kept records")
	got := exp.get()
	if got[0] != "level=info kept one" || got[1] != "level=error kept two" {
		t.Fatalf("kept records = %q", got)
	}

	// Metrics saw all four lines, not just the kept ones.
	waitFor(t, func() bool { return countMetric(t, set, "lines_total") == 4 }, "metric count 4")

	// The dropped lines' offsets committed: the file shows no lag.
	tl2 := tl // status is published by the running tailer
	waitFor(t, func() bool {
		for _, fs := range tl2.Status() {
			if fs.Lag == 0 && fs.Committed > 0 {
				return true
			}
		}
		return false
	}, "offsets committed past dropped lines")
}

// A batch where every record is dropped exports nothing but still commits.
func TestRulesAllDropped(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.statusEvery = 30 * time.Millisecond
	tl.cfg.Rules = mustLineFilter(t, []logline.LineRule{
		{Action: "drop", MatchRegexp: []string{"__line__=."}},
	})
	stop := startTailer(t, tl)
	defer stop()

	rateLines(t, dir, 0, 5)
	waitFor(t, func() bool {
		for _, fs := range tl.Status() {
			if fs.Committed > 0 && fs.Lag == 0 {
				return true
			}
		}
		return false
	}, "offsets committed with everything dropped")
	if n := len(exp.get()); n != 0 {
		t.Fatalf("exported %d records, want 0", n)
	}
}

// Sampling keeps a deterministic fraction of matching lines.
func TestRulesSample(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.cfg.Rules = mustLineFilter(t, []logline.LineRule{
		{Action: "keep", MatchRegexp: []string{"__line__=chatty"}, Sample: 0.5},
	})
	stop := startTailer(t, tl)
	defer stop()

	lines := make([]string, 0, 21)
	for i := 0; i < 20; i++ {
		lines = append(lines, fmt.Sprintf("%s stdout F chatty %02d", timeNowCRI(), i))
	}
	lines = append(lines, timeNowCRI()+" stdout F normal line")
	writeLog(t, dir, lines...)

	waitFor(t, func() bool { return len(exp.get()) == 11 }, "10 sampled + 1 unmatched")
	got := exp.get()
	if got[len(got)-1] != "normal line" {
		t.Fatalf("unmatched line missing: %q", got)
	}
}

// countMetric renders the set and returns the total of a counter.
func countMetric(t *testing.T, set *metrics.DynamicMetricSet, name string) float64 {
	t.Helper()
	exp := &capMetricsExporter{}
	if err := set.Export(t.Context(), exp, 0); err != nil {
		t.Fatal(err)
	}
	return exp.total(name)
}

// capMetricsExporter captures exported metrics for countMetric. Payloads are
// deep-copied: Export reuses and clears its payload after each ExportMetrics
// call (the real client has marshaled it by then).
type capMetricsExporter struct{ md []pmetric.Metrics }

func (c *capMetricsExporter) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	cp := pmetric.NewMetrics()
	md.CopyTo(cp)
	c.md = append(c.md, cp)
	return nil
}

func (c *capMetricsExporter) total(name string) float64 {
	var sum float64
	for _, md := range c.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					if ms.At(k).Name() != name || ms.At(k).Type() != pmetric.MetricTypeSum {
						continue
					}
					dps := ms.At(k).Sum().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						sum += dps.At(d).DoubleValue()
					}
				}
			}
		}
	}
	return sum
}
