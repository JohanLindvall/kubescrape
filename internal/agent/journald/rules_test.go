package journald

import (
	"context"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// journald has the same cost levers as the container-log tailer. A node's
// journal is dominated by kubelet/containerd chatter, and before this the only
// way to sample it down was a Starlark transform — the escape hatch, not the
// ergonomic path.
func TestJournaldRulesDropEntries(t *testing.T) {
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", MatchRegexp: []string{"__line__=healthz"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	entries := []rawEntry{
		mkEntry("c1", "kubelet.service", "GET /healthz ok", "6"),
		mkEntry("c2", "kubelet.service", "pod started", "6"),
		mkEntry("c3", "kubelet.service", "probe healthz again", "6"),
	}

	exp := &captureExporter{}
	r := New(Config{Exporter: exp, FlushInterval: 20 * time.Millisecond, Rules: rules})
	r.open = fakeOpener(entries, false)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	waitFor(t, "the kept entry exported", func() bool { return len(exp.records()) == 1 })
	time.Sleep(60 * time.Millisecond)
	got := exp.records()
	if len(got) != 1 || got[0] != "pod started" {
		t.Fatalf("exported %v, want only [pod started]: the drop rule did not apply to journal entries", got)
	}
}

// Metrics must observe EVERY entry, including ones the rules then drop — the
// same ordering the tailer guarantees, so counting what you drop still works.
func TestJournaldLogMetricsSeeDroppedEntries(t *testing.T) {
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "journal_lines_total", Type: metrics.CounterType, Value: "1",
	}})
	if err != nil {
		t.Fatal(err)
	}
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", MatchRegexp: []string{"__line__=noise"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	entries := []rawEntry{
		mkEntry("c1", "kubelet.service", "noise one", "6"),
		mkEntry("c2", "kubelet.service", "real event", "6"),
		mkEntry("c3", "kubelet.service", "noise two", "6"),
	}

	exp := &captureExporter{}
	r := New(Config{Exporter: exp, FlushInterval: 20 * time.Millisecond, Rules: rules, LogMetrics: set})
	r.open = fakeOpener(entries, false)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	waitFor(t, "the kept entry exported", func() bool { return len(exp.records()) == 1 })
	time.Sleep(60 * time.Millisecond)

	// Export the set and read the counter back: it must have counted all three.
	mexp := &metricCapture{}
	if err := set.Export(context.Background(), mexp, 0); err != nil {
		t.Fatal(err)
	}
	total := mexp.sum("journal_lines_total")
	if total != 3 {
		t.Fatalf("counted %v entries, want 3: log metrics must see every entry, including rule-dropped ones", total)
	}
}

// metricCapture reads a counter's exported total.
type metricCapture struct{ md []pmetric.Metrics }

func (c *metricCapture) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	cp := pmetric.NewMetrics()
	md.CopyTo(cp)
	c.md = append(c.md, cp)
	return nil
}

func (c *metricCapture) sum(name string) float64 {
	total := 0.0
	for _, md := range c.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)
					if m.Name() != name || m.Type() != pmetric.MetricTypeSum {
						continue
					}
					dps := m.Sum().DataPoints()
					// The last point: a counter's first export is preceded by
					// synthetic zero points that give rate() a baseline.
					if dps.Len() > 0 {
						total += dps.At(dps.Len() - 1).DoubleValue()
					}
				}
			}
		}
	}
	return total
}

// convert() runs the log-metrics observation, and flush() used to call it on
// every export ATTEMPT. So a transient failure — a collector rollout, a 503, a
// deadline — re-observed every record on each retry: configured counters and
// histograms over-counted by the number of failed attempts spanning the batch,
// biasing cumulative series permanently upward and showing a spike in rate()
// during exactly the outage an operator is investigating.
//
// Delivery is at-least-once; OBSERVATION must be exactly-once per record.
func TestJournaldLogMetricsCountOncePerRecordNotPerAttempt(t *testing.T) {
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "journal_lines_total", Type: metrics.CounterType, Value: "1",
	}})
	if err != nil {
		t.Fatal(err)
	}
	entries := []rawEntry{mkEntry("c1", "kubelet.service", "one real line", "6")}

	exp := &captureExporter{failures: 2} // two transient failures, then success
	r := New(Config{Exporter: exp, FlushInterval: 20 * time.Millisecond, LogMetrics: set})
	r.open = fakeOpener(entries, false)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	waitFor(t, "the entry exported after the retries", func() bool { return len(exp.records()) == 1 })
	time.Sleep(60 * time.Millisecond)

	mexp := &metricCapture{}
	if err := set.Export(context.Background(), mexp, 0); err != nil {
		t.Fatal(err)
	}
	if got := mexp.sum("journal_lines_total"); got != 1 {
		t.Errorf("journal_lines_total = %v after 2 failed attempts and 1 delivery, want 1 — "+
			"the record is observed once per export ATTEMPT", got)
	}
}
