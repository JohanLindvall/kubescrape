package tailer

import (
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

// The tailer's metricResolver must resolve metric values/labels and rule keys
// against RECORD attributes (line-derived, via logattrs) first and RESOURCE
// attributes (k8s metadata) second — the pooled resolver's per-record binding.
// The metrics package tests these semantics with fake closures; this pins the
// tailer's actual wiring.
func TestMetricResolverRecordAndResourceAttrs(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)

	// Lift the JSON key "dur" onto the log RECORD as attribute "req.ms".
	ex, err := logattrs.New(&logattrs.Config{Rules: []logattrs.Rule{
		{Key: "dur", Attribute: "req.ms"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	tl.cfg.LogAttrs = ex

	// Value from the RECORD attribute; label from a RESOURCE attribute
	// (k8s.pod.name comes from fakeMeta's metadata).
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "req_ms_total", Type: metrics.CounterType, Value: "req.ms",
		Labels: []string{"pod=$k8s.pod.name"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	tl.cfg.LogMetrics = set

	// A drop rule keyed on the RECORD attribute (metricResolver.ruleLookup's
	// attribute arm): lines with req.ms=13 are dropped from export.
	tl.cfg.Rules = mustLineFilter(t, []logline.LineRule{
		{Action: "drop", Match: []string{"req.ms=13"}},
	})

	dropped := obs.LogRulesDropped.Value()
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir,
		timeNowCRI()+` stdout F {"dur": 40, "msg": "a"}`,
		timeNowCRI()+` stdout F {"dur": 13, "msg": "unlucky"}`,
		timeNowCRI()+` stdout F {"dur": 2, "msg": "b"}`,
	)
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "2 kept records")
	waitFor(t, func() bool { return obs.LogRulesDropped.Value()-dropped == 1 }, "1 rule-dropped record")

	// Metrics saw all three lines (metrics run before rules): 40+13+2.
	waitFor(t, func() bool { return countMetric(t, set, "req_ms_total") == 55 }, "metric sum 55")

	// The label resolved from the file's RESOURCE attributes.
	expm := &capMetricsExporter{}
	if err := set.Export(t.Context(), expm, 0); err != nil {
		t.Fatal(err)
	}
	if !expm.hasLabel("req_ms_total", "pod", "pod1") {
		t.Fatal("label pod=pod1 (resource attribute) not resolved")
	}
}

// hasLabel reports whether any exported data point of the named metric carries
// the given label value.
func (c *capMetricsExporter) hasLabel(name, key, val string) bool {
	for _, md := range c.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)
					if m.Name() != name {
						continue
					}
					dps := m.Sum().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						if v, ok := dps.At(l).Attributes().Get(key); ok && v.Str() == val {
							return true
						}
					}
				}
			}
		}
	}
	return false
}
