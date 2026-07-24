package hostmetrics

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

type capExp struct{ md []pmetric.Metrics }

func (c *capExp) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	c.md = append(c.md, md)
	return nil
}

// Collect against the real /proc of the test host: the core families must be
// present with sane values (this is Linux-only, like the agent).
func TestCollectRealProc(t *testing.T) {
	c, err := New(Config{ProcPath: "/proc", Node: "testnode", Exporter: &capExp{}, Interval: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	md := c.collect()
	rm := md.ResourceMetrics().At(0)
	if v, _ := rm.Resource().Attributes().Get("service.name"); v.Str() != "node" {
		t.Fatalf("service.name = %q", v.Str())
	}
	ms := rm.ScopeMetrics().At(0).Metrics()
	got := map[string]pmetric.Metric{}
	for i := 0; i < ms.Len(); i++ {
		got[ms.At(i).Name()] = ms.At(i)
	}
	for _, want := range []string{
		"node_cpu_seconds_total", "node_memory_MemTotal_bytes",
		"node_load1", "node_boot_time_seconds", "node_network_receive_bytes_total",
	} {
		if _, ok := got[want]; !ok {
			t.Errorf("missing %s", want)
		}
	}
	// CPU counters must be cumulative monotonic sums with mode+cpu attrs.
	cpu := got["node_cpu_seconds_total"]
	if cpu.Type() != pmetric.MetricTypeSum || !cpu.Sum().IsMonotonic() {
		t.Fatal("cpu seconds must be a monotonic sum")
	}
	if cpu.Sum().DataPoints().Len() < 8 {
		t.Fatalf("cpu points = %d", cpu.Sum().DataPoints().Len())
	}
	if mt, _ := got["node_memory_MemTotal_bytes"].Gauge().DataPoints().At(0).DoubleValue(), 0; mt <= 0 {
		t.Fatal("MemTotal not positive")
	}
}
