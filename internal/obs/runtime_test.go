package obs

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type capExp struct{ md []pmetric.Metrics }

func (c *capExp) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	cp := pmetric.NewMetrics()
	md.CopyTo(cp)
	c.md = append(c.md, cp)
	return nil
}

// The Go runtime and process series must reach an OTLP export, since the
// Prometheus /metrics endpoint that used to carry them is gone.
func TestRuntimeMetricsExportOverOTLP(t *testing.T) {
	RegisterRuntimeMetrics()

	// Burn a few CPU ticks so process.cpu.time is meaningfully non-zero:
	// /proc/self/stat reports whole USER_HZ ticks (10ms), and a freshly
	// started test process can legitimately be at 0.
	spin := time.Now()
	for time.Since(spin) < 60*time.Millisecond {
		_ = strings.Repeat("x", 512)
	}

	exp := &capExp{}
	if err := Registry.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, md := range exp.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)
					if !strings.HasPrefix(m.Name(), "process.") {
						continue
					}
					switch m.Type() {
					case pmetric.MetricTypeGauge:
						if dps := m.Gauge().DataPoints(); dps.Len() > 0 {
							got[m.Name()] = dps.At(0).DoubleValue()
						}
					case pmetric.MetricTypeSum:
						// The LAST point: a counter's first export is preceded by
						// two synthetic zero points that give rate() a baseline.
						if dps := m.Sum().DataPoints(); dps.Len() > 0 {
							got[m.Name()] = dps.At(dps.Len() - 1).DoubleValue()
						}
					}
				}
			}
		}
	}

	// Series that must be present AND plausibly non-zero in any running process.
	for _, name := range []string{
		"process.runtime.go.goroutines",
		"process.runtime.go.mem.heap_alloc",
		"process.runtime.go.mem.total",
		"process.memory.rss",
		"process.open_file_descriptors",
		"process.cpu.time",
	} {
		v, ok := got[name]
		if !ok {
			t.Errorf("%s missing from the OTLP export", name)
			continue
		}
		if v <= 0 {
			t.Errorf("%s = %v, want > 0", name, v)
		}
	}
	// Registered but legitimately zero early in a process's life.
	for _, name := range []string{"process.runtime.go.gc.count", "process.uptime"} {
		if _, ok := got[name]; !ok {
			t.Errorf("%s missing from the OTLP export", name)
		}
	}
}
