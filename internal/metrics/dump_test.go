// Tests for the non-mutating Registry read (dump.go) behind the /metrics
// scrape bridge.
package metrics

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Dump must leave every piece of interval state the OTLP push path depends on
// untouched: repeated dumps are identical, counters keep their synthetic-zero
// initial flag, and a counter func's delta bookkeeping (last) stays unspent.
func TestDumpNonMutating(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("kubescrape_test_total", "d")
	c.Add(3)
	r.CounterFunc("kubescrape_testfn_total", "d", func() float64 { return 100 })

	d1 := r.Dump()
	d2 := r.Dump()
	if !reflect.DeepEqual(d1, d2) {
		t.Fatalf("dumps differ:\n%v\n%v", d1, d2)
	}

	// The plain counter's sample still awaits its FIRST export: initial (the
	// synthetic-zero baseline marker) and exported must be untouched.
	for _, samp := range r.series[0].db {
		if !samp.initial || samp.exported {
			t.Fatalf("dump consumed interval state: initial=%v exported=%v", samp.initial, samp.exported)
		}
	}
	// The counter func's delta state belongs to the push path alone.
	if got := r.funcs[0].last; got != 0 {
		t.Fatalf("dump spent the counter-func delta: last=%v, want 0", got)
	}

	// Values are the live totals.
	byName := map[string]RegistrySeries{}
	for _, s := range d1 {
		byName[s.Name] = s
	}
	if v := byName["kubescrape_test_total"].Points[0].Value; v != 3 {
		t.Fatalf("counter dump = %v, want 3", v)
	}
	if s := byName["kubescrape_testfn_total"]; s.Kind != CounterType || s.Points[0].Value != 100 {
		t.Fatalf("counter-func dump = %+v, want kind counter value 100", s)
	}
}

// Histogram dumps carry the Prometheus shape directly: cumulative per-bound
// counts, total count and sum from the +Inf stream.
func TestDumpHistogramShape(t *testing.T) {
	r := NewRegistry()
	h := r.HistogramVec("kubescrape_test_seconds", "d", []float64{1, 5}, "op")
	for _, v := range []float64{0.5, 0.7, 3, 10} {
		h.WithLabelValues("x").Observe(v)
	}
	var s RegistrySeries
	for _, d := range r.Dump() {
		if d.Name == "kubescrape_test_seconds" {
			s = d
		}
	}
	if s.Kind != HistogramType || len(s.Points) != 1 {
		t.Fatalf("dump = %+v", s)
	}
	p := s.Points[0]
	if p.Count != 4 || p.Sum != 14.2 {
		t.Fatalf("count/sum = %d/%v, want 4/14.2", p.Count, p.Sum)
	}
	if !reflect.DeepEqual(s.Bounds, []float64{1, 5}) || !reflect.DeepEqual(p.Buckets, []uint64{2, 3}) {
		t.Fatalf("bounds/buckets = %v/%v, want [1 5]/[2 3]", s.Bounds, p.Buckets)
	}
	if len(p.Labels) != 1 || p.Labels[0] != [2]string{"op", "x"} {
		t.Fatalf("labels = %v", p.Labels)
	}
}

// GaugeFuncVec dumps one point per label value, live-evaluated.
func TestDumpGaugeFuncVec(t *testing.T) {
	r := NewRegistry()
	r.GaugeFuncVec("kubescrape_test_backlog", "d", "signal", func() map[string]float64 {
		return map[string]float64{"logs": 7, "metrics": 9}
	})
	d := r.Dump()
	if len(d) != 1 || len(d[0].Points) != 2 {
		t.Fatalf("dump = %+v", d)
	}
	if d[0].Points[0].Labels[0] != [2]string{"signal", "logs"} || d[0].Points[0].Value != 7 {
		t.Fatalf("point 0 = %+v", d[0].Points[0])
	}
}

// A repeated registration of one NAME reuses the series (Registry.byName) but
// keeps a gaugeFunc per registration — the shape the per-instance Register*
// hooks advertise (two DynamicMetricSets in one process each publishing the
// drop family). Dump must still emit that name ONCE: two entries become two
// const metrics with an identical name and label set, which fails prometheus
// Gather and answers 500 to every /metrics scrape, taking the go_*/process_*
// collectors down with the kubescrape_* ones.
func TestDumpMergesRepeatedFuncRegistrations(t *testing.T) {
	r := NewRegistry()
	r.CounterFunc("kubescrape_dropped_total", "d", func() float64 { return 3 })
	r.CounterFunc("kubescrape_dropped_total", "d", func() float64 { return 4 })
	r.GaugeFunc("kubescrape_live", "d", func() float64 { return 1 })
	r.GaugeFunc("kubescrape_live", "d", func() float64 { return 2 })
	r.GaugeFuncVec("kubescrape_backlog", "d", "signal", func() map[string]float64 {
		return map[string]float64{"logs": 1}
	})
	r.GaugeFuncVec("kubescrape_backlog", "d", "signal", func() map[string]float64 {
		return map[string]float64{"logs": 2, "metrics": 5}
	})

	byName := map[string]RegistrySeries{}
	for _, d := range r.Dump() {
		if _, dup := byName[d.Name]; dup {
			t.Fatalf("%s dumped twice: the /metrics scrape 500s on a duplicate name+labels", d.Name)
		}
		byName[d.Name] = d
	}

	// Merged the way Export merges them into the shared series: a counter
	// accumulates both funcs' totals, a gauge keeps the last registration's.
	if v := byName["kubescrape_dropped_total"].Points[0].Value; v != 7 {
		t.Errorf("counter funcs = %v, want 7 (3+4, as Export accumulates them)", v)
	}
	if v := byName["kubescrape_live"].Points[0].Value; v != 2 {
		t.Errorf("gauge funcs = %v, want 2 (set: the last registration wins)", v)
	}
	pts := byName["kubescrape_backlog"].Points
	if len(pts) != 2 {
		t.Fatalf("labeled gauge funcs = %+v, want one point per label value", pts)
	}
	if pts[0].Labels[0] != [2]string{"signal", "logs"} || pts[0].Value != 2 {
		t.Errorf("point 0 = %+v, want signal=logs value 2", pts[0])
	}
	if pts[1].Labels[0] != [2]string{"signal", "metrics"} || pts[1].Value != 5 {
		t.Errorf("point 1 = %+v, want signal=metrics value 5", pts[1])
	}
}

// The /metrics bridge makes Dump a SECOND concurrent reader of the registry,
// alongside the push path's Export and the producers' observations. Nothing
// may race, and no lock ordering between the registry, series and func mutexes
// may deadlock.
func TestDumpConcurrentWithObserveAndExport(t *testing.T) {
	r := NewRegistry()
	c := r.CounterVec("kubescrape_x_total", "d", "k")
	h := r.HistogramVec("kubescrape_h_seconds", "d", nil, "k")
	r.GaugeFunc("kubescrape_g", "d", func() float64 { return 1 })
	r.CounterFunc("kubescrape_cf_total", "d", func() float64 { return 2 })
	r.GaugeFuncVec("kubescrape_gv", "d", "signal", func() map[string]float64 {
		return map[string]float64{"logs": 1, "metrics": 2}
	})

	var producers sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					c.WithLabelValues("a").Inc()
					h.WithLabelValues("a").Observe(0.3)
				}
			}
		}()
	}
	var readers sync.WaitGroup
	for i := 0; i < 2; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for j := 0; j < 200; j++ {
				_ = r.Dump()
			}
		}()
	}
	readers.Add(1)
	go func() {
		defer readers.Done()
		for j := 0; j < 200; j++ {
			_ = r.Export(context.Background(), nopDumpExporter{}, pcommon.NewResource())
		}
	}()
	readers.Wait()
	close(stop)
	producers.Wait()
}

type nopDumpExporter struct{}

func (nopDumpExporter) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }
