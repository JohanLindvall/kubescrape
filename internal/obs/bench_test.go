package obs_test

// What this measures, and why these four shapes.
//
// obs is the one package both binaries bump from inside their hottest loops,
// and it had no benchmark at all — so "the self-metrics cost nothing" was an
// assertion. The four shapes here are the four ways a kubescrape_* metric is
// actually touched in production:
//
//   Inc            a PRE-BOUND handle, the discipline every per-record site in
//                  this repo already follows (logenrich binds its four formats
//                  at package level, cgroupstats binds a dozen in its
//                  constructor). This is the number a per-line budget pays.
//   IncParallel    the same bump from concurrent goroutines. It is separate
//                  because the metaclient regression this campaign started from
//                  was 3.4x worse concurrent than serial: a per-call cost that
//                  looks small serially can be a contended cache line, and only
//                  the parallel arm shows it. The agent's concurrent bumpers are
//                  the ingest handlers and the scrape goroutines.
//   WithLabelValues the per-call label resolution, for the sites that legitimately
//                  cannot pre-bind (a label value known only at the call).
//   Export/Dump    the two whole-registry renders. Export is the OTLP push
//                  (-self-metrics-interval, 1m); Dump is the Prometheus scrape
//                  bridge, which the shipped chart drives every 30s because
//                  kubescrape scrapes its own pods. Both are per-INTERVAL, not
//                  per-record — measured to size them, not to optimise them.

import (
	"context"
	"strconv"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/metrics"
)

// benchReg is a private registry shaped like obs's own: enough series and
// label sets that Export/Dump walk a realistic tree rather than a fixture.
// obs.Registry itself is not used for the render benchmarks because its
// contents depend on which Register* hooks a test binary happens to have run.
func benchReg(tb testing.TB, families, labelSets int) (*metrics.Registry, []*metrics.RegCounter) {
	tb.Helper()
	r := metrics.NewRegistry()
	var bound []*metrics.RegCounter
	for f := range families {
		v := r.CounterVec("kubescrape_bench_"+strconv.Itoa(f)+"_total", "bench", "outcome", "pipeline")
		for l := range labelSets {
			c := v.WithLabelValues("outcome"+strconv.Itoa(l), "logs")
			c.Add(float64(l + 1))
			bound = append(bound, c)
		}
	}
	return r, bound
}

type discardExporter struct{ points int }

func (d *discardExporter) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	d.points += md.DataPointCount()
	return nil
}

// A pre-bound counter bump: the cost every per-record call site in this repo
// actually pays.
func BenchmarkCounterInc(b *testing.B) {
	r := metrics.NewRegistry()
	c := r.Counter("kubescrape_bench_inc_total", "bench")
	b.ReportAllocs()
	for b.Loop() {
		c.Inc()
	}
}

// The same bump from every P at once. A series is one mutex plus one map, so
// this is where a self-metric would show up as contention rather than as work.
func BenchmarkCounterIncParallel(b *testing.B) {
	r := metrics.NewRegistry()
	c := r.Counter("kubescrape_bench_incpar_total", "bench")
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Inc()
		}
	})
}

// Distinct series bumped concurrently — the shape a labelled counter has when
// each goroutine owns a different label value. Same registry, different series,
// so this isolates per-series contention from per-registry contention.
func BenchmarkCounterVecDistinctSeriesParallel(b *testing.B) {
	r := metrics.NewRegistry()
	v := r.CounterVec("kubescrape_bench_distinct_total", "bench", "outcome")
	var n int
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		n++
		c := v.WithLabelValues("outcome" + strconv.Itoa(n))
		for pb.Next() {
			c.Inc()
		}
	})
}

// Per-call label resolution: one cached-wrapper lookup under the vec's mutex.
// Single-label vecs take vecKey's alloc-free arm; two labels build a key.
func BenchmarkCounterWithLabelValues(b *testing.B) {
	r := metrics.NewRegistry()
	one := r.CounterVec("kubescrape_bench_wlv1_total", "bench", "outcome")
	two := r.CounterVec("kubescrape_bench_wlv2_total", "bench", "outcome", "pipeline")
	one.WithLabelValues("ok")
	two.WithLabelValues("ok", "logs")
	b.Run("1-label", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			one.WithLabelValues("ok").Inc()
		}
	})
	b.Run("2-label", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			two.WithLabelValues("ok", "logs").Inc()
		}
	})
}

// The OTLP push render, at the registry sizes both binaries actually reach.
// obs.go registers 137 metric families; the agent's labelled ones fan out to a
// few hundred series in total.
func BenchmarkRegistryExport(b *testing.B) {
	for _, sz := range []struct {
		name             string
		families, labels int
	}{
		{"100-series", 20, 5},
		{"500-series", 50, 10},
		{"2000-series", 100, 20},
	} {
		b.Run(sz.name, func(b *testing.B) {
			r, _ := benchReg(b, sz.families, sz.labels)
			exp := &discardExporter{}
			res := pcommon.NewResource()
			res.Attributes().PutStr("service.name", "kubescrape-agent")
			ctx := context.Background()
			b.ReportAllocs()
			for b.Loop() {
				if err := r.Export(ctx, exp, res); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// The Prometheus scrape bridge. Non-mutating by contract, so unlike Export it
// can be called repeatedly without spending interval state.
func BenchmarkRegistryDump(b *testing.B) {
	for _, sz := range []struct {
		name             string
		families, labels int
	}{
		{"100-series", 20, 5},
		{"2000-series", 100, 20},
	} {
		b.Run(sz.name, func(b *testing.B) {
			r, _ := benchReg(b, sz.families, sz.labels)
			b.ReportAllocs()
			for b.Loop() {
				if got := r.Dump(); len(got) == 0 {
					b.Fatal("empty dump")
				}
			}
		})
	}
}

// GaugeFunc/GaugeFuncVec are evaluated AT EXPORT, so a registry full of them
// pays its providers' cost once per interval. The store stats, the buffer
// stats, the self-metadata gauge and the monitors-rejected gauge all arrive
// this way.
func BenchmarkRegistryExportGaugeFuncs(b *testing.B) {
	r := metrics.NewRegistry()
	for f := range 20 {
		r.GaugeFunc("kubescrape_bench_gf_"+strconv.Itoa(f), "bench", func() float64 { return 42 })
	}
	for f := range 5 {
		vals := map[string]float64{}
		for l := range 10 {
			vals["v"+strconv.Itoa(l)] = float64(l)
		}
		r.GaugeFuncVec("kubescrape_bench_gfv_"+strconv.Itoa(f), "bench", "kind",
			func() map[string]float64 { return vals })
	}
	exp := &discardExporter{}
	res := pcommon.NewResource()
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if err := r.Export(ctx, exp, res); err != nil {
			b.Fatal(err)
		}
	}
}
