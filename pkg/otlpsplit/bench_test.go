package otlpsplit

// The COMMON path here is the one that does not split: every export measures
// its payload's exact proto size to decide, and only an over-cap payload pays
// anything more. These benchmarks are about that decision's cost on the shapes
// producers actually emit — a tailer flush, a promscrape chunk, a KSM split —
// with the splitting path measured beside it for scale.

import (
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

func benchResource(a pcommon.Map, i int) {
	a.PutStr("k8s.node.name", "node1")
	a.PutStr("k8s.namespace.name", fmt.Sprintf("team-%d", i%16))
	a.PutStr("k8s.pod.name", fmt.Sprintf("checkout-%d-7f9c4b8d5-abcde", i))
	a.PutStr("k8s.pod.uid", fmt.Sprintf("pod-uid-%d", i))
	a.PutStr("k8s.container.name", "app")
	a.PutStr("container.id", fmt.Sprintf("%064x", i))
	a.PutStr("service.name", "checkout")
	a.PutStr("service.namespace", fmt.Sprintf("team-%d", i%16))
	a.PutStr("service.instance.id", fmt.Sprintf("pod-uid-%d/app", i))
	a.PutStr("k8s.cluster.name", "eu-west-1-prod")
}

// benchLogs is a tailer flush: a resource per file, records carrying a real
// log line.
func benchLogs(resources, records int) plog.Logs {
	ld := plog.NewLogs()
	for r := 0; r < resources; r++ {
		rl := ld.ResourceLogs().AppendEmpty()
		benchResource(rl.Resource().Attributes(), r)
		sl := rl.ScopeLogs().AppendEmpty()
		for i := 0; i < records; i++ {
			lr := sl.LogRecords().AppendEmpty()
			lr.Body().SetStr(`level=info ts=2026-08-18T09:31:07.442Z caller=handler.go:214 msg="request completed" method=GET path=/api/v2/cart status=200 duration_ms=17.4`)
			lr.SetTimestamp(pcommon.Timestamp(1755500000000000000 + int64(i)))
			lr.Attributes().PutStr("log.file.name", "app.log")
		}
	}
	return ld
}

// benchMetrics is a promscrape chunk (one resource, many points) or a KSM
// split (many resources, few points each).
func benchMetrics(resources, metricsN, points int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	for r := 0; r < resources; r++ {
		rm := md.ResourceMetrics().AppendEmpty()
		benchResource(rm.Resource().Attributes(), r)
		sm := rm.ScopeMetrics().AppendEmpty()
		for m := 0; m < metricsN; m++ {
			me := sm.Metrics().AppendEmpty()
			me.SetName(fmt.Sprintf("http_server_request_duration_seconds_%d", m))
			me.SetUnit("s")
			dps := me.SetEmptySum().DataPoints()
			for p := 0; p < points; p++ {
				dp := dps.AppendEmpty()
				dp.SetDoubleValue(float64(p))
				dp.SetTimestamp(pcommon.Timestamp(1755500000000000000))
				dp.Attributes().PutStr("route", fmt.Sprintf("/api/v2/item/%d", p))
			}
		}
	}
	return md
}

// The decision alone, on payloads that fit: this is what every export pays.
func BenchmarkFitsLogs(b *testing.B) {
	for _, c := range []struct {
		name      string
		res, recs int
	}{
		{"tailer-flush/8x256", 8, 256},
		{"single/1x2000", 1, 2000},
	} {
		ld := benchLogs(c.res, c.recs)
		size := logMarshaler.LogsSize(ld)
		b.Run(fmt.Sprintf("%s/%dKiB", c.name, size>>10), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				if parts := Logs(ld, DefaultMaxBytes); len(parts) != 1 {
					b.Fatalf("split into %d parts; this payload must fit", len(parts))
				}
			}
		})
	}
}

func BenchmarkFitsMetrics(b *testing.B) {
	for _, c := range []struct {
		name                  string
		res, metricsN, points int
	}{
		{"promscrape-chunk/1x200x50", 1, 200, 50},
		{"ksm-split/2000x2x1", 2000, 2, 1},
	} {
		md := benchMetrics(c.res, c.metricsN, c.points)
		size := metricMarshaler.MetricsSize(md)
		b.Run(fmt.Sprintf("%s/%dKiB", c.name, size>>10), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				if parts := Metrics(md, DefaultMaxBytes); len(parts) != 1 {
					b.Fatalf("split into %d parts; this payload must fit", len(parts))
				}
			}
		})
	}
}

// What the SIZE decision costs against the MARSHAL the exporter runs on the
// same payload straight afterwards. The size walk is unavoidable — the cap is
// on encoded bytes — so the only question this answers is how large a fraction
// of the send path it is.
func BenchmarkSizeVsMarshal(b *testing.B) {
	ld := benchLogs(8, 256)
	b.Run("size", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = logMarshaler.LogsSize(ld)
		}
	})
	b.Run("marshal", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto(); err != nil {
				b.Fatal(err)
			}
		}
	})
	md := benchMetrics(1, 200, 50)
	b.Run("metrics-size", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = metricMarshaler.MetricsSize(md)
		}
	})
	b.Run("metrics-marshal", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := pmetricotlp.NewExportRequestFromMetrics(md).MarshalProto(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// The splitting path, for scale: a payload well past the cap.
func BenchmarkSplitLogs(b *testing.B) {
	ld := benchLogs(8, 4000) // ~12x the cap
	size := logMarshaler.LogsSize(ld)
	b.ReportAllocs()
	b.SetBytes(int64(size))
	for b.Loop() {
		if parts := Logs(ld, DefaultMaxBytes); len(parts) < 2 {
			b.Fatalf("expected a split, got %d part(s)", len(parts))
		}
	}
}
