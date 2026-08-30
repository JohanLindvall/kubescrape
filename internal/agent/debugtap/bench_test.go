package debugtap

// The tap is in the export chain unconditionally, so its ZERO-SUBSCRIBER cost
// is paid by every export in every deployment. The package doc promises "one
// atomic load per export"; this is what checks it.

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func benchLogs(resources, records int) plog.Logs {
	ld := plog.NewLogs()
	for r := 0; r < resources; r++ {
		rl := ld.ResourceLogs().AppendEmpty()
		a := rl.Resource().Attributes()
		a.PutStr("k8s.namespace.name", fmt.Sprintf("team-%d", r%16))
		a.PutStr("k8s.pod.name", fmt.Sprintf("checkout-%d", r))
		a.PutStr("service.name", "checkout")
		sl := rl.ScopeLogs().AppendEmpty()
		for i := 0; i < records; i++ {
			lr := sl.LogRecords().AppendEmpty()
			lr.Body().SetStr(`level=info msg="request completed" method=GET path=/api/v2/cart status=200`)
			lr.SetTimestamp(pcommon.Timestamp(1755500000000000000 + int64(i)))
		}
	}
	return ld
}

// Zero subscribers, which is every export on every node until somebody opens
// /debug/otlp. The payload size must not matter at all here: nothing walks it.
func BenchmarkTapNoSubscribers(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1, 512} {
		ld := benchLogs(n, 16)
		b.Run(fmt.Sprintf("resources=%d", n), func(b *testing.B) {
			t := New(&fakeInner{})
			b.ReportAllocs()
			for b.Loop() {
				if err := t.ExportLogs(ctx, ld); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTapNoSubscribersMetrics(b *testing.B) {
	ctx := context.Background()
	md := pmetric.NewMetrics()
	for r := 0; r < 512; r++ {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("k8s.namespace.name", "team-1")
		rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(1)
	}
	t := New(&fakeInner{})
	b.ReportAllocs()
	for b.Loop() {
		if err := t.ExportMetrics(ctx, md); err != nil {
			b.Fatal(err)
		}
	}
}

// One attached stream, for scale: the render runs on the EXPORTING goroutine,
// which is the cost the doc warns about.
func BenchmarkTapOneSubscriber(b *testing.B) {
	ctx := context.Background()
	ld := benchLogs(64, 16)
	t := New(&fakeInner{})
	sub, unsub := t.subscribe(sigLogs, nil, 100) // sample is a PERCENT: 100 keeps everything
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-sub.ch:
			case <-stop:
				return
			}
		}
	}()
	b.ReportAllocs()
	for b.Loop() {
		if err := t.ExportLogs(ctx, ld); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	unsub()
	close(stop)
}
