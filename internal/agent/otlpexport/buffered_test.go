package otlpexport

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/spool"
)

type fakeSender struct {
	mu       sync.Mutex
	failNext int // fail this many log sends before succeeding
	logs     []string
	metrics  []string
}

func (f *fakeSender) ExportLogs(_ context.Context, ld plog.Logs) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext > 0 {
		f.failNext--
		return context.DeadlineExceeded
	}
	rl := ld.ResourceLogs()
	for i := 0; i < rl.Len(); i++ {
		sl := rl.At(i).ScopeLogs()
		for j := 0; j < sl.Len(); j++ {
			lrs := sl.At(j).LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				f.logs = append(f.logs, lrs.At(k).Body().Str())
			}
		}
	}
	return nil
}

func (f *fakeSender) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	rm := md.ResourceMetrics()
	for i := 0; i < rm.Len(); i++ {
		sm := rm.At(i).ScopeMetrics()
		for j := 0; j < sm.Len(); j++ {
			ms := sm.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				f.metrics = append(f.metrics, ms.At(k).Name())
			}
		}
	}
	return nil
}

func (f *fakeSender) gotLogs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.logs...)
}

func (f *fakeSender) gotMetrics() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.metrics...)
}

func logsWith(body string) plog.Logs {
	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr(body)
	return ld
}

func metricsWith(name string) pmetric.Metrics {
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(name)
	m.SetEmptyGauge()
	return md
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func openBuffer(t *testing.T, dir string, send *fakeSender, max int64) (*Buffered, *spool.Spool, *spool.Spool) {
	t.Helper()
	ls, err := spool.Open(dir+"/logs", spool.Options{MaxBytes: max})
	if err != nil {
		t.Fatal(err)
	}
	ms, err := spool.Open(dir+"/metrics", spool.Options{MaxBytes: max})
	if err != nil {
		t.Fatal(err)
	}
	return NewBuffered(send, ls, ms, 10*time.Millisecond, nil), ls, ms
}

func TestBufferedDrainsBothSignals(t *testing.T) {
	send := &fakeSender{failNext: 2} // first two log sends fail, then succeed
	b, ls, ms := openBuffer(t, t.TempDir(), send, 0)
	defer func() { _ = ls.Close() }()
	defer func() { _ = ms.Close() }()

	if err := b.ExportLogs(context.Background(), logsWith("first")); err != nil {
		t.Fatal(err)
	}
	if err := b.ExportLogs(context.Background(), logsWith("second")); err != nil {
		t.Fatal(err)
	}
	if err := b.ExportMetrics(context.Background(), metricsWith("cpu")); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	waitFor(t, func() bool { return len(send.gotLogs()) == 2 }, "both log batches delivered")
	waitFor(t, func() bool { return len(send.gotMetrics()) == 1 }, "metric batch delivered")
	if got := send.gotLogs(); got[0] != "first" || got[1] != "second" {
		t.Fatalf("logs = %v", got)
	}
	if got := send.gotMetrics(); got[0] != "cpu" {
		t.Fatalf("metrics = %v", got)
	}
}

func TestBufferedSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	// Enqueue a metric while the collector is down; nothing drains.
	b, ls, ms := openBuffer(t, dir, &fakeSender{}, 0)
	if err := b.ExportMetrics(context.Background(), metricsWith("queued")); err != nil {
		t.Fatal(err)
	}
	_ = ls.Close()
	_ = ms.Close()

	// Restart: fresh spools re-read the queued batch and deliver it.
	send := &fakeSender{}
	b2, ls2, ms2 := openBuffer(t, dir, send, 0)
	defer func() { _ = ls2.Close() }()
	defer func() { _ = ms2.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b2.Run(ctx)

	waitFor(t, func() bool { return len(send.gotMetrics()) == 1 }, "queued metric delivered after restart")
	if got := send.gotMetrics(); got[0] != "queued" {
		t.Fatalf("metrics = %v", got)
	}
}

func TestBufferedFullPropagates(t *testing.T) {
	b, ls, ms := openBuffer(t, t.TempDir(), &fakeSender{}, 8)
	defer func() { _ = ls.Close() }()
	defer func() { _ = ms.Close() }()
	// A marshaled batch is well over the 8-byte cap.
	if err := b.ExportLogs(context.Background(), logsWith("too big for the cap")); err != spool.ErrFull {
		t.Fatalf("ExportLogs err = %v, want ErrFull", err)
	}
}
