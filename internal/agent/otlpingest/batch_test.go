package otlpingest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// flakyExporter fails ExportLogs with err while failing is set, delivering
// (and counting) otherwise.
type flakyExporter struct {
	mu      sync.Mutex
	err     error
	failing bool
	records int
	batches int
}

func (f *flakyExporter) ExportLogs(_ context.Context, ld plog.Logs) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failing {
		return f.err
	}
	f.records += ld.LogRecordCount()
	f.batches++
	return nil
}

func (f *flakyExporter) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }

func (f *flakyExporter) setFailing(on bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failing = on
}

func (f *flakyExporter) delivered() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.records
}

func oneRecord(body string) plog.Logs {
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr(body)
	return ld
}

// A transient export failure must not drop the acked batch: the batcher
// retries it (back-pressuring senders through the full queue meanwhile) and
// delivers it once the collector recovers.
func TestBatcherRetriesTransient(t *testing.T) {
	exp := &flakyExporter{err: context.DeadlineExceeded, failing: true}
	b := NewBatcher(exp, nil, BatchConfig{Items: 1, Timeout: 10 * time.Millisecond, QueueLen: 1}, nil)
	b.logs.retryBase = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	// While the batch retries, nothing is consumed from the queue: it fills
	// and enqueue back-pressures senders with the retryable error.
	pushed := 0
	var full bool
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := b.ExportLogs(ctx, oneRecord("r")); err != nil {
			if !errors.Is(err, errBatchQueueFull) {
				t.Fatalf("unexpected enqueue error: %v", err)
			}
			full = true
			break
		}
		pushed++
		time.Sleep(time.Millisecond)
	}
	if !full {
		t.Fatal("queue never filled while the export was retrying")
	}
	if exp.delivered() != 0 {
		t.Fatalf("delivered %d records while failing", exp.delivered())
	}

	// Collector recovers: the retried batch and everything queued behind it
	// is delivered — nothing acked was dropped.
	exp.setFailing(false)
	waitForCond(t, func() bool { return exp.delivered() == pushed }, "every acked record delivered")
}

// A permanent collector rejection drops the batch (retrying cannot fix it),
// counts it, and the batcher keeps flowing.
func TestBatcherDropsPermanent(t *testing.T) {
	exp := &flakyExporter{err: &otlpexport.HTTPStatusError{Code: 400, Body: "bad payload"}, failing: true}
	b := NewBatcher(exp, nil, BatchConfig{Items: 1, Timeout: 10 * time.Millisecond}, nil)
	b.logs.retryBase = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	before := obs.IngestDropped.WithLabelValues("logs").Value()
	if err := b.ExportLogs(ctx, oneRecord("poison")); err != nil {
		t.Fatal(err)
	}
	waitForCond(t, func() bool {
		return obs.IngestDropped.WithLabelValues("logs").Value() == before+1
	}, "dropped batch counted")

	// The next batch flows normally.
	exp.setFailing(false)
	if err := b.ExportLogs(ctx, oneRecord("fine")); err != nil {
		t.Fatal(err)
	}
	waitForCond(t, func() bool { return exp.delivered() == 1 }, "subsequent batch delivered")
}

// On shutdown the batcher drains everything already acknowledged to senders
// and flushes it; once stopped, enqueue fails with the retryable queue-full
// error instead of acking into a dead channel.
func TestBatcherShutdownDrainsThenRefuses(t *testing.T) {
	exp := &flakyExporter{}
	b := NewBatcher(exp, nil, BatchConfig{Items: 100, Timeout: time.Hour, QueueLen: 8}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); b.Run(ctx) }()

	for i := 0; i < 3; i++ {
		if err := b.ExportLogs(ctx, oneRecord("queued")); err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	<-done
	if exp.delivered() != 3 {
		t.Fatalf("delivered %d records at shutdown, want 3 (acked payloads must not die in the queue)", exp.delivered())
	}
	if err := b.ExportLogs(context.Background(), oneRecord("late")); !errors.Is(err, errBatchQueueFull) {
		t.Fatalf("enqueue after shutdown = %v, want errBatchQueueFull", err)
	}
}
