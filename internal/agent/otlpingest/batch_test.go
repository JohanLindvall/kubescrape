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

// The byte cap flushes a batch before a merge would push its encoded size
// past MaxBatchBytes, and a single payload already beyond the cap passes
// through as-is (never merged with anything).
func TestBatcherByteCap(t *testing.T) {
	sizer := &plog.ProtoMarshaler{}
	one := oneRecord("payload-of-a-fixed-size")
	sz := sizer.LogsSize(one)

	// The cap fits exactly two payloads; items would allow far more.
	exp := &syncCaptureExporter{}
	b := NewBatcher(exp, nil, BatchConfig{Items: 100, MaxBatchBytes: 2 * sz, Timeout: time.Hour}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); b.Run(ctx) }()

	for i := 0; i < 4; i++ {
		if err := b.ExportLogs(ctx, oneRecord("payload-of-a-fixed-size")); err != nil {
			t.Fatal(err)
		}
	}
	// Nothing but the byte cap can flush here (items huge, timeout an hour);
	// the trailing partial batch flushes on the shutdown drain.
	waitForCond(t, func() bool { return exp.logBatches() >= 1 }, "byte-cap flush")
	cancel()
	<-done
	if exp.logRecords() != 4 {
		t.Fatalf("delivered %d records, want 4", exp.logRecords())
	}
	if exp.logBatches() != 2 {
		t.Fatalf("delivered %d batches, want 2 (cap fits two payloads)", exp.logBatches())
	}
	for i, ld := range exp.batches {
		if got := sizer.LogsSize(ld); got > 2*sz {
			t.Errorf("batch %d encoded size %d exceeds the %d cap", i, got, 2*sz)
		}
	}

	// A single payload over the cap is exported unsplit, on its own.
	exp2 := &syncCaptureExporter{}
	b2 := NewBatcher(exp2, nil, BatchConfig{Items: 100, MaxBatchBytes: sz - 1, Timeout: time.Hour}, nil)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go b2.Run(ctx2)
	if err := b2.ExportLogs(ctx2, oneRecord("payload-of-a-fixed-size")); err != nil {
		t.Fatal(err)
	}
	waitForCond(t, func() bool { return exp2.logBatches() == 1 && exp2.logRecords() == 1 }, "oversized pass-through")
}

// A batch that keeps failing transiently must not wedge the queue forever:
// after the retry limit it is dropped, counted, and the batcher keeps flowing.
func TestBatcherDropsAfterRetryLimit(t *testing.T) {
	exp := &flakyExporter{err: context.DeadlineExceeded, failing: true}
	b := NewBatcher(exp, nil, BatchConfig{Items: 1, Timeout: 10 * time.Millisecond}, nil)
	b.logs.retryBase = time.Millisecond
	b.logs.retryLimit = 3
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	before := obs.IngestDropped.WithLabelValues("logs").Value()
	if err := b.ExportLogs(ctx, oneRecord("never-accepted")); err != nil {
		t.Fatal(err)
	}
	waitForCond(t, func() bool {
		return obs.IngestDropped.WithLabelValues("logs").Value() == before+1
	}, "batch dropped after the retry limit")

	// The queue is unwedged: the next batch flows.
	exp.setFailing(false)
	if err := b.ExportLogs(ctx, oneRecord("fine")); err != nil {
		t.Fatal(err)
	}
	waitForCond(t, func() bool { return exp.delivered() == 1 }, "subsequent batch delivered")
}

// The shutdown drain honors the same items/byte chunking as the live path:
// merging the whole queue into one payload could exceed the collector's recv
// limit.
func TestBatcherShutdownDrainChunked(t *testing.T) {
	// Items chunking: 5 queued single-record payloads drain as 2+2+1.
	exp := &syncCaptureExporter{}
	b := NewBatcher(exp, nil, BatchConfig{Items: 2, Timeout: time.Hour, QueueLen: 8}, nil)
	for i := 0; i < 5; i++ {
		if err := b.ExportLogs(context.Background(), oneRecord("queued")); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b.Run(ctx) // drains synchronously: ctx is already done
	if exp.logRecords() != 5 {
		t.Fatalf("drained %d records, want 5", exp.logRecords())
	}
	if exp.logBatches() != 3 {
		t.Fatalf("drained in %d batches, want 3 (items chunking)", exp.logBatches())
	}
	for i, ld := range exp.batches {
		if n := ld.LogRecordCount(); n > 2 {
			t.Errorf("drained batch %d has %d records, above the 2-item cap", i, n)
		}
	}

	// Byte chunking: a cap fitting two payloads drains 4 as 2+2.
	sizer := &plog.ProtoMarshaler{}
	sz := sizer.LogsSize(oneRecord("queued"))
	exp2 := &syncCaptureExporter{}
	b2 := NewBatcher(exp2, nil, BatchConfig{Items: 100, MaxBatchBytes: 2 * sz, Timeout: time.Hour, QueueLen: 8}, nil)
	for i := 0; i < 4; i++ {
		if err := b2.ExportLogs(context.Background(), oneRecord("queued")); err != nil {
			t.Fatal(err)
		}
	}
	b2.Run(ctx)
	if exp2.logRecords() != 4 || exp2.logBatches() != 2 {
		t.Fatalf("drained %d records in %d batches, want 4 in 2 (byte chunking)", exp2.logRecords(), exp2.logBatches())
	}
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

// --- audit: the shutdown drain gives an acked payload exactly ONE attempt ---

// TestBatcherShutdownDropIsCounted pins the shutdown-drain contract. Enqueue
// ACKS the sender, and the drain runs with its context already cancelled, so
// deliver's ctx.Err() branch drops on the FIRST transient failure: an
// upstream blip at SIGTERM loses acked data (the documented mitigation is
// -buffer-dir, whose spool.Append cannot fail transiently). The loss is
// unavoidable without a spool, so the bar is that it is COUNTED and logged —
// never silent. This test is the alarm on that counter.
func TestBatcherShutdownDropIsCounted(t *testing.T) {
	exp := &flakyExporter{err: errors.New("collector down"), failing: true}
	b := NewBatcher(exp, nil, BatchConfig{Items: 100, Timeout: time.Hour}, nil)
	b.logs.retryBase = time.Millisecond

	before := obs.IngestDropped.WithLabelValues("logs").Value()

	// Acked by the batcher, still sitting in the queue.
	if err := b.ExportLogs(context.Background(), oneRecord("acked")); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); b.Run(ctx) }()
	cancel()
	<-done

	if exp.delivered() != 0 {
		t.Fatalf("delivered = %d, want 0 (exporter is failing)", exp.delivered())
	}
	if got := obs.IngestDropped.WithLabelValues("logs").Value() - before; got != 1 {
		t.Fatalf("kubescrape_ingest_dropped_batches_total{logs} delta = %v, want 1 — an acked-then-dropped payload MUST be counted", got)
	}
}

// TestBatcherShutdownDrainStillDelivers is the other half: when the collector
// is healthy, everything acked before the cancel is delivered by the drain —
// nothing acked is lost on a clean stop.
func TestBatcherShutdownDrainStillDelivers(t *testing.T) {
	exp := &flakyExporter{}
	b := NewBatcher(exp, nil, BatchConfig{Items: 1000, Timeout: time.Hour}, nil)
	for i := 0; i < 20; i++ {
		if err := b.ExportLogs(context.Background(), oneRecord("x")); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); b.Run(ctx) }()
	cancel()
	<-done
	if got := exp.delivered(); got != 20 {
		t.Fatalf("delivered = %d, want 20", got)
	}
}

// slowExporter makes every export take `delay` and then fail — a dead
// collector, where each attempt burns the exporter's full timeout.
type slowExporter struct {
	delay time.Duration
	mu    sync.Mutex
	calls int
}

func (s *slowExporter) ExportLogs(context.Context, plog.Logs) error {
	time.Sleep(s.delay)
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return errors.New("collector down")
}

func (s *slowExporter) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }

func (s *slowExporter) attempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// TestBatcherShutdownDrainIsBounded: with a dead collector every drain attempt
// burns the exporter's full timeout, so an unbounded drain would take
// QueueLen x timeout — far past the pod's termination grace, and the SIGKILL
// would then take the rest of the ACKED backlog down silently. The drain must
// stop attempting past its deadline and DROP-AND-COUNT the remainder.
func TestBatcherShutdownDrainIsBounded(t *testing.T) {
	exp := &slowExporter{delay: 60 * time.Millisecond}
	b := NewBatcher(exp, nil, BatchConfig{Items: 1, Timeout: time.Hour, QueueLen: 64}, nil)
	b.logs.retryBase = time.Millisecond
	b.logs.drainTimeout = 50 * time.Millisecond

	const queued = 40
	for i := 0; i < queued; i++ {
		if err := b.ExportLogs(context.Background(), oneRecord("acked")); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	before := obs.IngestDropped.WithLabelValues("logs").Value()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	b.logs.run(ctx)
	elapsed := time.Since(start)

	// One in-flight attempt (60ms) may overrun the 50ms deadline; the other 39
	// must not each cost another 60ms.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("shutdown drain took %v — it is not bounded by drainTimeout", elapsed)
	}
	if got := exp.attempts(); got > 3 {
		t.Fatalf("export attempts during drain = %d, want <= 3 (deadline must stop the attempts)", got)
	}
	// Every acked payload is accounted for: delivered (none here) or counted.
	if got := obs.IngestDropped.WithLabelValues("logs").Value() - before; got < 1 {
		t.Fatalf("dropped counter delta = %v, want >= 1 — abandoned acked payloads must be counted", got)
	}
}
