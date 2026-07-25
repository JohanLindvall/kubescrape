package otlpbatch

// Regression tests for zero-count payloads: a payload can carry real encoded
// bytes while count() reports 0 (a ResourceLogs holding only resource
// attributes, a metric family with descriptors but no data points). Gating the
// accumulator's flush and cap decisions on the RECORD count made such payloads
// invisible to every bound.

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type recordingExp struct {
	mu      sync.Mutex
	logs    []plog.Logs
	metrics []pmetric.Metrics
}

func (e *recordingExp) ExportLogs(_ context.Context, ld plog.Logs) error {
	c := plog.NewLogs()
	ld.CopyTo(c)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.logs = append(e.logs, c)
	return nil
}

func (e *recordingExp) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	c := pmetric.NewMetrics()
	md.CopyTo(c)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.metrics = append(e.metrics, c)
	return nil
}

func (e *recordingExp) logStats() (payloads, rls int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, ld := range e.logs {
		rls += ld.ResourceLogs().Len()
	}
	return len(e.logs), rls
}

func (e *recordingExp) maxMetricBytes() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	worst := 0
	var m pmetric.ProtoMarshaler
	for _, md := range e.metrics {
		if n := m.MetricsSize(md); n > worst {
			worst = n
		}
	}
	return worst
}

// emptyResourceLogs carries resource attributes but zero log records:
// LogRecordCount() == 0 while the encoded payload is non-empty.
func emptyResourceLogs() plog.Logs {
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().Resource().Attributes().PutStr("service.name", "sender")
	return ld
}

// TestZeroCountPayloadsDoNotAccumulateUnbounded: zero-record payloads must be
// carried by the normal batch timeout like anything else. Before the fix they
// merged into the accumulator without moving accN, so no cap fired, the
// timeout kept being re-armed, and flush() early-returned on accN == 0 — the
// accumulator grew for the process's lifetime.
func TestZeroCountPayloadsDoNotAccumulateUnbounded(t *testing.T) {
	exp := &recordingExp{}
	b := NewBatcher(exp, nil, BatchConfig{Items: 100, Timeout: 20 * time.Millisecond, QueueLen: 4096}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { b.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	const n = 500
	for i := 0; i < n; i++ {
		if err := b.ExportLogs(ctx, emptyResourceLogs()); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	time.Sleep(300 * time.Millisecond) // many batch timeouts

	payloads, rls := exp.logStats()
	if payloads == 0 {
		t.Fatalf("nothing delivered after %d zero-record payloads + 300ms: they are still held in the accumulator (unbounded growth)", n)
	}
	if rls != n {
		t.Errorf("delivered %d ResourceLogs across %d payloads, want %d: acked zero-record payloads must be delivered, not retained or dropped", rls, payloads, n)
	}
}

// TestZeroCountPayloadsNotLostAtShutdown: the batcher ACKs the sender on
// enqueue, so every acked payload must be delivered or counted dropped. Before
// the fix a queue holding only zero-record payloads drained into a flush() that
// early-returned, so they were silently lost with nothing counted.
func TestZeroCountPayloadsNotLostAtShutdown(t *testing.T) {
	exp := &recordingExp{}
	b := NewBatcher(exp, nil, BatchConfig{Items: 100, Timeout: time.Hour, QueueLen: 4096}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { b.Run(ctx); close(done) }()

	const n = 10
	for i := 0; i < n; i++ {
		if err := b.ExportLogs(ctx, emptyResourceLogs()); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	time.Sleep(50 * time.Millisecond) // let them reach the accumulator
	cancel()                          // shutdown drain
	<-done

	payloads, rls := exp.logStats()
	if payloads == 0 || rls != n {
		t.Fatalf("shutdown delivered %d ResourceLogs in %d payloads, want %d: acked payloads were neither delivered nor counted dropped",
			rls, payloads, n)
	}
}

// TestByteCapNotBypassedByZeroPointPayloads: the byte cap exists because the
// collector rejects a payload over its receive limit outright — exceeding it
// is TOTAL loss for that batch, not a partial one. A metric payload with
// descriptors but no data points has count() == 0 and real bytes, so before
// the fix it bypassed every cap and the eventual flush blew far past the limit.
func TestByteCapNotBypassedByZeroPointPayloads(t *testing.T) {
	const maxBytes = 1 << 20

	// A metric family with descriptors and NO data points: real bytes, zero count.
	bigEmptyMetrics := func() pmetric.Metrics {
		md := pmetric.NewMetrics()
		sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
		for i := 0; i < 4000; i++ {
			m := sm.Metrics().AppendEmpty()
			m.SetName("a_reasonably_long_metric_family_name_number_" + string(rune('a'+i%26)))
			m.SetDescription("a description long enough to contribute real encoded bytes to the payload")
			m.SetUnit("bytes")
			m.SetEmptyGauge() // no data points
		}
		return md
	}
	var pm pmetric.ProtoMarshaler
	if sz := pm.MetricsSize(bigEmptyMetrics()); sz >= maxBytes {
		t.Fatalf("test fixture too large (%d >= cap %d): it would take the deliver-alone path", sz, maxBytes)
	}

	exp := &recordingExp{}
	b := NewBatcher(exp, nil, BatchConfig{Items: 100000, MaxBatchBytes: maxBytes, Timeout: 30 * time.Millisecond, QueueLen: 4096}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { b.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	for i := 0; i < 10; i++ {
		if err := b.ExportMetrics(ctx, bigEmptyMetrics()); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	time.Sleep(300 * time.Millisecond)

	if worst := exp.maxMetricBytes(); worst > maxBytes {
		t.Fatalf("delivered a payload of %d bytes, exceeding the %d-byte cap: zero-point payloads bypassed the byte accounting",
			worst, maxBytes)
	}
}
