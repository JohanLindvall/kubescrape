package otlpingest

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// errBatchQueueFull maps to a retryable status for the sender.
var errBatchQueueFull = errors.New("ingest batch queue full")

// BatchConfig sizes the optional ingest batcher (the collector batch
// processor's role): pushed payloads are coalesced per signal until Items
// accumulate or Timeout passes, cutting per-request export overhead when many
// apps push small batches. Items <= 0 disables batching (forward as
// received).
type BatchConfig struct {
	// Items caps one exported payload: log records, metric data points, or
	// spans.
	Items int
	// Timeout bounds how long the first item of a batch may wait (default
	// 200ms).
	Timeout time.Duration
	// QueueLen bounds pending payloads per signal (default 256); a full queue
	// back-pressures senders with a retryable error.
	QueueLen int
}

// Batcher wraps an exporter with per-signal coalescing. Enqueueing
// acknowledges the sender (like the collector's batch processor); delivery
// failures are logged and counted by the underlying exporter — pair batching
// with the disk buffer for at-least-once delivery.
type Batcher struct {
	logs    *sigBatch[plog.Logs]
	metrics *sigBatch[pmetric.Metrics]
	traces  *sigBatch[ptrace.Traces]
}

// NewBatcher wraps inner (and traces, which may be nil when the traces
// pipeline is off).
func NewBatcher(inner Exporter, traces TracesExporter, cfg BatchConfig, log *slog.Logger) *Batcher {
	if log == nil {
		log = slog.Default()
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 200 * time.Millisecond
	}
	if cfg.QueueLen <= 0 {
		cfg.QueueLen = 256
	}
	b := &Batcher{
		logs: newSigBatch(cfg, log, "logs", inner.ExportLogs, plog.NewLogs,
			func(md plog.Logs) int { return md.LogRecordCount() },
			func(dst, src plog.Logs) { src.ResourceLogs().MoveAndAppendTo(dst.ResourceLogs()) }),
		metrics: newSigBatch(cfg, log, "metrics", inner.ExportMetrics, pmetric.NewMetrics,
			func(md pmetric.Metrics) int { return md.DataPointCount() },
			func(dst, src pmetric.Metrics) { src.ResourceMetrics().MoveAndAppendTo(dst.ResourceMetrics()) }),
	}
	if traces != nil {
		b.traces = newSigBatch(cfg, log, "traces", traces.ExportTraces, ptrace.NewTraces,
			func(td ptrace.Traces) int { return td.SpanCount() },
			func(dst, src ptrace.Traces) { src.ResourceSpans().MoveAndAppendTo(dst.ResourceSpans()) })
	}
	return b
}

// ExportLogs enqueues a payload for coalescing.
func (b *Batcher) ExportLogs(_ context.Context, ld plog.Logs) error { return b.logs.enqueue(ld) }

// ExportMetrics enqueues a payload for coalescing.
func (b *Batcher) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	return b.metrics.enqueue(md)
}

// ExportTraces enqueues a payload for coalescing.
func (b *Batcher) ExportTraces(_ context.Context, td ptrace.Traces) error {
	if b.traces == nil {
		return errors.New("traces not enabled")
	}
	return b.traces.enqueue(td)
}

// Run drains all signals until ctx is done, then flushes what is pending.
func (b *Batcher) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, run := range []func(context.Context){b.logs.run, b.metrics.run, b.traces.run} {
		wg.Add(1)
		go func(r func(context.Context)) { defer wg.Done(); r(ctx) }(run)
	}
	wg.Wait()
}

// sigBatch coalesces one signal.
type sigBatch[T any] struct {
	ch      chan T
	export  func(context.Context, T) error
	fresh   func() T
	count   func(T) int
	merge   func(dst, src T)
	items   int
	timeout time.Duration
	log     *slog.Logger
	kind    string
}

func newSigBatch[T any](cfg BatchConfig, log *slog.Logger, kind string,
	export func(context.Context, T) error, fresh func() T,
	count func(T) int, merge func(dst, src T)) *sigBatch[T] {
	return &sigBatch[T]{
		ch: make(chan T, cfg.QueueLen), export: export, fresh: fresh,
		count: count, merge: merge, items: cfg.Items, timeout: cfg.Timeout,
		log: log, kind: kind,
	}
}

func (s *sigBatch[T]) enqueue(v T) error {
	select {
	case s.ch <- v:
		return nil
	default:
		return errBatchQueueFull
	}
}

// run coalesces queued payloads: a batch is exported when it reaches items or
// when its oldest payload has waited timeout. A single incoming payload
// already at or beyond the cap is exported as-is (payloads are not split
// below request granularity; the receiver's body cap bounds them). Safe on a
// nil receiver (signal disabled).
func (s *sigBatch[T]) run(ctx context.Context) {
	if s == nil {
		return
	}
	acc := s.fresh()
	accN := 0
	timer := time.NewTimer(s.timeout)
	if !timer.Stop() {
		<-timer.C
	}
	flush := func() {
		if accN == 0 {
			return
		}
		if err := s.export(context.Background(), acc); err != nil {
			s.log.Warn("batched ingest export failed", "signal", s.kind, "records", accN, "error", err)
		}
		acc = s.fresh()
		accN = 0
	}
	for {
		select {
		case <-ctx.Done():
			// Drain what was already acknowledged to senders, then flush.
			for {
				select {
				case v := <-s.ch:
					s.merge(acc, v)
					accN += s.count(v)
				default:
					flush()
					return
				}
			}
		case v := <-s.ch:
			n := s.count(v)
			if accN > 0 && accN+n > s.items {
				flush()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}
			if accN == 0 && n >= s.items {
				if err := s.export(context.Background(), v); err != nil {
					s.log.Warn("batched ingest export failed", "signal", s.kind, "records", n, "error", err)
				}
				continue
			}
			if accN == 0 {
				timer.Reset(s.timeout)
			}
			s.merge(acc, v)
			accN += n
			if accN >= s.items {
				flush()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}
		case <-timer.C:
			flush()
		}
	}
}
