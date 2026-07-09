package otlpexport

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/spool"
)

// Exporter exports both signals; implemented by *Client and *Buffered, so the
// agent can route every consumer through one value whether or not buffering is
// enabled.
type Exporter interface {
	ExportLogs(ctx context.Context, ld plog.Logs) error
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

// Buffered is a disk-backed write-ahead buffer in front of an exporter, for
// both logs and metrics. Export{Logs,Metrics} serialize the batch and append it
// to a durable on-disk spool (one per signal), returning as soon as it is
// persisted — so producers can commit their progress and source logs may rotate
// away while their data waits on the node. Run drains each spool to the real
// exporter with retries; a batch is removed only after the collector
// acknowledges it (at-least-once, surviving restarts). A full spool makes
// Export return spool.ErrFull, which the tailer treats as a failure and rewinds
// — bounding disk use and back-pressuring to the source.
type Buffered struct {
	logs    *sink[plog.Logs]
	metrics *sink[pmetric.Metrics]
}

// NewBuffered wraps inner. logSpool and metricSpool back the two signals;
// either may be nil to leave that signal unbuffered (exported directly).
func NewBuffered(inner Exporter, logSpool, metricSpool *spool.Spool, backoff time.Duration, log *slog.Logger) *Buffered {
	if backoff <= 0 {
		backoff = time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	b := &Buffered{}
	if logSpool != nil {
		lm := plog.ProtoMarshaler{}
		lu := plog.ProtoUnmarshaler{}
		b.logs = &sink[plog.Logs]{
			spool: logSpool, backoff: backoff, log: log, kind: "logs",
			marshal:   lm.MarshalLogs,
			unmarshal: lu.UnmarshalLogs,
			send:      inner.ExportLogs,
		}
	}
	if metricSpool != nil {
		mm := pmetric.ProtoMarshaler{}
		mu := pmetric.ProtoUnmarshaler{}
		b.metrics = &sink[pmetric.Metrics]{
			spool: metricSpool, backoff: backoff, log: log, kind: "metrics",
			marshal:   mm.MarshalMetrics,
			unmarshal: mu.UnmarshalMetrics,
			send:      inner.ExportMetrics,
		}
	}
	return b
}

// ExportLogs durably enqueues a log batch (Run sends it).
func (b *Buffered) ExportLogs(_ context.Context, ld plog.Logs) error {
	return b.logs.enqueue(ld)
}

// ExportMetrics durably enqueues a metric batch (Run sends it).
func (b *Buffered) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	return b.metrics.enqueue(md)
}

// Run drains both spools until ctx is done.
func (b *Buffered) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, run := range []func(context.Context){b.logs.drain, b.metrics.drain} {
		wg.Add(1)
		go func(r func(context.Context)) { defer wg.Done(); r(ctx) }(run)
	}
	wg.Wait()
}

// sink is one signal's buffer: a spool plus the (un)marshal and send functions
// for its pdata type.
type sink[T any] struct {
	spool     *spool.Spool
	marshal   func(T) ([]byte, error)
	unmarshal func([]byte) (T, error)
	send      func(context.Context, T) error
	backoff   time.Duration
	log       *slog.Logger
	kind      string
}

func (s *sink[T]) enqueue(v T) error {
	data, err := s.marshal(v)
	if err != nil {
		return err
	}
	return s.spool.Append(data)
}

// drain sends queued batches to the exporter until ctx is done. A nil sink (its
// signal unbuffered) returns immediately.
func (s *sink[T]) drain(ctx context.Context) {
	if s == nil {
		return
	}
	for {
		data, commit, ok := s.spool.Pop()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-s.spool.Signal():
			case <-time.After(time.Second):
			}
			continue
		}
		v, err := s.unmarshal(data)
		if err != nil {
			s.log.Warn("dropping corrupt buffered batch", "signal", s.kind, "error", err)
			commit()
			continue
		}
		if !s.trySend(ctx, v) {
			return // ctx cancelled mid-send; leave it queued
		}
		commit()
	}
}

// trySend retries until the exporter accepts the batch or ctx is cancelled.
func (s *sink[T]) trySend(ctx context.Context, v T) bool {
	backoff := s.backoff
	for {
		if err := s.send(ctx, v); err == nil {
			return true
		} else {
			s.log.Warn("buffered export failed, retrying", "signal", s.kind, "error", err, "backoff", backoff)
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
}
