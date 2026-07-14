package otlpexport

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/JohanLindvall/kubescrape/internal/agent/spool"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// Exporter exports logs and metrics; implemented by *Client and *Buffered, so
// the agent can route every consumer through one value whether or not
// buffering is enabled.
type Exporter interface {
	ExportLogs(ctx context.Context, ld plog.Logs) error
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

// TracesExporter exports traces. *Client implements it natively; *Buffered
// passes traces through to the inner exporter unbuffered (traces are a
// passthrough signal — the pushing sender owns retry).
type TracesExporter interface {
	ExportTraces(ctx context.Context, td ptrace.Traces) error
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
	inner   Exporter // direct path for a signal with no spool
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
	b := &Buffered{inner: inner}
	// The drain owns retry policy; when the inner exporter is the raw client,
	// bypass its own bounded retries so attempts do not multiply (drain x
	// client = up to 15 wire sends per cycle otherwise).
	sendLogs, sendMetrics := inner.ExportLogs, inner.ExportMetrics
	if c, ok := inner.(*Client); ok {
		sendLogs, sendMetrics = c.exportLogsCounted, c.exportMetricsCounted
	}
	if logSpool != nil {
		lm := plog.ProtoMarshaler{}
		lu := plog.ProtoUnmarshaler{}
		b.logs = &sink[plog.Logs]{
			spool: logSpool, backoff: backoff, log: log, kind: "logs",
			marshal:   lm.MarshalLogs,
			unmarshal: lu.UnmarshalLogs,
			send:      sendLogs,
		}
	}
	if metricSpool != nil {
		mm := pmetric.ProtoMarshaler{}
		mu := pmetric.ProtoUnmarshaler{}
		b.metrics = &sink[pmetric.Metrics]{
			spool: metricSpool, backoff: backoff, log: log, kind: "metrics",
			marshal:   mm.MarshalMetrics,
			unmarshal: mu.UnmarshalMetrics,
			send:      sendMetrics,
		}
	}
	return b
}

// ExportTraces passes traces to the inner exporter unbuffered.
func (b *Buffered) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	if te, ok := b.inner.(TracesExporter); ok {
		return te.ExportTraces(ctx, td)
	}
	return errors.New("inner exporter does not support traces")
}

// ExportLogs durably enqueues a log batch (Run sends it); with no log spool it
// exports directly.
func (b *Buffered) ExportLogs(ctx context.Context, ld plog.Logs) error {
	if b.logs == nil {
		return b.inner.ExportLogs(ctx, ld)
	}
	return b.logs.enqueue(ld)
}

// ExportMetrics durably enqueues a metric batch (Run sends it); with no metric
// spool it exports directly.
func (b *Buffered) ExportMetrics(ctx context.Context, md pmetric.Metrics) error {
	if b.metrics == nil {
		return b.inner.ExportMetrics(ctx, md)
	}
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
	cur       time.Duration // current backoff, persisted across trySend cycles of a failing head
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
		data, commit, ok, err := s.spool.Pop()
		if err != nil {
			// Both classes drop data and advance: a vanished head segment, and
			// a frame (or segment) the integrity check rejected. Anything else
			// is a transient read error that leaves the queue where it was.
			lost := "false"
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, spool.ErrCorrupt) {
				lost = "true"
			}
			obs.BufferReadErrors.WithLabelValues(s.kind, lost).Inc()
			s.log.Error("disk buffer read failed", "signal", s.kind, "data_lost", lost, "error", err)
		}
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
		switch s.trySend(ctx, v) {
		case sendOK:
			commit()
		case sendCancelled:
			return // ctx cancelled mid-send; leave it queued
		case sendRejected:
			// A definitive rejection (bad payload, auth, unimplemented):
			// retrying cannot fix it and keeping it would block the queue.
			s.log.Error("dropping buffered batch permanently rejected by the collector", "signal", s.kind)
			obs.BufferDropped.WithLabelValues(s.kind).Inc()
			commit()
		case sendStuck:
			// Repeated transient failures: rotate the batch to the back of the
			// queue so one undeliverable batch cannot block the signal —
			// delivery keeps being attempted every cycle. Requeueing is
			// pointless churn (a full rewrite + fsync) when this is the only
			// queued batch, so it stays at the head then. If the spool is full
			// the batch also stays put and the next loop retries in place.
			if s.spool.Bytes() > int64(len(data))+spool.FrameOverhead {
				if err := s.spool.Append(data); err == nil {
					obs.BufferRequeued.WithLabelValues(s.kind).Inc()
					commit()
				}
			}
		}
	}
}

type sendResult int

const (
	sendOK sendResult = iota
	sendCancelled
	sendRejected
	sendStuck
)

// stuckAfterAttempts is how many transient failures trySend tolerates before
// reporting the batch stuck (drain then rotates it to the back of the queue).
const stuckAfterAttempts = 5

// trySend retries with backoff until the exporter accepts the batch, the
// error is a permanent rejection, the attempt budget is spent, or ctx is
// cancelled.
func (s *sink[T]) trySend(ctx context.Context, v T) sendResult {
	// The backoff persists across trySend cycles (s.cur) so a long outage
	// actually reaches the 30s cap instead of restarting at s.backoff every
	// stuckAfterAttempts sends; success resets it.
	if s.cur < s.backoff {
		s.cur = s.backoff
	}
	for attempt := 1; ; attempt++ {
		err := s.send(ctx, v)
		if err == nil {
			s.cur = 0
			return sendOK
		}
		if IsPermanent(err) {
			s.cur = 0 // the connection demonstrably works; don't penalize the next batch
			s.log.Warn("buffered export permanently rejected", "signal", s.kind, "error", err)
			return sendRejected
		}
		if attempt >= stuckAfterAttempts {
			s.log.Warn("buffered export still failing, requeueing", "signal", s.kind, "error", err, "attempts", attempt)
			return sendStuck
		}
		s.log.Warn("buffered export failed, retrying", "signal", s.kind, "error", err, "backoff", s.cur)
		select {
		case <-ctx.Done():
			return sendCancelled
		case <-time.After(s.cur):
		}
		if s.cur *= 2; s.cur > 30*time.Second {
			s.cur = 30 * time.Second
		}
	}
}

// IsPermanent reports whether err is a definitive collector rejection that
// retrying cannot fix (bad payload, unimplemented signal). Everything
// ambiguous is transient. Deliberately transient despite the OTLP spec
// listing them non-retryable: auth failures (401/403, Unauthenticated,
// PermissionDenied) and 404 — a rotating bearer token, a collector rolling
// out behind an ingress, or a route being reprogrammed produces them for
// windows the disk buffer exists to survive; the bounded requeue path caps
// their cost, whereas classifying them permanent drains the whole backlog
// into drops. OutOfRange is retryable per the OTLP failure table.
func IsPermanent(err error) bool {
	var he *HTTPStatusError
	if errors.As(err, &he) {
		switch he.Code {
		case 400, 405, 413, 414, 415, 422, 431:
			return true
		}
		return false
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.InvalidArgument, codes.Unimplemented, codes.FailedPrecondition:
			return true
		}
	}
	return false
}
