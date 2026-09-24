package otlpexport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// singleAttempt is the seam through which the disk-buffer drain reaches an
// exporter's SINGLE-ATTEMPT sends, bypassing the client's own bounded retry
// loop: the drain owns retry policy (a backoff persisting across queue
// cycles, requeue rotation, the poison budget), and the Exporter interface
// alone cannot say that ExportMetrics retries internally while ExportLogs
// does not. It replaces a concrete-type switch over *Client / *PerSignal
// whose default arm silently fell back to RETRIED sends for any other
// Exporter — the enumeration is now each implementation's own business, and
// the fallback is loud (NewBuffered warns). Unexported deliberately:
// single-attempt sends are this package's internals, and a foreign wrapper
// stacking its own retries is exactly what the warning exists to name.
type singleAttempt interface {
	singleAttemptSends() (logs func(context.Context, plog.Logs) error, metrics func(context.Context, pmetric.Metrics) error)
}

// Exporter exports logs and metrics; implemented by *Client and *Buffered, so
// the agent can route every consumer through one value whether or not
// buffering is enabled.
type Exporter interface {
	ExportLogs(ctx context.Context, ld plog.Logs) error
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

// TracesExporter exports traces. *Client implements it natively; *Buffered
// passes traces through to the inner exporter unbuffered UNLESS the payload is
// marked Own(ctx) — see owned.go: a trace is normally a forwarded push whose
// sender owns the retry, and the one exception is a tail-sampling decision,
// which acked its senders seconds earlier.
type TracesExporter interface {
	ExportTraces(ctx context.Context, td ptrace.Traces) error
}

// Buffered is a disk-backed write-ahead buffer in front of an exporter, for
// logs, metrics and OWNED traces. Export{Logs,Metrics} serialize the batch and
// append it to a durable on-disk queue (github.com/JohanLindvall/diskqueue, one
// per signal), returning as soon as it is persisted — so producers can commit
// their progress and source logs may rotate away while their data waits on the
// node. Run drains each queue to the real exporter with retries; a batch is
// removed only after the collector acknowledges it (at-least-once, surviving
// restarts). A full queue makes Export return diskqueue.ErrFull, which the
// tailer treats as a failure and rewinds — bounding disk use and
// back-pressuring to the source.
//
// ExportTraces is the exception, and it is a PER-PAYLOAD one: a plain forwarded
// trace passes straight through (the pushing application still holds it and its
// SDK retries — spooling would ack a sender for data that has not shipped),
// while a payload marked Own(ctx) is spooled like the other two signals. The
// only marker in this repo is agent/tailbuffer, whose senders were acked when
// their spans were BUFFERED and hold nothing by the time a decision ships. See
// owned.go for the whole argument. With no traces spool open, an owned payload
// passes through as before — the marker asks for durability, it does not
// require it.
type Buffered struct {
	inner   Exporter // direct path for a signal with no buffer
	logs    *sink[plog.Logs]
	metrics *sink[pmetric.Metrics]
	traces  *sink[ptrace.Traces] // nil unless a traces spool was opened
	log     *slog.Logger
	// drainGate holds ONE token, taken for the whole of Run and for the whole
	// of FinalDrain. Both drive the same diskqueue Readers and the same
	// per-sink bookkeeping (cur backoff, delivered, the stuck map), none of
	// which is synchronised — it never needed to be while a single drain
	// goroutine per signal owned it.
	//
	// FinalDrain is documented to run "once the producers have stopped", but
	// the agent joins them on a BOUNDED budget and Run is one of them: a Run
	// still inside a send cycle when the budget expires overlaps the FinalDrain
	// that follows, so the two share that state with no happens-before between
	// them. The token restores the one-drain-at-a-time invariant.
	//
	// A channel rather than a Mutex because the acquire must be CANCELLABLE:
	// FinalDrain has to honour its own deadline instead of blocking behind a
	// drain wedged against a dead collector, and giving up there is safe —
	// nothing is committed until the collector acks, so whatever is still
	// spooled redelivers on the next start of this pod on this node.
	drainGate chan struct{}
}

// NewBuffered wraps inner. logBuf, metricBuf and traceBuf back the three
// signals; any of them may be nil to leave that signal unbuffered (exported
// directly). traceBuf is only ever consulted for a payload marked Own(ctx) —
// pass nil unless something in the chain marks one, or the queue is a directory
// and a segment file that never receive a record.
func NewBuffered(inner Exporter, logBuf, metricBuf, traceBuf *Buffer, backoff time.Duration, log *slog.Logger) *Buffered {
	if backoff <= 0 {
		backoff = time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	b := &Buffered{inner: inner, log: log, drainGate: make(chan struct{}, 1)}
	b.drainGate <- struct{}{} // the token starts free: FinalDrain without a Run must not wait
	// The drain owns retry policy; reach the inner exporter's single-attempt
	// sends through the singleAttempt seam so attempts do not multiply (drain
	// x client = up to 15 wire sends per cycle otherwise). The fallback for an
	// exporter outside this package is LOUD: stacked retries are a behavior
	// change worth a log line, not a silent property of whoever wired the
	// stack.
	sendLogs, sendMetrics := inner.ExportLogs, inner.ExportMetrics
	if sa, ok := inner.(singleAttempt); ok {
		sendLogs, sendMetrics = sa.singleAttemptSends()
	} else {
		log.Warn("inner exporter exposes no single-attempt sends; the drain's retries will stack on the exporter's own",
			"type", fmt.Sprintf("%T", inner))
	}
	// The raw seam is separate and optional: a spooled payload is already the
	// wire body, so the common drain path sends the reserved bytes verbatim and
	// materializes pdata only where it is genuinely needed (an over-cap payload
	// to split, a drop whose counter reports records). An exporter outside this
	// package cannot promise that equivalence and simply does not get it.
	var rawLogs, rawMetrics, rawTraces func(context.Context, []byte) error
	var rawMaxBytes int
	if ra, ok := inner.(rawSingleAttempt); ok {
		rawLogs, rawMetrics, rawTraces, rawMaxBytes = ra.rawSingleAttemptSends()
	}
	// Report what corruption cost at open: everything the recovery scan
	// dropped or truncated away is data no drain will ever see.
	for kind, buf := range map[string]*Buffer{"logs": logBuf, "metrics": metricBuf, "traces": traceBuf} {
		if buf == nil {
			continue
		}
		buf.kind, buf.log = kind, log
		st := buf.stats()
		if lost := truncatedBytes(st); lost > 0 {
			obs.BufferTruncated.WithLabelValues(kind).Add(float64(lost))
			log.Error("disk buffer lost data to damage discovered at open",
				"signal", kind, "bytesLost", lost)
		}
	}
	if logBuf != nil {
		lm := plog.ProtoMarshaler{}
		lu := plog.ProtoUnmarshaler{}
		b.logs = &sink[plog.Logs]{
			buf: logBuf, backoff: backoff, log: log, kind: "logs",
			enqueueWarns: logdedupe.New(3, bufferWarnEvery),
			marshal:      lm.MarshalLogs,
			unmarshal:    lu.UnmarshalLogs,
			count:        plog.Logs.LogRecordCount,
			send:         sendLogs,
			sendRaw:      rawLogs, maxSendBytes: rawMaxBytes,
		}
	}
	if metricBuf != nil {
		mm := pmetric.ProtoMarshaler{}
		mu := pmetric.ProtoUnmarshaler{}
		b.metrics = &sink[pmetric.Metrics]{
			buf: metricBuf, backoff: backoff, log: log, kind: "metrics",
			enqueueWarns: logdedupe.New(3, bufferWarnEvery),
			marshal:      mm.MarshalMetrics,
			unmarshal:    mu.UnmarshalMetrics,
			count:        pmetric.Metrics.DataPointCount,
			send:         sendMetrics,
			sendRaw:      rawMetrics, maxSendBytes: rawMaxBytes,
		}
	}
	if te, ok := inner.(TracesExporter); ok && traceBuf != nil {
		tm := ptrace.ProtoMarshaler{}
		tu := ptrace.ProtoUnmarshaler{}
		b.traces = &sink[ptrace.Traces]{
			buf: traceBuf, backoff: backoff, log: log, kind: "traces",
			enqueueWarns: logdedupe.New(3, bufferWarnEvery),
			marshal:      tm.MarshalTraces,
			unmarshal:    tu.UnmarshalTraces,
			count:        ptrace.Traces.SpanCount,
			// No counted-unwrap twin here (as sendLogs/sendMetrics have):
			// Client.ExportTraces is ALREADY a single counted attempt — the
			// pushing sender's retry has always been the trace path's retry —
			// so there is no client-side loop for the drain's own retries to
			// multiply with.
			send:    te.ExportTraces,
			sendRaw: rawTraces, maxSendBytes: rawMaxBytes,
		}
	} else if traceBuf != nil {
		log.Error("a traces disk buffer was opened but the exporter does not support traces; owned trace payloads will be refused")
	}
	return b
}

// ExportTraces spools an OWNED payload (see Own) and passes every other one
// straight to the inner exporter.
//
// The asymmetry with logs and metrics is the point: a forwarded trace is still
// held by the application that pushed it, and its SDK's retry is a better
// durability story than this spool — acking that sender to put its data in a
// queue would REMOVE the only other copy. A tail-sampling decision is the
// opposite case (its senders were acked when the spans were buffered, and are
// long gone), and it says so by marking the context.
func (b *Buffered) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	if b.traces != nil && Owned(ctx) {
		return b.traces.enqueue(td)
	}
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

// acquireDrain takes the single drain token, or reports false when ctx ends
// first. See Buffered.drainGate.
func (b *Buffered) acquireDrain(ctx context.Context) bool {
	select {
	case <-b.drainGate:
		return true
	case <-ctx.Done():
		return false
	}
}

func (b *Buffered) releaseDrain() { b.drainGate <- struct{}{} }

// Run drains every open spool (logs, metrics and, when one was opened, owned
// traces) until ctx is done.
func (b *Buffered) Run(ctx context.Context) {
	if !b.acquireDrain(ctx) {
		return // already cancelled, or another drain pass owns the queues
	}
	defer b.releaseDrain()
	var wg sync.WaitGroup
	for _, run := range []func(context.Context){b.logs.drain, b.metrics.drain, b.traces.drain} {
		wg.Go(func() { run(ctx) })
	}
	wg.Wait()
}

// Stats reports each signal's disk-buffer occupancy, for the gauges that make
// a filling buffer visible BEFORE it starts refusing writes.
func (b *Buffered) Stats() map[string]obs.BufferStat {
	out := map[string]obs.BufferStat{}
	for kind, buf := range map[string]*Buffer{"logs": b.logs.bufOf(), "metrics": b.metrics.bufOf(), "traces": b.traces.bufOf()} {
		if buf == nil {
			continue
		}
		st := buf.stats()
		out[kind] = obs.BufferStat{Backlog: st.BacklogBytes, Cap: st.MaxBytes, Segments: st.Segments}
	}
	return out
}

// FinalDrain empties every open spool (logs, metrics and, when one was opened,
// owned traces), returning as soon as they run dry (or when ctx expires). Run stops the instant its context is cancelled, so everything
// exported after SIGTERM — the tailer's and journald's last flushes, the
// ingest server's in-flight forwards, and the final log-metrics and
// self-metrics windows, which are exported only after those goroutines have
// joined — reaches the spool and nothing carries it further. Without this pass that data waits for
// the next start of this pod ON THIS NODE, and is lost outright when the pod
// never returns (node drained or scaled away, release uninstalled) or the
// buffer dir is not a persistent mount. Call it with a bounded fresh context
// once the producers have stopped, before closing the exporter and spools.
//
// "Once the producers have stopped" is best-effort at the caller: the agent
// joins them on a shutdown BUDGET, so a producer stuck against a dead
// collector can still enqueue while this runs, and whatever it enqueues after
// the drain runs dry stays spooled. That is durability-preserving — the spool
// is committed, so the next start of this pod on this node redelivers it —
// but it is not the same as "nothing is left behind", and the drain does not
// wait for a straggler it cannot bound.
//
// Run is one of the goroutines joined on that budget, and it drives the same
// queue readers and per-sink bookkeeping this pass does, so this first takes
// the drain token (Buffered.drainGate) — waiting for a Run that has not
// finished its cycle, WITHIN ctx. If the token does not come free in time this
// returns having drained nothing rather than racing the running drain: the
// spool is durable and redelivers, which is the better of the two outcomes a
// spent shutdown budget leaves.
func (b *Buffered) FinalDrain(ctx context.Context) {
	if !b.acquireDrain(ctx) {
		b.log.Warn("skipping the final disk-buffer drain: the background drain did not stop within the shutdown budget; " +
			"the spool is durable and redelivers on the next start")
		return
	}
	defer b.releaseDrain()
	var wg sync.WaitGroup
	for _, run := range []func(context.Context){b.logs.drainUntilEmpty, b.metrics.drainUntilEmpty, b.traces.drainUntilEmpty} {
		wg.Go(func() { run(ctx) })
	}
	wg.Wait()
}
