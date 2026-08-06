package otlpexport

// Sending a payload that is ALREADY the wire bytes.
//
// The disk buffer spools each batch as plog/pmetric/ptrace ProtoMarshaler
// output, and that encoding is byte-identical to the OTLP ExportRequest body:
// LogsData and ExportLogsServiceRequest are both `repeated ResourceLogs
// resource_logs = 1`, and likewise for the other two signals
// (TestSpooledBytesAreTheWireBytes pins it). So the drain's decode-then-let-
// the-client-re-encode round trip produced exactly the bytes it started from,
// at ~24x the cost of the marshal that produced them: measured for one
// 1024-record / 183 KB logs batch, marshal at enqueue was 662µs / 188 KB / 1
// alloc and the unmarshal at drain was 3.71ms / 522 KB / 11,297 allocs.
//
// These are the sends that skip it. They are unexported and reached through
// the rawSingleAttempt seam for the same reason singleAttempt is: a foreign
// Exporter has no way to promise this equivalence, so it simply does not get
// the fast path.
//
// OWNERSHIP: the bytes handed here must not be the queue reader's buffer.
// Both transports can still be referencing a request body after a FAILED
// attempt returns (net/http's writeLoop, grpc's loopyWriter), and the drain's
// very next queue operation reuses that buffer — Requeue clobbers it. The
// caller therefore copies once per batch (buffered.go), which is one
// allocation against the ~10,000 the decode cost.

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// errNoRawCodec is the belt-and-braces refusal for a gRPC client with no proto
// codec to borrow. rawSingleAttemptSends already withholds the path in that
// case, so the drain never reaches this.
var errNoRawCodec = errors.New("no proto codec registered for the raw send path")

// protoCodecName is the codec whose registration rawCodec borrows for
// RESPONSE decoding — the request half is the caller's bytes verbatim.
const protoCodecName = "proto"

// rawSingleAttempt is the seam for handing pre-marshaled bytes to the wire.
// maxBytes is the client's MaxSendBytes: a payload over it needs otlpsplit and
// therefore pdata, so the drain decodes that one (<= 0 means splitting is
// disabled and every payload may go raw). All three send funcs are nil when
// the client cannot offer the path at all.
type rawSingleAttempt interface {
	rawSingleAttemptSends() (logs, metrics, traces func(context.Context, []byte) error, maxBytes int)
}

// rawCodec sends one pre-marshaled request body verbatim and delegates the
// response to the registered proto codec, so the caller still gets a real
// ExportResponse (partial_success included). One per call: it carries that
// call's payload.
//
// Name() reports the inner codec's, which keeps the content-subtype — and
// hence the request's content-type header — exactly what an ordinary call
// sends.
type rawCodec struct {
	payload []byte
	inner   encoding.CodecV2
}

func (c *rawCodec) Marshal(any) (mem.BufferSlice, error) {
	// mem.SliceBuffer's Free is a no-op, so grpc never pools or mutates the
	// caller's bytes; the send completes inside Invoke, before the drain
	// touches the queue again.
	return mem.BufferSlice{mem.SliceBuffer(c.payload)}, nil
}

func (c *rawCodec) Unmarshal(data mem.BufferSlice, v any) error { return c.inner.Unmarshal(data, v) }

func (c *rawCodec) Name() string { return c.inner.Name() }

// rawCall builds the per-call codec option, or reports false when this client
// has no proto codec to borrow (grpc registers one in its own init, so this is
// belt and braces; the drain then decodes as it always did).
func (c *Client) rawCall(data []byte) (grpc.CallOption, bool) {
	if c.protoCodec == nil {
		return nil, false
	}
	return grpc.ForceCodecV2(&rawCodec{payload: data, inner: c.protoCodec}), true
}

// rawSingleAttemptSends implements the seam for one destination.
func (c *Client) rawSingleAttemptSends() (func(context.Context, []byte) error, func(context.Context, []byte) error, func(context.Context, []byte) error, int) {
	if c.conn != nil && c.protoCodec == nil {
		return nil, nil, nil, 0
	}
	return c.exportRawLogsCounted, c.exportRawMetricsCounted, c.exportRawTracesCounted, c.cfg.MaxSendBytes
}

// The counted wrappers mirror exportLogsCounted and friends: obs.Exports must
// carry wire-send outcomes whether or not the drain took the raw path.

func (c *Client) exportRawLogsCounted(ctx context.Context, data []byte) error {
	err := c.sendRawLogsOnce(ctx, data)
	obs.Exports.WithLabelValues("logs", outcome(err)).Inc()
	return err
}

func (c *Client) exportRawMetricsCounted(ctx context.Context, data []byte) error {
	err := c.sendRawMetricsOnce(ctx, data)
	obs.Exports.WithLabelValues("metrics", outcome(err)).Inc()
	return err
}

func (c *Client) exportRawTracesCounted(ctx context.Context, data []byte) error {
	err := c.sendRawTracesOnce(ctx, data)
	obs.Exports.WithLabelValues("traces", outcome(err)).Inc()
	return err
}

func (c *Client) sendRawLogsOnce(ctx context.Context, data []byte) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	if c.conn != nil {
		opt, ok := c.rawCall(data)
		if !ok {
			return errNoRawCodec
		}
		ctx, err := c.grpcAuth(ctx)
		if err != nil {
			return err
		}
		// The request VALUE is ignored — rawCodec marshals the payload instead —
		// but the response comes back typed, so partial_success is handled
		// exactly as on the pdata path.
		resp, err := c.logs.Export(ctx, plogotlp.NewExportRequest(), opt)
		if err == nil {
			c.notePartial("logs", resp.PartialSuccess().RejectedLogRecords(), resp.PartialSuccess().ErrorMessage())
		}
		return err
	}
	var raw []byte
	if err := c.httpPost(ctx, c.logsURL, data, &raw); err != nil {
		return err
	}
	if len(raw) > 0 {
		resp := plogotlp.NewExportResponse()
		if resp.UnmarshalProto(raw) == nil {
			c.notePartial("logs", resp.PartialSuccess().RejectedLogRecords(), resp.PartialSuccess().ErrorMessage())
		}
	}
	return nil
}

func (c *Client) sendRawMetricsOnce(ctx context.Context, data []byte) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	if c.conn != nil {
		opt, ok := c.rawCall(data)
		if !ok {
			return errNoRawCodec
		}
		ctx, err := c.grpcAuth(ctx)
		if err != nil {
			return err
		}
		resp, err := c.metrics.Export(ctx, pmetricotlp.NewExportRequest(), opt)
		if err == nil {
			c.notePartial("metrics", resp.PartialSuccess().RejectedDataPoints(), resp.PartialSuccess().ErrorMessage())
		}
		return err
	}
	var raw []byte
	if err := c.httpPost(ctx, c.metricsURL, data, &raw); err != nil {
		return err
	}
	if len(raw) > 0 {
		resp := pmetricotlp.NewExportResponse()
		if resp.UnmarshalProto(raw) == nil {
			c.notePartial("metrics", resp.PartialSuccess().RejectedDataPoints(), resp.PartialSuccess().ErrorMessage())
		}
	}
	return nil
}

func (c *Client) sendRawTracesOnce(ctx context.Context, data []byte) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	if c.conn != nil {
		opt, ok := c.rawCall(data)
		if !ok {
			return errNoRawCodec
		}
		ctx, err := c.grpcAuth(ctx)
		if err != nil {
			return err
		}
		resp, err := c.traces.Export(ctx, ptraceotlp.NewExportRequest(), opt)
		if err == nil {
			c.notePartial("traces", resp.PartialSuccess().RejectedSpans(), resp.PartialSuccess().ErrorMessage())
		}
		return err
	}
	var raw []byte
	if err := c.httpPost(ctx, c.tracesURL, data, &raw); err != nil {
		return err
	}
	if len(raw) > 0 {
		resp := ptraceotlp.NewExportResponse()
		if resp.UnmarshalProto(raw) == nil {
			c.notePartial("traces", resp.PartialSuccess().RejectedSpans(), resp.PartialSuccess().ErrorMessage())
		}
	}
	return nil
}

// rawSingleAttemptSends resolves each signal to ITS destination, like the
// pdata seam. maxBytes is the smallest of the participating destinations':
// they are all derived from one transport config today, and the smallest is
// the only value that is safe for every one of them.
func (p *PerSignal) rawSingleAttemptSends() (func(context.Context, []byte) error, func(context.Context, []byte) error, func(context.Context, []byte) error, int) {
	logs, metrics, traces := p.logsClient(), p.metricsClient(), p.tracesClient()
	maxBytes := 0
	for _, c := range []*Client{logs, metrics, traces} {
		if c == nil {
			continue
		}
		if c.conn != nil && c.protoCodec == nil {
			return nil, nil, nil, 0
		}
		if m := c.cfg.MaxSendBytes; m > 0 && (maxBytes == 0 || m < maxBytes) {
			maxBytes = m
		}
	}
	var lf, mf, tf func(context.Context, []byte) error
	if logs != nil {
		lf = logs.exportRawLogsCounted
	}
	if metrics != nil {
		mf = metrics.exportRawMetricsCounted
	}
	if traces != nil {
		tf = traces.exportRawTracesCounted
	}
	return lf, mf, tf, maxBytes
}
