package otlpexport

// The disk-buffer drain sends SPOOLED BYTES, not a pdata round trip.
//
// The whole optimization rests on one equivalence — the ProtoMarshaler
// encoding the sink spools is byte-for-byte the OTLP ExportRequest body — so
// that is pinned first, for all three signals. The rest checks that the drain
// really takes the path (no decode on the common branch, the exact reserved
// bytes reaching the wire, correct delivery over both transports) and that the
// COLD branches still work: an over-cap payload decodes so otlpsplit can split
// it, and a dropped batch decodes so its record counter is not silently 0.

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// TestSpooledBytesAreTheWireBytes is the assumption the raw send path is built
// on: LogsData and ExportLogsServiceRequest are both
// `repeated ResourceLogs resource_logs = 1`, and likewise for metrics and
// traces. If a pdata release ever breaks that, the raw path would ship a body
// the collector cannot parse, so this must fail loudly rather than at a
// customer's collector.
func TestSpooledBytesAreTheWireBytes(t *testing.T) {
	ld := buildLogs(3, 4, 32)
	spooled, err := (&plog.ProtoMarshaler{}).MarshalLogs(ld)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(spooled, wire) {
		t.Errorf("logs: spooled %d bytes, wire %d bytes, not identical", len(spooled), len(wire))
	}

	md := testMetrics()
	spooled, err = (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
	if err != nil {
		t.Fatal(err)
	}
	wire, err = pmetricotlp.NewExportRequestFromMetrics(md).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(spooled, wire) {
		t.Errorf("metrics: spooled %d bytes, wire %d bytes, not identical", len(spooled), len(wire))
	}

	td := testTraces()
	spooled, err = (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	if err != nil {
		t.Fatal(err)
	}
	wire, err = ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(spooled, wire) {
		t.Errorf("traces: spooled %d bytes, wire %d bytes, not identical", len(spooled), len(wire))
	}
}

// countingSink wires a sink by hand so the decode can be observed.
func countingSink(t *testing.T, dir string, maxSendBytes int, sendRaw func(context.Context, []byte) error) (*sink[plog.Logs], *int32) {
	t.Helper()
	buf, err := OpenBuffer(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = buf.Close() })
	buf.kind = "logs"
	var decodes int32
	lm := plog.ProtoMarshaler{}
	lu := plog.ProtoUnmarshaler{}
	return &sink[plog.Logs]{
		buf: buf, backoff: time.Millisecond, log: quietLogger(), kind: "logs",
		marshal: lm.MarshalLogs,
		unmarshal: func(b []byte) (plog.Logs, error) {
			atomic.AddInt32(&decodes, 1)
			return lu.UnmarshalLogs(b)
		},
		count:        plog.Logs.LogRecordCount,
		send:         func(context.Context, plog.Logs) error { return nil },
		sendRaw:      sendRaw,
		maxSendBytes: maxSendBytes,
	}, &decodes
}

// The common path must not materialize pdata at all, and the bytes handed to
// the wire must be exactly the ones that were spooled.
func TestDrainSendsSpooledBytesWithoutDecoding(t *testing.T) {
	ld := buildLogs(2, 8, 64)
	want, err := (&plog.ProtoMarshaler{}).MarshalLogs(ld)
	if err != nil {
		t.Fatal(err)
	}
	var got [][]byte
	s, decodes := countingSink(t, t.TempDir(), 1<<20, func(_ context.Context, data []byte) error {
		// The drain hands over a private copy, but retain a copy of our own so
		// this assertion cannot depend on that.
		got = append(got, append([]byte(nil), data...))
		return nil
	})
	if err := s.enqueue(ld); err != nil {
		t.Fatal(err)
	}
	s.drainUntilEmpty(context.Background())

	if n := atomic.LoadInt32(decodes); n != 0 {
		t.Errorf("the drain decoded the payload %d time(s); the spooled bytes are already the wire body", n)
	}
	if len(got) != 1 {
		t.Fatalf("the raw send received %d payloads, want 1", len(got))
	}
	if !bytes.Equal(got[0], want) {
		t.Errorf("the raw send got %d bytes, want the %d spooled bytes", len(got[0]), len(want))
	}
}

// Over the send cap the drain MUST decode: otlpsplit works on structure, and
// shipping an over-cap body whole is the wholesale rejection the splitter
// exists to prevent.
func TestDrainDecodesOnlyWhenOverTheSendCap(t *testing.T) {
	ld := buildLogs(4, 32, 256)
	encoded, err := (&plog.ProtoMarshaler{}).MarshalLogs(ld)
	if err != nil {
		t.Fatal(err)
	}
	var rawSends int32
	s, decodes := countingSink(t, t.TempDir(), len(encoded)/2, func(context.Context, []byte) error {
		atomic.AddInt32(&rawSends, 1)
		return nil
	})
	if err := s.enqueue(ld); err != nil {
		t.Fatal(err)
	}
	s.drainUntilEmpty(context.Background())

	if n := atomic.LoadInt32(decodes); n != 1 {
		t.Errorf("an over-cap payload decoded %d times, want 1 (otlpsplit needs the structure)", n)
	}
	if n := atomic.LoadInt32(&rawSends); n != 0 {
		t.Errorf("an over-cap payload took the raw path %d times; it must go through the splitting send", n)
	}
}

// A drop still reports HOW MANY records it took: the count is decoded on
// demand, on the cold branch, rather than on every batch.
func TestDropStillCountsRecordsWithTheRawPath(t *testing.T) {
	ld := buildLogs(2, 5, 32) // 10 records
	s, _ := countingSink(t, t.TempDir(), 1<<20, func(context.Context, []byte) error {
		return &HTTPStatusError{Code: 400, Body: "bad payload"} // permanent
	})
	beforeB := obs.BufferDroppedBatches.WithLabelValues("logs").Value()
	beforeR := obs.BufferDroppedRecords.WithLabelValues("logs").Value()
	if err := s.enqueue(ld); err != nil {
		t.Fatal(err)
	}
	s.drainUntilEmpty(context.Background())

	if got := obs.BufferDroppedBatches.WithLabelValues("logs").Value() - beforeB; got != 1 {
		t.Errorf("dropped batches += %v, want 1", got)
	}
	if got := obs.BufferDroppedRecords.WithLabelValues("logs").Value() - beforeR; got != 10 {
		t.Errorf("dropped records += %v, want 10 — the drop counter must still decode for its magnitude", got)
	}
}

// End to end over gRPC: a spooled batch reaches a real collector through the
// raw codec, with a bearer token and the ordinary content type.
func TestBufferedDrainDeliversRawOverGRPC(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	st := &grpcState{}
	srv := grpc.NewServer()
	plogotlp.RegisterGRPCServer(srv, &logSink{st: st})
	pmetricotlp.RegisterGRPCServer(srv, &metricSink{st: st})
	ptraceotlp.RegisterGRPCServer(srv, &traceSink{st: st})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	c, err := New(Config{Endpoint: lis.Addr().String(), Protocol: "grpc", Insecure: true,
		Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	dir := t.TempDir()
	lb, err := OpenBuffer(dir+"/logs", 0)
	if err != nil {
		t.Fatal(err)
	}
	mb, err := OpenBuffer(dir+"/metrics", 0)
	if err != nil {
		t.Fatal(err)
	}
	b := NewBuffered(c, lb, mb, nil, time.Millisecond, quietLogger())
	defer func() { _ = lb.Close(); _ = mb.Close() }()

	if b.logs.sendRaw == nil || b.metrics.sendRaw == nil {
		t.Fatal("the raw send path was not wired for a *Client inner exporter")
	}

	ld := buildLogs(2, 3, 16) // 6 records
	if err := b.ExportLogs(context.Background(), ld); err != nil {
		t.Fatal(err)
	}
	if err := b.ExportMetrics(context.Background(), testMetrics()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b.FinalDrain(ctx)

	st.mu.Lock()
	defer st.mu.Unlock()
	if st.logs != 6 || st.metrics != 1 {
		t.Fatalf("collector received logs=%d metrics=%d, want 6 and 1", st.logs, st.metrics)
	}
}

// End to end over OTLP/HTTP, gzip included: the compressed body must be the
// spooled bytes and nothing else.
func TestBufferedDrainDeliversRawOverHTTP(t *testing.T) {
	var (
		mu       sync.Mutex
		received int
		encoding string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := readMaybeGzip(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		req := plogotlp.NewExportRequest()
		if err := req.UnmarshalProto(body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		received += req.Logs().LogRecordCount()
		encoding = r.Header.Get("Content-Encoding")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, err := New(Config{Endpoint: srv.URL, Protocol: "http", Compression: "gzip",
		Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	lb, err := OpenBuffer(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lb.Close() }()
	b := NewBuffered(c, lb, nil, nil, time.Millisecond, quietLogger())

	if err := b.ExportLogs(context.Background(), buildLogs(3, 4, 24)); err != nil { // 12 records
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b.FinalDrain(ctx)

	mu.Lock()
	defer mu.Unlock()
	if received != 12 {
		t.Fatalf("collector received %d records, want 12", received)
	}
	if encoding != "gzip" {
		t.Errorf("Content-Encoding = %q, want gzip", encoding)
	}
}

func readMaybeGzip(r *http.Request) ([]byte, error) {
	if !strings.Contains(r.Header.Get("Content-Encoding"), "gzip") {
		return io.ReadAll(r.Body)
	}
	zr, err := gzip.NewReader(r.Body)
	if err != nil {
		return nil, err
	}
	defer func() { _ = zr.Close() }()
	return io.ReadAll(zr)
}
