package otlpingest

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/gzip"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// exporterFunc adapts a func to the Exporter interface (logs only).
type exporterFunc func(plog.Logs) error

func (f exporterFunc) ExportLogs(_ context.Context, ld plog.Logs) error { return f(ld) }

func (f exporterFunc) ExportMetrics(_ context.Context, _ pmetric.Metrics) error { return nil }

// bigLogsPayload builds a serialized OTLP logs request whose protobuf size is
// close to (but under) target bytes.
func bigLogsPayload(t *testing.T, target int) []byte {
	t.Helper()
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("container.id", "cafe01")
	lrs := rl.ScopeLogs().AppendEmpty().LogRecords()
	chunk := strings.Repeat("x", 64<<10)
	for size := 0; size < target-(128<<10); size += len(chunk) {
		lrs.AppendEmpty().Body().SetStr(chunk)
	}
	body, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	if len(body) >= target {
		t.Fatalf("payload construction overshot: %d >= %d", len(body), target)
	}
	return body
}

func gzipBytes(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// waitGoroutines polls until the goroutine count settles at or below want.
func waitGoroutines(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("goroutines did not settle: %d > %d", runtime.NumGoroutine(), want)
}

// Concurrent near-cap gzip bodies must all be handled consistently (200 for
// under-cap, 413 for over-cap), with no goroutine left behind afterwards.
func TestConcurrentGzipBodiesAtCap(t *testing.T) {
	var exported atomic.Int64
	srv := httpTestServer(t, exporterFunc(func(ld plog.Logs) error {
		exported.Add(1)
		return nil
	}))

	underCap := bigLogsPayload(t, maxIngestBody) // decompressed just under 16 MiB
	underGz := gzipBytes(t, underCap)

	// Over the decompressed cap: pad one payload past the limit.
	over := make([]byte, maxIngestBody+512)
	copy(over, underCap)
	overGz := gzipBytes(t, over)

	before := runtime.NumGoroutine()
	const workers = 8
	var wg sync.WaitGroup
	statuses := make([]int, workers)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := underGz
			if w == workers-1 {
				body = overGz
			}
			req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/logs", bytes.NewReader(body))
			if err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/x-protobuf")
			req.Header.Set("Content-Encoding", "gzip")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				statuses[w] = -1
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			statuses[w] = resp.StatusCode
		}()
	}
	wg.Wait()

	for w, st := range statuses {
		want := http.StatusOK
		if w == workers-1 {
			want = http.StatusRequestEntityTooLarge
		}
		if st != want {
			t.Errorf("worker %d: status %d, want %d", w, st, want)
		}
	}
	if got := exported.Load(); got != workers-1 {
		t.Errorf("exported %d batches, want %d", got, workers-1)
	}
	http.DefaultClient.CloseIdleConnections()
	waitGoroutines(t, before+3)
}

// An abrupt client disconnect mid-body must fail the read, never wedge the
// handler; the server keeps serving and no goroutine leaks.
func TestClientDisconnectMidBody(t *testing.T) {
	var exported atomic.Int64
	srv := httpTestServer(t, exporterFunc(func(plog.Logs) error {
		exported.Add(1)
		return nil
	}))
	addr := strings.TrimPrefix(srv.URL, "http://")

	before := runtime.NumGoroutine()
	for range 10 {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = fmt.Fprintf(conn,
			"POST /v1/logs HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-protobuf\r\nContent-Length: 1000000\r\n\r\n")
		_, _ = conn.Write(bytes.Repeat([]byte{0x0a}, 1024)) // 1 KiB of the promised 1 MB
		_ = conn.Close()                                    // hang up mid-body
	}
	waitGoroutines(t, before+3)
	if exported.Load() != 0 {
		t.Errorf("truncated bodies were exported: %d", exported.Load())
	}

	// The server still works.
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("alive")
	body, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(srv.URL+"/v1/logs", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("post-disconnect request: status %d", resp.StatusCode)
	}
}

// Content-Length lies: a shorter-than-body length truncates the payload at
// CL (a malformed proto → 400, never a silent partial ACK); a longer-than-
// body length starves the read (error, never a hang once the client is gone).
func TestContentLengthLies(t *testing.T) {
	var exported atomic.Int64
	srv := httpTestServer(t, exporterFunc(func(plog.Logs) error {
		exported.Add(1)
		return nil
	}))
	addr := strings.TrimPrefix(srv.URL, "http://")

	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr(strings.Repeat("z", 4096))
	body, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("shorter", func(t *testing.T) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		_, _ = fmt.Fprintf(conn,
			"POST /v1/logs HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-protobuf\r\nContent-Length: %d\r\n\r\n", len(body)/2)
		_, _ = conn.Write(body) // more bytes than promised
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatalf("no response: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("status %d, want 400 (truncated proto must not be ACKed)", resp.StatusCode)
		}
		if exported.Load() != 0 {
			t.Errorf("truncated payload was exported")
		}
	})

	t.Run("longer", func(t *testing.T) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = fmt.Fprintf(conn,
			"POST /v1/logs HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-protobuf\r\nContent-Length: %d\r\n\r\n", len(body)*2)
		_, _ = conn.Write(body) // fewer bytes than promised
		_ = conn.Close()        // and hang up
		// The handler's read fails with unexpected EOF; nothing to assert on
		// the wire, but nothing may be exported and nothing may leak.
		time.Sleep(300 * time.Millisecond)
		if exported.Load() != 0 {
			t.Errorf("starved payload was exported")
		}
	})
}
