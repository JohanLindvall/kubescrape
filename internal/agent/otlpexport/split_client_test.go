package otlpexport

// Client-side integration of the payload splitter (pkg/otlpsplit): one
// logical export counts once in obs regardless of part count, and a
// mid-sequence part failure fails fast.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/otlpsplit"
)

func buildLogs(resources, recordsPer, bodyLen int) plog.Logs {
	ld := plog.NewLogs()
	for r := 0; r < resources; r++ {
		rl := ld.ResourceLogs().AppendEmpty()
		res := rl.Resource().Attributes()
		res.PutStr("service.name", fmt.Sprintf("svc-%d", r))
		res.PutStr("k8s.pod.name", fmt.Sprintf("pod-%d-abcdef", r))
		res.PutStr("k8s.namespace.name", "production")
		res.PutStr("k8s.node.name", "node-01.internal.example.com")
		sl := rl.ScopeLogs().AppendEmpty()
		sl.Scope().SetName("test")
		for i := 0; i < recordsPer; i++ {
			lr := sl.LogRecords().AppendEmpty()
			lr.Body().SetStr(strings.Repeat("x", bodyLen))
			lr.Attributes().PutStr("log.iostream", "stdout")
			lr.Attributes().PutInt("id", int64(r*recordsPer+i))
		}
	}
	return ld
}

// ---------------------------------------------------------------------------
// Angle 5: obs.Exports counts once per LOGICAL export regardless of part count;
// a mid-sequence part failure fails fast and is counted exactly once.
// ---------------------------------------------------------------------------

func TestAuditObsCountsOncePerLogicalExport(t *testing.T) {
	var posts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		posts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, err := New(Config{Endpoint: srv.URL, Protocol: "http", Timeout: 5 * time.Second,
		Compression: "none", MaxSendBytes: 16 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	before := obs.Exports.WithLabelValues("logs", "ok").Value()
	if err := c.ExportLogs(context.Background(), buildLogs(1, 2000, 60)); err != nil {
		t.Fatal(err)
	}
	if posts.Load() < 2 {
		t.Fatalf("expected multiple POSTs, got %d", posts.Load())
	}
	if delta := obs.Exports.WithLabelValues("logs", "ok").Value() - before; delta != 1 {
		t.Fatalf("obs.Exports{logs,ok} delta = %v, want exactly 1 per logical export", delta)
	}
}

func TestAuditPartialSendFailsFastCountedOnce(t *testing.T) {
	var posts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if posts.Add(1) == 2 {
			http.Error(w, "boom", http.StatusServiceUnavailable) // transient
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, err := New(Config{Endpoint: srv.URL, Protocol: "http", Timeout: 5 * time.Second,
		Compression: "none", MaxSendBytes: 16 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	beforeErr := obs.Exports.WithLabelValues("logs", "error").Value()
	err = c.ExportLogs(context.Background(), buildLogs(1, 2000, 60))
	if err == nil {
		t.Fatal("expected error when part 2 fails")
	}
	// Fail-fast: no POST after the failing one.
	if posts.Load() != 2 {
		t.Fatalf("posts = %d, want 2 (fail-fast, no send after the failure)", posts.Load())
	}
	if delta := obs.Exports.WithLabelValues("logs", "error").Value() - beforeErr; delta != 1 {
		t.Fatalf("obs.Exports{logs,error} delta = %v, want exactly 1", delta)
	}
}

// ---------------------------------------------------------------------------
// BUG angle 4: a byte-oversized Logs whose only content is record-less scopes
// splits to ZERO parts, so exportLogsOnce reports success without sending
// anything (the value is silently "accepted"). Contrast with the under-cap
// path, which copies the whole ResourceLogs (empty scopes included).
// ---------------------------------------------------------------------------

func TestOverCapRecordlessResourceSingleWireSend(t *testing.T) {
	const maxBytes = 4 << 10
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	// One giant resource attribute pushes the resource over the cap.
	rl.Resource().Attributes().PutStr("huge", strings.Repeat("H", 8<<10))
	rl.ScopeLogs().AppendEmpty().Scope().SetName("record-less-scope")

	if (&plog.ProtoMarshaler{}).LogsSize(ld) <= maxBytes {
		t.Fatalf("test precondition: payload must exceed cap")
	}
	// A non-empty input must never yield zero parts (that would report the
	// export delivered while sending nothing): the over-cap record-less resource
	// is sent whole — rejected and counted at the collector, never dropped.
	parts := otlpsplit.Logs(ld, maxBytes)
	if len(parts) != 1 {
		t.Fatalf("over-cap record-less resource produced %d parts, want 1 (sent whole, never silently dropped)", len(parts))
	}

	// exportLogsOnce therefore makes exactly one wire send, not zero.
	var posts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		posts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c, _ := New(Config{Endpoint: srv.URL, Protocol: "http", Timeout: 2 * time.Second,
		Compression: "none", MaxSendBytes: maxBytes})
	defer func() { _ = c.Close() }()
	if err := c.exportLogsOnce(context.Background(), ld); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if posts.Load() != 1 {
		t.Fatalf("over-cap payload made %d wire sends, want 1 (never silent success)", posts.Load())
	}
}
