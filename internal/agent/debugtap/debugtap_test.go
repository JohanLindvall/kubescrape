package debugtap

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type fakeInner struct{ logs, metrics, traces atomic.Int64 }

func (f *fakeInner) ExportLogs(context.Context, plog.Logs) error { f.logs.Add(1); return nil }
func (f *fakeInner) ExportMetrics(context.Context, pmetric.Metrics) error {
	f.metrics.Add(1)
	return nil
}
func (f *fakeInner) ExportTraces(context.Context, ptrace.Traces) error { f.traces.Add(1); return nil }

func logsWithNamespaces(namespaces ...string) plog.Logs {
	ld := plog.NewLogs()
	for _, ns := range namespaces {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("k8s.namespace.name", ns)
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hello from " + ns)
	}
	return ld
}

// One streaming client: filtered resources arrive as JSON lines, non-matching
// ones are cut out of the payload, and the export always reaches the inner
// chain untouched.
func TestStreamFiltersByResourceAttributes(t *testing.T) {
	inner := &fakeInner{}
	tap := New(inner)
	srv := httptest.NewServer(http.HandlerFunc(tap.ServeHTTP))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "?signal=logs&attr=k8s.namespace.*%3Dteam-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	if !sc.Scan() || !strings.HasPrefix(sc.Text(), "#") {
		t.Fatalf("no banner line: %q", sc.Text())
	}

	// The subscriber registers asynchronously with the GET; wait for it.
	deadline := time.Now().Add(5 * time.Second)
	for tap.active.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}

	if err := tap.ExportLogs(context.Background(), logsWithNamespaces("team-a", "kube-system")); err != nil {
		t.Fatal(err)
	}
	if !sc.Scan() {
		t.Fatal("no payload line")
	}
	line := sc.Text()
	if !strings.Contains(line, "team-a") || strings.Contains(line, "kube-system") {
		t.Fatalf("filtering failed: %s", line)
	}
	if inner.logs.Load() != 1 {
		t.Fatalf("inner exports = %d, want 1", inner.logs.Load())
	}

	// Metrics are not subscribed (signal=logs): forwarded, not streamed.
	if err := tap.ExportMetrics(context.Background(), pmetric.NewMetrics()); err != nil {
		t.Fatal(err)
	}
	if inner.metrics.Load() != 1 {
		t.Fatalf("inner metrics = %d, want 1", inner.metrics.Load())
	}
}

// sample=0 keeps nothing; sample=100 keeps everything; the boundary is the
// subscriber's own RNG so the test injects a deterministic one.
func TestSamplePercentage(t *testing.T) {
	tap := New(&fakeInner{})
	tap.randFloat = func() float64 { return 0.5 } // every draw is 50%

	sub, unsub := tap.subscribe(sigLogs, nil, 49)
	if err := tap.ExportLogs(context.Background(), logsWithNamespaces("a")); err != nil {
		t.Fatal(err)
	}
	if len(sub.ch) != 0 {
		t.Fatal("49% sample kept a 50% draw")
	}
	unsub()

	sub, unsub = tap.subscribe(sigLogs, nil, 51)
	defer unsub()
	if err := tap.ExportLogs(context.Background(), logsWithNamespaces("a")); err != nil {
		t.Fatal(err)
	}
	if len(sub.ch) != 1 {
		t.Fatal("51% sample dropped a 50% draw")
	}
}

// A slow reader must never block the export path: over the channel bound the
// payload is dropped for that stream and counted, and the exports all land.
func TestSlowReaderNeverBlocksExports(t *testing.T) {
	inner := &fakeInner{}
	tap := New(inner)
	sub, unsub := tap.subscribe(sigLogs, nil, 100)
	defer unsub()

	n := cap(sub.ch) + 10
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range n {
			_ = tap.ExportLogs(context.Background(), logsWithNamespaces("a"))
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("exports blocked on an unread debug stream")
	}
	if got := inner.logs.Load(); got != int64(n) {
		t.Fatalf("inner exports = %d, want %d", got, n)
	}
	if sub.dropped.Load() != 10 {
		t.Fatalf("dropped = %d, want 10", sub.dropped.Load())
	}
}

func TestBadParametersAnswer400(t *testing.T) {
	tap := New(&fakeInner{})
	srv := httptest.NewServer(http.HandlerFunc(tap.ServeHTTP))
	defer srv.Close()
	for _, q := range []string{"?signal=nope", "?sample=101", "?sample=x", "?attr=noequals", "?attr=k8s.%5B=x"} {
		resp, err := http.Get(srv.URL + q)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", q, resp.StatusCode)
		}
	}
}

func TestUIServes(t *testing.T) {
	tap := New(&fakeInner{})
	rec := httptest.NewRecorder()
	tap.ServeUI(rec, httptest.NewRequest("GET", "/debug/otlp/ui", nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "OTLP debug stream") {
		t.Fatalf("ui = %d %q…", rec.Code, rec.Body.String()[:60])
	}
}

// The subscriber cap: the port is unauthenticated and each stream costs a
// render per export on the exporting goroutine, so streams past the cap are
// refused with 503 — and a closed one frees the slot.
func TestSubscriberCap(t *testing.T) {
	tap := New(&fakeInner{})
	var unsubs []func()
	for i := 0; i < maxSubscribers; i++ {
		sub, unsub := tap.subscribe(sigAll, nil, 100)
		if sub == nil {
			t.Fatalf("subscriber %d refused under the cap", i)
		}
		unsubs = append(unsubs, unsub)
	}
	if sub, _ := tap.subscribe(sigAll, nil, 100); sub != nil {
		t.Fatal("subscriber over the cap admitted")
	}
	srv := httptest.NewServer(http.HandlerFunc(tap.ServeHTTP))
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("over-cap stream = %d, want 503", resp.StatusCode)
	}
	unsubs[0]()
	if sub, unsub := tap.subscribe(sigAll, nil, 100); sub == nil {
		t.Fatal("slot not freed by unsubscribe")
	} else {
		unsub()
	}
	for _, u := range unsubs[1:] {
		u()
	}
}

// The queue is bounded by BYTES, not just slots: a reader that stops reading
// pins at most maxQueuedBytes, and everything past it drops (counted).
func TestQueueIsByteBounded(t *testing.T) {
	tap := New(&fakeInner{})
	sub, unsub := tap.subscribe(sigLogs, nil, 100)
	defer unsub()

	// ~1 MiB per rendered payload; maxQueuedBytes should bind long before the
	// 256-slot channel does.
	big := strings.Repeat("x", 1<<20)
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr(big)
	for i := 0; i < 64; i++ {
		_ = tap.ExportLogs(context.Background(), ld)
	}
	if sub.dropped.Load() == 0 {
		t.Fatal("64 MiB queued with no reader and nothing dropped")
	}
	if q := sub.queued.Load(); q > maxQueuedBytes {
		t.Fatalf("queued %d bytes, cap %d", q, int64(maxQueuedBytes))
	}
}

func TestSampleNaNRejected(t *testing.T) {
	tap := New(&fakeInner{})
	srv := httptest.NewServer(http.HandlerFunc(tap.ServeHTTP))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "?sample=NaN")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("sample=NaN = %d, want 400 (it parses, and every comparison against it is false)", resp.StatusCode)
	}
}
