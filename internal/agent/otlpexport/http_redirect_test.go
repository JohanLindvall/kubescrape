package otlpexport

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
)

// A collector behind a redirecting ingress makes net/http close the request
// body twice — the transport when it finishes writing, and Client.do at the
// bottom of its redirect loop, which runs BEFORE CheckRedirect is consulted.
// bufpool's Close is Release, so the same buffer went into a package-level
// pool twice while the writeLoop could still be reading it. Run this with
// -race; without the fix it reports both the double Put and the Read/reset.
func TestRedirectDoesNotDoubleReleaseTheGzipBody(t *testing.T) {
	var wg sync.WaitGroup
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Answer BEFORE reading the body: the writeLoop is then still
		// streaming it when the client processes the response.
		http.Redirect(w, r, "http://127.0.0.1:1/v1/logs", http.StatusFound)
	}))
	defer srv.Close()

	c, err := New(Config{
		Endpoint: srv.URL, Protocol: "http", Compression: "gzip",
		Timeout: 5 * time.Second, RetryAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	// A body big enough that writing it cannot complete before the redirect
	// response is processed.
	ld := plog.NewLogs()
	lrs := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for i := 0; i < 20000; i++ {
		lrs.AppendEmpty().Body().SetStr("a moderately long log line to make the payload big enough to stream")
	}

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// The 302 must surface as an error, never as a false ack.
			if err := c.ExportLogs(context.Background(), ld); err == nil {
				t.Error("a redirect was reported as a successful export; offsets would commit for records the collector never saw")
			}
		}()
	}
	wg.Wait()

	// The pool must still hand out usable buffers afterwards.
	if b, err := gzipBody([]byte("still works")); err != nil {
		t.Fatalf("gzip pool unusable after the redirects: %v", err)
	} else {
		b.Release()
	}
}

// The invariant the redirect path depends on, tested directly because the
// race it prevents is timing-dependent: net/http may Close a request body
// TWICE (the transport when it finishes writing, Client.do again at the
// bottom of its redirect loop) and the second Close can land while the
// transport is still reading. bufpool.Release resets the handle and returns
// the storage to a package-level pool, so a Close that released would reset
// memory under an active Read and hand live storage to another goroutine.
func TestPooledBodyReleasesOnlyOnceItIsFullyRead(t *testing.T) {
	gz, err := gzipBody([]byte("a payload net/http is still streaming"))
	if err != nil {
		t.Fatal(err)
	}
	pb := &pooledBody{buf: gz}
	size := pb.buf.Len()

	// net/http closes it twice while the write is still in flight.
	_ = pb.Close()
	_ = pb.Close()
	pb.release()
	if pb.buf.Len() != size {
		t.Fatal("the buffer was released while the transport could still be reading it")
	}

	// Read to EOF — now the transport provably cannot touch it again.
	if _, err := io.Copy(io.Discard, pb); err != nil {
		t.Fatal(err)
	}
	pb.release()
	if pb.buf.Len() != 0 {
		t.Fatal("a fully-read buffer was not returned to the pool")
	}
	pb.release() // idempotent: a second release must not re-pool it
}
