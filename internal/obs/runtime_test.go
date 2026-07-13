package obs

import (
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// TestRuntimeHandlerConcurrentScrapes hammers /metrics from 32 goroutines
// (the agent and service expose it to any in-cluster scraper): every response
// must be a well-formed 200 and the heap must stay steady — a per-request
// allocation blowup here would let a scrape loop balloon the process. Run
// under -race for the concurrency guarantee.
func TestRuntimeHandlerConcurrentScrapes(t *testing.T) {
	srv := httptest.NewServer(RuntimeHandler())
	defer srv.Close()

	scrape := func() (int, int64) {
		resp, err := http.Get(srv.URL)
		if err != nil {
			return -1, 0
		}
		n, _ := io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode, n
	}

	// Warm up, then measure steady-state heap growth across the hammer.
	if code, n := scrape(); code != http.StatusOK || n == 0 {
		t.Fatalf("warmup scrape: status %d, %d bytes", code, n)
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	const workers, perWorker = 32, 25
	var bad atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				if code, n := scrape(); code != http.StatusOK || n == 0 {
					bad.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("%d scrapes failed", bad.Load())
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	// 800 scrapes must not retain memory; allow generous slack for runtime
	// noise (pooled buffers, GC timing).
	if growth := int64(after.HeapAlloc) - int64(before.HeapAlloc); growth > 16<<20 {
		t.Fatalf("heap grew %d bytes across %d scrapes", growth, workers*perWorker)
	}
}
