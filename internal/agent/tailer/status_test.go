// Tests for the status snapshot (status.go).
package tailer

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Status reports per-file positions and lag for /debug/tailer.
func TestStatus(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.statusEvery = 30 * time.Millisecond
	stop := startTailer(t, tl)
	defer stop()

	rateLines(t, dir, 0, 3)
	waitFor(t, func() bool { return len(exp.get()) == 3 }, "3 records")

	waitFor(t, func() bool {
		for _, fs := range tl.Status() {
			if fs.Path != "" && fs.Committed > 0 && fs.Lag == 0 && fs.Resolved {
				return true
			}
		}
		return false
	}, "a caught-up file status")

	st := tl.Status()
	if len(st) != 1 || st[0].ContainerID == "" || st[0].Size != st[0].Committed {
		t.Fatalf("status = %+v", st)
	}
}

// TestStatusConcurrentScrape hammers the /debug/tailer serving path (an HTTP
// handler JSON-encoding Tailer.Status, exactly as cmd/kubescrape-agent wires
// it) from 16 goroutines while the tailer tails a live, growing file. The
// snapshot is published through an atomic pointer from the sweep goroutine;
// this is the -race exerciser proving readers never observe a torn snapshot
// and the handler stays alive under scrape pressure.
func TestStatusConcurrentScrape(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.statusEvery = 10 * time.Millisecond // publish aggressively
	stop := startTailer(t, tl)
	defer stop()

	// The exact handler shape from cmd/kubescrape-agent's mux.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/tailer", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(tl.Status())
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	duration := time.Second
	if testing.Short() {
		duration = 300 * time.Millisecond
	}
	done := make(chan struct{})

	// Writer: keep the file growing so positions/lag churn under the scrapes.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-done:
				return
			default:
			}
			i++
			writeLog(t, dir, fmt.Sprintf("%s stdout F line %d", timeNowCRI(), i))
			time.Sleep(2 * time.Millisecond)
		}
	}()

	var scrapes, failures atomic.Int64
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				resp, err := http.Get(srv.URL + "/debug/tailer")
				if err != nil {
					failures.Add(1)
					return
				}
				var got []FileStatus
				body, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusOK || json.Unmarshal(body, &got) != nil {
					failures.Add(1)
					return
				}
				for _, fs := range got {
					if fs.Lag < 0 || fs.Committed < 0 || fs.ReadPos < 0 {
						failures.Add(1)
						return
					}
				}
				scrapes.Add(1)
			}
		}()
	}

	time.Sleep(duration)
	close(done)
	wg.Wait()
	if failures.Load() != 0 {
		t.Fatalf("%d scrape failures", failures.Load())
	}
	if scrapes.Load() == 0 {
		t.Fatal("no scrapes completed")
	}
	waitFor(t, func() bool { return len(tl.Status()) == 1 }, "status to include the file")
	t.Logf("status scrapes: %d in %v (%.0f/s)", scrapes.Load(), duration, float64(scrapes.Load())/duration.Seconds())
}
