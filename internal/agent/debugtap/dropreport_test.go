package debugtap

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
)

// A render larger than the stream's WHOLE queue budget can never be delivered,
// however fast the reader. The stream used to report drops only after a later
// DELIVERY and to label every one "(slow reader)", so a stream whose every
// payload was over the budget — a whole ingest push, rendered as JSON —
// showed its banner and then nothing, which is exactly what "nothing matched
// the filters" looks like. The drops are now reported on a timer too, and an
// over-budget render is reported as what it is.
func TestOversizedRendersAreReportedWithoutADelivery(t *testing.T) {
	tap := New(&fakeInner{})
	tap.queueBudget = 1 << 10 // a 4 KiB body renders past it
	tap.reportEvery = 20 * time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(tap.ServeHTTP))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "?signal=logs")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	lines := make(chan string, 16)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	if banner := <-lines; !strings.HasPrefix(banner, "# streaming") {
		t.Fatalf("no banner line: %q", banner)
	}
	deadline := time.Now().Add(5 * time.Second)
	for tap.active.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}

	const exports = 3
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr(strings.Repeat("x", 4<<10))
	for range exports {
		if err := tap.ExportLogs(context.Background(), ld); err != nil {
			t.Fatal(err)
		}
	}

	// Nothing more is exported: the report must arrive on its own. It may be
	// split across ticks, so add the counts up.
	reported := 0
	timeout := time.After(5 * time.Second)
	for reported < exports {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatalf("stream ended after reporting %d of %d skipped payloads", reported, exports)
			}
			if !strings.HasPrefix(line, "#") {
				t.Fatalf("an over-budget payload was delivered: %.80s", line)
			}
			if strings.Contains(line, "slow reader") {
				t.Fatalf("an over-budget render was reported as a slow reader, whose remedy is the opposite: %q", line)
			}
			var n int
			if _, err := fmt.Sscanf(line, "# %d payload(s) rendered larger than this stream's whole queue budget", &n); err != nil {
				t.Fatalf("unexpected stream line %q: %v", line, err)
			}
			reported += n
		case <-timeout:
			t.Fatalf("after %d over-budget payloads and no delivery, the stream reported %d: its reader sees the banner and then silence", exports, reported)
		}
	}
	if reported != exports {
		t.Errorf("reported %d skipped payloads, want %d", reported, exports)
	}
}
