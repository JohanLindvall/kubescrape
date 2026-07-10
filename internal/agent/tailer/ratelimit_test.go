package tailer

import (
	"fmt"
	"testing"
	"time"
)

// rateLines writes n single-fragment CRI lines with current timestamps.
func rateLines(t *testing.T, dir string, from, n int) {
	t.Helper()
	lines := make([]string, 0, n)
	for i := 0; i < n; i++ {
		lines = append(lines, fmt.Sprintf("%s stdout F line-%03d", timeNowCRI(), from+i))
	}
	writeLog(t, dir, lines...)
}

// Pause mode: an exhausted file stops being read but nothing is lost — the
// backlog drains as tokens refill.
func TestRateLimitPause(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.cfg.RateLimit = 40 // lines/s
	tl.cfg.RateBurst = 10
	stop := startTailer(t, tl)
	defer stop()

	rateLines(t, dir, 0, 60)

	// Shortly after the burst is consumed only a fraction may have passed
	// (burst 10 + <=400ms of refill ≈ 26 max, with margin).
	time.Sleep(300 * time.Millisecond)
	if n := len(exp.get()); n >= 60 {
		t.Fatalf("rate limit had no effect: %d records immediately", n)
	}

	// Everything arrives eventually, in order (pause loses nothing).
	waitFor(t, func() bool { return len(exp.get()) == 60 }, "all 60 records")
	got := exp.get()
	for i, body := range got {
		if want := fmt.Sprintf("line-%03d", i); body != want {
			t.Fatalf("record %d = %q, want %q (order must be preserved)", i, body, want)
		}
	}
}

// Drop mode: excess lines are discarded, reading never stalls.
func TestRateLimitDrop(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.cfg.RateLimit = 20
	tl.cfg.RateBurst = 10
	tl.cfg.RateDrop = true
	stop := startTailer(t, tl)
	defer stop()

	rateLines(t, dir, 0, 200)

	// The file must drain promptly (drop mode never pauses); with burst 10 and
	// ~20/s refill only a fraction of the 200 survives.
	waitFor(t, func() bool { return len(exp.get()) >= 5 }, "some records")
	time.Sleep(500 * time.Millisecond)
	n := len(exp.get())
	if n >= 200 {
		t.Fatalf("drop mode exported all %d records", n)
	}
	// Survivors keep their original relative order.
	got := exp.get()
	last := -1
	for _, body := range got {
		var idx int
		if _, err := fmt.Sscanf(body, "line-%d", &idx); err != nil {
			t.Fatalf("unexpected body %q", body)
		}
		if idx <= last {
			t.Fatalf("out-of-order survivor %q after line-%03d", body, last)
		}
		last = idx
	}
}

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
