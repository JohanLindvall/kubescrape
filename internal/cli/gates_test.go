package cli

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// Both binaries' readiness loops run through WatchGates, so its contract is
// pinned once: the pending list rides under `gates` (the metric label's
// value), the first warning lands at the grace and the repeats are throttled,
// and readiness is reported to the caller the poll after the last gate clears.
func TestWatchGatesWarnsUnderGatesThenReportsReady(t *testing.T) {
	var clear atomic.Bool
	var buf lockedBuffer
	w := GateWatch{
		Pending: func() []string {
			if clear.Load() {
				return nil
			}
			return []string{"pods", "services"}
		},
		Poll: time.Millisecond, Grace: 10 * time.Millisecond, ReWarn: time.Hour,
		NotReady: "not ready", Note: "check RBAC", ShuttingDown: "shutting down",
	}
	type result struct {
		ready  bool
		waited time.Duration
	}
	done := make(chan result, 1)
	go func() {
		ready, waited := WatchGates(context.Background(), slog.New(NewLogfmtHandler(&buf, slog.LevelInfo)), w)
		done <- result{ready, waited}
	}()
	deadline := time.After(2 * time.Second)
	for !strings.Contains(buf.String(), "not ready") {
		select {
		case <-deadline:
			t.Fatalf("no warning after the grace:\n%s", buf.String())
		case <-time.After(time.Millisecond):
		}
	}
	// Past the first warning, polls keep coming; the hour-long ReWarn must hold
	// every one of them back.
	time.Sleep(30 * time.Millisecond)
	clear.Store(true)
	var r result
	select {
	case r = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("WatchGates did not return once every gate cleared")
	}
	if !r.ready || r.waited < w.Grace {
		t.Errorf("WatchGates = (%v, %v), want ready after at least the grace", r.ready, r.waited)
	}
	out := buf.String()
	if n := strings.Count(out, "not ready"); n != 1 {
		t.Errorf("warned %d times inside one ReWarn window, want 1:\n%s", n, out)
	}
	for _, want := range []string{"level=WARN", "gates=pods,services", "elapsed=", `note="check RBAC"`} {
		if !strings.Contains(out, want) {
			t.Errorf("the warning lacks %q:\n%s", want, out)
		}
	}
}

// A process shut down with gates pending says so once and reports not-ready;
// one shut down with nothing pending says nothing.
func TestWatchGatesReportsAShutdownBeforeReady(t *testing.T) {
	for _, pending := range [][]string{{"metadata-service"}, nil} {
		var buf lockedBuffer
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		ready, _ := WatchGates(ctx, slog.New(NewLogfmtHandler(&buf, slog.LevelInfo)), GateWatch{
			Pending: func() []string { return pending },
			Poll:    time.Hour, Grace: time.Hour, ReWarn: time.Hour,
			NotReady: "not ready", ShuttingDown: "shutting down before becoming ready",
		})
		if ready {
			t.Errorf("pending=%v: a cancelled watch reported ready", pending)
		}
		said := strings.Contains(buf.String(), `msg="shutting down before becoming ready" gates=metadata-service`)
		if said != (pending != nil) {
			t.Errorf("pending=%v: shutdown line present=%v:\n%s", pending, said, buf.String())
		}
	}
}
