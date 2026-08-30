package leader

// "Which pod is the leader?" and "did we just fail over?" must both be
// answerable from the log alone.

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuffer is a bytes.Buffer safe for the election's goroutines.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// A graceful stop RELEASES the lease — ReleaseOnCancel hands it straight to a
// successor. Reported as "lost leadership" it made every rolling update print
// the one line an operator greps for when the singleton has STOPPED working, so
// the unplanned failover was indistinguishable from a deploy.
func TestGracefulStopReportsAReleaseNotALoss(t *testing.T) {
	var buf syncBuffer
	started := make(chan struct{})
	cfg := testConfig(func(ctx context.Context) {
		close(started)
		<-ctx.Done()
	})
	cfg.Log = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("never acquired the lease")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}

	out := buf.String()
	if strings.Contains(out, "lost leadership") {
		t.Errorf("a graceful shutdown was reported as a lost lease:\n%s", out)
	}
	if !strings.Contains(out, "released leadership on shutdown") {
		t.Errorf("the release is not reported at all:\n%s", out)
	}
	// The identity is what answers "which pod was it": the lease name alone
	// does not, and it is the same on every replica.
	if !strings.Contains(out, "identity=pod-a") {
		t.Errorf("the release does not name the replica:\n%s", out)
	}
	if !strings.Contains(out, "acquired leadership") {
		t.Errorf("the acquisition is not reported:\n%s", out)
	}
	// A release is lifecycle, not a problem.
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "released leadership") && !strings.Contains(line, "level=INFO") {
			t.Errorf("the release line is not Info: %s", line)
		}
	}
}
