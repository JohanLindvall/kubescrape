package bearer

import (
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Cached is the accessor for a caller that must never block — the trace tier's
// gRPC auth tap, which runs on the connection's frame-read goroutine. It must
// answer even when a refresh is due AND the mount is wedged (a FIFO with no
// writer: open(2) hangs), which is exactly when Tokens would claim the read.
func TestCachedNeverReadsTheTokenFile(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "real")
	if err := os.WriteFile(real, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(dir, "wedged")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("mkfifo unsupported here: %v", err)
	}
	t.Cleanup(func() {
		if f, err := os.OpenFile(fifo, os.O_WRONLY|syscall.O_NONBLOCK, 0); err == nil {
			_ = f.Close()
		}
	})
	var mu sync.Mutex
	now := time.Now()
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	r, err := NewRotating(real, discard(), WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	r.path = fifo
	mu.Lock()
	now = now.Add(10 * time.Second) // a refresh is due
	mu.Unlock()

	done := make(chan []string, 1)
	go func() { done <- r.Cached() }()
	select {
	case got := <-done:
		if len(got) != 1 || got[0] != "first" {
			t.Fatalf("Cached() = %v, want the last good set [first]", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Cached() blocked on the token file")
	}
}

// Cached must not become a way to keep accepting a revoked token: the
// predecessor leaves the set at the end of its grace window on this path too.
func TestCachedDropsThePredecessorPastItsGrace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	now := time.Now()
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }
	r, err := NewRotating(path, discard(), WithClock(clock), WithGrace(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("second"), 0o600); err != nil {
		t.Fatal(err)
	}
	advance(2 * time.Second)
	r.Tokens() // the refresh that notices the rotation and arms the grace

	if got := r.Cached(); len(got) != 2 || got[0] != "second" || got[1] != "first" {
		t.Fatalf("inside the grace Cached() = %v, want [second first]", got)
	}
	advance(2 * time.Minute)
	if got := r.Cached(); len(got) != 1 || got[0] != "second" {
		t.Fatalf("past the grace Cached() = %v, want [second]: the revoked token is still accepted", got)
	}
}

// Run is what keeps Cached current, so it must tick at the REFRESH cadence: at
// the read interval (a minute) a client that rotated first is refused by a
// Cached caller for up to a minute instead of about a second. The refresh is
// shortened here WITHOUT touching the interval, which is what tells the two
// cadences apart (WithInterval lowers both).
func TestRotatingRunTicksAtTheRefreshCadence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := NewRotating(path, discard(), WithGrace(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	r.set.refresh = 5 * time.Millisecond // the interval stays DefaultReadInterval
	r.mu.Lock()
	r.nextRead = time.Time{}
	r.mu.Unlock()

	ctx := t.Context()
	go r.Run(ctx)
	if err := os.WriteFile(path, []byte("second"), 0o600); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := r.Cached(); got[0] == "second" {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("Run did not refresh within 2s at a 5ms refresh cadence: it is ticking at the read interval")
}
